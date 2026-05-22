# Multi-Level Memory 设计

## 背景

基于 MQ 消息协议，消息 envelope 包含四个关键标识：

```
message_id → thread_id（一对一）
thread_id  → user_id, agent_name, project_id（一对多，均可缺省）
```

MQ consumer 水平扩展时：**一个 thread 只被一个 consumer 执行**，thread 级别无并发冲突。但 user、agent、project 维度的 memory 涉及多个 thread 并发写入，需要专门的并发策略。

---

## 一、知识层次体系：三个主实体 + Scoped Facts

### 设计原则

不按"维度组合"建立独立层，而是按**主实体**（User / Agent / Project）存储，通过 `scope_key` 标签表达跨维度知识。每条事实有且仅有一个归属实体，无跨实体重复。

### Scope 全图（无冗余）

```
memory_user (user_id, scope_key):
  ""                              通用用户特征（跨 agent/project 不变）
  "agent:{name}"                  U×A：用户在该 agent 下的工作偏好

memory_project (project_id, scope_key):
  ""                              通用项目事实（跨 agent/user 不变）
  "agent:{name}"                  A×P：该 agent 对此项目的专业知识
  "user:{uid}"                    U×P：用户在此项目中的角色/职责

memory_agent (agent_name):
  （无 scope，全局共享）           L-Agent：全局工具/模型经验
```

U×A×P（完整个性化）通过注入时同时加载 `memory_user("agent:{name}")` + `memory_project("user:{uid}")` + `memory_project("agent:{name}")` 组合覆盖，无需单独存储。

### 归属原则

**U×P 关系统一归属于 Project**（`memory_project(scope="user:{uid}")`），不在 `memory_user` 中重复存储。理由：用户在项目中的角色（创意总监、技术负责人）是由**项目结构定义**的，项目是该关系的权威来源。

### 各 scope 典型内容

| 实体 + scope | 典型内容 |
|-------------|---------|
| `memory_user ""` | 语言/沟通风格、领域背景、广泛质量偏好 |
| `memory_user "agent:director"` | 是否需要 preview、是否自己改 prompt、对等待时间的容忍度 |
| `memory_project ""` | 项目目标、交付物、格式约束（集数、时长、分辨率）|
| `memory_project "agent:director"` | 视觉风格指南、已审批角色设定、资产路径、进度状态 |
| `memory_project "user:{uid}"` | 用户在项目中的职责、决策权限范围 |
| `memory_agent` | 模型性能经验、Prompt 规律、参数经验、已知失败场景 |

---

## 二、存储：复用现有 Persistence 层

### 不引入新配置

deerflow 已有 `DatabaseConfig` 统一管理 checkpointer 与 app persistence，memory 表直接加入同一个 DB：

```yaml
# config.yaml（无需新增字段）
database:
  backend: sqlite          # memory | sqlite | postgres
  sqlite_dir: .deer-flow/data
  # postgres_url: $DATABASE_URL
```

SQLite 模式下 checkpointer、runs、feedback、memory **共享同一个 `deerflow.db`**，WAL 模式已在 `init_engine` 中配置（并发读 + 单写不阻塞）。

### ORM Model

```python
# persistence/memory/model.py
from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from deerflow.persistence.base import Base

class MemoryUserRow(Base):
    __tablename__ = "memory_user"
    user_id:    Mapped[str] = mapped_column(String(128), primary_key=True)
    scope_key:  Mapped[str] = mapped_column(String(256), primary_key=True, default="")
    summary:    Mapped[str | None] = mapped_column(Text)
    facts:      Mapped[str] = mapped_column(Text, default="[]")  # JSON array
    version:    Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class MemoryProjectRow(Base):
    __tablename__ = "memory_project"
    project_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    scope_key:  Mapped[str] = mapped_column(String(256), primary_key=True, default="")
    summary:    Mapped[str | None] = mapped_column(Text)
    facts:      Mapped[str] = mapped_column(Text, default="[]")
    version:    Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

class MemoryAgentRow(Base):
    __tablename__ = "memory_agent"
    agent_name: Mapped[str] = mapped_column(String(128), primary_key=True)
    summary:    Mapped[str | None] = mapped_column(Text)
    facts:      Mapped[str] = mapped_column(Text, default="[]")
    version:    Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
```

在 `persistence/models/__init__.py` 中 import 三个 model，`Base.metadata.create_all` 启动时自动建表，无需额外操作。

读写通过 `get_session_factory()`：

```python
from deerflow.persistence.engine import get_session_factory

async def load_user_memory(user_id: str, scope_key: str):
    factory = get_session_factory()
    if factory is None:          # backend=memory → 无持久化
        return None
    async with factory() as session:
        return await session.get(MemoryUserRow, (user_id, scope_key))
```

---

## 三、并发分析

### 各实体冲突频率

| 实体 | 并发场景 | 冲突频率 | 策略 |
|------|---------|---------|------|
| `memory_agent` | 所有使用同一 agent 的 thread | **高** | Memory Update Worker（MQ 单消费者）|
| `memory_project ("agent:{name}")` | 同 project 的多个 thread | **中** | 乐观锁 + 重试 |
| `memory_project ("user:{uid}")` | 用户角色变更（罕见）| 低 | 乐观锁 |
| `memory_project ("")` | 通用项目信息并发更新 | 低 | 乐观锁 |
| `memory_user (任意 scope)` | 同一用户同时有多个 thread（少见）| 低 | 乐观锁（失败丢弃，下次补偿）|

### 乐观锁（中/低频冲突）

```python
MAX_RETRIES = 3
for attempt in range(MAX_RETRIES):
    row = await session.get(MemoryProjectRow, (project_id, scope_key))
    new_facts = merge_facts(row.facts if row else "[]", extracted_fact)
    result = await session.execute(
        update(MemoryProjectRow)
        .where(MemoryProjectRow.project_id == project_id,
               MemoryProjectRow.scope_key == scope_key,
               MemoryProjectRow.version == row.version)
        .values(facts=new_facts, version=row.version + 1, updated_at=now())
    )
    if result.rowcount == 1:
        break
    # version 不匹配 → 重新读取并重试
```

### Memory Update Worker（L-Agent 高频冲突）

```
Thread Consumer（N 个 Pod）
    │
    ├── thread 执行完成 → 提取 agent knowledge
    └── publish → memory_agent_update_queue（专用 MQ topic）
                          │
                          ▼
              Memory Update Worker（单实例）
                          │
                   串行：load → merge → save
                          │
                 memory_agent 表（无并发）
```

Agent knowledge 通过 MQ 专用队列串行化处理，不引入分布式锁，利用现有 MQ 基础设施。

### backend=memory 时的降级

`get_session_factory()` 返回 `None` → 所有 memory 操作跳过持久化，仅保留进程内缓存（重启丢失），行为与 `MemorySaver` 一致。

---

## 四、Extraction Skill 位置

知识提取由 LLM + Skill 文件驱动，而非代码固定 schema。

### Skill 文件位置

```
skills/public/memory/
  extract-user.md             ← 通用用户知识提取（scope="" 和 "agent:{name}"）
  extract-project.md          ← 通用项目知识提取（scope="" 和 "user:{uid}"）

agents/director_agent/
  memory/
    extract-agent.md          ← director agent 自身工具/模型经验
    extract-user.md           ← 覆盖通用版：增加 "agent:director" scope 的提取规则
    extract-project.md        ← 覆盖通用版：增加 "agent:director" scope（视觉资产等）
```

L-Agent 无通用 fallback，每个 agent 必须提供自己的 `extract-agent.md`。

### Skill 解析优先级

```python
def get_extraction_skill(entity_type: str, agent_name: str) -> str:
    agent_skill = load_skill(f"agents/{agent_name}/memory/extract-{entity_type}.md")
    if agent_skill:
        return agent_skill
    if entity_type in ("user", "project"):
        return load_skill(f"skills/public/memory/extract-{entity_type}.md")
    raise MissingSkillError(f"{agent_name} must define memory/extract-agent.md")
```

---

## 五、注入策略

### 静态注入（Thread 启动时，一次性）

```python
def build_injection(user_id, agent_name, project_id) -> str:
    active_dims = set()
    if agent_name: active_dims.add(f"agent:{agent_name}")
    if project_id: active_dims.add(f"project:{project_id}")
    if user_id:    active_dims.add(f"user:{user_id}")

    sections = []

    if user_id:
        # memory_user：注入 scope ⊆ active_dims 的所有行
        user_rows = await load_all_scopes("memory_user", user_id)
        sections.append(format_user(filter_by_scope(user_rows, active_dims)))

    if agent_name:
        agent_row = await load("memory_agent", agent_name)
        sections.append(format_agent(agent_row))

    if project_id:
        # memory_project：注入 scope ⊆ active_dims 的所有行
        project_rows = await load_all_scopes("memory_project", project_id)
        sections.append(format_project(filter_by_scope(project_rows, active_dims)))

    return "\n\n".join(sections)
```

### scope 过滤规则

```python
def filter_by_scope(rows, active_dims: set[str]) -> list:
    result = []
    for row in rows:
        required = {s for s in row.scope_key.split("+") if s}
        if required.issubset(active_dims):
            result.append(row)
    return result
```

### 注入示例：(user=U123, agent=director, project=P456)

`active_dims = {"agent:director", "project:P456", "user:U123"}`

| 表 + scope_key | 命中 | 注入内容 |
|---------------|------|---------|
| `memory_user(U123, "")` | ✓ | 通用用户特征 |
| `memory_user(U123, "agent:director")` | ✓ | 用户与 director agent 的工作偏好 |
| `memory_agent("director")` | ✓ | 工具/模型经验 |
| `memory_project(P456, "")` | ✓ | 通用项目事实 |
| `memory_project(P456, "agent:director")` | ✓ | director agent 的项目专业知识 |
| `memory_project(P456, "user:U123")` | ✓ | U123 在此项目中的角色 |
| `memory_project(P456, "user:OTHER")` | ✗ | OTHER 不在 active_dims |

---

## 六、维度缺省降级

| 维度缺省 | 影响 |
|---------|------|
| `user_id` 缺省 | 不查询 `memory_user`，跳过所有 user 相关注入 |
| `agent_name` 缺省 | 不查询 `memory_agent`，跳过 agent knowledge 注入 |
| `project_id` 缺省 | 不查询 `memory_project`，跳过项目相关注入 |
| `thread_id` | 必须存在，由 LangGraph checkpoint DB 统一管理 Thread 级状态 |

---

## 七、数据流总览

```
MQ 消息 (thread_id, agent_name, user_id, project_id)
    │
    ├─ Thread Consumer 执行 lead_agent
    │     └─ 启动时注入：filter memory_user + memory_agent + memory_project
    │
    ├─ Thread 执行完成
    │     ├─ 提取 user knowledge  → 乐观锁写 memory_user
    │     ├─ 提取 project knowledge → 乐观锁写 memory_project
    │     └─ publish agent knowledge 事件 → memory_agent_update_queue
    │
    └─ Memory Update Worker（单实例）
          └─ 消费 memory_agent_update_queue → 串行更新 memory_agent
```

---

## 八、L-Thread（会话状态）

Thread 级别的会话状态（上下文压缩摘要、实体状态、决策链）由 **LangGraph checkpoint DB** 统一管理，自动恢复，无需 memory 系统介入。Thread 结束后，提取管道从 checkpoint 读取结构化摘要，作为更新 memory_user / memory_agent / memory_project 的原材料。

---

## 九、设计原则总结

1. **三实体，无冗余**：U/A/P 三张表，scope 覆盖全部7种维度组合，每条事实唯一归属
2. **U×P 归属 Project**：用户在项目中的角色存入 `memory_project(scope="user:{uid}")`，不在 user 表重复
3. **复用现有 persistence 层**：跟随 `DatabaseConfig` + `Base` + `get_session_factory()` 模式，无新配置
4. **DB 行级隔离主冲突**：不同 user / 不同 project 天然无冲突
5. **乐观锁处理中低频冲突**：version CAS，失败重试
6. **Memory Worker 串行化高频冲突**：L-Agent 通过 MQ 专用队列单实例处理
7. **Skill 驱动提取**：知识结构由 skill 文件定义，agent 可自定义各层提取策略
8. **注入按 scope 过滤**：只注入当前上下文激活的维度组合
