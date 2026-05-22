# Multi-Level Memory 实现方案

> 本文档基于 [multi-level-memory设计.md](./multi-level-memory设计.md)，描述具体的代码实现路径。
> 所有新增代码直接放入 `packages/harness/deerflow/`，遵循现有架构模式。

---

## 一、整体架构图

```
MQ 消息 (thread_id, agent_name, user_id, project_id)
    │
    ├─ Thread Consumer 执行 lead_agent
    │     └─ MlmMiddleware.before_agent()（首轮）
    │           ├─ load memory_user   (user_id + scope 过滤)
    │           ├─ load memory_agent  (agent_name)
    │           └─ load memory_project (project_id + scope 过滤)
    │                 → 注入 <system-reminder> 到首条 HumanMessage 前
    │
    ├─ Thread 执行完成
    │     └─ MlmMiddleware.after_agent()
    │           ├─ 提取 user knowledge   → 乐观锁写 memory_user
    │           ├─ 提取 project knowledge → 乐观锁写 memory_project
    │           └─ publish agent knowledge event → memory_agent_update_queue（MQ topic）
    │
    └─ Memory Update Worker（单实例 MQ consumer）
          └─ 消费 memory_agent_update_queue → 串行更新 memory_agent
```

---

## 二、新增文件列表

所有新增文件均在 `packages/harness/deerflow/` 下，与现有模块平级：

```
deerflow/
  persistence/
    memory/                          ← 新增子包
      __init__.py
      model.py                       ← ORM 模型：MemoryUserRow / MemoryProjectRow / MemoryAgentRow
      repository.py                  ← 乐观锁 load / upsert

  agents/
    memory/                          ← 现有目录，追加文件
      skill_resolver.py              ← 新增：get_extraction_skill()
      extractor.py                   ← 新增：Skill 驱动的 LLM 提取
      injector.py                    ← 新增：build_injection() + filter_by_scope()
      mlm_queue.py                   ← 新增：MLM 防抖提取队列

    middlewares/                     ← 现有目录，追加文件
      mlm_middleware.py              ← 新增：注入（before_agent 首轮）+ 提取触发（after_agent）

  workers/                           ← 新增子包
    __init__.py
    memory_agent_worker.py           ← 新增：Memory Update Worker（单实例 MQ consumer）
```

### 修改的现有文件

| 文件 | 改动 |
|------|------|
| `persistence/models/__init__.py` | import 三个 memory model，触发 `Base.metadata.create_all` 建表 |
| `agents/lead_agent/agent.py` | `_build_middlewares()` 中注册 `MlmMiddleware` |
| `agents/memory/summarization_hook.py` | 注册 `mlm_flush_hook`（压缩前 flush 提取队列）|

### Skill 文件（deer-flow home 目录，遵循现有存储规范）

```
{deer_flow_home}/
  skills/public/memory/
    extract-user.md                  ← 通用 user 知识提取 skill
    extract-project.md               ← 通用 project 知识提取 skill

  agents/director_agent/memory/
    extract-agent.md                 ← director agent 专属工具/模型经验提取
    extract-user.md                  ← 覆盖通用版（增加 agent:director scope）
    extract-project.md               ← 覆盖通用版（增加视觉资产等）
```

---

## 三、ORM 模型（`deerflow/persistence/memory/model.py`）

```python
from datetime import datetime
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

在 `deerflow/persistence/models/__init__.py` 追加：

```python
from deerflow.persistence.memory.model import MemoryAgentRow, MemoryProjectRow, MemoryUserRow
```

---

## 四、Repository（`deerflow/persistence/memory/repository.py`）

### 4.1 Load

```python
from deerflow.persistence.engine import get_session_factory
from deerflow.persistence.memory.model import MemoryAgentRow, MemoryProjectRow, MemoryUserRow

async def load_user_scopes(user_id: str) -> list[MemoryUserRow]:
    factory = get_session_factory()
    if factory is None:
        return []
    async with factory() as session:
        result = await session.execute(
            select(MemoryUserRow).where(MemoryUserRow.user_id == user_id)
        )
        return list(result.scalars().all())

async def load_project_scopes(project_id: str) -> list[MemoryProjectRow]:
    ...  # 同上，查 memory_project

async def load_agent(agent_name: str) -> MemoryAgentRow | None:
    factory = get_session_factory()
    if factory is None:
        return None
    async with factory() as session:
        return await session.get(MemoryAgentRow, agent_name)
```

### 4.2 Upsert（乐观锁）

```python
MAX_RETRIES = 3

async def upsert_user_scope(
    user_id: str, scope_key: str,
    new_facts: list[dict], new_summary: str | None,
) -> bool:
    factory = get_session_factory()
    if factory is None:
        return False

    for _ in range(MAX_RETRIES):
        async with factory() as session:
            row = await session.get(MemoryUserRow, (user_id, scope_key))
            if row is None:
                session.add(MemoryUserRow(
                    user_id=user_id, scope_key=scope_key,
                    facts=json.dumps(new_facts, ensure_ascii=False),
                    summary=new_summary, version=0, updated_at=now(),
                ))
                try:
                    await session.commit()
                    return True
                except IntegrityError:
                    continue  # 并发 INSERT，重试

            result = await session.execute(
                update(MemoryUserRow)
                .where(MemoryUserRow.user_id == user_id,
                       MemoryUserRow.scope_key == scope_key,
                       MemoryUserRow.version == row.version)
                .values(facts=json.dumps(new_facts, ensure_ascii=False),
                        summary=new_summary,
                        version=row.version + 1, updated_at=now())
            )
            await session.commit()
            if result.rowcount == 1:
                return True
            # rowcount == 0 → version 被并发更新，重试

    return False

async def upsert_project_scope(project_id, scope_key, new_facts, new_summary):
    ...  # 结构相同，操作 MemoryProjectRow

async def upsert_agent(agent_name, new_facts, new_summary):
    ...  # PK 为 agent_name 单列，仅由 Memory Update Worker 调用
```

---

## 五、Skill 解析（`deerflow/agents/memory/skill_resolver.py`）

```python
from deerflow.config.paths import get_paths

def get_extraction_skill(entity_type: str, agent_name: str) -> str:
    paths = get_paths()
    agent_path = paths.base_dir / "agents" / agent_name / "memory" / f"extract-{entity_type}.md"
    if agent_path.exists():
        return agent_path.read_text()

    if entity_type in ("user", "project"):
        public_path = paths.base_dir / "skills" / "public" / "memory" / f"extract-{entity_type}.md"
        return public_path.read_text()

    raise MissingSkillError(
        f"Agent '{agent_name}' must provide agents/{agent_name}/memory/extract-agent.md"
    )
```

---

## 六、Extractor（`deerflow/agents/memory/extractor.py`）

```python
@dataclass
class ExtractionResult:
    facts: list[dict]       # skill 文件定义结构
    summary: str | None
    scope_key: str          # 归属的 scope_key

async def extract_user_knowledge(
    messages: list,
    user_id: str,
    agent_name: str,
    existing: dict[str, list[dict]],   # scope_key → facts
) -> list[ExtractionResult]:
    skill_text = get_extraction_skill("user", agent_name)
    # LLM 调用，输出 [{scope_key, facts, summary}, ...]
    ...

async def extract_project_knowledge(
    messages, project_id, agent_name, user_id, existing
) -> list[ExtractionResult]:
    skill_text = get_extraction_skill("project", agent_name)
    ...

async def extract_agent_knowledge(
    messages, agent_name, existing_facts
) -> ExtractionResult:
    skill_text = get_extraction_skill("agent", agent_name)
    ...
```

---

## 七、Injector（`deerflow/agents/memory/injector.py`）

```python
def filter_by_scope(rows: list, active_dims: set[str]) -> list:
    result = []
    for row in rows:
        required = {s for s in row.scope_key.split("+") if s}
        if required.issubset(active_dims):
            result.append(row)
    return result

async def build_injection(
    user_id: str | None,
    agent_name: str | None,
    project_id: str | None,
) -> str:
    active_dims: set[str] = set()
    if agent_name:  active_dims.add(f"agent:{agent_name}")
    if project_id:  active_dims.add(f"project:{project_id}")
    if user_id:     active_dims.add(f"user:{user_id}")

    sections: list[str] = []

    if user_id:
        rows = await load_user_scopes(user_id)
        relevant = filter_by_scope(rows, active_dims)
        if relevant:
            sections.append(_format_user(relevant))

    if agent_name:
        row = await load_agent(agent_name)
        if row:
            sections.append(_format_agent(row))

    if project_id:
        rows = await load_project_scopes(project_id)
        relevant = filter_by_scope(rows, active_dims)
        if relevant:
            sections.append(_format_project(relevant))

    return "\n\n".join(sections)
```

---

## 八、MLM 队列（`deerflow/agents/memory/mlm_queue.py`）

复用现有 `MemoryUpdateQueue` 的防抖模式（30s debounce，per-thread 去重）：

```python
class MlmUpdateQueue:
    """user/project 乐观锁写 DB；agent knowledge 发布到 MQ topic。"""

    async def _process(self, ctx: MlmContext) -> None:
        if ctx.user_id:
            existing = {row.scope_key: json.loads(row.facts)
                        for row in await load_user_scopes(ctx.user_id)}
            for result in await extract_user_knowledge(
                ctx.messages, ctx.user_id, ctx.agent_name, existing
            ):
                merged = _merge_facts(existing.get(result.scope_key, []), result.facts)
                await upsert_user_scope(ctx.user_id, result.scope_key, merged, result.summary)

        if ctx.project_id:
            existing = {row.scope_key: json.loads(row.facts)
                        for row in await load_project_scopes(ctx.project_id)}
            for result in await extract_project_knowledge(
                ctx.messages, ctx.project_id, ctx.agent_name, ctx.user_id, existing
            ):
                merged = _merge_facts(existing.get(result.scope_key, []), result.facts)
                await upsert_project_scope(ctx.project_id, result.scope_key, merged, result.summary)

        if ctx.agent_name:
            existing_row = await load_agent(ctx.agent_name)
            existing_facts = json.loads(existing_row.facts) if existing_row else []
            result = await extract_agent_knowledge(ctx.messages, ctx.agent_name, existing_facts)
            await _publish_agent_update(ctx.agent_name, result)  # → MQ topic

def _merge_facts(existing: list[dict], new: list[dict]) -> list[dict]:
    seen = {f.get("content", ""): f for f in existing}
    for fact in new:
        seen[fact.get("content", "")] = fact
    return list(seen.values())

_mlm_queue: MlmUpdateQueue | None = None

def get_mlm_queue() -> MlmUpdateQueue:
    global _mlm_queue
    if _mlm_queue is None:
        _mlm_queue = MlmUpdateQueue()
    return _mlm_queue
```

---

## 九、MlmMiddleware（`deerflow/agents/middlewares/mlm_middleware.py`）

```python
class MlmMiddleware(AgentMiddleware):
    """首轮注入三实体 memory；每次执行后异步提取。"""

    async def before_agent(self, state, runtime) -> dict | None:
        if self._already_injected(state):
            return None

        context = runtime.context or {}
        injection = await build_injection(
            user_id=context.get("user_id"),
            agent_name=context.get("agent_name"),
            project_id=context.get("project_id"),
        )
        if not injection:
            return None

        return _prepend_reminder_message(state, injection, flag="mlm_injected")

    async def after_agent(self, state, runtime) -> dict | None:
        context = runtime.context or {}
        thread_id = runtime.thread_id
        if not thread_id:
            return None

        get_mlm_queue().add(
            thread_id=thread_id,
            messages=_filter_messages_for_memory(state.get("messages", [])),
            user_id=context.get("user_id"),
            agent_name=context.get("agent_name"),
            project_id=context.get("project_id"),
        )
        return None

    def _already_injected(self, state) -> bool:
        return any(
            getattr(m, "additional_kwargs", {}).get("mlm_injected")
            for m in state.get("messages", [])
        )
```

### Summarization Hook（追加到 `deerflow/agents/memory/summarization_hook.py`）

```python
def mlm_flush_hook(event: SummarizationEvent) -> None:
    context = event.runtime.context or {}
    get_mlm_queue().add_nowait(
        thread_id=event.thread_id,
        messages=_filter_messages_for_memory(list(event.messages_to_summarize)),
        user_id=context.get("user_id"),
        agent_name=context.get("agent_name"),
        project_id=context.get("project_id"),
    )
```

---

## 十、Memory Update Worker（`deerflow/workers/memory_agent_worker.py`）

```python
class MemoryAgentWorker:
    """单实例：串行消费 memory_agent_update_queue，无并发冲突。"""

    async def run(self):
        async for msg in self.mq_consumer.consume("memory_agent_update_queue"):
            await self._handle(msg)

    async def _handle(self, msg: AgentKnowledgeEvent) -> None:
        existing_row = await load_agent(msg.agent_name)
        existing_facts = json.loads(existing_row.facts) if existing_row else []
        merged = _merge_facts(existing_facts, msg.new_facts)
        summary = msg.summary or (existing_row.summary if existing_row else None)
        await upsert_agent(msg.agent_name, merged, summary)
        await self.mq_consumer.ack(msg)
```

**部署**：单 Pod 或使用 MQ exclusive consumer，不随 Thread Consumer 水平扩展。

---

## 十一、注册到 lead_agent（`deerflow/agents/lead_agent/agent.py`）

```python
from deerflow.agents.middlewares.mlm_middleware import MlmMiddleware
from deerflow.agents.memory.summarization_hook import mlm_flush_hook

# _build_middlewares()
middlewares = [
    ...,
    DynamicContextMiddleware(...),
    MlmMiddleware(),          # 新增
    MemoryMiddleware(...),
    ...,
]

# _build_hooks()
hooks = [..., mlm_flush_hook]  # 新增
```

---

## 十二、实现步骤（按依赖顺序）

| # | 组件 | 文件 | 依赖 | 可独立测试 |
|---|------|------|------|---------|
| 1 | ORM 模型 | `persistence/memory/model.py` | `Base` | ✅ schema 检查 |
| 2 | 模型注册 | `persistence/models/__init__.py` | 步骤 1 | ✅ 建表验证 |
| 3 | Repository | `persistence/memory/repository.py` | 步骤 1–2 | ✅ SQLite in-memory |
| 4 | Skill Resolver | `agents/memory/skill_resolver.py` | skill `.md` 文件 | ✅ |
| 5 | Skill 文件 | `{home}/skills/public/memory/*.md` 等 | 无 | ✅（人工审阅）|
| 6 | Extractor | `agents/memory/extractor.py` | 步骤 3–4 | ✅（mock LLM）|
| 7 | Injector | `agents/memory/injector.py` | 步骤 3 | ✅ |
| 8 | MLM Queue | `agents/memory/mlm_queue.py` | 步骤 3、6 | ✅ |
| 9 | MlmMiddleware | `agents/middlewares/mlm_middleware.py` | 步骤 7–8 | ✅（mock state）|
| 10 | Flush Hook | `agents/memory/summarization_hook.py` | 步骤 8 | ✅ |
| 11 | Memory Worker | `workers/memory_agent_worker.py` | 步骤 3、MQ | ✅ |
| 12 | lead_agent 注册 | `agents/lead_agent/agent.py` | 步骤 9–10 | integration test |

---

## 十三、与现有 Memory 的共存

| | 现有 `MemoryMiddleware` | `MlmMiddleware`（新增）|
|---|---|---|
| **注入时机** | 每轮 `before_model`（动态）| 首轮 `before_agent`（一次性）|
| **注入内容** | 通用用户 facts | 三实体结构化知识（DB）|
| **提取时机** | `after_agent` + summarization hook | 同上（共享触发点）|
| **存储** | `users/{uid}/memory.json`（文件）| `memory_user/project/agent` 表（DB）|
| **并发策略** | atomic rename | 乐观锁 + Memory Update Worker |
