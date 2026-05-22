# Project Management 设计文档

## 背景

基于 multi-level-memory 设计，`project_id` 是 memory 系统的核心维度之一。同一个 project 可以包含多个 thread，多个 user 可以共同协作。本文档设计支持"多用户创建和共享 project、project 包含多个 thread"的后端 API、路由和前端界面逻辑。

MQ 消息协议已支持 `project_id`（`TaskMessage.project_id`），memory 表已就绪（`MemoryProjectRow`），本次补齐 project 实体本身的 CRUD 和 UI。

---

## 一、数据模型

### 新增：`projects` 表

```python
class ProjectRow(Base):
    __tablename__ = "projects"

    project_id:  Mapped[str]      # UUID, primary key
    name:        Mapped[str]      # 显示名称，最长 256 字符
    description: Mapped[str|None] # 可选描述
    owner_id:    Mapped[str]      # 创建者 user.id，index
    created_at:  Mapped[datetime]
    updated_at:  Mapped[datetime]
```

### 新增：`project_members` 表

```python
class ProjectMemberRow(Base):
    __tablename__ = "project_members"

    project_id: Mapped[str]  # PK part 1
    user_id:    Mapped[str]  # PK part 2，index
    role:       Mapped[str]  # "owner" | "editor" | "viewer"
    joined_at:  Mapped[datetime]
```

角色权限：

| role    | 读 threads | 新建 thread | 修改 project | 管理成员 | 删除 project |
|---------|-----------|-------------|-------------|---------|-------------|
| owner   | ✓         | ✓           | ✓           | ✓       | ✓           |
| editor  | ✓         | ✓           | ✗           | ✗       | ✗           |
| viewer  | ✓         | ✗           | ✗           | ✗       | ✗           |

### 变更：`threads_meta` 表

新增可空列：

```python
project_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
```

Thread 创建时可携带 `project_id`，关联后不再变更。无 `project_id` 的 thread 为"个人 thread"，行为与现在完全一致（向后兼容）。

---

## 二、后端 API

### 路由前缀：`/api/projects`

注册在 `backend/app/gateway/routers/projects.py`，加入 `routers/__init__.py`。

#### 2.1 Project CRUD

| Method | Path | 说明 | 权限 |
|--------|------|-----|------|
| `GET` | `/api/projects` | 列出我参与的所有 project（owner + member） | 登录 |
| `POST` | `/api/projects` | 创建 project，自动写一条 owner 成员记录 | 登录 |
| `GET` | `/api/projects/{project_id}` | 获取 project 详情（含成员列表） | member |
| `PATCH` | `/api/projects/{project_id}` | 修改 name / description | owner |
| `DELETE` | `/api/projects/{project_id}` | 删除 project（级联软删除 threads 关联） | owner |

**列表响应 `ProjectResponse`：**
```json
{
  "project_id": "uuid",
  "name": "剧集 S01",
  "description": "...",
  "owner_id": "user-uuid",
  "my_role": "editor",
  "member_count": 3,
  "thread_count": 12,
  "created_at": "ISO",
  "updated_at": "ISO"
}
```

#### 2.2 成员管理

| Method | Path | 说明 | 权限 |
|--------|------|-----|------|
| `GET` | `/api/projects/{project_id}/members` | 列出所有成员 | member |
| `POST` | `/api/projects/{project_id}/members` | 邀请成员（by email 或 user_id） | owner |
| `PATCH` | `/api/projects/{project_id}/members/{user_id}` | 变更角色 | owner |
| `DELETE` | `/api/projects/{project_id}/members/{user_id}` | 移除成员（owner 不可被移除） | owner |

**成员响应 `ProjectMemberResponse`：**
```json
{
  "user_id": "uuid",
  "email": "user@example.com",
  "role": "editor",
  "joined_at": "ISO"
}
```

#### 2.3 Project 下的 Threads

| Method | Path | 说明 | 权限 |
|--------|------|-----|------|
| `GET` | `/api/projects/{project_id}/threads` | 查询属于该 project 的 threads（支持 limit/offset） | member |

复用现有 `ThreadResponse` 结构，通过 `ThreadMetaStore.search(metadata={"project_id": project_id})` 实现（无需新 SQL，沿用 metadata JSON filter）。

> 另一选项：直接在 `threads_meta` 加 `project_id` 专用列并建索引，查询性能更好。两种方案都可行，专用列方案推荐用于 project thread 数量 > 1000 的场景。

#### 2.4 Thread 创建变更

在现有 `ThreadCreateRequest` 增加可选字段：

```python
project_id: str | None = Field(default=None, description="Associate thread with a project")
```

创建时：
1. 若提供 `project_id`，校验当前用户是该 project 的 member（且 role ≠ viewer）
2. 将 `project_id` 存入 `threads_meta`

---

## 三、前端设计

### 3.1 路由结构

```
/workspace/
  chats/                    # 现有：个人 threads
  agents/                   # 现有：agents
  projects/                 # 新增
    page.tsx                # 项目列表页
    [project_id]/
      page.tsx              # 重定向 → .../threads
      layout.tsx            # 注入 ProjectContext
      threads/
        page.tsx            # 项目 threads 列表（同 chats 页但带 project filter）
      settings/
        page.tsx            # 项目设置（名称、描述、成员管理）
```

### 3.2 Sidebar 变更

在 `WorkspaceNavChatList` 新增 **Projects** 入口，位于 Chats 和 Agents 之间：

```
[MessagesSquare] Chats
[FolderOpen]     Projects          ← 新增
[BotIcon]        Agents
```

点击展开时，在 sidebar 内显示项目列表（折叠 icon 模式下仅显示 icon）：

```
▼ Projects                    [+]
  📁 剧集 S01              (active)
  📁 剧集 S02
  📁 Demo 项目
```

`[+]` 触发创建项目对话框。

### 3.3 页面结构

#### 项目列表页 `/workspace/projects`

```
┌─────────────────────────────────────────────────┐
│  Projects                          [+ 新建项目]  │
├─────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐           │
│  │ 📁 剧集 S01   │  │ 📁 剧集 S02   │           │
│  │ 3 成员 · 12   │  │ 2 成员 · 5    │           │
│  │ threads       │  │ threads       │           │
│  │ owner         │  │ editor        │           │
│  └───────────────┘  └───────────────┘           │
│  ┌───────────────┐                              │
│  │ 📁 Demo 项目  │                              │
│  │ 1 成员 · 2    │                              │
│  │ threads       │                              │
│  │ owner         │                              │
│  └───────────────┘                              │
└─────────────────────────────────────────────────┘
```

#### 项目 Threads 页 `/workspace/projects/[project_id]/threads`

复用现有 `chats` 页面结构，差异：
- 页面顶部显示项目名称 + breadcrumb（`Projects > 剧集 S01`）
- Thread 列表通过 `GET /api/projects/{project_id}/threads` 获取
- 新建 Thread 时自动带 `project_id`
- 右上角「⚙️ 项目设置」入口

```
┌────────────────────────────────────────────────┐
│  < Projects  /  剧集 S01               [⚙️] [+] │
├────────────────────────────────────────────────┤
│  ● 第一集场景设计         2h ago               │
│  ● 角色造型讨论           昨天                 │
│  ● 分镜脚本 v2            3天前                │
└────────────────────────────────────────────────┘
```

#### 项目设置页 `/workspace/projects/[project_id]/settings`

两个 Tab：

**信息 Tab**
- 项目名称（可编辑，owner only）
- 描述（可编辑，owner only）
- 创建时间 / 最后更新

**成员 Tab**
```
成员管理                              [+ 邀请]
─────────────────────────────────────────────
avatar  dollychen@example.com   owner    —
avatar  alice@example.com       editor   [角色▼] [移除]
avatar  bob@example.com         viewer   [角色▼] [移除]
```

邀请对话框：输入 email → 选择角色 → 发送邀请（后端按 email 查找 UserRow）

### 3.4 前端模块结构

```
frontend/src/core/projects/
  types.ts         # Project, ProjectMember, ProjectRole 类型
  api.ts           # fetch* 函数，调用 /api/projects 系列
  hooks.ts         # useProjects, useProject, useProjectMembers,
                   # useCreateProject, useUpdateProject, useDeleteProject,
                   # useInviteMember, useRemoveMember
  index.ts         # re-export

frontend/src/components/
  project-list.tsx               # 项目卡片网格
  project-nav-list.tsx           # sidebar 展开列表
  create-project-dialog.tsx      # 新建项目对话框
  project-settings-members.tsx   # 成员管理面板
  workspace-nav-chat-list.tsx    # 现有文件，新增 Projects 入口
```

### 3.5 状态管理

- 使用 `@tanstack/react-query`（现有项目已用）
- Query keys:
  - `["projects"]` — 项目列表
  - `["projects", projectId]` — 单个项目详情
  - `["projects", projectId, "members"]` — 成员列表
  - `["projects", projectId, "threads"]` — 项目 threads
- 创建/修改/删除后 `invalidate` 对应 query key
- `ProjectContext`（React Context）在 `/workspace/projects/[project_id]/layout.tsx` 注入，提供当前 `project_id` 和用户角色，供子页面判断权限

---

## 四、数据流

```
用户操作
  │
  ├─ 创建 Project
  │    POST /api/projects → 写 projects + project_members(role=owner)
  │
  ├─ 邀请成员
  │    POST /api/projects/{id}/members → 按 email 查 users → 写 project_members
  │
  ├─ 在 Project 下新建 Thread
  │    POST /api/threads {project_id} → 写 threads_meta(project_id=...) + checkpoint
  │
  ├─ 发送消息（MQ 路径）
  │    MQ envelope 携带 project_id → Consumer 注入 memory_project 上下文
  │    → Thread 执行完成 → 乐观锁写 memory_project
  │
  └─ 查看 Project Threads
       GET /api/projects/{id}/threads → filter threads_meta by project_id
```

---

## 五、实现优先级

| 优先级 | 工作 | 说明 |
|--------|------|------|
| P0 | ProjectRow + ProjectMemberRow 模型 | 依赖所有后续功能 |
| P0 | `threads_meta` 加 `project_id` 列 | Thread ↔ Project 关联 |
| P0 | `/api/projects` CRUD 路由 | 基础功能 |
| P0 | `/api/projects/{id}/threads` | Project 内 thread 列表 |
| P1 | Frontend core/projects/ 模块 | 类型 + API client + hooks |
| P1 | Sidebar Projects 导航 + 项目列表页 | 主要 UI 入口 |
| P1 | 项目 Threads 页 | 核心使用场景 |
| P2 | 成员管理 API + UI | 协作功能 |
| P2 | 项目设置页 | 管理功能 |
| P3 | Thread 创建带 `project_id` 校验 | 权限细化 |

---

## 六、待决策点

1. **`project_id` 存储位置**：存入 `threads_meta` 专用列（推荐，查询性能好）vs. 存入 `metadata_json`（无需改表）？
2. **邀请机制**：直接按 email 加入（简单）vs. 邀请链接（后续扩展）？目前建议按 email 直接加入。
3. **删除 project 行为**：仅解除 threads 的 `project_id` 关联，还是同步删除 project 内所有 threads？建议：解除关联（threads 本身保留）。
4. **viewer 是否看到 thread 内容**：目前设计 viewer 可以读 project threads，是否需要更细粒度的 thread 级权限？建议本期不做 thread 级权限。
