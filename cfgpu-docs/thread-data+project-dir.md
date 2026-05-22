# Thread Data 与 Project 共享目录设计

## 一、背景与需求

### 当前 ThreadData 结构

`ThreadDataMiddleware` 为每个 thread 设置三个路径，存入 LangGraph state 的 `thread_data` 字段：

```python
class ThreadDataState(TypedDict):
    workspace_path: str | None   # agent 沙箱工作目录
    uploads_path: str | None     # 用户上传文件目录
    outputs_path: str | None     # agent 输出给用户的文件目录
```

物理路径（以 AioSandbox 为例）：

```
.deer-flow/users/{user_id}/threads/{thread_id}/user-data/
  workspace/    ← bash/file 工具的工作目录，/mnt/user-data/workspace
  uploads/      ← 用户上传文件，/mnt/user-data/uploads
  outputs/      ← present_files 工具展示给用户，/mnt/user-data/outputs
```

### 需求：Project 资产跨 Thread 共享

多个 thread 可属于同一个 project（通过 MQ 消息 envelope 的 `project_id` 字段关联）。当 thread A 生成并审批了一张角色图后，thread B 需要能读取该资产（例如作为生视频的 `first_frame` 参数）。

当前设计中每个 thread 的 sandbox workspace 相互隔离，无法直接访问其他 thread 产出的文件。

---

## 二、Sandbox 关键行为：per-thread 而非 per-user

### Sandbox 以 thread_id 为键

```python
# sandbox_id 仅由 thread_id 决定，与 user_id 无关
def _deterministic_sandbox_id(thread_id: str) -> str:
    return hashlib.sha256(thread_id.encode()).hexdigest()[:8]
```

同一个 thread，无论哪个 user 发起请求，都命中同一个容器（in-process cache → warm pool → backend discover）。**1 个 thread = 1 个容器**。

### Mounts 在容器创建时固化

```python
@staticmethod
def _get_thread_mounts(thread_id: str):
    user_id = get_effective_user_id()   # ← 容器创建时刻的 user_id
    return [
        (paths.host_sandbox_work_dir(thread_id, user_id=user_id),
         "/mnt/user-data/workspace", False),
        ...
    ]
```

`_get_thread_mounts()` 仅在 `_create_sandbox()` 时调用一次。容器启动后，挂载点固化，后续其他 user acquire 同一 sandbox 时走缓存，**不会重新 mount**。

### 现有设计的隐含矛盾

| | ThreadDataMiddleware | AioSandbox 容器 |
|---|---|---|
| 设计意图 | per-user：`workspace_path = users/{user_id}/threads/{tid}/...` | 实际 per-thread：容器 mount 固化为**第一个** user 的目录 |
| 多 user 访问同一 thread | `thread_data.workspace_path` 随当前 user 变化 | 容器内 `/mnt/user-data/workspace` 始终是第一个 user 的目录 |

这一矛盾在当前 deerflow 中不会触发（前端 thread 是单用户的），但在 MQ 多用户场景下会产生路径不一致。

---

## 三、设计方案：新增 project_path

### 3.1 ThreadDataState 扩展

```python
class ThreadDataState(TypedDict):
    workspace_path: NotRequired[str | None]   # 不变
    uploads_path:   NotRequired[str | None]   # 不变
    outputs_path:   NotRequired[str | None]   # 不变
    project_path:   NotRequired[str | None]   # 新增：project_id 存在时设置
```

### 3.2 物理目录布局

`{base_dir}` 默认为 `{project_root}/.deer-flow`：

```
.deer-flow/
  users/{user_id}/threads/{thread_id}/user-data/   ← 现有结构，保持不变
    workspace/
    uploads/
    outputs/

  projects/{project_id}/                            ← 新增 project 共享目录
    workspace/                                      ← project 级共享工作区
      assets/                                       ← 审批后的资产（图片、视频）
        characters/
        scenes/
        videos/
```

### 3.3 虚拟路径契约

| 虚拟路径 | 物理路径 | 读写 | 说明 |
|---------|---------|------|------|
| `/mnt/user-data/workspace` | `users/{uid}/threads/{tid}/user-data/workspace` | rw | thread 私有，不变 |
| `/mnt/user-data/uploads` | `users/{uid}/threads/{tid}/user-data/uploads` | rw | thread 私有，不变 |
| `/mnt/user-data/outputs` | `users/{uid}/threads/{tid}/user-data/outputs` | rw | thread 私有，不变 |
| `/mnt/project` | `projects/{project_id}/workspace` | rw | project 共享，新增；无 project_id 时不挂载 |

### 3.4 ThreadDataMiddleware 改动

```python
def before_agent(self, state, runtime):
    context = runtime.context or {}
    thread_id = ...  # 原有逻辑不变
    user_id = get_effective_user_id()

    paths = self._get_thread_paths(thread_id, user_id=user_id)

    # 新增：条件性设置 project_path
    project_id = context.get("project_id")
    if project_id:
        paths["project_path"] = str(self._paths.project_workspace_dir(project_id))

    return {"thread_data": paths}
```

### 3.5 Paths 类改动

新增 `project_workspace_dir(project_id)` 方法：

```python
def project_workspace_dir(self, project_id: str) -> Path:
    return self.base_dir / "projects" / project_id / "workspace"

def host_project_workspace_dir(self, project_id: str) -> Path:
    return self.host_base_dir / "projects" / project_id / "workspace"
```

### 3.6 AioSandboxProvider 改动

在 `_get_thread_mounts()` 中增加 project mount（需要从 runtime context 获取 project_id）：

```python
@staticmethod
def _get_thread_mounts(thread_id: str, project_id: str | None = None):
    paths = get_paths()
    user_id = get_effective_user_id()
    paths.ensure_thread_dirs(thread_id, user_id=user_id)

    mounts = [
        (paths.host_sandbox_work_dir(thread_id, user_id=user_id),
         f"{VIRTUAL_PATH_PREFIX}/workspace", False),
        (paths.host_sandbox_uploads_dir(thread_id, user_id=user_id),
         f"{VIRTUAL_PATH_PREFIX}/uploads", False),
        (paths.host_sandbox_outputs_dir(thread_id, user_id=user_id),
         f"{VIRTUAL_PATH_PREFIX}/outputs", False),
        (paths.host_acp_workspace_dir(thread_id, user_id=user_id),
         "/mnt/acp-workspace", True),
    ]

    if project_id:
        project_dir = paths.host_project_workspace_dir(project_id)
        project_dir.mkdir(parents=True, exist_ok=True)
        mounts.append((str(project_dir), "/mnt/project", False))

    return mounts
```

**注意**：`project_id` 需要在容器创建时从 runtime context 传入，因为 mounts 在创建时固化。同一 project 的所有 thread 挂载的是**相同的物理路径**，所以不存在像 user_id 那样"谁先创建容器就用谁的目录"的问题。

### 3.7 LocalSandboxProvider 改动

`LocalSandboxProvider` 通过 `PathMapping` 做虚拟路径翻译，同步加入 project 映射：

```python
if project_id:
    path_mappings.append(PathMapping(
        virtual_prefix="/mnt/project",
        host_path=paths.project_workspace_dir(project_id),
    ))
```

---

## 四、资产写入流程

```
Thread A 生成角色图（cfgpu 返回 CDN URL）
  → HIL 审批通过
  → agent 将图片下载/复制到 /mnt/project/assets/characters/char_01.png
  → 更新 /mnt/project/assets.json（追加记录）

Thread B 启动
  → L4 注入 assets.json（含 char_01.png 的路径和描述）
  → 生视频：first_frame = "/mnt/project/assets/characters/char_01.png"
```

### assets.json 并发写入

多个 thread 并发写 `assets.json` 时，使用 atomic rename（与 deerflow memory 系统一致）：

```
write → tmp file → os.replace(tmp, assets.json)
```

各 thread 写入不同 asset 文件（文件名不冲突）是安全的，无需加锁。

---

## 五、与 multi-level memory 的关系

| | 路径 | 用途 |
|---|---|---|
| `memory/projects/{project_id}/assets.json` | memory 目录 | L4 资产**元数据**索引（描述、创建时间、对应 thread） |
| `projects/{project_id}/workspace/assets/` | project 目录 | 资产**实体文件**（本地持久化，供 sandbox 直接访问） |

两者互补：memory 层存索引供 context 注入，project workspace 存文件供工具直接读写。

---

## 六、实现步骤

1. `ThreadDataState` 加 `project_path` 字段
2. `Paths` 类加 `project_workspace_dir()` / `host_project_workspace_dir()` 方法
3. `ThreadDataMiddleware.before_agent()` 读 `runtime.context["project_id"]`，有则设 `project_path`
4. `AioSandboxProvider._get_thread_mounts()` 接受 `project_id` 参数，有则追加 `/mnt/project` mount
5. `LocalSandboxProvider` 同步加 `/mnt/project` PathMapping
6. 更新 `sandbox/tools.py` 的路径校验，将 `/mnt/project` 加入允许的读写路径白名单
