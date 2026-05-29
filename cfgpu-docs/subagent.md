# Deerflow Subagent 架构与 LangGraph Checkpoint 机制

版本：1.0 | 适用分支：director-agent

---

## 1. Subagent 的决策与触发

### 1.1 通过 `task` tool 触发

Lead agent 的 LLM 决策后，通过调用 `task` tool（`@tool("task", ...)`）委派工作给 subagent：

```python
# tools/builtins/task_tool.py
@tool("task", parse_docstring=True)
async def task_tool(
    runtime: Runtime,
    description: str,   # 任务简短描述（用于 SSE 展示）
    prompt: str,        # 传给 subagent 的完整任务描述
    subagent_type: str, # subagent 类型（general-purpose / bash / 自定义）
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> str:
```

**核心流程**：

```
LLM 输出 AIMessage { tool_calls: [task(scene_1), task(scene_2)] }
        ↓
task_tool 被调用（tools node 内）
        ↓
SubagentExecutor.execute_async(prompt, task_id=tool_call_id)
        ↓ (每 5s poll)
writer(task_started / task_running / task_completed)
        ↓
返回 "Task Succeeded. Result: ..." → ToolMessage
```

`task_id` 直接复用 `tool_call_id`，天然唯一，可作为恢复的幂等键。

### 1.2 并发控制

`SubagentLimitMiddleware` 在 `after_model` 阶段截断超出上限的 `task` tool_calls，防止并发爆炸：

```python
# config
max_concurrent_subagents = cfg.get("max_concurrent_subagents", 3)  # 默认 3
```

### 1.3 SSE 事件

| 事件类型 | 时机 |
|---|---|
| `task_started` | subagent 开始执行 |
| `task_running` | subagent 产生新 AIMessage 时（每条） |
| `task_completed` | subagent 正常完成 |
| `task_failed` | subagent 执行失败 |
| `task_timed_out` | 超时 |
| `task_cancelled` | 用户取消 |

---

## 2. Subagent 执行引擎

### 2.1 完整的独立 Agent Loop

Subagent **是**一个完整的 agent loop，与 lead agent 使用相同的 `create_agent` 框架：

```python
# subagents/executor.py SubagentExecutor._create_agent()
return create_agent(
    model=model,
    tools=tools,              # 过滤掉 task tool（防止递归嵌套）
    middleware=middlewares,   # 独立 middleware 链
    system_prompt=None,
    state_schema=ThreadState,
)
```

**关键差异**：
- `subagent_enabled=False`：不包含 `task` tool，无法生成 sub-subagent
- 独立 `ThreadState`：不与 parent 共享 graph state
- **无 checkpointer**：当前实现不保存 subagent 自身的 checkpoint（详见第 4 节）

### 2.2 Isolated Event Loop 架构

Subagent 不能直接在 parent 的 asyncio loop 里运行，因为 `task_tool` 本身就跑在 parent 的 tools node 里（同一个 loop），嵌套会死锁。解法：独立的 daemon thread + 专属 event loop：

```
Parent Agent (asyncio loop L1)
  └── tools node: task_tool await
        └── execute_async() → _scheduler_pool 提交 run_task
              └── _submit_to_isolated_loop_in_context()
                    └── asyncio.run_coroutine_threadsafe(
                          _aexecute(), _isolated_subagent_loop  ← L2
                        )

_isolated_subagent_loop (daemon thread "subagent-persistent-loop", loop L2)
  ├── subagent_1._aexecute() (coroutine)
  ├── subagent_2._aexecute() (coroutine)
  └── subagent_3._aexecute() (coroutine)
```

`_isolated_subagent_loop` 是全局单例，进程生命周期内持久存在，所有 subagent coroutine 在同一个 loop 上调度（非线程并发，asyncio 交替执行）。

### 2.3 执行流程（`_aexecute`）

```python
async def _aexecute(self, task, result_holder):
    state, filtered_tools = await self._build_initial_state(task)
    # state = [SystemMessage(system_prompt), HumanMessage(task)]

    agent = self._create_agent(filtered_tools)
    run_config = {
        "recursion_limit": self.config.max_turns,
        "callbacks": [collector],          # token 收集
        "configurable": {"thread_id": self.thread_id},  # sandbox 访问
    }

    async for chunk in agent.astream(state, config=run_config, stream_mode="values"):
        if result.cancel_event.is_set():   # 协作式取消
            ...
        # 收集 AI messages 到 result.ai_messages（实时更新）
        ...
```

### 2.4 取消机制

取消是**协作式**的，在 `astream` 迭代边界检测：

```python
result.cancel_event.set()          # parent 发出取消信号
# subagent 在下一次 chunk yield 时检测到并 break
```

长时间运行的工具调用（如 cfgpu MCP）在单次 astream 迭代内无法被中断，只能等待工具调用完成后才能响应取消。

---

## 3. LangGraph Checkpoint 保存机制

### 3.1 Checkpoint 存储结构（Postgres）

```sql
-- 主 checkpoint 表
checkpoints (
    thread_id, checkpoint_ns, checkpoint_id,  -- 联合主键
    parent_checkpoint_id,                      -- 上一个 checkpoint 的 id
    checkpoint JSONB,                          -- channel state 快照
    metadata JSONB                             -- step, source, parents
)

-- 单次 tool call 的写操作（step 内实时落盘）
checkpoint_writes (
    thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
    channel, type, blob
)

-- channel 值的 blob 存储
checkpoint_blobs (
    thread_id, checkpoint_ns, channel, version, type, blob
)
```

`parent_checkpoint_id` 在同一 `(thread_id, checkpoint_ns)` 内形成**线性链表**，代表时间线历史，不是跨 subagent 的父子树。

### 3.2 LangGraph Subgraph 的真正树结构

LangGraph 原生 subgraph 通过 `checkpoint_ns` 建立树形关系：

```
thread_id=T, checkpoint_ns=""           → 主图检查点链
thread_id=T, checkpoint_ns="tools:abc"  → 子图1检查点链
thread_id=T, checkpoint_ns="tools:def"  → 子图2检查点链
```

`metadata.parents` 记录各 namespace 的"来源 checkpoint"：
```json
{"parents": {"": "parent_checkpoint_id_in_root_ns"}}
```

**但 deerflow 的 subagent 不使用此机制**（详见第 3.4 节）。

### 3.3 Checkpoint 触发时机

Checkpointer **不在 node 内部执行**，由 Pregel loop 在以下时机自动触发：

```
astream() 开始
  └── __enter__: aget_tuple() 加载该 thread_id 最新 checkpoint

每一个 step 的生命周期:

  [1] tick()
      ├── prepare_next_tasks() → 从当前 checkpoint 决定下一批 nodes
      ├── interrupt_before 检查 → 命中时 raise GraphInterrupt
      └── 返回 True（开始执行 tasks）

  [2] tasks 并发执行（agent node / tools node）
      └── 每个 task 有写操作时:
          └── put_writes(task_id, writes)
              └── checkpointer.put_writes() ← 立刻写 checkpoint_writes 表
                  （不等 step 结束，crash 可恢复单次 tool call 结果）

  [3] after_tick()    ← 所有 tasks 完成后
      ├── apply_writes() → 合并 writes 到 channel state
      ├── _put_checkpoint({"source": "loop"}) ← 完整 step checkpoint
      └── interrupt_after 检查 → 命中时 raise GraphInterrupt

  [4] 若出现 GraphInterrupt（interrupt() 或 interrupt_after）:
      └── _suppress_interrupt() 捕获
          ├── _put_checkpoint(self.checkpoint_metadata)
          │   exiting=True → id = self.checkpoint["id"]（复用同一 id，UPSERT）
          └── _put_pending_writes() → 确保 interrupt write 落盘
```

### 3.4 每个 step 对应哪些 checkpoint

标准 react agent 中，每个 step 只有一种 node，所以：

| Step | 执行内容 | 保存时机 | Checkpoint 包含 |
|---|---|---|---|
| 第 1 个 step | `agent` node（LLM 调用） | `after_tick()` | messages + 新 AIMessage(tool_calls) |
| 第 2 个 step | `tools` node（工具执行） | `after_tick()` | messages + 所有 ToolMessages |
| 遇到 `interrupt()` | `agent` node 中途 | `_suppress_interrupt()` | interrupt 之前的 messages，INTERRUPT pending write |

**工具调用结果的实时落盘**：tools node 执行时，每个工具调用完成后立即通过 `put_writes` 写入 `checkpoint_writes` 表。若进程崩溃，已完成的工具结果在 DB 中有记录，下次 resume 可跳过已完成的工具（取决于 LangGraph 版本对 pending_writes 的处理）。

### 3.5 interrupt() 时的 checkpoint_id

`interrupt()` 发生在 `after_model` hook 内（agent node 执行期间）。此时的 `runtime.config["configurable"]["checkpoint_id"]` 即为本次 step 对应的 checkpoint_id。`_suppress_interrupt` 保存 interrupt checkpoint 时复用这个 id（`exiting=True`），因此：

> **`runtime.config["configurable"]["checkpoint_id"]` == interrupt checkpoint 的 checkpoint_id**

这意味着 `HumanApprovalMiddleware` 可以在 `tool_approval_required` SSE 事件里携带此 checkpoint_id，供客户端参考：

```python
def after_model(self, state, runtime):
    config = getattr(runtime, 'config', None) or {}
    checkpoint_id = config.get('configurable', {}).get('checkpoint_id')
    writer({
        "type": "tool_approval_required",
        "tool_calls": pending_payload,
        "checkpoint_id": checkpoint_id,   # 可选，供客户端调试/多断点管理
    })
    interrupt({..., "checkpoint_id": checkpoint_id})
```

---

## 4. Subagent Checkpoint：当前状态与改进方案

### 4.1 当前实现：无 Checkpoint

当前 `_aexecute` 的 `run_config` 不包含 checkpointer：

```python
run_config = {
    "recursion_limit": self.config.max_turns,
    "callbacks": [collector],          # 只有 token 收集
    "configurable": {"thread_id": self.thread_id},
}
# agent.checkpointer 未设置 → subagent 执行零写入 checkpoints 表
```

**中断时的状态**：

| 中断场景 | Subagent 状态 | Parent 状态 |
|---|---|---|
| 进程崩溃 | `_background_tasks` 内存丢失 | Checkpoint 保留 AIMessage(task tool_calls) |
| 用户取消 | `cancel_event` 触发，协作停止 | `astream()` 退出，状态为 interrupted |
| Subagent 超时 | `TIMED_OUT`，ToolMessage 携带错误 | 下次发消息可重试 |

Parent 从 checkpoint 恢复后，`task_tool` 被重新调用（同一 `tool_call_id`），subagent **从头重跑**。

### 4.2 为什么不使用 LangGraph 原生 Subgraph

| 问题 | 说明 |
|---|---|
| **嵌套 event loop 冲突** | `task_tool` 运行在 parent 的 tools node 内（L1 loop），`agent.astream()` 需要自己的 loop，无法在同一 loop 里嵌套运行另一个 Pregel loop |
| **动态 subagent 类型** | LangGraph subgraph 必须在 `graph.compile()` 时静态定义；deerflow 的 subagent 类型从 `config.yaml` 运行时加载，LLM 动态选择 |
| **Checkpointer 共享复杂度** | 原生 subgraph 共享 parent checkpointer，产生 `checkpoint_ns` 命名、多 subgraph 并发写竞争等问题 |

**Claude Code 的参考架构**（与 deerflow 的共同点）：Claude Code 也采用"tool call 触发独立 Agent 执行，结果 string 回传 parent"的模式，底层是直接 Anthropic API，不使用 LangGraph，同样不做 subagent checkpoint。

### 4.3 改进方案：Subagent 独立线程（thread_id = task_id）

**核心思路**：把 subagent task 当作一个独立的"对话线程"来处理——拥有自己的 `thread_id`（= `task_id` = `tool_call_id`），与主 agent 无父子关系，使用同一个 Postgres 数据库。

**Postgres MVCC 的天然支持**：

不同 `thread_id` 的 checkpoint 写入是完全独立的行。3 个并行 subagent 各自 `INSERT` 到不同 `thread_id`，无行级锁竞争：

```sql
-- subagent_1: thread_id="call_abc123"
INSERT INTO checkpoints (thread_id="call_abc123", checkpoint_id=..., ...)

-- subagent_2: thread_id="call_def456"
INSERT INTO checkpoints (thread_id="call_def456", checkpoint_id=..., ...)

-- 零冲突，MVCC 天然处理
```

**实现要点**：

AsyncPregelLoop 要求 async checkpointer（调用 `await checkpointer.aget_tuple(...)`），sync `PostgresSaver` 的 `aget_tuple` 未实现。因此需在 `_aexecute` 内（即 `_isolated_subagent_loop` 上）创建独立的 `AsyncPostgresSaver` 连接：

```python
# executor.py _aexecute
async def _aexecute(self, task, result_holder):
    subagent_thread_id = self.task_id   # = tool_call_id，全局唯一

    if self.checkpointer_conn_string:
        async with AsyncPostgresSaver.from_conn_string(
            self.checkpointer_conn_string
        ) as checkpointer:
            await checkpointer.setup()   # CREATE TABLE IF NOT EXISTS，幂等
            state, filtered_tools = await self._build_initial_state(task)
            agent = self._create_agent(filtered_tools)
            agent.checkpointer = checkpointer

            # 检查是否已有 checkpoint（parent 从断点恢复时重跑 task_tool）
            existing = await checkpointer.aget_tuple(
                {"configurable": {"thread_id": subagent_thread_id, "checkpoint_ns": ""}}
            )
            input_state = None if existing else state
            # existing=None → 新任务; existing 有值 → 从上次断点续跑

            run_config["configurable"] = {"thread_id": subagent_thread_id}
            async for chunk in agent.astream(input_state, config=run_config, ...):
                ...
    else:
        # 无 checkpointing，保持现有行为
        ...
```

**task_tool 侧的完成态检测**（防止重复执行已完成的任务）：

```python
# task_tool.py：execute_async 之前检查
if checkpointer_conn_string:
    result = await _check_subagent_completed(task_id, checkpointer_conn_string)
    if result:
        writer({"type": "task_completed", "task_id": task_id, "result": result})
        return f"Task Succeeded. Result: {result}"
# 否则正常启动 subagent
```

`_check_subagent_completed`：加载 subagent 的最新 checkpoint，若最后一条消息是无 `tool_calls` 的 AIMessage，则认为正常完成。

### 4.4 最小改动清单（改进方案）

| 文件 | 改动 |
|---|---|
| `subagents/executor.py` | `SubagentExecutor.__init__` 新增 `checkpointer_conn_string: str \| None`；`_aexecute` 内创建 `AsyncPostgresSaver`，设置 `thread_id=task_id` |
| `tools/builtins/task_tool.py` | 从 `app_config` 读取 conn_string 传给 executor；添加完成态检测逻辑 |
| `config/checkpointer_config.py` | 暴露 conn_string 获取函数（复用现有 checkpointer 后端配置） |

### 4.5 恢复语义

```
Parent checkpoint (thread_id=T)
  └── AIMessage { tool_calls: [task(scene_1, task_id=X)] }   ← 已 checkpoint

Subagent checkpoint (thread_id=X，独立)
  ├── checkpoint_1: SystemMessage + HumanMessage(scene_1 prompt)
  ├── checkpoint_2: AIMessage(cfgpu tool_call)           ← crash 发生
  └── (未写) checkpoint_3: ToolMessage(cfgpu 结果)

Parent 恢复后重跑 task_tool(tool_call_id=X):
  → 检查 thread_id=X: 存在 checkpoint 但未完成
  → SubagentExecutor._aexecute(input=None, thread_id=X)
  → LangGraph 从 checkpoint_2 续跑
  → 重新发出 cfgpu tool call
  → cfgpu 侧幂等（检查 job_id 文件）
  → 完成 → ToolMessage 回传 parent
```

---

## 5. 多场景长任务的恢复建议

对于生图生视频的多场景长任务，要实现完整恢复需要三层协同：

| 层 | 保证 | 实现方式 |
|---|---|---|
| **Parent 层** | 哪些场景已完成 | 使用持久 checkpointer（SQLite/Postgres）；场景任务**串行**而非并行（每个 ToolMessage 完成后立即 checkpoint） |
| **Subagent 层** | 场景内部进度 | 方案 4.3：独立 `thread_id` checkpoint；中断后从最后节点边界续跑 |
| **cfgpu 层** | MCP 工具调用幂等 | Subagent 在调用前检查 workspace 中的 `scene_{id}.job_id` 文件；有则续 poll，无则新提交 |

串行调度的 checkpoint 效果：

```
parent thread (thread_id=T):
  [checkpoint] AIMessage + ToolMessage(scene_1 → done)  ← scene_1 完成
  [checkpoint] AIMessage + ToolMessage(scene_2 → done)  ← scene_2 完成
  [AIMessage]  task(scene_3) ← checkpoint: 下次从这里恢复，只重跑 scene_3
```

---

## 6. 关键文件索引

| 文件 | 说明 |
|---|---|
| `tools/builtins/task_tool.py` | task tool 实现，poll 循环，SSE 事件发送 |
| `subagents/executor.py` | `SubagentExecutor`，`_aexecute`，isolated loop 管理 |
| `subagents/config.py` | `SubagentConfig`（name, system_prompt, tools, timeout, max_turns） |
| `subagents/registry.py` | subagent 类型注册表（built-in + config 自定义） |
| `subagents/builtins/general_purpose.py` | general-purpose subagent 默认配置 |
| `agents/middlewares/subagent_limit_middleware.py` | 并发限制 middleware |
| `runtime/checkpointer/provider.py` | sync checkpointer 工厂（InMemory / SQLite / Postgres） |
| `runtime/checkpointer/async_provider.py` | async checkpointer 工厂 |
| `runtime/runs/worker.py` | `run_agent()`，parent agent 的 checkpoint 集成 |
