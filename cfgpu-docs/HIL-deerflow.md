# HIL（Human-in-the-Loop）工具确认机制 — deerflow 设计

版本：2.0 | 适用分支：director-agent

---

## 1. 背景与目标

director agent 在生图、生视频流水线中会调用高花费工具（`cfgpu__generate_image`、`cfgpu__generate_video`）。这类工具执行前需要：

1. **展示参数**给用户确认（prompt、模型、分辨率等）
2. **允许用户修改参数**（如改写 prompt、切换模型）
3. **精确执行**：以用户最终确认的参数调用工具，不重跑 LLM

---

## 2. 机制选型

### 2.1 选用 LangGraph 原生 `interrupt()` + State 驱动 Resume

| 方案 | 问题 |
|------|------|
| ClarificationMiddleware 式（`Command(goto=END)`） | resume 后 LLM 需重新理解对话并重新发起工具调用，参数精度无法保证 |
| 状态存储 + 二次 run | 需要识别"确认消息"，逻辑复杂，难以透传修改后的参数 |
| **LangGraph `interrupt()` + `after_model`** | 断点精确：工具执行前整批暂停；state 驱动 resume 无重复 SSE；支持批量审批 |

### 2.2 拦截位置：after_model，而非 wrap_tool_call

LangGraph 的 `ToolNode` 默认用 `asyncio.gather` **并行**执行同一个 `AIMessage` 里的所有工具调用。若在 `wrap_tool_call` 里逐个 `interrupt()`，会产生并发竞争：多个 `interrupt()` 同时抢占 checkpoint，客户端收到多个 SSE 审批事件，resume 对应关系不清晰。

`after_model` 在 LLM 生成完 `AIMessage`、`ToolNode` 执行**之前**触发，此时可以一次性拿到当轮所有工具调用，用单次 `interrupt()` 整批处理。

### 2.3 State 驱动 Resume，避免重复 SSE

LangGraph `interrupt()` 机制决定了 `after_model` 在 resume 时会**从头重新执行**。若不加保护，SSE 事件会在 resume 时重复发出。

解决方案：在 `ThreadState` 增加 `tool_approvals` 字段，客户端 resume 时通过 `Command.update` 把决策写入 state。`after_model` 重入时先检查 state，若所有待审批工具都有决策，直接应用，完全跳过 SSE 和 `interrupt()`。

```
第一次执行：state.tool_approvals 无决策 → 发 SSE → interrupt() 抛出 → graph 暂停
                       ↓
客户端写入：Command(update={tool_approvals: {tc1: approved, tc2: rejected}})
                       ↓
resume 重入：state.tool_approvals 已有全部决策 → 直接应用，无 SSE，无 interrupt()
```

### 2.4 关键保证

- `after_model` 在 `AgentMiddleware` 基类中是正式 hook（LangGraph/LangChain 原生支持）
- `interrupt()` 在 `after_model` 中调用抛出 `GraphInterrupt`，由 LangGraph 运行器捕获并 checkpoint，不会干扰其他 middleware
- `astream()` 在 interrupt 后正常退出（不抛异常），需通过 `aget_state()` 检测 `tasks[].interrupts` 判断是否暂停
- `ThreadState.tool_approvals` 使用 merge reducer，`Command.update` 写入时合并而不覆盖历史数据

---

## 3. 架构概览

```
LLM 调用 → AIMessage(tool_calls=[gen_img, gen_vid, search])
                          │
                          ▼  AgentMiddleware.after_model
              HumanApprovalMiddleware
                          │
              ┌───────────┴────────────┐
              │  检查 state.tool_approvals
              │
              ├─ 有全部决策（resume path）
              │   └─ _build_response() → 应用决策，无 SSE，无 interrupt()
              │       ├─ approved：保留 tool_call，替换 args
              │       └─ rejected：移除 tool_call，注入 error ToolMessage
              │
              └─ 无决策（first call）
                  ├─① get_stream_writer().write({type:"tool_approval_required", tool_calls:[...]})
                  │    ↳ HTTP: 批量 SSE 事件    MQ: custom 消息
                  │
                  └─② interrupt({tool_calls:[gen_img, gen_vid]})
                       ↳ 图暂停，checkpoint 保存，astream() 结束
                       ↳ 等待 Command(update={tool_approvals:{...}})
                          │
                          ▼  (resume 后，after_model 重入，走上面的"有全部决策"分支)
              修改后的 AIMessage(tool_calls=[gen_img_with_new_args])
                          │
                          ▼
                       ToolNode
                (只执行 approved 的 tool_calls)
```

---

## 4. 组件设计

### 4.1 `HumanApprovalMiddleware`

**位置**：`backend/packages/harness/deerflow/agents/middlewares/human_approval_middleware.py`

**在 middleware 链中的位置**（`agent.py` `_build_middlewares()`）：

```
... (其他 middleware)
HumanApprovalMiddleware   ← 倒数第二，在 ClarificationMiddleware 之前
ClarificationMiddleware   ← 必须 last
```

**核心逻辑**：

```python
def after_model(self, state: AgentState, runtime: Any) -> dict | None:
    # 找到最新 AIMessage
    last_msg = next((m for m in reversed(state["messages"]) if isinstance(m, AIMessage)), None)
    pending = [tc for tc in last_msg.tool_calls if self._needs_approval(tc["name"])]
    if not pending:
        return None

    # ── Resume path：state 中已有全部决策 ──
    tool_approvals = state.get("tool_approvals") or {}
    pending_ids = {tc["id"] for tc in pending}
    if pending_ids <= tool_approvals.keys():          # 全部决策已在 state
        return self._build_response(last_msg, tool_approvals)

    # ── First call：发批量 SSE → 单次 interrupt() ──
    pending_payload = [{"id": tc["id"], "name": tc["name"], "args": tc["args"]}
                       for tc in pending]
    writer = get_stream_writer()
    writer({"type": "tool_approval_required", "tool_calls": pending_payload})

    # 单次 interrupt，整批工具调用，graph 在此处 checkpoint 并暂停
    # resume 时若 state 已有决策不会到达这里；
    # fallback：客户端只发 Command(resume=...) 时，interrupt() 返回 resume 值
    fallback = interrupt({"type": "tool_approval_required", "tool_calls": pending_payload})
    return self._build_response(last_msg, {**tool_approvals, **_parse_fallback(fallback)})
```

**`_build_response` 决策应用规则**：

| 工具类型 | 决策 | 处理方式 |
|---|---|---|
| 不匹配 pattern | — | 原样保留，ToolNode 正常执行 |
| 匹配 pattern | `approved` | 以用户确认的 args 保留在 tool_calls |
| 匹配 pattern | `rejected` | 从 tool_calls 移除，注入 error ToolMessage |

**工具名匹配**：支持 `fnmatch` 模式（`"cfgpu__generate_*"`、`"*generate*"` 等）。

### 4.2 `ThreadState.tool_approvals`

**位置**：`backend/packages/harness/deerflow/agents/thread_state.py`

```python
tool_approvals: Annotated[dict[str, Any], merge_tool_approvals]
# tool_call_id → {status: "approved"|"rejected", args?: {...}, reason?: "..."}
```

Reducer `merge_tool_approvals`：合并策略，新值覆盖相同 key 的旧值，不清空历史。

```python
def merge_tool_approvals(existing, new):
    if existing is None: return new or {}
    if new is None: return existing
    return {**existing, **new}
```

### 4.3 `AgentConfig` 扩展

**位置**：`backend/packages/harness/deerflow/config/agents_config.py`

```python
class AgentConfig(BaseModel):
    name: str
    description: str = ""
    model: str | None = None
    tool_groups: list[str] | None = None
    skills: list[str] | None = None
    approval_required_tools: list[str] | None = None  # ← 新增
```

director agent 的 `config.yaml`：

```yaml
name: director
approval_required_tools:
  - "cfgpu__generate_image"
  - "cfgpu__generate_video"
```

### 4.4 `_build_middlewares()` 注入

**位置**：`backend/packages/harness/deerflow/agents/lead_agent/agent.py`

`HumanApprovalMiddleware` 的注入需要**两个条件同时满足**：

```python
# 在注入 ClarificationMiddleware 之前
ask = cfg.get("ask", False)   # 运行时开关，来自 task config
if ask and agent_config and agent_config.approval_required_tools:
    from deerflow.agents.middlewares.human_approval_middleware import HumanApprovalMiddleware
    middlewares.append(HumanApprovalMiddleware(set(agent_config.approval_required_tools)))

middlewares.append(ClarificationMiddleware())   # 必须 last
```

| 条件 | 来源 | 说明 |
|------|------|------|
| `config.ask=true` | 运行时（task payload） | 客户端按任务粒度控制是否开启 HIL；`false` 时不注入 middleware，高花费工具直接执行 |
| `approval_required_tools` 非空 | 静态（agent config.yaml） | 定义哪些工具名需要审批（支持 fnmatch 模式） |

两者分工：**静态配置决定能力边界，运行时开关决定是否启用**。

### 4.5 `run_agent()` 接受 `command`

**位置**：`backend/packages/harness/deerflow/runtime/runs/worker.py`

新增 `command: dict | None = None` 参数：

```python
async def run_agent(..., command: dict | None = None, ...):
    if command:
        from langgraph.types import Command as LGCommand
        stream_input = LGCommand(**command)
    else:
        stream_input = graph_input

    async for chunk in agent.astream(stream_input, config=runnable_config, ...):
        ...
```

`astream()` 结束后通过 `aget_state()` 检测是否暂停：

```python
is_paused = False
if checkpointer is not None:
    final_state = await agent.aget_state(runnable_config)
    if final_state and any(t.interrupts for t in (final_state.tasks or [])):
        is_paused = True

if is_paused:
    await run_manager.set_status(run_id, RunStatus.interrupted)
    await bridge.publish(run_id, "custom", {"type": "run_paused", "reason": "tool_approval_required"})
else:
    await run_manager.set_status(run_id, RunStatus.success)
```

### 4.6 `start_run()` 透传 `command`

**位置**：`backend/app/gateway/services.py`

`RunCreateRequest.command` 字段已存在，接线到 `run_agent()`：

```python
task = asyncio.create_task(
    run_agent(
        bridge, run_mgr, record,
        ctx=run_ctx,
        agent_factory=agent_factory,
        graph_input=graph_input,
        command=body.command,        # ← 透传
        config=config,
        stream_modes=stream_modes,
        ...
    )
)
```

---

## 5. 客户端协议（HTTP/SSE）

### 5.1 订阅 `custom` 事件

客户端发起 run 时需在 `stream_mode` 中包含 `"custom"`：

```json
{
  "stream_mode": ["values", "messages-tuple", "custom"],
  "input": { "messages": [{"role": "user", "content": "..."}] }
}
```

### 5.2 识别审批请求

SSE 流中收到 `event: custom`，`data.type == "tool_approval_required"`。

同一个 AI Message 产生的所有待审批工具调用**合并在一个事件**里：

```
event: custom
data: {
  "type": "tool_approval_required",
  "tool_calls": [
    {
      "id": "call_abc123",
      "name": "cfgpu__generate_image",
      "args": {
        "prompt": "英雄归途，晨雾中的山谷",
        "model": "flux-pro",
        "width": 1024,
        "height": 1024
      }
    },
    {
      "id": "call_def456",
      "name": "cfgpu__generate_video",
      "args": {
        "prompt": "英雄归途，晨雾中的山谷，电影感",
        "model": "wan-2.0",
        "aspect_ratio": "16:9",
        "duration_seconds": 5
      }
    }
  ]
}
```

紧接着收到 `event: end` 表示图已暂停。

### 5.3 发送审批决策（Resume）

用户审核（可修改参数）后，通过 `Command.update` 将决策写入 state，发起新 run：

```
POST /api/threads/{thread_id}/runs/stream

{
  "stream_mode": ["values", "messages-tuple", "custom"],
  "command": {
    "update": {
      "tool_approvals": {
        "call_abc123": {
          "status": "approved",
          "args": {
            "prompt": "英雄，暮色中独行，落日余晖，油画风格",
            "model": "flux-pro",
            "width": 1024,
            "height": 1024
          }
        },
        "call_def456": {
          "status": "rejected",
          "reason": "视频暂不生成，只需要图片"
        }
      }
    }
  }
}
```

**全部通过（参数不修改）**：

```json
{
  "command": {
    "update": {
      "tool_approvals": {
        "call_abc123": {"status": "approved"},
        "call_def456": {"status": "approved"}
      }
    }
  }
}
```

**全部拒绝**：

```json
{
  "command": {
    "update": {
      "tool_approvals": {
        "call_abc123": {"status": "rejected", "reason": "暂不生成"},
        "call_def456": {"status": "rejected", "reason": "暂不生成"}
      }
    }
  }
}
```

> **注意**：resume 时不需要 `input` 字段，`command` 和 `input` 互斥。`args` 字段缺省时沿用 LLM 原始参数。

---

## 6. 完整交互序列（HTTP/SSE）

```
POST /runs/stream {input: {messages: ["帮我生成主角第三集的场景图和视频"]}}

SSE stream:
  event: metadata   data: {run_id, thread_id}
  event: values     data: {...}
  event: messages   data: [AIMessageChunk...]     ← LLM 决定同时生图+生视频
  event: custom     data: {
                      type: "tool_approval_required",
                      tool_calls: [
                        {id:"tc1", name:"cfgpu__generate_image", args:{prompt:"...", model:"flux-pro"}},
                        {id:"tc2", name:"cfgpu__generate_video", args:{prompt:"...", model:"wan-2.0"}}
                      ]
                    }                              ← 单个事件，两个工具一起审批
  event: end        data: null                    ← 图暂停，stream 结束

── 客户端展示参数编辑界面（两个工具卡片） ──
── 用户修改图片 prompt，拒绝视频 ──

POST /runs/stream {
  command: {
    update: {
      tool_approvals: {
        "tc1": {status:"approved", args:{prompt:"英雄，暮色中独行", model:"flux-pro"}},
        "tc2": {status:"rejected", reason:"视频暂不需要"}
      }
    }
  }
}

SSE stream:
  event: metadata   data: {run_id, thread_id}
  event: messages   data: [ToolMessage{tc2, error}]  ← 视频拒绝消息（LLM 可感知）
  event: messages   data: [ToolMessage{tc1, urls:[...]}]  ← 图片生成结果
  event: messages   data: [AIMessageChunk...]        ← LLM 对结果的总结
  event: values     data: {..., messages:[...全部]}
  event: end        data: null                    ← 正常完成
```

---

## 7. command 消息缺失 `config.ask=true` 的防护

### 7.1 问题

`HumanApprovalMiddleware` 只在 `config.ask=true` 时注入。若 resume（command）消息未携带该字段，middleware 不会出现在 graph 中，`after_model` 不存在，`state.tool_approvals` 中的用户决策将被静默忽略——所有工具（含被拒绝的）直接进 ToolNode 执行。

### 7.2 防护策略：Auto-correct + Warning

在 `setup_agent()` / `make_lead_agent()` 调用**之前**检测并修正，同时向客户端发 warning 事件：

```python
if message.command and not config.get("ask"):
    logger.warning("HIL resume command without config.ask=True; auto-injecting (thread=%s)", thread_id)
    await bridge.publish_progress(run_id, "custom", {
        "type":    "warning",
        "code":    "HIL_ASK_REQUIRED",
        "message": "HIL resume 消息必须设置 config.ask=true；本次已自动修正，请检查客户端实现",
    }, seq)
    seq += 1
    config["ask"] = True   # 修正后再构建 graph，确保 middleware 被注入
```

### 7.3 各角色感知

| 角色 | 感知 |
|------|------|
| 客户端开发者 | stream 中收到 `custom {type:"warning", code:"HIL_ASK_REQUIRED"}` 事件，明确知道漏了 `ask=true` |
| 用户 | resume 照常完成，工具决策正常应用，无感 |
| LangGraph | 收到完整 `Command(update={tool_approvals:{...}})`，`HumanApprovalMiddleware` 正常执行 resume path |

Warning 事件在其他 progress 事件之前、result 之前推送。适用于 HTTP/SSE 路径（`run_agent()`）和 MQ Consumer 路径（`AgentRunner.run()`）。

---

## 8. 文件变更清单


| 文件 | 操作 | 说明 |
|------|------|------|
| `middlewares/human_approval_middleware.py` | 新建 | 核心 middleware，使用 `after_model` hook |
| `agents/thread_state.py` | 修改 | `ThreadState` 加 `tool_approvals` 字段及 reducer |
| `config/agents_config.py` | 修改 | `AgentConfig` 加 `approval_required_tools` |
| `agents/lead_agent/agent.py` | 修改 | `_build_middlewares()` 注入 middleware |
| `runtime/runs/worker.py` | 修改 | `run_agent()` 接受 `command`，检测暂停状态 |
| `app/gateway/services.py` | 修改 | `start_run()` 透传 `body.command` |
| `tests/test_human_approval_middleware.py` | 新建 | 单元测试（17 个用例） |

---

## 9. 与旧设计的差异（v1.0 → v2.0）

| 维度 | v1.0（wrap_tool_call） | v2.0（after_model） |
|---|---|---|
| 拦截时机 | 每个工具调用单独拦截 | AI Message 生成后整批拦截 |
| interrupt 次数 | N 个审批工具 = N 次（并发竞争） | 1 次，整批 |
| SSE 事件数 | N 个事件（每工具一个） | 1 个批量事件 |
| resume SSE 重复 | 必然重复 | 无重复（state 检查先于 interrupt） |
| 参数格式 | `{approved, args}` 单工具 | `{tool_approvals: {id: decision}}` 批量 |
| state 依赖 | 无 | `ThreadState.tool_approvals` |

---

## 10. 待确认事项

- [ ] 前端 `useStream` hook 是否自动接收 `custom` 事件，还是需要额外订阅
- [ ] 审批超时处理：用户长时间未确认，checkpoint 永久保留还是设置 TTL 清理
- [ ] 全部拒绝时 LLM 重新规划的行为是否符合预期（LLM 会收到全部 error ToolMessage）
