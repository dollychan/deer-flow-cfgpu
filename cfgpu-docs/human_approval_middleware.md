# HumanApprovalMiddleware 设计文档

## 一、作用

`HumanApprovalMiddleware` 是一个 deerflow Agent 中间件，用于在 Agent 执行高风险或高花费工具前，将工具调用参数推送给客户端进行人工审核，并在用户确认（或修改参数）后，以断点恢复的方式精确继续执行。

典型场景：Director Agent 生成的图像/视频任务在调用 `cfgpu__generate_image`、`cfgpu__generate_video` 等工具前，先将 prompt、分辨率等参数返回给前端，等用户确认或调整后再实际调用。

---

## 二、设计原理

### 2.1 为什么不在 wrap_tool_call 拦截

最直观的 HIL 方案是在 `wrap_tool_call` 里逐个拦截工具调用并 `interrupt()`。但这有一个根本缺陷：

LangGraph 的 `ToolNode` 默认用 `asyncio.gather` **并行**执行同一个 `AIMessage` 里的所有工具调用。如果 AI 同时发出两个生图调用，两个 `wrap_tool_call` 会并发触发，导致：
- 两个 `interrupt()` 同时竞争 checkpoint
- 客户端收到两个 SSE 审批事件，逻辑复杂
- `Command(resume=...)` 的对应关系不清晰

### 2.2 在 after_model 拦截

`after_model` 在 LLM 生成完 `AIMessage`（含 `tool_calls`）之后、`ToolNode` 执行之前触发，此时可以一次性拿到当轮所有工具调用。

```
LLM 调用
    ↓
AIMessage(tool_calls=[gen_img, gen_vid, search])
    ↓
after_model 触发（整批处理）
    ↓
路由 → ToolNode（只执行通过审批的工具）
```

在 `after_model` 里：
1. 扫描 `AIMessage.tool_calls`，筛出匹配配置 pattern 的工具
2. 整批用一个 SSE 事件推送给客户端
3. 调用**一次** `interrupt()`，整个 graph checkpoint 并暂停
4. Resume 时修改 `AIMessage.tool_calls`（应用用户确认的参数，移除被拒绝的调用）
5. `ToolNode` 只执行修改后的 tool_calls

### 2.3 State 驱动的 Resume，避免重复 SSE

LangGraph `interrupt()` 的机制决定了 `after_model` 在 resume 时会**从头重新执行**一遍。如果不加保护，SSE 事件会重复发出。

解决方案：在 `ThreadState` 增加 `tool_approvals` 字段，客户端 resume 时通过 `Command.update` 把决策写入 state。`after_model` 重入时先检查 state，若所有待审批工具都有决策记录，直接应用，跳过 SSE 和 `interrupt()`。

```
第一次执行：state.tool_approvals 为空 → 发 SSE → interrupt() 抛出 → graph 暂停
               ↓
客户端写入：Command(update={tool_approvals: {tc1: approved, tc2: rejected}})
               ↓
第二次执行（resume）：state.tool_approvals 已有所有决策 → 直接应用，无 SSE，无 interrupt()
```

---

## 三、涉及组件

### 3.1 HumanApprovalMiddleware

**文件**：`packages/harness/deerflow/agents/middlewares/human_approval_middleware.py`

**构造参数**：
```python
HumanApprovalMiddleware(tool_patterns: set[str])
```

`tool_patterns` 是 fnmatch 格式的工具名匹配模式集合，例如：
- `"cfgpu__generate_image"` — 精确匹配
- `"cfgpu__generate_*"` — MCP server 前缀 glob
- `"*generate*"` — 子串 glob

**核心方法**：

| 方法 | 说明 |
|---|---|
| `after_model(state, runtime)` | 同步版本，核心逻辑所在 |
| `aafter_model(state, runtime)` | 异步版本，委托给同步版本 |
| `_needs_approval(tool_name)` | 判断工具是否需要审批 |
| `_pending_tool_calls(ai_msg)` | 从 AIMessage 中提取待审批工具列表 |
| `_build_response(ai_msg, tool_approvals)` | 根据决策修改 AIMessage，生成 state 更新 |

**`after_model` 执行逻辑**：

```
找出 AIMessage 中待审批的 tool_calls
    ↓
检查 state.tool_approvals 是否已有所有决策
    ├── 全部有决策（resume path）→ _build_response() 应用决策，返回
    └── 有缺失（first call）→ 发 SSE → interrupt()
                                    ├── 抛出（正常 first call）→ graph 暂停
                                    └── 返回（fallback resume，无 state update）→ _build_response()
```

**`_build_response` 决策应用规则**：

| 工具类型 | 决策 | 处理方式 |
|---|---|---|
| 非审批工具（不匹配 pattern） | — | 原样保留在 tool_calls |
| 审批工具 | `approved` | 以用户确认的 args 保留在 tool_calls，由 ToolNode 执行 |
| 审批工具 | `rejected` | 从 tool_calls 移除，注入 error ToolMessage（LLM 可感知拒绝原因） |

### 3.2 ThreadState.tool_approvals

**文件**：`packages/harness/deerflow/agents/thread_state.py`

```python
tool_approvals: Annotated[dict[str, Any], merge_tool_approvals]
```

字段结构：
```
{
  "<tool_call_id>": {
    "status": "approved" | "rejected",
    "args": {...},      # 仅 approved 时有效，用户确认（可能修改过）的参数
    "reason": "..."     # 仅 rejected 时可选，给 LLM 的拒绝原因
  }
}
```

Reducer `merge_tool_approvals`：合并策略，新值覆盖相同 key 的旧值，不清空已有数据。

### 3.3 AgentConfig.approval_required_tools

**文件**：`packages/harness/deerflow/config/agents_config.py`

```python
class AgentConfig(BaseModel):
    approval_required_tools: list[str] | None = None
```

在 Agent 的 `config.yaml` 中配置：
```yaml
name: director
approval_required_tools:
  - "cfgpu__generate_image"
  - "cfgpu__generate_video"
```

`_build_middlewares()` 在 lead_agent 初始化时读取此配置并注入 `HumanApprovalMiddleware`。注入条件：**静态配置**（`approval_required_tools` 非空）与**运行时开关**（`config.ask=true`）同时满足：

```python
ask = cfg.get("ask", False)
if ask and agent_config and agent_config.approval_required_tools:
    middlewares.append(HumanApprovalMiddleware(set(agent_config.approval_required_tools)))
middlewares.append(ClarificationMiddleware())  # 始终最后
```

两个条件的分工：
- `approval_required_tools`：**静态**，定义哪些工具需要审批（Agent 配置层面）
- `config.ask`：**运行时**，客户端按任务按需开启 HIL（MQ task payload `config.ask: true`）

---

## 四、完整交互时序

```
Client                          Gateway / LangGraph               HumanApprovalMiddleware
  │                                     │                                    │
  │  POST /threads/{id}/runs/stream     │                                    │
  │  {stream_mode: ["values","custom"]} │                                    │
  │────────────────────────────────────►│                                    │
  │                                     │  LLM 生成 AIMessage                │
  │                                     │  tool_calls: [gen_img, gen_vid]    │
  │                                     │────────────────────────────────────►
  │                                     │                                    │  after_model 触发
  │                                     │                                    │  tool_approvals = {} (空)
  │                                     │                                    │  pending = [gen_img, gen_vid]
  │◄────────────────────────────────────│◄───────────────────────────────────│
  │  event: custom                      │  get_stream_writer().write(...)     │
  │  {                                  │                                    │
  │    type: "tool_approval_required",  │                                    │
  │    tool_calls: [                    │                                    │
  │      {id:"tc1", name:"cfgpu__generate_image", args:{prompt:"...",width:1024}},
  │      {id:"tc2", name:"cfgpu__generate_video", args:{prompt:"...",duration:5}}
  │    ]                                │                                    │
  │  }                                  │                                    │
  │                                     │                                    │  interrupt() 触发
  │  event: end                         │  graph checkpoint，状态保存         │  graph 暂停
  │◄────────────────────────────────────│                                    │
  │                                     │                                    │
  │  用户在 UI 审核参数，修改 prompt      │                                    │
  │                                     │                                    │
  │  POST /threads/{id}/runs/stream     │                                    │
  │  {                                  │                                    │
  │    command: {                       │                                    │
  │      update: {                      │                                    │
  │        tool_approvals: {            │                                    │
  │          "tc1": {status:"approved", args:{prompt:"修改后的文案",width:1024}},
  │          "tc2": {status:"rejected", reason:"视频暂不生成"}
  │        }                            │                                    │
  │      }                              │                                    │
  │    }                                │                                    │
  │  }                                  │                                    │
  │────────────────────────────────────►│                                    │
  │                                     │  LangGraph 先应用 state update      │
  │                                     │  state.tool_approvals = {tc1,tc2}  │
  │                                     │────────────────────────────────────►
  │                                     │                                    │  after_model 重入
  │                                     │                                    │  tool_approvals 已有 tc1,tc2 全部决策
  │                                     │                                    │  跳过 SSE 和 interrupt()
  │                                     │                                    │  _build_response():
  │                                     │                                    │    tc1: 保留，args 替换为修改后的文案
  │                                     │                                    │    tc2: 移除，注入 ToolMessage(error)
  │                                     │◄───────────────────────────────────│
  │                                     │  ToolNode 执行：                   │
  │                                     │    cfgpu__generate_image(prompt="修改后的文案", width=1024)
  │◄────────────────────────────────────│  结果通过 SSE 流式返回              │
  │  event: values/messages/custom      │                                    │
  │  event: end                         │                                    │
```

---

## 五、SSE 事件格式

客户端需要在 `stream_mode` 中包含 `"custom"` 以接收审批事件。

**审批请求事件**（`event: custom`）：
```json
{
  "type": "tool_approval_required",
  "tool_calls": [
    {
      "id": "tc-abc-123",
      "name": "cfgpu__generate_image",
      "args": {
        "prompt": "A serene mountain lake at sunset",
        "width": 1024,
        "height": 1024,
        "model": "flux-pro"
      }
    },
    {
      "id": "tc-def-456",
      "name": "cfgpu__generate_video",
      "args": {
        "prompt": "Waves crashing on rocky shore",
        "duration": 5
      }
    }
  ]
}
```

同一个 AI Message 产生的所有待审批工具调用合并在**一个事件**里。

---

## 六、Resume 协议

### 主路径（推荐，无重复 SSE）

resume 请求**必须**在 `config.configurable` 中携带 `ask: true`，否则 `HumanApprovalMiddleware` 不会注入，`tool_approvals` 决策将被静默忽略。

```http
POST /api/threads/{thread_id}/runs/stream
Content-Type: application/json

{
  "stream_mode": ["values", "custom"],
  "config": {
    "configurable": { "ask": true }
  },
  "command": {
    "update": {
      "tool_approvals": {
        "tc-abc-123": {
          "status": "approved",
          "args": {
            "prompt": "A serene mountain lake at sunset, oil painting style",
            "width": 1024,
            "height": 1024,
            "model": "flux-pro"
          }
        },
        "tc-def-456": {
          "status": "rejected",
          "reason": "视频暂不需要，只生成图片"
        }
      }
    }
  }
}
```

LangGraph 先将 `command.update` 合并入 `ThreadState.tool_approvals`，再重新执行 `after_model`。此时 state 里已有全部决策，middleware 跳过 SSE 和 `interrupt()`，直接应用决策并继续执行。

### `ask` 缺失时的 Warning 防护

若 resume 请求携带了 `command` 但未设置 `ask=true`，运行层（`run_agent()` / `AgentRunner.run()`）在构建 graph 之前检测到该情况，会：

1. 自动将 `ask` 置 `true`，确保 `HumanApprovalMiddleware` 被注入
2. 向客户端推送一条 warning custom 事件（在其他 progress 事件之前）：

```json
{
  "type": "warning",
  "code": "HIL_ASK_REQUIRED",
  "message": "HIL resume 消息必须设置 config.ask=true；本次已自动修正，请检查客户端实现"
}
```

resume 照常完成，用户无感；客户端开发者应在开发阶段修复实现。

### Fallback 路径（兼容，可能重复 SSE）

```http
POST /api/threads/{thread_id}/runs/stream
Content-Type: application/json

{
  "command": {
    "resume": {
      "approved": [
        {"id": "tc-abc-123", "args": {"prompt": "修改后的 prompt", "width": 1024}}
      ],
      "rejected": ["tc-def-456"]
    }
  }
}
```

`interrupt()` 会在 `after_model` 重入时返回 `resume` 值（而不是抛出），middleware 从返回值中解析决策并应用。此路径因 `after_model` 重入会再次发出 SSE 事件，客户端需做幂等处理（忽略审批对话框已关闭后的重复事件）。

---

## 七、与 MQ 消息通道的集成

### 审批暂停状态

当 `interrupt()` 触发后，Worker 通过 `aget_state()` 检测到 graph 处于 interrupt 状态，将 thread 标记为 `paused_for_approval`（区别于 `idle`，避免普通任务消息覆盖待审批状态）。

MQ 结果消息示例（状态 = `paused_for_approval`）：
```json
{
  "type": "result",
  "thread_id": "xxx",
  "status": "paused_for_approval"
}
```

### 审批 Resume 消息

MQ resume task 必须在 `config` 中携带 `ask: true`（与普通任务相同），否则触发 warning 防护：

```json
{
  "type": "task",
  "thread_id": "xxx",
  "messages": null,
  "command": {
    "update": {
      "tool_approvals": {
        "tc-abc-123": {"status": "approved", "args": {"prompt": "..."}},
        "tc-def-456": {"status": "rejected", "reason": "..."}
      }
    }
  },
  "config": { "ask": true }
}
```

缺少 `config.ask=true` 时，`AgentRunner` 自动修正并向客户端发 `warning {code:"HIL_ASK_REQUIRED"}` custom 事件，resume 仍正常完成。详细 MQ Consumer HIL 设计见 `tmp-HIL-consumer.md`。

---

## 八、注意事项

**工具调用 ID 的唯一性**

`tool_call_id` 是 LLM 生成的，每次 AI Message 都是新的 UUID。`tool_approvals` state 中的历史记录对新一轮 tool_calls 无影响（ID 不同），不会误判为已审批。

**全部拒绝的情况**

若所有工具调用都被拒绝，修改后的 `AIMessage.tool_calls` 为空列表，且 `messages` 中注入了对应的 error `ToolMessage`。Agent 路由检测到 `AIMessage` 无 tool_calls，会将控制权返还给 LLM，LLM 可感知拒绝原因（通过 error ToolMessage 的 `reason` 字段）并决定后续行为。

**非审批工具的透传**

同一 AI Message 中不匹配任何 pattern 的工具调用不受影响，直接透传给 ToolNode 执行，不需要等待审批。审批流程只针对匹配 `approval_required_tools` 的工具。

**Middleware 在链中的位置**

`HumanApprovalMiddleware` 插入在 `ClarificationMiddleware` 之前（`ClarificationMiddleware` 必须始终最后）。其他 middleware（如 `GuardrailMiddleware`）在 `wrap_tool_call` 层面运行，不影响 `after_model` 的拦截逻辑，两者正交。

**`ask` 的双重条件**

`HumanApprovalMiddleware` 只在 `config.ask=true`（运行时）**且** `approval_required_tools` 非空（静态 agent config）时注入。resume（command）请求若缺失 `ask=true`，运行层会自动修正并发 warning，但客户端应在实现层面保证始终携带该字段。

**LangGraph 版本要求**

依赖 `interrupt()`（LangGraph ≥ 1.0）和 `get_stream_writer()`（LangGraph ≥ 1.0）。已在 LangGraph 1.2.15 上验证。
