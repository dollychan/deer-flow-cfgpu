# MessageStreamMiddleware 设计文档

## 一、作用

`MessageStreamMiddleware` 是一个 deerflow Agent 中间件，在 Agent 执行的两个关键节点**主动 emit custom 事件**：

- **LLM 调用完成后**（`wrap_model_call`）：emit `ai_message` custom 事件，携带 AI 回复内容和工具调用列表
- **工具执行完成后**（`wrap_tool_call`）：emit `tool_result` custom 事件，携带工具执行结果

配合 `stream_event_types=["custom"]`，客户端只接收语义明确的 custom 事件，不再处理 LangGraph 自动产生的 `messages` / `values` 全量 state 快照，从而消除中间件内部修改（summarization 压缩、dynamic context 注入、MLM 注入）对下行消息流的污染。

---

## 二、设计原理

### 2.1 问题：LangGraph 自动事件的噪音

使用 `stream_mode=["messages", "values"]` 时，LangGraph 自动为每个 graph 节点产生事件：

- **`values` 事件**：每个 node 产生一条全量 state 快照，包含所有历史消息。SummarizationMiddleware 压缩消息、DynamicContextMiddleware 注入 system-reminder、MLMMiddleware 注入 memory 提示，都会在 values 事件中体现，客户端收到大量无关数据。
- **`messages` 事件**：逐 chunk 流式输出，对 token 级别流式场景有意义，但 MQ 通道本身已引入延迟，token 级别流式意义不大。

### 2.2 方案：在钩子点主动 emit

```
LLM 调用
    ↓
wrap_model_call 拿到 AIMessage
    → stream_writer({"type": "ai_message", ...})   ← MessageStreamMiddleware
    → 返回 AIMessage
    ↓
after_model 链执行（HumanApprovalMiddleware 等）
    ↓
ToolNode 并行执行工具
    ↓
wrap_tool_call 拿到 ToolMessage
    → stream_writer({"type": "tool_result", ...})  ← MessageStreamMiddleware
    → 返回 ToolMessage
```

`stream_writer` 写入的数据在 `stream_mode` 含 `"custom"` 时，以 `("custom", data)` 的形式从 `agent.astream()` yield 出来，经 `MQStreamBridge.publish()` 封装为 progress 消息发送给上游。

### 2.3 自然过滤：哪些情况不会触发

| 情况 | 是否触发 wrap_model_call / wrap_tool_call |
|------|------------------------------------------|
| **SummarizationMiddleware** 内部 LLM 调用 | ✗ 直接构造 chain，不经过 agent model binding |
| **DynamicContextMiddleware** 注入 system-reminder | ✗ 仅修改 state.messages，无模型调用 |
| **MLMMiddleware** 注入 memory 提示 | ✗ 仅修改 state.messages，无模型调用 |
| **DanglingToolCallMiddleware** 注入占位 ToolMessage | ✗ state update，未调用实际工具 |
| **HumanApprovalMiddleware** 注入拒绝 ToolMessage | ✗ 人工构造，非 wrap_tool_call 路径 |
| 主 Agent LLM 调用 | ✓ |
| 主 Agent 工具调用（bash、MCP 工具等） | ✓ |

Summarization、middleware 注入等内部操作全部被自然过滤，无需额外判断。

### 2.4 与 HumanApprovalMiddleware 的顺序关系

`wrap_model_call` 和 `after_model` 是两个独立阶段：

```
wrap_model_call 执行（MessageStreamMiddleware emit ai_message）
    → emit ai_message（含原始 tool_calls、原始 args）
    → 返回 AIMessage，写入 state

after_model 执行（HumanApprovalMiddleware 拦截）
    → emit tool_approval_required
    → interrupt() 暂停 graph
```

客户端收到事件的顺序：
```
ai_message        ← 模型想调用哪些工具（原始参数）
tool_approval_required  ← 等待用户审批
（用户操作后 resume）
tool_result       ← 审批通过的工具的执行结果
ai_message        ← 模型对结果的最终回复
```

`ai_message` 中的 `tool_calls.args` 是模型生成的原始参数；审批后用户可能修改参数，实际执行的参数体现在 `tool_result` 的执行上下文中，不再反映到 `ai_message`。

---

## 三、Custom 事件 Schema

### 3.1 ai_message — LLM 回复

由 `wrap_model_call` 在每次主 Agent LLM 调用完成后 emit。

```json
{
  "type": "ai_message",
  "message_id": "msg_01Abc123",
  "content": "根据搜索结果，2025 年 AI 主要趋势包括...",
  "tool_calls": [
    {
      "id": "call_abc123",
      "name": "cfgpu__generate_image",
      "args": {
        "prompt": "英雄归途，晨雾中的山谷，油画风格",
        "model": "flux-pro",
        "width": 1024,
        "height": 1024
      }
    }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| type | string | 固定 `"ai_message"` |
| message_id | string | AIMessage 的 ID，用于客户端去重和关联 |
| content | string | AI 回复文字，可为空字符串（纯工具调用轮次） |
| tool_calls | array | 本轮工具调用列表，无工具调用时为空数组 `[]` |
| tool_calls[].id | string | 工具调用 ID（`tool_call_id`），与后续 `tool_result` 关联 |
| tool_calls[].name | string | 工具名（含 MCP server 前缀，如 `cfgpu__generate_image`） |
| tool_calls[].args | object | 模型生成的原始工具参数 |

**emit 条件**：`content` 非空 **或** `tool_calls` 非空；两者均为空（即空 AIMessage）时跳过，不 emit。

### 3.2 tool_result — 工具执行结果

由 `wrap_tool_call` 在每次工具调用完成后 emit（每个工具调用独立一条事件）。

```json
{
  "type": "tool_result",
  "message_id": "msg_tool_uuid",
  "tool_call_id": "call_abc123",
  "name": "cfgpu__generate_image",
  "content": "{\"image_url\": \"https://cdn.example.com/output.png\", \"width\": 1024}",
  "status": "success"
}
```

**工具执行失败**：

```json
{
  "type": "tool_result",
  "message_id": "msg_tool_uuid2",
  "tool_call_id": "call_def456",
  "name": "bash",
  "content": "Command failed: Permission denied",
  "status": "error"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| type | string | 固定 `"tool_result"` |
| message_id | string | ToolMessage 的 ID |
| tool_call_id | string | 对应 `ai_message.tool_calls[].id` |
| name | string | 工具名 |
| content | string | 工具输出内容。超出 `max_content_chars`（默认 4096）时截断，附加 `[truncated]` 标记 |
| status | enum | `"success"` 或 `"error"` |

---

## 四、在 Middleware 链中的位置

`MessageStreamMiddleware` 应插入在 `HumanApprovalMiddleware` **之后**、`ClarificationMiddleware` **之前**：

```
...
LoopDetectionMiddleware
HumanApprovalMiddleware      ← after_model 修改 AIMessage
MessageStreamMiddleware      ← wrap_model_call / wrap_tool_call emit 事件
ClarificationMiddleware      ← 始终最后
```

**为什么在 HumanApprovalMiddleware 之后**：

middleware 链的 `after_model` 按 LIFO 顺序执行（后 append 的先触发），但 `wrap_model_call` 是嵌套调用链（后 append 的在内层，即更接近 LLM）。

`MessageStreamMiddleware` 在 `HumanApprovalMiddleware` 之后 append，意味着：
- `wrap_model_call`：MessageStream 在外层，先拿到 AIMessage → emit ai_message → 再向内调用（最终到 LLM）
- `after_model`：HumanApproval（后 append）先触发，修改 AIMessage / emit tool_approval_required

两者在不同阶段运行，互不干扰。`ai_message` 事件携带模型原始输出，`tool_approval_required` 事件在其后独立发出，客户端收到顺序清晰。

**注入条件**：`MessageStreamMiddleware` 始终注入，无额外运行时开关。

---

## 五、配套 stream_mode 设置

使用 `MessageStreamMiddleware` 时，推荐 `reply_config` 设置：

```json
{
  "stream_events": true,
  "stream_event_types": ["custom"]
}
```

`stream_mode=["custom"]` 下，`agent.astream()` 只 yield `stream_writer` 显式写入的 custom 事件，LangGraph 不自动产生 messages / values 事件，彻底消除噪音。

仍支持 `["messages", "custom"]`、`["values", "custom"]` 等混合模式，但无必要——`MessageStreamMiddleware` 已覆盖所有需要下行的内容。

---

## 六、与 MQ 消息通道的集成

### result_cache 结构变化

`AgentRunner` 的内部 `result_cache`（用于重复投递幂等重放）不再保存 `final_state`，改为保存本次执行**开始前**的 checkpoint ID：

```python
# 执行前记录
start_checkpoint_id = await _get_current_checkpoint_id(agent, runnable_config)

# 执行完成后存入 result_cache
result_cache = {
    "status": "success",
    "start_checkpoint_id": start_checkpoint_id,
}
```

**重复投递重放**：从 `start_checkpoint_id` 之后的 checkpoint 序列中，逐步提取新增 AIMessage / ToolMessage，按 `ai_message` / `tool_result` schema 重新 emit 为 custom 事件，与首次执行的下行消息流保持一致。

**原因**：执行过程中 SummarizationMiddleware 可能压缩消息、DynamicContextMiddleware 可能修改消息，`final_state` 中的 messages 不能完整还原执行过程；从 checkpoint 逐步重放才能准确反映每一步的输出。

### result 消息变化

`stream_events=true` 时，result payload 不再包含 `final_state`（内容已通过 custom 事件全量下发）：

```json
{
  "status": "success"
}
```

`stream_events=false` 时，result payload 保留 `final_state`（客户端无法从 progress 获取内容）。

---

## 七、注意事项

**tool_result 大内容截断**

工具输出（如网页抓取、长文档读取）可能超出 MQ 消息体积限制。`wrap_tool_call` 中对 `content` 超过 `max_content_chars`（默认 4096 字符）的部分截断并追加 `[truncated: N chars omitted]`。客户端如需完整内容应通过 artifact 路径获取。

**HumanApproval 拒绝的 ToolMessage 不 emit**

`HumanApprovalMiddleware` 拒绝工具调用时，注入的 error ToolMessage 是人工构造、直接写入 state 的，不经过 `wrap_tool_call`，因此 `MessageStreamMiddleware` 不会 emit `tool_result` 事件。客户端通过 `ai_message.tool_calls` 中对应 ID 没有后续 `tool_result` 来感知（或通过 `tool_approval_required` 事件的拒绝决策）。

**Subagent task 事件**

Subagent（`task` 工具）执行期间发出的 `task_started`、`task_running`、`task_completed` 等事件由 SubagentExecutor 直接通过 `stream_writer` 写入，属于 custom 事件，`stream_event_types=["custom"]` 下正常下行，与 `MessageStreamMiddleware` 正交。
