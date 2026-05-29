# Middleware 执行顺序与 tool_call / tool_result 对应关系

本文档面向 cfgpu MQ consumer 开发者，说明：

1. 各钩子（hook）的执行顺序
2. 哪些情况下 tool_call 不会执行
3. 各种情况下是否会有对应的 `tool_result` custom 事件

---

## 一、Middleware 加载顺序与各钩子执行顺序

### 1.1 加载顺序（append 顺序）

```
[0] ThreadData
[1] Uploads
[2] Sandbox
[3] DanglingToolCall
[4] LLMErrorHandling
[5] Guardrail（可选）
[6] SandboxAudit
[7] ToolErrorHandling
[8] DynamicContext
[9] Mlm（可选）
[10] Summarization（可选）
[11] TodoList（可选）
[12] TokenUsage（可选）
[13] Title
[14] Memory
[15] ViewImage
[16] DeferredToolFilter（可选）
[17] SubagentLimit（可选）
[18] LoopDetection（可选，默认开启）
[custom middlewares...]
[19] HumanApproval（可选）
[20] MessageStream
[21] SafetyFinishReason（可选）
[22] Clarification
```

### 1.2 after_model：**逆序**执行（后 append 的先触发）

```
SafetyFinishReason[21] → HumanApproval[19] → LoopDetect[18] → SubagentLimit[17]
→ Title[13] → TokenUsage[12] → TodoList[11]
```

### 1.3 wrap_model_call：**洋葱模型**（后 append 的在内层，更接近 LLM）

```
外层（先进后出）：
  DanglingToolCall[3]
    LLMErrorHandling[4]
      TodoList[11]
        DeferredToolFilter[16]
          LoopDetect[18]
            MessageStream[20]   ← 最内层（最接近 LLM 调用）
              ← LLM 调用 →
```

执行时序：
1. 外层 middleware 按顺序修改 request（如 DanglingToolCall 注入占位 ToolMessage、LoopDetect 注入 loop warning）
2. MessageStream 调用 LLM，拿到 AIMessage，**立即发出 `ai_message` 事件**
3. 返回 AIMessage，逐层向外传递

**关键**：`ai_message` 事件在 `wrap_model_call` 阶段发出，此时 `after_model` 尚未执行。后续的 `after_model` 可能修改 AIMessage（见第二节）。

### 1.4 wrap_tool_call：**洋葱模型**（后 append 的在内层）

```
外层（先进后出）：
  Guardrail[5]
    SandboxAudit[6]
      ToolErrorHandling[7]
        DeferredToolFilter[16]
          MessageStream[20]
            Clarification[22]   ← 最内层（最接近实际工具调用）
              ← tool 执行 →
```

MessageStream 在 Clarification 外层：调用内层后拿到结果，若是 `ToolMessage` 则发出 `tool_result` 事件，若是 `Command` 则直接返回（不发事件）。

---

## 二、tool_call 不会执行的情形

### 2.1 SafetyFinishReasonMiddleware 触发（after_model）

**触发条件**：LLM 输出因安全原因被截断（OpenAI `content_filter`、Anthropic `refusal`、Gemini `SAFETY` 等），但截断的输出中仍带有不完整的 tool_calls。

**行为**：
- 清除 AIMessage 的所有 tool_calls
- 发出 `safety_termination` custom 事件（含被取消的工具名和数量）
- 图路由到 END

**时序问题**：MSM 已在 `wrap_model_call` 中发出了含这些 tool_calls 的 `ai_message`，但 Safety 在 `after_model` 中才清除。

**tool_result 情况**：
- ✗ 无 `tool_result` 事件（工具从未执行）
- ✓ 有 `safety_termination` 通知事件，consumer 可据此清理未完成的 tool_call 占位

---

### 2.2 LoopDetectionMiddleware hard-stop（after_model）

**触发条件**：
- Layer 1（hash-based）：同一组 tool_calls（相同工具+相同参数的集合）重复出现 ≥ `hard_limit`（默认 5）次
- Layer 2（frequency-based）：同一工具名（无论参数）被调用 ≥ `tool_freq_hard_limit`（默认 50）次

**行为**：
- 清除 AIMessage 的所有 tool_calls
- 将 hard-stop 说明文字追加到 AIMessage.content
- **为每个被取消的 tool_call 发出 `tool_result` custom 事件**（status=error，content 含 hard-stop 原因）
- 图路由到 END（无 tool_calls → should_continue → END）
- 不进入下一轮 LLM 调用，hard-stop 文字就是最终回复

**tool_result 情况**：
- ✓ 有 `tool_result` 事件（每个被取消的 tool_call 各一条，status=error）

**cfgpu 注意事项**：`tool_key_overrides` 必须配置，否则不同 prompt 的 `cfgpu_generate_*` 调用会被误判为循环：

```yaml
loop_detection:
  tool_key_overrides:
    "*generate*":
      mode: full   # hash 全部参数，不同 prompt → 不同 hash
```

---

### 2.3 LoopDetectionMiddleware warn（after_model）

**触发条件**：重复次数达到 `warn_threshold`（默认 3），未到 `hard_limit`。

**行为**：
- 不清除 tool_calls
- 将 warning 文字存入 pending_warnings，在下一次 `wrap_model_call` 时作为 HumanMessage 追加到消息列表，注入给 LLM
- 本轮工具正常执行

**tool_result 情况**：
- ✓ 工具正常执行，MSM 正常发出 `tool_result` 事件

---

### 2.4 HumanApprovalMiddleware interrupt（after_model）

**触发条件**：AIMessage 中有匹配 `approval_required_tools` 模式的 tool_calls，且用户尚未做出决策。

**行为**：
- 发出 `tool_approval_required` custom 事件
- 调用 `interrupt()`，抛出 GraphInterrupt，图暂停
- **此时 after_model 链中排在 HAM 之后的 middleware（LoopDetect[18]、SubagentLimit[17] 等）不会执行**
- 等待 resume（用户决策）

**resume 后行为**：
- approved：工具正常进入执行节点，MSM 发出 `tool_result` 事件
- rejected：HAM 直接注入 error ToolMessage 到 state，不经过 wrap_tool_call 链

**tool_result 情况**：
- ✓ approved 工具：有 `tool_result` 事件（经 MSM wrap_tool_call）
- ✗ rejected 工具：无 `tool_result` custom 事件（HAM 直接写 state，绕过 MSM）
  - rejected 工具是用户主动拒绝，客户端通过 `tool_approval_required` 中的决策可知结果，无 `tool_result` 是可接受的设计

---

### 2.5 ClarificationMiddleware 拦截 ask_clarification（wrap_tool_call）

**触发条件**：工具调用名称为 `ask_clarification`。

**行为**：
- 拦截工具调用，不实际执行工具
- 返回 `Command(update={"messages": [ToolMessage]}, goto=END)`
- 图直接跳到 END，不进入后续轮次

**MSM 的处理**：
- MSM 收到 `Command`（非 `ToolMessage`），不发出 `tool_result` 事件

**ai_message / tool_message 内容**：
- `ai_message`（MSM 在 `wrap_model_call` 发出）：含 `ask_clarification` tool_call，args 包含 `question`/`context`/`options` 等
- ToolMessage（通过 `Command.update` 直接写入 state，不经 MSM）：格式化好的问题文本，供前端展示

**tool_result 情况**：
- ✗ 无 `tool_result` 事件（设计决定：clarification 是中断机制，不是真正工具执行）
- consumer 处理方式：收到含 `ask_clarification` tool_call 的 `ai_message` 后，等待 `ask_clarification` 的 `tool_result` 事件永远不会到来；当 run 结束（END）时，consumer 应以"等待用户回复"状态结束本轮处理

---

### 2.6 User cancel / run 中断（跨 run dangling）

**触发条件**：用户取消正在执行的 run，部分 tool_calls 尚未执行完或尚未开始。

**行为**：
- 下一个 run 启动时，DanglingToolCallMiddleware 在 `wrap_model_call` 中检测到未配对的 tool_calls
- 注入占位 ToolMessage（`"[Tool call was interrupted and did not return a result.]"`）到 LLM 的 request 中，但不写入 state
- LLM 在新 context 中看到这些占位 ToolMessage，可据此生成合理回复

**tool_result 情况**：
- ✗ 无 `tool_result` 事件（取消是用户主动操作，客户端知道 run 已取消）
- 这是可接受的设计：用户 cancel 后立即重发新消息，新 run 的 LLM 会解释上次的中断

---

## 三、tool_result 对应关系汇总

| 情形 | 工具是否执行 | tool_result 事件 | 额外 custom 事件 |
|------|------------|-----------------|----------------|
| 正常执行 | ✓ | ✓ 经 MSM wrap_tool_call | — |
| Safety 截断 tool_calls | ✗ | ✗ | `safety_termination`（含 detector、reason_field、reason_value、suppressed_tool_call_names） |
| LoopDetect hard-stop | ✗ | ✓ status=error（LoopDetect 主动发出） | — |
| LoopDetect warn | ✓ | ✓ 正常执行 | — |
| HAM approved | ✓ | ✓ 经 MSM wrap_tool_call | — |
| HAM rejected | ✗ | ✗ | — （用户通过 `tool_approval_required` 决策可知） |
| ask_clarification | ✗（中断机制）| ✗ | — （图到 END，consumer 等待用户回复） |
| User cancel（dangling）| ✗ | ✗ | — （客户端自知 run 已取消） |

---

## 四、Consumer 处理建议

### 4.1 ai_message 中 tool_calls 与 tool_result 的关联

每个 `ai_message` 中的 `tool_calls[].id` 对应一个（或零个）后续的 `tool_result.tool_call_id`。Consumer 应：

1. 收到 `ai_message` 时，为每个 tool_call 建立"等待 tool_result"的占位
2. 收到 `tool_result` 时，按 `tool_call_id` 匹配并完成占位
3. 对于**永远不会到来**的 tool_result（見上表），通过以下信号来关闭占位：
   - `safety_termination` 事件：关闭所有未配对的 tool_calls
   - `tool_result` status=error 且 content 含 `"cancelled"`：LoopDetect hard-stop（已有 tool_result，直接处理即可）
   - run 结束（END）且有未配对的 `ask_clarification` tool_call：等待用户回复状态
   - run 结束（END）且有其他未配对 tool_calls：用户 cancel 或 Safety 截断（安全关闭占位）

### 4.2 ask_clarification 的特殊处理

当 `ai_message.tool_calls` 中出现 `ask_clarification` 时：
- 不等待 `tool_result`
- 等待 run 结束后，将对话状态标记为"等待用户回复澄清问题"
- 问题内容在 `ai_message.tool_calls[].args.question` 和 `args.options` 中

### 4.3 cfgpu generate 工具的注意事项

`cfgpu_generate_image` / `cfgpu_generate_video` 等工具耗时较长，tool_result 可能在 ai_message 发出后几十秒才到来。Consumer 应对每个 tool_call 维护独立的超时，而非对整个 run 设置单一超时。
