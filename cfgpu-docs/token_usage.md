# Token Usage 统计设计

本文档面向 cfgpu MQ Consumer 开发者，说明：

1. deerflow 原框架的 token 统计机制及其设计目的
2. 哪些步骤会消耗 token
3. Consumer 路径的实现方案
4. MQ 消息协议中 progress custom 事件的 usage 字段扩展

---

## 一、所有 token 消耗来源

| 来源 | 触发路径 | 经过 wrap_model_call？ | 客户端可见性 |
|------|----------|----------------------|------------|
| 主 agent LLM 调用 | `wrap_model_call`（MessageStreamMiddleware 最内层） | **是** | `ai_message.usage` ✓ |
| cfgpu 工具（generate_image / generate_video 等） | `wrap_tool_call` 执行外部模型 API | 否（工具执行） | `tool_result.usage` ✓（需工具配合） |
| 子 agent（`task` 工具） | 子图独立运行，有自己的 LLM 调用 | 否（子图独立 callback） | `tool_result.usage` ✓（需聚合子图 tokens） |
| SummarizationMiddleware | `before_model` 直接调用 chain（非 wrap_model_call） | **否** | **不可见**（内部上下文压缩） |
| TitleMiddleware | `after_model` 直接 `model.ainvoke()`（非 wrap_model_call） | **否** | **不可见**（内部标题生成） |
| MemoryMiddleware | 异步后台 timer 线程，run 结束后才执行 | **否** | **不可见**（后台任务，不属于任何 run） |

### 不对客户端暴露的三类内部消耗

**SummarizationMiddleware**：在 `before_model` 钩子中通过直接 chain 调用执行，MessageStreamMiddleware 的 docstring 明确将其标注为 "naturally excluded"。摘要是透明的上下文压缩机制，暴露给客户端会引起困惑（用户看到一次未知的 LLM 调用）。

**TitleMiddleware**：在 `after_model` 钩子中调用 `model.ainvoke()`，token 消耗极小（生成数个词的标题），不单独上报。

**MemoryMiddleware**：通过 background timer（debounce 30s）在独立线程中执行 memory 更新，从技术上无法归入任何特定 run 的 usage。

---

## 二、deerflow 原框架的 token 统计机制

deerflow 有**两套独立系统**，服务不同目的：

### 系统一：TokenUsageMiddleware（前端 UI 归因展示）

**文件**：`packages/harness/deerflow/agents/middlewares/token_usage_middleware.py`

**触发时机**：`after_model` 钩子（每次 LLM 调用完成后，在下一个 node 执行前）

**三个核心职责**：

1. **打印 token 日志**：读 `state.messages[-1].usage_metadata` → `logger.info` 记录 input/output/total
2. **写入 attribution 注解**：将步骤类型（`final_answer` / `tool_batch` / `subagent_dispatch` / `todo_update`）写入 `AIMessage.additional_kwargs["token_usage_attribution"]`，供前端展示"这步做了什么、用了多少 token"
3. **合并子 agent token**：检测最新 `ToolMessage` 是否来自 `task` 工具调用，通过 `pop_cached_subagent_usage(tool_call_id)` 把子 agent 的 token 合并回父 `AIMessage.usage_metadata`，实现跨层 token 归因

**数据流向**：token 数字存在 LangGraph state 的 `messages` 里（每条 `AIMessage.usage_metadata`），通过 checkpointer 持久化，供 Gateway API 的 `/messages` 端点查询。

**注意**：`AIMessage.usage_metadata` 是 LangChain 框架级别的标准字段，由 LLM provider 在响应时**自动填充**，**不依赖 TokenUsageMiddleware 才有**。TokenUsageMiddleware 只是额外的日志打印和前端 attribution 注解。

### 系统二：RunJournal（run 级别聚合 + 消息历史存储）

**文件**：`packages/harness/deerflow/runtime/journal.py`

**仅存在于 Gateway 路径**（Consumer 路径无此机制）。

**实现方式**：LangChain callback handler，挂载在 `RunnableConfig["callbacks"]`

| Callback | 触发时机 | 作用 |
|----------|---------|------|
| `on_chat_model_start` | LLM 调用前 | 记录 `llm.human.input` 事件，提取第一条用户消息 |
| `on_llm_end` | LLM 调用完成 | 记录 `llm.ai.response` 事件，累加 `_total_input_tokens` 等计数器 |
| `on_chain_start` (root) | graph 根节点启动 | 记录 `run.start` trace |
| `on_chain_end/error` | chain 结束 | 记录 `run.end/error`，触发 flush |

**run 结束时**：`journal.get_completion_data()` → `run_manager.update_run_completion()` → 写入数据库 RunRow

```python
# get_completion_data() 返回结构
{
    "total_input_tokens": N,
    "total_output_tokens": N,
    "total_tokens": N,
    "llm_call_count": N,
    "lead_agent_tokens": N,
    "subagent_tokens": N,
    "middleware_tokens": N,
    "message_count": N,
    "last_ai_message": "...",
    "first_human_message": "...",
}
```

**重要**：Gateway SSE 的 `result` 消息本身**也不含 `usage` 字段**，token 数据存在数据库，由 `GET /threads/{id}/runs/{rid}/token-usage` API 单独查询。

### RunEventStore 与 RunStore 的目的

**RunStore**（runs 表）：存 run 生命周期元数据（status、时间戳、token 汇总、first_human_message）。用于 run 历史列表查询、计费统计、run 状态跟踪。

**RunEventStore**（run events 流）：
- `category="message"`：`llm.human.input`、`llm.ai.response` 事件 —— **这是前端聊天历史的数据来源**（`GET /runs/{rid}/messages`），刻意绕过 LangGraph checkpoint，以过滤掉 Summarization 摘要注入、MLM memory 注入等中间件内部消息，保持聊天历史干净
- `category="trace"`：`run.start`、`run.error` 等执行追踪，供调试和审计用

---

## 三、Consumer 路径：token usage 获取方案

Consumer 路径无 Gateway 的 `RunJournal`，也不需要 RunStore/RunEventStore（Consumer 只负责通过 MQ 推送结果，不做 Gateway 侧的历史存储）。

Consumer 需要在以下两处附加 usage：

### 3.1 ai_message custom 事件（主 agent LLM）

**实现位置**：`MessageStreamMiddleware._emit_ai_message()`（**已实现**）

`wrap_model_call` 在 `handler(request)` 返回后即可直接读取 `AIMessage.usage_metadata`，无需额外机制。实际实现：

```python
def _emit_ai_message(self, ai_msg: AIMessage) -> None:
    content = _extract_text_content(ai_msg.content)
    tool_calls = [
        {"id": tc["id"], "name": tc["name"], "args": tc["args"]}
        for tc in (ai_msg.tool_calls or [])
    ]
    if not content and not tool_calls:
        return

    event: dict = {
        "type": "ai_message",
        "message_id": ai_msg.id or "",
        "content": content,
        "tool_calls": tool_calls,
    }
    if ai_msg.usage_metadata:
        event["usage"] = dict(ai_msg.usage_metadata)
    self._emit(event)
```

`UsageMetadata` 是 LangChain 的 TypedDict，运行时即普通 dict，`dict()` 展开后直接可 JSON 序列化，包含 `input_tokens`、`output_tokens`、`total_tokens` 及可选的 `input_token_details`、`output_token_details`（部分 provider 不返回细分字段时这两个 key 缺省）。

**时序说明**（见 middlewares-sequence.md §1.3）：`MessageStreamMiddleware` 是 `wrap_model_call` 链的最内层，调用 LLM 后立即 emit `ai_message` 事件。此时 `after_model` 尚未执行，`TokenUsageMiddleware` 还没有合并子 agent token。这是合理的：`ai_message` 报告的是这次 LLM 调用本身的 token，不含子 agent 的合并量。

### 3.2 tool_result custom 事件（cfgpu 工具）

**状态：协议已定义，代码待实现**（等待 cfgpu MCP 服务端确认返回结构后补充）。

cfgpu 工具（`generate_image`、`generate_video` 等）调用外部模型 API，其"token"含义与 LLM token 不同（可能是 prompt token、compute units 或 credits），具体字段结构由 cfgpu MCP 服务端定义。

**前提**：cfgpu MCP 服务端需要在工具返回时携带用量信息。推荐通过 `ToolMessage.additional_kwargs["usage"]` 传递（而非嵌入 content JSON，避免耦合）：

```python
# cfgpu MCP 工具返回 ToolMessage 时附加 usage（结构待 cfgpu 确认）
ToolMessage(
    content='{"image_url": "https://cdn.example.com/output.png", "width": 1024}',
    tool_call_id="call_abc123",
    additional_kwargs={"usage": {"compute_units": 1.5, "model": "flux-pro"}},
)
```

**实现位置**：`MessageStreamMiddleware._emit_tool_result()`，待 cfgpu 返回结构确认后添加：

```python
# 附加工具自报的资源消耗（cfgpu 等有外部模型 API 的工具），待实现
additional = getattr(tool_msg, "additional_kwargs", {}) or {}
if usage := additional.get("usage"):
    event["usage"] = usage
```

### 3.3 result 消息中的全局 usage 汇总（可选）

若上游系统需要整个 run 的 token 总量（用于计费或展示），可在 `AgentRunner._execute()` 中挂一个轻量 callback，在 `astream()` 期间实时累加：

```python
from langchain_core.callbacks import BaseCallbackHandler

class _RunUsageCollector(BaseCallbackHandler):
    """Collect cumulative LLM token usage for the current run."""

    def __init__(self):
        super().__init__()
        self.input_tokens = self.output_tokens = self.total_tokens = 0
        self._seen: set[str] = set()

    def on_llm_end(self, response, *, run_id, **kwargs):
        rid = str(run_id)
        if rid in self._seen:
            return
        for gen_list in response.generations:
            for gen in gen_list:
                u = getattr(getattr(gen, "message", None), "usage_metadata", None) or {}
                it = u.get("input_tokens", 0) or 0
                ot = u.get("output_tokens", 0) or 0
                tt = u.get("total_tokens", 0) or (it + ot)
                if tt > 0:
                    self._seen.add(rid)
                    self.input_tokens += it
                    self.output_tokens += ot
                    self.total_tokens += tt
                    return
```

在 `_build_config()` 的 callbacks 里注入，run 结束后传给 `publish_result(usage={...})`。

**注意**：不能用 `final_state.values["messages"]` 累加——`aget_state()` 返回的是 checkpoint **全量历史**，包含本次 run 之前所有轮次的 AIMessage，直接累加会重复计算旧 run 的 token。

---

## 四、MQ 消息协议更新：progress custom 事件 usage 字段

### 4.1 ai_message 新增 usage 字段

```json
{
  "event_type": "custom",
  "data": {
    "type": "ai_message",
    "message_id": "msg_01Abc123",
    "content": "根据你的描述，我来生成这张图片。",
    "tool_calls": [
      {
        "id": "call_abc123",
        "name": "cfgpu__generate_image",
        "args": { "prompt": "英雄归途，晨雾中的山谷，油画风格" }
      }
    ],
    "usage": {
      "input_tokens": 9307,
      "output_tokens": 57,
      "total_tokens": 9364,
      "input_token_details": { "cache_read": 8192, "cache_creation": 0 },
      "output_token_details": { "reasoning": 23 }
    }
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `usage` | object | 否 | 本次 LLM 调用的 token 用量；模型未返回计费信息时省略 |
| `usage.input_tokens` | int | - | 输入 token 数（含 system prompt、对话历史） |
| `usage.output_tokens` | int | - | 输出 token 数（含 reasoning tokens） |
| `usage.total_tokens` | int | - | 合计 |
| `usage.input_token_details` | object | - | 细分字段：`cache_read`、`cache_creation` 等（provider 支持时存在） |
| `usage.output_token_details` | object | - | 细分字段：`reasoning`（thinking 模式下存在）等 |

**客户端注意**：
- `usage` 字段缺省时，表示本次模型调用未返回 token 信息（部分 provider 或本地模型）
- `input_token_details.cache_read` 反映 prompt cache 命中量，可用于计算实际计费 token
- 每条 `ai_message` 的 `usage` 报告的是**这次单次 LLM 调用**的 token，不含子 agent 或 cfgpu 工具的消耗

### 4.2 tool_result 新增 usage 字段

```json
{
  "event_type": "custom",
  "data": {
    "type": "tool_result",
    "message_id": "msg_tool_uuid",
    "tool_call_id": "call_abc123",
    "name": "cfgpu__generate_image",
    "content": "{\"image_url\": \"https://cdn.example.com/output.png\", \"width\": 1024, \"height\": 1024}",
    "status": "success",
    "usage": {
      "input_tokens": 77,
      "compute_units": 1
    }
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `usage` | object | 否 | 工具执行消耗的计算资源；仅 cfgpu 生图/生视频等有外部模型 API 消耗的工具携带，普通工具省略 |
| `usage.input_tokens` | int | - | 工具侧模型 API 的 prompt token 数（如文本 prompt 被图像模型计价） |
| `usage.compute_units` | int | - | 消耗的计算单元数（cfgpu 平台计费单位，工具自定义） |

`usage` 字段内容由工具自定义，非强制标准化。客户端可按工具名称区分处理逻辑。

### 4.3 各 custom 事件 usage 字段汇总

| custom 事件类型 | usage 字段 | 含义 |
|----------------|-----------|------|
| `ai_message` | 可选 | 主 agent 本次 LLM 调用的 token（input/output/total 及细分） |
| `tool_result` | 可选 | 工具调用的外部模型 API 消耗（cfgpu 工具专有，结构由工具定义） |
| `tool_approval_required` | 无 | 审批事件，不消耗 token |
| `warning` | 无 | 系统警告，不消耗 token |
| `safety_termination` | 无 | 安全截断通知，不消耗 token |

---

## 五、wrap_model_call 与 after_model 的时序关系

理解 usage 字段的准确含义，需要了解 `MessageStreamMiddleware` 在 middleware 链中的位置（详见 middlewares-sequence.md §1.2 和 §1.3）：

```
wrap_model_call（洋葱，MessageStream 最内层）
  → LLM 调用
  → ai_message 事件 emit（含本次调用的 usage_metadata）
  ↓ 返回 AIMessage，逐层向外传递

after_model（逆序执行，MessageStream 不在此链）
  SafetyFinishReason → HumanApproval → LoopDetect → SubagentLimit
  → Title → TokenUsage → TodoList
```

**关键**：`ai_message` 事件在 `wrap_model_call` 阶段发出，此时 `after_model` 尚未执行。后续 `TokenUsageMiddleware`（`after_model`）的子 agent token 合并发生在 `ai_message` 已经 emit 之后。因此：

- `ai_message.usage`：报告**此次 LLM 调用本身**的 token，不含子 agent 合并量（合并发生更晚）
- 若需要含子 agent 的总量，需等 run 完成后从 result 消息的全局 usage 中读取（需实现 §3.3 的 `_RunUsageCollector`）
