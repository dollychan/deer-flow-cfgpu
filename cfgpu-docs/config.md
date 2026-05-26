# Consumer 运行时 Config 参数参考

本文档整理 MQ task 消息的 `config` / `reply_config` 字段如何经由 `AgentRunner._build_config()` 转换为 `RunnableConfig`，最终被 `make_lead_agent()` 消费。

---

## 数据流概览

```
TaskMessage.config / reply_config
        │
        ▼
AgentRunner._build_config()
        │
        ├─ config["configurable"]   → LangGraph checkpointer 路由 + agent 工厂读取
        └─ config["context"]        → runtime.context（所有 middleware + Python 工具读取）
                │
                ▼
        make_lead_agent(config)
          _get_runtime_config(config)  ← 将 configurable + context 合并为扁平 cfg
```

**关键设计**：Gateway 路径通过 `worker.py` 的 `_build_runtime_context` 填充 `context`；Consumer 路径绕过 `worker.py`，必须在 `_build_config` 中手动填充，否则所有 middleware 拿到的 `runtime.context` 为空。

---

## MQ 协议字段 → Config 映射表

### `config` 字段（task payload）

| MQ 字段 | 类型 | 默认值 | configurable key | context key | make_lead_agent 用途 |
|---------|------|--------|-----------------|-------------|---------------------|
| `thinking_enabled` | bool | `true` | `thinking_enabled` | `thinking_enabled` | 开启 model extended thinking；同时映射为 `is_plan_mode` |
| `thinking_enabled`（映射） | — | `false` | `is_plan_mode` | `is_plan_mode` | 启用 `TodoMiddleware`（plan mode） |
| `web_search_enabled` | bool | `true` | `web_search_enabled` | `web_search_enabled` | 控制 `group="web"` 工具是否加载（web_search / web_fetch） |
| `ask` | bool | `false` | `ask` | `ask` | 启用 `HumanApprovalMiddleware`（HIL 工具审批中断） |
| `timeout_seconds` | int | null | — | — | AgentRunner 层面 `asyncio.wait_for` 超时，不传入 agent |
| `message_mode` | enum | `followup` | — | — | Consumer 路由策略（排队/拒绝），不传入 agent |
| `models` | object | null | — | `models` | cfgpu 工具模型偏好（生图/生视频），存入 context 供 middleware 注入 LLM prompt |
| `subagent_enabled` | bool | — | `subagent_enabled`（有值时） | `subagent_enabled`（有值时） | 加载 `task_tool`；启用 `SubagentLimitMiddleware` |
| `reasoning_effort` | string | null | `reasoning_effort`（有值时） | `reasoning_effort`（有值时） | 传给 `create_chat_model` 控制推理力度 |

> `subagent_enabled` 和 `reasoning_effort` 无 config.yaml 全局开关，仅当 MQ 消息显式设置时透传。

### 消息信封字段（非 config 子对象）

| 信封字段 | configurable key | context key | make_lead_agent 用途 |
|---------|-----------------|-------------|---------------------|
| `thread_id` | `thread_id` | `thread_id` | LangGraph checkpointer 路由；middleware 读取沙盒/内存隔离 |
| `message_id`（→ run_id） | `run_id` | `run_id` | `ThreadDataMiddleware`、`TokenUsageMiddleware` 等读取 |
| `agent_name`（非 "lead_agent"） | `agent_name` | `agent_name` | 加载自定义 agent 的 `AgentConfig`（工具组/技能/审批工具列表） |
| `user_id` | — | `user_id` | `resolve_runtime_user_id` → 文件/内存隔离路径（缺失时退化为 "default"） |
| `AgentRunner._app_config` | `app_config` | `app_config` | 全局配置对象，传给所有子函数 |

### `reply_config` 字段

| 字段 | 类型 | 默认值 | 用途 |
|------|------|--------|------|
| `stream_events` | bool | `true` | 控制 `publish_result` 是否推送流式事件；`false` 时追加 `final_state` 数据 |
| `stream_event_types` | array | `["custom"]` | 过滤推送的 LangGraph stream mode（仅 `stream_events=true` 时生效）。默认只推送 custom 事件，配合 `MessageStreamMiddleware` 获得语义化输出 |

> `reply_config` 由 `MQStreamBridge` 消费，不传入 `make_lead_agent`。

---

## `make_lead_agent` 全部读取的 cfg 参数

`_get_runtime_config(config)` 将 `configurable` + `context` 合并（`context` 覆盖同名 key），以下是 `_make_lead_agent` 和 `_build_middlewares` 读取的完整列表：

### `_make_lead_agent`（agent.py L362）

| cfg key | 默认值 | 用途 |
|---------|--------|------|
| `app_config` | `get_app_config()` | 全局配置；来自 `make_lead_agent` 第一层读取 |
| `thinking_enabled` | `True` | 开启 model extended thinking；模型不支持时自动降级 |
| `reasoning_effort` | `None` | 推理力度参数，传给 `create_chat_model` |
| `model_name` / `model` | `None`（→ config.yaml 首个） | LLM 模型选择；`model` 为兼容别名 |
| `is_plan_mode` | `False` | 决定是否创建 `TodoMiddleware` |
| `subagent_enabled` | `False` | 加载 `task_tool`；传给 `SubagentLimitMiddleware` |
| `max_concurrent_subagents` | `3` | 子 agent 并发上限（MQ 协议无此字段，用默认值） |
| `is_bootstrap` | `False` | bootstrap 模式：仅挂载 `setup_agent` 工具，用于创建新 agent（MQ 协议无此字段） |
| `web_search_enabled` | `False`\* | 控制 `group="web"` 工具加载；consumer 传入默认 `True` |
| `agent_name` | `None` | 自定义 agent 名称，驱动 `load_agent_config` 和工具组/技能过滤 |

> \* agent.py 代码默认值为 `False`（安全兜底），实际运行以 consumer `_build_config` 传入值为准（默认 `True`）。

### `_build_middlewares`（agent.py L240）

| cfg key | 默认值 | 用途 |
|---------|--------|------|
| `is_plan_mode` | `False` | `TodoMiddleware` 开关 |
| `subagent_enabled` | `False` | `SubagentLimitMiddleware` 开关 |
| `max_concurrent_subagents` | `3` | `SubagentLimitMiddleware` 并发上限 |
| `ask` | `False` | `HumanApprovalMiddleware` 开关（须同时有 `agent_config.approval_required_tools`） |

---

## `context` vs `configurable` 的职责分工

| slot | 读取者 | 典型 key |
|------|--------|---------|
| `configurable` | LangGraph checkpointer、`_get_runtime_config` | `thread_id`、`run_id`、`model_name`、`thinking_enabled` |
| `context` | `runtime.context`（所有 middleware 和 Python 工具） | `thread_id`、`run_id`、`user_id`、`app_config`、`agent_name` |

两者通过 `_get_runtime_config` 合并，`context` 同名 key 覆盖 `configurable`。`_build_config` 将所有业务 key 同时写入两者，确保两条读取路径均可用。

---

## `config.models` 的处理状态

MQ 协议的 `config.models` 是复杂对象，描述 cfgpu 生图/生视频工具的模型偏好：

```json
{
  "type": "auto",
  "content": [{ "type": "image", "model_names": ["GPT Image 2 Auto"] }]
}
```

当前状态：

- `_build_config` 将其存入 `context["models"]`
- `make_lead_agent` 不读取此字段
- **缺失**：需要 middleware（如扩展 `DynamicContextMiddleware`）读取 `runtime.context["models"]`，将模型偏好以 `<system-reminder>` 形式注入 LLM prompt，引导 LLM 在构造工具调用参数时选择正确的模型名称
- cfgpu 工具为 MCP 工具（外部进程），无法直接读取 `runtime.context`，只能通过 LLM 生成的工具调用参数获得模型选择

---

## 完整 `_build_config` 输出示例

```python
RunnableConfig(
    configurable={
        "thread_id":          "t_abc123",
        "run_id":             "msg_uuid",
        "agent_name":         None,           # 或自定义 agent 名
        "thinking_enabled":   False,
        "is_plan_mode":       False,          # = thinking_enabled
        "ask":                True,
        "app_config":         <AppConfig>,
        "web_search_enabled": True,
        # 有值时才出现：
        "model_name":         "gpt-4o",
        "subagent_enabled":   False,
        "reasoning_effort":   "medium",
    },
    context={
        "thread_id":          "t_abc123",     # middleware 读取
        "run_id":             "msg_uuid",     # middleware 读取
        "app_config":         <AppConfig>,    # middleware 读取
        "user_id":            "u_xyz",        # 文件/内存隔离
        "agent_name":         None,
        "thinking_enabled":   False,
        "is_plan_mode":       False,
        "ask":                True,
        "web_search_enabled": True,
        # 有值时才出现：
        "model_name":         "gpt-4o",
        "subagent_enabled":   False,
        "reasoning_effort":   "medium",
        "models":             { "type": "auto", "content": [...] },
    },
)
```
