DeerFlow Consumer Instance 与外部消息队列 RocketMQ 之间的消息协议（version：2.4）

## 概述

RocketMQ 的 topic 管理、消费者组、权限配置由上游系统负责；本协议只定义消息的格式、类型语义和交互流程。

| 方向 | Topic | 说明 |
|------|-------|------|
| 上游 → 智能体服务 | `$AGENT_TASKS` | 任务消息：task（含 HIL resume）（inject 已废弃） |
| 上游 → 智能体服务 | `$AGENT_SIGNALS` | 控制信号：cancel / ping |
| 智能体服务 → 上游 | `$AGENT_RESULTS` | 结果回调：progress / result / error / pong 消息 |

**Topic 拆分设计**：`$AGENT_TASKS` 与 `$AGENT_SIGNALS` 分离，使 Consumer 可以在任务槽位全满（semaphore 耗尽）时继续消费 cancel/ping 信号，而无需对 task topic 的 poll 设置"最小拉取 1 条"的 workaround。cancel 响应延迟不受 task 并发负载影响。

**Topic 类型**：三个 topic 均使用**普通消息**（非顺序消息）。同一 thread_id 的路由亲和性由 Consumer 服务内部的运行状态表保证，详见《Consumer 运行管理设计》。

---

## 通用消息信封（Message Envelope）

所有消息共享同一信封结构，`type` 字段区分消息类型，`payload` 携带具体内容。

```json
{
  "schema_version": "2.4",
  "message_id":     "uuid-v4",
  "message_seq":    0,
  "timestamp":      "2026-05-06T08:00:00.000Z",
  "type":           "task | cancel | ping | progress | result | error | pong",
  "payload":        { },
  "thread_id":      "t_abc123",
  "agent_name":     "lead_agent",
  "user_id":        "u_xyz",
  "project_id":     "p_proj01"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| schema_version | string | 是 | 当前版本 "2.4"，协议变更时递增 |
| message_id | string(uuid) | 是 | 发送方生成，全局唯一。同一任务的上行/下行保持一致 |
| message_seq | int | 下行时 | 下行回复消息流的顺序号，从 0 开始递增，用于接收端排序和丢包检测（per message_id） |
| timestamp | string(ISO8601) | 是 | 消息产生时间，UTC 时区 |
| type | enum | 是 | 消息类型，见下文 |
| payload | object | 是 | 消息体，根据 type 不同结构不同 |
| thread_id | string(uuid) | 是 | 会话标识，全局唯一，由客户端负责生成 |
| agent_name | string | 否 | 目标 DeerFlow agent（对应 `agents/` 目录），缺省使用 lead_agent |
| user_id | string(uuid) | 否 | 操作用户 ID |
| project_id | string(uuid) | 否 | 所属项目 ID，null 表示无项目上下文。见「project_id 语义」 |

**注意**：thread_id、user_id、project_id 是 DeerFlow server 全局唯一标识，由客户端系统保证唯一性。

---

## 上行消息（上游 → 智能体服务）

### $AGENT_TASKS — 任务消息

| 消息类型 | 前置条件 | 说明 |
|----------|----------|------|
| task | 无 | 发起新的 agent 执行轮次，或多轮对话的后续用户消息。当同 thread_id 有 task 正在执行时，新消息自动转为上下文注入（inject 语义），在下一个 LangGraph step 前注入。`payload.command` 非空时为 HIL resume 消息 |

### $AGENT_SIGNALS — 控制信号

| 消息类型 | 前置条件 | 说明 |
|----------|----------|------|
| cancel | 同 thread_id 有运行中 task | 取消当前执行；无运行中 task 时忽略 |
| ping | 无 | 健康检查；Consumer Instance 回复 pong |

**Consumer 对两个上行 topic 使用独立的 SimpleConsumer 实例**：`$AGENT_SIGNALS` 的 poll 不受任务并发槽位限制，始终可消费；`$AGENT_TASKS` 的 poll 在槽位耗尽时可暂停拉取，不再需要强制 capacity≥1。

### task — 任务消息

task 消息有两种用途：**普通任务**和 **HIL resume（工具确认回复）**，通过 `messages` 与 `command` 字段区分，两者互斥。

#### 普通任务（messages 非空）

```json
{
  "messages": [
    {
      "role": "user",
      "content": [
        { "type": "text", "text": "帮我分析这份财务报表" },
        { "type": "image_url",    "url": ["https://cdn.example.com/chart.png"] },
        { "type": "document_url", "url": ["https://cdn.example.com/report.pdf"] },
        { "type": "audio_url",    "url": ["https://cdn.example.com/recording.mp3"] },
        { "type": "video_url",    "url": ["https://cdn.example.com/demo.mp4"] }
      ]
    }
  ],
  "command": null,

  "config": {
    "models": {
      "type": "auto",
      "content": [{ "type": "image", "model_names": ["GPT Image 2 Auto"] }]
    },
    "thinking_enabled":   false,
    "web_search_enabled": false,
    "timeout_seconds":    300,
    "ask":                true,
    "message_mode":       "followup",
    "subagent_enabled":  false,
    "reasoning_effort":  false
  },

  "reply_config": {
    "stream_events":      true,
    "stream_event_types": ["custom"]
  }
}
```

#### HIL resume — 工具确认回复（command 非空）

`config.ask:true`时，当 agent 调用高花费工具（如生图、生视频）前暂停等待用户确认时，客户端在用户操作后发送 resume 消息。`messages` 字段为 null，`command.update.tool_approvals` 携带批量用户决策（与审批事件中的 `tool_calls[].id` 一一对应）。

**完整示例（部分通过、部分拒绝）**：

```json
{
  "messages": null,
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
          "reason": "视频暂不需要，只生成图片"
        }
      }
    }
  },
  "config": { "thinking_enabled": false },
  "reply_config": {
    "stream_events":      true,
    "stream_event_types": ["custom"]
  }
}
```

**全部通过（参数不修改）**：

```json
{
  "messages": null,
  "command": {
    "update": {
      "tool_approvals": {
        "call_abc123": { "status": "approved" },
        "call_def456": { "status": "approved" }
      }
    }
  }
}
```

**全部拒绝**：

```json
{
  "messages": null,
  "command": {
    "update": {
      "tool_approvals": {
        "call_abc123": { "status": "rejected", "reason": "暂不执行" },
        "call_def456": { "status": "rejected", "reason": "暂不执行" }
      }
    }
  }
}
```

**`command.update.tool_approvals` 字段说明**：

key 为审批事件中的 `tool_calls[].id`（`tool_call_id`），value 为该工具调用的决策：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| status | enum | 是 | `"approved"` = 确认执行，`"rejected"` = 取消该工具调用 |
| args | object | 否 | 用户确认（可修改）后的工具参数，完整覆盖原参数；缺省时沿用 LLM 原始参数 |
| reason | string | 否 | 拒绝原因，仅 status=rejected 时有效；注入 agent 上下文供 LLM 感知 |

每次 resume 须为**审批事件中所有 `tool_calls`** 提供决策（key 覆盖所有 id），否则 agent 会再次触发审批暂停。

**路由语义**：resume 消息到达时，若 thread 处于 `paused_for_approval` 状态，Consumer 直接 claim 并执行恢复；若 thread 处于 `running`（意外重复投递），走 inject 路径（Consumer 内部处理）。

**config 字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| models | object | 模型选择策略，每条消息实时加载，无需重启生效 |
| thinking_enabled | bool | 是否开启扩展思考模式 |
| web_search_enabled | bool | 是否启用网络搜索 |
| timeout_seconds | int | agent 执行超时时间（秒）。超时触发 AGENT_TIMEOUT error。可选，缺省无限制 |
| ask | bool | 是否对 agent 的高风险高花费工具进行 HIL 中断 |
| message_mode | enum | 当 thread 正在运行时，本消息的处理方式。`followup`（默认）：排队，当前 run 完成后作为独立新一轮执行；`reject`：拒绝并返回 AGENT_BUSY error；`steer`（协议预留，待实现）：注入当前执行，当前版本降级为 followup。详见下方「message_mode 语义」|

**reply_config 字段说明**：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| stream_events | bool | **true** | 是否推送 progress 流式事件 |
| stream_event_types | array | **["custom"]** | 推送的 LangGraph stream_mode 事件类型过滤。仅在 stream_events=true 时有效。推荐只使用 `"custom"`（详见下方「推荐的 stream 模式」） |

**推荐的 stream 模式**：

Consumer 部署了 `MessageStreamMiddleware`，在 LLM 调用完成后 emit `ai_message` custom 事件，在每个工具调用完成后 emit `tool_result` custom 事件。配合 `stream_event_types=["custom"]`，客户端可获得所有需要的 Agent 输出，同时避免接收 LangGraph 自动产生的 values 全量 state 快照和 messages token 级别 chunk（两者均含有 middleware 内部产生的系统消息，噪音较多）。

| stream_event_types 配置 | 客户端收到 | 推荐场景 |
|------------------------|-----------|---------|
| `["custom"]` | ai_message、tool_result、tool_approval_required、subagent task 事件等 custom 类型 | **推荐**，配合 MessageStreamMiddleware 使用 |
| `["messages", "custom"]` | LangGraph token chunk + custom 事件 | 需要 token 级别流式文字（如实时打字效果）的场景 |
| `["values", "custom"]` | 每步 state 全量快照 + custom 事件 | 调试、需要 state 全量数据的场景 |

**message_mode 语义**：

当同一 thread_id 有 task 正在执行时，新到达 task 消息的处理方式由 `config.message_mode` 决定：

| message_mode | 行为 | 状态 |
|-------------|------|------|
| `followup`（**默认**） | 写入排队队列，立即 MQ ack；当前 run 完成后 Consumer 以本消息的 `messages` 为 input 发起新一轮执行 | 已实现 |
| `reject` | 拒绝，立即推送 `error(AGENT_BUSY, retriable=true)`，不排队，MQ ack | 已实现 |
| `steer` | 协议预留。注入当前执行上下文，在下一个 LangGraph node 边界或模型调用前生效。**当前版本降级为 followup**，不报错 | **待实现** |

**客户端职责**：`debounce`（防抖）和多消息合并（collect）**由客户端/上游负责**，Consumer 不做消息批处理。若需发送多条关联消息，应在单条 task 的 `messages` 数组中打包。

详见《Consumer 运行管理设计 — message_mode 与并发行为》。

**URL 说明**：所有 content 中的 URL 须公网可访问，Consumer 不预下载（URL 直接传入 agent 上下文）。

### cancel — 取消消息（topic: $AGENT_SIGNALS）

取消指定 thread 当前正在运行的 task。若当前无运行中 task，忽略此消息。

```json
{
  "reason": "user_requested"
}
```

| reason 值 | 说明 |
|-----------|------|
| user_requested | 用户主动取消 |
| timeout | 客户端侧超时触发取消 |
| admin | 管理员操作 |

**路由**：cancel 消息携带 thread_id（信封字段），Consumer 服务通过运行状态表定位执行该 thread 的 Consumer 实例并传递取消信号。若接收消息的 Consumer 不是执行实例，由运行状态表中的取消信号通道转发。

### ping — 健康检查（topic: $AGENT_SIGNALS）

Consumer Instance 收到 ping 后立即回复 pong，用于上游探活。

```json
{
  "instance_id": "consumer-hostname-pid"
}
```

`instance_id` 可选。指定时只有匹配的实例回复；不指定时任意可用实例回复。Consumer 实例 ID 格式及注册机制见《Consumer 运行管理设计 — 实例管理》。

---

## 下行消息（智能体服务 → 上游，topic: $AGENT_RESULTS）

### progress — 流式进度事件

仅当 task 消息的 `reply_config.stream_events=true` 时推送。每个 LangGraph 执行步骤产生一条或多条 progress 消息，上游可实时展示 agent 思考/输出过程。

**推荐使用 `stream_event_types=["custom"]`**，配合 `MessageStreamMiddleware` 接收语义化 custom 事件（ai_message、tool_result 等）。`messages` 和 `values` 模式仍支持，用于兼容或调试场景。

**progress payload 结构**（`message_seq` 在信封层）：

```json
{
  "event_type": "messages | values | custom",
  "data": { }
}
```

#### messages 事件 data 结构

对应 LangGraph `stream_mode=messages` 的 `[chunk, metadata]` 二元组（token 级别流式输出，`stream_event_types` 含 `"messages"` 时下行）：

```json
{
  "event_type": "messages",
  "data": {
    "chunk": {
      "type": "AIMessageChunk",
      "id": "message_id_0",
      "content": "根据搜索结果，",
      "tool_call_chunks": [],
      "additional_kwargs": {}
    },
    "metadata": {
      "langgraph_node": "researcher",
      "langgraph_step": 4,
      "ls_model_name": "claude-3-5-sonnet"
    }
  }
}
```

#### values 事件 data 结构

`values` 模式输出当前 graph 的完整 state snapshot，`stream_event_types` 含 `"values"` 时下行（调试场景）：

```json
{
  "event_type": "values",
  "data": {
    "title": "AI发展趋势分析",
    "messages": [ "..." ],
    "artifacts": [{ "type": "report", "title": "AI发展趋势报告", "content": "..." }]
  }
}
```

#### custom 事件

data 由 agent 自定义，通过 LangGraph `stream_writer` 显式 emit。`stream_event_types` 含 `"custom"`（推荐）时下行。目前有五类固定 schema：

**AI 回复**（`MessageStreamMiddleware` 在每次 LLM 调用完成后 emit）：

```json
{
  "event_type": "custom",
  "data": {
    "type": "ai_message",
    "message_id": "msg_01Abc123",
    "content": "根据你的描述，我来帮你生成这张图片。",
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
}
```

| 字段 | 说明 |
|------|------|
| type | 固定值 `"ai_message"` |
| message_id | AIMessage 的 ID，用于客户端去重和与 tool_result 关联 |
| content | AI 回复文字，纯工具调用轮次可为空字符串 |
| tool_calls | 本轮工具调用列表，无工具调用时为 `[]` |
| tool_calls[].id | 工具调用 ID，与后续 `tool_result.tool_call_id` 对应 |
| tool_calls[].args | 模型生成的原始工具参数（审批前）。用户通过 HIL resume 修改的参数不回写此字段 |

`content` 和 `tool_calls` 均为空时不 emit（空 AIMessage 跳过）。

---

**工具执行结果**（`MessageStreamMiddleware` 在每个工具调用完成后 emit）：

正常完成：

```json
{
  "event_type": "custom",
  "data": {
    "type": "tool_result",
    "message_id": "msg_tool_uuid",
    "tool_call_id": "call_abc123",
    "name": "cfgpu__generate_image",
    "content": "{\"image_url\": \"https://cdn.example.com/output.png\", \"width\": 1024, \"height\": 1024}",
    "status": "success"
  }
}
```

工具执行失败：

```json
{
  "event_type": "custom",
  "data": {
    "type": "tool_result",
    "message_id": "msg_tool_uuid2",
    "tool_call_id": "call_def456",
    "name": "bash",
    "content": "Command failed: Permission denied",
    "status": "error"
  }
}
```

| 字段 | 说明 |
|------|------|
| type | 固定值 `"tool_result"` |
| message_id | ToolMessage 的 ID |
| tool_call_id | 对应 `ai_message.tool_calls[].id` |
| name | 工具名 |
| content | 工具输出。超出 4096 字符时截断并追加 `[truncated: N chars omitted]` |
| status | `"success"` 或 `"error"` |

**注意**：HIL 拒绝的工具调用由 `HumanApprovalMiddleware` 人工注入 error ToolMessage，不经过 `wrap_tool_call`，因此不产生 `tool_result` 事件。客户端通过 `tool_approval_required` 事件中对应 `id` 的拒绝决策感知。

---

**HIL 工具审批请求**（agent 在调用高花费工具前暂停，等待用户确认）：

同一 AI Message 产生的所有待审批工具调用**合并在一个事件**里（批量审批，单次 interrupt）：

```json
{
  "event_type": "custom",
  "data": {
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
}
```

| 字段 | 说明 |
|------|------|
| type | 固定值 `"tool_approval_required"` |
| tool_calls | 待审批工具调用列表，同一 AI Message 的所有待审批工具合并在此 |
| tool_calls[].id | 工具调用 ID（`tool_call_id`），resume 消息中的 key |
| tool_calls[].name | 工具名 |
| tool_calls[].args | 当前参数，客户端展示给用户并允许修改 |

此事件发出后，紧跟 `result {status: "paused_for_approval"}`，stream 结束。客户端需为 `tool_calls` 中的**每一项**提供决策，在用户操作后发送 HIL resume task 消息（见上文）。

**系统 Warning**（Consumer 检测到协议使用错误时推送，面向开发者）：

```json
{
  "event_type": "custom",
  "data": {
    "type": "warning",
    "code": "HIL_ASK_REQUIRED",
    "message": "HIL resume 消息必须设置 config.ask=true；本次已自动修正，请检查客户端实现"
  }
}
```

| 字段 | 说明 |
|------|------|
| type | 固定值 `"warning"` |
| code | 错误码，标识具体问题 |
| message | 人类可读描述 |

目前定义的 warning code：

| code | 触发条件 | Consumer 行为 |
|------|----------|--------------|
| `HIL_ASK_REQUIRED` | command（HIL resume）消息未设置 `config.ask=true` | 自动注入 `ask=true` 后继续处理，resume 正常完成 |

Warning 事件在 result 之前推送。客户端应在开发阶段监听并修复，不影响当次 resume 的正常完成。

**其他 custom 事件**：data schema 不固定，按具体业务定义。

---

### result — 最终结果

每个 task 有且仅有一条 result 消息（agent 执行结束后推送，同时触发 MQ ack）。

**status 枚举**：

| status | 说明 |
|--------|------|
| `success` | agent 正常完成 |
| `cancelled` | 被 cancel 消息中止 |
| `paused_for_approval` | agent 在调用高花费工具前暂停，等待用户 HIL resume（v2.2 新增） |

**正常完成（stream_events=true）**：

```json
{
  "status": "success"
}
```

`stream_events=true` 时，执行过程中的所有 AI 回复和工具结果已通过 `ai_message` / `tool_result` custom 事件实时下行，result 不再重复携带 `final_state`。

**正常完成（stream_events=false）**：

```json
{
  "status": "success",
  "final_state": {
    "title": "AI发展趋势分析",
    "messages": [
      { "type": "human", "id": "...", "content": "用户本次输入" },
      { "type": "ai",    "id": "...", "content": "本次回答..." }
    ],
    "artifacts": [...]
  }
}
```

`stream_events=false` 时客户端无法从 progress 获取内容，result 附带 `final_state` 供客户端展示。

> **`final_state` 字段语义说明**（仅 stream_events=false 场景）
>
> - `messages`：**仅含本次 run 新增的消息**，通过 run 前后消息 ID 差集计算得出。若本次 run 触发了历史压缩（summarization），增量中会包含新生成的 summary 消息（新 ID）和本次 run 的正常回复，不含被保留的历史消息（原 ID 已存在）。客户端可将其直接追加到本地消息列表。
> - `title`、`artifacts` 等其他字段：thread **当前全量状态**（非增量），反映本次 run 结束后整个 thread 的最新值。客户端应用这些字段时直接替换本地对应字段，而非追加。

**HIL 暂停时**（status=paused_for_approval，无 usage，无 final_state）：

```json
{
  "status": "paused_for_approval"
}
```

客户端收到此 result 前，会先收到 `progress custom {type:"tool_approval_required"}` 事件携带待审批工具调用信息；result 作为 HIL 流程的终止信号，客户端据此展示参数确认界面，并在用户操作后发送 HIL resume task 消息。

---

### error — 错误通知

task 执行过程中发生不可恢复的错误时推送。同一 task 必定有 error 或 result，不会同时存在两者。

```json
{
  "error": {
    "code":      "AGENT_TIMEOUT | TOOL_FAILED | QUOTA_EXCEEDED | INTERNAL_ERROR | AGENT_BUSY",
    "message":   "Agent execution timed out after 300s",
    "retriable": true,
    "node":      "researcher"
  }
}
```

| 错误码 | 说明 |
|--------|------|
| AGENT_TIMEOUT | 执行超出 config.timeout_seconds |
| TOOL_FAILED | 工具调用失败且无法恢复 |
| QUOTA_EXCEEDED | 模型配额超限 |
| INTERNAL_ERROR | Consumer 内部错误 |
| AGENT_BUSY | thread 正在运行且 `message_mode=reject`，消息被拒绝；retriable=true，客户端可在稍后重发或改用 followup |

**retriable**：建议值，指示该错误是否值得重试。是否实际重试由客户端决定，Consumer 不自动重试。

---

### pong — 健康检查回复

```json
{ "instance_id": "consumer-hostname-12345" }
```

---

## 附录

### thread_id 生命周期

thread_id 由客户端生成并保证全局唯一（推荐 UUID v4）。DeerFlow Consumer 将 thread_id 视为不透明标识符，用于：
- LangGraph checkpointer 状态键（对话历史持久化）
- 沙箱工作目录路径（`$DEER_FLOW_HOME/threads/{thread_id}/`）
- 运行状态路由依据

**Consumer 不负责 thread 的创建和删除**，也不维护 thread 的元数据（如创建时间、所属用户列表）。thread 数据的清理由外部运维决定。

### project_id 语义

project_id 用于**多层 memory 扩展**：一个 project 可包含多个 threads，一个 thread 也可被多个用户访问。project 级别的 memory 系统在各 thread 执行过程中积累 project 范围的知识，并注入 agent 上下文。

project_id 为 null 时，仅使用 user 级别和 thread 级别的 memory。

### 超时配置

timeout_seconds 在 task 的 `config` 中由客户端按需设置。Consumer 内部使用 asyncio timeout 控制执行时长，超时后发送 `error(AGENT_TIMEOUT)`，并完成 MQ ack。

### inject 消息（deprecated v2.1）

`type=inject` 的独立消息类型已废弃。`message_mode=steer` 为该能力的协议预留字段，当前版本尚未实现（降级为 followup）。待实现后，Consumer 将通过 InjectMiddleware 在下一个 node 边界或模型调用前注入，见《Consumer 运行管理设计 — Inject 机制》。
