# MQ 执行路径：Frontend 消息类型、Gateway 路由与结果回传设计

## 一、Frontend 消息类型全景

Frontend 通过 `useStream` → LangGraph SDK 发出两种 "run" 调用：

| 调用类型 | 触发时机 | SDK 方法 | 关键参数 |
|---------|---------|---------|---------|
| **新消息** | 用户输入 | `thread.submit(input, opts)` | `input.messages: [{type:"human", content:[...]}]` |
| **HIL 恢复** | 工具审批 | `thread.submit(null, opts)` | `command: {update: {tool_approvals: {...}}}` |

运行模式通过 `context` 字段区分（不是不同的 API，而是同一 API 的参数变体）：

```
context.is_plan_mode      → TodoList middleware 激活
context.thinking_enabled  → Opus 扩展思考
context.subagent_enabled  → 子图执行
context.ask = true        → 启用 HumanApprovalMiddleware interrupt
```

LangGraph SDK 内部将 `submit()` 翻译为：
```
POST /api/threads/{thread_id}/runs/stream
Body: RunCreateRequest {
  input: { messages: [...] } | null,
  command: { update: {...} } | null,
  context: { model_name, thinking_enabled, is_plan_mode, ... },
  stream_mode: ["values", "messages-tuple", "custom", "metadata"],
  stream_subgraphs: true,
  stream_resumable: true,
  on_disconnect: "cancel" | "continue",
}
```

Frontend 消费的 SSE 事件类型：

| SSE event | 来源 | 内容 |
|-----------|-----|------|
| `metadata` | RunManager | `{run_id, thread_id}` |
| `messages` | LangGraph messages-tuple | `(AIMessageChunk, metadata)` 增量 token |
| `values` | LangGraph state | 完整 channel_values（去掉 `__pregel_*`） |
| `custom` | Middleware emit | `tool_approval_required` / `task_running` / `llm_retry` |
| `updates` | LangGraph writes | 节点级别 delta |
| `end` | 流结束标记 | `{}` |

---

## 二、现有 Gateway Direct 执行链

```
Frontend (useStream)
    │  POST /api/threads/{id}/runs/stream
    ▼
Gateway thread_runs router
    │  start_run() → RunRecord
    │  launch asyncio.create_task(run_agent(...))
    ▼
MemoryStreamBridge (内存事件队列)          ← publish 端
    │                                        ← subscribe 端
run_agent() / worker.py
    │  agent.astream() → LangGraph 执行
    │  每个 chunk → bridge.publish(run_id, event, data)
    ▼
sse_consumer() (异步生成器)
    │  从 MemoryStreamBridge 读事件
    │  format_sse(event, data) → "event: messages\ndata: {...}\n\n"
    ▼
StreamingResponse (text/event-stream)
    │
    ▼
Frontend SSE 消费 → useStream → onUpdateEvent / onCustomEvent / onLangChainEvent
```

**关键特性**：Gateway、Bridge、Worker 在同一进程内，`bridge.publish()` 是内存写，延迟极低。

---

## 三、MQ 路径的问题核心

当执行移到 MQ Consumer 后，链条断开：

```
Frontend SSE ←→ Gateway (进程 A)
                    ↓  提交到 MQ
                MQ Consumer (进程 B)
                    │  agent.astream() → LangGraph 执行
                    │  MQStreamBridge.publish() → $AGENT_RESULTS 主题
                    ▼
            $AGENT_RESULTS (MQ topic)
                                ← 谁来消费这里，转给 Frontend？
```

`MQStreamBridge`（Consumer 侧）已经实现：把每个 LangGraph chunk 封装成 MQ 消息发布到 `$AGENT_RESULTS`，消息格式为：

```json
{
  "message_id": "run-uuid",
  "type": "progress",           // "progress" | "result" | "error"
  "thread_id": "...",
  "agent_name": "director",
  "payload": {
    "event_type": "messages",   // LangGraph stream 事件类型
    "data": { ... }
  },
  "seq": 42
}
```

终止消息：
```json
{ "type": "result",  "payload": { "status": "success", "final_state": {...} } }
{ "type": "error",   "payload": { "code": "AGENT_TIMEOUT", "retriable": true } }
```

**问题就是：这些 MQ 消息，谁来把它们交付给 Frontend？**

---

## 四、三种方案对比

### 方案 A：Gateway 作 MQ↔SSE 桥（推荐）

```
Frontend SSE  ←───────────────────────────── Gateway (进程 A)
                                                 │  POST → $AGENT_TASKS
                                                 │  订阅 $AGENT_RESULTS (by run_id)
                                               MQ
                                                 │  MQ Consumer (进程 B)
                                                 │  agent.astream()
                                                 └→ $AGENT_RESULTS
```

**工作方式**：
1. Gateway 接受 `POST /runs/stream`，将 `RunCreateRequest` 转译为 `TaskMessage` 发送到 `$AGENT_TASKS`
2. Gateway 同时订阅 `$AGENT_RESULTS`，按 `run_id`/`message_id` 过滤
3. 收到 MQ progress 消息 → 格式化成 SSE → 写入现有 `StreamingResponse`
4. 收到 MQ `result`/`error` 消息 → 发送 `end` SSE → 关闭连接

**可复用的现有代码**：
- `sse_consumer()` 的 SSE 格式化和连接管理逻辑 → 可基本复用
- `RunCreateRequest` → `TaskMessage` 的字段映射 → 直接对照
- `format_sse()` → 直接复用
- `StreamBridge` 抽象 → 新增 `MQBackedGatewayBridge` 实现

**新增代码**：

```python
# 新增：backend/app/gateway/mq_bridge.py
class MQBackedGatewayBridge:
    """Gateway 侧桥：提交到 MQ + 订阅 $AGENT_RESULTS 转 SSE"""

    async def submit(self, task_message: TaskMessage) -> None:
        # 发布到 $AGENT_TASKS
        await self._producer.send(AGENT_TASKS_TOPIC, task_message.to_dict())

    async def subscribe_sse(
        self,
        run_id: str,
        disconnect_event: asyncio.Event,
    ) -> AsyncIterator[str]:
        # 订阅 $AGENT_RESULTS，按 message_id == run_id 过滤
        # 将 progress 消息格式化成 SSE 帧
        # 收到 result/error 时 yield end 帧并返回
        async for mq_msg in self._consumer.consume(AGENT_RESULTS_TOPIC):
            if mq_msg["message_id"] != run_id:
                continue
            if mq_msg["type"] == "progress":
                yield format_sse(
                    mq_msg["payload"]["event_type"],
                    mq_msg["payload"]["data"]
                )
            elif mq_msg["type"] in ("result", "error"):
                yield format_sse("end", {})
                return
            if disconnect_event.is_set():
                return
```

```python
# 修改：backend/app/gateway/routers/thread_runs.py
# 在 stream_run() 中，根据配置选择执行路径

async def stream_run(...):
    if app_config.execution_mode == "mq":
        # MQ 路径
        task_msg = build_task_message(thread_id, body, request)
        await mq_bridge.submit(task_msg)
        return StreamingResponse(
            mq_bridge.subscribe_sse(task_msg.message_id, disconnect_event),
            media_type="text/event-stream",
        )
    else:
        # 现有 Direct 路径（保持不变）
        record = await start_run(body, thread_id, request)
        return StreamingResponse(
            sse_consumer(bridge, record, request, run_mgr),
            media_type="text/event-stream",
        )
```

**优点**：Frontend 零改动；SSE 协议不变；逐步灰度（按 agent_name/用户切换）
**缺点**：Gateway 需要订阅 $AGENT_RESULTS，需要处理 MQ Consumer Group 的 topic 过滤（或每个 run_id 独立 subscription）

---

### 方案 B：专用 Agent Result Consumer 服务 + WebSocket

```
Frontend WebSocket ←→ Result Delivery Service
                           │  订阅 $AGENT_RESULTS (全量)
                           │  按 user_id / session_id 路由给对应 WS 连接
                         MQ
                           │
                       MQ Consumer
```

**工作方式**：
1. Frontend 通过 WS 连接 Result Delivery Service，携带认证 token
2. Frontend 向 Gateway 提交任务（REST POST，返回 `run_id`），Gateway 写到 MQ
3. Result Delivery Service 消费全量 `$AGENT_RESULTS`，按 `user_id`/`run_id` 路由到对应 WS 连接
4. Frontend WS 收到事件，适配现有 `useStream` hook

**可复用的现有代码**：
- `MQStreamBridge` 的消息格式 → 直接用
- SSE event type 到 WS message type 映射 → 需新写适配层

**新增代码量**：
- 新的 Result Delivery Service（独立进程）
- Frontend WS 客户端（替换或包装 `useStream`）
- 认证/路由逻辑

**优点**：关注点分离；Gateway 完全无状态；支持断线重连（WS + seq 号）
**缺点**：新增服务运维成本；Frontend 需要改用 WS；开发量较大

---

### 方案 C：Frontend 轮询（最简但无流式）

```
Frontend
  │  POST /api/threads/{id}/runs  → 返回 { run_id }
  │  loop: GET /api/threads/{id}/runs/{run_id}/events?after_seq=N
  ▼
Gateway 查 $AGENT_RESULTS 缓存（ProcessedMessageRow.result_cache 已有）
```

MQ Consumer 已经将 events 缓存在 `result_cache` 里（`ProcessedMessageRow`）。Gateway 可以读 DB 按 `run_id + seq` 分页返回事件。

**优点**：无 MQ 订阅问题；Frontend 实现简单；天然支持断线重连
**缺点**：有轮询延迟（建议 500ms~1s）；不是真正的流式；高频轮询增加 DB 压力

---

## 五、推荐方案与实现路径

**推荐：方案 A（Gateway MQ↔SSE 桥）**，理由：
1. Frontend 不需要改动（继续用 LangGraph SDK `useStream`，SSE 协议不变）
2. 最大复用现有 Gateway 代码（SSE 格式化、认证、错误处理）
3. 开发量最小，可与现有 Direct 模式并存

### 实现分层

```
backend/app/gateway/
  mq_bridge.py               ← 新增：MQBackedGatewayBridge
  routers/thread_runs.py     ← 修改：按 execution_mode 分支

backend/packages/harness/deerflow/runtime/
  bridge/
    base.py                  ← 现有抽象（StreamBridge）
    memory.py                ← 现有 MemoryStreamBridge
    mq_consumer.py           ← 现有 MQStreamBridge（Consumer 侧发布）
    mq_gateway.py            ← 新增：Gateway 侧订阅 $AGENT_RESULTS
```

### MQ 订阅的实现选择

`$AGENT_RESULTS` 是广播 topic，多个 Gateway 实例同时订阅会有消息分片问题。两种解法：

**解法 1：每个 run_id 用独立 Consumer Group tag（推荐）**
- RocketMQ 支持 tag 过滤：Consumer Group `gateway-{run_id}` 订阅 `$AGENT_RESULTS` with tag `run_id`
- 消费完成后 Unsubscribe（不积累 Consumer Group）

**解法 2：Gateway 内部广播（适合小规模）**
- 所有 Gateway 实例都订阅 `$AGENT_RESULTS`
- 每个实例只处理本实例正在等待的 `run_id`（其他消息 nack/ignore）
- 利用现有 `MemoryStreamBridge` 作为进程内 fan-out

### 与 HIL（Human-in-the-Loop）的适配

Consumer 已经处理 HIL：检测 `task.interrupts` → 发布 `type=result, status=paused_for_approval` + `custom: tool_approval_required`。

Gateway 收到 `paused_for_approval` result 时：
- 发送 `custom` SSE 事件（工具审批请求）→ Frontend 已有 `onCustomEvent` 处理
- 不关闭 SSE 连接（等待下一次 `command` 提交）
- Frontend 用户审批后，`submit(null, {command: {update: {tool_approvals: ...}}})` 重新触发 `POST /runs/stream`，Gateway 再次向 MQ 提交（`is_resume=true` 的 TaskMessage）

---

## 六、关键问题决策

| 问题 | 结论 |
|------|------|
| Frontend 是否需要改动？ | 方案 A 下**不需要**，SSE + LangGraph SDK 复用 |
| 是否需要独立的 Agent Result Consumer 服务？ | 不需要，Gateway 自己订阅 $AGENT_RESULTS |
| 能否复用 Gateway 代码？ | 能复用：SSE 格式化、认证、错误处理、StreamBridge 抽象 |
| 新增代码核心是什么？ | Gateway 侧 MQ 订阅逻辑（`MQBackedGatewayBridge`）+ `RunCreateRequest → TaskMessage` 转译 |
| Direct 和 MQ 路径能并存吗？ | 能，通过 `config.execution_mode` 或 `agent_name` 灰度切换 |
| HIL 如何处理？ | Consumer 发 `paused_for_approval`，Gateway 保持 SSE 开放，下次 resume 再提交 MQ |
