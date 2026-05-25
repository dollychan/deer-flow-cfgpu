# Consumer 实现计划

基于《Consumer 运行管理设计》和《MQ 消息协议 v2.3》的具体实现指南。

---

## 文件布局

```
backend/
├── app/consumer/                          ← 应用入口层（与 app/gateway/ 并列）
│   ├── __init__.py
│   ├── __main__.py                        ✅ 已完成
│   ├── constants.py                       ✅ 已完成（StrEnum 常量集中定义）
│   ├── schemas.py                         ✅ 已完成
│   ├── task_consumer.py                   ✅ 已完成
│   ├── agent_runner.py                    ✅ 已完成
│   ├── run_registry.py                    ✅ 已完成
│   └── stream_bridge/
│       └── mq.py                          ✅ 已完成
│
└── packages/harness/deerflow/
    ├── runtime/runs/
    │   ├── worker.py                      ← 现有（gateway 模式，不修改）
    │   ├── manager.py                     ← 现有（gateway 模式，不修改）
    │   └── models.py                      ✅ 已完成（5 张 ORM 表）
    └── agents/middlewares/
        └── inject.py                      ← InjectMiddleware（steer 待实现）
```

---

## 实现步骤

### 步骤 1 — schemas.py ✅ 已完成

`app/consumer/schemas.py`

**内容**：RocketMQ 消息信封（v2.3）反序列化 + schema 校验。

```python
UPLINK_TYPES: frozenset[str]   # {"task", "cancel", "ping"} — 合法的上行消息类型

class SchemaValidationError(ValueError):
    reason: str   # 人类可读的校验失败原因，直接用于 INVALID_SCHEMA error 回发

ContentItem          # 单块内容（text / image_url / document_url / ...）
UserMessage          # role + content list-or-str
ReplyConfig          # stream_events, stream_event_types
TaskMessage          # 完整信封 + 校验 + from_json() / from_dict() 工厂
  .message_mode      # → config.message_mode，默认 "followup"
  .is_resume         # command 非空 → True（HIL resume）
  .timeout_seconds   # → config.timeout_seconds，无则 None
  .user_id           # 信封 user_id，用于 context 注入
  .agent_name        # 信封 agent_name，用于 context 注入
  ._validate_raw(data)         # 信封级校验（classmethod）：
                               #   schema_version 主版本必须为 "2"（若存在）
                               #   message_id / type / thread_id / payload 必填非空
                               #   type 必须在 UPLINK_TYPES 中
  ._validate_task_payload(payload)  # task 专属校验（classmethod）：
                               #   messages 与 command 二选一（互斥且不能同时为 null）
                               #   messages：非空数组，每项含 role + content
                               #   command：含 update.tool_approvals（dict）
```

**校验时机**：`from_dict()` / `from_json()` 在构造对象前调用 `_validate_raw()`，校验失败抛出 `SchemaValidationError`（不再抛 `KeyError`）。`cancel` 和 `ping` 类型只做信封级校验，无额外 payload 约束。

---

### 步骤 2 — models.py ✅ 已完成

`deerflow/runtime/runs/models.py`

**内容**：5 张 ORM 表，继承 `deerflow.persistence.base.Base`，随 `Base.metadata.create_all` 自动建表。

| 表 | 用途 |
|----|------|
| `consumer_instances` | Consumer 进程注册 + heartbeat |
| `thread_run_state` | 路由状态（IDLE / RUNNING，`SELECT FOR UPDATE`） |
| `thread_msg_queue` | followup 消息队列 + current 崩溃恢复行（policy 列区分） |
| `thread_cancel_signals` | cancel 信号，任意实例写入，runner 轮询 |
| `processed_messages` | 幂等日志 + result 缓存（含 PAUSED_FOR_APPROVAL 状态） |

**重要设计决定**：`thread_run_state.status` 只有 `idle` / `running` 两种状态，**不含 `paused_for_approval`**。HIL 暂停时 thread 立即回到 `idle`，PAUSED_FOR_APPROVAL 仅记录在 `processed_messages`。resume 消息到来时正常 claim，无需特判。

**注意**：`__main__.py` 启动时必须先 `import deerflow.runtime.runs.models`，再调用 `init_engine_from_config()`，使这 5 张表注册进 `Base.metadata`。

---

### 步骤 3 — constants.py ✅ 已完成

`app/consumer/constants.py`

**内容**：所有 StrEnum 常量集中定义，避免跨文件魔法字符串。

```python
class ThreadStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"

class InstanceStatus(StrEnum):
    ACTIVE = "active"
    DRAINING = "draining"
    DEAD = "dead"

class QueuePolicy(StrEnum):
    CURRENT = "current"    # 崩溃恢复行（每线程最多 1 条）
    FOLLOWUP = "followup"  # 排队等待执行的后续消息
    STEER = "steer"        # 协议预留

class ProcessedStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED_FOR_APPROVAL = "paused_for_approval"

class MessageMode(StrEnum):
    FOLLOWUP = "followup"
    STEER = "steer"
    REJECT = "reject"

class ClaimResult(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
```

---

### 步骤 4 — run_registry.py ✅ 已完成

`app/consumer/run_registry.py`

**内容**：封装所有 DB 操作，通过注入的 `async_sessionmaker` 获取 session。

```python
class RunRegistry:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession])

    # 实例管理
    async def register_instance(instance_id, hostname, pid) -> None
    async def heartbeat_instance(instance_id) -> None
    async def mark_instance_draining(instance_id) -> None
    async def delete_instance(instance_id) -> None
    async def get_instance(instance_id) -> ConsumerInstanceRow | None

    # Thread 路由（核心）
    async def claim_thread(thread_id, instance_id, message_id)
        -> ClaimResult  # CLAIMED or RUNNING
        # SELECT FOR UPDATE → INSERT or UPDATE to RUNNING
        # 仅 idle 可被 claim（HIL pause 后 thread 已回 idle）
    async def update_thread_run(thread_id, new_message_id) -> None
    async def mark_thread_idle(thread_id) -> None
        # 同时原子删除 policy=current 的崩溃恢复行
    async def heartbeat_thread(thread_id) -> None

    # Crash 恢复行（current 策略）
    async def upsert_current_msg(thread_id, message_id, body) -> None
        # claim 成功后立即写入，供 watchdog 重试使用
    async def get_current_msg(thread_id) -> ThreadMsgQueueRow | None

    # Followup 队列
    async def enqueue_inject(thread_id, message_id, body, policy) -> None
    async def peek_inject_queue(thread_id, policy) -> list[ThreadMsgQueueRow]
    async def consume_followup(queue_id) -> None
    async def transition_thread_followup(thread_id, queue_id, new_message_id, new_body) -> None
        # 原子三步：consume followup + update thread_run + replace current_msg
        # 进程崩溃时不会丢失 followup

    # Cancel 信号
    async def insert_cancel_signal(thread_id, reason) -> None
    async def has_cancel_signal(thread_id) -> bool
    async def clear_cancel_signal(thread_id) -> None

    # 幂等
    async def check_processed(message_id) -> ProcessedMessageRow | None
    async def mark_processed(message_id, thread_id, status, result_cache) -> None

    # Stale run watchdog
    async def find_stale_runs(timeout_seconds=60) -> list[ThreadRunStateRow]
        # 双条件：run heartbeat 超时 AND 所属 instance heartbeat 也超时
    async def claim_stale_run(thread_id, instance_id) -> bool
        # SELECT FOR UPDATE，多 Consumer 只有一个抢到
    async def increment_retry_count(thread_id) -> None
    async def reset_retry_count(thread_id) -> None  # 正常完成后清零

    # 清理
    async def cleanup_processed_messages(ttl_days) -> int
```

**`claim_thread` 实现要点**：
```python
async with session.begin():
    row = await session.execute(
        select(ThreadRunStateRow)
        .where(ThreadRunStateRow.thread_id == thread_id)
        .with_for_update()
    )
    row = row.scalar_one_or_none()
    if row is None or row.status == ThreadStatus.IDLE:
        # INSERT or UPDATE to RUNNING
        return ClaimResult.CLAIMED
    return ClaimResult.RUNNING
```

SQLite 不支持行级锁（`FOR UPDATE` 静默忽略），单 Consumer 开发模式下行为正确；生产必须使用 postgres。

---

### 步骤 5 — stream_bridge/mq.py ✅ 已完成

`app/consumer/stream_bridge/mq.py`

**内容**：实现 `StreamBridge` 接口，把事件发布到 RocketMQ `$AGENT_RESULTS` topic。

```python
class MQProducer(Protocol):
    async def send_async(self, body: bytes, *, keys: str = "") -> None

class MQStreamBridge(StreamBridge):
    def __init__(self, producer: MQProducer, *, result_topic: str)

    # Run 上下文（AgentRunner 调用）
    def register_run(run_id, thread_id, reply_config, *,
                     agent_name="", user_id="", project_id="") -> None
    def unregister_run(run_id) -> None

    # StreamBridge 接口
    async def publish(run_id, event, data) -> None
        # 过滤规则：metadata/end 抑制；stream_events=False 抑制全部；
        # event 不在 stream_event_types 中抑制
        # 通过所有过滤后：send → 若 event=="custom"，追加到 ctx.buffered_events
    async def publish_end(run_id) -> None  # no-op
    def subscribe(...): raise NotImplementedError  # write-only

    # Custom 事件缓冲（幂等重放用）
    def get_buffered_events(run_id) -> list[dict]
        # 返回 ctx.buffered_events 的副本；AgentRunner 执行完成后调用，存入 result_cache

    # MQ 专用方法
    async def publish_result(run_id, *, status, thread_id, stream_events,
                             final_state=None, usage=None) -> None
        # stream_events=True：omit final_state；stream_events=False：include final_state
    async def publish_error(run_id, code, *, thread_id, message, retriable,
                            node=None) -> None
    async def replay(message_id, thread_id, result_cache, *,
                     agent_name="", user_id="", project_id="") -> None
        # 重复投递重放（bypass run 注册，从 result_cache 直接重发）
        # 发送顺序：
        #   1. result_cache["events"] 中的每条 custom 事件 → progress(event_type=custom)
        #   2. result_cache["tool_approval_required"] → progress(event_type=custom)（若有）
        #   3. 终止：result_cache["error"] → error 信封；否则 → result 信封
        #      result_cache["stream_events"]=False 时附带 final_state
    async def publish_pong(ping_message_id, instance_id, *,
                           target_instance_id=None, target_status=None,
                           last_heartbeat=None) -> None
        # 广播 ping：payload 仅含 instance_id
        # 定向 ping：额外含 target_instance_id / target_status / last_heartbeat
```

**序列号**：每个 run 独立计数，`register_run` 重置为 0，`publish` 每次调用自增。`replay()` 独立计数从 0 开始，不依赖 run 上下文。

---

### 步骤 6 — inject.py（预留，steer 待实现）

`deerflow/agents/middlewares/inject.py`

当前版本**不实现**。`steer` 消息收到时自动降级为 followup，不报错。

---

### 步骤 7 — agent_runner.py ✅ 已完成

`app/consumer/agent_runner.py`

**内容**：替换 `deerflow/runtime/runs/worker.py`，驱动 LangGraph，通过 MQStreamBridge 发布事件。

```python
class AgentRunner:
    def __init__(self, registry: RunRegistry, bridge: MQStreamBridge,
                 checkpointer: Any, app_config: AppConfig)

    async def run(self, message: TaskMessage) -> None:
        # 注册 run 到 bridge
        # 启动 heartbeat_loop + cancel_watcher
        # 执行 _execute（含 timeout 支持）
        # finally: unregister_run + mark_processed + reset_retry_count + _drain_and_release

    async def _execute(self, message, run_id) -> bool:
        # HIL_ASK_REQUIRED 自动修正（resume 消息未带 ask=True）
        # _build_config → _build_graph → astream
        # is_resume → Command(**message.command)
        # 否则 → _normalize_messages(message.messages)
        # aget_state() 检测 interrupt → is_paused
        # is_paused=True: 重发 tool_approval_required（防 LangGraph 丢失）
        # publish_result(PAUSED_FOR_APPROVAL) 或 publish_result("success")
        # returns True if paused, False otherwise

    async def publish_fatal_error(message_id, thread_id, message) -> None
        # watchdog 调用：无法恢复时发布 INTERNAL_ERROR(retriable=False)

    def _build_graph(runnable_config) -> CompiledStateGraph
        # make_lead_agent(config=runnable_config)
        # 注入 checkpointer

    def _build_config(message, run_id) -> RunnableConfig
        # 构造 configurable + context 两个 slot（解决 worker.py bypass 问题）
        # context 必须手动填充：thread_id, run_id, app_config, user_id,
        #   agent_name, thinking_enabled, is_plan_mode, ask,
        #   web_search_enabled, model_name, subagent_enabled, reasoning_effort, models

    async def _drain_and_release(thread_id) -> None
        # peek followup queue
        # 无 → mark_thread_idle; return
        # 有 → transition_thread_followup (原子) → create_task(self.run(next_task))

    async def trigger_drain(thread_id) -> None
        # watchdog 调用：从 run 上下文外触发 drain

    async def _heartbeat_loop(thread_id, interval=10) -> None
    async def _cancel_watcher(thread_id, runner_task, poll_interval=2) -> None
```

**关键设计**：Consumer 路径绕过 `worker.py`，`_build_config` 必须同时填充 `config["configurable"]` 和 `config["context"]`。缺少后者会导致所有 middleware 拿到空的 `runtime.context`（`thread_id`、`user_id`、`app_config` 均为 None）。

**HIL 暂停流程**：
1. `_execute` 返回 `True`（is_paused）
2. 发布 `PAUSED_FOR_APPROVAL` result
3. `finally` 块：`mark_processed(PAUSED_FOR_APPROVAL)` + `_drain_and_release`
4. `_drain_and_release` 无 followup → `mark_thread_idle`
5. thread 回到 idle，等待 resume 消息正常 claim

**辅助函数**（模块级）：
```python
def _normalize_messages(messages: list[UserMessage]) -> dict
    # 将协议 UserMessage 转为 LangChain 多模态 content block

def _append_content_block(blocks, item: ContentItem) -> None
    # text / image_url 转 LangChain block；其他 URL 类型转文本占位
```

---

### 步骤 8 — task_consumer.py ✅ 已完成

`app/consumer/task_consumer.py`

**内容**：实现路由算法（《Consumer 运行管理设计 — 4.3》）。

```python
class TaskConsumer:
    def __init__(self, registry, runner, bridge, instance_id, max_concurrent=10)
        # asyncio.Semaphore(max_concurrent)
        # _running_count: int（用于 available_slots 属性）

    @property
    def available_slots(self) -> int  # 用于 poll loop 节流

    async def handle_message(body: str | bytes) -> None:
        # 三步处理，不 raise，始终 ACK：
        # ① JSON 解析：提取 message_id / thread_id（用于后续 error 回发）
        #    失败 → log error，return（无 message_id，无法回发）
        # ② Schema 校验 + 反序列化（TaskMessage.from_dict）：
        #    SchemaValidationError → log error
        #                          → 若有 message_id：bridge.publish_error(INVALID_SCHEMA, retriable=False)
        #                          → return
        #    其他异常 → log error，return
        # ③ 按 type 分发：
        #    ping → _handle_ping
        #    cancel → registry.insert_cancel_signal
        #    task → _handle_task
        #    （_validate_raw 已确保 type 合法，无需 else 分支）

    async def _handle_ping(message) -> None:
        # target = message.config.get("instance_id")
        # 若有 target → 查 DB 获取 target 实例状态（任意实例均可回答）
        # 发布 pong（广播 or 定向）

    async def _handle_task(message, raw_envelope) -> None:
        # ① 幂等检查 check_processed → skip or replay result
        # ② claim_thread → CLAIMED: upsert_current_msg + _start_run
        # ② claim_thread → RUNNING: _handle_busy

    async def _start_run(message) -> None:
        # sem.acquire + _running_count++ + create_task(_run_and_release)

    async def _run_and_release(message) -> None:
        # runner.run(message) + finally: _running_count-- + sem.release

    async def _handle_busy(message, raw_envelope) -> None:
        # REJECT → publish_error(AGENT_BUSY)
        # STEER → 降级为 followup（记录日志）
        # FOLLOWUP → enqueue_inject(body=raw_envelope, policy=followup)
```

**注意**：followup 队列存储的是 `raw_envelope`（完整 MQ 信封 dict），而非仅 `config`，确保 watchdog 重试时可完整重建 TaskMessage。

---

### 步骤 9 — __main__.py ✅ 已完成

`app/consumer/__main__.py`

**内容**：Consumer 进程启动入口。

```python
class _RocketMQProducerAdapter:
    # 包装同步 RocketMQ Producer，提供 async send_async()
    # 通过 loop.run_in_executor 在线程池中调用阻塞 producer.send()

async def _poll_loop(mq_consumer, task_consumer, executor, *,
                     batch_size, invisible_duration, stop_event,
                     throttle, task_prefix, loop_name) -> None:
    # throttle=True（task topic）：available_slots==0 时退避 0.2s，不拉取
    # throttle=False（signal topic）：始终 poll，不受 task 槽位限制

async def _handle_and_ack(msg, body, mq_consumer, task_consumer, executor) -> None:
    # 调用 handle_message → finally 始终 ack（不重投）

async def _instance_heartbeat_loop(registry, instance_id, interval=10) -> None
async def _stale_run_watchdog(registry, runner, instance_id,
                               interval=30, timeout_seconds=60, max_retries=3) -> None:
    # 双条件检测：run heartbeat + instance heartbeat 均超时
    # 已在 processed_messages → trigger_drain
    # retry_count >= max_retries → publish_fatal_error + mark_thread_idle
    # 否则 → claim_stale_run（SELECT FOR UPDATE，多实例只有一个赢）
    #         → increment_retry_count + runner.run(reconstructed_message)
async def _processed_messages_cleanup(registry, ttl_days, interval=3600) -> None

async def main() -> None:
    # 1. get_app_config() + 日志配置
    # 2. init_engine_from_config() → get_session_factory()
    # 3. async with make_checkpointer(config) as checkpointer:
    # 4. ThreadPoolExecutor(max_workers=6)（task-recv/signal-recv/send/ack×2/spare）
    # 5. RocketMQ Producer.startup()
    # 6. _RocketMQProducerAdapter + RunRegistry + register_instance
    # 7. MQStreamBridge + AgentRunner + TaskConsumer
    # 8. SimpleConsumer × 2（task_topic + signal_topic）
    # 9. SIGTERM/SIGINT → stop_event
    # 10. 后台任务：
    #     - instance_heartbeat_loop
    #     - stale_run_watchdog
    #     - poll_loop(task, throttle=True)
    #     - poll_loop(signal, throttle=False)
    #     - processed_messages_cleanup（ttl_days > 0 时启动）
    # 11. stop_event.wait()
    # 12. 优雅退出：cancel bg_tasks + shutdown consumers + mark_draining + delete_instance
    #     + shutdown producer + executor + close_engine()
```

**config.yaml consumer 配置**：
```yaml
consumer:
  endpoint: $ROCKETMQ_ENDPOINT
  username: $ROCKETMQ_USERNAME
  password: $ROCKETMQ_PASSWORD
  task_topic: $AGENT_TASKS
  signal_topic: $AGENT_SIGNALS
  result_topic: $AGENT_RESULTS
  consumer_group: $AGENT_CONSUMER_GROUP
  signal_consumer_group: $AGENT_SIGNAL_CONSUMER_GROUP
  max_concurrent_runs: 10
  poll_batch_size: 20
  invisible_duration_seconds: 300
  processed_messages_ttl_days: 7    # 0 = 不清理
```

---

## 依赖关系

```
schemas.py ✅
constants.py ✅
    └─► models.py ✅
            └─► run_registry.py ✅
                    ├─► stream_bridge/mq.py ✅
                    └─► agent_runner.py ✅ (依赖 registry + bridge + checkpointer)
                                └─► task_consumer.py ✅ (依赖 registry + runner + bridge)
                                            └─► __main__.py ✅
```

| 步骤 | 文件 | 状态 | 备注 |
|------|------|------|------|
| 1 | schemas.py | ✅ 已完成 | |
| 2 | models.py | ✅ 已完成 | thread_run_state 只有 idle/running |
| 3 | constants.py | ✅ 已完成 | 新增，不在原计划中 |
| 4 | run_registry.py | ✅ 已完成 | 新增 upsert_current_msg、transition_thread_followup、stale watchdog 方法 |
| 5 | stream_bridge/mq.py | ✅ 已完成 | 新增 register/unregister_run，pong 支持定向查询 |
| 6 | inject.py | **待实现（steer）** | steer 收到时降级为 followup |
| 7 | agent_runner.py | ✅ 已完成 | 新增 _build_config、publish_fatal_error、trigger_drain |
| 8 | task_consumer.py | ✅ 已完成 | |
| 9 | __main__.py | ✅ 已完成 | 两个 poll loop + stale watchdog |

---

## 关键设计决定（已定）

- **HIL 暂停状态**：`thread_run_state.status` 只有 `idle` / `running`。HIL 暂停后 thread 立即回到 `idle`，PAUSED_FOR_APPROVAL 只记录在 `processed_messages`。resume 消息正常 claim，无需特判。
- **Topic 分离**：`$AGENT_TASKS` 和 `$AGENT_SIGNALS` 使用独立 SimpleConsumer 实例。signal poll 不受 task 槽位影响，cancel 响应延迟不受并发负载影响。
- **Throttled poll**：task poll loop 在 `available_slots == 0` 时退避 0.2s，不拉取新 task，避免任务在 semaphore 后面堆积。
- **`_build_config` 填充 context**：Consumer 绕过 `worker.py`，必须手动同时填充 `configurable` 和 `context`，确保所有 middleware 能从 `runtime.context` 读取 `thread_id`、`user_id`、`app_config` 等。
- **Followup 存储完整信封**：`thread_msg_queue.body` 存储整个 `raw_envelope`（dict），而非仅 `config`，确保 watchdog 和 drain 均可完整重建 TaskMessage。
- **`transition_thread_followup` 原子操作**：consume followup + update thread_run + replace current_msg 三步在同一事务中，防止进程崩溃时 followup 丢失。
- **Stale watchdog 双条件**：仅当 run heartbeat 超时 **且** 所属 Consumer instance 的 heartbeat 也超时时，才认定为 stale。避免 heartbeat_loop 崩溃但 Consumer 进程仍然活跃时误判。
- **message_mode**：`followup`（默认）和 `reject` 已实现；`steer` 协议预留，收到时降级为 followup，不报错。
- **ProcessedMessages TTL**：每小时清理 `processed_at < now - ttl_days` 的记录，`ttl_days=0` 时不启动清理任务。
- **paused_approval_watchdog**：原计划中的 HIL 超时 watchdog **未实现**，因 HIL 暂停后 thread 已回 idle，过期的 paused state 无需主动清理。

---

## 待实现 / 待确认事项

| # | 问题 | 影响模块 |
|---|------|---------|
| 1 | `steer` 消息的真正实现：需要 `InjectMiddleware` 在 LangGraph node 边界注入新消息 | inject.py |
| 2 | `config.models`（cfgpu 生图/生视频模型偏好）目前存入 `context["models"]`，但无 middleware 将其注入 LLM prompt。需扩展 `DynamicContextMiddleware` | dynamic_context_middleware.py |
| 3 | RocketMQ 消息 tag 过滤（`task_topic_tag` / `signal_topic_tag`）是否需要按需配置 | __main__.py |
