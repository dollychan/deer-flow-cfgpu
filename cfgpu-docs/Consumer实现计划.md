# Consumer 实现计划

基于《Consumer 运行管理设计》和《MQ 消息协议 v2.4》的具体实现指南。

---

## 文件布局

```
backend/
├── app/consumer/                          ← 应用入口层（与 app/gateway/ 并列）
│   ├── __init__.py
│   ├── __main__.py                        ✅ 已完成
│   ├── constants.py                       ✅ 已完成
│   ├── schemas.py                         ✅ 已完成
│   ├── task_consumer.py                   ✅ 已完成
│   ├── agent_runner.py                    ✅ 已完成
│   ├── models.py                          ✅ 已完成
│   ├── run_registry.py                    ✅ 已完成
│   └── stream_bridge/
│       └── mq.py                          ✅ 已完成
│
└── packages/harness/deerflow/
    ├── runtime/runs/
    │   ├── worker.py                      ← 现有（gateway 模式，不修改）
    │   ├── manager.py                     ← 现有（gateway 模式，不修改）
    │   └── models.py                      ← 现有（不修改）
    └── agents/middlewares/
        └── inject.py                      ← InjectMiddleware（steer 待实现）
```

---

## 实现步骤

### 步骤 1 — schemas.py ✅ 已完成

`app/consumer/schemas.py`

**内容**：RocketMQ 消息信封（v2.4）反序列化 + schema 校验。

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
  .timeout_seconds   # → config.timeout_seconds，无则 None（缺省无限制）
  .user_id           # 信封 user_id，用于 context 注入
  .agent_name        # 信封 agent_name，用于 context 注入
  .downlink_echo()   # 返回需要在下行消息中回显的信封字段 dict
  ._validate_raw(data)         # 信封级校验（classmethod）
  ._validate_task_payload(payload)  # task 专属校验（classmethod）
```

**校验时机**：`from_dict()` / `from_json()` 在构造对象前调用 `_validate_raw()`，校验失败抛出 `SchemaValidationError`。`cancel` 和 `ping` 类型只做信封级校验，无额外 payload 约束。

**ping payload instance_id 合并**（已修复）：ping 消息的 `instance_id` 在 `payload.instance_id`（顶级），`from_dict` 已在 ping 类型时将其合并进 `config` dict，`_handle_ping` 可通过 `message.config.get("instance_id")` 正确读取。

---

### 步骤 2 — models.py ✅ 已完成

`app/consumer/models.py`

4 张自定义 ORM 表，继承 `deerflow.persistence.base.Base`，随 `Base.metadata.create_all` 自动建表。

| 表 | 状态 | 说明 |
|----|------|------|
| `consumer_instances` | ✅ | 字段正确 |
| `thread_run_state` | ✅ | 含 `thread_msg_seq`、`drain_mode`、`retry_count` |
| `thread_msg_queue` | ✅ | 含 `thread_msg_seq`，已移除 `consumed_at`；policy 支持 current/followup/cancel/prefix/steer |
| `processed_messages` | ✅ | 字段正确 |
| ~~`thread_cancel_signals`~~ | ✅ 已删除 | cancel 语义已通过 thread_msg_queue policy='cancel' 实现 |

**重要设计决定**：
- `thread_run_state.status` 只有 `idle` / `running` 两种状态，**不含 `paused_for_approval`**。
- `__main__.py` 启动时已通过 `import app.consumer.models` 注册所有表。

---

### 步骤 3 — constants.py ✅ 已完成

`app/consumer/constants.py`

所有 StrEnum 常量已定义：

```python
class ThreadStatus(StrEnum):   IDLE / RUNNING
class InstanceStatus(StrEnum): ACTIVE / DRAINING / DEAD
class QueuePolicy(StrEnum):    CURRENT / FOLLOWUP / CANCEL / PREFIX / STEER
class ProcessedStatus(StrEnum): COMPLETED / FAILED / CANCELLED / PAUSED_FOR_APPROVAL
class MessageMode(StrEnum):    FOLLOWUP / COLLECT / STEER / REJECT
class ClaimResult(StrEnum):    CLAIMED / RUNNING
```

---

### 步骤 4 — run_registry.py ✅ 已完成

`app/consumer/run_registry.py`

所有 DB 操作已实现，主要 API：

```python
class RunRegistry:
    # 实例管理
    register_instance / heartbeat_instance / mark_instance_draining / delete_instance / get_instance

    # Thread 路由
    get_thread_state(thread_id) -> ThreadRunStateRow | None
    claim_thread(thread_id, instance_id, message_id, thread_msg_seq=0) -> ClaimResult
    mark_thread_idle(thread_id)        # SET status='idle', drain_mode='followup' + DELETE current 行
    heartbeat_thread(thread_id)
    get_drain_mode(thread_id) -> str   # 返回 'followup' 或 'collect'
    update_thread_run(thread_id, new_message_id)  # 切换 run（_drain_and_release 用）

    # 消息队列（统一接口，替换原 cancel/inject 分离 API）
    enqueue_message(thread_id, message_id, body, thread_msg_seq, policy)
    upsert_current_msg(thread_id, message_id, body, thread_msg_seq=0)
    get_current_msg(thread_id) -> ThreadMsgQueueRow | None
    peek_thread_queue(thread_id, policies) -> list[ThreadMsgQueueRow]
    find_cancel_after_seq(thread_id, current_task_seq) -> ThreadMsgQueueRow | None
    get_followup_before_seq(thread_id, cancel_seq) -> list[ThreadMsgQueueRow]
    convert_to_prefix(thread_id, row_ids)
    delete_queue_items(thread_id, row_ids)
    transition_thread_followup(thread_id, queue_id, new_message_id, new_body, thread_msg_seq,
                               *, prefix_ids=None)

    # 幂等
    check_processed / mark_processed

    # Watchdog
    find_stale_runs(timeout_seconds=60) -> list[ThreadRunStateRow]
    claim_stale_run(thread_id, instance_id) -> bool
    increment_retry_count / reset_retry_count
    cleanup_processed_messages(ttl_days) -> int
```

**注意**：`set_drain_mode` 未单独实现为方法，collect 模式的 drain_mode 写入待 task_consumer.py 更新时一并实现（`UPDATE thread_run_state SET drain_mode='collect'`）。

**废弃 API（不再存在）**：
- ~~`insert_cancel_signal`~~ → 使用 `enqueue_message(..., policy=QueuePolicy.CANCEL)`
- ~~`has_cancel_signal`~~ → 使用 `find_cancel_after_seq`
- ~~`clear_cancel_signal`~~ → 使用 `delete_queue_items`
- ~~`enqueue_inject`~~ → 使用 `enqueue_message(..., policy=QueuePolicy.FOLLOWUP)`

---

### 步骤 5 — stream_bridge/mq.py ✅ 已完成

`app/consumer/stream_bridge/mq.py`

**已实现特性**：
- `echo` dict 统一承载下行透传字段（message_id、thread_id、thread_msg_seq、agent_name、user_id、project_id），`_build_envelope` 只需 4 个参数
- `message_seq` 从 **1** 开始（`_RunContext.seq = 1`），`pong`、fallback error/result 使用 **0** 作为 N/A sentinel
- `_RunContext` 含 `reply_config`、`echo`、`seq`、`buffered_events`（custom 事件缓存）
- `get_buffered_events(run_id)` 供 result_cache 存储，用于幂等 replay
- `replay()` 发送顺序：buffered custom events → tool_approval_required → 终止信封
- `publish_pong` 定向 ping payload 含 `host_instance_id`

```python
class MQStreamBridge(StreamBridge):
    def register_run(run_id, reply_config, *, echo=None) -> None   # seq 初始化为 1
    def unregister_run(run_id) -> None
    async def publish(run_id, event, data) -> None
    def get_buffered_events(run_id) -> list[dict]
    async def publish_result(run_id, *, status, stream_events, echo=None, final_state=None, usage=None) -> None
    async def publish_error(code, *, echo, message, retriable, node=None) -> None
    async def replay(result_cache, *, echo=None) -> None
    async def publish_pong(instance_id, *, echo=None, target_instance_id=None, ...) -> None
```

---

### 步骤 6 — inject.py（预留，steer 待实现）

`deerflow/agents/middlewares/inject.py`

当前版本**不实现**。`steer` 消息收到时自动降级为 followup，不报错。

---

### 步骤 7 — agent_runner.py ✅ 已完成

`app/consumer/agent_runner.py`

- `run()` 主体：heartbeat + cancel_watcher + execute + processed + drain
- `current_task_seq = message.thread_msg_seq` 传递给 `_cancel_watcher`
- `_cancel_watcher(thread_id, current_task_seq, runner_task)`：轮询 `find_cancel_after_seq`（seq > current_task_seq）
- `except asyncio.CancelledError`：cancel barrier 清理（convert_to_prefix + **publish_result(status=CANCELLED)** + delete cancel row）
- `_drain_and_release()`：cancel barrier 处理 + **prefix 消息合并** + followup drain 链式调用
- prefix merge：遍历 prefix_rows 按 seq 顺序解析 messages，前缀拼接进 next_task.messages
- `_execute()`：HIL resume、astream、paused 检测、final_state 序列化、buffered events
- `_build_config()`：configurable + Runtime(context=...) 两个 slot 均填充
- `publish_fatal_error()` / `trigger_drain()`

**cancel barrier 通知**：已统一为 `publish_result(row.message_id, status=ProcessedStatus.CANCELLED, stream_events=False, echo={...})`，不再使用 `publish_error`。

---

### 步骤 8 — task_consumer.py ✅ 已完成

`app/consumer/task_consumer.py`

- `handle_message()`：JSON 解析 → schema 校验 → type 分发（ping/cancel/task）
- `_handle_ping()`：定向/广播 pong，定向时查 `get_instance`
- `_handle_cancel()`：`enqueue_message(..., QueuePolicy.CANCEL)`
- `_handle_task()`：幂等检查 → reject 短路 → steer 降级提示 → `enqueue_message(..., QueuePolicy.FOLLOWUP)` → `_try_dispatch`
- `_try_dispatch()`：cancel barrier 处理 → `claim_thread` → `upsert_current_msg` → `delete_queue_items` → `create_task(_run_and_release)`
- cancel barrier 通知：已统一为 `publish_result(..., status=ProcessedStatus.CANCELLED, stream_events=False, echo={...})`
- `_run_and_release()`：Semaphore 控制
- `shutdown()`：等待 active_tasks 完成

---

### 步骤 9 — __main__.py ✅ 已完成

`app/consumer/__main__.py`

**已实现**：

- 单 SimpleConsumer + 单 poll-loop（无节流，全速拉取，立即 ACK）
- `ThreadPoolExecutor(max_workers=5)`
- 后台任务：instance-heartbeat、stale-run-watchdog、poll-loop、processed-messages-cleanup
- SIGTERM/SIGINT 优雅退出：cancel bg_tasks → shutdown consumer → mark_draining → delete_instance → shutdown producer
- `_stale_run_watchdog`：双条件（run heartbeat + instance heartbeat 均超时）
- `_processed_messages_cleanup`：`ttl_days=0` 时不启动

**config.yaml consumer 配置**：
```yaml
consumer:
  endpoint: $ROCKETMQ_ENDPOINT
  username: $ROCKETMQ_USERNAME
  password: $ROCKETMQ_PASSWORD
  task_topic: $AGENT_TASKS          # 单 topic：task/cancel/ping 所有类型
  result_topic: $AGENT_RESULTS
  consumer_group: $AGENT_CONSUMER_GROUP
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
                    └─► agent_runner.py ✅
                                └─► task_consumer.py ✅
                                            └─► __main__.py ✅
```

| 步骤 | 文件 | 状态 | 剩余工作 |
|------|------|------|---------|
| 1 | schemas.py | ✅ | — |
| 2 | models.py | ✅ | — |
| 3 | constants.py | ✅ | — |
| 4 | run_registry.py | ✅ | set_drain_mode 待 collect 实现时补充 |
| 5 | stream_bridge/mq.py | ✅ | — |
| 6 | inject.py | **待实现（steer）** | — |
| 7 | agent_runner.py | ✅ | — |
| 8 | task_consumer.py | ✅ | — |
| 9 | __main__.py | ✅ | — |

---

## 关键设计决定（已定）

- **单 topic 架构**：`$AGENT_TASKS` 承载所有消息类型（task/cancel/ping），poll-loop 不节流，全速拉取，立即 ACK；cancel 响应延迟由 cancel_watcher 的 `poll_interval`（默认 2s）决定，与 poll 积压无关。
- **cancel 入队**：cancel 写入 `thread_msg_queue`（policy='cancel'），携带 `thread_msg_seq`；通过 `thread_msg_seq > current_task_seq` 过滤历史遗留 cancel。`thread_cancel_signals` 表废弃。
- **cancel barrier**：`_try_dispatch` 和 `_drain_and_release` 在取下一个 task 前先处理 cancel barrier——将 barrier 前的 followup 转为 prefix（保留 LLM 上下文），通知上游，删除 cancel 行。
- **HIL 暂停状态**：`thread_run_state.status` 只有 `idle` / `running`。HIL 暂停后 thread 立即回 idle，PAUSED_FOR_APPROVAL 只记录在 `processed_messages`。resume 消息正常 claim，无需特判。
- **echo dict**：上行→下行的透传字段（message_id、thread_id、thread_msg_seq、agent_name、user_id、project_id）统一在一个 dict 中，`_build_envelope` 只需 4 个参数。
- **`_build_config` 填充 Runtime(context=...)**：Consumer 绕过 `worker.py`，通过 `configurable["__pregel_runtime"] = Runtime(context=context)` 注入 runtime context，确保所有 middleware 能读取必要字段。
- **followup / current 存储完整信封**：`thread_msg_queue.body` 存储整个 `raw_envelope`，无损重建 TaskMessage。
- **transition_thread_followup 原子操作**：DELETE followup + DELETE prefix + UPDATE thread_run_state + UPSERT current 四步在同一事务中。
- **stale watchdog 双条件**：仅当 run heartbeat **且** Consumer instance heartbeat 均超时才判 stale，避免 heartbeat_loop 崩溃但进程存活时误判。
- **message_seq 从 1 开始**：progress/result/error 消息的 `message_seq` 从 1 递增；0 为 N/A sentinel（用于 pong、fallback error/result、上行消息）。
- **ProcessedMessages TTL**：每小时清理 `processed_at < now - ttl_days` 的记录，`ttl_days=0` 时不启动清理任务。

---

## 待实现 / 待确认事项

| # | 问题 | 优先级 | 影响模块 |
|---|------|--------|---------|
| 1 | **collect 模式**（设计已完成，见设计文档 §5.3）：`_drain_and_release` collect 分支 + `run_registry.set_drain_mode` + `transition_thread_collect` + `TaskMessage.with_messages()` | 中 | run_registry.py / agent_runner.py / task_consumer.py |
| 2 | **`steer` 消息真正实现**：`InjectMiddleware` 在 LangGraph node 边界注入新消息 | 低 | inject.py |
| 3 | **`config.models` guardrail**：`config.models`（cfgpu 生图/生视频模型偏好）存入 `context["models"]`，但无校验机制约束 LLM 生成的 tool 参数（见设计文档 §10） | 低 | GuardrailMiddleware 或 MCP tool interceptor |
