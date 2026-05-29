# Consumer 运行管理设计 v2

本文描述 DeerFlow Consumer 集群的新运行管理设计。v2 的核心变化是将 RocketMQ 消费确认、任务入库、任务调度、Agent 执行彻底分层：

```text
RocketMQ PollAck/Ingest 只负责可靠入库和 ACK
Scheduler 以本实例空闲 slot 为驱动，从 DB 原子 claim 可运行消息
AgentRunner 只负责执行一条已 claim 的消息
```

旧版设计中，`poll-loop` 入队后会尝试 `_try_dispatch(thread_id)`，并在当前 thread 的 run 完成后由 `_drain_and_release()` 链式处理 followup。该模型在 semaphore 满时存在调度缺口：消息已经 ACK 并入库，但如果当时没有空闲 slot，后续可能缺少全局唤醒机制。v2 将 followup/resume 统一交给 Scheduler 调度，避免消息滞留在某个 thread 或某个 Consumer 实例之后。

---

## 1. 整体架构

```text
上游
  │
  │  task / cancel / ping
  ▼
RocketMQ $AGENT_TASKS
  │
  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       Consumer 集群（N 个实例）                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ PollAck / Ingest                                                │  │
│  │ - receive RocketMQ                                              │  │
│  │ - ping 直接回复 pong                                            │  │
│  │ - task/cancel durable enqueue 到 DB                      │  │
│  │ - DB commit 成功后 ACK RocketMQ                                 │  │
│  └───────────────────────────────┬────────────────────────────────┘  │
│                                  │ notify local scheduler            │
│                                  ▼                                   │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ PostgreSQL thread_msg_queue / thread_run_state / thread_control_state / processed_messages │  │
│  │ - queue 保存完整 MQ envelope                                   │  │
│  │ - thread_run_state 保证同一 thread 同时只有一个 running.         │  │
│  │ - processed_messages 提供幂等 replay                           │  │
│  └───────────────────────────────┬────────────────────────────────┘  │
│                                  │ claim next runnable message        │
│                                  ▼                                   │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ Slot-driven Scheduler                                           │  │
│  │ - 持有本实例 asyncio.Semaphore(max_concurrent_runs)             │  │
│  │ - slot 空闲、新消息入队、定时 tick 时尝试调度                  │  │
│  │ - 多实例通过 DB 行锁竞争 pending message，实现负载均衡          │  │
│  │ - 默认策略：各 thread 最早 pending 中取全局 created_at 最老者   │  │
│  └───────────────────────────────┬────────────────────────────────┘  │
│                                  │ create AgentRunner task            │
│                                  ▼                                   │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ AgentRunner                                                     │  │
│  │ - 执行单条 TaskMessage                                          │  │
│  │ - 每个 run 自带 heartbeat_loop + cancel_watcher                 │  │
│  │ - 输出经 MQStreamBridge 发布到 $AGENT_RESULTS                  │  │
│  │ - 完成/失败/取消/HIL 暂停后释放 slot 并唤醒 Scheduler           │  │
│  └───────────────────────────────┬────────────────────────────────┘  │
└──────────────────────────────────┼───────────────────────────────────┘
                                   ▼
                         RocketMQ $AGENT_RESULTS
                         progress / result / error / pong
```

共享依赖：

```text
PostgreSQL:
  LangGraph checkpointer
  consumer_instances
  thread_run_state
  thread_control_state
  thread_msg_queue
  processed_messages

共享文件系统:
  $DEER_FLOW_HOME
  config / skills / agents / users/{user_id}/threads/{thread_id}/user-data
```

生产环境多 Consumer 必须使用 PostgreSQL。SQLite 仅用于单实例开发/测试。

---

## 2. 分层职责

### 2.1 PollAck / Ingest 层

职责：

- 使用 RocketMQ `SimpleConsumer.receive()` 拉取 `$AGENT_TASKS`。
- 解析 JSON 和校验 MQ schema。
- `ping` 直接发布 `pong`。
- `task`(包括普通消息和 HIL `resume`)、`cancel` 写入 PostgreSQL。
- DB commit 成功后 ACK RocketMQ。
- 入库成功后 notify 本地 Scheduler。

PollAck 不负责：

- 不持有 Agent 执行 slot。
- 不判断哪个 Consumer 执行任务。
- 不运行 LangGraph。
- 不等待 Agent 完成。

ACK 边界：

```text
ping:
  pong 发布完成后 ACK

task/cancel:
  durable enqueue commit 成功后 ACK

schema invalid:
  能提取 message_id 时发布 INVALID_SCHEMA 后 ACK
  无法提取 message_id 时仅记录日志后 ACK
```

如果 DB commit 失败，不应 ACK，让 RocketMQ 在 invisible duration 后重投。

### 2.2 Scheduler 层

职责：

- 持有本实例 `asyncio.Semaphore(max_concurrent_runs)`。
- 监听三类唤醒：
  - 新消息入队。
  - AgentRunner 结束并释放 slot。
  - 定时兜底 tick。
- 只要有空闲 slot，就主动从 DB 中 claim 一个可运行消息。
- 创建 `AgentRunner.run(message)` task。
- 多实例之间通过 PostgreSQL 行锁竞争 pending rows，自然实现负载均衡。

Scheduler 不负责：

- 不直接 ACK RocketMQ。
- 不直接调用 RocketMQ receive。
- 不执行业务 graph。

核心循环示意：

```python
async def scheduler_loop():
    while not stopping:
        await wait_for(new_message_event | slot_available_event | periodic_tick)

        while has_free_slot():
            await sem.acquire()
            claimed = await registry.claim_next_runnable(instance_id)
            if claimed is None:
                sem.release()
                break

            task = asyncio.create_task(run_and_release(claimed))
            active_tasks.add(task)
```

`run_and_release()`：

```python
async def run_and_release(message):
    try:
        await runner.run(message)
    finally:
        sem.release()
        scheduler.notify_slot_available()
```

### 2.3 AgentRunner 层

职责：

- 根据已 claim 的 `TaskMessage` 构造 DeerFlow LangGraph config。
- 注入 `thread_id`、`user_id`、`project_id`、`agent_name`、runtime context。
- 驱动 `graph.astream()`。
- 通过 `MQStreamBridge` 发布 progress / result / error。
- 维护 run heartbeat。
- 通过 cancel watcher 响应 cancel。
- 在 terminal 状态写入 `processed_messages`。
- HIL 暂停时释放 slot，使 thread 等待后续 resume 消息。

AgentRunner 不再负责 followup drain。run 结束后只更新状态并通知 Scheduler。

---

## 3. 进程内线程与 asyncio 模型

每个 Consumer 实例是单进程、单 asyncio event loop。唯一线程池用于 RocketMQ Python SDK 的阻塞调用。

```text
Consumer 进程
│
├── 主 asyncio event loop
│   ├── poll_ack_loop
│   ├── scheduler_loop
│   ├── instance_heartbeat_loop
│   ├── stale_run_watchdog
│   ├── processed_messages_cleanup
│   ├── 每条 MQ 消息的 ingest task
│   └── 每个 AgentRunner run task
│
└── ThreadPoolExecutor(max_workers=5, thread_name_prefix="rmq")
    ├── mq_consumer.receive(...)
    ├── mq_consumer.ack(...)
    └── mq_producer.send(...)
```

`ThreadPoolExecutor(max_workers=5)` 不是 5 个业务 Consumer，也不是 5 个 AgentRunner。它只承接同步 RocketMQ SDK 的阻塞 I/O。

建议后台协程：

| 协程 | 归属 | 说明 |
|------|------|------|
| `poll_ack_loop` | per-instance | 每个实例独立消费自己的 MQ 份额 |
| `scheduler_loop` | per-instance | 每个实例根据自身 slot 竞争 DB pending rows |
| `instance_heartbeat_loop` | per-instance | 更新自身 `consumer_instances.last_heartbeat` |
| `stale_run_watchdog` | cluster-level | 可每实例幂等执行；后续可用 advisory lock 选主 |
| `processed_messages_cleanup` | cluster-level | 可每实例幂等执行；后续可用 advisory lock 选主 |

---

## 4. 数据库模型

### 4.1 `consumer_instances`

```sql
CREATE TABLE consumer_instances (
    instance_id    TEXT PRIMARY KEY,
    hostname       TEXT NOT NULL,
    pid            INT  NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL
);
```

状态：

```text
active | draining | dead
```

### 4.2 `thread_run_state`

`thread_run_state` 只描述 thread 是否正在被某个 Consumer instance 执行，不保存具体消息的业务运行状态。

```text
idle | running
```

```sql
CREATE TABLE thread_run_state (
    thread_id         TEXT PRIMARY KEY,
    instance_id       TEXT,
    message_id        TEXT,
    thread_msg_seq    INT  NOT NULL DEFAULT 0,
    status            TEXT NOT NULL,
    -- idle | running
    retry_count       INT  NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ,
    last_heartbeat    TIMESTAMPTZ NOT NULL
);
```

语义：

| status | 是否占用 Consumer slot | Scheduler 行为 |
|--------|------------------------|----------------|
| `idle` | 否 | 结合 `thread_control_state.gate` 判断可 claim 的消息类型 |
| `running` | 是 | 不 claim 该 thread 的普通 task；cancel 由 cancel_watcher 处理 |

HIL 暂停时，`thread_run_state` 应回到 `idle`，因为该 thread 已经不占用 Consumer instance 和 semaphore slot。LangGraph interrupt 状态保存在 checkpointer 中，resume 消息作为新的 MQ task 入队，由 Scheduler 后续 claim 执行。

### 4.3 `thread_msg_queue`

```sql
CREATE TABLE thread_msg_queue (
    id              BIGSERIAL PRIMARY KEY,
    thread_id       TEXT NOT NULL,
    message_id      TEXT NOT NULL UNIQUE,
    thread_msg_seq  INT  NOT NULL,
    body            JSONB NOT NULL,
    policy          TEXT NOT NULL DEFAULT 'followup',
    -- current | followup | cancel | prefix | steer
    status          TEXT NOT NULL DEFAULT 'pending',
    -- pending | claimed | consumed | cancelled
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    attempt_count   INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

建议索引/约束：

```sql
CREATE UNIQUE INDEX ux_thread_msg_queue_message_id
ON thread_msg_queue(message_id);

CREATE INDEX ix_msg_queue_pending
ON thread_msg_queue(status, policy, created_at);

CREATE INDEX ix_msg_queue_thread_seq
ON thread_msg_queue(thread_id, thread_msg_seq)
WHERE status = 'pending';

CREATE UNIQUE INDEX ux_thread_current
ON thread_msg_queue(thread_id)
WHERE policy = 'current';
```

`body` 必须保存完整 MQ envelope，而不是只保存 payload。原因是 stale recovery、resume、agent routing 都需要 `agent_name`、`user_id`、`project_id`、`reply_config` 等 envelope 字段。

`policy` 语义：

| policy | 写入方 | 消费方 | 说明 |
|--------|--------|--------|------|
| `followup` | PollAck 收到 task/resume | Scheduler | 普通待执行消息 |
| `cancel` | PollAck 收到 cancel | Scheduler / cancel_watcher | cancel barrier 或取消 running task |
| `prefix` | Scheduler 处理 cancel barrier | Scheduler | 被 cancel 跳过但要作为上下文前缀合入后续 task |
| `current` | Scheduler claim 成功 | stale-run-watchdog | 当前 running 消息完整 envelope，用于 crash recovery |
| `steer` | 预留 | InjectMiddleware | 当前版本可降级为 followup |

### 4.4 `processed_messages`

```sql
CREATE TABLE processed_messages (
    message_id   TEXT PRIMARY KEY,
    thread_id    TEXT NOT NULL,
    status       TEXT NOT NULL,
    -- completed | failed | cancelled | paused_for_approval
    result_cache JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`processed_messages` 只记录 terminal 或 paused run 的结果，用于 RMQ 重投后的 replay。同一 `message_id` 已经进入 `processed_messages` 后，不重新执行。

为防止“已入队未完成”阶段的 RMQ 重投重复入队，`thread_msg_queue.message_id` 也需要唯一约束。幂等判断顺序建议为：

```text
1. processed_messages 中存在 -> replay result_cache，ACK
2. thread_msg_queue 中存在同 message_id -> 视为已入队，ACK
3. 都不存在 -> 新消息入队
```

入队必须使用 `INSERT ... ON CONFLICT (message_id) DO NOTHING`，把 RocketMQ 重复投递转成无副作用操作。`rowcount = 0` 表示该消息已经处于 queued/current 状态，PollAck 可以直接 ACK，不应再次 notify Scheduler。

```sql
INSERT INTO thread_msg_queue (
    thread_id, message_id, thread_msg_seq, body, policy, status
)
VALUES (
    :thread_id, :message_id, :thread_msg_seq, :body, :policy, 'pending'
)
ON CONFLICT (message_id) DO NOTHING;
```

注意：如果实现仍保留 `policy='current'` crash-recovery 行，`followup -> current` 的转换必须在同一事务内删除原 pending 行并写入 current 行，或改为更新同一行的 `policy/status`。否则全局 `UNIQUE(message_id)` 会让同一消息在转换阶段自撞约束。

### 4.5 `thread_control_state`

HIL/approval 这类业务 gate 不属于 Consumer instance ownership。为了避免普通 followup 绕过 checkpoint 中的人工确认点，建议单独维护 thread 级控制状态：

```sql
CREATE TABLE thread_control_state (
    thread_id    TEXT PRIMARY KEY,
    gate         TEXT NOT NULL DEFAULT 'open',
    -- open | hil_waiting
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

该表只回答“这个 thread 业务上是否允许普通 task 继续执行”，不回答“这个 thread 是否正在被某个 Consumer instance 执行”。HIL 的具体断点仍由 LangGraph checkpointer 保存；resume 仍通过新的 MQ `message_id` 和同一个 `thread_id` 恢复 checkpoint。

如果暂时不引入该表，也必须有等价的 gate 来源；不能只依赖 `thread_run_state.status='idle'`，否则 HIL 暂停后普通 followup 可能被 Scheduler 误 claim。

---

## 5. PollAck / Ingest 流程

### 5.1 通用入口

```text
receive MQ message
  -> JSON parse
  -> schema validate
  -> dispatch by type
  -> DB commit / publish response
  -> ACK RocketMQ
```

### 5.2 ping

```text
type=ping
  -> 如果 payload.instance_id 存在，查询 consumer_instances
  -> 发布 pong(instance_id, target_status, last_heartbeat)
  -> ACK
```

### 5.3 task / HIL resume

HIL resume 仍是 `type=task`，通过 `payload.command != null` 区分。

```text
type=task
  -> check processed_messages(message_id)
       found: replay result_cache, ACK
  -> insert thread_msg_queue(policy=followup, status=pending)
       unique conflict: 视为已入队，ACK
  -> commit
  -> scheduler.notify_new_message()
  -> ACK
```

`message_mode=reject` 可在入队前快速短路：

```text
if message_mode=reject and (
    thread_run_state.status = 'running'
    or thread_control_state.gate = 'hil_waiting'
):
    publish error(AGENT_BUSY, retriable=true)
    ACK
```

`message_mode=steer` 当前版本建议仍入 `followup`，后续实现 InjectMiddleware 后可改为 `policy=steer`。

`message_mode=collect` v2 不建议继续使用 thread 级 `drain_mode`。若要实现 collect，应作为 Scheduler 的 claim 策略：当某 thread 被选中时，一次 claim 该 thread 当前可合并的多条 pending task，合成一个 run。

### 5.4 cancel

```text
type=cancel
  -> insert thread_msg_queue(policy=cancel, status=pending)
       unique conflict: 视为已入队
  -> commit
  -> scheduler.notify_new_message()
  -> ACK
```

cancel 的实际效果按 thread 当前状态区分：

| thread 状态 | 处理者 | 行为 |
|-------------|--------|------|
| `running` | AgentRunner cancel_watcher | 发现 seq 更大的 cancel 后 `runner_task.cancel()` |
| `idle` | Scheduler | 作为 cancel barrier 处理队列，取消 cancel 之前的 pending task |
| `hil_waiting` | Scheduler | 可清理 approval gate，然后回到 open |

---

## 6. Scheduler 设计

### 6.1 唤醒机制

Scheduler 不应只靠定时扫表，也不能只靠 event。推荐组合：

```text
本地 asyncio.Event:
  - PollAck 入队成功后 set()
  - AgentRunner 释放 slot 后 set()

定时兜底:
  - 每 1s/2s tick 一次，防止漏通知

PostgreSQL LISTEN/NOTIFY:
  - 可选优化，用于跨实例快速唤醒
```

即使某个实例没有收到本地 event，定时 tick 也能重新扫描 DB pending rows。

### 6.2 claim 策略

默认策略：

```text
1. 同一 thread 内严格按 thread_msg_seq 顺序。
2. 不同 thread 之间，从“每个 thread 最早 pending row”中选 created_at 最老者。
3. thread.status=running 时不 claim 普通 task。
4. thread_control_state.gate='hil_waiting' 时只 claim HIL resume 或 cancel。
5. 一个 thread 同一时刻最多一个 AgentRunner。
```

不能简单地取全局 `created_at` 最老 row，因为那可能选到某个 thread 的第二条消息，而第一条还 pending。正确做法是先为每个 thread 取最早候选，再在候选集合中排序。

后续可扩展策略：

```text
priority
project_id fair-share
user_id fair-share
per-user concurrency limit
per-project concurrency limit
aging 防饥饿
```

### 6.3 原子 claim

Scheduler 多实例并发时，claim 必须在 PostgreSQL 事务内完成：

```text
begin
  1. 找到一个可运行 candidate row，并锁定该 row/thread state
  2. 确认 thread 状态允许执行
  3. 将 thread_run_state 更新为 running
  4. 将 queue row 标记 claimed/consumed，或直接删除 pending row
  5. upsert policy=current row，保存完整 body
commit
```

PostgreSQL 建议使用 `FOR UPDATE SKIP LOCKED`，避免多个 Scheduler 卡在同一行上。

伪 SQL 方向：

```sql
WITH first_per_thread AS (
  SELECT DISTINCT ON (thread_id)
         id, thread_id, message_id, thread_msg_seq, created_at
  FROM thread_msg_queue
  WHERE status = 'pending'
    AND policy IN ('followup', 'cancel', 'prefix')
  ORDER BY thread_id, thread_msg_seq ASC, created_at ASC
),
candidate AS (
  SELECT q.*
  FROM first_per_thread f
  JOIN thread_msg_queue q ON q.id = f.id
  LEFT JOIN thread_run_state s ON s.thread_id = q.thread_id
  LEFT JOIN thread_control_state c ON c.thread_id = q.thread_id
  WHERE
    (s.thread_id IS NULL OR s.status = 'idle')
    AND (
      c.thread_id IS NULL
      OR c.gate = 'open'
      OR (c.gate = 'hil_waiting' AND (q.policy = 'cancel' OR q.body->'payload'->'command' IS NOT NULL))
    )
  ORDER BY q.created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
SELECT * FROM candidate;
```

实际实现中还需要锁定对应 `thread_run_state` 行；当 row 不存在时创建新 state row。若使用 `thread_control_state`，claim 时也需要把该 thread 的 control row 纳入同一事务判断，避免 gate 状态和 claim 决策之间出现竞态。

### 6.4 cancel barrier

Scheduler claim 到某个 thread 前，应先处理该 thread 的最早 cancel barrier。

```text
pending rows ordered by thread_msg_seq:
  followup(seq=1)
  followup(seq=2)
  cancel(seq=3)
  followup(seq=4)

处理 cancel(seq=3):
  - seq < 3 的 followup 转 cancelled/prefix
  - 对这些 followup 发布 result(cancelled)
  - 删除/消费 cancel row
  - seq=4 之后的 followup 仍可继续等待调度
```

是否把 cancel 前的 followup 转 `prefix`，取决于业务是否希望“被取消的用户输入仍作为上下文保留”。如果希望保留上下文，可转 `prefix` 并在下一个 task claim 时合并到 messages 前面；如果不希望保留，可直接标记 `cancelled` 并删除。

### 6.5 HIL paused / resume

HIL interrupt 后：

```text
AgentRunner 捕获 paused 状态
  -> publish tool_approval_required / result(paused_for_approval)
  -> mark_processed(message_id, paused_for_approval)
  -> thread_run_state.status = idle
  -> thread_control_state.gate = hil_waiting
  -> 不占用 semaphore slot
  -> Scheduler 可继续处理其他 thread
```

resume 消息：

```text
type=task
payload.command != null
config.ask = true
```

PollAck 将 resume 当普通 task 入队。Scheduler 看到 `thread_control_state.gate='hil_waiting'` 时，只允许 claim resume 或 cancel：

```text
hil_waiting + resume:
  -> claim
  -> thread_run_state.status=running
  -> thread_control_state.gate=open
  -> AgentRunner graph.astream(Command(resume=...))

hil_waiting + normal task:
  -> 留在 queue，不越过审批点

hil_waiting + cancel:
  -> 清理 pending approval gate，thread_control_state.gate=open
```

这样既释放 Consumer 资源，又不会让普通 followup 绕过未完成的人工审批。

---

## 7. AgentRunner 执行流程

```text
AgentRunner.run(message)
  -> MQStreamBridge.register_run
  -> start heartbeat_loop
  -> start cancel_watcher
  -> build RunnableConfig / Runtime context
  -> build DeerFlow graph
  -> graph.astream(...)
  -> publish progress / result / error
  -> mark_processed
  -> update thread_run_state:
       completed/failed/cancelled -> idle
       paused_for_approval        -> idle
  -> update thread_control_state:
       paused_for_approval        -> hil_waiting
       resume/cancel completes    -> open
  -> stop heartbeat/cancel watcher
  -> unregister_run
  -> release scheduler slot
  -> scheduler.notify_slot_available()
```

### 7.1 cancel_watcher

每个 running AgentRunner 保留一个 cancel watcher：

```text
每 2s 查询:
  thread_msg_queue
  WHERE thread_id = current_thread
    AND policy = 'cancel'
    AND thread_msg_seq > current_task_seq
    AND status = 'pending'

找到后:
  runner_task.cancel()
```

`asyncio.Task.cancel()` 会在下一个 `await` 点注入 `CancelledError`，能覆盖 LLM 调用、tool 调用、DB I/O 等等待点。

### 7.2 HIL paused 不占 slot

HIL 暂停不是运行中阻塞。AgentRunner 应将其视为本次 run 的一个可持久化状态：

```text
processed_messages.status = paused_for_approval
thread_run_state.status = idle
thread_control_state.gate = hil_waiting
semaphore slot release
```

后续 resume 消息通过 Scheduler 重新 claim，而不是由原 run 继续占用 slot 等待。

---

## 8. Stale Run 恢复

运行中 run 的恢复仍依赖双 heartbeat：

```text
consumer_instances.last_heartbeat: 进程心跳
thread_run_state.last_heartbeat: run 心跳
```

只有当 run heartbeat 和 owner instance heartbeat 都超时时，才判定 stale，避免单个 heartbeat 协程异常导致误判。

恢复流程：

```text
stale-run-watchdog 发现 thread_run_state.status='running' 且 owner dead
  -> 读取 thread_msg_queue policy='current'
  -> check processed_messages(message_id)
       found:
         mark thread idle
         scheduler.notify()
       not found:
         retry_count < max_retries:
           claim stale run 到当前 instance
           Scheduler/Runner 从 current body 重建 TaskMessage
           LangGraph 从 checkpoint 恢复
         retry_count >= max_retries:
           publish FATAL error
           mark thread idle
           scheduler.notify()
```

`current` row 的完整 envelope 是 stale recovery 的关键。它需要包含：

```text
message_id
thread_id
thread_msg_seq
agent_name
user_id
project_id
payload.messages / payload.command / payload.config / payload.reply_config
```

---

## 9. 幂等与可靠性

### 9.1 MQ ack 边界

v2 的 ACK 原则：

```text
RocketMQ ACK = 消息已被 Consumer 可靠接收并持久化，或已产生明确拒绝/错误响应。
RocketMQ ACK != Agent 执行完成。
```

task/cancel 消息只有在 DB commit 成功后 ACK。这样即使 Consumer 在 ACK 前崩溃，RocketMQ 会重投；如果 ACK 后崩溃，DB 中已有 queue/current/processed 状态，Scheduler 或 watchdog 能继续推进。

### 9.2 去重范围

需要两层去重：

| 阶段 | 表 | 语义 |
|------|----|------|
| 已完成/暂停 | `processed_messages` | 重投时 replay result_cache |
| 已入队未完成 | `thread_msg_queue` unique message_id | 重投时不重复入队，直接 ACK |

### 9.3 result 可靠发布

当前可继续使用 `MQStreamBridge` 直接发布结果。若后续需要更强可靠性，建议引入 outbox：

```text
agent_result_outbox
  -> Runner 将 result/progress/error 写 DB
  -> producer loop 可靠发送 MQ
  -> 发送成功后标记 sent
```

这会进一步解决“Agent 已完成但 result MQ publish 失败”的边界问题。

---

## 10. 与旧版设计的主要差异

| 主题 | 旧版 | v2 |
|------|------|----|
| MQ 层 | poll-loop 入队后尝试 `_try_dispatch` | PollAck 只负责可靠入库和 ACK |
| 执行调度 | 入队时按 thread 触发 `_try_dispatch` | Scheduler 按空闲 slot 主动全局 claim |
| followup | 当前 run 结束后 `_drain_and_release` 链式执行 | followup 回到全局 queue，由 Scheduler 统一分发 |
| semaphore 满 | `_try_dispatch` return，可能缺少唤醒 | slot 释放必定 notify Scheduler，且有定时兜底 |
| HIL paused | 旧文档倾向 thread 回 idle 或实现不统一 | `thread_run_state` 回 idle；独立 gate 阻止普通 followup，只允许 resume/cancel |
| 负载均衡 | followup 倾向由当前 owner 继续 drain | 多实例 Scheduler 竞争 DB pending rows，自然均衡 |
| cancel | cancel_watcher + drain barrier | cancel_watcher + Scheduler barrier |

---

## 11. 实现建议

建议分阶段迁移：

1. 引入 `Scheduler` 类，但先复用现有 `AgentRunner` 和 `RunRegistry`。
2. 将 `_try_dispatch` 从 `TaskConsumer` 中移除，改为 PollAck 入库后 `scheduler.notify()`。
3. 将 `AgentRunner._drain_and_release` 改为 run 结束时只更新 thread 状态并 notify Scheduler。
4. 增加 `RunRegistry.claim_next_runnable(instance_id)`。
5. 为 `thread_msg_queue.message_id` 增加唯一约束，补入队幂等。
6. 增加独立 `thread_control_state.gate='hil_waiting'` 语义，明确 HIL resume claim 规则。
7. 为 cluster-level watchdog/cleanup 增加 advisory lock，降低重复扫描。
8. 后续再实现 collect、steer、outbox、LISTEN/NOTIFY、公平调度策略。

---

## 12. 待确认事项

- [ ] `hil_waiting` gate 下收到普通 followup，是一直等待 resume，还是允许客户端配置为 reject。
- [ ] cancel `hil_waiting` thread 时，是否需要 rollback checkpoint，还是只清理 gate 并标记相关 run cancelled。
- [ ] cancel barrier 中，被 cancel 的历史 followup 是否需要转 `prefix` 保留上下文，还是直接删除。
- [ ] collect 模式是否仍需要；如果需要，应改为 Scheduler claim 策略而非 `_drain_and_release`。
- [ ] 是否引入 result outbox 来保证下行结果发布的强可靠。
- [ ] 是否需要 per-user / per-project 并发限制，避免单个用户占满整个 Consumer 集群。
- [ ] PostgreSQL `claim_next_runnable` 的具体 SQL 需要针对实际 schema 做压测和 EXPLAIN。
