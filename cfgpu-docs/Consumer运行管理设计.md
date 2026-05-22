# Consumer 运行管理设计

DeerFlow Agent Server 的 Consumer 层运行管理，涵盖实例管理、任务路由、inject 机制和幂等性控制。

---

## 1. 整体架构

```
上游 ──► RocketMQ ($AGENT_TASKS,  普通消息)  ─────────────────────┐
上游 ──► RocketMQ ($AGENT_SIGNALS, 普通消息)  ──────────┐          │
                                                       │          │
        ┌──────────────────────────────────────────────▼──────────▼──────┐
        │                   Consumer 集群（N 个实例）                      │
        │                                                                 │
        │  ┌────────────────────┐   ┌─────────────────────────────────┐  │
        │  │  Signal Consumer   │   │        Task Consumer             │  │
        │  │  cancel / ping     │   │  task（含 HIL resume）           │  │
        │  │  （无容量限制）      │    │  （受 Semaphore 槽位节流）       │  │
        │  └────────┬───────────┘   └──────────────┬──────────────────┘  │
        │           │                              │                     │
        │           ▼                              ▼                     │
        │  ┌────────────────┐          ┌───────────────────┐            │
        │  │  TaskConsumer  │◄─────────│    AgentRunner     │            │
        │  │  handle_signal │          │  (LangGraph graph) │            │
        │  └────────────────┘          └─────────┬─────────┘            │
        │                                        │                      │
        │                           ┌────────────▼──────────┐           │
        │                           │   MQStreamBridge       │           │
        │                           │ (替换 MemoryStreamBridge)│          │
        │                           └────────────┬──────────┘           │
        │                                        │                      │
        └────────────────────────────────────────┼──────────────────────┘
                                                 │
                             ┌───────────────────▼──────────────┐
                             │     RocketMQ ($AGENT_RESULTS)     │
                             │   progress / result / error / pong│
                             └───────────────────────────────────┘

共享依赖（多节点共享）：
  PostgreSQL ── checkpointer + 运行状态表 + inject 队列 + 幂等记录
  共享文件系统 ── $DEER_FLOW_HOME（config / skills / agents / threads/user-data）
```

**不使用**：deerflow FastAPI gateway、nginx、前端、MemoryStreamBridge、RunManager（进程内字典）。

---

## 2. 数据库设计

### 2.1 延用 deerflow 的数据库配置方式

deerflow 已内置统一的数据库后端 `DatabaseConfig`，通过 `config.yaml` 中的 `database` 字段统一控制：

```yaml
database:
  backend: postgres          # multi-consumer 生产必选
  postgres_url: $DATABASE_URL  # 从 .env 读取
```

| backend | 适用场景 | 说明 |
|---------|---------|------|
| `sqlite` | 单节点开发/测试 | 文件存储于 `{sqlite_dir}/deerflow.db`（默认 `$DEER_FLOW_HOME/data/deerflow.db`），WAL 模式支持单进程并发读写 |
| `postgres` | **多 Consumer 生产** | 所有实例连接同一 PostgreSQL，连接池（默认 pool_size=5），真正并发安全 |
| `memory` | 单元测试 | 无持久化 |

**Multi-Consumer 部署必须使用 `backend: postgres`**。SQLite WAL 模式仅适合同一进程内的并发，多进程共享同一 SQLite 文件在 NFS 等网络文件系统上文件锁不可靠，容易导致数据库损坏。

### 2.2 所有表（共用同一数据库实例）

所有持久化组件共用同一个数据库（sqlite 或 postgres），由 `DatabaseConfig` 统一管理：

| 表 / Schema | 归属 | 创建方式 |
|------------|------|---------|
| `checkpoints`、`checkpoint_blobs`、`checkpoint_writes` | LangGraph `AsyncPostgresSaver` | `await saver.setup()` 在 Consumer 启动时自动创建 |
| `runs`、`threads_meta`、`run_events`、`feedback`、`users` | `deerflow.persistence.models` ORM | `Base.metadata.create_all` 在 `init_engine` 时自动创建 |
| `consumer_instances` | 本设计（消费者注册表） | 定义为 SQLAlchemy ORM model，随 `Base.metadata.create_all` 自动创建 |
| `thread_run_state` | 本设计（路由状态） | 同上 |
| `thread_msg_queue` | 本设计（inject 队列） | 同上 |
| `thread_cancel_signals` | 本设计（取消信号） | 同上 |
| `processed_messages` | 本设计（幂等记录） | 同上 |

本设计新增的 5 张自定义表定义为继承 `deerflow.persistence.base.Base` 的 SQLAlchemy ORM model，与 `runs`、`threads_meta` 等写法完全一致，随 Consumer 启动时 `init_engine_from_config(config.database)` 自动建表，无需单独的 migration 脚本（幂等安全）。RunRegistry 使用 `get_session_factory()` 获取 session，与其他 deerflow ORM 表共用同一 engine，自动跟随 `config.yaml` 的 `database.backend` 配置。

### 2.3 `SELECT FOR UPDATE` 在 SQLite 下的行为

RunRegistry 的原子 claim 依赖 `SELECT ... FOR UPDATE` 行级锁。SQLite 不支持行级锁，该语句会被静默忽略，但不影响正确性：

| 场景 | 结论 |
|------|------|
| `sqlite` + 单 Consumer（开发模式） | 只有一个进程，不存在并发 claim，no-op 无影响，行为正确 |
| `sqlite` + 多 Consumer | 不安全，禁止此配置（见 2.1） |
| `postgres` + 多 Consumer | 行级锁正常生效，原子 claim 有保障 |

### 2.4 并发安全分析

**Checkpointer 写冲突**：

同一 thread_id 在同一时刻只有一个 Consumer 实例持有执行权（由 `thread_run_state` 的 `SELECT FOR UPDATE` 原子 claim 保证）。因此：
- 正常运行时，同一 thread 的 checkpoint 行只有一个写入者 → 无行级写冲突
- 不同 thread 的 checkpoint 完全独立的行 → PostgreSQL MVCC 无争用

**thread_run_state claim 并发**：

多个 Consumer 可能同时收到同一 thread_id 的消息：
```sql
-- 原子 claim：PostgreSQL SELECT FOR UPDATE 保证只有一个事务成功写入
SELECT * FROM thread_run_state WHERE thread_id=T FOR UPDATE;
```
PostgreSQL 行级锁确保只有一个事务的 UPSERT 生效，其余等待后读到 `status='running'` 并走 inject 路径。

**inject 队列消费并发**：

InjectMiddleware 消费 inject 队列时通过原子更新防止重复消费：
```sql
UPDATE thread_msg_queue SET consumed_at=now()
WHERE thread_id=T AND consumed_at IS NULL
RETURNING payload;
```
PostgreSQL 保证此 UPDATE 的原子性，即使多个进程并发执行（实际上 claim 机制已确保只有一个实例执行该 thread）。

**连接池配置建议**：

每个 Consumer 实例维护独立连接池。建议按并发 thread 数调整：
```yaml
database:
  pool_size: 10    # 每个 Consumer 实例的连接池大小（默认 5）
```
N 个 Consumer 实例 × pool_size = PostgreSQL 总连接数，需在 PostgreSQL `max_connections` 范围内。

---

## 3. Consumer 实例管理

### 3.1 instance_id

每个 Consumer 进程启动时生成唯一实例 ID：

```
instance_id = "{hostname}-{pid}"
```

示例：`worker-node01-12345`

### 3.2 实例注册表（PostgreSQL）

```sql
CREATE TABLE consumer_instances (
    instance_id    TEXT PRIMARY KEY,
    hostname       TEXT NOT NULL,
    pid            INT  NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    -- status: active | draining | dead
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL
);
```

- **启动时**：INSERT 自身记录，status=active
- **运行中**：每 10 秒 UPDATE last_heartbeat
- **正常下线**：UPDATE status='draining'，等待当前 run 完成后 DELETE
- **异常宕机**：heartbeat 超过 60 秒未更新 → 其他实例将其标记为 dead，接管其遗留 run

### 3.3 Scale in/out

| 事件 | 操作 |
|------|------|
| 新实例启动（scale out） | 注册 consumer_instances，开始消费 |
| 实例优雅下线（scale in） | status=draining，停止接收新 task，完成当前 run，DELETE 记录 |
| 实例宕机 | 另一实例检测 heartbeat 超时后标记 dead，遗留 run 可被重新调度 |

### 3.4 单 Consumer 实例内部架构

每个 Consumer 实例是一个**单进程 + asyncio 事件循环**，进程内不再派生子进程。所有并发由协程（coroutine）和 asyncio 任务实现；唯一的线程池用于调用阻塞式 RocketMQ SDK。

#### 3.4.1 进程内任务一览

```
Consumer 进程（单 asyncio 事件循环）
│
├── [固定后台协程，进程启动时创建]
│   ├── task-poll-loop              拉取 $AGENT_TASKS；槽位耗尽时暂停（capacity=0 → sleep 0.2s）
│   ├── signal-poll-loop            拉取 $AGENT_SIGNALS（cancel/ping）；无槽位限制，始终拉取
│   ├── instance-heartbeat          每 10 s → UPDATE consumer_instances.last_heartbeat
│   ├── stale-run-watchdog          每 30 s → 检测并重置心跳超时（>60 s）的 running thread
│   └── processed-messages-cleanup  每 3600 s → 删除超过 TTL（默认 7 天）的幂等记录
│                                   （processed_messages_ttl_days=0 时不启动）
│
├── [动态消息任务，每条 MQ 消息创建一个]
│   ├── msg-<id>   来自 task-poll-loop（类型一定是 task）
│   │              ├── task_consumer.handle_message(body)
│   │              └── task_mq_consumer.ack(msg)
│   └── sig-<id>   来自 signal-poll-loop（类型一定是 cancel 或 ping）
│                  ├── task_consumer.handle_message(body)
│                  └── signal_mq_consumer.ack(msg)
│
└── [动态 Agent 运行任务，每个 claim 成功的 task 创建一个]
    └── run-<id>   AgentRunner.run()
                   ├── heartbeat_loop   每 10 s → UPDATE thread_run_state.last_heartbeat
                   ├── cancel_watcher   每  2 s → SELECT thread_cancel_signals → task.cancel()
                   └── graph.astream()  驱动 LangGraph，输出通过 MQStreamBridge 发布
```

#### 3.4.2 并发控制机制

| 机制 | 作用 |
|------|------|
| `asyncio.Semaphore(max_concurrent_runs)` | 限制同时执行的 Agent run 任务数量（默认 10）；`_run_and_release` 包装每次 run，完成后自动释放槽位 |
| task-poll-loop 槽位节流 | `capacity = min(task_batch_size, available_slots)`；`available_slots == 0` 时 sleep 0.2s 跳过本轮，不拉取新 task，防止任务消息积压 |
| signal-poll-loop 无节流 | cancel/ping 使用独立 SimpleConsumer，不受 Semaphore 约束，始终可消费；cancel 响应延迟不受 task 负载影响 |
| ACK 与 run 解耦 | `handle_message` 返回后立即 ACK，run 作为独立后台 Task 继续执行；poll loop 无需等待 run 完成即可接受下一批消息，吞吐不受单次 run 时长影响 |
| `asyncio.create_task()` | msg 处理（handle + ack）与 run 执行均为独立 Task，互不阻塞；asyncio 事件循环单线程，内存状态（`_runs` 字典、Semaphore）无需额外锁 |
| `ThreadPoolExecutor(max_workers=6)` | 所有阻塞 SDK 调用通过 `run_in_executor` 在线程池执行；6 workers = task-receive(1) + signal-receive(1) + send(1) + ack×2 + spare(1) |

#### 3.4.3 后台协程的 per-instance vs cluster-level 分类

并非所有后台协程都需要在每个 Consumer 实例上独立运行：

| 协程 | 归属 | 原因 |
|------|------|------|
| `task-poll-loop` | **per-instance** | 每个实例独立消费自己的 MQ 份额 |
| `signal-poll-loop` | **per-instance** | 同上 |
| `instance-heartbeat` | **per-instance** | 更新自身的 `consumer_instances.last_heartbeat`，只有自己能做 |
| `stale-run-watchdog` | cluster-level | 整个集群只需一个实例检测并重置 stale run |
| `processed-messages-cleanup` | cluster-level | 只需一个实例执行定期清理 |

**当前实现**：所有实例运行所有协程。cluster-level 协程的 DB 操作均为幂等（watchdog 的 `UPDATE ... WHERE status='running' AND heartbeat < cutoff` 在行已被重置后命中空；cleanup 的 `DELETE` 对已删行无副作用），正确性不受影响，仅产生 N-1 次无效 DB 操作。

**已知改进方向**：对 cluster-level 协程引入 PostgreSQL Advisory Lock（`pg_try_advisory_lock`），持锁实例执行，其他实例跳过；锁随连接关闭自动释放，Consumer 崩溃后无泄漏风险。当前集群规模小，暂不实现，待后续评估。

#### 3.4.4 容量估算

```
同时运行的协程数 ≈ 5（固定后台，含 processed-messages-cleanup；ttl_days=0 时为 4）
                 + min(task_batch_size, max_concurrent_runs)（task msg 任务，短暂存活）
                 + signal_batch_size（signal msg 任务，极短暂存活）
                 + max_concurrent_runs × 3（每个 run：run 主体 + heartbeat + cancel_watcher）

线程数 = ThreadPoolExecutor.max_workers（默认 6，固定不变）
```

例：`max_concurrent_runs=10`、`task_batch_size=20`、`signal_batch_size=10` 时，协程峰值约 **55**，线程数固定 **6**。

---

## 4. 任务路由

### 4.1 为什么不用 RocketMQ 顺序消息

顺序消息（FIFO）保证同一 MessageGroup 的消息串行投递到同一消费队列，但：
- 若队列对应的消费者繁忙，同 thread 的新消息会阻塞等待，影响响应延迟
- 消费者宕机时该队列的消息会积压，无法被其他消费者接管

因此使用**普通消息**，由服务层主动管理 thread → 实例的路由亲和性。

### 4.2 Thread 运行状态表（PostgreSQL）

```sql
CREATE TABLE thread_run_state (
    thread_id      TEXT PRIMARY KEY,
    instance_id    TEXT NOT NULL,        -- 当前持有执行权的 Consumer 实例；无 FK，dead 实例行会被删除
    message_id     TEXT NOT NULL,        -- 当前运行中 task 的 message_id
    status         TEXT NOT NULL,        -- running | idle （仅两个状态）
    drain_mode     TEXT NOT NULL DEFAULT 'followup',  -- followup | collect（见 §5.3）
    retry_count    INT  NOT NULL DEFAULT 0,  -- stale run 自动重试次数，成功后归零
    started_at     TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ NOT NULL
);
-- reply_config 已移至 thread_msg_queue.body（完整 MQ envelope 中）
-- paused_for_approval 状态已移除（见下方说明）

CREATE TABLE thread_cancel_signals (
    thread_id    TEXT PRIMARY KEY,
    reason       TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**字段变更说明：**

| 变更 | 原因 |
|------|------|
| 删除 `reply_config` | 完整 MQ body（含 reply_config）已存入 `thread_msg_queue`（policy='current'），无需冗余存储 |
| 新增 `retry_count` | 记录 stale run 自动重试次数，防止无限 crash 循环；成功完成后归零 |
| 删除 `paused_for_approval` 状态和 `paused_reason` 字段 | HIL interrupt 后 agent 正常结束，thread 直接回 `idle`；LangGraph checkpoint 保存 interrupt 状态，resume 消息以新消息方式处理；线程管理层无需感知 HIL 等待语义 |
| 新增 `drain_mode` | 控制 followup 队列的排空策略（`followup` \| `collect`）；per-thread 粒度，由 collect 消息入队时写入，`mark_thread_idle` 时重置为 `followup` |

### 4.3 路由算法

Consumer 接收到一条消息后的处理流程：

```
收到消息 M（raw body）

// ─── 步骤 ①：JSON 解析 ───────────────────────────────────────────────────────
try: raw_envelope = json.loads(body)
except JSONDecodeError:
    log error; return   // 无 message_id，无法回发，静默 ACK

message_id = raw_envelope.get("message_id")   // 供后续 error 回发使用
thread_id  = raw_envelope.get("thread_id")

// ─── 步骤 ②：Schema 校验 + 反序列化 ─────────────────────────────────────────
try: message = TaskMessage.from_dict(raw_envelope)
except SchemaValidationError as e:
    log error(message_id, e.reason)
    if message_id:
        publish error(INVALID_SCHEMA, retriable=False, message=e.reason) → $AGENT_RESULTS
    return   // ACK，不重投

// ─── 步骤 ③：按 type 路由 ────────────────────────────────────────────────────
// _validate_raw 已保证 type ∈ {"task","cancel","ping"}，无需 else 分支

if type == ping:
    target = M.config.get("instance_id")
    if target:
        // 定向 ping：查 consumer_instances 表，任意 Consumer 均可回答
        row = SELECT FROM consumer_instances WHERE instance_id=target
        target_status = row.status if row else "not_found"
        last_heartbeat = row.last_heartbeat if row else null
        publish pong(
            instance_id=self.instance_id,        // 实际处理这条 ping 的 Consumer
            target_instance_id=target,
            target_status=target_status,         // active | draining | not_found
            last_heartbeat=last_heartbeat,
        )
    else:
        // 广播 ping：只报告自身
        publish pong(instance_id=self.instance_id)
    return                                       // handle_message 返回 → 立即 ACK

if type == cancel:
    INSERT INTO thread_cancel_signals(T, reason)  -- 所有实例都写；执行实例轮询检测
    return

if type == task:
    // ① 幂等检查
    if message_id 已在 processed_messages 表中:
        （可选）重发已缓存的 result
        MQ ack; return

    // ② 尝试原子 claim
    result = 执行以下 PostgreSQL 事务：
        SELECT * FROM thread_run_state WHERE thread_id=T FOR UPDATE
        if 不存在 or status='idle':
            UPSERT thread_run_state SET
                instance_id=self, message_id=M.id,
                status='running', reply_config=M.reply_config,
                started_at=now(), last_heartbeat=now()
            return "claimed"
        elif status='running':
            return "running_on", existing.instance_id

    if result == "claimed":
        // ② 写入 "current" 行：存完整 MQ body，供 stale run 恢复使用
        UPSERT thread_msg_queue:
            DELETE FROM thread_msg_queue WHERE thread_id=T AND policy='current'
            INSERT INTO thread_msg_queue(T, M.message_id, body=raw_envelope, policy='current')
        启动 AgentRunner 执行 M

    if result == ("running_on", other_instance):
        mode = M.payload.config.get("message_mode", "followup")

        if mode == "reject":
            // ③-a reject：拒绝，立即回复 AGENT_BUSY
            publish error(AGENT_BUSY, retriable=True) → $AGENT_RESULTS
            MQ ack; return

        else:  // followup / collect；steer 协议已预留，当前版本降级为 followup
            // ③-b followup / collect：写入 inject 队列，立即 ack 释放 consumer slot
            // body 存完整 MQ envelope，_drain_and_release 在当前 run 完成后驱动执行
            INSERT INTO thread_msg_queue(T, M.message_id, body=raw_envelope, policy='followup')
            if mode == "collect":
                // collect 是 per-thread 设置：原子写入 drain_mode（SELECT FOR UPDATE 保护）
                UPDATE thread_run_state SET drain_mode='collect' WHERE thread_id=T
                // 注意：无需原子事务包裹 INSERT + UPDATE——即使顺序分开，
                // drain_mode 只在 drain 时被读取（run 结束后），此时写入已完成
            MQ ack; return

### 4.4 Heartbeat 与 Stale Run 恢复

**心跳更新责任方：**

| heartbeat 字段 | 更新者 | 生命周期 | 含义 |
|---|---|---|---|
| `thread_run_state.last_heartbeat` | `AgentRunner._heartbeat_loop`（每 10s） | 随单个 run 存活，run 结束时协程取消 | 这个 **run** 还在运行 |
| `consumer_instances.last_heartbeat` | `_instance_heartbeat_loop`（每 10s） | 随整个 Consumer 进程存活 | 这个 **进程** 还存活 |

**Stale run 检测（双 heartbeat 交叉验证）：**

后台守护任务（每 30 秒）执行：

```sql
SELECT trs.*
FROM thread_run_state trs
WHERE trs.status = 'running'
  AND trs.last_heartbeat < now() - interval '60 seconds'
  AND NOT EXISTS (
      SELECT 1 FROM consumer_instances ci
      WHERE ci.instance_id = trs.instance_id
        AND ci.last_heartbeat >= now() - interval '60 seconds'
  )
```

仅当 **run 心跳** 和 **实例心跳** 都超时才判定为 stale，避免 `heartbeat_loop` 协程自身异常（进程存活但 run 心跳停止）导致的误判。

**Stale run 恢复流程：**

```
detected stale run: thread_id=T, instance_id=X（dead）, message_id=M
│
├─ 查 thread_msg_queue WHERE thread_id=T AND policy='current'
│   → current_row（含完整 MQ envelope：agent_name, user_id, project_id, config, reply_config...）
│
├─ check processed_messages(M)
│   │
│   ├─ 已有记录（run 在 crash 前已完成，result 已发出）
│   │   → mark_thread_idle(T)
│   │   → 触发 drain：检查 followup queue，有则作为独立新 run 继续执行
│   │
│   └─ 无记录（run 中途 crash，result 未发出）
│       ├─ retry_count < MAX_RETRIES（默认 3）
│       │   → claim_stale_run(T, this_instance)   ← SELECT FOR UPDATE，多实例竞争只有一个赢
│       │   → TaskMessage.from_json(current_row.body)
│       │   → AgentRunner.run(message)
│       │       LangGraph 从 checkpoint 恢复（thread_id 相同，checkpoint 记录断点前所有 state）
│       │   → thread_run_state.retry_count += 1
│       │
│       └─ retry_count >= MAX_RETRIES
│           → publish error(FATAL, retriable=false) → $AGENT_RESULTS
│           → mark_thread_idle(T)（放弃，等待上游决策）
```

**"current" 行生命周期：**

| 事件 | `thread_msg_queue` 中 policy='current' 的行 |
|------|---------------------------------------------|
| `claim_thread` 成功（新 message_id） | UPSERT：删旧行，插入完整 body |
| HIL interrupt → agent 正常结束 → `mark_thread_idle` | DELETE（与普通 run 完成相同路径） |
| HIL resume 到来（新消息）→ `claim_thread` | UPSERT：resume envelope 替换旧行，正常执行 |
| stale retry 重新 claim（同 message_id） | UPSERT：body 不变，等同于幂等刷新 |
| run 完成 → `mark_thread_idle` | DELETE（thread 空闲，"current" 已无恢复价值） |

> **注意**：`reply_config`、`agent_name`、`user_id`、`project_id` 均包含在完整 body 中，stale retry 无需原始 MQ 消息即可完整重建 `TaskMessage`。LangGraph checkpoint 记录了 run 中断前所有 state，`graph.astream(None, config={"thread_id": T})` 从断点续跑。

**为什么不依赖上游重发：**

Consumer-internal retry 相比上游重发的优势：
- 无 round-trip 延迟，恢复更快
- 上游无需实现 retry 逻辑，降低耦合
- LangGraph checkpoint 本为此设计，resume 语义天然对应

超过 MAX_RETRIES 后降级为 `error(FATAL)` 通知上游，上游可决策是否重试或放弃。

---

## 5. Inject 机制

### 5.1 inject 队列（PostgreSQL）

```sql
CREATE TABLE thread_msg_queue (
    id          BIGSERIAL PRIMARY KEY,
    thread_id   TEXT NOT NULL,
    message_id  TEXT NOT NULL,
    body        JSONB NOT NULL,
    -- 完整 MQ envelope（schema_version + message_id + agent_name +
    --   user_id + project_id + payload.{messages,command,config,reply_config}）
    -- 取代原 payload 列（仅存 payload 子集），不再有信息丢失
    policy      TEXT NOT NULL DEFAULT 'followup',  -- 'current' | 'followup' | 'steer'
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at TIMESTAMPTZ               -- 已废弃，保留列但不再写入；followup 行消费时直接 DELETE
);

CREATE INDEX ON thread_msg_queue(thread_id, policy, consumed_at) WHERE consumed_at IS NULL;
```

`policy` 列区分三种用途：

| policy | 状态 | 写入时机 | 消费者 | 消费时机 |
|--------|------|---------|--------|---------|
| `current` | **已设计** | `claim_thread` 成功时 UPSERT（每 thread 至多一行） | `stale-run-watchdog` | crash 恢复时读取，`mark_thread_idle` 时删除 |
| `followup` | **已实现** | thread 正在运行时新消息到来（message_mode=followup） | `_drain_and_release` | 当前 run 完成后，作为独立新一轮的 input；消费时 DELETE 行（不使用 consumed_at 软删除） |
| `steer` | **待实现** | — | InjectMiddleware（预留） | 当前 run 的 before_agent / before_model |

**`body` 列存储完整 MQ envelope 的原因：**

原 `payload` 列只存 `{messages, command, config, reply_config}`，丢弃了 envelope 层的 `agent_name`、`user_id`、`project_id` 等字段。`agent_name` 决定加载哪个 agent 定义（graph 结构），`user_id`/`project_id` 是 multi-level memory 的必要上下文。改为存完整 body 后，`TaskMessage.from_json(row.body)` 可无损重建，无需任何额外参数。

### 5.2 InjectMiddleware（预留，steer 待实现）

`InjectMiddleware` 为 `steer` 模式预留，当前版本**不实现**。

设计意图：挂载 `before_agent`（node 边界）和 `before_model`（LLM 调用前）两个钩子，从 inject 队列取出 `policy='steer'` 的消息并注入 state.messages，使 Agent 在当前 run 内感知新输入。

实现时需在 middleware 链**最前**插入，确保 steer 消息在 DynamicContextMiddleware 处理前已加入 state。cancel 检测不在 middleware 中处理（由 cancel watcher 负责，见第 7 节）。

### 5.3 Drain-before-Idle：驱动 followup/collect 后续执行

**问题（空占 + 轮询）**：若 followup/collect 消息留在 MQ 中等待 thread idle，consumer 需要持有消息等待（空占 slot）并不断轮询 `thread_run_state`，浪费资源。

**解决方案**：入队消息**写入 inject 队列，立即 ack MQ**，释放 consumer slot。AgentRunner 完成 run 后，由 `_drain_and_release` 主动检查队列并驱动后续执行——由完成事件触发，无需轮询。

**两种排空策略**（由 thread 级别的 `drain_mode` 控制，见 §5.5）：

- **followup**（默认）：每次取队列中最早一条，作为独立新一轮执行；新 run 的 `finally` 再次触发 `_drain_and_release`，自然顺序排空。
- **collect**：一次取出所有待消费 followup，将其 messages 合并后作为单次 run 的完整 input，仅发起一轮执行。

```python
async def _drain_and_release(self, thread_id: str):
    """完成当前 run 后，按 drain_mode 驱动后续执行，或释放 thread。"""
    pending = await self._registry.peek_inject_queue(thread_id, policy='followup')

    if not pending:
        await self._registry.mark_thread_idle(thread_id)   # 同时重置 drain_mode='followup'
        return

    drain_mode = await self._registry.get_drain_mode(thread_id)

    if drain_mode == 'followup':
        # 取最早一条，以其完整 envelope 发起独立新一轮
        next_row = pending[0]
        next_task = TaskMessage.from_json(json.dumps(next_row.body))
        await self._registry.transition_thread_followup(
            thread_id, next_row.id, next_row.message_id, next_row.body
        )
        # transition_thread_followup 在同一事务内：
        #   DELETE followup 行（id == next_row.id）
        #   DELETE 旧 current 行
        #   INSERT 新 current 行（完整 body）
        #   UPDATE thread_run_state.message_id / started_at / last_heartbeat
        asyncio.create_task(self.run(next_task))
        # 新 run 的 finally 再次调用 _drain_and_release，链式排空

    else:  # collect
        # 一次取出全部待消费行，合并 messages，发起单次 run
        merged_messages = [
            msg
            for row in pending
            for msg in row.body["payload"]["messages"]
        ]
        # reply_config、agent_name、user_id 取最早一条（FIFO）
        first_row = pending[0]
        last_row  = pending[-1]
        collect_task = TaskMessage.from_json(json.dumps(first_row.body))
        collect_task = collect_task.with_messages(merged_messages)

        # 原子标记全部行已消费，并将 current 更新为最后一条 envelope
        # （stale recovery 用 last_row 的 agent_name/user_id/reply_config；
        #  LangGraph 从 checkpoint 续跑，merged input 无需持久化）
        await self._registry.transition_thread_collect(
            thread_id, pending, last_row.message_id, last_row.body
        )
        asyncio.create_task(self.run(collect_task))
        # collect run 完成后 _drain_and_release 再次检查：
        #   若有新积压 followup → 继续按当时的 drain_mode 处理
        #   若队列空 → mark_thread_idle（drain_mode 重置为 followup）
```

**`mark_thread_idle` 负责重置 `drain_mode`**，保证 thread 空闲后 drain_mode 始终回到默认值 `followup`，不会跨会话污染下一轮 run：

```sql
UPDATE thread_run_state
SET status='idle', drain_mode='followup'
WHERE thread_id = :thread_id
```

**`transition_thread_collect`** 相比 `transition_thread_followup` 需要额外原子标记多行：

```sql
-- 原子标记所有待消费行（单条 SQL，避免循环）
UPDATE thread_msg_queue
SET consumed_at = now()
WHERE thread_id = :thread_id
  AND policy = 'followup'
  AND consumed_at IS NULL
  AND id IN :all_pending_ids;

-- 更新 current（供 stale recovery 读取 agent_name/user_id 等 envelope 元数据）
DELETE FROM thread_msg_queue WHERE thread_id = :thread_id AND policy = 'current';
INSERT INTO thread_msg_queue(thread_id, message_id, body, policy)
VALUES (:thread_id, :last_message_id, :last_body, 'current');

-- 推进 thread_run_state.message_id 至新 run_id（collect run 的合成 run_id）
UPDATE thread_run_state
SET message_id = :collect_run_id, started_at = now(), last_heartbeat = now()
WHERE thread_id = :thread_id;
```

**stale recovery 对 collect run 的处理**：collect run 中途 crash 时，`current` 行保存的是最后一条 followup 的 envelope，`message_id` 为合成 `collect_run_id`。watchdog 从 checkpoint 续跑时无需原始 merged input——LangGraph 从断点恢复，envelope 中的 `agent_name`、`user_id`、`reply_config` 足够重建 `TaskMessage`。

### 5.4 与现有 middleware 的关系

当前 middleware 链（不含待实现的 InjectMiddleware）：

```
[DynamicContextMiddleware]  ← 注入 memory + 日期
[UploadsMiddleware]         ← 注入上传文件信息
[ThreadDataMiddleware]      ← 确保 sandbox 目录存在
... 其他 middleware
// 预留：[InjectMiddleware] 实现 steer 时插入链首
```

**Cancel 不在 middleware 中处理**：cancel 由 AgentRunner 的 cancel watcher 协程负责，通过 `asyncio.Task.cancel()` 在任意 `await` 点生效，与 inject 逻辑完全解耦。

### 5.5 message_mode 与并发行为

`message_mode` 控制 thread 繁忙时新消息的处置策略，分为两类：

**per-message 设置**（由客户端在 task payload 的 `config` 中声明）：

```json
{ "config": { "message_mode": "followup" } }
```

优先级：`task.config.message_mode` > `config.yaml` 中 `consumer.agent_policies.<agent_name>` > `consumer.message_mode`（全局默认）> 硬编码默认值 `followup`。

**per-thread 设置**（`drain_mode`，控制 followup 队列的排空策略）：

`collect` 模式需要 per-thread 粒度，因为同一 thread 的所有入队消息都由同一个 drain 逻辑消费，不同 message 的 `message_mode` 可能不一致（例如混入了 followup 和 collect），而排空时已无原始消息的 config 可读。`drain_mode` 的配置方式待设计（见 §9）。

| 场景 | message_mode / drain_mode | 行为 | 状态 |
|------|--------------------------|------|------|
| thread 空闲，新 task 到来 | 任意 | AgentRunner claim 并执行新一轮，checkpoint 从上次结束状态恢复 | 已实现 |
| thread 运行中，新 task 到来 | `followup`（**默认**） | 写入 inject 队列（policy='followup'），立即 ack MQ；`_drain_and_release` 在当前 run 完成后逐条顺序执行 | 已实现 |
| thread 运行中，新 task 到来 | `collect` | 写入 inject 队列（policy='followup'），立即 ack MQ；`_drain_and_release` 在当前 run 完成后一次取出全部待消费行，合并 messages 为单次 run input | **待实现** |
| thread 运行中，新 task 到来 | `reject` | 立即回复 `AGENT_BUSY` error，不入队，MQ ack | 已实现 |
| thread 运行中，新 task 到来 | `steer` | 当前降级为 `followup`；待实现：InjectMiddleware 在下一个 before_agent/before_model 注入 | **待实现** |
| thread 完成，followup 队列有残留，drain_mode=followup | — | `_drain_and_release` 取最早一条，发起新一轮；链式排空 | 已实现 |
| thread 完成，followup 队列有残留，drain_mode=collect | — | `_drain_and_release` 取全部待消费行，合并 messages，发起单次 run | **待实现** |

---

## 6. 幂等性控制

### 6.1 已处理消息记录

```sql
CREATE TABLE processed_messages (
    message_id   TEXT PRIMARY KEY,
    thread_id    TEXT NOT NULL,
    status       TEXT NOT NULL,    -- completed | failed | cancelled | paused_for_approval
    -- paused_for_approval：该 run 触发了 HIL interrupt；thread 本身已回 idle，
    --   此状态仅用于 per-run 幂等追踪，不代表线程被挂起
    result_cache JSONB,            -- 缓存 result payload，用于重发（见下表）
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**result_cache 各状态内容**：

| status | result_cache |
|--------|-------------|
| `completed` | `{"status": "success", "final_state": {"messages": [<thread全量消息>], "title": "...", "artifacts": [...], ...}}` |
| `paused_for_approval` | `{"status": "paused_for_approval", "tool_approval_required": {...}}` |
| `cancelled` | `{"status": "cancelled"}` |
| `failed`（超时） | `{"error": {"code": "AGENT_TIMEOUT", "retriable": true, "message": "..."}}` |
| `failed`（异常） | `{"error": {"code": "INTERNAL_ERROR", "retriable": false, "message": "..."}}` |

> **注意**：schema 校验失败（`INVALID_SCHEMA` error）在消息进入路由前就被拒绝，**不写入 `processed_messages`**，也不会被幂等查询命中。`INVALID_SCHEMA` 属于客户端协议错误，无需重试记录；`retriable=false` 通知上游修正后重发。

**下行 error 错误码汇总**（`$AGENT_RESULTS` topic）：

| 错误码 | 触发阶段 | retriable | 说明 |
|--------|---------|-----------|------|
| `INVALID_SCHEMA` | 消息路由前（`handle_message`） | false | MQ 消息 schema 不合法（字段缺失 / 类型错误 / 版本不匹配），客户端协议错误 |
| `AGENT_BUSY` | 路由（task 占用，`message_mode=reject`） | true | thread 正在运行，消息被拒绝；客户端可稍后重发或改用 followup |
| `AGENT_TIMEOUT` | 执行（超出 `config.timeout_seconds`） | true | 执行超时；LangGraph checkpoint 保留，重发可从断点续跑 |
| `INTERNAL_ERROR` | 执行（未知异常） | false | Consumer 内部错误；可人工介入后以新 message_id 重试 |

`paused_for_approval` 的 `tool_approval_required` 字段包含待审批工具调用信息，replay 时会先以 `progress custom` 事件推送该字段，再推送 result 信封，使客户端能重建审批界面。`final_state` 序列化失败时 `completed` 状态的 cache 中该字段缺失；中间件未产生 `tool_approval_required` 事件时 HIL cache 中该字段缺失。

**`final_state.messages` 为全量**：`result.final_state` 包含 run 完成后 thread checkpoint 的完整 messages 列表（不做增量过滤），`title`、`artifacts` 等字段同样为 thread 当前全量状态。客户端通过流式 `progress(messages)` events 收取增量内容；`result.final_state` 作为完整快照，主要用于幂等 replay（重复投递时重建完整状态）。在 stale retry 场景下（checkpoint 已有消息，run 直接从断点恢复），全量方案同样能正确返回 messages，不会出现空数组。

### 6.2 幂等处理流程

```
收到 task 消息（message_id=X）：
  ├─ SELECT FROM processed_messages WHERE message_id=X
  │     ├─ 找到（任意 status）→ bridge.replay(message_id, thread_id, result_cache)
  │     │     ├─ result_cache 有 tool_approval_required → 先发 progress custom 事件
  │     │     └─ 发 result 或 error 信封
  │     │   MQ ack，return
  │     └─ 未找到 → 继续正常处理

执行完成后：
  INSERT INTO processed_messages(X, thread_id, status, result_cache)
  （result_cache = None 时静默丢弃，不 replay）
```

> **注意**：`failed` 状态下重发同一 `message_id` 只会 replay error 信封，不会重新执行。如需重试，客户端应发送新的 `message_id`。

### 6.3 MQ ack 时机

**设计原则：handle_message 返回后立即 ACK，不等 agent 跑完。**

```
RocketMQ                   Consumer
   │                          │
   │── msg delivered ───►     │
   │                    handle_message()
   │                          ├─ ping/cancel：处理完毕，return
   │                          ├─ task（busy）：入队/拒绝，return
   │                          └─ task（claimed）：claim 写 DB，create_task(run)，return
   │◄── ACK ──────────────    │   ← handle_message 返回后立刻 ack，与 run 是否完成无关
   │                          │
   │                          │   run 在后台继续执行...
   │                          │         ├─ heartbeat_loop
   │                          │         ├─ cancel_watcher
   │                          │         └─ graph.astream()
   │◄═══ $AGENT_RESULTS ══════════════════╝  结果单独发回，与 ACK 完全解耦
```

各场景 ack 时机：

| 场景 | ack 时机 |
|------|---------|
| JSON 解析失败 | log error，`handle_message` 返回，立即 ack（无 message_id，无法回发） |
| schema 校验失败（有 message_id） | 发布 `INVALID_SCHEMA` error，`handle_message` 返回，立即 ack |
| schema 校验失败（无 message_id） | log error，`handle_message` 返回，立即 ack（无法回发） |
| ping / cancel | 消息处理完毕（pong 发送 / cancel 信号写 DB）后，`handle_message` 返回，立即 ack |
| task（claimed） | DB claim 写入、后台 run 任务创建后，`handle_message` 返回，立即 ack；run 继续在后台执行 |
| task（inject） | followup 写入 `thread_msg_queue` 后，`handle_message` 返回，立即 ack |
| task（reject） | `AGENT_BUSY` error 发布后，`handle_message` 返回，立即 ack |
| 任何异常 | `except Exception` 捕获并记录日志，`finally` 块无条件 ack，不触发重投 |

**为什么不等 agent 跑完再 ack？**

RocketMQ SimpleConsumer（POP 模式）的 `invisible_duration` 有上限。agent 可能运行数分钟，等待期间需要不断 `changeInvisibleDuration` 续租，逻辑复杂且容易因网络抖动导致消息重投。当前设计将"消息交付"与"任务执行"解耦：MQ 只负责将消息可靠投递给某个 Consumer，执行结果通过 `$AGENT_RESULTS` topic 单独返回。

**ack 后 Consumer 崩溃的处理：**

若 Consumer 在 ack 后、run 完成前崩溃，`thread_run_state` 仍显示 `running`，但 heartbeat 不再更新。`_stale_run_watchdog` 在 60 秒后检测到超时，从 `thread_msg_queue`（policy='current'）读取完整 body，由另一个 Consumer 直接从 LangGraph checkpoint 续跑，无需上游重发（见 §4.4）。

Consumer 端不依赖 RocketMQ 的去重能力，自行维护幂等记录（`processed_messages` 表）。

### 6.4 processed_messages TTL 清理

**有效窗口分析：**

`processed_messages` 记录的唯一查询时机是：ACK 失败 → 消息在 `invisible_duration_seconds`（默认 300s）后重投 → `check_processed` 查询一次。之后：

- ACK 成功的消息：MQ 已删除，record 永远不会被查（绝大多数情况）
- 重投的消息：查询一次后不再查

因此记录在 `invisible_duration_seconds` 过后即无幂等价值，但保留数天便于排查问题（对应 §4.4 crash 场景的 run_id 追踪）。

**推荐 TTL：7 天**（远大于 `invisible_duration_seconds`，兼顾运维排查）。

**清理方案：在 Consumer 进程内增加后台清理协程。**

```python
async def _processed_messages_cleanup(
    registry: RunRegistry, ttl_days: int = 7, interval: int = 3600
) -> None:
    """每小时清理超过 TTL 的 processed_messages 记录。"""
    while True:
        await asyncio.sleep(interval)
        try:
            deleted = await registry.cleanup_processed_messages(ttl_days)
            if deleted:
                logger.info("Cleaned up %d expired processed_messages records", deleted)
        except Exception:
            logger.debug("processed_messages cleanup error", exc_info=True)
```

`RunRegistry.cleanup_processed_messages(ttl_days)` 执行：

```sql
DELETE FROM processed_messages
WHERE processed_at < now() - interval '{ttl_days} days'
RETURNING message_id;
```

**配置方式**（`config.yaml`）：

```yaml
consumer:
  processed_messages_ttl_days: 7    # 默认 7 天，0 = 不清理
```

清理协程 `_processed_messages_cleanup` 在 Consumer 启动时加入 bg_tasks（`ttl_days=0` 时跳过），`RunRegistry.cleanup_processed_messages()` 执行实际删除并返回行数。

**不建议依赖数据库级 TTL**（如 pg_cron）：Consumer 进程自管理避免运维方引入额外组件依赖，且清理逻辑与 `invisible_duration_seconds` 配置保持一致更容易维护。

---

## 7. AgentRunner 核心结构

### 7.1 Cancel 机制：Cancel Watcher + asyncio.Task.cancel()

**deerflow 的 cancel 机制**（worker.py）：
- `RunManager.cancel()` 同时做两件事：设置 `abort_event`（供 chunk 间检查），并调用 `task.cancel()`（向 asyncio Task 注入 `CancelledError`）
- `CancelledError` 在 Task 下一个 `await` 点立即生效，包括 LLM 调用、tool 调用、DB 查询等任意 IO 等待点

**我们的方案**：cancel 信号来自 PostgreSQL，需要轮询检测，但取消动作复用相同的 `asyncio.Task.cancel()` 机制。在 AgentRunner 内启动独立的 **cancel watcher** 协程，每隔 N 秒查询 `thread_cancel_signals`，检测到信号后直接 `runner_task.cancel()`——与 deerflow 完全相同的粒度（任意 `await` 点），无需在 middleware 中检查 cancel。

```python
async def _cancel_watcher(
    self, thread_id: str, runner_task: asyncio.Task, poll_interval: int = 2
):
    """轮询 thread_cancel_signals，检测到 cancel 信号后取消 runner_task。"""
    while True:
        await asyncio.sleep(poll_interval)
        if await self._has_cancel_signal(thread_id):
            await self._clear_cancel_signal(thread_id)
            runner_task.cancel()   # 与 deerflow RunManager.cancel() 相同机制
            break
```

### 7.1a MQStreamBridge 发布行为

**RocketMQ message keys**：每条发往 `$AGENT_RESULTS` 的消息，均以 `message_id`（运行 ID）作为 RocketMQ `Message.keys`，便于按 key 索引同一次运行产生的所有 progress / result / error 消息。

**空 `messages` chunk 过滤**：LangGraph `messages` 流模式在 LLM 调用开始和结束时会产生 `content=""` 的 `AIMessageChunk`（无文字内容、无工具调用），这类 chunk 在 `astream()` 循环中被过滤，**不发布**到 MQ，减少无意义消息数量。过滤条件：`content` 为空字符串或空列表，且 `tool_call_chunks` 和 `tool_calls` 均为空。

### 7.2 核心执行结构

```python
class AgentRunner:
    """替换 deerflow run_worker.py，直接驱动 LangGraph 执行并发布到 RocketMQ。"""

    async def run(self, message: TaskMessage):
        thread_id = message.thread_id
        run_id = message.message_id
        runner_task = asyncio.current_task()

        # 启动心跳 + cancel watcher（与 deerflow task.cancel() 等价机制）
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(thread_id, interval=10)
        )
        cancel_watcher_task = asyncio.create_task(
            self._cancel_watcher(thread_id, runner_task, poll_interval=2)
        )

        seq = 0
        try:
            graph = await setup_agent(
                thread_id=thread_id,
                agent_name=message.agent_name,
                context={**message.context, "run_id": run_id},
            )

            async for mode, chunk in graph.astream(
                message.input,
                config=build_langgraph_config(message),
                stream_mode=message.reply_config.stream_event_types,
            ):
                if mode in ("values", "messages", "custom"):
                    await self.bridge.publish_progress(run_id, mode, chunk, seq)
                    seq += 1

            await self.bridge.publish_result(run_id, status="success", seq=seq)

        except asyncio.CancelledError:
            # cancel watcher 调用了 runner_task.cancel()，在某个 await 点生效
            await self.bridge.publish_result(run_id, status="cancelled", seq=seq)
        except asyncio.TimeoutError:
            await self.bridge.publish_error(run_id, "AGENT_TIMEOUT", retriable=True)
        except Exception as e:
            await self.bridge.publish_error(run_id, "INTERNAL_ERROR", message=str(e))
        finally:
            cancel_watcher_task.cancel()
            heartbeat_task.cancel()
            await self._drain_and_release(thread_id)
```

### 7.3 Cancel 信号流

```
上游发送 cancel 消息（thread_id=T）
        │
        ▼
TaskConsumer（任意实例）
        │  INSERT INTO thread_cancel_signals(T, reason)
        ▼
thread_cancel_signals 表
        │
        ▼  poll 每 2 秒
Cancel Watcher（AgentRunner 内协程）
        │  _has_cancel_signal(T) → True
        │  _clear_cancel_signal(T)
        │  runner_task.cancel()          ← asyncio 注入 CancelledError
        ▼
graph.astream() 下一个 await 点（LLM call / tool call / DB query）
        │  CancelledError 抛出
        ▼
except asyncio.CancelledError
        │  publish_result(status="cancelled")
        ▼
finally: drain_and_release
```

**取消延迟**：最坏情况 = cancel watcher 的 `poll_interval`（默认 2 秒）。可按需调整，权衡 DB 查询频率与响应延迟。

---

## 8. 新增 / 替换的组件清单

| 组件 | 类型 | 替换 deerflow 的 | 说明 |
|------|------|-----------------|------|
| `TaskConsumer` | 新增 | FastAPI routes + RunManager.create | 从 RocketMQ 消费消息，执行路由算法 |
| `AgentRunner` | 新增 | `run_worker.py` | 驱动 LangGraph，发布事件到 MQ |
| `MQStreamBridge` | 新增 | `MemoryStreamBridge` | 把 LangGraph 事件发布到 $AGENT_RESULTS topic |
| `InjectMiddleware` | 待实现 | 无（新能力） | steer 模式：LangGraph step 前从 inject 队列注入 steer 消息 |
| `RunRegistry` | 新增 | `RunManager`（进程内字典）| PostgreSQL 存储 thread 运行状态 |
| PostgreSQL checkpointer | 复用 | SQLite checkpointer | 多节点共享对话状态 |
| `setup_agent` / `create_lead_agent` | 复用 | — | agent graph 创建逻辑不变 |
| AioSandboxProvider | 复用 | — | 每个节点管理本地 Docker sandbox |
| 所有 tool 实现 | 复用 | — | bash、文件、sandbox 工具不变 |

---

## 9. 待确认事项

- [x] stale run 恢复：Consumer-internal retry，见 §4.4；优先从 checkpoint 续跑，超过 MAX_RETRIES 后降级通知上游
- [x] processed_messages TTL 清理：见 §6.4，已实现
- [x] followup 队列顺序：已确认为逐条顺序执行（取最早一条，链式触发）
- [ ] **collect 模式实现**（设计已完成，见 §4.2 + §5.3 + §5.5）：
  - `thread_run_state` 新增 `drain_mode TEXT NOT NULL DEFAULT 'followup'` 列
  - `TaskConsumer._handle_task`：`message_mode=collect` 入队时额外执行 `UPDATE thread_run_state SET drain_mode='collect'`
  - `RunRegistry.mark_thread_idle`：`SET status='idle', drain_mode='followup'`（重置）
  - `RunRegistry.get_drain_mode(thread_id)`：读取当前 drain_mode
  - `RunRegistry.transition_thread_collect(thread_id, pending_rows, last_message_id, last_body)`：原子标记全部 followup 行 consumed_at + 更新 current + 推进 message_id
  - `AgentRunner._drain_and_release`：按 drain_mode 分支，collect 分支合并所有 pending messages，调用 `transition_thread_collect`，`TaskMessage.with_messages()` 构造合成任务
  - `TaskMessage.with_messages(merged_messages)`：返回替换了 input 的新 TaskMessage 实例
- [x] `stale-run-watchdog` 需要 AgentRunner 引用才能触发 drain 和 re-execution：已实现，`_stale_run_watchdog` 接收 `runner: AgentRunner` 参数，通过 `runner.trigger_drain(thread_id)` 触发
- [x] `thread_run_state.retry_count` 归零时机：已实现，在 `AgentRunner.run()` 的 `finally` 块中调用 `registry.reset_retry_count(thread_id)`，在任意 terminal 状态（completed / cancelled / failed）后归零，不区分成功与否。比文档描述的"仅成功时清零"更宽松，避免历史 retry_count 影响后续正常调度

**steer 待实现时需确认**：
- [ ] steer 消息的 reply_config：继承正在运行的 task 的 reply_config，还是以 steer 消息自身的为准？
- [ ] steer 注入时机：before_agent + before_model，是否需要更细粒度？

---

## 10. 待设计：Runtime Config 透传与 Guardrail Harness

### 背景

`AgentRunner._build_config()` 已将 MQ 消息中的 `config.models` 透传进 `RunnableConfig.context`（见 `agent_runner.py:292`）：

```python
if task_cfg.get("models"):
    context["models"] = task_cfg["models"]
```

当前这个字段仅做透传，没有任何机制将其约束到 MCP 工具调用上。以 cfgpu 为例，`config.models` 指定了本次任务允许使用的生图/生视频模型范围，但 LLM 生成的 tool 参数（`model` 字段）目前不受这个配置约束。

### 问题拓展

这是一个通用问题：**某些 runtime config 参数需要被 agent 运行时严格遵循，而不仅仅是透传给 LLM 作为参考**。类似的参数还可能包括：

- `config.models` — 限制可用的生图/生视频模型
- `config.max_duration` — 限制生视频的最大时长
- `config.aspect_ratios` — 限制可用的画面比例
- 其他业务方在消息层注入的约束参数

这些参数的共同特征：由调用方（消息生产者）在 MQ 消息中指定，需要在 harness 层（tool 执行前）强制校验，LLM 自身不能绕过。

### 期望的统一设计

需要设计一套机制，覆盖以下三个层次：

1. **Config 配置层**：在 `extensions_config.json` 或 `config.yaml` 中声明哪些 runtime config 参数需要被哪些 MCP 工具/tool 参数遵循，以及校验规则（枚举白名单、数值范围等）。

2. **参数透传层**：MQ 消息中的 `config.*` 字段经过 `AgentRunner._build_config()` 标准化后，统一存入 `RunnableConfig.context`，供后续层读取。当前 `config.models` 已经实现了这一步。

3. **Guardrail Harness 层**：在 tool 执行前（`GuardrailMiddleware` 或 MCP tool interceptor）读取 `context` 中的约束参数，对 LLM 生成的 tool 参数做强制校验；校验失败返回 error `ToolMessage`，由 LLM 自我修正后重试。对于 stdio 传输的 MCP server（如 cfgpu），约束只能在 deerflow 侧做，不能下推到 MCP server 进程。

### 当前状态

- `config.models` 透传：**已实现**（`agent_runner.py:292`）
- 校验机制：**待设计实现**

需求进一步明确后再进行详细设计和实现。
