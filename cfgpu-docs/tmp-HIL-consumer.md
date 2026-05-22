# HIL × MQ Consumer — 集成设计（草稿）

> **状态**：待 MQ Consumer 消息模式确定后合并进《Consumer 运行管理设计》
> **依赖**：HIL-deerflow.md（deerflow 层实现），MQ消息协议.md v2.3

---

## 1. 新增的 thread 状态：`paused_for_approval`

### 1.1 `thread_run_state` 表变更

```sql
-- 在现有 status: running | idle 基础上新增
-- status: running | idle | paused_for_approval

-- 新增可选列，记录暂停原因（为后续扩展预留）
ALTER TABLE thread_run_state
    ADD COLUMN paused_reason TEXT;   -- 'tool_approval_required' | null
```

完整表结构（含新增列）：

```sql
CREATE TABLE thread_run_state (
    thread_id      TEXT PRIMARY KEY,
    instance_id    TEXT NOT NULL REFERENCES consumer_instances(instance_id),
    message_id     TEXT NOT NULL,
    status         TEXT NOT NULL,         -- running | idle | paused_for_approval
    paused_reason  TEXT,                  -- 新增：暂停原因
    reply_config   JSONB,
    started_at     TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ NOT NULL
);
```

### 1.2 `paused_for_approval` 的路由语义

在路由 claim 判断中，`paused_for_approval` 等同于 `idle`（允许被 claim）：

```sql
-- 原子 claim 事务（路由算法第 ② 步）
SELECT * FROM thread_run_state WHERE thread_id=T FOR UPDATE;

if status IN ('idle', 'paused_for_approval'):   -- ← 新增 paused_for_approval
    UPSERT thread_run_state SET status='running', paused_reason=NULL, ...
    return "claimed"
elif status == 'running':
    return "running_on", existing.instance_id
```

**说明**：
- resume task（`command.update.tool_approvals` 非空）到来 → claim，AgentRunner 用 `Command(update={tool_approvals:{...}})` 驱动 graph 恢复
- 普通 task（`messages` 非空）到来 → claim，AgentRunner 开新 run，审批窗口自然失效（checkpoint 会被新 run 覆盖）
- inject 期间 thread 处于 `running`，不受影响

### 1.3 Stale run 守护任务的调整

现有守护任务检查 `status='running' AND heartbeat 超时`，需**排除** `paused_for_approval`：

```sql
-- 现有查询（无需修改，paused_for_approval 状态无 heartbeat 要求，不会被误判）
SELECT * FROM thread_run_state
WHERE status = 'running'
  AND last_heartbeat < now() - interval '60 seconds';
```

`paused_for_approval` 不是 `running`，天然不在守护任务的扫描范围内，无需额外处理。

---

## 2. `AgentRunner.run()` 变更

### 2.1 支持 `command` 输入

`TaskMessage` 新增 `command` 字段（对应 task payload 的 `command`）：

```python
@dataclass
class TaskMessage:
    thread_id:    str
    message_id:   str
    agent_name:   str | None
    messages:     list[dict] | None   # 普通任务
    command:      dict | None         # HIL resume（与 messages 互斥）
    config:       dict
    reply_config: ReplyConfig
```

### 2.2 `run()` 核心改动

```python
class AgentRunner:
    async def run(self, message: TaskMessage):
        thread_id = message.thread_id
        run_id = message.message_id
        runner_task = asyncio.current_task()
        is_paused = False

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(thread_id))
        cancel_watcher_task = asyncio.create_task(
            self._cancel_watcher(thread_id, runner_task, poll_interval=2)
        )
        seq = 0
        try:
            # ← 新增：command 消息必须带 config.ask=True，否则 HumanApprovalMiddleware
            #   不会注入，tool_approvals 决策将被忽略，所有工具直接进 ToolNode 执行。
            #   自动修正并向客户端发 warning，保证 command payload 能顺滑到达 LangGraph。
            if message.command and not message.config.get("ask"):
                logger.warning(
                    "HIL resume command without config.ask=True (thread=%s); auto-injecting",
                    thread_id,
                )
                await self.bridge.publish_progress(
                    run_id, "custom",
                    {
                        "type": "warning",
                        "code": "HIL_ASK_REQUIRED",
                        "message": (
                            "HIL resume 消息必须设置 config.ask=true；"
                            "本次已自动修正，请检查客户端实现"
                        ),
                    },
                    seq,
                )
                seq += 1
                message.config["ask"] = True

            graph = await setup_agent(
                thread_id=thread_id,
                agent_name=message.agent_name,
                context={**message.config, "run_id": run_id},
            )

            # ← 新增：resume 路径 vs 普通路径
            if message.command:
                from langgraph.types import Command as LGCommand
                stream_input = LGCommand(**message.command)
            else:
                stream_input = normalize_input(message.messages)

            async for mode, chunk in graph.astream(
                stream_input,
                config=build_langgraph_config(message),
                stream_mode=message.reply_config.stream_event_types,
            ):
                if mode in ("values", "messages", "custom"):
                    await self.bridge.publish_progress(run_id, mode, chunk, seq)
                    seq += 1

            # ← 新增：astream 正常退出后检测是否因 interrupt() 暂停
            try:
                final_state = await graph.aget_state(build_langgraph_config(message))
                if final_state and any(t.interrupts for t in (final_state.tasks or [])):
                    is_paused = True
            except Exception:
                pass   # 无法检测时按 success 处理

            if is_paused:
                await self.bridge.publish_result(run_id, status="paused_for_approval", seq=seq)
            else:
                await self.bridge.publish_result(run_id, status="success", seq=seq)

        except asyncio.CancelledError:
            await self.bridge.publish_result(run_id, status="cancelled", seq=seq)
        except asyncio.TimeoutError:
            await self.bridge.publish_error(run_id, "AGENT_TIMEOUT", retriable=True)
        except Exception as e:
            await self.bridge.publish_error(run_id, "INTERNAL_ERROR", message=str(e))
        finally:
            cancel_watcher_task.cancel()
            heartbeat_task.cancel()

            # ← 新增：暂停时走专用路径，不执行 drain-and-release
            if is_paused:
                await self._mark_thread_paused(thread_id, "tool_approval_required")
            else:
                await self._drain_and_release(thread_id)

    async def _mark_thread_paused(self, thread_id: str, reason: str):
        """将 thread 标记为 paused_for_approval，保留 checkpoint。"""
        # UPDATE thread_run_state
        # SET status='paused_for_approval', paused_reason=reason, last_heartbeat=now()
        # WHERE thread_id=thread_id
        ...
```

### 2.3 `_drain_and_release` 与 `_mark_thread_paused` 的分支逻辑

| 情况 | finally 路径 |
|------|-------------|
| 正常完成 | `_drain_and_release()` → 检查 inject 队列 → idle 或继续新 run |
| 因 cancel 中止 | `_drain_and_release()` → idle（队列残留按正常处理）|
| 因 interrupt() 暂停 | `_mark_thread_paused()` → status=paused_for_approval，不清空 inject 队列 |

### 2.4 command 消息缺少 `config.ask=true` 的处理

`HumanApprovalMiddleware` 的注入条件之一是运行时 `config.ask=true`（见 HIL-deerflow.md §3.3）。若 command（resume）消息未携带该字段，middleware 不会注入，`state.tool_approvals` 中的用户决策将被忽略，所有工具直接进 ToolNode 执行——静默错误。

**处理策略：Auto-correct + Warning**

在 `setup_agent()` 之前检测并修正，确保 command payload 顺滑到达 LangGraph：

1. 检测到 `command` 非空但 `config.ask` 缺失或为 `false`
2. 向客户端发送 `warning` custom 进度事件（开发者可感知，用户无感）
3. 将 `message.config["ask"]` 强制置 `True` 再继续构建 graph

Warning 事件格式：

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

| 角色 | 感知 |
|------|------|
| 客户端开发者 | stream 中收到 `warning` custom 事件，明确知道漏了 `ask=true` |
| 用户 | resume 照常完成，工具决策正常应用，无感 |
| LangGraph | 收到完整 `Command(update={tool_approvals:{...}})`，`HumanApprovalMiddleware` 正常执行 resume path |

---

## 3. `processed_messages` 变更

新增 `paused_for_approval` 状态记录暂停的 task：

```sql
-- status: completed | failed | cancelled | paused_for_approval（新增）
INSERT INTO processed_messages(message_id, thread_id, status)
VALUES (X, T, 'paused_for_approval');
```

**幂等行为**：
- 同一 `message_id` 重复投递，发现 `paused_for_approval` → 可重发 result（或忽略），MQ ack 后 return
- resume 消息有自己的 `message_id`，单独记录为 `completed`（resume 成功）或 `failed`

---

## 4. inject 队列在暂停期间的行为

| 场景 | 行为 |
|------|------|
| 暂停期间 inject 队列有残留（罕见） | 队列保留；resume 完成后 `_drain_and_release` 处理残留 |
| 暂停期间收到普通 task | claim（等同 idle），开新 run，`before_agent` 中 InjectMiddleware 消费残留 inject |
| 暂停期间收到 resume task | claim，AgentRunner 用 `Command(update={tool_approvals:{...}})` 恢复，resume 完成后 `_drain_and_release` 处理残留 |

---

## 5. 完整交互序列（RocketMQ 路径）

```
客户端 → $AGENT_TASKS:
  task {messages: ["帮我生成主角第三集的场景图和视频"], command: null}

Consumer → claim thread_run_state(T, idle → running)
AgentRunner.run(message):
  stream_input = normalize_input(messages)
  graph.astream(stream_input, ...)

  LLM 生成 AIMessage(tool_calls=[cfgpu__generate_image(...), cfgpu__generate_video(...)])
  HumanApprovalMiddleware.after_model():
    state.tool_approvals 为空 → first call 路径
    get_stream_writer().write({
      type: "tool_approval_required",
      tool_calls: [
        {id:"tc1", name:"cfgpu__generate_image", args:{prompt:"英雄归途...", model:"flux-pro", ...}},
        {id:"tc2", name:"cfgpu__generate_video", args:{prompt:"英雄归途...", model:"wan-2.0", ...}}
      ]
    })
    interrupt({...})   ← 图暂停，单次 interrupt 覆盖整批，checkpoint 保存，astream() 结束

  aget_state() → tasks[0].interrupts 非空 → is_paused=True
  publish_result(status="paused_for_approval")

$AGENT_RESULTS ← Consumer:
  progress  {event_type:"custom", data:{
               type:"tool_approval_required",
               tool_calls:[
                 {id:"tc1", name:"cfgpu__generate_image", args:{prompt:"英雄归途...", ...}},
                 {id:"tc2", name:"cfgpu__generate_video", args:{prompt:"英雄归途...", ...}}
               ]}}           ← 单个批量事件，两个工具一起展示
  result    {status:"paused_for_approval"}

Consumer finally:
  _mark_thread_paused(T, "tool_approval_required")
  processed_messages INSERT(message_id=M1, status="paused_for_approval")

─────── 客户端展示两张参数卡片，用户修改图片 prompt，拒绝视频 ───────

客户端 → $AGENT_TASKS:
  task {messages: null,
        command: {
          update: {
            tool_approvals: {
              "tc1": {status:"approved",
                      args:{prompt:"英雄，暮色中独行，落日余晖，油画风格",
                            model:"flux-pro", width:1024, height:1024}},
              "tc2": {status:"rejected", reason:"视频暂不需要，只生成图片"}
            }
          }
        }}

Consumer → claim thread_run_state(T, paused_for_approval → running)
AgentRunner.run(message):
  stream_input = Command(update={tool_approvals: {tc1: approved, tc2: rejected}})
  graph.astream(Command(update=...), config=...)

  图从 checkpoint 恢复，LangGraph 先将 tool_approvals 写入 ThreadState
  HumanApprovalMiddleware.after_model() 重入:
    state.tool_approvals 已有 tc1、tc2 全部决策 → resume path，跳过 SSE 和 interrupt()
    _build_response():
      tc1: 保留，args 替换为修改后的文案
      tc2: 移除，注入 ToolMessage(error, reason="视频暂不需要")
  ToolNode 执行：cfgpu__generate_image(prompt="英雄，暮色中独行...", ...)
  → ToolMessage {urls:["https://cdn.cfgpu.com/image_xxx.jpg"]}
  LLM 生成最终回复（含图片 URL，感知视频已拒绝）

  aget_state() → tasks 无 interrupts → is_paused=False
  publish_result(status="success")

$AGENT_RESULTS ← Consumer:
  progress  {event_type:"messages", data:{chunk:ToolMessage{tc2, error}}}
  progress  {event_type:"messages", data:{chunk:ToolMessage{tc1, urls:[...]}}}
  progress  {event_type:"messages", data:{chunk:AIMessageChunk{...}}}   (AI 最终回复)
  result    {status:"success", usage:{...}}

Consumer finally:
  processed_messages INSERT(message_id=M2, status="completed")
  _drain_and_release(T)   → idle（或处理 inject 残留）
```

---

## 6. 待确认事项（合并前需解决）

- [ ] **`paused_for_approval` 超时**：用户长时间不确认（如 30 分钟），thread 永远停在该状态。需要守护任务扫描超时的 `paused_for_approval` 记录，自动转 idle 并通知客户端（发 error 或特殊 result）。超时阈值建议可配置。
- [ ] **resume 消息的 `reply_config`**：resume task 的 `reply_config` 是否沿用原 task 的配置（从 `thread_run_state.reply_config` 读取），还是以 resume 消息中的 `reply_config` 为准？建议以 resume 消息自身为准（客户端可能在等待期间改变偏好）。
- [x] **多工具并行调用时的 HIL**：~~LLM 在一次响应中同时调用多个高花费工具，每个 `interrupt()` 独立暂停~~ → **已解决**：`after_model` hook 在 ToolNode 执行前触发，整批工具调用用**单次 `interrupt()`** 处理，`command.update.tool_approvals` 按 `tool_call_id` 分别决策，无并发竞争。
- [ ] **inject 队列在暂停期间的语义**：当前设计中普通 task 到来会覆盖审批窗口（claim 并开新 run）。是否需要保护机制——例如，暂停期间禁止普通 task claim，只允许 resume task 和 cancel？
