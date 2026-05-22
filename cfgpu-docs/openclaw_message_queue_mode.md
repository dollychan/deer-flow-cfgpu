# OpenClaw 消息队列模式：分析与参考

本文档总结 OpenClaw 的消息 Queue Mode 设计，用于参考我们的 Consumer 并发任务策略设计。

源码目录：`/Users/dollychen/claude/Public/openclaw`

---

## 1. 概述

OpenClaw 通过一个 **lane-aware FIFO 队列**序列化 inbound 消息，防止同一 session 的多次 agent run 并发冲突。核心设计是：**每条消息可以独立声明自己进入队列时的处理模式**，系统按优先级链解析最终生效的 mode。

---

## 2. Queue Mode 类型

```typescript
// /src/auto-reply/reply/queue/types.ts
export type QueueMode =
  | "steer"         // 注入正在运行的 run（前提：run 处于 streaming 状态）
  | "steer-backlog" // 注入 + 同时保留副本排队（双重处理）
  | "followup"      // 排队，当前 run 结束后逐条执行
  | "collect"       // 排队，当前 run 结束后合并成一条执行（默认）
  | "interrupt"     // 中止当前 run，立即执行新消息
  | "queue";        // "steer" 的别名（legacy）
```

| Mode | 当前 run 存在时的行为 | 是否排队 | 是否注入当前 run |
|------|-------------------|---------|----------------|
| `steer` | 注入（需 streaming），失败时降级 followup | 否（成功时） | 是 |
| `steer-backlog` | 注入 + 保留副本排队 | 是 | 是 |
| `followup` | 排队，依次执行 | 是 | 否 |
| `collect` | 排队，合并执行（**默认**） | 是 | 否 |
| `interrupt` | 中止当前 run，立即执行 | 否 | 否 |

---

## 3. 优先级配置链

Queue mode 在 4 个层级配置，**优先级从高到低**：

```
① inline directive  ─── 消息内嵌 /queue steer         per-message，不持久化
② session store     ─── /queue collect（独立命令）     持久化，影响后续所有消息
③ per-channel cfg   ─── messages.queue.byChannel      配置文件，按渠道
④ global config     ─── messages.queue.mode           配置文件，全局
⑤ 硬编码默认值      ─── "collect"
```

完整解析逻辑（`/src/auto-reply/reply/queue/settings.ts`）：

```typescript
const resolvedMode =
  params.inlineMode ??                                    // ① 消息内 inline
  normalizeQueueMode(params.sessionEntry?.queueMode) ??  // ② session 持久化
  normalizeQueueMode(providerModeRaw) ??                 // ③ per-channel 配置
  normalizeQueueMode(queueCfg?.mode) ??                  // ④ 全局配置
  defaultQueueModeForChannel(channelKey);                // ⑤ 默认 "collect"
```

其余参数（debounce、cap、drop policy）遵循相同优先级链：

```
inline options → session store → per-channel → global → default
  debounceMs: 1000ms
  cap:        20
  dropPolicy: "summarize"
```

---

## 4. 各 Mode 的实现机制

### 4.1 steer —— 注入正在运行的 run

注入前有 3 个前提条件检查（`/src/agents/pi-embedded-runner/runs.ts`）：

```typescript
export function queueEmbeddedPiMessage(sessionId: string, text: string): boolean {
  const handle = ACTIVE_EMBEDDED_RUNS.get(sessionId);
  if (!handle)            return false;  // run 不存在
  if (!handle.isStreaming()) return false;  // 未处于 streaming 状态
  if (handle.isCompacting()) return false;  // 正在压缩 context
  void handle.queueMessage(text);
  return true;  // 注入成功
}
```

**降级行为**：注入失败时自动降级为 followup（排队等待）。

### 4.2 collect —— 合并多条消息执行

drain 时将队列中所有消息合并成一个 prompt（`/src/auto-reply/reply/queue/drain.ts`）：

```typescript
const prompt = buildCollectPrompt({
  title: "[Queued messages while agent was busy]",
  items,
  renderItem: (item, idx) => `---\nQueued #${idx + 1}\n${item.prompt}`.trim(),
});
await runFollowup({ prompt, ... });
queue.items.splice(0, items.length);  // 清空已合并的消息
```

**cross-channel 检测**：如果队列中的消息来自不同 channel/thread，不合并，回退到逐条处理。

### 4.3 followup —— 逐条顺序执行

队列 drain 时通过 `drainNextQueueItem` 依次处理每一条。

### 4.4 interrupt —— 中止并立即执行

```typescript
if (resolvedQueue.mode === "interrupt" && laneSize > 0) {
  clearCommandLane(sessionLaneKey);        // 清空队列
  abortEmbeddedPiRun(sessionIdFinal);     // 中止当前 run
}
// 继续执行新消息（不入队）
```

### 4.5 steer-backlog —— 注入 + 排队双重

`shouldSteer=true` 且 `shouldFollowup=true` 同时生效。消息既注入当前 run，又入队等待下一轮执行，可能产生重复响应。

---

## 5. Debounce 机制

对 `collect`/`followup` 模式，drain 前等待一个静默窗口，防止 burst 消息触发多次执行：

```typescript
// /src/utils/queue-helpers.ts
export function waitForQueueDebounce(queue): Promise<void> {
  const check = () => {
    const since = Date.now() - queue.lastEnqueuedAt;
    if (since >= debounceMs) { resolve(); return; }
    setTimeout(check, debounceMs - since);
  };
  check();
}
```

Drain 循环：
```typescript
while (queue.items.length > 0 || queue.droppedCount > 0) {
  await waitForQueueDebounce(queue);  // 等静默窗口
  // 按 mode 处理队列
}
```

**效果示例**：debounce=1000ms，用户 200ms 内连发 3 条 → 等 1 秒安静后 → 3 条合并为 1 次执行。

---

## 6. Queue 溢出控制（cap + drop policy）

```typescript
// /src/utils/queue-helpers.ts
export function applyQueueDropPolicy(params): boolean {
  if (params.queue.items.length < cap) return true;  // 正常入队

  if (dropPolicy === "new") return false;  // 丢弃新消息

  // "old" 或 "summarize"：丢弃最旧的消息
  const dropped = params.queue.items.splice(0, dropCount);
  if (dropPolicy === "summarize") {
    for (const item of dropped) {
      queue.summaryLines.push(buildQueueSummaryLine(summarize(item)));
    }
  }
  return true;
}
```

| drop policy | 行为 |
|------------|------|
| `old` | 丢弃最旧的消息，无通知 |
| `new` | 拒绝新消息入队 |
| `summarize`（默认） | 丢弃旧消息，生成摘要注入下一次执行："[3 messages were dropped]" |

---

## 7. Session 混合模式

**同一 session 内不同消息可以使用不同 mode**，有两种方式：

### 7.1 Inline（临时，per-message）

消息内容中嵌入 `/queue <mode>`，只影响这一条消息的处理，不改变 session 状态：

```
用户：帮我分析这份报告 /queue steer    → 按 steer 处理（注入当前 run）
用户：顺便查一下最新价格              → 按 session/global 默认处理（collect）
```

inline 提取（`/src/auto-reply/reply/get-reply-directives-apply.ts`）：

```typescript
const perMessageQueueMode =
  directives.hasQueueDirective && !directives.queueReset
    ? directives.queueMode
    : undefined;
// 只作为 inlineMode 参数传入 resolveQueueSettings()，不写入 session store
```

### 7.2 Session 命令（持久，影响后续所有消息）

单独发送 `/queue collect` 作为命令（不带实质内容），持久化到 session store：

```typescript
// /src/auto-reply/reply/directive-handling.impl.ts
if (directives.hasQueueDirective) {
  if (directives.queueMode)    sessionEntry.queueMode = directives.queueMode;
  if (directives.debounceMs)   sessionEntry.queueDebounceMs = directives.debounceMs;
  if (directives.cap)          sessionEntry.queueCap = directives.cap;
  if (directives.dropPolicy)   sessionEntry.queueDrop = directives.dropPolicy;
}
await updateSessionStore(storePath, store => { store[sessionKey] = sessionEntry; });
```

`/queue reset` 或 `/queue default` 清除 session 级别设置，回退到 channel/global 默认。

---

## 8. Directive 语法

```
/queue <mode> [debounce:<duration>] [cap:<number>] [drop:<policy>]
```

示例：
```
/queue steer
/queue collect debounce:2s
/queue followup cap:10 drop:old
/queue collect debounce:500ms cap:5 drop:summarize
/queue reset                        # 清除 session 级别设置
```

规范化别名（`/src/auto-reply/reply/queue/normalize.ts`）：
```typescript
"queue" | "queued"                           → "steer"
"interrupt" | "abort"                        → "interrupt"
"followup" | "follow-ups" | "followups"      → "followup"
"collect" | "coalesce"                       → "collect"
"steer+backlog" | "steer-backlog"            → "steer-backlog"
"old" | "oldest"                             → "old"（drop policy）
"summarize" | "summary"                      → "summarize"（drop policy）
```

---

## 9. 关键文件索引

| 功能 | 文件 |
|------|------|
| 类型定义 | `src/auto-reply/reply/queue/types.ts` |
| 优先级解析 | `src/auto-reply/reply/queue/settings.ts` |
| Directive 提取 | `src/auto-reply/reply/queue/directive.ts` |
| Mode 规范化 | `src/auto-reply/reply/queue/normalize.ts` |
| 运行时分支 | `src/auto-reply/reply/get-reply-run.ts` (L438-459) |
| 持久化 | `src/auto-reply/reply/directive-handling.impl.ts` (L337-461) |
| Drain 循环 | `src/auto-reply/reply/queue/drain.ts` |
| Steer 注入 | `src/agents/pi-embedded-runner/runs.ts` |
| 溢出控制 | `src/utils/queue-helpers.ts` |
| 队列状态 | `src/auto-reply/reply/queue/state.ts` |

---

## 10. 对我们设计的启示

| OpenClaw 机制 | 我们是否需要 | 说明 |
|-------------|------------|------|
| collect mode（合并执行） | **是** | 多条消息合并成一轮，减少 LLM 调用，结果更连贯 |
| debounce | **是** | 防止 burst，等静默窗口后再 drain |
| cap + drop policy | **是** | 限制队列深度，`summarize` 避免消息静默丢失 |
| steer 的降级行为 | **是** | inject 未被消费时自动升级为新一轮执行（我们已有，需文档化） |
| inline per-message mode | **是**（已有） | `task.config.concurrent_task_policy` |
| session-level 持久化 | **否** | 上游是程序化系统，直接在 payload 声明即可 |
| per-agent default | **是** | 不同 agent 对并发容忍度不同，可在 config.yaml 按 agent_name 配置 |
| steer-backlog | **否** | 双重处理在 MQ 场景会产生重复 result，不适用 |
