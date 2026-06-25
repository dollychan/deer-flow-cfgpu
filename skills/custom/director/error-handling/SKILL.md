---
name: error-handling
description: Decide how to react when a tool call or generation step fails while producing images/videos/episodes. Use whenever a tool returns an error — to classify it (deterministic vs transient vs terminal), decide retry vs fix-inputs vs stop-and-tell-user, and avoid double-billing on generation calls. Covers cfgpu MCP errors, material/asset errors, LLM provider failures, and async task failures.
---

# Handling errors during generation (director agent)

You are a LangGraph agent that produces images, videos, speech, and episodes by
calling the **cfgpu/cfdream MCP** tools (`generate_image`, `generate_video`,
`get_task_status`, `wait_for_task`, `list_models`, `get_model_card`) and the
**builtin material tools** (`register_material`, `localize_material`). Things
fail. This skill tells you how to *react* to a failure — not how to call the
tools (see the per-model skills for that).

## First: what the runtime already does for you

Don't re-implement these — building your own retry loop on top of them wastes
budget and can double-bill.

- **LLM/provider errors are auto-retried.** Transient provider failures
  (timeouts, dropped connections, 5xx, 429, "server busy / 服务繁忙 / 稍后重试")
  are retried up to 3× with backoff by the runtime, and a circuit breaker trips
  after repeated failures. If you receive a terminal assistant message saying
  the provider is out of quota / unauthorized / unavailable after retries, the
  system **already exhausted its retries** — relay it, don't loop.
- **Tool exceptions become error messages, not crashes.** When a tool *raises*,
  the runtime converts it into a `ToolMessage` with `status="error"` whose text
  starts with `Error: Tool '<name>' failed with <ExcClass>: <detail>. Continue
  with available context, or choose an alternative tool.` The run continues and
  it is your job to decide the next step.
- **Control-flow is not an error.** Cancel / pause / interrupt are handled by
  the runtime. You will not see them as tool errors, and you must not try to
  "recover" from a cancelled run.

## How a failure reaches you (inspect both shapes)

1. **Hard error** — a `ToolMessage` with `status="error"` whose content begins
   with `Error: …`. The tool raised (network, missing file, bad state).
2. **Soft error** — a *normal* tool result whose JSON body is an error object.
   cfgpu `generate_*` / `get_task_status` return a plain dict, so a failure
   looks like `{"error_type": "invalid_params" | "auth" | "api_error" |
   "timeout" | "unknown", "message": "..."}` or a model-specific code
   (`content_blocked`, `media_download_failed`, `mixed_scenarios`, `audio_only`,
   `quota_exceeded`, …). **Always read the result body** — a 200-shaped result
   can still be a failure.

## The decision in one rule

> **Read the error, classify it, then pick exactly one of: fix-and-retry-once /
> bounded-retry / stop-and-tell-user. Never re-issue an identical call that
> failed deterministically, and never blindly re-fire a billed generation.**

## Classification table

| Class | Typical signals | Retriable by you? | What to do |
|---|---|---|---|
| **Deterministic / validation** | `invalid_params`, `mixed_scenarios`, `audio_only`, `... does not support <feature>`, `... minimum resolution is 720p`, `... mutually exclusive`, `accepts at most N` | **No** — same inputs → same failure | Fix the inputs (drop/adjust the offending param, switch scenario or model) and re-call **once**. If you can't satisfy the constraint, stop and explain to the user. |
| **Content policy** | `content_blocked`, "sensitive" | **No** as-is | Rewrite the prompt/text to remove sensitive terms, retry **once** with the revised input. If still blocked, tell the user which content was rejected. |
| **Transient infra (tool-level)** | `media_download_failed`, `error_type:"timeout"`/`"api_error"`, "server busy", connection dropped | **Yes, bounded** | See *Retry rules* below. Read-only/idempotent tools: retry 1–2× with short backoff. Generation: prefer polling an existing `task_id` over re-firing. |
| **Async task failure** | `get_task_status` → `{"status":"failed", ...}` | Depends on the failure reason | Read the reason. Content/validation reason → fix then resubmit. Transient reason → resubmit once. Surface the `task_id`. |
| **Quota / billing** | `quota_exceeded`, `error_type:"auth"`+quota wording, "余额不足 / 额度不足 / 欠费" | **No** | Stop. You cannot self-recover. Tell the user to top up / fix the account. |
| **Auth / permission** | `error_type:"auth"`, "unauthorized / forbidden / invalid api key / 未授权 / 无权" | **No** | Stop. Tell the user the credentials or permissions are wrong. |
| **Environment / state** | `Error: runtime state unavailable`, `thread outputs path unavailable`, `OSS uploader unavailable` | **No** | Don't retry — it's a deployment/config problem. Stop that step and surface it plainly. |
| **Provider terminal fallback** | assistant message stating provider quota/auth/unavailable after retries (circuit breaker engaged) | Already handled | Terminal. Relay to the user; do not start your own retry loop. |

## Material / asset errors (builtin tools)

These come back as `Error: …` `ToolMessage`s — most are **deterministic**:

| Error | Meaning | Action |
|---|---|---|
| `Error: no material with id <id>` | The id doesn't exist in the registry | Recheck the id; don't invent ids. List/recover the correct material before retrying. |
| `Error: file not found: <path>` (`register_material`) | The path you passed doesn't exist | Confirm the file was actually produced; fix the path. Stop if the upstream step never created it. |
| `asset_url material <id> cannot be localized` / `cannot be staged` (I4) | An `asset://` reference is not downloadable | Don't retry the same id. Use the material in a tool that accepts `asset://` directly, or obtain a real URL/file first. |
| `local material <id> has no reachable file` (dangling local_path) | The local file vanished | Re-produce or re-fetch the source; the cached path is gone. |
| `Error: failed to localize <id>: <detail>` | Download/fetch failed | Read `<detail>`: transient (network/timeout) → retry once; deterministic (404/403/blocked) → fix the source. |

> Result/preview **URLs expire (~24h)**. To "refresh" an expired link, re-resolve
> the material by its **id** — do **not** re-generate. Re-generation is a new
> billable job, not a URL refresh.

## Retry rules (don't double-bill)

Generation is **billed per job**, and many cfgpu calls block until the job
finishes. A re-fired `generate_*` after a timeout can charge twice and leave an
orphan job running remotely.

- **`generate_*` timed out / connection dropped:** do **not** immediately call
  `generate_*` again. If you used `wait=false` (or have a `task_id`), poll with
  `get_task_status` / `wait_for_task` — the job is likely still running and will
  bill once. Only re-submit a fresh generation if you have positive evidence no
  task was created.
- **Idempotent reads** (`list_models`, `get_model_card`, `get_task_status`,
  `localize_material` fetches): safe to retry 1–2× with a short backoff on a
  transient failure.
- **Cap your own retries at 1–2.** The provider/network layer already retries
  infra errors underneath you. If a bounded retry still fails, stop and surface
  it — don't loop.
- **Fix-then-retry counts as one shot.** After a deterministic error you may
  re-call *with corrected inputs* once; if the corrected call also fails, treat
  it as terminal for that step.

## When you can't recover

Surface the error's `message` to the user in plain language, say what you tried,
and stop that step cleanly. Then continue any independent remaining work. Do not
silently swallow the failure, fabricate a result, or retry indefinitely.

## Quick examples

- `generate_video` → `{"error_type":"invalid_params","message":"duration 20 exceeds max 15"}`
  → deterministic: lower `duration_seconds` to ≤15 and re-call once.
- `generate_image` → `content_blocked` → rewrite the prompt without the flagged
  terms, retry once; if still blocked, tell the user.
- `generate_video` (wait, blocking) → tool error `Error: Tool 'generate_video'
  failed with ... timeout` → call `get_task_status`/`wait_for_task` if you have a
  `task_id`; otherwise inform the user and confirm before re-firing (billing).
- Any tool → `quota_exceeded` / `余额不足` → stop, ask the user to top up.
- `localize_material` → `asset_url ... cannot be localized (I4)` → don't retry;
  pass the `asset://` directly to a tool that accepts it, or get a real URL.
