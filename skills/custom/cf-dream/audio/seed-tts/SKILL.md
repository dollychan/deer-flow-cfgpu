---
name: seed-tts
description: Synthesize speech from text with Doubao 语音合成 2.0 (seed-tts-2.0) by calling the cfgpu MCP generate_audio tool — asynchronous text-to-speech with 122 expressive Chinese system voices (角色扮演/有声阅读/客服/视频配音), emotion and ASMR support. Use when the user wants Doubao/豆包 TTS, Chinese character/role voices, or audiobook narration.
---

# Synthesizing speech with seed-tts-2.0 (MCP)

This skill is for an agent that calls the **cfgpu MCP server** tools. The relevant tools:

- `generate_audio(...)` — submit a TTS task (the main one)
- `task_status(task_id)` — poll an async task's status
- `task_wait(task_id, timeout)` — block until a task finishes, then return the result
- `list_models(task_type="audio")` — enumerate voice models
- `get_model_card(model_name)` — fetch the model's full doc, including the **full system voice list (122 speakers)**

> Tool names may be namespaced by the host (e.g. `mcp__cfgpu__generate_audio`). Use whatever prefix your environment exposes; the parameters below are identical.

seed-tts-2.0 is **asynchronous** (submit → poll `/voice/tasks/{task_id}`). By default `generate_audio` runs with `wait=true`, so it polls internally and returns the finished result in one call. Only use `wait=false` + `task_wait` to fire-and-forget or run several jobs concurrently.

## Step 1 — Select the model

There is one Doubao TTS model: `model="seed-tts-2-0"`.

| `model` (adapter_id) | 价格 | cost / speed | 调用 |
|---|---|---|---|
| `seed-tts-2-0` | 2.94 元/万字符 | cost 3/5 · speed 3/5 | 异步（提交后轮询） |

> For English/multilingual narration, the finest pitch/speed/emotion control, or synchronous one-call output, consider the **minimax-speech** skill instead.

## Step 2 — Choose a voice (`voice` parameter)

`voice` maps to seed-tts `req_params.speaker`. Default (when omitted) is `zh_female_xiaohe_uranus_bigtts` (小何 2.0).

The model card lists **all 122 system speakers** by scene (通用场景 / 角色扮演 / 视频配音 / 有声阅读 / 客服场景 / 教育 / 多语种). **Call `get_model_card("seed-tts-2-0")` to look up the exact `speaker` id** before committing to a named voice.

Naming convention helps narrow it down:
- `zh_female_*` / `zh_male_*` `_uranus_bigtts` — Chinese voices supporting 情感变化、指令遵循、ASMR
- `en_male_*` / `en_female_*` `_uranus_bigtts` — 美式英语 (e.g. `en_male_tim_uranus_bigtts`)
- `saturn_*_tob` — 角色扮演 voices with 指令遵循、COT/QA 能力
- `saturn_*_cs_tob` — 客服 voices

A few common ids: `zh_female_xiaohe_uranus_bigtts` (小何, default), `zh_male_sunwukong_uranus_bigtts` (猴哥), `zh_male_qingcang_uranus_bigtts` (擎苍·有声阅读), `zh_female_xiaoxue_uranus_bigtts` (儿童绘本).

## Step 3 — Output parameters

| Parameter | Default | Notes |
|---|---|---|
| `audio_format` | `mp3` | `mp3` / `wav` / `pcm` / `flac` |
| `sample_rate` | `24000` | Hz |
| `wait` | `true` | `false` returns a `task_id` immediately for later `task_wait` |
| `timeout` | auto (~300s) | max wait seconds |
| `return_metadata` | `true` | include `model_used` / `usage` in the result |
| `model_specific` | — | raw API extras merged last (e.g. `callback_url`) |

> `speed` / `volume` / `pitch` / `emotion` / `bitrate` are **MiniMax-only** and ignored by seed-tts. To shape delivery here, encode it in the `text` and pick an expressive speaker; many Doubao voices respond to emotional/ASMR cues in the prompt.

## Step 4 — Call the tool

### Basic (waits, returns the URL)
```json
generate_audio({
  "text": "明朝开国皇帝朱元璋也称这本书为，万物之根。",
  "model": "seed-tts-2-0",
  "voice": "zh_female_xiaohe_uranus_bigtts"
})
```

### Character voice, WAV output
```json
generate_audio({
  "text": "俺老孙来也！妖怪哪里走！",
  "model": "seed-tts-2-0",
  "voice": "zh_male_sunwukong_uranus_bigtts",
  "audio_format": "wav",
  "sample_rate": 24000
})
```

### Async: fire now, collect later
```json
generate_audio({ "text": "...", "model": "seed-tts-2-0", "wait": false })  // → { "task_id": "...", "status": "pending" }
task_status({ "task_id": "..." })                                          // → { "status": "running" | "succeeded" | "failed", ... }
task_wait({ "task_id": "...", "timeout": 300 })                            // blocks, then → final result
```

## Reading the result

`generate_audio` / `task_wait` return a normalized object:

```json
{
  "urls": ["https://.../speech.mp3"],     // the generated audio
  "expires_at": "2026-06-18T12:00:00Z",   // URL valid ~24h — download promptly
  "artifact": true,
  "payload": { ... },                      // the exact seed-tts API request that was sent
  "metadata": {                            // present when return_metadata=true
    "task_id": "...",
    "model_used": "seed-tts.2.0",
    "usage": { ... }                       // billing is per character (按字符计费)
  }
}
```

Give the user the `urls` value and warn that the link expires in ~24 hours. On error the tool returns an error dict instead — surface its `message`.

## Notes & troubleshooting

- Billing is **per character** (2.94 元/万字符), so cost scales with `text` length.
- If a requested speaker isn't in the card's 122-voice list, fall back to the default `zh_female_xiaohe_uranus_bigtts` and tell the user, rather than inventing an id.
- `content_blocked` → rewrite sensitive text. `invalid_params` → check format/sample_rate/speaker against the card. `quota_exceeded` → top up the account.
