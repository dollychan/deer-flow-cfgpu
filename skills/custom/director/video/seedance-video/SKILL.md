---
name: seedance-video
description: Generate videos with the Doubao Seedance model family (Seedance 2.0 / 2.0 fast / 1.5 Pro) by calling the cfgpu MCP tools — text-to-video, image-to-video (first/last frame), multimodal reference, video edit/extend, and synchronized audio. Use when the user wants to create, animate, edit, or extend a video, or asks which Seedance model to pick.
---

# Generating videos with Seedance (MCP)

This skill is for an agent that calls the **cfgpu MCP server** tools. The relevant tools:

- `generate_video(...)` — create a video task (this is the main one)
- `task_status(task_id)` — poll an async task's status
- `task_wait(task_id, timeout)` — block until a task finishes, then return the result
- `list_models(task_type="video")` — enumerate video models
- `get_model_card(model_name)` — fetch a model's full parameter/usage doc

> Tool names may be namespaced by the host (e.g. `mcp__cfgpu__generate_video`). Use whatever prefix your environment exposes; the parameters below are identical.

Generation is **asynchronous**. By default `generate_video` runs with `wait=true`, so it polls internally and returns the finished result in one call. Only switch to `wait=false` + `task_wait` when you need to fire-and-forget or run several jobs concurrently.

## Step 1 — Pick the model (`model` parameter)

| `model` (adapter_id) | Best for | Max duration | Resolutions | Reference video/audio · edit · extend |
|---|---|---|---|---|
| `doubao-seedance-2-0` | Highest quality, all capabilities | 15s | 480p/720p/1080p | ✅ |
| `doubao-seedance-2-0-fast` | Faster + cheaper, same capabilities | 12s | 480p/720p/1080p | ✅ |
| `doubao-seedance-1-5-pro` | 1080p, sample-mode preview | 12s | 480p/720p/1080p | ❌ (text / image-frame / audio-gen only) |

Decision guide:
- Top quality + every feature → `doubao-seedance-2-0`
- Speed/cost with the same feature set → `doubao-seedance-2-0-fast`
- Reference **video/audio**, video **editing**, or **extension** → must be a 2.0 model (1.5 Pro rejects these inputs)
- Fast cheap preview before the real render → `doubao-seedance-1-5-pro` with `model_specific={"sample_mode": true}`
- Unsure → pass `model="auto"` and let the router choose; or call `list_models(task_type="video")` first.

When in doubt about exact constraints, call `get_model_card("doubao-seedance-2-0")` (Seedance 2.0 is API-identical to `wan-2-0`, which has the fullest card).

## Step 2 — Choose ONE scenario

The three image scenarios are **mutually exclusive** — never combine first/last-frame with reference inputs in a single call (yields a `mixed_scenarios` error).

| Scenario | Set these parameters |
|---|---|
| Text-to-video | `prompt` only |
| Image-to-video (first frame) | `prompt` + `first_frame` |
| Image-to-video (first + last frame) | `prompt` + `first_frame` + `last_frame` |
| Multimodal reference *(2.0 only)* | `prompt` + `reference_images` (0–9) and/or `reference_videos` (0–3) and/or `reference_audios` (0–3) |
| Video edit *(2.0 only)* | `prompt` describing the edit + `reference_videos` + `reference_images` |
| Video extend *(2.0 only)* | `prompt` + up to 3 `reference_videos` to stitch |

`reference_audios` can never be sent alone — it must accompany at least one reference image or video (`audio_only` error otherwise).

## Step 3 — Output parameters

| Parameter | Default | Notes |
|---|---|---|
| `duration_seconds` | `5` | 4–15 (12 max for fast / 1.5 Pro); `-1` = smart/auto |
| `aspect_ratio` | `"adaptive"` | `16:9 9:16 1:1 4:3 3:4 21:9 adaptive` — `adaptive` matches the input / first frame |
| `resolution` | `"720p"` | `480p 720p 1080p` |
| `with_audio` | `true` | set `false` to skip synchronized audio (also lowers cost) |
| `watermark` | model default | `true`/`false` |
| `wait` | `true` | `false` returns a `task_id` immediately for later `task_wait` |
| `timeout` | auto | max wait seconds |
| `return_metadata` | `true` | include `seed`, `model_used`, token `usage` in the result |
| `model_specific` | — | extra raw API params, e.g. `{"tools": [{"type": "web_search"}]}` (web search is text-to-video only), or `{"sample_mode": true}` for 1.5 Pro |

## Step 4 — Call the tool

### Text-to-video
```json
generate_video({
  "prompt": "海浪拍打沙滩，黄昏，电影质感，镜头逐渐拉近",
  "model": "doubao-seedance-2-0",
  "resolution": "1080p",
  "duration_seconds": 5,
  "aspect_ratio": "16:9"
})
```

### Image-to-video (first frame), no audio
```json
generate_video({
  "prompt": "镜头缓慢推进，猫咪慢慢睁开眼睛",
  "model": "doubao-seedance-2-0-fast",
  "first_frame": "https://example.com/cat.jpg",
  "with_audio": false
})
```

### First + last frame control
```json
generate_video({
  "prompt": "一只猫咪从睡姿变成站姿",
  "first_frame": "https://example.com/sleep.jpg",
  "last_frame": "https://example.com/stand.jpg"
})
```

### Multimodal reference (2.0 only)
```json
generate_video({
  "prompt": "全程使用视频1的第一视角构图，使用音频1作为背景音乐...",
  "model": "doubao-seedance-2-0",
  "reference_images": ["https://example.com/p1.jpg", "https://example.com/p2.jpg"],
  "reference_videos": ["https://example.com/v1.mp4"],
  "reference_audios": ["https://example.com/a1.mp3"],
  "duration_seconds": 11,
  "aspect_ratio": "16:9"
})
```

### Video edit (2.0 only)
```json
generate_video({
  "prompt": "将视频1礼盒中的香水替换成图片1中的面霜，运镜不变",
  "model": "doubao-seedance-2-0",
  "reference_videos": ["https://example.com/perfume.mp4"],
  "reference_images": ["https://example.com/cream.jpg"]
})
```

### 1.5 Pro fast preview (sample mode)
```json
generate_video({
  "prompt": "宇宙飞船穿越星云",
  "model": "doubao-seedance-1-5-pro",
  "resolution": "480p",
  "model_specific": {"sample_mode": true}
})
```

### Async: fire now, collect later
```json
generate_video({ "prompt": "...", "wait": false })   // → { "task_id": "cgt-..." }
task_status({ "task_id": "cgt-..." })                 // → { "status": "running" | "succeeded" | "failed", ... }
task_wait({ "task_id": "cgt-...", "timeout": 600 })   // blocks, then → final result
```

## Reading the result

`generate_video` / `task_wait` return a normalized object:

```json
{
  "urls": ["https://.../output.mp4"],     // the generated video(s)
  "expires_at": "2026-06-18T12:00:00Z",   // URL valid ~24h — download promptly
  "artifact": true,
  "metadata": {                            // present when return_metadata=true
    "task_id": "cgt-...",
    "model_used": "doubao-seedance-2-0-260128",
    "aspect_ratio": "16:9",
    "seed": 15233,
    "usage": { "totalTokens": 108900 }     // billing is per token
  }
}
```

Give the user the `urls` value and warn that the link expires in ~24 hours. On error, the tool returns an error dict instead — surface its `message`.

## Writing good prompts

Structure: **subject → action → camera language → mood/style**. There are no `motion_intensity` / `camera_movement` parameters — encode camera moves in the prompt text: 镜头逐渐拉近 (zoom in), 360度环绕运镜 (orbit), 第一人称视角 (POV), 镜头向左平移 (pan left).

With audio on (default), put spoken dialogue in double quotes so the model voices it, e.g. `男人说："你记住，以后不可以用手指指月亮。"`. The model auto-adds matching voice, sound effects, and background music. Keep prompts ≤500 Chinese chars / ≤1000 English words.

## Input asset requirements

- **Images**: jpeg/png/webp/bmp/tiff/gif, aspect ratio 0.4–2.5, 300–6000 px/side, ≤30 MB. URL, base64 (`data:image/png;base64,...`), or `asset://<ID>`.
- **Reference videos** (2.0 only): mp4/mov, 480p/720p, 2–15s each, ≤3 totaling ≤15s, ≤50 MB, 24–60 fps.
- **Reference audios** (2.0 only): wav/mp3, 2–15s each, ≤3 totaling ≤15s, ≤15 MB.
- Prefer public URLs over base64 for large files (request body ≤64 MB).

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `content_blocked` | sensitive prompt/media | rewrite, drop sensitive terms |
| `invalid_params` | duration/resolution/ratio out of range | check against the limits above |
| `media_download_failed` | reference URL unreachable | ensure the URL is publicly accessible |
| `audio_only` | only `reference_audios` supplied | add at least one reference image or video |
| `mixed_scenarios` | first/last-frame mixed with references | use exactly one scenario |
| `quota_exceeded` | low balance | top up the account |
