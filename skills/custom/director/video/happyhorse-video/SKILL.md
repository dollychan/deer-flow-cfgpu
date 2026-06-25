---
name: happyhorse-video
description: Generate videos with the HappyHorse 1.0 model family (t2v / i2v / r2v / video-edit) by calling the cfgpu MCP tools — text-to-video, image-to-video (first frame), multi-image reference-to-video, and natural-language video editing. Use when the user wants to create, animate, reference, or edit a video with HappyHorse, or asks which HappyHorse model to pick.
---

# Generating videos with HappyHorse 1.0 (MCP)

This skill is for an agent that calls the **cfgpu MCP server** tools. The relevant tools:

- `generate_video(...)` — create a video task (this is the main one)
- `task_status(task_id)` — poll an async task's status
- `task_wait(task_id, timeout)` — block until a task finishes, then return the result
- `list_models(task_type="video")` — enumerate video models
- `get_model_card(model_name)` — fetch a model's full parameter/usage doc

> Tool names may be namespaced by the host (e.g. `mcp__cfgpu__generate_video`). Use whatever prefix your environment exposes; the parameters below are identical.

Generation is **asynchronous**. By default `generate_video` runs with `wait=true`, so it polls internally and returns the finished result in one call. Only switch to `wait=false` + `task_wait` when you need to fire-and-forget or run several jobs concurrently.

> HappyHorse has **no audio** at all — `with_audio` is ignored. It also has **no `480p`**, **no `last_frame`**, and **no `-1` smart duration**. If the user needs audio, reference *videos/audios*, last-frame control, or 480p, use the Seedance family instead.

## Step 1 — Pick the model (`model` parameter)

HappyHorse splits its capabilities across **four task-specialized models** — unlike Seedance, you must match the model to the scenario.

| `model` (adapter_id) | Scenario | Required inputs | Optional inputs |
|---|---|---|---|
| `happyhorse-1.0-t2v` | Text-to-video **+** first-frame i2v **+** reference-to-video (all-in-one) | `prompt` | `first_frame` **or** `reference_images` (≤9, mutually exclusive) |
| `happyhorse-1.0-i2v` | First-frame image-to-video only | `prompt` + `first_frame` | — |
| `happyhorse-1.0-r2v` | Multi-image reference-to-video (subject/scene consistency) | `prompt` + `reference_images` (≤9) | — |
| `happyhorse-1.0-video-edit` | Natural-language video editing | `prompt` + `reference_videos` (exactly 1 source) | `reference_images` (≤5) |

Decision guide:
- Pure text → `happyhorse-1.0-t2v` (prompt only).
- Animate one starting image → `happyhorse-1.0-t2v` or the dedicated `happyhorse-1.0-i2v` (both take `prompt` + `first_frame`).
- Keep a subject/outfit/scene consistent across the clip from up to 9 reference photos → `happyhorse-1.0-r2v` (or `happyhorse-1.0-t2v` with `reference_images`).
- Edit/restyle an existing video by instruction (e.g. swap an outfit, replace an object) → `happyhorse-1.0-video-edit`.
- `t2v` is the Swiss-army model (text, first-frame, and references in one); `i2v` / `r2v` are specialized single-purpose variants. Quality, cost (tier 2/5), and speed (tier 3/5) are identical across the family.

When unsure of exact constraints, call `get_model_card("happyhorse-1.0-t2v")` (or the specific model's card).

## Step 2 — Choose ONE scenario

The image scenarios are **mutually exclusive**. On `t2v`, `first_frame` and `reference_images` cannot both be set (the call is rejected). Pick the model that matches the single scenario you want.

| Scenario | Model | Set these parameters |
|---|---|---|
| Text-to-video | `happyhorse-1.0-t2v` | `prompt` only |
| Image-to-video (first frame) | `happyhorse-1.0-t2v` or `happyhorse-1.0-i2v` | `prompt` + `first_frame` |
| Reference-to-video (≤9 images) | `happyhorse-1.0-t2v` or `happyhorse-1.0-r2v` | `prompt` + `reference_images` |
| Video edit (1 source + ≤5 refs) | `happyhorse-1.0-video-edit` | `prompt` + `reference_videos` (1) + optional `reference_images` |

In reference-to-video prompts you can point at a specific image with `[Image N]`, e.g. `[Image 1]中的女性手持[Image 2]中的折扇`.

## Step 3 — Output parameters

| Parameter | Default | Notes |
|---|---|---|
| `prompt` | — | Required. ≤500 Chinese chars / ≤1000 English words. |
| `resolution` | `720p` | Only `720p` / `1080p` (sent uppercased as `720P` / `1080P`). **`480p` is rejected.** Pricing: ≤720P ¥0.945/s, >720P ¥1.68/s. |
| `aspect_ratio` | `16:9` | `16:9 9:16 1:1 4:3 3:4`. `4:5` / `5:4` only via `model_specific`. **`21:9` and `adaptive` are not supported** — `adaptive` is dropped (API defaults to `16:9`). |
| `duration_seconds` | `5` | Explicit seconds. **No `-1` smart mode.** |
| `watermark` | `true` | `true`/`false`. |
| `wait` | `true` | `false` returns a `task_id` immediately for later `task_wait`. |
| `timeout` | auto | Max wait seconds (poll default ~300s). |
| `return_metadata` | `true` | Include `seed`, `model_used`, token `usage` in the result. |
| `model_specific` | — | Extra raw API params, e.g. `{"seed": 42}` (range 0–2147483647), or `{"ratio": "4:5"}` for the extra aspect ratios. |

**Not supported (ignored or rejected):** `with_audio` (no audio), `last_frame`, `reference_audios`, `reference_videos` (except on `video-edit`).

> **`video-edit` exception:** output duration and aspect ratio **follow the source video**, so `duration_seconds` and `aspect_ratio` are not sent — only `resolution`, `watermark`, and `model_specific` apply.

## Step 4 — Call the tool

### Text-to-video
```json
generate_video({
  "prompt": "一座由硬纸板和瓶盖搭建的微型城市，在夜晚焕发出生机。一列硬纸板火车缓缓驶过，小灯点缀其间。",
  "model": "happyhorse-1.0-t2v",
  "resolution": "1080p",
  "aspect_ratio": "16:9",
  "duration_seconds": 5
})
```

### Image-to-video (first frame)
```json
generate_video({
  "prompt": "一只猫在草地上奔跑，镜头缓慢跟随",
  "model": "happyhorse-1.0-i2v",
  "first_frame": "https://example.com/cat.jpg",
  "resolution": "720p",
  "duration_seconds": 5
})
```

### Reference-to-video (≤9 images, subject/scene consistency)
```json
generate_video({
  "prompt": "[Image 1]中身着红色旗袍的女性，轻抬玉手展开[Image 2]中的折扇，[Image 3]中的流苏耳坠随头部转动轻盈摆动，多视角展现东方韵味。",
  "model": "happyhorse-1.0-r2v",
  "reference_images": [
    "https://example.com/1.jpg",
    "https://example.com/2.jpg",
    "https://example.com/3.jpg"
  ],
  "resolution": "720p",
  "aspect_ratio": "16:9",
  "duration_seconds": 5
})
```

### Video edit (1 source video + up to 5 reference images)
```json
generate_video({
  "prompt": "让视频中的马头人身角色穿上图片中的条纹毛衣，运镜不变",
  "model": "happyhorse-1.0-video-edit",
  "reference_videos": ["https://example.com/source.mp4"],
  "reference_images": ["https://example.com/sweater.jpg"],
  "resolution": "720p"
})
```

### Fixed seed for reproducibility
```json
generate_video({
  "prompt": "宇宙飞船穿越星云",
  "model": "happyhorse-1.0-t2v",
  "resolution": "1080p",
  "model_specific": {"seed": 42}
})
```

### Async: fire now, collect later
```json
generate_video({ "prompt": "...", "model": "happyhorse-1.0-t2v", "wait": false })  // → { "task_id": "task-..." }
task_status({ "task_id": "task-..." })                // → { "status": "running" | "succeeded" | "failed", ... }
task_wait({ "task_id": "task-...", "timeout": 600 })  // blocks, then → final result
```

## Reading the result

`generate_video` / `task_wait` return a normalized object:

```json
{
  "urls": ["https://.../video.mp4"],      // the generated video
  "expires_at": "2026-06-18T12:00:00Z",   // URL valid ~24h — download promptly
  "artifact": true,
  "metadata": {                            // present when return_metadata=true
    "task_id": "task-...",
    "model_used": "happyhorse-1.0-t2v",
    "aspect_ratio": "16:9",
    "seed": 42,
    "usage": { "total_tokens": 230 }       // billing is per token / per second
  }
}
```

Give the user the `urls` value and warn that the link expires in ~24 hours. On error, the tool returns an error dict instead — surface its `message`.

## Writing good prompts

Structure: **subject → action → camera language → mood/style**. There are no `motion_intensity` / `camera_movement` parameters — encode camera moves in the prompt text: 镜头逐渐拉近 (zoom in), 360度环绕运镜 (orbit), 第一人称视角 (POV), 镜头向左平移 (pan left), 低角度仰拍 (low-angle).

For reference-to-video, anchor each subject to its image with `[Image N]` so the model knows which reference to draw from. HappyHorse produces **no audio**, so don't bother writing spoken dialogue — it won't be voiced.

## Input asset requirements

- **Images** (`first_frame` / `reference_images`): jpeg/png/webp/bmp/tiff/gif. URL, base64 (`data:image/png;base64,...`), or `asset://<ID>`.
- **Source video** (`video-edit` only): a single mp4/mov URL in `reference_videos`.
- Prefer public URLs over base64 for large files (request body ≤64 MB).
- `first_frame` and `reference_images` are mutually exclusive on `t2v`.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `... does not support last_frame` | `last_frame` set | HappyHorse has no last-frame control; drop it (use Seedance if needed) |
| `... does not support reference_videos` | reference videos on a non-edit model | use `happyhorse-1.0-video-edit`, or Seedance |
| `... minimum resolution is 720p` | `resolution="480p"` | use `720p` or `1080p` |
| `... requires an explicit duration (no -1 smart mode)` | `duration_seconds=-1` | pass an explicit number of seconds |
| `first_frame and reference_images are mutually exclusive` | both set on `t2v` | choose one scenario |
| `... accepts at most 9 reference_images` | >9 references | trim to ≤9 (≤5 for `video-edit`) |
| `... requires a source video (reference_videos)` | `video-edit` with no source | supply exactly one source video |
| `content_blocked` | sensitive prompt/media | rewrite, drop sensitive terms |
| `invalid_params` | duration/resolution/ratio out of range | check against the limits above (no `21:9`/`adaptive`/`480p`) |
| `media_download_failed` | reference URL unreachable | ensure the URL is publicly accessible |
| `quota_exceeded` | low balance | top up the account |
