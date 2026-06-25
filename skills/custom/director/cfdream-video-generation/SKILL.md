---
name: cfdream-video-generation
description: Use this skill when the user requests to generate, create, animate, or imagine videos by calling the cfdream MCP generate_video tool. Covers prompt craft, text-to-video vs image-to-video (first/last frame) vs multimodal reference, synchronized audio, and duration/resolution choices.
---

# Video Generation Skill (cfdream MCP)

## Overview

Generate high-quality video by calling the cfdream MCP `generate_video` tool. This skill is the **craft layer** — how to author a strong video prompt and which schema parameters to set. Workflow conventions (materials, delivery, async job management, approval, errors) are owned by your SOUL and the `error-handling` skill; follow those. For deep per-model constraints, see the model-specific skills (e.g. `seedance-video`, `happyhorse-video`).

> `generate_video` takes a **single text `prompt` string** plus typed parameters — there is **no JSON prompt file**. Compose the structured thinking below into one rich English description, and map references / frames / output specs onto the tool's schema fields.

Generation is **asynchronous** (typically 2–10 min). By default `wait=true` polls internally and returns the finished clip in one call. Use `wait=false` + `task_status`/`task_wait` to fire-and-forget or run scenes concurrently (manage these jobs transparently per your SOUL).

## Step 1 — Understand the request

Identify before composing:

- **Subject / action** — what happens in the shot
- **Visual language** — shot type, camera movement, style, mood, palette
- **Technical specs** — duration, aspect ratio, resolution, audio on/off
- **References** — material ids for a first/last frame or reference images/videos/audios

You do **not** need to inspect `/mnt/user-data` folders first.

## Step 2 — Choose ONE input scenario

These are **mutually exclusive** — never mix a first/last frame with reference inputs in a single call.

| Scenario | Set these parameters |
|---|---|
| Text-to-video | `prompt` only |
| Image-to-video (first frame) | `prompt` + `first_frame` |
| First + last frame control | `prompt` + `first_frame` + `last_frame` |
| Multimodal reference | `prompt` + `reference_images` (≤9) and/or `reference_videos` (≤3) and/or `reference_audios` (≤3) |

> In a storyboard flow, the common path is **image-to-video**: generate a scene image with `cfdream-image-generation`, then pass that material id as `first_frame`. `reference_audios` can never be sent alone — it must accompany a reference image or video.

## Step 3 — Compose the prompt (the craft)

Author the `prompt` as plain English. Structure: **subject → action → camera language → mood/style**. There are no `motion`/`camera` parameters — encode camera moves in the text: 镜头逐渐拉近 / slow zoom-in, 360° orbit, first-person POV, pan left, rack focus, tracking shot.

With audio on (default), put spoken dialogue in double quotes so the model voices it (e.g. `the mother says: "Be brave for me, darling."`); the model auto-adds matching voice, SFX, and background music. Keep prompts ≤ ~500 Chinese chars / ~1000 English words.

Use this checklist as a thinking aid (folds into the single string, not a payload): background/era/location · characters · camera (type, movement, angle, focus) · dialogue · audio cues (whistle, strings swell, ambient).

Example — text-to-video:
```json
generate_video({
  "prompt": "A crowded 1940s London railway platform, steam and smoke in the air; close-up two-shot of a mother and her young daughter saying goodbye, profile framing, both faces in focus, soft bokeh background; subtle handheld motion, slow push-in. The mother says: \"You must be brave for me, darling.\" Train whistle blows, strings swell then fade, station ambience. Cinematic wartime mood, desaturated warm palette, film grain.",
  "model": "auto",
  "resolution": "1080p",
  "duration_seconds": 5,
  "aspect_ratio": "16:9"
})
```

Example — image-to-video from a generated scene frame:
```json
generate_video({
  "prompt": "Camera slowly pushes in as the cat opens its eyes and the curtains stir in a soft breeze.",
  "model": "auto",
  "first_frame": "m3",
  "duration_seconds": 5
})
```

## Step 4 — Tool parameters (schema)

| Parameter | Default | Notes |
|---|---|---|
| `prompt` | — | Full structured description (required) |
| `model` | `"auto"` | Adapter id (e.g. `wan-2-0`, `doubao-seedance-2-0`) or a list to constrain; honor the **locked Production Spec** model. See per-model skills for capability differences. |
| `first_frame` / `last_frame` | — | **Material ids** of guiding frames (image-to-video) |
| `reference_images` / `reference_videos` / `reference_audios` | — | **Material ids** for multimodal reference (≤9 / ≤3 / ≤3); mutually exclusive with first/last frame |
| `duration_seconds` | `5` | 4–15 (model-dependent caps; `-1` = smart/auto). Prefer **5s while iterating**, 8–10s only for the final pass |
| `aspect_ratio` | `"adaptive"` | `16:9 9:16 1:1 4:3 3:4 21:9 adaptive` — keep **identical across all clips**; `adaptive` matches the first frame |
| `resolution` | `"720p"` | `480p 720p 1080p` — keep consistent across clips |
| `with_audio` | `true` | `false` skips synchronized audio (also lowers cost) |
| `quality_tier` | `"balanced"` | `balanced` while iterating, `best` only for the final pass |
| `watermark` | model default | `true`/`false` |
| `wait` | `true` | `false` returns a `task_id` for later `task_status`/`task_wait` |
| `model_specific` | — | raw API extras merged last, e.g. `{"tools": [{"type": "web_search"}]}` (text-to-video only) |

> Tool names may be host-namespaced (e.g. `mcp__cfdream__generate_video` / `cfdream_generate_video`). Call `list_models(task_type="video")` for enabled models, or `get_model_card(...)` for exact per-model duration/resolution/reference constraints (1080p support, reference-video/edit/extend capability, etc.).

## After generation

Each clip is **automatically streamed to the user and registered as a new material** on completion — do not present, download, or re-deliver it (your SOUL covers this). Report any `task_id` and continue other work while async jobs run; never abandon a task without reporting back. Local assembly (trim/concat/voiceover/subtitles via ffmpeg) is a separate post step — `localize_material` the clips you need to edit, then `present_files` the assembled local output. On error, surface the error `message` and follow the `error-handling` skill (never blindly re-fire a billed generation; poll an existing `task_id` instead of re-submitting after a timeout).

## Notes

- Always write prompts in **English** regardless of the user's language.
- The structured checklist is authoring discipline, not a payload — the tool consumes one `prompt` string.
- Pass **material ids** for frames/references, never URLs or local paths.
- Encode camera movement and spoken dialogue in the prompt text; there are no separate motion/voice parameters.
- Iterative refinement is normal; refine the prompt rather than regenerating identically.
