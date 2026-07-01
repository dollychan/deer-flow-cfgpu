---
name: happyhorse-video
description: Understand what a user wants from a video and match it to the right HappyHorse 1.0 model (t2v / i2v / r2v / video-edit), the right scenario (text-to-video, image-to-video, multi-image reference-to-video, natural-language video editing), and parameter values that fit their intent. Use when the user wants to create, animate, reference, or edit a video with HappyHorse, or asks which HappyHorse model to pick.
---

# Choosing a HappyHorse model and parameters

The job of this skill is to read the user's request, figure out what they actually want, and translate that into: **which HappyHorse model**, **which scenario**, and **which parameter values**. Focus on intent — not on how the video is technically produced.

> HappyHorse has **no audio** at all, **no 480p**, **no last-frame control**, and **no smart/auto duration**. If the user needs audio, a reference video/audio track, last-frame morphing, or 480p, steer them to the Seedance family instead.

## Step 1 — Pick the model = pick the scenario

Unlike Seedance, HappyHorse splits its capabilities across **four task-specialized models**, so choosing the model *is* choosing the scenario. Quality, cost, and speed are identical across the family — pick purely by what the user wants to do.

| Model | Scenario | What the user provides |
|---|---|---|
| `happyhorse-1.0-t2v` | All-in-one: text-to-video, first-frame animate, or reference-to-video | a prompt, optionally a first frame **or** reference images (not both) |
| `happyhorse-1.0-i2v` | Animate a single starting image | prompt + first frame |
| `happyhorse-1.0-r2v` | Keep a subject/outfit/scene consistent from up to 9 photos | prompt + reference images |
| `happyhorse-1.0-video-edit` | Change something inside an existing video by instruction | prompt + the source video + optional reference image(s) |

How to decide, based on intent:

- Just a description → `happyhorse-1.0-t2v`
- Bring one still image to life → `happyhorse-1.0-i2v` (or `t2v` with a first frame)
- Keep a character/outfit/scene consistent across the clip from example photos → `happyhorse-1.0-r2v` (or `t2v` with reference images)
- Restyle or alter an existing video (swap an outfit, replace an object) → `happyhorse-1.0-video-edit`
- Think of `t2v` as the Swiss-army model; `i2v` / `r2v` are focused single-purpose variants.

## Step 2 — Read the scenario correctly

The image scenarios are **mutually exclusive** — on `t2v` you cannot combine a first frame with reference images. Pick the one scenario the user actually wants.

- First-frame animate → a single starting image only.
- Reference-to-video → up to 9 photos for subject/scene consistency. In the prompt, anchor each subject to its image with `[Image N]`, e.g. `[Image 1]中的女性手持[Image 2]中的折扇`.
- Video edit → exactly one source video, plus optional reference images that describe the change. Note the output length and aspect ratio **follow the source video**, so duration/aspect-ratio choices don't apply here.
- If the user's request implies both "start image" and "use these as references," clarify which they mean — HappyHorse can't do both at once.

## Step 3 — Set parameters to match intent

| Parameter | Default | How to choose from intent |
|---|---|---|
| Duration | 5s | Explicit seconds only (no smart/auto). Longer for storytelling, shorter for loops/clips. |
| Aspect ratio | `16:9` | `16:9` landscape/cinematic, `9:16` phone/social/vertical, `1:1` square, `4:3`/`3:4`. `21:9` and `adaptive` are **not** supported (adaptive falls back to 16:9). |
| Resolution | 720p | `720p` general, `1080p` when quality matters. **480p is not available.** |
| Watermark | on | Turn off only if the user explicitly asks for no watermark. |
| Seed | random | Set a fixed seed when the user wants reproducible results. |

## Step 4 — Help the user write a good prompt

Structure a prompt as: **subject → action → camera language → mood/style**.

- There are no separate "camera movement" or "motion intensity" controls — encode camera moves in the prompt text: 镜头逐渐拉近 (zoom in), 360度环绕运镜 (orbit), 第一人称视角 (POV), 镜头向左平移 (pan left), 低角度仰拍 (low-angle).
- For reference-to-video, anchor each subject to its image with `[Image N]` so the model knows which reference to draw from.
- HappyHorse produces **no audio** — don't write spoken dialogue, it won't be voiced.
- Keep prompts within roughly 500 Chinese characters / 1000 English words.

## Step 5 — Confirm the choice back to the user

Before generating, briefly state the model, scenario, and key parameters you chose and why, so the user can correct any misread intent. If a request is ambiguous (e.g. which images are a start frame vs. references, or a feature HappyHorse can't do like audio/480p/last-frame), ask one focused question or point them to Seedance rather than guessing.
