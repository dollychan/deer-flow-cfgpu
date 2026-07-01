---
name: seedance-video
description: Understand what a user wants from a video and match it to the right Doubao Seedance model (Seedance 2.0 / 2.0 fast / 2.0 mini / 1.5 Pro), the right generation scenario (text-to-video, image-to-video, multimodal reference, edit, extend), and parameter values that fit their intent. Use when the user wants to create, animate, edit, or extend a video, or asks which Seedance model to pick.
---

# Choosing a Seedance model and parameters

The job of this skill is to read the user's request, figure out what they actually want, and translate that into: **which Seedance model**, **which scenario**, and **which parameter values**. Focus on intent — not on how the video is technically produced.

## Step 1 — Pick the model

| Model | Best for | Max duration | Resolutions | Reference video/audio · edit · extend |
|---|---|---|---|---|
| `doubao-seedance-2-0` | Highest quality, all capabilities | 15s | 480p/720p/1080p | ✅ |
| `doubao-seedance-2-0-fast` | Faster + cheaper, same capabilities | 12s | 480p/720p/1080p | ✅ |
| `doubao-seedance-2-0-mini` | Cheapest, high-frequency / at-scale, same capabilities | 15s | 480p/720p/1080p | ✅ |
| `doubao-seedance-1-5-pro` | 1080p with a fast preview mode | 12s | 480p/720p/1080p | ❌ (text / image-frame / audio-gen only) |

How to decide, based on what the user signals:

- Wants **top quality** or "the best" → `doubao-seedance-2-0`
- Cares about **speed or cost** but wants the full feature set → `doubao-seedance-2-0-fast`
- Wants the **cheapest** option or is generating **many videos / at scale** → `doubao-seedance-2-0-mini`
- Needs a **reference video/audio**, wants to **edit** an existing video, or **extend/stitch** clips → must be a 2.0 model (1.5 Pro can't do these)
- Wants a **quick cheap preview** before committing to a full render → `doubao-seedance-1-5-pro` in sample mode
- Gives no preference → default to `doubao-seedance-2-0`

## Step 2 — Identify the scenario

Match the user's inputs and intent to exactly ONE scenario. These are **mutually exclusive** — never mix first/last-frame with reference inputs.

| The user wants… | Scenario | What they provide |
|---|---|---|
| A video purely from a description | Text-to-video | just a prompt |
| To animate a starting image | Image-to-video (first frame) | prompt + first frame |
| To morph from one image to another | Image-to-video (first + last frame) | prompt + first frame + last frame |
| To guide the result with example media *(2.0 only)* | Multimodal reference | prompt + reference images (up to 9) and/or reference videos (up to 3) and/or reference audios (up to 3) |
| To change something inside an existing video *(2.0 only)* | Video edit | prompt describing the edit + the source video + reference image(s) |
| To lengthen or join existing clips *(2.0 only)* | Video extend | prompt + up to 3 videos to stitch |

Rules for reading intent:
- A reference **audio** never stands alone — it only makes sense alongside at least one reference image or video.
- If the user mixes a "start/end image" idea with "use these as references," clarify which one they mean; you can't do both at once.
- Reference / edit / extend intents require a 2.0 model — if the user asked for 1.5 Pro, steer them to a 2.0 model.

## Step 3 — Set parameters to match intent

Pick values from what the user says (or implies); otherwise use the sensible default.

| Parameter | Default | How to choose from intent |
|---|---|---|
| Duration | 5s | 4–15s (12s max on fast / 1.5 Pro). Use `-1` for smart/auto when the user doesn't care. Longer for storytelling, shorter for loops/clips. |
| Aspect ratio | `adaptive` | `16:9` for landscape/cinematic, `9:16` for phone/social/vertical, `1:1` for square posts, `4:3`/`3:4`, `21:9` for ultra-wide. `adaptive` matches the input image. |
| Resolution | 720p | `480p` for drafts/previews or speed/cost, `720p` general, `1080p` when quality matters. |
| Audio | on | Keep on when the user wants sound, dialogue, music, or effects. Turn off for silent clips or to reduce cost. |
| Watermark | model default | Turn off only if the user explicitly asks for no watermark. |
| Sample mode | off | Turn on for 1.5 Pro when the user wants a fast cheap preview. |
| Web search | off | Available only for text-to-video; enable when the prompt needs current/real-world grounding. |

## Step 4 — Help the user write a good prompt

Structure a prompt as: **subject → action → camera language → mood/style**.

- There are no separate "camera movement" or "motion intensity" controls — encode camera moves in the prompt text: 镜头逐渐拉近 (zoom in), 360度环绕运镜 (orbit), 第一人称视角 (POV), 镜头向左平移 (pan left).
- With audio on, put spoken dialogue in double quotes so the model voices it, e.g. `男人说："你记住，以后不可以用手指指月亮。"`. The model adds matching voice, sound effects, and background music automatically.
- Keep prompts within roughly 500 Chinese characters / 1000 English words.

## Step 5 — Confirm the choice back to the user

Before generating, briefly state the model, scenario, and key parameters you chose and why, so the user can correct any misread intent. If a request is ambiguous (e.g. quality vs. cost, which images are start/end vs. references), ask one focused question rather than guessing.
