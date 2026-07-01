---
name: wan-video
description: Understand what a user wants from a video and match it to the right 万相 (Wanxiang / WAN) 2.6 or 2.7 model, the right scenario (text-to-video, image-to-video, audio-driven avatar, reference-to-video, video edit), and parameter values that fit their intent. Use when the user wants 万相/WAN video, cinematic shot-by-shot narration, audio-driven avatars, character reference video, or video element replacement.
---

# Choosing a 万相 (WAN) model and parameters

The job of this skill is to read the user's request, figure out what they actually want, and translate that into: **which 万相 model**, **which scenario**, and **which parameter values**. Focus on intent — not on how the video is technically produced.

## Step 1 — Pick the model = pick the scenario

Unlike Seedance, the 万相 family bakes the *capability into the model id* — there is no single "wan" model that does everything. Choose the variant that matches what the user wants.

| Model | Scenario | What the user provides |
|---|---|---|
| `wan-2-7-t2v` | Text-to-video (2.7, newest) | a prompt |
| `wan-2-6-t2v` | Text-to-video (2.6) | a prompt |
| `wan-2-7-i2v` | Image-to-video (2.7) | prompt + first frame |
| `wan-2-6-i2v` | Image-to-video **+ audio-drive** (2.6) | prompt + first frame + optionally one audio track |
| `wan-2-7-r2v` | Reference-to-video (2.7) | prompt + reference video(s) and/or image(s) |
| `wan-2-6-r2v` | Reference-to-video (2.6) | prompt + reference video(s) and/or image(s) |
| `wan-2-7-videoedit` | Video editing (2.7 only) | prompt + one source video + optional reference image(s) |

How to decide, based on intent:

- Just a description → `wan-2-7-t2v` (default to 2.7 for newest quality)
- Bring a still image to life → `wan-2-7-i2v`
- **Audio-driven avatar** — make the figure in an image sing/rap/talk to an audio track → `wan-2-6-i2v` (only 2.6 i2v accepts an audio track; 2.7 i2v does not)
- Drive a video from **character/scene references** (the prompt refers to `character1`, etc.) → `wan-2-7-r2v` / `wan-2-6-r2v`
- **Edit an existing video** (replace clothing/objects using a reference image) → `wan-2-7-videoedit`
- Default to the **2.7** variant for quality; reach for **2.6** specifically when you need audio-driven i2v.

## Step 2 — Read the scenario correctly

Send only the inputs that match the chosen variant.

- **t2v** takes a prompt only.
- **i2v** is first-frame only — no last-frame morphing anywhere in this family.
- Audio-driven avatar is a **2.6 i2v** feature only; if the user asks for it on 2.7, switch them to `wan-2-6-i2v`.
- **r2v** needs at least one reference (video or image); in the prompt, videos come before images and are named `character1`, `character2`, …
- **videoedit** needs exactly one source video, plus optional reference image(s) that define the change.

## Step 3 — Set parameters to match intent

| Parameter | Default | How to choose from intent |
|---|---|---|
| Duration | none | Must be an explicit number of seconds (no smart/auto). Longer for storytelling, shorter for clips. |
| Resolution | 720p | `720p` is the cheaper tier; go higher only when quality matters (raises cost). |

## Step 4 — Help the user write a good prompt

- **t2v** shines at shot-by-shot narration — structure as `第N个镜头[start-end秒] 景别：描述`, then subject → action → camera language → mood. Encode camera moves in text: 镜头逐渐拉近, 360度环绕运镜, 第一人称视角, 镜头向左平移.
- **r2v** prompts reference the supplied media as `character1`, `character2`, … (videos are listed before images).
- **videoedit** prompts are edit instructions ("将…替换为…") describing how the reference image changes the source video.
- Keep prompts within roughly 500 Chinese characters / 1000 English words.

## Step 5 — Confirm the choice back to the user

Before generating, briefly state the model, scenario, and key parameters you chose and why, so the user can correct any misread intent. If a request is ambiguous (e.g. audio-driven vs. plain i2v, or which media are references vs. a source to edit), ask one focused question rather than guessing.
