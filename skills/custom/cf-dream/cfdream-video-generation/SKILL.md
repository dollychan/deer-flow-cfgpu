---
name: cfdream-video-generation
description: "Use this skill when the user requests to generate, create, animate, or imagine videos by calling the cfdream MCP generate_video tool. It drives the full loop — understand the intent, plan the production steps, then execute each step: confirm the step's target, choose the right model (by capability, cost, speed, and parameter ranges), and emit a concrete tool call from the MCP schema. Covers prompt craft, text-to-video vs image-to-video (first/last frame) vs multimodal reference, synchronized audio, and duration/resolution choices."
---

# Video Generation Skill (cfdream MCP)

## Overview

This skill helps you turn a user's video intent into finished clips by calling the cfdream MCP `generate_video` tool. It is organized as a **three-phase loop**:

1. **Understand the intent** — what the user actually wants.
2. **Plan the steps** — break the intent into concrete production steps, each with a target output.
3. **Execute each step** — before every generation, confirm the step target, pick the right model, compose the prompt, and emit a tool call from the MCP schema.

This is the **craft + decision layer**: how to author a strong video prompt, how to choose a model, and which schema parameters to set. Workflow conventions (materials, delivery, async job management, approval, errors) are owned by your SOUL and the `error-handling` skill; follow those. For deep per-model constraints, read the model-specific skills (`seedance-video`, `happyhorse-video`, `wan-video`).

> `generate_video` takes a **single text `prompt` string** plus typed parameters — there is **no JSON prompt file**. Compose the structured thinking below into one rich English description, and map references / frames / output specs onto the tool's schema fields.

Generation is **asynchronous** (typically 2–10 min). By default `wait=true` polls internally and returns the finished clip in one call. Use `wait=false` + `task_status`/`task_wait` to fire-and-forget or run scenes concurrently (manage these jobs transparently per your SOUL).

---

## Phase 1 — Understand the intent

Before touching any tool, extract what the user is really asking for. Identify:

- **Deliverable & scope** — one clip, or a multi-scene sequence / episode (剧集)? A one-shot render, or an iterate-then-finalize job?
- **Subject / action** — what happens in the shot.
- **Visual language** — shot type, camera movement, style, mood, palette.
- **Technical specs** — duration, aspect ratio, resolution, audio on/off.
- **References** — material ids for a first/last frame or reference images/videos/audios.
- **Priorities & constraints** — does the user lean toward **quality**, **speed**, or **cost**? Any budget, deadline, or a **locked Production Spec** to honor?

You do **not** need to inspect `/mnt/user-data` folders first. If the intent is genuinely ambiguous on something that changes the output (e.g. quality vs. cost, or which images are start/end vs. references), ask **one** focused question rather than guessing.

---

## Phase 2 — Plan the steps

Translate the intent into an explicit, ordered list of production steps. Each step should have a **single, testable target output** (usually one new material).

- **Decompose the deliverable.** A single clip is one step. A multi-scene sequence is one step per scene (plus a final assembly step). For a storyboard/episode, the common shape is: generate each scene image with `cfdream-image-generation` → animate each into a clip via image-to-video → assemble locally.
- **Order by dependency.** Image-to-video needs its `first_frame` material to exist first, so the image step precedes the clip step. Reference-driven clips need their reference materials ready.
- **Lock cross-clip consistency up front.** Decide `aspect_ratio` and `resolution` once and keep them **identical across every clip** so the pieces cut together.
- **Plan the iterate→final strategy.** Default to cheap/fast settings (5s, `720p`, `balanced`, `wait=true` one at a time) while dialing in a look, then a final pass at the committed settings. Note where concurrency (`wait=false`) helps for independent scenes.

State the plan back to the user when the job is non-trivial, so they can correct a misread before generation spends budget.

---

## Phase 3 — Execute each step

Work the steps in order. For **each** step, run this checklist **before** firing the tool:

### 3a. Confirm the step target
Restate what this specific step must produce (e.g. "a 5s establishing shot of the rainy alley, 16:9, to be the opening clip"). Everything below is chosen to serve that target.

### 3b. Choose ONE input scenario
These are **mutually exclusive** — never mix a first/last frame with reference inputs in a single call.

| Scenario | Set these parameters |
|---|---|
| Text-to-video | `prompt` only |
| Image-to-video (first frame) | `prompt` + `first_frame` |
| First + last frame control | `prompt` + `first_frame` + `last_frame` |
| Multimodal reference | `prompt` + `reference_images` (≤9) and/or `reference_videos` (≤3) and/or `reference_audios` (≤3) |

> In a storyboard flow the common path is **image-to-video**: pass the scene image's material id as `first_frame`. `reference_audios` can never be sent alone — it must accompany a reference image or video.

### 3c. Choose the model (capability · cost · speed · parameter ranges)
Use the `cfdream-video-model-select` skill — it holds the full model catalog and runs the capability → parameter → priority filter. Its decision, in short, reasons in this order:

1. **Capability gate first.** Some scenarios only exist on some families — filter to models that *can* do the step at all:
   - Need **audio** (dialogue/SFX/music synced)? → Seedance or WAN 2.6 i2v. HappyHorse has **no audio**.
   - Need a **reference video/audio**, **edit**, or **extend/stitch**? → a Seedance 2.0 model or the matching WAN variant.
   - Need **last-frame morphing**? → Seedance (HappyHorse/WAN have no last frame).
   - Need **480p** or **smart/auto duration** (`-1`)? → Seedance (HappyHorse has neither).
2. **Then quality vs cost vs speed**, following the user's priority from Phase 1: top quality → the family's flagship (e.g. `doubao-seedance-2-0-260128`); speed/cost with full features → a `fast` variant; cheapest / at-scale → a `mini` variant; a quick cheap look before committing → a preview/sample mode.
3. **Check parameter ranges fit the target** — max duration and supported resolutions differ per model (e.g. duration caps, 1080p support). If the target needs a value a model can't reach, pick a model that can.
4. **`auto` is a deliberate choice, not a skip.** `model="auto"` scores enabled models by quality/speed/cost, and a **list** constrains the pool — use either only when you have genuinely decided you're indifferent within that set. Always honor a **locked Production Spec** model.

**You must reach a concrete model before firing.** If the deciding information is missing — the scenario is ambiguous (is that image a first frame or a reference?), the quality-vs-cost-vs-speed priority is unstated, or the target needs a capability no clearly-right model covers — `ask_clarification` and wait rather than defaulting silently. If the client restricted the models for this task, you'll see a manual-mode `<system-reminder>` listing the allowed video model IDs; keep your recommendation (and any alternatives) **inside that list**, and if nothing in it fits the scenario, tell the user and ask instead of reaching outside it.

Because generation is **Human-in-the-Loop** — the user can swap the model at the confirmation — don't lock in a single model silently. Have `cfdream-video-model-select` produce a **ranked recommended list**: one **recommended** model plus 2–3 alternatives, each with a one-line reason (the cost/speed/quality trade-off) and its limits. Every option must pass the capability + parameter gates for this scenario, so any swap the user makes still works.

Use `list_models(task_type="video")` for the currently enabled models and `get_model_card(<id>)` for exact per-model duration/resolution/reference limits. For the decision detail of a family, read its skill: `seedance-video`, `happyhorse-video`, `wan-video`.

### 3d. Compose the prompt (the craft)
Author the `prompt` as plain **English**. Structure: **subject → action → camera language → mood/style**. There are no `motion`/`camera` parameters — encode camera moves in the text: 镜头逐渐拉近 / slow zoom-in, 360° orbit, first-person POV, pan left, rack focus, tracking shot.

With audio on (default, on capable models), put spoken dialogue in double quotes so the model voices it (e.g. `the mother says: "Be brave for me, darling."`); the model auto-adds matching voice, SFX, and background music. Keep prompts ≤ ~500 Chinese chars / ~1000 English words.

Use this checklist as a thinking aid (it folds into the single string, it is not a payload): background/era/location · characters · camera (type, movement, angle, focus) · dialogue · audio cues (whistle, strings swell, ambient).

### 3e. Emit the tool call from the schema
Map the choices onto the `generate_video` schema (§ Schema below) and call the tool. At the Human-in-the-Loop confirmation, present the **recommended model + the ranked alternatives with their reasons** (from 3c) alongside the scenario and key params, so the user can approve the pick or **swap the `model`** with the trade-offs in front of them. If the user swaps, re-check that the new model still supports the scenario and the chosen `duration_seconds` / `resolution` / audio (drop or adjust any param the new model can't take — e.g. no `-1`, no 480p, no audio), then fire.

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

### 3f. Verify against the step target, then advance
When the clip returns, check it actually meets the step target (subject, motion, specs, consistency with sibling clips). If not, **refine the prompt** and re-run — don't regenerate identically. When it passes, move to the next planned step. Never abandon an async `task_id` without reporting back.

---

## Schema — `generate_video` parameters

| Parameter | Default | Notes |
|---|---|---|
| `prompt` | — | Full structured description (required) |
| `model` | `"auto"` | Model id / cfgpu model id (e.g. `doubao-seedance-2-0-260128`) or a list to constrain; honor the **locked Production Spec** model. See per-model skills for capability differences. |
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
- The structured checklists are authoring discipline, not payloads — the tool consumes one `prompt` string.
- Pass **material ids** for frames/references, never URLs or local paths.
- Encode camera movement and spoken dialogue in the prompt text; there are no separate motion/voice parameters.
- Keep `aspect_ratio` and `resolution` identical across every clip in a production so they cut together.
- Iterative refinement is normal; refine the prompt rather than regenerating identically.
