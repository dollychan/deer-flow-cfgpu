---
name: cfdream-video-model-select
description: Use this skill to pick the single best cfdream video model before calling generate_video. It compares every enabled video model across the Seedance / WAN 2.0, WAN 2.6-2.7, HappyHorse, and Kling families by capability, cost, speed, and parameter ranges (duration, resolution, aspect ratio, audio, references), then returns a ranked, reasoned shortlist of recommended model ids (primary + alternatives) to feed the generate tool — so the Human-in-the-Loop user can swap the model at confirmation with the trade-offs spelled out. Invoke whenever you must decide "which video model?" for a text-to-video, image-to-video, first/last-frame, reference-to-video, audio-driven, video-edit, or video-extend request.
---

# Choosing a cfdream video model

This skill runs the **model-selection decision** for video: given the user's intent, it filters and ranks every enabled video model by **capability → parameter fit → cost/speed priority**, and returns one `model` model id (plus the value ranges that model allows) for the `generate_video` call.

- It is the concrete engine for **Phase 3c "Choose the model"** of `cfdream-video-generation` (the overall understand → plan → execute loop).
- For deep per-family scenario reading and prompt craft, hand off to the family skill after selecting: `seedance-video`, `wan-video` (2.6/2.7), `happyhorse-video`. (Kling has no deep skill yet — this file is the reference.)
- The catalog below is a **snapshot**. It may be gated per deployment — verify with `list_models(task_type="video")` and confirm exact limits with `get_model_card(<id>)` before committing on anything cost-sensitive.
- **All model identifiers below are `model_id` (cfgpu model id), not adapter id.** The `generate_video(model=…)` parameter accepts both, but `model_id` is the canonical identifier used by the cfgpu API internally. To cross-reference with the adapter id (e.g. `doubao-seedance-2-0`), use `list_models` or consult the adapter registry.

**Tier legend:** `cost` 1 = cheapest … 5 = most expensive. `speed` 1 = slowest … 5 = fastest. (Higher speed = faster turnaround.)

---

## Step 1 — Determine the scenario

Read the user's inputs and intent into exactly ONE scenario. Scenarios are **mutually exclusive** — never mix a first/last frame with reference inputs.

| Scenario | The user provides / wants |
|---|---|
| **t2v** — text-to-video | just a prompt |
| **i2v** — image-to-video (first frame) | prompt + one starting image |
| **FL** — first + last frame | prompt + start image + end image (morph/precise control) |
| **ref** — multimodal reference | prompt + reference images (≤9) and/or reference videos (≤3) and/or reference audios (≤3) |
| **audio-drive** — audio-driven avatar | prompt + one image + one driving audio track (make the figure sing/talk/rap) |
| **edit** — video edit | prompt (edit instruction) + one source video + optional reference image(s) |
| **extend** — video extend/stitch | prompt + up to 3 videos to lengthen/join |

Rules for reading intent: a reference **audio** never stands alone (needs an image or video with it); if the user mixes "start/end image" with "use these as references", clarify which; **edit/extend** always need a source video.

## Step 2 — Capability gate (filter to models that CAN do the scenario)

Keep only models whose capability set covers the scenario. This is a hard filter — a model that lacks the capability is simply ineligible.

| Model (model id) | Family | t2v | i2v | FL | ref | audio-drive | edit | extend | cost | speed |
|---|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| `doubao-seedance-2-0-260128` | Seedance 2.0 | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | 3 | 2 |
| `wan-video` *(= Seedance 2.0 alias)* | Seedance 2.0 | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | 3 | 2 |
| `doubao-seedance-2-0-fast-260128` | Seedance 2.0 | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | 2 | 4 |
| `wan-video-fast` *(= Seedance 2.0 fast alias)* | Seedance 2.0 | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | 2 | 4 |
| `Doubao-Seedance-2.0-mini` | Seedance 2.0 | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ | **1** | 3 |
| `doubao-seedance-1-5-pro-251215` | Seedance 1.5 | ✅ | ✅ | ✅ | — | — | — | — | 2 | 3 |
| `wan2.7-t2v` | WAN 2.7 | ✅ | — | — | — | — | — | — | 3 | 2 |
| `wan2.7-i2v` | WAN 2.7 | — | ✅ | — | — | — | — | — | 3 | 2 |
| `wan2.7-r2v` | WAN 2.7 | — | — | — | ✅ *(img+vid)* | — | — | — | 3 | 2 |
| `wan2.7-videoedit` | WAN 2.7 | — | — | — | — | — | ✅ | — | 3 | 2 |
| `wan2.6-t2v` | WAN 2.6 | ✅ | — | — | — | — | — | — | 3 | 2 |
| `wan2.6-i2v` | WAN 2.6 | — | ✅ | — | — | ✅ | — | — | 3 | 2 |
| `wan2.6-r2v` | WAN 2.6 | — | — | — | ✅ *(img+vid)* | — | — | — | 3 | 2 |
| `happyhorse-1.0-t2v` | HappyHorse | ✅ | ✅ | — | ✅ *(img only)* | — | — | — | 2 | 3 |
| `happyhorse-1.0-i2v` | HappyHorse | — | ✅ | — | — | — | — | — | 2 | 3 |
| `happyhorse-1.0-r2v` | HappyHorse | — | — | — | ✅ *(img ≤9)* | — | — | — | 2 | 3 |
| `happyhorse-1.0-video-edit` | HappyHorse | — | — | — | — | — | ✅ *(≤5 ref img)* | — | 2 | 3 |
| `kling-video-o1` | Kling | ✅ | — | — | — | — | — | — | 4 | 2 |
| `kling-v3-omni` | Kling | ✅ | — | — | — | — | — | — | **5** | 2 |

Notes on the "ref" column: Seedance 2.0 accepts reference **images + videos + audios**; WAN 2.6/2.7 r2v accept **videos + images** (no audio ref); HappyHorse r2v/t2v accept **images only**.

## Step 3 — Parameter gate (filter by required value ranges)

Drop any surviving model that can't hit a value the intent demands. Parameter ranges are themselves capabilities.

| Model | Resolutions | Max duration | `-1` smart duration | Synced audio output | Notable extras / limits |
|---|---|---|:--:|:--:|---|
| `doubao-seedance-2-0-260128` / `wan-video` | 480p / 720p / 1080p | 15s | ✅ | ✅ (`with_audio`) | full multimodal; `web_search` (t2v) |
| `doubao-seedance-2-0-fast-260128` | 480p / 720p / 1080p | 12s | ✅ | ✅ | flat pricing across resolutions |
| `wan-video-fast` | 480p / 720p (**t2v: no 1080p**) | 12s | ✅ | ✅ | t2v capped at 720p |
| `Doubao-Seedance-2.0-mini` | 480p / 720p / 1080p | 15s | ✅ | ✅ | cheapest full-capability model |
| `doubao-seedance-1-5-pro-251215` | 480p / 720p / 1080p | 12s | ✅ | ✅ | **`sample_mode`** fast cheap preview; no ref/edit/extend/web_search |
| `wan2.7-*` / `wan2.6-t2v` / `wan2.6-r2v` | 720p / 1080p | explicit only | ❌ | ❌ (no audio) | must pass an explicit duration |
| `wan2.6-i2v` | 720p / 1080p | explicit only | ❌ | audio **input** drive (1 track) | audio-driven avatar; no synced-audio generation |
| `happyhorse-1.0-*` | **720p / 1080p (no 480p)** | explicit only | ❌ | ❌ (**no audio at all**) | no last-frame; no `21:9`/`adaptive`; `seed` supported; edit follows source dur/ratio |
| `kling-video-o1` | 480p / 720p / 1080p | explicit only | ❌ | not exposed via unified schema | `quality_tier: best → pro` mode |
| `kling-v3-omni` | up to >1080p | explicit only | ❌ | not exposed via unified schema | premium tier; `best → pro` mode |

Common disqualifiers to check:
- **Need synced audio / dialogue / music?** → Seedance / WAN 2.0 family (`with_audio`). HappyHorse and WAN 2.6-t2v/2.7/r2v have none. For an **audio-driven** figure, only `wan2.6-i2v`.
- **Need 480p** (drafts / cheapest) → Seedance/WAN 2.0 or Kling; not HappyHorse, not WAN 2.6/2.7.
- **Need `-1` smart/auto duration** → only the Seedance/WAN 2.0 family; everyone else needs an explicit number of seconds.
- **Need duration > 12s** → only the 15s models (`doubao-seedance-2-0-260128`, `wan-video`, `Doubao-Seedance-2.0-mini`).
- **Need a quick cheap preview before the real render** → `doubao-seedance-1-5-pro-251215` in `sample_mode`.

## Step 4 — Rank by the user's priority (quality vs cost vs speed)

Among the models that pass both gates, pick with the priority the user signaled in the intent:

- **Top quality / "the best"** → the family flagship. General default across families: `doubao-seedance-2-0-260128` (full capability, high fidelity). For pure text-to-video cinematic quality, `wan2.7-t2v` or `kling-*` are strong; Kling is the premium (and priciest) option.
- **Speed / cost with the full feature set** → a `fast` variant (`doubao-seedance-2-0-fast-260128` / `wan-video-fast`, cost 2 / speed 4).
- **Cheapest / high-volume / at scale** → `Doubao-Seedance-2.0-mini` (cost 1) — same capabilities as 2.0.
- **No preference** → default to `doubao-seedance-2-0-260128` (safe, full-capability); switch to a `fast`/`mini` variant if the job is large or budget-sensitive.

Tie-breakers: prefer the model with the **narrowest** superfluous capability (don't pay Kling's premium for a shot a Seedance model does), keep the whole production on **one family** for visual consistency across clips, and honor any **locked Production Spec** model over all of the above.

## Step 5 — Return a recommended model list (not a single pick)

Generation is **Human-in-the-Loop**: the user can swap the model at the `generate_video` confirmation. So don't hand back one model — return a short **ranked shortlist** the user can choose from, each with the reason behind it. This turns the swap into an informed choice instead of a guess.

Return, in order:

1. **Recommended** — the model you'd default to (top of Step 4), marked as the pick, with the parameter ranges it allows (max duration, resolutions, audio yes/no, whether `-1` is legal).
2. **2–3 alternatives** the user is likely to want instead — typically the "cheaper", the "faster", and/or the "higher quality" neighbor that still passes the Step 2–3 gates for this scenario.

For **each** entry give a one-line reason tied to the trade-off that matters here, e.g.:

> - **`doubao-seedance-2-0-260128` — recommended.** Full capability, highest fidelity; 1080p, up to 15s, synced audio. *(cost 3 / speed 2)*
> - `doubao-seedance-2-0-fast-260128` — same features, ~2× faster and cheaper, slightly lower fidelity; 12s cap. *(cost 2 / speed 4)*
> - `Doubao-Seedance-2.0-mini` — cheapest full-capability option for batch/at-scale; 15s. *(cost 1 / speed 3)*
> - `doubao-seedance-1-5-pro-251215` — use `sample_mode` for a fast cheap preview before the real render. *(cost 2 / speed 3)*

Rules for the shortlist:
- Every listed model **must pass the Step 2 capability gate and Step 3 parameter gate** for this scenario — never offer a model that can't actually do the job (e.g. don't list HappyHorse for a scenario that needs audio, or Kling for image-to-video).
- Keep alternatives on **compatible families** so a swap doesn't silently drop a required feature; if an alternative changes what's possible (loses audio, loses a resolution, needs an explicit duration), **say so** in its reason.
- State the **shared parameter constraints** the user should keep identical across clips (aspect ratio, resolution) so a mid-production swap stays consistent.
- Honor a **locked Production Spec** model: still surface it as the recommended pick, and note that alternatives would deviate from the spec.

Then hand the prompt-craft and exact `generate_video` schema mapping to `cfdream-video-generation` (and the family skill).

---

## Quick scenario → shortlist

| Scenario / need | Go-to (quality → cost) |
|---|---|
| Text-to-video, general | `doubao-seedance-2-0-260128` → `-fast` → `-mini`; cinematic t2v: `wan2.7-t2v` / `kling-video-o1` |
| Image-to-video (first frame) | `doubao-seedance-2-0-260128` → `-fast` → `-mini`; also `wan2.7-i2v`, `happyhorse-1.0-i2v` |
| First + last frame morph | Seedance only: `doubao-seedance-2-0-260128` / `-fast` / `-mini` / `doubao-seedance-1-5-pro-251215` |
| Reference-to-video (images) | `doubao-seedance-2-0-260128` (img+vid+aud), `happyhorse-1.0-r2v` (≤9 img), `wan2.7-r2v` (img+vid) |
| Reference with a **video** track | Seedance 2.0 family or `wan2.*-r2v` (HappyHorse can't take a reference video) |
| Audio-driven avatar | **only** `wan2.6-i2v` |
| Synced audio (voice/SFX/music) | Seedance / WAN 2.0 family (`with_audio=true`) |
| Video edit (swap object/outfit) | `wan2.7-videoedit`, `happyhorse-1.0-video-edit`, or Seedance 2.0 edit |
| Video extend / stitch clips | Seedance 2.0 family only (`doubao-seedance-2-0-260128` / `-fast` / `-mini`, `wan-video`) |
| Cheapest at scale | `Doubao-Seedance-2.0-mini` (cost 1) |
| Fast preview before final | `doubao-seedance-1-5-pro-251215` + `sample_mode` |

## Notes

- `wan-video` / `wan-video-fast` are **aliases of Seedance 2.0 / 2.0 fast** (same `SeedanceVideoAdapter`, identical capabilities and schema) — treat them as the Seedance 2.0 family, just different ids.
- The Seedance/WAN 2.0 family uses the unified `content[]` schema and is the only family with `-1` smart duration, 480p, synced-audio generation, multimodal reference, edit, and extend all in one model.
- WAN 2.6/2.7, HappyHorse, and Kling **split capabilities across task-specialized model ids** — choosing the model *is* choosing the scenario; they need an **explicit** duration.
- Pricing shapes: Seedance/WAN 2.0 bill **per token** (audio and video-input change the rate); WAN 2.6/2.7 and HappyHorse and Kling bill **per second** with a resolution tier (≤720p vs >720p). Kling is the priciest family; `-mini` is the cheapest model.
- Always confirm live availability and exact caps with `list_models` / `get_model_card` — deployments can disable models or change limits.
