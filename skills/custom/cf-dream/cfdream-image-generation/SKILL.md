---
name: cfdream-image-generation
description: Use this skill when the user requests to generate, create, imagine, or visualize images — characters, scenes, products, comics, or any visual content — by calling the cfdream MCP generate_image tool. Covers reading user intent — especially when a reference image is given (style/identity reference vs. edit, reframe, or upscale) — plus prompt craft, reference images, image groups (组图), and aspect ratio / resolution choices.
---

# Image Generation Skill (cfdream MCP)

## Overview

Generate high-quality images by calling the cfdream MCP `generate_image` tool. This skill is the **craft layer** — how to author a strong image prompt and which schema parameters to set. Workflow conventions (materials, delivery, approval, async, errors) are owned by your SOUL and the `error-handling` skill; follow those, don't restate them.

> `generate_image` takes a **single text `prompt` string** plus typed parameters — there is **no JSON prompt file**. Compose the structured thinking below into one rich English description, and map everything else (references, ratio, resolution) onto the tool's schema fields.

## Step 1 — Understand the request (do this first, it decides everything)

The single most important step is reading **what the user actually wants** — the same image and words can mean very different jobs, and misreading the intent wastes a billed generation. Identify before composing:

- **Goal / operation** — what should happen? (create something new, keep a character's identity, restyle, edit one element, reframe, upscale …) — see the reference-image table below.
- **Subject / content** — what is in the image (character, scene, product, comic panel …)
- **Style** — art style, mood, color palette, rendering (photographic, illustration, 3D …)
- **Composition & technical** — framing, lighting, level of detail, aspect ratio, resolution
- **References** — any material ids (uploads / prior generations / search hits) and, crucially, **the role each one plays** for this goal.

You do **not** need to inspect `/mnt/user-data` folders first.

### When a reference image is involved — read the intent, don't assume

A reference image is **not** self-explanatory. Before calling the tool, decide which of these the user means — each is a different job:

| The user wants to… | Intent | How to handle |
|---|---|---|
| Create something **new** guided by the reference (borrow style/composition/mood) | Style / composition reference | Pass the id via `reference_images`; describe the new subject in the prompt, referring to the ref positionally ("in the style of reference 1"). |
| **Keep the same character/subject/product** in a new pose, scene, or outfit | Identity reference | `reference_images` + a prompt that fixes the identity and changes only the context ("the same woman from reference 1, now walking on a beach"). Consider `image_search` for extra angles. |
| **Combine** elements from several images (this character + that background/object) | Multi-reference composition | Multiple ids in `reference_images`; name each positionally in the prompt ("character from reference 1, jacket from reference 2"). |
| **Edit / modify** one thing in the image (swap an object, change a color, add/remove an element) | Local edit | `reference_images` + a precise prompt describing *only* the change and what to preserve. |
| **Reframe / recompose** — change aspect ratio, crop, or extend the canvas (outpaint) | Reframe | Set the target `aspect_ratio`; prompt to extend/recompose while preserving the subject. For an **exact** crop/resize with no content change, prefer a local tool (ffmpeg/ImageMagick in the sandbox) over a billed re-generation. |
| **Upscale / enhance** — higher resolution or cleaner detail, same content | Upscale | Raise `resolution` (and `quality_tier`) with a faithful prompt. If the user only wants a bigger file with identical pixels, a local upscale is cheaper than regenerating. |

If the goal is genuinely ambiguous (e.g. "use this image" — reference vs. edit vs. upscale?), **ask one focused question** before generating rather than guessing. State the interpretation you're acting on when you proceed, so the user can correct it.

## Step 2 — Compose the prompt (the craft)

Author the `prompt` as plain English (best model adherence regardless of the user's language). Cover, in one flowing description: **subject → style → composition → lighting → color palette**, plus a short "avoid …" clause for negative intent. Use this checklist as a thinking aid — it is *not* a JSON payload, it folds into the single string:

- **Character design** — gender, age, ethnicity, body type; facial features & expression; clothing & accessories; era/setting; pose & context.
- **Scene generation** — environment; time of day & weather; mood/atmosphere; focal point & composition.
- **Product visualization** — materials & details; lighting setup; background/context; presentation angle.

When referencing assets, name them positionally in the text (e.g. "the character from the first reference, beside the vehicle from the second") and pass their **material ids** via `reference_images`.

Example — request "a 1990s Tokyo street-style woman":
```json
generate_image({
  "prompt": "Japanese woman, mid-20s, slender and elegant; delicate features, expressive eyes, subtle lip-focused makeup, long dark hair partly wet from rain; stylish trench coat, designer handbag, high heels, 1990s Tokyo street fashion. Leica M11 street-photography aesthetic, film grain, natural warm palette, bokeh background. Medium shot, rule of thirds, subject off-center, Tokyo street context, shallow depth of field. Neon storefront lighting, wet-pavement reflections, rim light from background neons. Avoid: blurry/deformed face, oversaturated colors, studio/posed/selfie look.",
  "model": "auto",
  "aspect_ratio": "2:3",
  "resolution": "2K"
})
```

Example — with reference materials (refer positionally, pass ids):
```json
generate_image({
  "prompt": "The character from reference 1 standing next to a vehicle inspired by reference 2 on a bustling alien marketplace street, Star Wars original-trilogy aesthetic; worn leather jacket and utility vest, blaster holster; weathered repulsor vehicle in desert dust; multi-level alien architecture, hanging market stalls, alien passers-by; twin-suns golden hour, atmospheric dust, practical stall lighting; gritty lived-in look, film grain, cinematic medium-wide shot.",
  "model": "auto",
  "reference_images": ["m1", "m2"],
  "aspect_ratio": "16:9"
})
```

## Step 3 — Tool parameters (schema)

| Parameter | Default | Notes |
|---|---|---|
| `prompt` | — | Full structured description (required) |
| `model` | `"auto"` | Adapter id (e.g. `doubao-seedream-5-0-lite`) or a list to constrain auto-selection; honor the **locked Production Spec** model |
| `aspect_ratio` | `"1:1"` | `1:1 3:2 2:3 4:3 3:4 16:9 9:16 21:9` — keep **identical across all assets** in a production |
| `resolution` | `"2K"` | `1K 2K 3K 4K` — keep consistent across scenes |
| `reference_images` | — | List of **material ids** (`["m1"]`), never raw URLs |
| `n` | `1` | Image group size 1–15 for a related sequence (组图); only `doubao-seedream-*` models support `n>1` |
| `quality_tier` | `"balanced"` | `fast`/`balanced`/`best` — `balanced` while iterating, `best` only for the final pass |
| `watermark` | model default | `true`/`false`; ignored by some models |
| `wait` | `true` | leave true — image models are effectively synchronous; no polling needed |
| `model_specific` | — | raw API extras merged last, e.g. `{"tools": [{"type": "web_search"}]}` |

> Tool names may be host-namespaced (e.g. `mcp__cfdream__generate_image` / `cfdream_generate_image`). Use whatever prefix your environment exposes; parameters are identical. Call `list_models(task_type="image")` to see enabled models, or `get_model_card("doubao-seedream-5-0-lite")` for exact per-model constraints.

## Improving quality with reference images

For scenarios where visual accuracy matters (character poses/expressions, specific real objects/products, architecture/environment authenticity, fashion/clothing detail), **call `image_search` first**, then reference the results by their **material id**:

1. `image_search(query="Japanese woman street photography 1990s", size="Large")`
2. The search hits are registered as materials (they appear in your `<materials>` ledger with ids).
3. Pass the chosen ids as `reference_images` to `generate_image`.

Concrete visual guidance beats text alone.

## After generation

The result is **automatically streamed to the user and registered as a new material** — do not present, download, or re-deliver it (your SOUL covers this). Briefly describe the result and offer to iterate. If a result is wrong, **refine the prompt** rather than regenerating identically; to re-share an expired link, re-resolve the material by id — never re-generate (that re-bills). On error, surface the error `message` and follow the `error-handling` skill (never blindly re-fire a billed generation).

## Notes

- Always write prompts in **English** regardless of the user's language (Doubao models also handle Chinese well if the user insists).
- The structured checklist is authoring discipline, not a payload — the tool consumes one `prompt` string.
- Pass **material ids** for references, never URLs or local paths.
- Iterative refinement is normal; tune the prompt, swap references, or raise `quality_tier` for the final pass.
