---
name: cfdream-image-generation
description: Use this skill when the user requests to generate, create, imagine, or visualize images — characters, scenes, products, comics, or any visual content — by calling the cfdream MCP generate_image tool. Covers prompt craft, reference images, image groups (组图), and aspect ratio / resolution choices.
---

# Image Generation Skill (cfdream MCP)

## Overview

Generate high-quality images by calling the cfdream MCP `generate_image` tool. This skill is the **craft layer** — how to author a strong image prompt and which schema parameters to set. Workflow conventions (materials, delivery, approval, async, errors) are owned by your SOUL and the `error-handling` skill; follow those, don't restate them.

> `generate_image` takes a **single text `prompt` string** plus typed parameters — there is **no JSON prompt file**. Compose the structured thinking below into one rich English description, and map everything else (references, ratio, resolution) onto the tool's schema fields.

## Step 1 — Understand the request

Identify before composing:

- **Subject / content** — what is in the image (character, scene, product, comic panel …)
- **Style** — art style, mood, color palette, rendering (photographic, illustration, 3D …)
- **Composition & technical** — framing, lighting, level of detail, aspect ratio
- **References** — any material ids (uploads / prior generations / search hits) that should guide identity, style, or composition

You do **not** need to inspect `/mnt/user-data` folders first.

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
