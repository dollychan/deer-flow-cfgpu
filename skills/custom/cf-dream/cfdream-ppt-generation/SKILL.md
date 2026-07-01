---
name: cfdream-ppt-generation
description: Use this skill when the user requests to generate, create, or make a presentation / slide deck (PPT/PPTX). Builds a visually consistent deck by generating one image per slide with the cfdream MCP generate_image tool (each slide references the previous one for style continuity), then composing the slides into a PPTX file with a local python-pptx script.
---

# PPT Generation Skill (cfdream MCP)

## Overview

Generate a professional slide deck by producing **one AI image per slide** with the cfdream MCP `generate_image` tool, then composing those images into a `.pptx` with a local `python-pptx` script. The cfdream MCP has **no slide/PPT tool** — so the pipeline is: plan the deck → generate slide images sequentially (each referencing the previous slide's **material id** for visual consistency) → localize the slide materials into the sandbox → compose the PPTX locally → `present_files` the deck.

Workflow conventions (materials, delivery, approval, async, errors) are owned by your SOUL and the `error-handling` skill; for image prompt craft and the `generate_image` schema, defer to `cfdream-image-generation`. This skill only owns the **deck-specific orchestration**.

> **Why this skill exists (do not shell out to a raw image API).** Slide images **must** be produced with `generate_image` so they flow through the cfdream materials system (auto-registered as ids, auto-streamed, billed through the per-task cfgpu token). Do **not** call the public `image-generation/scripts/generate.py` — it hits an external MiniMax/Gemini endpoint with separate keys, bypasses cfgpu billing, and writes raw local files that never enter the `<materials>` ledger.

## Workflow

### Step 1 — Understand requirements

Identify before planning:

- **Topic / subject** — what the deck is about
- **Slide count** — how many slides (default 5–10)
- **Style** — pick one from the Style Catalog below (glassmorphism / dark-premium / gradient-modern / neo-brutalist / 3d-isometric / editorial / minimal-swiss / keynote)
- **Aspect ratio** — `16:9` (standard) or `4:3` (classic)
- **Content outline** — the key points per slide

You do **not** need to inspect `/mnt/user-data` folders first.

### Step 2 — Create the presentation plan

Write the deck structure to `/mnt/user-data/workspace/{name}-plan.json`. This is your **working plan** and the composer's input — it defines the shared visual style so every slide reads as one deck. **Include the `style` and `style_guidelines` fields** (copy the chosen entry from the Style Catalog).

```json
{
  "title": "Introducing Nova AI",
  "style": "keynote",
  "style_guidelines": {
    "color_palette": "Deep black backgrounds, white text, single blue accent (#0071e3)",
    "typography": "Bold SF Pro Display headlines, clean body, dramatic size contrast",
    "imagery": "Cinematic photography, full-bleed images, shallow depth of field",
    "layout": "Generous whitespace, single focal point per slide, no clutter"
  },
  "aspect_ratio": "16:9",
  "slides": [
    {"slide_number": 1, "type": "title", "title": "Introducing Nova AI", "subtitle": "Intelligence, Reimagined", "visual_description": "Detailed description for image generation"},
    {"slide_number": 2, "type": "content", "title": "Why Nova?", "key_points": ["10x faster", "Human-like understanding", "Enterprise security"], "visual_description": "Detailed description for image generation"}
  ]
}
```

The composer reads `aspect_ratio` (`16:9` → 13.333×7.5 in, `4:3` → 10×7.5 in) and writes each slide's `title` / `subtitle` / `key_points` into the slide's **speaker notes**. `visual_description` is your source material for the image prompt — it is **not** consumed by the composer.

Lock the deck's **Production Spec** here (per your SOUL): identical `aspect_ratio`, a fixed `resolution`, and one consistent `style` across every slide.

### Step 3 — Generate slide images sequentially (the consistency rule)

**Generate slides strictly one at a time, in order (1 → 2 → 3 …). Never parallelize or batch.** Each slide (from slide 2 on) uses the **previous slide's material id** as a `reference_images` anchor — that reference chain is what keeps the palette, typography, and treatment consistent across the deck. Parallel generation breaks continuity.

Defer to `cfdream-image-generation` for prompt craft. Deck-specific rules:

**Slide 1 (establishes the visual language)** — compose a prompt from the plan's `style_guidelines` + slide 1's `visual_description`; no reference image:

```json
generate_image({
  "prompt": "Professional presentation title slide. <style_guidelines: color palette, typography, imagery, layout, effects>. Title: 'Introducing Nova AI', subtitle 'Intelligence, Reimagined'. <slide-1 visual_description>. Clean text hierarchy, generous negative space. This slide establishes the visual language for the entire deck.",
  "model": "auto",
  "aspect_ratio": "16:9",
  "resolution": "2K",
  "quality_tier": "balanced"
})
```

This returns a **material id** (e.g. `m1`), auto-streamed to the user.

**Slide 2+ (reference the previous slide by id)** — pass the prior slide's material id via `reference_images` and command exact style continuity:

```json
generate_image({
  "prompt": "Presentation slide continuing the EXACT visual style of reference 1 — SAME background/gradient, SAME glass/material treatment, SAME typography and color palette. Title: 'Why Nova?'; three key points as subtle badges. <slide-2 visual_description>. CRITICAL: must look like it belongs in the same deck as reference 1.",
  "model": "auto",
  "reference_images": ["m1"],
  "aspect_ratio": "16:9",
  "resolution": "2K",
  "quality_tier": "balanced"
})
```

Continue: slide 3 references slide 2's id, slide 4 references slide 3's id, and so on. Hold the **same `aspect_ratio`, `resolution`, and `model`** across every call. Keep `quality_tier="balanced"` while iterating; only re-run at `best` for a final polish pass the user approved.

> Tool names may be host-namespaced (`mcp__cfdream__generate_image` / `cfdream_generate_image`) — parameters are identical.

### Step 4 — Localize the slide images into the sandbox

The composer is a local script and needs real image files. `localize_material(id)` each slide's material **in slide order** to a predictable local path:

```
localize_material("m1")  ->  save/copy to /mnt/user-data/outputs/slide-01.jpg
localize_material("m2")  ->  /mnt/user-data/outputs/slide-02.jpg
...
```

`localize_material` returns a `/mnt/user-data/...` path; use it directly, or place the files at `slide-NN.jpg` so the ordering is unambiguous when you pass them to the composer.

### Step 5 — Compose the PPTX (local script)

Call the bundled composer with the plan file and the localized slide images **in order**:

```bash
python /mnt/skills/custom/cf-dream/cfdream-ppt-generation/scripts/generate.py \
  --plan-file /mnt/user-data/workspace/nova-plan.json \
  --slide-images /mnt/user-data/outputs/slide-01.jpg /mnt/user-data/outputs/slide-02.jpg /mnt/user-data/outputs/slide-03.jpg \
  --output-file /mnt/user-data/outputs/nova-presentation.pptx
```

Parameters:
- `--plan-file` — absolute path to the plan JSON (required)
- `--slide-images` — absolute paths to slide images in order, space-separated (required)
- `--output-file` — absolute path to the output `.pptx` (required)

The script (`python-pptx` + `Pillow`, both pure-local, no network/API keys) fits each image to the slide preserving aspect ratio and adds the plan's title/subtitle/key-points as speaker notes. Do **not** read the python file — just call it. If it reports `ModuleNotFoundError`, the sandbox lacks `python-pptx`/`Pillow`; report that rather than improvising.

### Step 6 — Deliver

`present_files` the final `.pptx` (this is the real deliverable; individual slide images were already auto-shown as they generated). Give a brief description (title, slide count, style) and offer to regenerate specific slides. To fix one slide, re-generate just that slide (referencing its neighbour for continuity), re-localize it, and re-run Step 5 — never re-generate the whole deck.

## Style Catalog

Pick one and copy its `style_guidelines` into the plan. Be specific in prompts — exact hex codes, font weights, and effect values beat vague adjectives.

| Style | Palette / Feel | Best for |
|---|---|---|
| **glassmorphism** | Vibrant gradients, frosted translucent panels, backdrop blur, visionOS depth | Tech/AI/SaaS launches, futuristic pitches |
| **dark-premium** | Rich black (#0a0a0a), luminous accent, subtle glow, luxury restraint | Premium brands, executive decks |
| **gradient-modern** | Bold mesh gradients (Stripe/Linear), oversized type, energetic | Startups, creative agencies, launches |
| **neo-brutalist** | High-contrast primaries, ultra-bold uppercase, hard shadows, anti-design | Edgy/Gen-Z brands, disruptive pitches |
| **3d-isometric** | Soft clay-render isometric illustrations, muted palette, friendly | Tech explainers, product features |
| **editorial** | Magazine layouts, serif headlines, dramatic photography, Vogue/Bloomberg | Reports, luxury, thought leadership |
| **minimal-swiss** | Grid precision, Helvetica-style type, bold negative space | Architecture, design firms, consulting |
| **keynote** | Apple aesthetic, cinematic imagery, extreme weight contrast, high drama | Keynotes, product reveals, inspirational talks |

Example `style_guidelines` (glassmorphism):
```json
{
  "color_palette": "Vibrant purple-to-cyan gradient (#667eea→#00d4ff), frosted white panels ~20% opacity, electric accents",
  "typography": "SF Pro Display / Inter, bold 600–700 white titles with subtle drop shadow, clean 400 body",
  "imagery": "Abstract 3D glass spheres, floating translucent geometric shapes, soft luminous orbs, layered transparency",
  "layout": "Centered frosted cards, 32px rounded corners, 48–64px padding, layered depth with soft shadows",
  "effects": "Backdrop blur 20–40px, 1px white border (rgba 255,255,255,0.25), soft color-tinted shadows, light refraction"
}
```
(Keynote / dark-premium / editorial / etc. follow the same shape — palette, typography, imagery, layout, effects, visual_language. Fill each from the row above.)

## Notes

- **Slide images go through `generate_image`, never a raw external image API** — that's the whole point of the cfdream variant (materials, auto-stream, cfgpu billing).
- **Sequential generation with reference chaining is mandatory** — slide 1 sets the language; every later slide references the previous slide's **material id**. Generating in parallel breaks visual consistency.
- Always write image **prompts in English** (best model adherence), regardless of the user's language.
- Hold the locked Production Spec — identical `aspect_ratio`, consistent `resolution`, one `style` — across all slides; use `quality_tier="balanced"` to iterate, `best` only for an approved final pass (cost discipline).
- Pass **material ids** to `reference_images` / `localize_material`; never raw URLs or object keys.
- The **PPTX is the deliverable** — `present_files` it at the end. On any generation error, surface the error `message` and follow the `error-handling` skill (never blindly re-fire a billed generation).
