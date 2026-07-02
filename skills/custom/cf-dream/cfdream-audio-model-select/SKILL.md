---
name: cfdream-audio-model-select
description: Use this skill to pick the best cfdream text-to-speech model AND voice (音色) before calling the audio generation tool. Compares the three enabled TTS models — MiniMax speech 2.8 HD / Turbo and Doubao seed-tts 2.0 — by language coverage, voice catalog, controls, cost, and speed, then helps navigate their large voice lists to match the user's intended speaker. Returns a recommended model + voice (with alternatives) for the HIL user to confirm or swap. Invoke whenever you must decide "which TTS model and which voice?" for narration, dialogue, podcast, character/role voices, audiobook, announcements, or customer-service audio.
---

# Choosing a cfdream TTS model and voice

This skill runs the **model + voice selection** for text-to-speech: given the user's intent, it filters the three TTS models by **language + controls → cost/speed**, then helps you pick a concrete **voice id** from each model's large catalog. It returns a recommended `model` + `voice` (plus alternatives) to feed the generation call.

- For deep per-model voice guidance and settings, hand off to the family skill after selecting: `audio/minimax-speech` (MiniMax HD/Turbo) or `audio/seed-tts` (Doubao). Those hold the fuller voice lists.
- The full authoritative voice catalog is large (hundreds of ids). This file gives the **decision procedure + representative anchors** — for the complete list of a model's voices, read its family skill or call `get_model_card(<id>)`.
- Verify live availability with `list_models(task_type="audio")` before committing on anything cost-sensitive.
- **All model identifiers below are `model_id` (cfgpu model id), not adapter id.** The `generate_audio(model=…)` parameter accepts both, but `model_id` is the canonical identifier used by the cfgpu API internally. To cross-reference with the adapter id (e.g. `minimax-speech-2-8-hd`), use `list_models` or consult the adapter registry.

**Tier legend:** `cost` 1 = cheapest … 5 = most expensive. `speed` 1 = slowest … 5 = fastest.

---

## Step 1 — Read the intent

Before picking anything, extract:

- **Language / dialect** — the decisive filter. Mandarin, Cantonese, a Chinese dialect (四川/陕西/东北), English, Japanese, Korean, Spanish, Portuguese, French, or one of ~20 others?
- **Speaker persona** — gender, age, and role/character (narrator, news anchor, young man, 御姐, child, 霸道少爷, a named IP character…), plus tone/emotion.
- **Use case** — audiobook / 有声阅读, podcast, news / announcement, character role-play, customer service, children's content, video dubbing / 视频配音, ASMR.
- **Control needs** — does the user want explicit speed / pitch / volume / emotion tuning, or custom pronunciation? (This gates the model.)
- **Priority & shape** — quality vs cost vs speed; **one voice or several** (a dialogue / podcast needs a distinct voice per speaker, kept consistent throughout).

If a decisive detail (language, or which persona) is genuinely unclear, ask **one** focused question rather than guessing.

## Step 2 — Pick the model (language + controls gate, then cost/speed)

| Model (model id) | Voices | Languages | Controls | Call | cost | speed | Price (per 万字符) |
|---|---|---|---|---|:--:|:--:|---|
| `MiniMax/speech-2.8-hd` | ~327 | 中文(普/粤) · English · 日 · 한 · Español · Português · Français · Deutsch · Русский · +~14 more | speed / volume / pitch / **emotion** / pronunciation_dict | **sync** | 2 | 3 | 0.3675 元 |
| `MiniMax/speech-2.8-turbo` | ~327 (same list) | same as HD | same as HD | **sync** | **1** | 4 | 0.21 元 |
| `seed-tts-2.0` | ~122 | **中文-focused** (Mandarin + 方言 via Vivi; a few 美式英语) | none (shape via text + expressive speaker) | **async** (poll) | 3 | 3 | **2.94 元** |

Decide in this order:

1. **Language gate.** Any non-Chinese or multilingual need (EN / JA / KO / ES / PT / FR / …) → **MiniMax** (20+ languages, 327 voices). `seed-tts-2.0` is Chinese-focused with only a handful of English voices.
2. **Controls gate.** Need explicit **speed / pitch / volume / emotion** tuning or **custom pronunciation** → **MiniMax** (`seed-tts-2.0` has none of those params).
3. **Chinese-specialist gate.** For rich **Chinese character/role voices**, **dialects** (四川/陕西/东北/粤), **famous IP characters** (孙悟空 / 唐僧 / 猪八戒 / 熊二 / 武则天 …), **有声阅读**, **ASMR**, or **COT/QA role-play** (the `saturn_*` voices) → **`seed-tts-2.0`** is the specialist.
4. **Then cost/speed.** Among MiniMax: **HD** for top fidelity and emotional nuance; **Turbo** for the cheapest, fastest bulk narration (same voices/controls). If a Chinese job fits both families, note that **seed-tts is ~8× the price of HD and ~14× Turbo** — reserve it for when its specialist voices are the reason to use it.
5. **Sync vs async.** MiniMax returns synchronously (immediate); `seed-tts-2.0` is asynchronous (submit → poll `get_task_status` / `wait_for_task`).

**Reach a concrete model + voice before you fire `generate_audio` — never leave it unresolved.** If the deciding information is missing — the **language/dialect** is unknown, or the intended **speaker persona** (gender/age/character) is unclear — `ask_clarification` and wait rather than defaulting silently (these two gate everything downstream). If the client restricted the models for this task, you'll see a manual-mode `<system-reminder>` listing the allowed audio model IDs; pick your model **from that list only** (then choose a voice it actually offers), and if none of the allowed models can serve the need, tell the user and ask instead of reaching outside it.

## Step 3 — Choose the voice (音色) — the heart of this skill

Each model exposes **many** voices. Don't scan the whole list — narrow with this procedure:

1. **Language / dialect** → drops to the right voice family.
2. **Gender + age** → male / female, adult / youth / child / elder.
3. **Persona / scene** → narrator, anchor, character, customer-service, audiobook, child, etc.
4. **Map to the naming convention** (below) → shortlist 1–3 candidate ids.
5. **Fallback and tell.** If the described voice doesn't exist, fall back to the model default and **say so** — never invent a voice id.

### MiniMax naming (≈327 voices)
- Chinese persona ids: `male-qn-qingse` (青涩青年, **default**), `male-qn-jingying` (精英青年), `male-qn-badao` (霸道青年), `female-shaonv` (少女), `female-yujie` (御姐), `female-chengshu` (成熟女性), `female-tianmei` (甜美), plus children `clever_boy` / `lovely_girl` and roles `junlang_nanyou` (俊朗男友), `badao_shaoye` (霸道少爷).
- Descriptive/professional ids embed language + persona: `Chinese (Mandarin)_News_Anchor`, `Chinese (Mandarin)_Radio_Host`, `Chinese (Mandarin)_Gentleman`, `Cantonese_ProfessionalHost（M)`.
- Other languages are prefixed: `English_Graceful_Lady`, `Japanese_KindLady`, `Korean_SweetGirl`, `Spanish_Narrator`, `Portuguese_Narrator`, `French_MaleNarrator`, festive `Santa_Claus` / `Grinch`.
- Best for: multilingual work and any voice you want to fine-tune with speed/pitch/emotion.

### seed-tts naming (≈122 voices)
- `zh_female_*` / `zh_male_*` `_uranus_bigtts` — Chinese voices with 情感变化 / 指令遵循 / ASMR. Default `zh_female_xiaohe_uranus_bigtts` (小何).
- `en_female_*` / `en_male_*` `_uranus_bigtts` — 美式英语 (`en_male_tim_uranus_bigtts`, `en_female_dacey_uranus_bigtts`).
- `saturn_zh_*_tob` — 角色扮演 voices with 指令遵循 / COT·QA.
- `saturn_zh_*_cs_tob` — 客服 voices.
- Organized by scene: 通用场景 / 角色扮演 / 视频配音 / 有声阅读 / 客服场景 / 教育场景 / 多语种. Many are 抖音/豆包/剪映同款 IP characters (`zh_male_sunwukong_uranus_bigtts` 猴哥, `zh_male_tangseng_uranus_bigtts` 唐僧, `zh_male_qingcang_uranus_bigtts` 擎苍·有声阅读, `zh_female_xiaoxue_uranus_bigtts` 儿童绘本).
- Best for: Chinese character/role, dialects, audiobook, ASMR, customer service.

> The lists above are **anchors, not the full catalog.** For the complete voice table, read the `audio/minimax-speech` or `audio/seed-tts` skill, or call `get_model_card(<id>)`.

### For dialogue / podcast
Assign a **distinct voice per speaker** and keep each speaker's voice **identical across the whole piece**. Prefer voices from the **same model** so timbre/loudness match. (The `cfdream-podcast-generation` skill owns the two-host pipeline.)

## Step 4 — Note the output settings (defer detail to the family skill)

- **MiniMax**: `speed` (1.0), `volume` (1.0), `pitch` (0), `emotion` (auto/`happy`/`sad`/`angry`…), `audio_format` (mp3), `sample_rate` (32000), optional `pronunciation_dict`. Set these to match delivery intent.
- **seed-tts**: only `audio_format` (mp3) and `sample_rate` (24000). No speed/pitch/volume/emotion params — **encode delivery in the text** and pick an expressive speaker.

## Step 5 — Return a recommendation (model + voice), for the HIL user to confirm or swap

Generation is Human-in-the-Loop — the user can change the model or voice at confirmation. So return, in order:

1. **Recommended** — the model + voice you'd default to, with a one-line reason (language fit, persona fit, cost/speed) and key settings.
2. **1–2 alternative voices** (and, if relevant, the other model) the user is likely to want instead — e.g. a same-persona different-timbre voice, or "Turbo instead of HD to cut cost."

Every option must actually exist and fit the language/persona; if an alternative changes what's possible (loses emotion control, switches to async, costs much more), say so. State the interpretation you acted on so the user can correct a misread. Then hand the script + exact schema mapping to the generation flow (and the family skill).

---

## Quick intent → shortlist

| Intent | Model | Voice starting point |
|---|---|---|
| English / other-language narration | `MiniMax/speech-2.8-hd` (or Turbo for bulk) | `English_*` / `Japanese_*` / `Korean_*` / `Spanish_*` … |
| Cheapest / bulk multilingual narration | `MiniMax/speech-2.8-turbo` | any MiniMax voice |
| Fine speed/pitch/emotion control | MiniMax (HD) | persona id + tuned controls |
| Chinese audiobook / 有声阅读 | `seed-tts-2.0` | `zh_male_qingcang_*`, `zh_male_baqiqingshu_*` |
| Chinese character / IP role | `seed-tts-2.0` | `zh_male_sunwukong_*`, `zh_male_tangseng_*`, `saturn_zh_*_tob` |
| Chinese customer service | `seed-tts-2.0` | `saturn_zh_*_cs_tob`, `zh_female_kefunvsheng_*` |
| Chinese children's content | `seed-tts-2.0` or MiniMax | `zh_female_xiaoxue_*` / `lovely_girl` |
| Chinese dialect (四川/粤语…) | `seed-tts-2.0` (Vivi) or MiniMax `Cantonese_*` | `zh_female_vv_uranus_bigtts` |
| Podcast (two hosts) | one model, two voices | pick two distinct same-model voices |

## Notes

- **Cost scales with text length** for all three; `seed-tts-2.0` is by far the priciest per character — don't reach for it unless its Chinese-specialist voices are the reason.
- MiniMax **HD and Turbo share the exact same voice list and controls** — switching between them never changes the available voices, only fidelity/cost/speed.
- Only MiniMax supports `emotion` and pronunciation control; for `seed-tts-2.0`, shape emotion through the **text** and an expressive `_uranus_bigtts` speaker.
- Never invent a voice id. If unsure a voice exists, confirm via the family skill or `get_model_card(<id>)`, and fall back to the model default (`male-qn-qingse` for MiniMax, `zh_female_xiaohe_uranus_bigtts` for `seed-tts-2.0`) if needed.
