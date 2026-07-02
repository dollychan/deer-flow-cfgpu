---
name: minimax-speech
description: Understand what a user wants from text-to-speech and match it to the right MiniMax speech 2.8 model (HD / Turbo), the right voice, and voice-control values (speed / volume / pitch / emotion / format) that fit their intent. Use when the user wants MiniMax TTS, English/multilingual narration, or fine-grained voice tuning.
---

# Choosing a MiniMax speech model and voice settings

The job of this skill is to read the user's request, figure out what they actually want to hear, and translate that into: **which MiniMax model**, **which voice**, and **which voice-control values**. Focus on intent — not on how the audio is technically produced.

## Step 1 — Pick the model

| Model | Best for | Cost / speed |
|---|---|---|
| `MiniMax/speech-2.8-hd` | Highest fidelity, emotional nuance, custom pronunciation | cost 2/5 · speed 3/5 |
| `MiniMax/speech-2.8-turbo` | Faster + cheaper, same voices and controls | cost 1/5 · speed 4/5 |

How to decide, based on intent:

- Wants **best quality** or emotional nuance → `MiniMax/speech-2.8-hd`
- Cares about **cost or latency**, or is generating bulk narration → `MiniMax/speech-2.8-turbo`
- No preference → `MiniMax/speech-2.8-hd`

> For Chinese-only character/role voices, consider the **seed-tts** skill (Doubao seed-tts-2.0) instead. MiniMax is the better fit for English/multilingual and fine voice tuning.

## Step 2 — Choose a voice

MiniMax offers **327 system voices** across 中文 / 粤语 / English / 日本語 / 한국어 / Español / Português / Français / Deutsch / Русский and ~14 more languages; HD and Turbo share the same list. Match the voice to the user's described speaker — language, gender, age, and role.

A few common ones: `male-qn-qingse` (青涩青年, the default), `female-shaonv` (少女), `presenter_male` / `presenter_female` (主持人), `audiobook_male_1` / `audiobook_female_1` (有声书).

Pick the voice that best fits the intent; if the user's described voice isn't available, fall back to a sensible default and tell them rather than inventing one.

## Step 3 — Set voice controls to match intent

| Control | Default | How to choose from intent |
|---|---|---|
| Speed | 1.0 | Faster (~1.1–2.0) for energetic/urgent delivery, slower (~0.5–0.9) for calm/serious. |
| Volume | 1.0 | Raise or lower for loudness. |
| Pitch | 0 | Negative for a deeper voice, positive for higher. |
| Emotion | inferred | `happy` / `sad` / `angry` / `fearful` / `disgusted` / `surprised` / `neutral` — set it when the user names a mood; otherwise let the model infer from the text. |
| Audio format | mp3 | `mp3` general, `wav`/`flac` for higher quality, `pcm` for raw. |
| Sample rate | 32000 Hz | Higher for better fidelity, lower to save size. |
| Pronunciation | — | If the user needs a specific pronunciation of a term, note it so it can be enforced. |

## Step 4 — Confirm the choice back to the user

Before synthesizing, briefly state the model, voice, and key controls you chose and why, so the user can correct any misread intent. If the desired voice or language is unclear, ask one focused question rather than guessing. Note that cost scales with text length, and Turbo is roughly 1.75× cheaper than HD.
