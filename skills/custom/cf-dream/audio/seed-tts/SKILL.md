---
name: seed-tts
description: Understand what a user wants from text-to-speech and match it to Doubao 语音合成 2.0 (seed-tts-2.0) — choosing the right expressive Chinese system voice (角色扮演 / 有声阅读 / 客服 / 视频配音) and output settings that fit their intent. Use when the user wants Doubao/豆包 TTS, Chinese character/role voices, or audiobook narration.
---

# Choosing a seed-tts voice and settings

The job of this skill is to read the user's request, figure out what they actually want to hear, and translate that into: **the right voice** and **the right output settings**. Focus on intent — not on how the audio is technically produced.

## Step 1 — Confirm this is the right model

There is one Doubao TTS model: `seed-tts-2.0`. It offers **122 expressive Chinese system voices** with emotion and ASMR support.

> For English/multilingual narration or the finest pitch/speed/emotion control, consider the **minimax-speech** skill instead. seed-tts is the better fit for Chinese character/role voices and audiobook narration.

## Step 2 — Choose a voice

seed-tts offers **122 system speakers** organized by scene: 通用场景 / 角色扮演 / 视频配音 / 有声阅读 / 客服场景 / 教育 / 多语种. Match the voice to the user's described speaker and use case.

The naming convention helps narrow it down:
- `zh_female_*` / `zh_male_*` `_uranus_bigtts` — Chinese voices with 情感变化、指令遵循、ASMR
- `en_male_*` / `en_female_*` `_uranus_bigtts` — 美式英语 (e.g. `en_male_tim_uranus_bigtts`)
- `saturn_*_tob` — 角色扮演 voices with 指令遵循 / COT·QA ability
- `saturn_*_cs_tob` — 客服 voices

A few common ones: `zh_female_xiaohe_uranus_bigtts` (小何, the default), `zh_male_sunwukong_uranus_bigtts` (猴哥), `zh_male_qingcang_uranus_bigtts` (擎苍·有声阅读), `zh_female_xiaoxue_uranus_bigtts` (儿童绘本).

Pick the speaker that best fits the intent; if the user's described voice isn't available, fall back to the default `zh_female_xiaohe_uranus_bigtts` and tell them rather than inventing one.

## Step 3 — Set output to match intent

| Setting | Default | How to choose from intent |
|---|---|---|
| Audio format | mp3 | `mp3` general, `wav`/`flac` for higher quality, `pcm` for raw. |
| Sample rate | 24000 Hz | Higher for better fidelity, lower to save size. |

> seed-tts has no separate speed / volume / pitch / emotion controls. To shape delivery, **encode it in the text** and pick an expressive speaker — many Doubao voices respond to emotional and ASMR cues written into the prompt.

## Step 4 — Confirm the choice back to the user

Before synthesizing, briefly state the voice and settings you chose and why, so the user can correct any misread intent. If the desired voice or tone is unclear, ask one focused question rather than guessing. Note that cost scales with text length.
