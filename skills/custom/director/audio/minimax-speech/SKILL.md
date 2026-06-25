---
name: minimax-speech
description: Synthesize speech from text with the MiniMax speech 2.8 models (HD / Turbo) by calling the cfgpu MCP generate_audio tool — synchronous text-to-speech with fine voice control (327 system voices, speed/volume/pitch/emotion, pronunciation dictionary). Use when the user wants MiniMax TTS, English/multilingual narration, or fine-grained voice tuning.
---

# Synthesizing speech with MiniMax (MCP)

This skill is for an agent that calls the **cfgpu MCP server** tools. The relevant tools:

- `generate_audio(...)` — synthesize speech (the main one)
- `list_models(task_type="audio")` — enumerate voice models
- `get_model_card(model_name)` — fetch a model's full parameter/usage doc, including the **full system voice list (327 voice_ids)**

> Tool names may be namespaced by the host (e.g. `mcp__cfgpu__generate_audio`). Use whatever prefix your environment exposes; the parameters below are identical.

MiniMax speech is **synchronous** — the audio URL comes back in the same `generate_audio` call. Leave `wait=true` (the default); there is no task to poll.

## Step 1 — Pick the model (`model` parameter)

| `model` (adapter_id) | Best for | 价格 | cost / speed |
|---|---|---|---|
| `minimax-speech-2-8-hd` | Highest fidelity, emotion, pronunciation dict | 0.3675 元/万字符 | cost 2/5 · speed 3/5 |
| `minimax-speech-2-8-turbo` | Faster + cheaper, same voices/controls | 0.21 元/万字符 | cost 1/5 · speed 4/5 |

Decision guide:
- Best quality / emotional nuance → `minimax-speech-2-8-hd`
- Lowest cost & latency, bulk narration → `minimax-speech-2-8-turbo`
- Let the router restrict to MiniMax only → `model=["minimax-speech-2-8-hd", "minimax-speech-2-8-turbo"]`
- Don't care which provider → `model="auto"`

> For Chinese-only character/role voices or async submission, consider the **seed-tts** skill (Doubao seed-tts-2.0) instead.

## Step 2 — Choose a voice (`voice` parameter)

`voice` maps to MiniMax `voice_setting.voice_id`. Default (when omitted) is `male-qn-qingse`.

The model card lists **all 327 system voices** across 中文/粤语/English/日本語/한국어/Español/Português/Français/Deutsch/Русский and ~14 more languages. **Always call `get_model_card("minimax-speech-2-8-hd")` to look up an exact `voice_id`** before promising a specific voice. Both HD and Turbo share the same voice list.

A few common ids: `male-qn-qingse` (青涩青年), `female-shaonv` (少女), `presenter_male` / `presenter_female` (主持人), `audiobook_male_1` / `audiobook_female_1` (有声书).

## Step 3 — Tune the voice (all MiniMax-only)

| Parameter | Default | Range / notes |
|---|---|---|
| `speed` | `1.0` | speech rate multiplier (~0.5–2.0) |
| `volume` | `1.0` | volume multiplier |
| `pitch` | `0` | pitch offset (negative = lower) |
| `emotion` | none | `happy` / `sad` / `angry` / `fearful` / `disgusted` / `surprised` / `neutral` — omit to let the model infer from text |
| `audio_format` | `mp3` | `mp3` / `wav` / `pcm` / `flac` |
| `sample_rate` | `32000` | Hz, e.g. `16000` / `24000` / `32000` |
| `bitrate` | `128000` | bps (MiniMax only) |
| `model_specific` | — | raw API extras merged last, e.g. a `pronunciation_dict` to override pronunciations, or `subtitle_enable` |

## Step 4 — Call the tool

### Basic
```json
generate_audio({
  "text": "明朝开国皇帝朱元璋也称这本书为，万物之根。",
  "model": "minimax-speech-2-8-hd"
})
```

### Specific voice + emotion + faster, on Turbo
```json
generate_audio({
  "text": "Welcome aboard! We are about to begin our journey.",
  "model": "minimax-speech-2-8-turbo",
  "voice": "presenter_female",
  "speed": 1.1,
  "emotion": "happy"
})
```

### High-quality WAV with custom pronunciation
```json
generate_audio({
  "text": "CFGPU 读作 C-F-G-P-U。",
  "model": "minimax-speech-2-8-hd",
  "voice": "audiobook_male_1",
  "audio_format": "wav",
  "sample_rate": 32000,
  "model_specific": { "pronunciation_dict": { "tone": ["处理/(chu3)(li3)"] } }
})
```

## Reading the result

`generate_audio` returns a normalized object:

```json
{
  "urls": ["https://.../speech.mp3"],     // the generated audio
  "expires_at": "2026-06-18T12:00:00Z",   // URL valid ~24h — download promptly
  "artifact": true,
  "payload": { ... },                      // the exact MiniMax API request that was sent
  "metadata": {                            // present when return_metadata=true
    "model_used": "MiniMax/speech-2.8-hd",
    "usage": { ... }                       // billing is per character (按字符计费)
  }
}
```

Give the user the `urls` value and warn that the link expires in ~24 hours. On error the tool returns an error dict instead — surface its `message`.

## Notes & troubleshooting

- Billing is **per character** (元/万字符), so cost scales with `text` length — Turbo is ~1.75× cheaper than HD.
- `speed` / `volume` / `pitch` / `emotion` are **MiniMax-only**; seed-tts ignores them.
- If a requested voice isn't in the card's 327-voice list, fall back to the default and tell the user, rather than inventing an id.
- `content_blocked` → rewrite sensitive text. `invalid_params` → check format/sample_rate/voice_id against the card. `quota_exceeded` → top up the account.
