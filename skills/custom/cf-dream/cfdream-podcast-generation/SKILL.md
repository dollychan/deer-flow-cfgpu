---
name: cfdream-podcast-generation
description: Use this skill when the user requests to generate, create, or produce a podcast from text content. Converts written content into a two-host conversational podcast by writing a dialogue script, synthesizing each line with the cfdream MCP generate_audio tool (two distinct voices), and assembling the clips into a final MP3 with ffmpeg.
---

# Podcast Generation Skill (cfdream MCP)

## Overview

Generate a two-host conversational podcast from text content. The cfdream MCP exposes **per-utterance text-to-speech** (`generate_audio`) — there is no single podcast tool — so the pipeline is: write a dialogue script → synthesize each line with the matching host voice → assemble the clips into one MP3 with ffmpeg → deliver the MP3 + a transcript. Workflow conventions (materials, delivery, errors) are owned by your SOUL and the `error-handling` skill; for fine voice control see the `seed-tts` (Chinese) and `minimax-speech` (multilingual) skills.

## Workflow

### Step 1 — Understand requirements

- **Source content** — the text/article/report to convert
- **Language** — English or Chinese (drives voice choice)
- You do **not** need to inspect `/mnt/user-data` folders first.

### Step 2 — Write the dialogue script

Author a structured script and save it to `/mnt/user-data/workspace/{name}-script.json` (this is your **working plan** for synthesis and the transcript — it is *not* a tool input; `generate_audio` takes plain text per line):

```json
{
  "title": "The History of Artificial Intelligence",
  "locale": "en",
  "lines": [
    {"speaker": "male", "paragraph": "Hello Deer! Welcome back to another fascinating episode. Today we're diving into the history of artificial intelligence."},
    {"speaker": "female", "paragraph": "Oh, I love this topic! AI feels so modern, but it actually has roots going back over seventy years."},
    {"speaker": "male", "paragraph": "Exactly! The term was coined by John McCarthy in 1956 at the Dartmouth conference."}
  ]
}
```

Fields: `title` (optional, used as transcript heading), `locale` (`en`/`zh`), `lines[]` with `speaker` (`male`/`female`) and `paragraph` (the spoken text).

**Script-writing guidelines** (keep these — they make the audio listenable):
- *Format*: only two hosts, male and female, alternating naturally; ~10 minutes (≈40–60 lines); **start with the male host greeting that includes "Hello Deer"**.
- *Tone*: natural, conversational — two friends chatting; casual transitions, reactions, follow-up questions; avoid academic/formal tone.
- *Content*: frequent back-and-forth; short, easy-to-speak sentences; plain text only (no markdown/formulas/code); translate technical concepts into accessible language; exclude meta info (dates, author names, document structure).

### Step 3 — Pick the two host voices

Choose **one male and one female voice** and keep them fixed for the whole episode. Pass them via the `voice` parameter of `generate_audio`:

- **English / multilingual** → MiniMax presenter voices read well as hosts: `voice="presenter_male"` and `voice="presenter_female"` (model `minimax-speech-2-8-hd` or `-turbo`). See `minimax-speech` for the full 327-voice list.
- **Chinese** → either the MiniMax presenters above, or Doubao `seed-tts-2-0` voices (female default `zh_female_xiaohe_uranus_bigtts`; pick a 通用/视频配音 male voice from `get_model_card("seed-tts-2-0")`). See `seed-tts`.
- Or leave `model="auto"` and just set the two `voice` ids — but confirm both ids exist (call `get_model_card`/`list_models(task_type="audio")`); never invent a voice id.

### Step 4 — Synthesize each line

For every line in order, call `generate_audio` with that line's text and the matching host voice. Keep `model`, `audio_format`, and `sample_rate` identical across all lines so the clips concatenate cleanly:

```json
generate_audio({
  "text": "Hello Deer! Welcome back to another fascinating episode. Today we're diving into the history of artificial intelligence.",
  "model": "minimax-speech-2-8-hd",
  "voice": "presenter_male",
  "audio_format": "mp3",
  "sample_rate": 32000
})
```

Each call returns an audio **material id** (auto-streamed to the user). MiniMax is synchronous; `seed-tts` is async (`wait=true` still returns in one call). To run lines concurrently, use `wait=false` and collect with `task_wait` — but mind ordering when you assemble.

> Tool names may be host-namespaced (e.g. `mcp__cfdream__generate_audio` / `cfdream_generate_audio`). Parameters are identical.

### Step 5 — Assemble the final MP3 (ffmpeg)

`localize_material(id)` each line's audio into the sandbox in script order, then concatenate with ffmpeg into one MP3 under `/mnt/user-data/outputs/`:

```bash
# After localizing each clip to /mnt/user-data/workspace/line01.mp3, line02.mp3, ...
printf "file 'line01.mp3'\nfile 'line02.mp3'\nfile 'line03.mp3'\n" > /mnt/user-data/workspace/podcast_list.txt
ffmpeg -f concat -safe 0 -i /mnt/user-data/workspace/podcast_list.txt -c copy /mnt/user-data/outputs/ai-history-podcast.mp3
```

(If `-c copy` fails on mixed parameters, re-encode: `-c:a libmp3lame -q:a 2`.)

### Step 6 — Write the transcript and deliver

Write a readable transcript from the script JSON to `/mnt/user-data/outputs/{name}-transcript.md` (title heading + speaker-labeled lines), then `present_files` the **final MP3 and the transcript** together. Provide a brief description (topic, duration, hosts). Offer to regenerate if adjustments are needed.

## Output format

The podcast follows the "Hello Deer" format: two hosts (one male, one female), natural alternating dialogue, opening with a "Hello Deer" greeting, ~10 minutes.

## Notes

- Match the script language to the content (`en`/`zh`); write spoken text plainly — no markdown, formulas, or code in lines.
- Keep the **same two voices and the same audio format/sample rate** across every line so the clips concatenate cleanly.
- Per-line audio clips are auto-shown as they generate; the **assembled MP3 is the real deliverable** — `present_files` it (with the transcript) at the end.
- Billing is per character — long content means many TTS calls. On error, surface the error `message` and follow the `error-handling` skill (never blindly re-fire a billed call; poll an existing `task_id` after a timeout).
- Pass **material ids** to `localize_material`/`present_files`, never URLs.
