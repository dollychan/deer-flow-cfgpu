---
name: extract-user
description: Extract persistent user knowledge from a conversation transcript
output_format: json
---

# User Knowledge Extraction

You are analyzing a conversation transcript to extract **persistent, reusable knowledge about the user**.

## What to extract

Extract facts that are stable preferences, traits, or knowledge the user has demonstrated — things that would be useful to remember for future conversations.

**Good candidates:**
- Communication style preferences (e.g. prefers concise replies, likes code examples)
- Domain expertise level (e.g. expert in Python, new to React)
- Recurring goals or constraints (e.g. always optimizing for latency, targets Python 3.11+)
- Workflow preferences (e.g. prefers TDD, likes step-by-step breakdowns)
- Personal context that affects work (e.g. working in a startup, has deadline pressure)

**Do not extract:**
- One-off task details specific to this conversation
- Information about the project (use project extraction for that)
- Anything speculative or not clearly stated

## Scope keys

Each extracted group gets a `scope_key`:
- `""` — general user traits, independent of any agent
- `"agent:{name}"` — user preferences specific to a particular agent (only if clearly scoped)

## Output format

Return a JSON array. Each element corresponds to one scope group:

```json
[
  {
    "scope_key": "",
    "facts": [
      {"content": "<concise fact statement>", "category": "<preference|knowledge|context|behavior|goal>", "confidence": <0.0-1.0>},
      ...
    ],
    "summary": "<1-2 sentence narrative summarizing this scope group>"
  }
]
```

`confidence` is your certainty that this fact is a stable, reusable trait worth remembering across future conversations (not just true for this one task). Use the scale:
- `0.9-1.0` — explicitly and repeatedly stated by the user (e.g. "always reply in Chinese")
- `0.7-0.9` — clearly demonstrated once, unlikely to change
- `0.6-0.7` — reasonable inference from behavior
- below `0.6` — speculative; prefer not to emit it at all

Facts below the system's confidence threshold are discarded on write, so do not pad the list with low-confidence guesses.

Return `[]` if no persistent user knowledge was found in this conversation.

## Existing facts (for deduplication)

{existing_facts_json}

Do not re-emit facts that are already captured in existing facts unless you have a meaningful update to the content.

## Conversation transcript

{conversation}

---

Return only the JSON array. No explanation, no markdown fences.
