---
name: extract-project
description: Extract persistent project knowledge from a conversation transcript
output_format: json
---

# Project Knowledge Extraction

You are analyzing a conversation transcript to extract **persistent, reusable knowledge about the project**.

## What to extract

Extract facts that are stable characteristics of the project — things that would be useful to remember in future conversations about the same project.

**Good candidates:**
- Project goals, deliverables, or success criteria
- Technical constraints or architectural decisions
- Stack, tools, or libraries the project uses
- Team structure or roles (if mentioned)
- Non-obvious domain facts or business rules
- Performance, quality, or compliance requirements

**Do not extract:**
- Transient task status from this conversation
- User-specific preferences (use user extraction for that)
- Anything speculative or not clearly stated

## Scope keys

Each extracted group gets a `scope_key`:
- `""` — general project facts, independent of any agent or user
- `"agent:{name}"` — agent-specific knowledge about this project (only if clearly scoped)
- `"user:{uid}"` — a user's role or responsibilities within this project (only if clearly scoped)

When `user:{uid}` scope is used, set the `uid` to the user identifier provided in the context.

## Output format

Return a JSON array. Each element corresponds to one scope group:

```json
[
  {
    "scope_key": "",
    "facts": [
      {"content": "<concise fact statement>", "category": "<goal|constraint|technical|domain|team|compliance>"},
      ...
    ],
    "summary": "<1-2 sentence narrative summarizing this scope group>"
  }
]
```

Return `[]` if no persistent project knowledge was found in this conversation.

## Context

- Project ID: {project_id}
- User ID: {user_id}
- Agent: {agent_name}

## Existing facts (for deduplication)

{existing_facts_json}

Do not re-emit facts already captured in existing facts unless you have a meaningful update.

## Conversation transcript

{conversation}

---

Return only the JSON array. No explanation, no markdown fences.
