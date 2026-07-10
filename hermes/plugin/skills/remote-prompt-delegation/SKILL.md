---
name: remote-prompt-delegation
description: Delegate prompts to trusted Caduceus LAN workers.
version: 0.2.0
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [caduceus, mcp, delegation, local-first]
    category: mcp
---

# Caduceus Remote Prompt Delegation

## When to use

Use Caduceus when the user wants to delegate a prompt to a trusted worker on
their local network. The Hermes MCP server key must be `caduceus`; the tool
names below depend on that key.

## Procedure

1. Call `mcp__caduceus__caduceus_list_workers`.
2. Select a worker only when `allowed` is `true`, `capabilities.llm` is `true`,
   and its `trust_level` satisfies the request. Use `auto` only after the user
   accepts the local trust assumptions.
3. Construct a `run_remote_task` request and call
   `mcp__caduceus__caduceus_validate_task` with that exact request.
4. Continue only when validation returns both `ok: true` and
   `data.valid: true`, then pass the same request to
   `mcp__caduceus__caduceus_run_remote_task`.
5. `run_remote_task` waits for a terminal result. After it returns, use
   `mcp__caduceus__caduceus_get_task_events` and
   `mcp__caduceus__caduceus_get_task_result` for audit or recovery when needed.
6. For any task that is still active, call
   `mcp__caduceus__caduceus_cancel_task` if the user asks to stop.

## Safety

- Never send secrets, credentials, private source code, or sensitive personal
  data unless the user explicitly asks and the selected worker is trusted.
- Do not treat a matching group hash as worker identity.
- Do not execute a task when validation fails.

## Verification

Report the selected worker ID, task ID, terminal task status, and whether the
result came from the requested worker.
