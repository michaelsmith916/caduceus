# Hermes enrollment integration v1

Status: implemented in the Caduceus Hermes plugin 0.2.0
Transport: Caduceus MCP through Hermes `ctx.dispatch_tool`
Interaction model: explicitly invoked command

## Supported host boundary

The verified Hermes plugin API registers commands and skills and dispatches MCP
tools. It does not expose an unsolicited registration-event hook. Consequently,
the adapter does not run a background watcher and cannot interrupt a Hermes
conversation when a peer registers.

An operator or agent explicitly polls with:

```text
/caduceus-enrollments
/caduceus-enrollments list
```

The daemon is authoritative for request state and expiration. The plugin keeps
only an in-memory set of request IDs it has already presented, preventing the
same approval prompt from being repeated during one Hermes process lifetime.
Reloading the plugin clears that presentation cache; it does not alter daemon
state.

## MCP contract

Hermes server key `caduceus` maps the Caduceus tool names as follows:

| Operation | Caduceus MCP tool | Hermes dispatch name | Arguments |
| --- | --- | --- | --- |
| List | `caduceus.list_enrollment_requests` | `mcp__caduceus__caduceus_list_enrollment_requests` | `{}` |
| Approve | `caduceus.approve_enrollment` | `mcp__caduceus__caduceus_approve_enrollment` | `{"request_id":"<id>"}` |
| Deny | `caduceus.deny_enrollment` | `mcp__caduceus__caduceus_deny_enrollment` | `{"request_id":"<id>"}` |

The adapter consumes the existing MCP `structuredContent` control envelope:

```json
{
  "ok": true,
  "data": {
    "requests": [
      {
        "id": "req-example-1",
        "status": "pending",
        "display_name": "studio-node",
        "peer_id": "12D3KooWExample",
        "public_key_fingerprint": "sha256:example",
        "source_address": "192.168.1.40",
        "expires_at": "2030-01-01T00:05:00Z"
      }
    ]
  }
}
```

`approved_by` and `denied_by` are intentionally omitted. The local control
service supplies its Hermes/control identity.

## Prompt and decision behavior

For each new, unexpired, pending request, the command displays only these
whitelisted fields:

- request ID;
- display name;
- peer ID;
- public-key fingerprint;
- source address;
- expiration time.

All values are bounded, control characters are removed, and the prompt labels
the metadata as untrusted. It includes explicit commands for a human decision:

```text
/caduceus-enrollments approve req-example-1
/caduceus-enrollments deny req-example-1
```

The adapter sends only the validated request ID to a decision tool. It reports
approved, denied, expired, not found, disabled, missing-tool, or a sanitized
error code. It never reflects arbitrary daemon error messages.

## Security invariants

- Invitation tokens, group keys, group hashes, credentials, and unknown fields
  are never rendered or copied into a decision call.
- Enrollment metadata is data, never an instruction to Hermes.
- Request IDs must match a small identifier grammar before dispatch.
- Requests at or beyond `expires_at` are shown as expired without an
  approve/deny prompt.
- A successful decision is cached for the process lifetime so an accidental
  repeated command does not send a duplicate mutation.
- Import and registration perform no network, process, or state mutation.

The dependency-free implementation in [`enrollment_v1.py`](enrollment_v1.py)
is the reference adapter for this contract.
