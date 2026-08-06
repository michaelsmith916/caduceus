# Caduceus

Caduceus is a local-first, trusted-LAN P2P worker fabric for LLM agents. It lets MCP clients discover approved workers, explain deterministic routing decisions, and run prompt tasks with bounded admission, durable requester-side queuing, explicit attempts, and conservative failover.

Phase 2 keeps the intentionally small deployment model: Go binaries, go-libp2p with Noise, LAN mDNS discovery, peer allowlists, a routing group hash, a local control API, and a native MCP stdio server. It does not introduce a central coordinator, database, DHT, relay, or distributed consensus.

## What Phase 2 does

- Tracks worker sessions with ordered heartbeats and `starting`, `available`, `busy`, `unavailable`, `draining`, and coordinator-observed `offline` states.
- Fans heartbeats out with at most 16 concurrent sends and one interval-wide deadline.
- Enforces worker concurrency and optional bounded worker-side waiting; zero queue depth rejects excess work immediately.
- Separately bounds the requester's durable FIFO and the number of tasks claimed for dispatch.
- Persists an exact FIFO owned by the requester and reconciles interrupted attempts after restart.
- Filters hard requirements before scoring eligible workers with deterministic tie-breaking and route explanations.
- Applies bounded, process-local `retry_after` worker cooldowns and reroutes from fresh registry snapshots.
- Collects CPU/RAM telemetry and rolling model throughput without treating missing metrics as zero. GPU telemetry is unsupported until a platform provider is integrated.
- Fences late generation-2 results with attempt IDs and random attempt tokens.
- Replays ambiguous or interrupted work only when the task is explicitly idempotent and has remaining attempt budget.
- Provides disabled-by-default trusted-LAN enrollment with expiring invitations, CIDR/rate controls, explicit approval, replay protection, and secret-free audit records.

## Non-goals and limitations

No DHT, relay, NAT traversal, cloud discovery, containers, web dashboard, remote shell execution, arbitrary filesystem access, account system, payments, distributed reservation, auto-update, or sandboxed code execution. Caduceus does not provide exactly-once execution: fencing protects requester state, but cannot undo an external side effect performed by an old partitioned attempt. Generation-1 fallback cannot provide generation-2 attempt-token fencing.

## Quick start: WSL/Linux

```bash
./scripts/build.sh
./dist/linux-amd64/caduceusctl init
./dist/linux-amd64/caduceusctl ollama check
./dist/linux-amd64/caduceusd
```

In another shell:

```bash
./dist/linux-amd64/caduceusctl status
./dist/linux-amd64/caduceusctl route --explain examples/task-route.example.json
./dist/linux-amd64/caduceusctl run-prompt --worker auto --prompt "Say hello from Caduceus in one sentence."
```

Use `./scripts/install-linux.sh` for a user-local install and optional systemd user service.

## Quick start: Windows

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install.ps1
& "$env:LOCALAPPDATA\Caduceus\bin\caduceusd.exe"
```

Optional startup and service helpers:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\add-startup.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install-service.ps1
```

Service installation requires an elevated PowerShell session. It reuses the current user's configuration and node identity; reinstalling binaries preserves an existing `config.yaml`.

## Quick start: macOS

```bash
./scripts/build.sh
./scripts/install-macos.sh
caduceusd
```

The installer can create a launchd user agent when requested.

## Ollama and compatible backends

Caduceus checks for Ollama first and does not install it without confirmation. The task path uses OpenAI-compatible HTTP, so it can target another compatible backend:

```bash
export CADUCEUS_OPENAI_BASE_URL="http://127.0.0.1:11434/v1"
export CADUCEUS_DEFAULT_MODEL="qwen3.5:9b"
```

## MCP and Hermes

Run `caduceus-mcp` and configure it as a stdio MCP server:

```json
{
  "mcpServers": {
    "caduceus": {
      "command": "caduceus-mcp",
      "args": []
    }
  }
}
```

Phase 2 tools include worker/task operations, `caduceus.explain_route`, and the sanitized enrollment administration tools `caduceus.list_enrollment_requests`, `caduceus.approve_enrollment`, and `caduceus.deny_enrollment`. See [docs/mcp.md](docs/mcp.md).

Hermes Agent v0.18.2 or newer can install the plugin:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

The plugin uses `caduceus-mcp`, exposes `/caduceus-status`, and provides the explicitly invoked `/caduceus-enrollments [list|approve <id>|deny <id>]` workflow. Hermes does not receive unsolicited enrollment notifications. See [hermes/README.md](hermes/README.md).

Trusted-LAN enrollment is disabled by default. Once an operator enables and
reviews it, use `caduceusctl enrollment invite|list|get|approve|deny|audit` on the
coordinator and `caduceusctl enrollment request --token-file <path|->` followed by
`caduceusctl enrollment status <request-id>` on the applicant. The CLI deliberately
has no `--token` flag, preventing invitation secrets from appearing in process lists.

## Security warning

Caduceus is for trusted LANs. A routing group hash is not an authentication credential. Keep `require_allowlist: true`, approve only verified libp2p peer identities, and do not send secrets to a worker unless you trust that machine and user account. Enrollment transfers only a routing group hash, coordinator addresses, and coordinator identity/fingerprint; it never transfers a private key, shared key, or reusable invitation token.

## Documentation

- [Phase 2 architecture and acceptance criteria](docs/phase-2.md)
- [Protocol generation 2](docs/protocol-v2.md)
- [Configuration](docs/configuration.md)
- [Operator guide](docs/operator-phase-2.md)
- [Migration and compatibility](docs/migration-phase-2.md)
- [Security](docs/security.md)
- [Roadmap](docs/roadmap.md)

## Build and test

```bash
./scripts/build.sh
./scripts/test.sh
```

Copyright (c) 2026 Verdant Code, LLC
