# Roadmap

## Phase 1 — complete

- Go daemon, CLI, MCP server, and public package.
- Noise-secured libp2p, LAN mDNS, shared routing group hash, and manual allowlists.
- Prompt execution through an OpenAI-compatible backend with local file storage.

## Phase 2 — complete

- Worker lifecycle, ordered heartbeats, suspect/evict handling, and opaque availability policy.
- Hard concurrency, bounded worker admission, structured rejection, and requester-owned durable FIFO recovery.
- CPU/RAM telemetry, rolling model performance, resource/trust constraints, deterministic weighted routing, and explanations.
- Explicit attempts, random fencing tokens, conservative idempotent failover, and late-result rejection.
- Disabled-by-default trusted-LAN enrollment with expiring one-time invitations, CIDR/rate controls, approval, replay protection, atomic allowlist persistence, audit, CLI/control/MCP operations, and invoked Hermes review.
- Generation-2 worker/task protocols with generation-1 negotiation fallback.

## Phase 3 candidates

- Sandboxed tools with explicit policy and more task kinds after the prompt boundary is well tested.
- Web dashboard or polished native tray UI.
- Better operator diagnostics and platform-specific GPU probes.
- An exported operational metrics endpoint for fleet dashboards and alerts.
- Multiple LLM backend profiles and stronger artifact policy.

## Future research, not Phase 2 commitments

- Non-LAN discovery, relay, and NAT traversal.
- Signed worker attestations and stronger workload isolation.
- Richer policy engines and administrative federation.

Caduceus does not currently plan distributed consensus, exactly-once side effects, or an implicit trust model based on private IP addresses.
