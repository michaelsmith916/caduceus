# Security

Caduceus is designed for trusted LANs, not hostile networks. Phase 2 improves authorization, replay resistance, bounded resource use, and auditability; it does not turn self-reported workers into trusted execution environments.

## Trust model

Each node has a libp2p identity keypair. Noise authenticates the remote libp2p peer and protects stream confidentiality and integrity. The default allowlist authorizes that peer ID and optional public-key fingerprint. A shared/routing group hash is only a discovery and routing namespace: possession or knowledge of it grants no identity or authorization.

Keep `require_allowlist: true`. Enrollment approval adds a specific authenticated peer; it never distributes a private identity key or shared group key.

## Threat assessment

| Threat | Phase 2 control | Residual risk |
| --- | --- | --- |
| Unknown LAN peer | Noise identity, group filter, default allowlist | mDNS still reveals service presence |
| Stolen invitation | short TTL, one-time hash, CIDR/rate checks, approval | thief on an allowed network can submit before expiry |
| Replay/duplicate enrollment | nonce, peer/fingerprint duplicate checks, persistent state | compromised approved identity must be revoked |
| Forged/stale worker status | authenticated sender, session ID, monotonic sequence, receipt-time liveness | approved worker may lie about resources/performance |
| Capacity exhaustion | hard concurrency and bounded admission/decoder/queue limits | approved peers can still submit permitted load |
| Late result after failover | attempt ID/token fencing | old partitioned work may still cause external side effects |
| Coordinator restart | active attempts interrupted; replay only if explicitly idempotent | operator must inspect ambiguous non-idempotent work |
| Control API exposure | Unix socket or loopback bearer token | local account compromise remains in scope |

## Enrollment secrets and privacy

Invitation tokens are random, displayed once, stored only as hashes, and sent only over the encrypted enrollment stream. They are omitted from list/status responses, audit records, normal logs, MCP, and Hermes. Enrollment decisions return coordinator addresses, peer identity/fingerprint, and routing group hash only.

CIDR restrictions reduce exposure but are not authentication. Keep explicit approval enabled and verify the pending peer ID/fingerprint through an independent channel. Pending display names and other peer-supplied metadata are untrusted text.

Worker activity detectors remain local. Peers receive an opaque availability state and `accepting_work`, not usernames, login identities, foreground applications, or precise idle time.

## Scheduling and telemetry

Worker-reported capabilities, models, resources, cost, queue depth, and performance are untrusted hints. Hard trust/allowlist checks occur independently. Missing or unsupported soft metrics are neutral; an unknown hard resource requirement fails closed.

CPU and RAM probes describe the worker host. GPU status is unsupported unless a platform provider is integrated. Telemetry should not be used as billing evidence or attestation. The first CPU sample can be unknown because it establishes a measurement baseline; activity-dependent `idle` and `logged_out` modes fail closed without their platform providers.

A concurrency permit is a task-slot reservation, not a CPU/RAM/GPU reservation. Fresh telemetry is revalidated before execution, but concurrently admitted tasks can observe the same resource headroom. Use conservative thresholds and concurrency limits where aggregate isolation matters.

Version 1 peers lack generation-2 attempt-token fencing. Compatibility negotiation does not confer generation-2 stale-result protection or exactly-once semantics.

## Prompt and data leakage

Prompt tasks send text to another machine and may persist task metadata, events, results, and artifacts locally. Do not send secrets, credentials, unreleased source, private files, or sensitive personal data unless the user explicitly approves and the peer, machine, backend, and user account are trusted.

## Why no remote shell

Remote shell execution remains excluded because it would turn a prompt worker into a general remote-execution system. Phase 2 still accepts validated task kinds and does not provide arbitrary filesystem or command access.

## Safe defaults

- allowlist required;
- trusted-LAN enrollment disabled and approval required when enabled;
- LAN mDNS only; no DHT, relay, or NAT traversal;
- worker concurrency of one and worker wait queue depth zero;
- non-idempotent, one-attempt tasks unless explicitly opted into replay;
- bounded generation-2 envelopes;
- local file persistence and local-only control transport;
- no auto-updating daemon.
