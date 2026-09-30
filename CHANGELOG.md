# Changelog

All notable changes to Caduceus are documented here.

## Unreleased — Phase 2

### Added

- Ordered worker sessions and heartbeats, lifecycle/availability policy, suspect/evict handling, and privacy-preserving status.
- Hard concurrency, bounded worker admission, structured rejection, and requester-owned durable FIFO recovery.
- CPU/RAM telemetry, rolling per-model throughput, resource/trust constraints, deterministic weighted routing, and read-only route explanations.
- Explicit task attempts, cryptographic attempt tokens, late-result fencing, and bounded failover for explicitly idempotent work.
- Generation-2 worker/task protocols with negotiated generation-1 fallback.
- Disabled-by-default trusted-LAN enrollment with expiring invitations, CIDR/rate controls, approval, replay/duplicate prevention, atomic allowlist persistence, and secret-free audit records.
- Enrollment administration through the control API, CLI, MCP, and the explicitly invoked Hermes `/caduceus-enrollments` workflow.
- Phase 2 architecture, protocol, migration, operator, security, configuration, and troubleshooting documentation.

### Changed

- `worker.max_concurrent_tasks` is now a hard positive limit; zero is invalid.
- `worker.max_queue_depth: 0` now explicitly means no worker-side wait queue.
- Automatic routing filters hard requirements before deterministic weighted scoring instead of selecting the first matching worker.
- Restart recovery interrupts active attempts and automatically requeues only explicitly idempotent work with remaining attempt budget.

### Security

- All generation-2 envelopes are bounded and validated before use.
- Attempt tokens fence stale generation-2 results, but Caduceus still does not claim exactly-once side effects.
- Enrollment transfers only coordinator addresses, peer identity/fingerprint, and routing group hash—never private/shared keys or reusable invitation secrets.

### Compatibility

- Persisted Phase 1 task records remain readable because Phase 2 fields are additive.
- Generation-1 stream fallback remains available, without generation-2 attempt-token guarantees.
- Existing manual allowlists continue to work; enrollment is opt-in.
