# Caduceus protocol generation 2

Status: implemented
Version: 2.0
Compatibility: additive data model with negotiated Phase 1 fallback

## Streams and envelopes

Generation 2 adds `/caduceus/worker/2.0.0` and
`/caduceus/task/2.0.0`. Trusted-LAN enrollment uses
`/caduceus/enrollment/1.0.0`; its version is independent because enrollment
precedes group membership. A new peer offers generation 2 first and Phase 1
second. The selected libp2p stream ID determines which payload variant is sent.

Every message remains one newline-delimited JSON envelope. Envelopes are capped
at 1 MiB and require a supported version, known type, bounded ID, RFC 3339
timestamp, authenticated sender peer ID, lowercase 32-byte group hash, and a
non-empty JSON payload. Messages more than 24 hours old or more than five
minutes in the future are rejected. The enrollment handler intentionally does
not require a matching group hash because the requester is not a member yet;
Noise identity, the invitation, nonce, fingerprint, CIDR policy, and approval
are its trust controls.

## Worker status

`worker_status` is sent periodically and contains:

- `worker_id` and `peer_id`, both equal to the authenticated libp2p identity;
- a random daemon `session_id` and strictly increasing `sequence`;
- protocol version, timestamp, lifecycle `state`, and `accepting_work`;
- running/max task counts and current/max worker admission queue depth;
- capabilities, loaded models, resource snapshot, cost weight, and rolling
  per-model performance when measured.

The receiver acknowledges the exact session and sequence. A sequence cannot go
backward. Once a worker changes session, messages from the retired session are
rejected. Liveness is based on receiver time, not the advertised timestamp.

A node fans status heartbeats out concurrently with at most 16 active sends. All sends in one round share the heartbeat-interval deadline. Work still waiting behind the concurrency bound is abandoned when that deadline expires and retried on the next interval; this is bounded fanout, not guaranteed delivery to every peer in every round.

Metric values use `current`, `stale`, `unknown`, or `unsupported`; an absent
value is never decoded as measured zero. GPU collection is explicitly
`unsupported` until a platform provider is integrated.

## Task assignment, rejection, and attempts

A generation 2 `task_request` payload is:

```json
{
  "request": {"task_id": "task-123", "kind": "prompt", "task": {}},
  "attempt": {
    "attempt_id": "attempt-...",
    "attempt_token": "random-opaque-value",
    "attempt_number": 1,
    "task_id": "task-123",
    "worker_id": "12D3Koo...",
    "worker_session_id": "session-...",
    "state": "queued",
    "created_at": "2026-08-05T18:00:00Z",
    "updated_at": "2026-08-05T18:00:00Z"
  }
}
```

The worker revalidates availability, authorization, capabilities, model,
trust, CPU, RAM, GPU/runtime constraints, concurrency, and queue depth. It then
sends either `task_accept` bound to the attempt ID/token/session or a versioned
`task_reject`. Stable rejection reasons include `at_capacity`, `queue_full`,
`worker_draining`, `worker_unavailable`, `capability_missing`,
`model_unavailable`, `insufficient_cpu`, `insufficient_ram`,
`insufficient_gpu_memory`, `not_allowed`, `insufficient_trust`, and
`stale_assignment`.

`retry_after` is a process-local worker cooldown hint. The coordinator keeps the later deadline for that worker in bounded in-memory state and applies it to subsequent routing decisions. A worker tried in the current dispatch remains excluded even after its cooldown expires. When only cooling candidates remain, the dispatcher waits within the task context and then routes from a fresh registry snapshot.

Generation 2 results wrap the result with the attempt ID and token. A requester
accepts a result only when task ID, attempt ID, and token match its active
attempt. A late result is ignored/fenced. This protects coordinator state; it
cannot undo an external side effect performed by a partitioned old attempt.

Only requests explicitly marked `idempotent: true` may be replayed after an
ambiguous accepted/lost attempt, and never beyond `max_attempts`. A rejection
received before acceptance may be routed to another candidate even for
non-idempotent work because the rejecting worker did not begin execution.
Phase 1 peers retain the old request/result shape and therefore cannot provide
cross-worker attempt-token guarantees.

Version 1 has no generation-2 `attempt_id` or `attempt_token`. Attempt-token result fencing, stale-result rejection, and the corresponding idempotent failover guarantee therefore apply only to version-2 assignments. Compatibility negotiation does not make version-1 work exactly-once or generation-2 fenced.

## Queue and restart semantics

The requester persists an exact local FIFO in versioned `state.json` before it
claims an assignment. Task records retain attempts, rejections, routing
decision, queue owner/sequence/position, and interruption reason. Same-directory
temporary files are flushed and atomically replaced before directory sync.

On restart, active attempts become `interrupted`. An explicitly idempotent task
with remaining budget is restored to the FIFO; non-idempotent work is never
automatically replayed. Worker liveness is not restored from disk.

## Enrollment messages

`enrollment_request` contains the one-time token, nonce, authenticated peer ID,
display name, public-key fingerprint, and timestamp. The token is used only on
the encrypted initial request and is stored as a SHA-256 hash. It is excluded
from pending-request lists, decisions, audit records, normal logs, MCP, and
Hermes. `enrollment_decision` initially returns `pending`. Polling with
the unguessable request ID is restricted to the same peer identity.

After approval, the decision returns the coordinator peer ID/fingerprint,
addresses, and group routing hash. It never returns a private key or invitation
token. When the applicant submits or polls and receives the approved decision,
it binds the returned identity to the authenticated Noise peer, persists the
routing hash and coordinator allowlist entry, activates membership in memory,
connects to the coordinator, and sends an initial hello/status without requiring
a restart. Each file replacement is atomic and returned errors trigger rollback,
but the two files are not one crash-consistent transaction. Authentication still
comes from Noise identity plus the two-sided allowlist; a group hash alone grants
no trust.

## Evolution rules

- Add optional fields rather than changing existing meanings.
- Add a new stream protocol ID for an incompatible payload.
- Bound all new collections and strings before persistence or allocation.
- Keep unknown telemetry distinct from zero.
- Treat worker-reported metrics and cost as untrusted scheduling hints.
- Preserve conservative Phase 1 defaults: one non-idempotent attempt and no
  worker-side wait queue unless configured.
