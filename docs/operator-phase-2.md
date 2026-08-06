# Phase 2 operator guide

## Heartbeats and flapping

The default heartbeat/suspect/evict windows are 10/30/90 seconds. Keep
`suspect_after` greater than the interval and `evict_after` greater than
`suspect_after`. A suspect worker is immediately excluded but retained for
diagnostics until eviction. Increase both windows for laptops that frequently
sleep or networks with long pauses; increasing only eviction does not keep a
suspect worker schedulable.

Heartbeat fanout runs at most 16 sends concurrently and gives the whole round
one heartbeat-interval deadline. With many blocked peers, later peers may miss a
round, so retain several intervals of margin in suspect and eviction thresholds.

Session changes are normal after daemon restart. Repeated session churn without
process restarts usually indicates crashes, duplicate identities, or an unstable
service manager. Out-of-order status errors indicate a replay, duplicate sender,
or implementation defect and should not be bypassed.

## Queue sizing and backpressure

Concurrency is a hard execution limit. The worker wait queue holds assignments
that already passed local policy but are waiting for a permit. Set it to zero
for latency-sensitive interactive work; use a small positive depth when brief
bursts are expected. The requester durable FIFO is separate and is the
authoritative restart record. Bound it with `scheduler.max_queue_depth` and bound
claimed dispatches with `scheduler.max_in_flight_tasks`. Exact FIFO governs claim
order, not completion order after concurrent dispatch begins. Worker-side admission
waiters use best-effort runtime wake ordering and do not have the requester's strict
FIFO fairness guarantee.

A concurrency permit reserves a task slot. It does not reserve aggregate CPU, RAM, or GPU capacity; use conservative resource thresholds and concurrency limits.

`at_capacity` means no permit and no enabled wait slot. `queue_full` means the
configured wait slots are already occupied. `worker_draining` and
`worker_unavailable` are policy decisions, not capacity. Scheduler decisions and
rejections are retained with the task.

## Safe failover

Use `idempotent: true` only when repeating the whole operation cannot cause an
incorrect duplicate side effect. A pure prompt is often replay-safe; a prompt
that drives a tool, payment, email, deployment, or mutation might not be. Keep
`max_attempts` small. Attempt-token fencing prevents an old result from changing
task state, but does not stop a partitioned worker that already began work.

After coordinator restart, running attempts become interrupted. Only explicitly
idempotent tasks with remaining attempt budget re-enter the FIFO. Worker sessions
must heartbeat again; disk state never makes them live.

## Reading route explanations

Run:

```bash
caduceusctl route --explain examples/task-route.example.json
caduceusctl route --explain --output json examples/task-route.example.json
cat examples/task-route.example.json | caduceusctl route --explain -
```

Hard filter reasons explain ineligibility. Eligible workers are compared with
the configured throughput, running-task load, queue, model residency, RTT, cost,
and resource-headroom weights. Missing soft metrics are neutral and called out;
they are never silently treated as zero. Equal scores use least-recently-assigned
then worker ID, making a fixed snapshot deterministic.

## Enrollment and revocation

Enrollment is an opt-in convenience around the same Noise identity and
allowlist trust model. Enable it only on the intended coordinator, restrict
allowed networks, keep interactive approval enabled, and distribute invitation
tokens out of band. A private source address is not authorization.

The administrator creates a short-lived invitation, the applicant submits it
over an encrypted enrollment stream, and Hermes or the CLI lists sanitized
pending metadata. Coordinator approval uses a durable approving state before it
adds the authenticated peer ID/fingerprint. Approval becomes effective on the
applicant when the applicant receives the approved decision during submission or
status polling; approval alone does not push state to an applicant that never polls.
Audit records never contain the invitation token.

Approved-status handling verifies the authenticated coordinator identity, replaces the coordinator configuration and peer allowlist files, updates live membership, connects, and sends an initial hello/status without a restart. Each file is replaced atomically. If an operation returns an error, Caduceus attempts to roll back prior changes and reports the failure. This is not a crash-consistent transaction across both files or across coordinator and applicant state; inspect both files after a crash during approval application.

Coordinator administration:

```bash
caduceusctl enrollment invite
caduceusctl enrollment list
caduceusctl enrollment get <request-id>
caduceusctl enrollment approve <request-id> --actor operator-name
caduceusctl enrollment deny <request-id> --actor operator-name
caduceusctl enrollment audit
```

On an applicant, read the invitation from a protected file or stdin so it does not
appear in the process list. The coordinator defaults to
`enrollment.trusted_lan.coordinator_address` when omitted:

```bash
caduceusctl enrollment request --coordinator <multiaddr> --token-file ./invite.txt --name new-node
printf '%s' "$INVITATION" | caduceusctl enrollment request --token-file -
caduceusctl enrollment status <request-id> --coordinator <multiaddr>
```

There is intentionally no `--token` flag. Do not persist invitation files longer
than needed. After approval, have the applicant poll status and confirm both its configured coordinator entry and allowlist before considering auto-join complete.

To revoke a node, remove it from `allowed-peers.yaml` with `caduceusctl peers
remove <peer-id>` on every node that authorized it, then restart or reload those
daemons. Rotate invitations after suspected disclosure. Rotate the group
namespace and rebuild allowlists if multiple identities or configuration files
may be compromised.

## Telemetry

CPU and RAM are measured locally. The first CPU-utilization sample can be
unknown because it establishes a baseline. GPU status is `unsupported` without
a vendor probe. Rolling tokens/sec becomes influential only after the configured
sample minimum and before the maximum metric age. Worker telemetry is an
untrusted hint, not billing proof or an authorization signal.
