# Troubleshooting

## Daemon not reachable

Run `caduceusctl status`. If it cannot connect, start `caduceusd` and inspect the configured local endpoint with `caduceusctl config show`. Unix defaults to a Unix socket; Windows defaults to loopback HTTP with a bearer token stored in the user data directory.

## No eligible workers

First request an explanation:

```bash
caduceusctl route --explain examples/task-route.example.json
caduceusctl route --explain --output json examples/task-route.example.json
```

Common hard-filter reasons are stale/offline status, unavailable or draining policy, allowlist/trust mismatch, missing capability/model/runtime, exhausted capacity, or unknown/insufficient resources. Confirm both nodes use the same routing group hash and authorize each other's authenticated peer IDs where two-sided execution is required.

An empty registry after restart is expected until workers heartbeat; live worker state is never restored from disk.

## Assignments are rejected

- `at_capacity`: all execution permits are occupied and no wait slot is available.
- `queue_full`: configured worker wait slots are occupied.
- `worker_draining` or `worker_unavailable`: local policy rejects new work.
- `stale_assignment`: the session or attempt binding is no longer current.
- resource/capability/model reasons: the worker revalidated a hard requirement and failed closed.

A structured rejection may include `retry_after`. The coordinator records that deadline in an in-memory cooldown keyed by worker ID, and the cooldown also applies to later tasks in this process. A later, longer deadline replaces an earlier one. Cooldowns are capped at 24 hours and the table at 4096 workers; they are not persisted across coordinator restarts. A cooling worker appears in route explanations as `retry_cooldown`.

For one dispatch, a worker that has already been tried is never retried. If every otherwise-eligible, untried worker is cooling, the dispatcher waits within the task context until the earliest cooldown can expire, using timer intervals no longer than one minute, then takes a fresh registry snapshot and routes again. `CancelTask` tracks the whole dispatch lifetime, including recovered tasks, and interrupts this wait immediately.

`retry_after` is process-local backpressure rather than a durable scheduling lease. A coordinator restart clears the cooldown table; current-dispatch exclusion still prevents a task from returning to a worker it already tried.

## Queued or interrupted after restart

The requester's durable queue has exact FIFO order. Active attempts are reconciled to interrupted at startup. Only tasks explicitly marked idempotent with remaining attempt budget are automatically restored to the FIFO. Non-idempotent work remains visible for operator inspection and is never silently replayed.

Recovery restores only work owned by this requester; inbound worker assignments
remain interrupted history. Pre-execution rejections do not consume the
execution budget. Recovered FIFO entries wait for eligible workers to appear
after discovery, keeping their queue position until assignment, cancellation,
or the request timeout during recovery.

An evicted worker renews its session after an authenticated registration
challenge. Old-session status, assignments, and results remain fenced. If both
peers were evicted, reciprocal heartbeat rounds can be needed to renew both
sessions.

## Late or duplicate result

Generation-2 results must match the active task ID, attempt ID, and random attempt token. A mismatched late result is ignored/fenced. This does not prove that the old worker stopped or undo an external side effect. Generation-1 fallback does not provide this protection.

## Enrollment rejected or remains pending

Check that trusted-LAN enrollment is enabled on the coordinator, the invitation and request have not expired, the source is inside an allowed CIDR, and the rate limit has not been reached. Pending requests require explicit approval by default:

```bash
caduceusctl enrollment list
caduceusctl enrollment approve <request-id>
```

Enrollment status polling reads historical decisions and never restores a
revoked allowlist entry. After `peers remove` and restart, a previous approval
does not authorize the peer again. `hermes_mode: disabled` blocks both the
Hermes enrollment command and direct enrollment MCP tools; local CLI/control
administration remains available.

The applicant must poll status after approval. Verify the coordinator multiaddress and request ID. Never place an invitation token in logs or support tickets. Replayed, duplicate-peer, duplicate-fingerprint, and invalid-token failures intentionally return a generic rejection.

## Missing or unsupported telemetry

The first CPU sample can be unknown because it establishes a baseline. GPU telemetry is unsupported unless a platform provider is integrated. Missing soft metrics are neutral in scoring; unknown hard resource constraints exclude the worker.

## Ollama errors

Run `caduceusctl ollama check` and confirm the OpenAI-compatible endpoint responds at `/v1/models`.

## WSL mDNS

Some WSL networking modes limit multicast. On Windows, run an elevated shell and allow mDNS:

```powershell
.\deploy\windows\enable-mdns-firewall.cmd
```

If discovery works but task connections to a WSL daemon fail, configure a fixed libp2p TCP port:

```yaml
node:
  listen_addrs:
    - "/ip4/0.0.0.0/tcp/37392"
```

Then add the matching WSL Hyper-V firewall rule and restart `caduceusd`:

```powershell
.\deploy\windows\enable-mdns-firewall.cmd -AllowWSLDaemonTcp -WSLDaemonTcpPort 37392
```

If PowerShell rejects a script from a WSL path, use the `.cmd` wrapper or `powershell.exe -NoProfile -ExecutionPolicy Bypass -File ...`.
