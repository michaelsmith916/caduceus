# Configuration

Default configuration paths:

- Linux/WSL: `~/.config/caduceus/config.yaml`
- macOS: `~/Library/Application Support/Caduceus/config.yaml`
- Windows: `%APPDATA%\Caduceus\config.yaml`

Default data paths:

- Linux/WSL: `~/.local/share/caduceus`
- macOS: `~/Library/Application Support/Caduceus`
- Windows: `%LOCALAPPDATA%\Caduceus`

Run:

```bash
caduceusctl init
caduceusctl config show
caduceusctl key generate
```

See [examples/config.example.yaml](../examples/config.example.yaml) for a complete Phase 2 example. Existing Phase 1 configuration remains compatible because omitted Phase 2 fields receive conservative defaults.

## Worker admission and availability

`worker.max_concurrent_tasks` is a hard limit from 1 through 1024; zero is invalid and never means unlimited. `worker.max_queue_depth` (range 0 through 100000) bounds the worker's in-memory admission waiters. Its default, `0`, rejects assignments immediately when all execution permits are occupied. Worker-side waiters use best-effort runtime wake ordering and do not promise strict FIFO fairness; the requester queue below is the exact FIFO.

### Requester queue and in-flight bounds

`scheduler.max_queue_depth` (default `1024`, range 1 through 1000000) bounds the requester's persisted task queue. Accepted entries are recovered and claimed in exact FIFO order. `scheduler.max_in_flight_tasks` (default `64`, range 1 through 4096) is a semaphore bounding entries that have been claimed for dispatch or execution. The dispatcher acquires an in-flight permit before removing the queue head, so it cannot drain an unbounded durable queue into memory. FIFO describes claim order, not completion order after concurrent dispatch begins.

`worker.cost_weight` must be finite and positive and is an untrusted scheduling hint. Availability modes are `always`, `manual`, `unavailable`, `draining`, `idle`, `logged_out`, and `scheduled`. Activity-dependent modes fail closed when their local detector is unsupported or fails. Peers receive only the resulting opaque schedulability state.

Scheduled windows use lowercase day names (`sun` through `sat`), local `HH:MM` start/end times, and an optional IANA timezone such as `America/Los_Angeles`.

Admission reserves a concurrency permit and then recollects availability and CPU/RAM/GPU telemetry immediately before execution. CPU, RAM, and GPU quantities are validation inputs, not entries in a multi-resource reservation ledger. Concurrent tasks can therefore validate against the same observed headroom. Use conservative thresholds and `worker.max_concurrent_tasks` where strict aggregate resource isolation matters.

GPU telemetry is explicitly unsupported unless a provider is integrated; Caduceus does not synthesize GPU capacity. The first CPU sample can be unknown until a measurement interval exists. `idle` and `logged_out` modes require platform providers and fail closed when their signals are unavailable.

## Heartbeats

Defaults are:

```yaml
heartbeat:
  interval_seconds: 10
  suspect_after_seconds: 30
  evict_after_seconds: 90
  lease_seconds: 120
```

`suspect_after_seconds` must exceed the interval, and `evict_after_seconds` must exceed the suspect window. A suspect worker is immediately excluded from routing; eviction only removes the diagnostic registry entry. Liveness is never restored from disk.

Heartbeat delivery uses at most 16 concurrent peer sends with one shared interval deadline. Large sets of blocked peers can cause later peers to miss a round, so configure suspect and eviction thresholds with multiple intervals of margin. `lease_seconds` is reserved configuration in this release; routing liveness is governed by the suspect and eviction thresholds.

## Scheduler

Weights are non-negative and at least one must be positive. The defaults sum to 1.0, but normalization does not require that. `minimum_metric_samples` gates model-throughput scoring, `metric_max_age_seconds` rejects stale performance samples, `max_reroute_attempts` bounds pre-execution rerouting after structured rejections, and `score_epsilon` groups numerically equivalent scores before least-recently-assigned/worker-ID tie-breaking. Structured `retry_after` cooldowns are process-local, capped at 24 hours and 4096 worker entries, and are not restored after restart.

Scheduler assignment history, current worker telemetry, and rolling performance are in memory. Routing decisions saved with tasks are durable; the live scoring history is not restored after restart.

## Trusted-LAN enrollment

Enrollment is disabled by default:

```yaml
enrollment:
  trusted_lan:
    enabled: false
    coordinator_address: ""
    hermes_mode: invoked
    request_ttl_seconds: 300
    token_ttl_seconds: 600
    require_approval: true
    allowed_networks:
      - "10.0.0.0/8"
      - "172.16.0.0/12"
      - "192.168.0.0/16"
      - "fd00::/8"
    rate_limit: 10
    rate_window_seconds: 60
```

Enable it only on an intended coordinator, narrow `allowed_networks`, and keep approval enabled unless an operator has explicitly accepted the risk. `hermes_mode: invoked` permits the explicit `/caduceus-enrollments` command; `disabled` disables that integration mode. `coordinator_address` stores the selected coordinator multiaddress on an enrolled applicant.

Approved-status handling atomically replaces each affected configuration or allowlist file and rolls back changes when an operation returns an error. The two files are not a single crash-consistent transaction: inspect both after a process or host crash during enrollment application.

Enrollment persists only the routing group hash and the approved coordinator peer/fingerprint on the applicant. It never copies the coordinator's private key or shared group key. The routing hash remains a namespace/filter, not authorization.

## Peer authorization

Manual peer management remains supported and is required on every node that should accept the peer:

```bash
caduceusctl peers add 12D3Koo... --name office-rtx5090 --trust-level trusted-lan
caduceusctl peers list
caduceusctl peers remove 12D3Koo...
```

Keep `security.require_allowlist: true`. A valid lowercase SHA-256 `security.p2p_key_hash` may be loaded directly; it does not authenticate peers.
