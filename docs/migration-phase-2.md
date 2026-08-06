# Migrating from Phase 1 to Phase 2

Phase 2 keeps the four binaries, local control transport, prompt task kind,
allowlist YAML, and file-backed task history. No database conversion is needed.
Restart every daemon after upgrading so it creates a fresh worker session and
negotiates the generation 2 stream IDs.

## Behavior changes

- `worker.max_concurrent_tasks` is enforced and must be at least `1`; `0` no
  longer means unspecified or unlimited.
- `worker.max_queue_depth: 0` means reject immediately when all permits are in
  use. Increase it only when local waiting is intentional.
- Automatic routing uses lifecycle, authorization, capability/model, capacity,
  resource, and trust filters before scoring. It no longer selects the first
  map entry.
- Missing resource data is `unknown`/`unsupported`; a hard resource constraint
  fails closed when the worker cannot prove it currently fits.
- New tasks are non-idempotent with one attempt unless both `idempotent: true`
  and a bounded `max_attempts` are supplied.
- Active Phase 1 task JSON remains readable. Newly saved records gain attempts,
  rejections, routing, and queue metadata.
- Trusted-LAN enrollment is disabled by default. Existing manual allowlists
  continue to work unchanged.

## Recommended upgrade

1. Back up `config.yaml`, `allowed-peers.yaml`, and the data directory.
2. Install the Phase 2 binaries and run `caduceusctl config show`.
3. Set a positive concurrency limit and review queue depth, heartbeat windows,
   scheduler weights, and availability mode.
4. Leave enrollment disabled until its CIDRs and approval path are reviewed.
5. Restart all nodes. Confirm each worker has a new session and current status.
6. Use `caduceusctl route --explain examples/task-route.example.json` before
   executing representative tasks.
7. Mark only genuinely replay-safe work idempotent.

## Rollback

Phase 1 readers ignore additive JSON fields, but a Phase 1 daemon does not
understand generation 2 attempts or structured rejection. Stop all daemons
before rolling binaries back. Preserve the data directory; do not delete
`state.json`. Tasks interrupted during rollback should be inspected manually,
especially if they might have external side effects.
