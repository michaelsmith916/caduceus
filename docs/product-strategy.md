# Product Strategy and Feature Proposal

Status: proposal. Supersedes nothing; `docs/roadmap.md` remains the committed plan
until items here are accepted.

## 1. Where Caduceus stands today

Phase I delivers a working slice: libp2p + Noise + mDNS, group hash and allowlist,
an OpenAI-compatible adapter, a plain-file task store, and an MCP front door with
13 tools. The transport and trust layers are the hard part and they are done.

The gaps below are load-bearing for everything in this document.

| Gap | Where | Effect |
|---|---|---|
| Worker selection is arbitrary | `internal/p2p/node.go:619` `chooseWorker` returns the first non-local allowed worker from a Go map range | Non-deterministic. A 4090 and a laptop are equally likely. |
| Constraints are validated but unused | `tasks.Constraints.PreferredModels` / `RequiredCapabilities` are checked in `ValidateRequest`, never read by `chooseWorker` | The API promises routing control it does not deliver. |
| Concurrency limit is cosmetic | `MaxConcurrentTasks` is advertised in `selfWorker` and defaulted in config; no semaphore or queue exists | A worker accepts unbounded work and thrashes. |
| Tasks are synchronous | `RunTask` returns `tasks.Result` inline | A 20-minute job holds the caller open. No detach, no resume. |
| No liveness management | `Worker.LastSeen` is written, never read for eviction or failover | Dead peers stay in the registry; a mid-task worker loss is unrecoverable. |
| One backend per node | `config.OpenAIConfig` is singular | A node cannot expose Ollama and a cloud endpoint side by side. |
| Pairing is manual | Copy the shared key, then copy peer IDs into `allowed-peers.yaml` | The single largest adoption barrier. |

## 2. The strategic bet

Caduceus is not competing with Ollama, and it should not compete with exo or
`llama.cpp` RPC.

- **Ollama / LocalAI** run one model on one machine. Caduceus's premise is a fleet.
- **exo / llama.cpp RPC** shard one model's tensors across devices. That is
  bandwidth-bound and fragile on consumer LANs, and it makes the fleet only as
  fast as its slowest link.
- **Ray / vLLM** assume a homogeneous datacenter and an operator.

Caduceus occupies a different slot: **task-level distribution across a
heterogeneous, intermittently-available, personally-owned fleet, addressed by an
agent.** A gaming PC, a Mac Studio, an old Xeon with a P40, and a work laptop.
Different models, different speeds, different availability, no operator.

That framing implies the product's real job: **routing and orchestration**, not
inference. Inference is already solved by the backends. Nobody has solved "which
of my six machines should run this, right now, without annoying whoever is sitting
at it."

Everything below follows from that.

## 3. Feature proposals

### Tier 1 — Make the core promise true

These are not novel. They are the price of admission, and none of them exist yet.

**1.1 A real scheduler.** Replace `chooseWorker` with a scoring function over the
registry:

- hard filters: required capabilities, model availability, allowlist, trust level
- soft scores: measured tokens/sec for this model on this worker, current queue
  depth, model residency (below), RTT, worker-declared cost weight
- tie-break: least-recently-used, so a fleet spreads load

Ship `caduceusctl route --explain <task.json>` that prints the score table. Routing
you cannot inspect is routing nobody trusts.

**1.2 Model residency tracking.** Cold-loading a 30B model costs 20–60 seconds and
dominates end-to-end latency for short prompts. Track which models are currently
resident in each worker's backend (Ollama exposes this via `/api/ps`), score
resident models far higher, and expose `caduceus.warm_model` so an agent can
pre-load before a batch. This is the cheapest large latency win available.

**1.3 Queue, admission control, and backpressure.** Enforce
`MaxConcurrentTasks` with a semaphore. Add `task_reject` with a reason and a
retry-after hint so the requester's scheduler can re-route instead of blocking.
Persist queue position and surface it in `get_task_status`.

**1.4 Async submission with resumable streams.** The store already appends events
with a monotonic cursor and `GetTaskEvents(taskID, cursor)` already exists — the
hard part is done. Split `run_remote_task` into `submit_task` (returns immediately)
and the existing cursor-based event polling. An agent submits a long job, drops
the connection, and reattaches at the cursor. This unblocks every batch feature
in Tier 2.

**1.5 Heartbeat, eviction, and failover.** Periodic worker status pings; evict on
missed heartbeats. On mid-task worker loss, re-dispatch to the next-best worker if
the task is marked idempotent. Fleets of consumer machines lose members constantly
— someone closes a laptop lid. Surviving that gracefully is a feature users will
notice immediately.

**1.6 Structured output.** Pass through `response_format` / JSON schema / grammar
constraints to backends that support them. Delegated results that agents can parse
programmatically are worth far more than free text, and this is a small change to
`PromptTask` and the OpenAI adapter.

**1.7 Pairing.** Replace manual key and peer-ID copying:

```
host:   caduceusctl pair --host       # prints 6-word code, 120s window
joiner: caduceusctl pair --join gray-otter-lantern-vivid-north-clay
```

Short-lived PAKE over the LAN, exchanging group key and peer IDs, with both sides
confirming a matching fingerprint. Every install that fails at step three is a lost
user, and today step three is hand-editing YAML.

### Tier 2 — Do things one machine cannot

This is the differentiated layer. A fleet is not just a faster single machine; it
has a fundamentally different shape, and these features are only expressible on a
fleet.

**2.1 Council / ensemble tasks.** Fan one prompt out to N workers running
*different* models in parallel, return all responses plus an optional
judge/synthesis pass on the strongest worker.

```
caduceus.run_council { prompt, workers: 3, models: ["qwen3-coder:30b", "llama4:17b", "gpt-oss:20b"], mode: "synthesize" }
```

Wall-clock cost of the slowest single model, quality above any of them.
Impossible on one machine — a single box runs them serially and reloads weights
between each. This is the demo that makes people install it.

**2.2 Map-reduce over a corpus.** Chunk an input, fan the map step across the
fleet, reduce locally.

```
caduceus.map_reduce { inputs: [...], map_prompt, reduce_prompt, max_parallel }
```

"Summarize every file in this repo" goes from a 40-minute serial grind to a few
minutes on four machines. For agentic coding this is the highest-value primitive
in the document, and it is the natural consumer of 1.3 and 1.4.

**2.3 `embed` as the second task kind.** Embedding is embarrassingly parallel,
carries none of the risk that pushed remote shell out of scope, and is the obvious
next `Kind` after `prompt`. Distributed indexing of a codebase or document set
across the fleet, then a shared LAN vector index the group can query. This turns
Caduceus from a delegation tool into infrastructure.

**2.4 Presence-aware, polite scheduling.** Workers declare and enforce local
policy:

- pause when a fullscreen application has focus, or GPU utilization from another
  process exceeds a threshold
- pause on battery, or below a charge floor
- quiet hours, thermal ceilings, per-day token budgets

This is what makes people willing to run Caduceus on a spouse's desktop or an
office machine. It is also completely absent from every comparable project, and it
is the difference between a fleet of one and a fleet of six.

**2.5 Content-addressed result cache across the mesh.** Hash `(model, params,
system, prompt)`; check the local store, then ask the group before dispatching.
Agents re-ask near-identical questions constantly. Opt-in per trust level, with a
TTL, since cache sharing is an information-flow decision.

**2.6 Speculative racing.** For latency-critical short prompts, dispatch to the two
best workers and take the first result, cancelling the loser. `cancel_task` already
exists. Cheap to build on top of 2.1's fan-out machinery, and it makes the fleet
feel fast in a way benchmarks do not capture.

### Tier 3 — Trust, reach, and visibility

**3.1 Signed result receipts.** The worker signs a receipt over the result hash,
model ID, quantization, sampling parameters, token counts, and timing, using its
existing libp2p identity key. The requester stores it alongside the result. This
delivers the roadmap's "signed worker attestations" and answers the question every
serious user eventually asks: *which machine and which model actually produced
this?* Verification via `caduceusctl verify <task-id>`.

**3.2 Off-LAN via existing overlays, not DHT.** Phase II lists "relay and NAT
traversal." Most of the target audience already runs Tailscale or WireGuard.
Supporting explicit peer addresses over an existing overlay — with mDNS off and the
allowlist mandatory — delivers "my laptop in a café uses my home 4090" for a
fraction of the effort and a fraction of the attack surface of a DHT. Ship this
first; treat DHT and public relays as a separate, later, opt-in decision.

**3.3 Multiple backend profiles and burst policy.** Make `OpenAIConfig` a list.
A node can then expose Ollama, a `llama.cpp` server, and a cloud endpoint as
distinct capability sets, and the scheduler can honor a policy like: *prefer local;
if no worker can start within 30 seconds, burst to the cloud profile; never send
tasks tagged `sensitive` off-fleet.* The privacy-preserving default with an
explicit, auditable escape hatch is a strong story.

**3.4 Fleet dashboard.** The roadmap's web dashboard, scoped to what matters: live
task stream, per-worker utilization and queue depth, model residency map, tokens
and watt-hours per worker per day. Read-only, served from the control API, bound to
loopback.

**3.5 `caduceusctl bench`.** Benchmark every worker across resident models and
print a ranked table with tokens/sec and, where readable, watts. It feeds real
numbers to the scheduler in 1.1, it is the fastest way for a new user to see value,
and a shareable "my fleet" table is the kind of artifact that spreads on its own.

## 4. Proposed sequencing

**Phase II — Routing (the core promise)**

1.1 scheduler · 1.2 model residency · 1.3 queue and admission control ·
1.4 async submission · 1.5 heartbeat and failover · 1.7 pairing

Exit criterion: on a four-machine fleet, 100 mixed tasks complete with no manual
worker selection, correct model matching, and no failures when one worker is
killed mid-run.

**Phase III — Fleet-native primitives (the differentiation)**

2.1 council · 2.2 map-reduce · 1.6 structured output · 2.6 speculative racing ·
3.5 bench

Exit criterion: map-reduce over a 500-file repository shows near-linear speedup to
four workers.

**Phase IV — Infrastructure**

2.3 embed kind and distributed index · 2.4 presence-aware scheduling ·
2.5 mesh cache · 3.3 backend profiles and burst policy

**Phase V — Trust and reach**

3.1 signed receipts · 3.2 overlay networking · 3.4 dashboard

## 5. Non-goals (explicit)

- **Remote shell or arbitrary code execution.** `docs/security.md` is right. Keep it.
- **Tensor sharding across machines.** Bandwidth-bound on consumer LANs, and it
  makes the fleet as fragile as its worst link. Task-level distribution is the
  stronger position; ceding sharding to exo is correct.
- **Public DHT discovery.** Large attack surface for a system whose trust model is
  "trusted LAN." Overlay networks (3.2) cover the real use case.
- **Accounts, hosted control plane, or a marketplace for spare GPU time.** Both
  invert the trust model that makes Caduceus adoptable.

## 6. Highest-leverage single item

If only one thing ships: **1.1, the scheduler.** Every other feature is either
blocked on it or made unconvincing by its absence. A fleet that routes to a random
machine is not a fleet.

If only one *novel* thing ships: **2.2, map-reduce.** It is the clearest answer to
"why would I install this instead of just running Ollama," and its value scales
with the number of machines — which is exactly the growth behavior the project
wants.
