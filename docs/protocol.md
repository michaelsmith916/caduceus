# Protocol

Caduceus uses bounded newline-delimited JSON envelopes over Noise-secured libp2p streams. Phase 2 introduces negotiated generation-2 worker and task protocols while retaining generation-1 fallback.

Protocols:

- `/caduceus/worker/2.0.0` with `/caduceus/worker/1.0.0` fallback;
- `/caduceus/task/2.0.0` with `/caduceus/task/1.0.0` fallback;
- `/caduceus/events/1.0.0`;
- `/caduceus/enrollment/1.0.0`, independent because it precedes membership.

Generation-2 envelopes are capped at 1 MiB and validate version, type, bounded identifiers, timestamp, authenticated sender peer ID, routing group hash where applicable, and payload before use. Enrollment intentionally does not require a matching group hash; Noise identity, invitation, nonce/fingerprint, CIDR/rate policy, and approval govern that flow.

Generation 2 adds ordered worker sessions/status, explicit acceptance or stable structured rejection, attempt IDs/tokens, fenced results/cancellation, exact requester-owned FIFO recovery, and enrollment messages. A generation-1 peer retains its legacy request/result shape and cannot provide cross-worker attempt-token fencing.

All additions to persisted Phase 1 task JSON are optional and additive. New implementations offer generation 2 first and send the payload variant selected by libp2p negotiation.

See [protocol-v2.md](protocol-v2.md) for message semantics, compatibility rules, limits, rejection reasons, and restart behavior.
