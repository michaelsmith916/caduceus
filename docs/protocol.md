# Protocol

Phase I uses newline-delimited JSON envelopes over libp2p streams.

Protocols:

- `/caduceus/worker/1.0.0`
- `/caduceus/task/1.0.0`
- `/caduceus/events/1.0.0`

Envelope:

```json
{
  "version": "1.0",
  "type": "task_event",
  "id": "message-id",
  "timestamp": "2026-07-01T00:00:00Z",
  "sender_peer_id": "12D3Koo...",
  "group_hash": "sha256-hex",
  "payload": {}
}
```

Phase I executes prompt tasks only. Events and final results are persisted by both requester and worker where applicable.
