# Security

Caduceus is designed for trusted LANs, not hostile networks.

## Trust Model

Every node has a libp2p identity keypair. Every trusted group shares P2P key material. Caduceus derives a stable SHA-256 group hash from that key and advertises only the hash. The raw key is stored locally and is never logged.

## Peer Identity

The group hash is not identity. A peer must also be allowed by peer ID when `require_allowlist: true`, which is the default.

## mDNS Exposure

mDNS can reveal that a Caduceus node exists on the LAN. Unknown peers may be discovered, but Phase I ignores peers that fail group hash or allowlist checks.

## Noise

libp2p streams are secured with Noise. Noise protects stream confidentiality and integrity between libp2p peers.

## Local Control API

The control API is local only. Unix systems default to a Unix socket. Windows defaults to loopback HTTP with a random bearer token in the user data directory.

## Prompt and Data Leakage

Prompt tasks send prompt text to another machine. Do not send secrets, credentials, unreleased source, private files, or sensitive personal data unless the user explicitly approves and the peer is trusted.

## Why No Remote Shell

Remote shell execution is excluded because it turns a prompt worker into a general remote execution system. Phase I only accepts validated prompt tasks.

## Safe Defaults

- `require_allowlist: true`
- LAN mDNS only
- no DHT, relay, or NAT traversal
- prompt tasks only
- local file storage
- no auto-updating daemon
