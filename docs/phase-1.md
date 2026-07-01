# Phase I

Phase I is a LAN-only MVP for trusted local worker delegation.

Implemented:

- Native Go daemon, MCP server, and CLI.
- go-libp2p host with Noise transport security.
- mDNS LAN discovery.
- Shared group key hash and manual allowlist checks.
- Worker capability hello exchange.
- Prompt task request, event, result, and cancel messages.
- OpenAI-compatible chat adapter with streaming SSE support.
- Plain-file task store.
- Native install scripts for Linux/WSL, macOS, and Windows.

Deferred:

- Internet discovery, DHT, relay, NAT traversal.
- Remote shell or arbitrary filesystem access.
- Web dashboard and polished tray UI.
- Sandboxed code execution.
- Databases, containers, and account systems.
