# Architecture

Caduceus uses three processes:

```text
MCP client -> caduceus-mcp -> local control API -> caduceusd -> libp2p LAN workers -> OpenAI-compatible backend
```

`caduceusd` owns libp2p identity, mDNS discovery, worker registry, task execution, task storage, and local control. `caduceus-mcp` is a stdio JSON-RPC MCP bridge. `caduceusctl` is a human CLI over the same control API.

On Linux and macOS the control API defaults to an HTTP server over a Unix socket. On Windows it defaults to loopback HTTP with a random local bearer token stored in the user data directory.
