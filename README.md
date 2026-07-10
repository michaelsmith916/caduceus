# Caduceus

Caduceus is a local-first LAN P2P worker fabric for LLM agents. It lets MCP clients discover trusted local workers and run prompt tasks against them. Phase I is intentionally small: Go binaries, go-libp2p with Noise, LAN mDNS discovery, manual peer allowlists, a shared group key hash, a local control API, and an MCP stdio server.

## What Phase I Does

- Runs `caduceusd`, a local libp2p daemon and worker.
- Runs `caduceus-mcp`, a native stdio MCP server.
- Runs `caduceusctl`, a CLI for config, status, peers, workers, and tasks.
- Discovers workers over LAN mDNS only.
- Filters discovered peers by shared P2P group key hash and manual allowlist.
- Executes prompt tasks through an OpenAI-compatible `/chat/completions` endpoint.
- Defaults to Ollama at `http://127.0.0.1:11434/v1`.
- Stores task metadata, JSONL events, results, and artifacts as local files.

## What Phase I Does Not Do

No DHT, relay, NAT traversal, cloud discovery, containers, web dashboard, remote shell execution, arbitrary filesystem access, account systems, payments, distributed consensus, auto-updates, or sandboxed code execution.

## Quick Start: WSL/Linux

```bash
./scripts/build.sh
./dist/linux-amd64/caduceusctl init
./dist/linux-amd64/caduceusctl ollama check
./dist/linux-amd64/caduceusd
```

In another shell:

```bash
./dist/linux-amd64/caduceusctl status
./dist/linux-amd64/caduceusctl run-prompt --worker auto --prompt "Say hello from Caduceus in one sentence."
```

Use `./scripts/install-linux.sh` for a user-local install and optional systemd user service.

## Quick Start: Windows

From PowerShell:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install.ps1
& "$env:LOCALAPPDATA\Caduceus\bin\caduceusd.exe"
```

Optional startup and service helpers:

```powershell
.\deploy\windows\add-startup.ps1
.\deploy\windows\install-service.ps1
```

Service install requires an elevated PowerShell session.

## Quick Start: macOS

```bash
./scripts/build.sh
./scripts/install-macos.sh
caduceusd
```

The installer can create a launchd user agent when requested.

## Ollama

Caduceus checks for Ollama first and does not install it without confirmation. The task path uses OpenAI-compatible HTTP, so you may point Caduceus at any compatible backend:

```bash
export CADUCEUS_OPENAI_BASE_URL="http://127.0.0.1:11434/v1"
export CADUCEUS_DEFAULT_MODEL="qwen3.5:9b"
```

## MCP

Run:

```bash
caduceus-mcp
```

Example MCP client config:

```json
{
  "mcpServers": {
    "caduceus": {
      "command": "caduceus-mcp",
      "args": []
    }
  }
}
```

Core tools include `caduceus.list_workers`, `caduceus.validate_task`, `caduceus.run_remote_prompt`, `caduceus.get_task_events`, and `caduceus.get_task_result`.

### Hermes Agent

Install and enable the Caduceus plugin with Hermes Agent v0.18.2 or newer:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

The plugin bundles the safe-delegation skill and `/caduceus-status`. It keeps
`caduceus-mcp` as the tool transport instead of duplicating MCP tools in
Python. See the [WSL Ubuntu and native Windows setup guide](hermes/README.md)
for MCP configuration and verification.

## Security Warning

Caduceus is for trusted LANs. A matching group hash is not identity. Keep `require_allowlist: true`, add peers manually, and never send secrets to a worker unless you explicitly trust that machine and user account.

## Build and Test

```bash
./scripts/build.sh
./scripts/test.sh
```

## Roadmap

Phase II adds non-LAN discovery options, relay/NAT traversal, and richer worker capabilities. Later phases explore sandboxed tools, a polished tray/dashboard, scheduling, policy, multiple backends, and signed worker attestations.
