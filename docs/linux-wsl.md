# Linux and WSL

Build:

```bash
./scripts/build.sh
./dist/linux-amd64/caduceusctl init
```

Install:

```bash
./scripts/install-linux.sh
```

The installer copies binaries to `~/.local/bin`, initializes config, optionally installs Ollama after confirmation, and installs a systemd user service when available.

WSL environments without systemd can run:

```bash
caduceusd
```

To start manually from a shell profile, add a guarded background launch that fits your WSL workflow.
