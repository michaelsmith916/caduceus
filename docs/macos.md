# macOS

Build and install:

```bash
./scripts/build.sh
./scripts/install-macos.sh
```

The installer copies binaries to `~/.local/bin`, initializes config, checks for Ollama, and can install a launchd user agent.

If Ollama is missing:

```bash
brew install ollama
```

or install from the Ollama website.
