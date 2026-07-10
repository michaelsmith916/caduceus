# Caduceus plugin installed

The plugin is installed. Configure the Caduceus MCP executable in the active
Hermes profile, then restart Hermes or run `/reload-mcp`. Use a current
Caduceus build; older `Content-Length`-only MCP binaries are incompatible with
Hermes's MCP SDK.

WSL/Linux:

```bash
hermes mcp add caduceus --command "$(command -v caduceus-mcp)"
hermes mcp test caduceus
```

Native Windows PowerShell:

```powershell
hermes mcp add caduceus --command "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe"
hermes mcp test caduceus
```

Keep `caduceusd` running, then use `/caduceus-status`. To load the packaged
workflow, ask Hermes to load `caduceus:remote-prompt-delegation`.
