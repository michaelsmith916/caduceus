# Caduceus for Hermes Agent

Caduceus integrates with Hermes Agent as an installable plugin backed by the
native `caduceus-mcp` server. The plugin supplies Hermes-native guidance and a
connectivity command; MCP remains the single transport for Caduceus tools.

Verified with [Hermes Agent v0.18.2](https://github.com/NousResearch/hermes-agent/releases/tag/v2026.7.7.2).
Use a current Caduceus build: older `caduceus-mcp` binaries that only support
`Content-Length` framing cannot connect to Hermes's MCP SDK.

| Component | Runs as | Responsibility |
|---|---|---|
| `caduceusd` | Persistent process | LAN discovery, workers, tasks, and local control API |
| `caduceus-mcp` | Child process started by Hermes | MCP tools and resources over stdio |
| Caduceus Hermes plugin | In the Hermes process | Delegation skill and `/caduceus-status` |

The plugin deliberately does not reimplement Caduceus tools in Python, start
the daemon, or edit configuration during import.

## What the plugin installs

- The instruction-only `caduceus:remote-prompt-delegation` skill.
- `/caduceus-status`, a slash command that calls the MCP status tool.
- Post-install instructions for connecting Hermes to `caduceus-mcp`.

The skill is bundled with the plugin, so it does not require a second install
command. Plugin skills are namespaced and loaded on demand; ask Hermes to load
`caduceus:remote-prompt-delegation` before delegating work.

## WSL Ubuntu

Windows and WSL Hermes installations are separate. Complete every step below
inside the WSL distribution where `hermes` runs.

### 1. Install or update Hermes

Fresh install:

```bash
curl -fsSL https://hermes-agent.nousresearch.com/install.sh | bash
source ~/.bashrc
```

For an existing installation, run `hermes update`. Then verify:

```bash
hermes --version
```

Hermes data, plugins, and configuration live under `~/.hermes` in WSL.

### 2. Verify Caduceus

For a Caduceus installation running inside WSL:

```bash
caduceusctl status
command -v caduceus-mcp
```

Both commands must succeed before configuring Hermes. Start `caduceusd` if the
status command cannot reach the daemon. If Caduceus was installed from an older
checkout, rebuild it with `./scripts/build.sh` and rerun
`./scripts/install-linux.sh` first.

### 3. Install the plugin

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

This is a true subdirectory install supported by Hermes v0.18.2; only
`hermes/plugin` is installed in the active Hermes profile.

### 4. Add and test the MCP server

When Caduceus runs inside WSL:

```bash
hermes mcp add caduceus --command "$(command -v caduceus-mcp)"
hermes mcp test caduceus
```

Keep the server name exactly `caduceus`; the packaged skill relies on the MCP
tool namespace derived from that name.

If Hermes runs in WSL while Caduceus runs natively on Windows, launch the
Windows MCP executable through WSL interoperability instead. Read the native
Windows profile paths, then explicitly forward them to the Win32 child:

```bash
WIN_USERPROFILE="$(cmd.exe /d /c echo %USERPROFILE% | tr -d '\r')"
WIN_APPDATA="$(cmd.exe /d /c echo %APPDATA% | tr -d '\r')"
WIN_LOCALAPPDATA="$(cmd.exe /d /c echo %LOCALAPPDATA% | tr -d '\r')"

hermes mcp add caduceus \
  --command "$(wslpath -u "$WIN_LOCALAPPDATA")/Caduceus/bin/caduceus-mcp.exe" \
  --env "USERPROFILE=$WIN_USERPROFILE" "APPDATA=$WIN_APPDATA" \
        "LOCALAPPDATA=$WIN_LOCALAPPDATA" \
        "WSLENV=USERPROFILE/w:APPDATA/w:LOCALAPPDATA/w"
hermes mcp test caduceus
```

Use the Windows `caduceus-mcp.exe` with the Windows daemon because it reads the
same `%APPDATA%\Caduceus\config.yaml` and control credentials. Do not point a
default WSL `caduceus-mcp` configuration at a native Windows daemon. Hermes
filters the inherited environment of MCP subprocesses, which is why this
cross-OS case supplies the three Windows profile variables explicitly.
`WSLENV`'s `/w` flags make WSL forward those already-Windows-formatted values
to the Win32 process without path conversion.

### 5. Verify in Hermes

Start a new session, or run `/reload-mcp` in an existing session, then run:

```text
/caduceus-status
```

For a delegated task, ask:

```text
Load caduceus:remote-prompt-delegation, then list my trusted Caduceus workers.
```

## Native Windows

Run these commands in PowerShell. Native Windows Hermes stores its data under
`$env:LOCALAPPDATA\hermes`; this is not `$HOME\.hermes` unless you explicitly
override `HERMES_HOME`.

### 1. Install or update Hermes

Fresh install:

```powershell
iex (irm https://hermes-agent.nousresearch.com/install.ps1)
```

For an existing installation, run `hermes update`. Then verify:

```powershell
hermes --version
```

Open a new PowerShell window if `hermes` is not found immediately after the
installer updates the user PATH.

### 2. Verify Caduceus

```powershell
& "$env:LOCALAPPDATA\Caduceus\bin\caduceusctl.exe" status
Test-Path "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe"
```

The first command must succeed and the second must print `True`. Start
`caduceusd.exe` using your normal startup or service configuration if needed.
If Caduceus was installed from an older checkout, reinstall the current build
from the repository root. Stop the existing daemon or `Caduceus` service first
so Windows can replace the executable, then run the installer without
permanently changing PowerShell policy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install.ps1 -Build
```

### 3. Install the plugin

```powershell
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

The plugin is installed under
`$env:LOCALAPPDATA\hermes\plugins\caduceus`.

### 4. Add and test the MCP server

```powershell
hermes mcp add caduceus --command "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe"
hermes mcp test caduceus
```

If Caduceus uses a non-default configuration file, append `--args` last because
Hermes passes everything after it to the MCP process:

```powershell
hermes mcp add caduceus --command "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe" --args --config "C:\path\to\config.yaml"
```

### 5. Verify in Hermes

Start a new session, or run `/reload-mcp`, then run:

```text
/caduceus-status
```

For a delegated task, ask Hermes to load
`caduceus:remote-prompt-delegation` first.

## Update

Hermes subdirectory installs do not retain the repository's `.git` directory,
so `hermes plugins update caduceus` cannot pull this plugin. Reinstall it in one
command instead:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable --force
```

The same command works in Bash and PowerShell.

## Uninstall

```bash
hermes plugins remove caduceus
hermes mcp remove caduceus
```

These commands do not stop or uninstall Caduceus.

## Troubleshooting

Check each layer independently:

```text
caduceusctl status
hermes plugins list --enabled
hermes mcp test caduceus
/caduceus-status
```

- If `caduceusctl status` fails, fix or start `caduceusd` first.
- If the plugin is not enabled, run `hermes plugins enable caduceus` and start
  a new Hermes session.
- If the MCP test fails, verify that its executable belongs to the same Windows
  or WSL environment as the daemon.
- If the MCP test succeeds but tools are absent, run `/reload-mcp`.
- On WSL, use `~/.hermes/config.yaml`; on native Windows, use
  `$env:LOCALAPPDATA\hermes\config.yaml`.

Set `HERMES_PLUGINS_DEBUG=1` before starting Hermes when plugin discovery needs
verbose diagnostics.

## Development and specification

- [Plugin README](plugin/README.md)
- [Runtime specification](plugin/SPEC.md)
- [Standalone skill source](skill/SKILL.md)
- [MCP server documentation](../docs/mcp.md)

Run all repository tests:

```bash
./scripts/test.sh
```

Run only the dependency-free plugin tests:

```bash
python3 -m unittest discover -s hermes/plugin/tests -v
```

The implementation follows the official Hermes
[plugin](https://hermes-agent.nousresearch.com/docs/user-guide/features/plugins),
[MCP](https://hermes-agent.nousresearch.com/docs/user-guide/features/mcp), and
[skills](https://hermes-agent.nousresearch.com/docs/user-guide/features/skills)
contracts.
