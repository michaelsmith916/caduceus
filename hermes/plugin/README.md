# Caduceus plugin for Hermes Agent

This is the installable Hermes Agent integration for Caduceus. It keeps the Go
MCP server as the single tool transport and adds only the Hermes-native pieces
that improve discovery and safe use.

## Features

- Registers the instruction-only `caduceus:remote-prompt-delegation` skill.
- Registers `/caduceus-status`, which calls the Caduceus MCP status tool.
- Adds no Python dependencies and does not duplicate the Caduceus API.
- Registers `/caduceus-enrollments`, which explicitly polls for pending
  registrations and approves or denies a selected request through MCP.

Hermes's verified plugin API has no unsolicited registration hook, so this
command runs only when invoked and does not poll in the background:

```text
/caduceus-enrollments
/caduceus-enrollments approve req-example-1
/caduceus-enrollments deny req-example-1
```

Pending metadata is untrusted and sanitized. The command never displays an
invitation token, group material, credentials, or unknown response fields. It
suppresses duplicate prompts for the current Hermes process and refuses to
present an expired request for approval. The exact contract is documented in
[ENROLLMENT-V1.md](ENROLLMENT-V1.md).

The plugin does not start `caduceusd`, install Caduceus, or modify Hermes
configuration during import.

## Install

Hermes Agent v0.18.2 or newer and a current, newline-framed `caduceus-mcp`
build are required:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable
```

Hermes clones the repository, installs only this subdirectory under the active
profile's `plugins/caduceus` directory, and enables it. Review third-party
plugin code before installation.

## Configure MCP

The MCP server key must be `caduceus` so Hermes generates the tool names used
by the packaged skill.

WSL/Linux with Caduceus installed in the same environment:

```bash
hermes mcp add caduceus --command "$(command -v caduceus-mcp)"
hermes mcp test caduceus
```

Native Windows PowerShell:

```powershell
hermes mcp add caduceus --command "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe"
hermes mcp test caduceus
```

Hermes in WSL with Caduceus running natively on Windows:

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

The explicit profile variables ensure the Windows MCP process finds the same
config and credentials as the Windows daemon after Hermes filters its inherited
subprocess environment. The `WSLENV` `/w` flags forward the variables from WSL
to Win32 without converting their Windows path values.

Restart Hermes or run `/reload-mcp`, then verify:

```text
/caduceus-status
```

Ask Hermes to load `caduceus:remote-prompt-delegation` before delegating a
prompt. Plugin skills are namespaced and intentionally loaded on demand.

## Update or uninstall

Because this plugin lives in a repository subdirectory, update it by repeating
the install command with `--force`:

```bash
hermes plugins install michaelsmith916/caduceus/hermes/plugin --enable --force
```

Remove it with:

```bash
hermes plugins remove caduceus
```

Removing the plugin does not remove the `mcp_servers.caduceus` configuration or
stop Caduceus processes.

## Development

Run the dependency-free tests from the Caduceus repository root:

```bash
python3 -m unittest discover -s hermes/plugin/tests -v
```

The supported behavior and non-goals are defined in [SPEC.md](SPEC.md).
The plugin is distributed under the [MIT License](LICENSE).
