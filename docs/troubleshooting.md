# Troubleshooting

## Daemon Not Reachable

Run:

```bash
caduceusctl status
```

If it cannot connect, start `caduceusd` and check the configured control endpoint with `caduceusctl config show`.

## No Workers

Confirm both nodes have the same group key hash and that each peer ID is in the other node's allowlist.

## Ollama Errors

Run:

```bash
caduceusctl ollama check
```

Confirm the OpenAI-compatible endpoint responds at `/v1/models`.

## WSL mDNS

Some WSL networking modes limit multicast. If mDNS discovery fails, use local tasks or test on a network mode that supports multicast.

On Windows hosts, run an elevated PowerShell session and allow Caduceus mDNS through Windows Firewall:

```powershell
.\deploy\windows\enable-mdns-firewall.cmd
```

If running from a WSL path trips PowerShell's unsigned-script policy, use the `.cmd` wrapper or run `powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\enable-mdns-firewall.ps1`.

The script also adds WSL Hyper-V firewall mDNS rules when those cmdlets are available on the Windows build. If discovery works but task connections still fail, configure Caduceus to listen on a fixed TCP port and add a matching inbound firewall rule for that port.
