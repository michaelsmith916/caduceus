# Windows

Native Windows support is PowerShell based.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install.ps1
& "$env:LOCALAPPDATA\Caduceus\bin\caduceusd.exe"
```

Pass `-Build` to force rebuilding the Windows binaries before installation:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\install.ps1 -Build
```

Current-user startup:

```powershell
.\deploy\windows\add-startup.ps1
```

Optional service install:

```powershell
Start-Process PowerShell -Verb RunAs
.\deploy\windows\install-service.ps1
```

Allow LAN mDNS discovery through Windows Firewall:

```powershell
Start-Process PowerShell -Verb RunAs
.\deploy\windows\enable-mdns-firewall.cmd
```

If the daemon runs natively on Windows and the firewall still blocks task connections after discovery, also allow inbound TCP for `caduceusd.exe`:

```powershell
.\deploy\windows\enable-mdns-firewall.cmd -AllowNativeDaemonTcp
```

If the daemon runs inside WSL, the native `caduceusd.exe` program rule does not apply. Configure the WSL daemon to use a fixed libp2p TCP port, for example:

```yaml
node:
  listen_addrs:
    - "/ip4/0.0.0.0/tcp/37392"
```

Then allow that port through the WSL Hyper-V firewall:

```powershell
.\deploy\windows\enable-mdns-firewall.cmd -AllowWSLDaemonTcp -WSLDaemonTcpPort 37392
```

Restart the WSL `caduceusd` process after changing `listen_addrs`.

When running directly from a WSL path like `\\wsl.localhost\...`, Windows may treat the PowerShell script as an unsigned remote script. Use the `.cmd` wrapper above, or launch PowerShell with a process-scoped bypass:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\enable-mdns-firewall.ps1 -AllowWSLDaemonTcp -WSLDaemonTcpPort 37392
```

Phase I includes `caduceus-tray` as a stub. It documents the future tray boundary and avoids adding GUI dependencies to the core daemon.
