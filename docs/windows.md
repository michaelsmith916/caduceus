# Windows

Native Windows support is PowerShell based.

```powershell
 Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\deploy\windows\install.ps1
& "$env:LOCALAPPDATA\Caduceus\bin\caduceusd.exe"
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

When running directly from a WSL path like `\\wsl.localhost\...`, Windows may treat the PowerShell script as an unsigned remote script. Use the `.cmd` wrapper above, or launch PowerShell with a process-scoped bypass:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\deploy\windows\enable-mdns-firewall.ps1 -AllowNativeDaemonTcp
```

Phase I includes `caduceus-tray` as a stub. It documents the future tray boundary and avoids adding GUI dependencies to the core daemon.
