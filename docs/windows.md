# Windows

Native Windows support is PowerShell based.

```powershell
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

Phase I includes `caduceus-tray` as a stub. It documents the future tray boundary and avoids adding GUI dependencies to the core daemon.
