param(
  [string]$InstallDir = "$env:LOCALAPPDATA\Caduceus\bin",
  [string]$ServiceName = "Caduceus"
)

$ErrorActionPreference = "Stop"
$Exe = Join-Path $InstallDir "caduceusd.exe"
if (!(Test-Path $Exe)) {
  throw "caduceusd.exe not found at $Exe"
}

$Principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (!$Principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  throw "Installing a Windows service requires an elevated PowerShell session."
}

$Config = Join-Path $env:APPDATA "Caduceus\config.yaml"
$ConfigDir = Split-Path -Parent $Config
$DataDir = Join-Path $env:LOCALAPPDATA "Caduceus"
if (!(Test-Path $Config)) {
  throw "Caduceus config not found at $Config. Run deploy\windows\install.ps1 first."
}

$BinaryPath = "`"$Exe`" --service-name `"$ServiceName`" --config `"$Config`" --config-dir `"$ConfigDir`" --data-dir `"$DataDir`""
$ExistingService = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($ExistingService) {
  if ($ExistingService.Status -ne "Stopped") {
    Stop-Service -Name $ServiceName
  }
  $ServiceInstance = Get-CimInstance Win32_Service -Filter "Name='$ServiceName'"
  $ChangeResult = Invoke-CimMethod -InputObject $ServiceInstance -MethodName Change -Arguments @{
    PathName = $BinaryPath
    StartMode = "Automatic"
  }
  if ($ChangeResult.ReturnValue -ne 0) {
    throw "Failed to update Windows service $ServiceName (Win32_Service.Change return value $($ChangeResult.ReturnValue))."
  }
  Write-Host "Updated existing service $ServiceName."
} else {
  New-Service -Name $ServiceName -BinaryPathName $BinaryPath -DisplayName "Caduceus daemon" -StartupType Automatic | Out-Null
  Write-Host "Installed service $ServiceName."
}

try {
  Start-Service -Name $ServiceName
} catch {
  $Details = Get-CimInstance Win32_Service -Filter "Name='$ServiceName'"
  throw "Service $ServiceName could not start (state=$($Details.State), exit_code=$($Details.ExitCode), service_exit_code=$($Details.ServiceSpecificExitCode)). Rebuild and reinstall the Windows binaries, then retry. $($_.Exception.Message)"
}
Write-Host "Started service $ServiceName using config $Config."
