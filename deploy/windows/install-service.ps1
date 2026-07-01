param(
  [string]$InstallDir = "$env:LOCALAPPDATA\Caduceus\bin",
  [string]$ServiceName = "Caduceus"
)

$ErrorActionPreference = "Stop"
$Exe = Join-Path $InstallDir "caduceusd.exe"
if (!(Test-Path $Exe)) {
  throw "caduceusd.exe not found at $Exe"
}
Write-Host "Installing a Windows service requires an elevated PowerShell session."
New-Service -Name $ServiceName -BinaryPathName "`"$Exe`"" -DisplayName "Caduceus daemon" -StartupType Automatic
Start-Service $ServiceName
Write-Host "Installed and started service $ServiceName"
