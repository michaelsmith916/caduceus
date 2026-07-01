param(
  [string]$InstallDir = "$env:LOCALAPPDATA\Caduceus\bin"
)

$ErrorActionPreference = "Stop"
if (Test-Path $InstallDir) {
  Remove-Item -Recurse -Force $InstallDir
}
Write-Host "Removed Caduceus binaries from $InstallDir"
