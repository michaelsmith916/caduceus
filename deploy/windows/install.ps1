param(
  [string]$InstallDir = "$env:LOCALAPPDATA\Caduceus\bin"
)

$ErrorActionPreference = "Stop"
$Root = Resolve-Path "$PSScriptRoot\..\.."
$Dist = Join-Path $Root "dist\windows-amd64"

if (!(Test-Path $Dist)) {
  Write-Host "Building Windows binaries..."
  & bash "$Root\scripts\build.sh"
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
Copy-Item "$Dist\caduceusd.exe", "$Dist\caduceusctl.exe", "$Dist\caduceus-mcp.exe", "$Dist\caduceus-tray.exe" -Destination $InstallDir -Force
& "$InstallDir\caduceusctl.exe" init

Write-Host "Installed Caduceus to $InstallDir"
Write-Host "Add $InstallDir to PATH if desired."
