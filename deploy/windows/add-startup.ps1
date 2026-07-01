param(
  [string]$InstallDir = "$env:LOCALAPPDATA\Caduceus\bin"
)

$ErrorActionPreference = "Stop"
$Exe = Join-Path $InstallDir "caduceusd.exe"
if (!(Test-Path $Exe)) {
  throw "caduceusd.exe not found at $Exe"
}
$RunKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
New-ItemProperty -Path $RunKey -Name "Caduceus" -Value "`"$Exe`"" -PropertyType String -Force | Out-Null
Write-Host "Registered Caduceus daemon for current-user startup."
