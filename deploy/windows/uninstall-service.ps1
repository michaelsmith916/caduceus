param(
  [string]$ServiceName = "Caduceus"
)

$ErrorActionPreference = "Stop"
Write-Host "Removing a Windows service requires an elevated PowerShell session."
if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
  Stop-Service $ServiceName -ErrorAction SilentlyContinue
  sc.exe delete $ServiceName | Out-Null
}
Write-Host "Removed service $ServiceName"
