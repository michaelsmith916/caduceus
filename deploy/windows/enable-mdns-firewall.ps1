[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [ValidateSet("Domain", "Private", "Public", "Any")]
  [string[]]$Profile = @("Private"),

  [switch]$SkipWindowsFirewall,
  [switch]$SkipWSL,
  [switch]$AllowNativeDaemonTcp,
  [string]$CaduceusExe = "$env:LOCALAPPDATA\Caduceus\bin\caduceusd.exe",
  [switch]$AllowWSLDaemonTcp,
  [ValidateRange(1, 65535)]
  [int]$WSLDaemonTcpPort = 37392
)

$ErrorActionPreference = "Stop"

$RuleGroup = "Caduceus"
$WslCreatorId = "{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}"
$Profiles = if ($Profile -contains "Any") { @("Any") } else { $Profile }
$MDNSMulticastAddresses = @("224.0.0.251", "ff02::fb")

function Test-IsAdministrator {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = [Security.Principal.WindowsPrincipal]::new($identity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Remove-WindowsRuleIfPresent {
  param([string]$Name)

  $existing = Get-NetFirewallRule -Name $Name -ErrorAction SilentlyContinue
  if ($null -ne $existing) {
    $existing | Remove-NetFirewallRule
  }
}

function Add-WindowsMDNSRules {
  if ($PSCmdlet.ShouldProcess("Windows Defender Firewall", "allow Caduceus mDNS UDP 5353")) {
    Remove-WindowsRuleIfPresent -Name "Caduceus-mDNS-UDP5353-In"
    Remove-WindowsRuleIfPresent -Name "Caduceus-mDNS-UDP5353-Out"

    New-NetFirewallRule `
      -Name "Caduceus-mDNS-UDP5353-In" `
      -DisplayName "Caduceus mDNS (UDP 5353 In)" `
      -Group $RuleGroup `
      -Enabled True `
      -Profile $Profiles `
      -Direction Inbound `
      -Action Allow `
      -Protocol UDP `
      -LocalPort 5353 `
      -RemotePort 5353 `
      -RemoteAddress LocalSubnet | Out-Null

    New-NetFirewallRule `
      -Name "Caduceus-mDNS-UDP5353-Out" `
      -DisplayName "Caduceus mDNS (UDP 5353 Out)" `
      -Group $RuleGroup `
      -Enabled True `
      -Profile $Profiles `
      -Direction Outbound `
      -Action Allow `
      -Protocol UDP `
      -RemotePort 5353 `
      -RemoteAddress $MDNSMulticastAddresses | Out-Null
  }
}

function Add-NativeDaemonRule {
  if (!(Test-Path $CaduceusExe)) {
    Write-Warning "Skipping native daemon TCP rule because caduceusd.exe was not found at $CaduceusExe"
    return
  }

  if ($PSCmdlet.ShouldProcess("Windows Defender Firewall", "allow inbound TCP for $CaduceusExe")) {
    Remove-WindowsRuleIfPresent -Name "Caduceus-NativeDaemon-TCP-In"
    New-NetFirewallRule `
      -Name "Caduceus-NativeDaemon-TCP-In" `
      -DisplayName "Caduceus daemon TCP In" `
      -Group $RuleGroup `
      -Enabled True `
      -Profile $Profiles `
      -Direction Inbound `
      -Action Allow `
      -Protocol TCP `
      -Program $CaduceusExe | Out-Null
  }
}

function Remove-HyperVRuleIfPresent {
  param([string]$Name)

  $existing = Get-NetFirewallHyperVRule -Name $Name -ErrorAction SilentlyContinue
  if ($null -ne $existing) {
    $existing | Remove-NetFirewallHyperVRule
  }
}

function Add-WSLMDNSRules {
  Add-WSLHyperVRules -RuleKind "mDNS" -TcpPort 0
}

function Add-WSLDaemonTCPRule {
  Add-WSLHyperVRules -RuleKind "DaemonTCP" -TcpPort $WSLDaemonTcpPort
}

function Add-WSLHyperVRules {
  param(
    [ValidateSet("mDNS", "DaemonTCP")]
    [string]$RuleKind,
    [int]$TcpPort
  )

  $newHyperVRule = Get-Command New-NetFirewallHyperVRule -ErrorAction SilentlyContinue
  $getHyperVRule = Get-Command Get-NetFirewallHyperVRule -ErrorAction SilentlyContinue
  $removeHyperVRule = Get-Command Remove-NetFirewallHyperVRule -ErrorAction SilentlyContinue
  if ($null -eq $newHyperVRule -or $null -eq $getHyperVRule -or $null -eq $removeHyperVRule) {
    Write-Warning "Skipping WSL Hyper-V firewall rules because NetFirewallHyperV cmdlets are unavailable on this Windows build."
    return
  }

  if ($RuleKind -eq "mDNS") {
    if ($PSCmdlet.ShouldProcess("WSL Hyper-V Firewall", "allow Caduceus mDNS UDP 5353")) {
      Remove-HyperVRuleIfPresent -Name "Caduceus-WSL-mDNS-UDP5353-In"
      Remove-HyperVRuleIfPresent -Name "Caduceus-WSL-mDNS-UDP5353-Out"

      New-NetFirewallHyperVRule `
        -Name "Caduceus-WSL-mDNS-UDP5353-In" `
        -DisplayName "Caduceus WSL mDNS (UDP 5353 In)" `
        -VMCreatorId $WslCreatorId `
        -Direction Inbound `
        -Action Allow `
        -Protocol UDP `
        -LocalPorts 5353 `
        -RemotePorts 5353 | Out-Null

      New-NetFirewallHyperVRule `
        -Name "Caduceus-WSL-mDNS-UDP5353-Out" `
        -DisplayName "Caduceus WSL mDNS (UDP 5353 Out)" `
        -VMCreatorId $WslCreatorId `
        -Direction Outbound `
        -Action Allow `
        -Protocol UDP `
        -RemotePorts 5353 | Out-Null
    }
    return
  }

  if ($PSCmdlet.ShouldProcess("WSL Hyper-V Firewall", "allow Caduceus daemon TCP $TcpPort")) {
    Remove-HyperVRuleIfPresent -Name "Caduceus-WSL-Daemon-TCP$TcpPort-In"

    New-NetFirewallHyperVRule `
      -Name "Caduceus-WSL-Daemon-TCP$TcpPort-In" `
      -DisplayName "Caduceus WSL daemon (TCP $TcpPort In)" `
      -VMCreatorId $WslCreatorId `
      -Direction Inbound `
      -Action Allow `
      -Protocol TCP `
      -LocalPorts $TcpPort | Out-Null
  }
}

if (!(Test-IsAdministrator)) {
  throw "Run this script from an elevated PowerShell session."
}

if (!$SkipWindowsFirewall) {
  Add-WindowsMDNSRules
}

if ($AllowNativeDaemonTcp) {
  Add-NativeDaemonRule
}

if (!$SkipWSL) {
  Add-WSLMDNSRules

  if ($AllowWSLDaemonTcp) {
    Add-WSLDaemonTCPRule
  }
}

Write-Host "Caduceus mDNS firewall rules are configured."
Write-Host "Profiles: $($Profiles -join ', ')"
Write-Host "Use -AllowNativeDaemonTcp to also allow inbound TCP to native Windows caduceusd.exe."
Write-Host "Use -AllowWSLDaemonTcp -WSLDaemonTcpPort 37392 after configuring WSL caduceusd to listen on that fixed port."
