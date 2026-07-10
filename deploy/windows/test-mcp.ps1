param(
  [string]$McpExe = "$env:LOCALAPPDATA\Caduceus\bin\caduceus-mcp.exe",
  [string]$Config = "",
  [ValidateRange(1, 300)]
  [int]$TimeoutSeconds = 10
)

$ErrorActionPreference = "Stop"
$Utf8 = New-Object System.Text.UTF8Encoding($false)

function Write-McpMessage {
  param(
    [System.IO.Stream]$Stream,
    [hashtable]$Message
  )

  $json = $Message | ConvertTo-Json -Depth 20 -Compress
  $body = $Utf8.GetBytes("$json`n")
  $Stream.Write($body, 0, $body.Length)
  $Stream.Flush()
}

function Read-WithTimeout {
  param(
    [System.IO.Stream]$Stream,
    [byte[]]$Buffer,
    [int]$Offset,
    [int]$Count,
    [int]$TimeoutMilliseconds
  )

  $readTask = $Stream.ReadAsync($Buffer, $Offset, $Count)
  if (!$readTask.Wait($TimeoutMilliseconds)) {
    throw "Timed out waiting for a response from caduceus-mcp.exe."
  }
  return $readTask.Result
}

function Read-McpMessage {
  param(
    [System.IO.Stream]$Stream,
    [int]$TimeoutMilliseconds
  )

  $body = New-Object System.Collections.Generic.List[byte]
  $oneByte = New-Object byte[] 1
  while ($true) {
    $count = Read-WithTimeout $Stream $oneByte 0 1 $TimeoutMilliseconds
    if ($count -eq 0) {
      throw "caduceus-mcp.exe closed stdout before sending a complete response."
    }
    if ($oneByte[0] -eq 10) {
      break
    }
    $body.Add($oneByte[0])
    if ($body.Count -gt 16777216) {
      throw "MCP response exceeded 16 MiB."
    }
  }

  if ($body.Count -gt 0 -and $body[$body.Count - 1] -eq 13) {
    $body.RemoveAt($body.Count - 1)
  }
  return $Utf8.GetString($body.ToArray()) | ConvertFrom-Json
}

function Assert-RpcResponse {
  param(
    $Response,
    [int]$ExpectedId,
    [string]$Operation
  )

  if ($Response.id -ne $ExpectedId) {
    throw "$Operation returned response id '$($Response.id)'; expected '$ExpectedId'."
  }
  if ($null -ne $Response.error) {
    throw "$Operation failed: $($Response.error.message)"
  }
}

if (!(Test-Path -LiteralPath $McpExe -PathType Leaf)) {
  throw "caduceus-mcp.exe was not found at '$McpExe'. Use -McpExe to specify its path."
}
if ($Config -and !(Test-Path -LiteralPath $Config -PathType Leaf)) {
  throw "Caduceus config was not found at '$Config'."
}

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = (Resolve-Path -LiteralPath $McpExe).Path
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
$startInfo.RedirectStandardInput = $true
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError = $true
if ($Config) {
  $escapedConfig = (Resolve-Path -LiteralPath $Config).Path.Replace('"', '\"')
  $startInfo.Arguments = "--config `"$escapedConfig`""
}

$process = New-Object System.Diagnostics.Process
$process.StartInfo = $startInfo
$timeoutMs = $TimeoutSeconds * 1000
$started = $false

try {
  if (!$process.Start()) {
    throw "Failed to start '$McpExe'."
  }
  $started = $true

  $inputStream = $process.StandardInput.BaseStream
  $outputStream = $process.StandardOutput.BaseStream

  Write-McpMessage $inputStream @{
    jsonrpc = "2.0"
    id = 1
    method = "initialize"
    params = @{
      protocolVersion = "2024-11-05"
      capabilities = @{}
      clientInfo = @{ name = "caduceus-mcp-smoke-test"; version = "1.0" }
    }
  }
  $initialize = Read-McpMessage $outputStream $timeoutMs
  Assert-RpcResponse $initialize 1 "MCP initialize"
  if ($initialize.result.serverInfo.name -ne "caduceus-mcp") {
    throw "Unexpected MCP server name '$($initialize.result.serverInfo.name)'."
  }
  Write-Host "PASS: MCP handshake completed with caduceus-mcp $($initialize.result.serverInfo.version)."

  Write-McpMessage $inputStream @{
    jsonrpc = "2.0"
    method = "notifications/initialized"
    params = @{}
  }

  Write-McpMessage $inputStream @{
    jsonrpc = "2.0"
    id = 2
    method = "tools/list"
    params = @{}
  }
  $tools = Read-McpMessage $outputStream $timeoutMs
  Assert-RpcResponse $tools 2 "MCP tools/list"
  $statusTool = $tools.result.tools | Where-Object { $_.name -eq "caduceus.get_local_node_status" }
  if ($null -eq $statusTool) {
    throw "caduceus.get_local_node_status was not advertised by the MCP server."
  }
  Write-Host "PASS: MCP tools are available ($($tools.result.tools.Count) advertised)."

  Write-McpMessage $inputStream @{
    jsonrpc = "2.0"
    id = 3
    method = "tools/call"
    params = @{
      name = "caduceus.get_local_node_status"
      arguments = @{}
    }
  }
  $status = Read-McpMessage $outputStream $timeoutMs
  Assert-RpcResponse $status 3 "MCP tools/call"
  if ($status.result.isError -or !$status.result.structuredContent.ok) {
    $detail = $status.result.structuredContent.error.message
    if (!$detail) { $detail = "the daemon returned an unsuccessful status" }
    throw "MCP server could not reach caduceusd.exe: $detail"
  }

  Write-Host "PASS: caduceus-mcp.exe is connected to caduceusd.exe."
  Write-Host "Caduceus MCP smoke test passed."
}
finally {
  if ($started -and !$process.HasExited) {
    $process.StandardInput.Close()
    if (!$process.WaitForExit(1000)) {
      $process.Kill()
      $process.WaitForExit()
    }
  }
  if ($started -and $process.HasExited -and $process.ExitCode -ne 0) {
    $stderr = $process.StandardError.ReadToEnd().Trim()
    if ($stderr) {
      Write-Warning "caduceus-mcp.exe: $stderr"
    }
  }
  $process.Dispose()
}
