$ctl = "$env:LOCALAPPDATA\Caduceus\bin\caduceusctl.exe"

$run = & $ctl --json run-prompt --worker auto --prompt "hello" | ConvertFrom-Json
$taskId = $run.data.task_id

$status = & $ctl --json status | ConvertFrom-Json
$task = & $ctl --json tasks get $taskId | ConvertFrom-Json

echo "Task ID: $taskId"
echo "Peer ID: $($status.data.peer_id)"
echo "Worker Peer: $($task.data.worker_peer)"
echo "If Peer ID and Worker Peer match, the task is running on the same machine as this script."