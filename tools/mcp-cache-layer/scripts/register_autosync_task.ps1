# auto_sync 작업 스케줄러 등록 스크립트
# 4시간 주기 (08:00, 12:00, 16:00, 20:00), 매일 실행

$pythonExe = "D:\Vibe Dev\Slack Bot\venv\Scripts\python.exe"
$scriptPath = "D:\Vibe Dev\QA Ops\mcp-cache-layer\scripts\auto_sync.py"
$workDir = "D:\Vibe Dev\QA Ops\mcp-cache-layer"

$action = New-ScheduledTaskAction -Execute $pythonExe -Argument $scriptPath -WorkingDirectory $workDir

# 4시간 주기: 08:00, 12:00, 16:00, 20:00
$triggers = @()
@("08:00","12:00","16:00","20:00") | ForEach-Object {
    $triggers += New-ScheduledTaskTrigger -Daily -At $_
}

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask -TaskName "MCP_AutoSync" -Description "MCP Cache Layer auto sync (Wiki+Jira delta sync, 4h interval 08-20)" -Action $action -Trigger $triggers -Settings $settings -Force

Write-Output "=== 등록 완료 ==="
Get-ScheduledTask -TaskName "MCP_AutoSync" | Format-List TaskName, State, Description
