# scripts/install_watchdog_external.ps1 — Register/unregister the external
# dead-man watchdog as a Windows Scheduled Task.
#
# Usage (run as the user, NOT elevated — task runs under user's context):
#     powershell -ExecutionPolicy Bypass -File scripts\install_watchdog_external.ps1 -Action install
#     powershell -ExecutionPolicy Bypass -File scripts\install_watchdog_external.ps1 -Action uninstall
#     powershell -ExecutionPolicy Bypass -File scripts\install_watchdog_external.ps1 -Action status
#
# Schedule: every 15 minutes, 24/7. Task runs whether user is logged in or
# not (provided the user has "log on as batch job" right).
#
# Task name: QTronExternalWatchdog

param(
    [ValidateSet('install', 'uninstall', 'status')]
    [string]$Action = 'status',
    [string]$TaskName = 'QTronExternalWatchdog',
    [int]$IntervalMinutes = 15
)

$ErrorActionPreference = 'Stop'

# Resolve repo root (this script lives in <repo>/scripts/)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot = Resolve-Path (Join-Path $ScriptDir '..')
$BatchPath = Join-Path $ScriptDir 'watchdog_external.bat'
# 2026-04-24: pythonw.exe 직접 호출로 cmd 창 spawn 방지. .bat 은 수동 CLI
# 테스트용으로만 유지 (재설치 시 Task 는 .bat 을 거치지 않음).
$PythonwExe = Join-Path $RepoRoot '.venv64\Scripts\pythonw.exe'
$WatchdogPy = Join-Path $ScriptDir 'watchdog_external.py'
$StdoutLog = Join-Path $RepoRoot 'backup\reports\incidents\watchdog_stdout.log'

if ($Action -eq 'install') {
    if (-not (Test-Path $WatchdogPy)) {
        throw "watchdog_external.py not found at $WatchdogPy"
    }
    if (-not (Test-Path $PythonwExe)) {
        throw "pythonw.exe not found at $PythonwExe (run venv setup first)"
    }

    Write-Host "Installing scheduled task '$TaskName'"
    Write-Host "  Pythonw:  $PythonwExe"
    Write-Host "  Script:   $WatchdogPy"
    Write-Host "  Interval: every $IntervalMinutes minutes"
    Write-Host "  Repo:     $RepoRoot"

    # Remove existing registration if present
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "  (removing existing registration first)"
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    # 2026-04-24 Jeff 보고건: 기존은 .bat 경유 → cmd.exe 콘솔 창이 15분마다
    # 떴다가 사라짐. pythonw.exe 를 직접 Execute 하면 console window 자체가
    # 생성되지 않는다. Hidden 설정은 Task 관리 UI 에서 숨기는 별도 기능.
    # pythonw 는 stdout 이 null 로 처리되어 print() 출력이 사라지므로,
    # watchdog 의 상세 동작은 `backup/reports/incidents/` 의 incident md 파일
    # 및 `_log.info` 로그로 확인한다 (`print(json.dumps(summary))` 는 소실).
    $taskAction = New-ScheduledTaskAction `
        -Execute $PythonwExe `
        -Argument "`"$WatchdogPy`"" `
        -WorkingDirectory $RepoRoot
    $taskTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
        -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes)
    $taskSettings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable:$false `
        -Hidden `
        -ExecutionTimeLimit (New-TimeSpan -Minutes 3)
    # LogonType Interactive — task runs within the user's login session (no
    # admin required to register). Aligns with how tray itself runs: both
    # need Jeff to be logged in. If 24/7 (logged-out) execution is needed
    # later, switch to S4U and elevate the install.
    $taskPrincipal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive

    Register-ScheduledTask -TaskName $TaskName -Action $taskAction -Trigger $taskTrigger `
        -Settings $taskSettings -Principal $taskPrincipal `
        -Description 'Q-TRON external dead-man watchdog (reads heartbeat + completion marker, alerts via Telegram DEADMAN channel)'

    Write-Host "`nInstalled. First run should trigger within $IntervalMinutes min."
    Write-Host "Before first alert fires, verify these env vars are set for your user account:"
    Write-Host "  QTRON_TELEGRAM_TOKEN_DEADMAN"
    Write-Host "  QTRON_TELEGRAM_CHAT_ID_DEADMAN"
    Write-Host "`nUser-level vars example (PowerShell):"
    Write-Host "  [Environment]::SetEnvironmentVariable('QTRON_TELEGRAM_TOKEN_DEADMAN', '<token>', 'User')"
    Write-Host "  [Environment]::SetEnvironmentVariable('QTRON_TELEGRAM_CHAT_ID_DEADMAN', '<chat_id>', 'User')"
}
elseif ($Action -eq 'uninstall') {
    $existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "Unregistered '$TaskName'"
    } else {
        Write-Host "Task '$TaskName' not registered — nothing to do"
    }
}
elseif ($Action -eq 'status') {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($task) {
        $info = Get-ScheduledTaskInfo -TaskName $TaskName
        Write-Host "Task: $TaskName"
        Write-Host "  State: $($task.State)"
        Write-Host "  LastRunTime: $($info.LastRunTime)"
        Write-Host "  LastTaskResult: $($info.LastTaskResult)"
        Write-Host "  NextRunTime: $($info.NextRunTime)"
        Write-Host "  NumberOfMissedRuns: $($info.NumberOfMissedRuns)"
    } else {
        Write-Host "Task '$TaskName' not installed"
    }
}
