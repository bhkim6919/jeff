# Install / reinstall the Crypto Lab D3-2 daily DB↔CSV reconcile task.
#
# Mirrors install_incremental_listings_task.ps1 — see that file for design
# notes (Register-ScheduledTask -Force, ScheduledTasks module).
#
# Usage:
#   .\scripts\crypto\scheduler\install_reconcile_db_csv_task.ps1
#   .\scripts\crypto\scheduler\install_reconcile_db_csv_task.ps1 -Uninstall
#   .\scripts\crypto\scheduler\install_reconcile_db_csv_task.ps1 -RunOnDemand

[CmdletBinding()]
param(
    [switch]$Uninstall,
    [switch]$RunOnDemand
)

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$XmlPath   = Join-Path $ScriptDir 'reconcile_db_csv_task.xml'
$TaskName  = 'Q-TRON\crypto-reconcile-db-csv'

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\' -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\' -Confirm:$false
        Write-Host "[uninstalled] $TaskName"
    } else {
        Write-Host "[skip] $TaskName not found"
    }
    return
}

if (-not (Test-Path $XmlPath)) {
    throw "Task XML not found: $XmlPath"
}

$ExpectedPython = 'C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe'
if (-not (Test-Path $ExpectedPython)) {
    throw "Python venv missing: $ExpectedPython (edit the XML if your venv path differs)"
}

$ExpectedWorktree = 'C:\Q-TRON-32_ARCHIVE-crypto-d1'
if (-not (Test-Path $ExpectedWorktree)) {
    throw "Worktree missing: $ExpectedWorktree"
}

$XmlContent = Get-Content -Path $XmlPath -Raw -Encoding Unicode
Register-ScheduledTask -Xml $XmlContent -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\' -Force | Out-Null
Write-Host "[installed] $TaskName"

$Task = Get-ScheduledTask -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\'
$Info = Get-ScheduledTaskInfo -InputObject $Task
Write-Host "  next run    : $($Info.NextRunTime)"
Write-Host "  last run    : $($Info.LastRunTime)"
Write-Host "  last result : 0x$('{0:X}' -f $Info.LastTaskResult)"

if ($RunOnDemand) {
    Write-Host '[run-on-demand] starting task …'
    Start-ScheduledTask -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\'
    Start-Sleep -Seconds 2
    $Info = Get-ScheduledTaskInfo -InputObject (Get-ScheduledTask -TaskName 'crypto-reconcile-db-csv' -TaskPath '\Q-TRON\')
    Write-Host "  state       : $($Info.LastRunTime) result=0x$('{0:X}' -f $Info.LastTaskResult)"
}
