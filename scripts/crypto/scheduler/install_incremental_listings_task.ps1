# Install / reinstall the Crypto Lab D3-1 daily incremental listings task.
#
# Usage (from any PowerShell, run as the user who will own the task):
#   .\scripts\crypto\scheduler\install_incremental_listings_task.ps1
#
# Optional:
#   -Uninstall            Remove the task only.
#   -RunOnDemand          Trigger the task immediately after install (smoke test).
#
# The task XML lives next to this script. Re-installing replaces the existing
# task definition (Register-ScheduledTask -Force).
#
# Reference: schtasks docs do not expose a clean "update from XML" path, so we
# use the PowerShell ScheduledTasks module which is built in to Windows 10+.

[CmdletBinding()]
param(
    [switch]$Uninstall,
    [switch]$RunOnDemand
)

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$XmlPath   = Join-Path $ScriptDir 'incremental_listings_task.xml'
$TaskName  = 'Q-TRON\crypto-incremental-listings'

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\' -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\' -Confirm:$false
        Write-Host "[uninstalled] $TaskName"
    } else {
        Write-Host "[skip] $TaskName not found"
    }
    return
}

if (-not (Test-Path $XmlPath)) {
    throw "Task XML not found: $XmlPath"
}

# Validate the venv path the XML points at exists, so install fails loudly
# rather than at first scheduled fire.
$ExpectedPython = 'C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe'
if (-not (Test-Path $ExpectedPython)) {
    throw "Python venv missing: $ExpectedPython (edit the XML if your venv path differs)"
}

$ExpectedWorktree = 'C:\Q-TRON-32_ARCHIVE-crypto-d1'
if (-not (Test-Path $ExpectedWorktree)) {
    throw "Worktree missing: $ExpectedWorktree"
}

$XmlContent = Get-Content -Path $XmlPath -Raw -Encoding Unicode

# Register-ScheduledTask -Xml requires the XML as a string. -Force replaces
# any existing task at the same path.
Register-ScheduledTask -Xml $XmlContent -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\' -Force | Out-Null
Write-Host "[installed] $TaskName"

# Show the resulting schedule for sanity.
$Task = Get-ScheduledTask -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\'
$Info = Get-ScheduledTaskInfo -InputObject $Task
Write-Host "  next run    : $($Info.NextRunTime)"
Write-Host "  last run    : $($Info.LastRunTime)"
Write-Host "  last result : 0x$('{0:X}' -f $Info.LastTaskResult)"

if ($RunOnDemand) {
    Write-Host '[run-on-demand] starting task …'
    Start-ScheduledTask -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\'
    Start-Sleep -Seconds 2
    $Info = Get-ScheduledTaskInfo -InputObject (Get-ScheduledTask -TaskName 'crypto-incremental-listings' -TaskPath '\Q-TRON\')
    Write-Host "  state       : $($Info.LastRunTime) result=0x$('{0:X}' -f $Info.LastTaskResult)"
}
