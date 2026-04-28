# Install both Crypto Lab D3 daily tasks (incremental + reconcile).
#
# Per Jeff's D3-3 safety order (2026-04-28), do NOT call this until the manual
# smoke checks pass:
#
#   1. Run incremental once manually:
#        python -X utf8 scripts/crypto/run_incremental_listings.py --max-pages 1
#      → confirm exit 0 + evidence at
#        crypto/data/_verification/incremental_listings_<utc-date>.json
#
#   2. Run reconcile once manually:
#        python -X utf8 scripts/crypto/reconcile_db_csv.py --no-telegram
#      → confirm exit 0 + drift_detected=false in evidence
#
#   3. Only then run THIS script. It registers both tasks back-to-back so
#      the daily order is incremental (00:30 UTC) → reconcile (00:40 UTC).
#
# Usage:
#   .\scripts\crypto\scheduler\install_all_tasks.ps1                   # install both
#   .\scripts\crypto\scheduler\install_all_tasks.ps1 -RunOnDemand      # install + smoke fire each
#   .\scripts\crypto\scheduler\install_all_tasks.ps1 -Uninstall        # remove both

[CmdletBinding()]
param(
    [switch]$Uninstall,
    [switch]$RunOnDemand
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

$Common = @{}
if ($Uninstall)   { $Common['Uninstall']   = $true }
if ($RunOnDemand) { $Common['RunOnDemand'] = $true }

Write-Host "==[1/2] crypto-incremental-listings ==" -ForegroundColor Cyan
& (Join-Path $ScriptDir 'install_incremental_listings_task.ps1') @Common

Write-Host ""
Write-Host "==[2/2] crypto-reconcile-db-csv ==" -ForegroundColor Cyan
& (Join-Path $ScriptDir 'install_reconcile_db_csv_task.ps1') @Common

Write-Host ""
Write-Host "==summary==" -ForegroundColor Green
Get-ScheduledTask -TaskPath '\Q-TRON\' -ErrorAction SilentlyContinue |
    Where-Object { $_.TaskName -in 'crypto-incremental-listings', 'crypto-reconcile-db-csv' } |
    ForEach-Object {
        $Info = Get-ScheduledTaskInfo -InputObject $_
        '{0,-30}  next={1}  last_result=0x{2:X}' -f $_.TaskName, $Info.NextRunTime, $Info.LastTaskResult
    }
