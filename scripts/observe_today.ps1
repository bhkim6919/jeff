# scripts/observe_today.ps1 — Quick daily operational snapshot.
#
# Usage:
#   .\scripts\observe_today.ps1              # today (KST)
#   .\scripts\observe_today.ps1 -Date 20260422   # explicit YYYYMMDD
#
# Shows:
#   - Heartbeat state (age + tick_seq)
#   - Completion marker: per-run_type status + attempt + error
#   - Today's incident files
#   - DEADMAN alert eligibility summary

param(
    [string]$Date = (Get-Date -Format 'yyyyMMdd')
)

$pipelineDir = Join-Path $PSScriptRoot '..\kr\data\pipeline'
$incidentDir = Join-Path $PSScriptRoot '..\backup\reports\incidents'

Write-Host "=== Q-TRON Observation Snapshot ($Date) ===" -ForegroundColor Cyan
Write-Host ""

# ---- Heartbeat ----
$hbPath = Join-Path $pipelineDir 'heartbeat.json'
if (Test-Path $hbPath) {
    try {
        $hb = Get-Content $hbPath -Encoding UTF8 | ConvertFrom-Json
        $hbTs = [DateTime]::Parse($hb.ts)
        $ageSec = [int]((Get-Date).ToUniversalTime() - $hbTs.ToUniversalTime()).TotalSeconds
        $color = if ($ageSec -gt 120) { 'Red' } elseif ($ageSec -gt 60) { 'Yellow' } else { 'Green' }
        Write-Host "Heartbeat:" -NoNewline
        Write-Host " age=${ageSec}s tick_seq=$($hb.tick_seq) pid=$($hb.pid)" -ForegroundColor $color
    } catch {
        Write-Host "Heartbeat: PARSE_ERROR — $_" -ForegroundColor Red
    }
} else {
    Write-Host "Heartbeat: MISSING (tray not running?)" -ForegroundColor Red
}

# ---- Completion marker ----
$markerPath = Join-Path $pipelineDir "run_completion_$Date.json"
Write-Host ""
if (Test-Path $markerPath) {
    $m = Get-Content $markerPath -Encoding UTF8 | ConvertFrom-Json
    Write-Host "Marker (last_update=$($m.last_update)):" -ForegroundColor Cyan
    if ($m.runs) {
        $m.runs.PSObject.Properties | ForEach-Object {
            $rt = $_.Name
            $r = $_.Value
            $statusColor = switch ($r.status) {
                'SUCCESS'                 { 'Green' }
                'RUNNING'                 { 'Yellow' }
                'MISSING'                 { 'Yellow' }
                'PARTIAL'                 { 'Yellow' }
                'FAILED'                  { 'Red' }
                'PRE_FLIGHT_FAIL'         { 'Red' }
                'PRE_FLIGHT_STALE_INPUT'  { 'Red' }
                default                   { 'White' }
            }
            Write-Host ("  {0,-10} status=" -f $rt) -NoNewline
            Write-Host ("{0,-22}" -f $r.status) -ForegroundColor $statusColor -NoNewline
            Write-Host " attempt=$($r.attempt_no) worst=$($r.worst_status_today)"
            if ($r.error) {
                $msg = $r.error.message
                if ($msg.Length -gt 200) { $msg = $msg.Substring(0, 200) + '...' }
                Write-Host "    err[$($r.error.stage)]: $msg" -ForegroundColor DarkRed
            }
            # Checks summary (any False)
            if ($r.checks) {
                $falseChecks = @()
                $r.checks.PSObject.Properties | ForEach-Object {
                    if ($_.Value -eq $false) { $falseChecks += $_.Name }
                }
                if ($falseChecks.Count -gt 0) {
                    Write-Host "    failed_checks: $($falseChecks -join ', ')" -ForegroundColor DarkRed
                }
            }
        }
    } else {
        Write-Host "  (no runs recorded yet)"
    }
    if ($m.known_bombs -and $m.known_bombs.Count -gt 0) {
        Write-Host ""
        Write-Host "Known bombs (blocking SUCCESS):" -ForegroundColor Red
        $m.known_bombs | ForEach-Object {
            Write-Host "  $($_.module) state=$($_.state) since=$($_.detected_since)"
        }
    }
} else {
    Write-Host "Marker: NOT_PRESENT (no runs yet or different date)" -ForegroundColor Yellow
}

# ---- Today's incidents ----
Write-Host ""
if (Test-Path $incidentDir) {
    $incFiles = Get-ChildItem $incidentDir -Filter "$Date*.md" -ErrorAction SilentlyContinue | Sort-Object Name
    if ($incFiles.Count -gt 0) {
        Write-Host "Incidents ($($incFiles.Count)):" -ForegroundColor Yellow
        $incFiles | ForEach-Object {
            Write-Host "  $($_.Name) ($($_.Length) bytes)"
        }
    } else {
        Write-Host "Incidents: 0 today" -ForegroundColor Green
    }
} else {
    Write-Host "Incidents: dir not found"
}

# ---- DEADMAN env check ----
Write-Host ""
$tok = [Environment]::GetEnvironmentVariable('QTRON_TELEGRAM_TOKEN_DEADMAN', 'User')
$cid = [Environment]::GetEnvironmentVariable('QTRON_TELEGRAM_CHAT_ID_DEADMAN', 'User')
$dm = if ($tok -and $cid) { 'configured' } else { 'MISSING' }
$dmColor = if ($dm -eq 'configured') { 'Green' } else { 'Red' }
Write-Host "DEADMAN: $dm" -ForegroundColor $dmColor

# ---- Task Scheduler status ----
$task = Get-ScheduledTask -TaskName 'QTronExternalWatchdog' -ErrorAction SilentlyContinue
if ($task) {
    $info = Get-ScheduledTaskInfo -TaskName 'QTronExternalWatchdog'
    Write-Host "Watchdog task: $($task.State) (last=$($info.LastRunTime), next=$($info.NextRunTime))" -ForegroundColor Green
} else {
    Write-Host "Watchdog task: NOT_INSTALLED" -ForegroundColor Red
}
