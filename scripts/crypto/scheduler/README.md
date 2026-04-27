# Crypto Lab — Windows Task Scheduler entries

D3-1 (incremental listings) and future recurring jobs install here.

## Active tasks

| Task | Schedule (KST) | Schedule (UTC) | Job module |
|---|---|---|---|
| `\Q-TRON\crypto-incremental-listings` | daily 09:30 | daily 00:30 | `crypto.jobs.incremental_listings` |

## Install

```powershell
# from worktree root
.\scripts\crypto\scheduler\install_incremental_listings_task.ps1
```

The script validates that `.venv64` and the worktree exist before registering,
and uses `Register-ScheduledTask -Force` so reinstalling replaces the existing
definition without prompting.

## Smoke test (run on demand)

```powershell
.\scripts\crypto\scheduler\install_incremental_listings_task.ps1 -RunOnDemand
```

After it fires, evidence is written to:

```
crypto/data/_verification/incremental_listings_<utc-date>.json
```

Telegram notification fires on net change (best-effort — see `crypto/jobs/_telegram.py`).

## Uninstall

```powershell
.\scripts\crypto\scheduler\install_incremental_listings_task.ps1 -Uninstall
```

## Why Windows Task Scheduler

Per Jeff D3 Q1 (2026-04-28): KR/US already use Windows Task Scheduler for
recurring orchestration (e.g. `eod_scheduler.py`, watchdog tasks). Adding a
second runner (APScheduler / cron daemon) for crypto would complicate ops
without benefit at this scale (one job, daily fire).

## Why 09:30 KST

Upbit posts new delisting notices during KST business hours. The notice API
day rollover happens at KST 00:00 = UTC 15:00. Firing at KST 09:30 (= UTC
00:30) gives Upbit's editors 30 minutes after KST 09:00 to publish the day's
notices and runs well before the KR market opens.

## Failure handling

| Failure | Behavior |
|---|---|
| Lock contention (concurrent run) | exit 2, no writes, Telegram notify (best-effort) |
| Crawl error rate > events | exit 1, no writes, Telegram notify |
| PG transaction failure | exit 1, no writes (rollback), CSV untouched, Telegram notify |
| CSV write fails after PG commit | exit 1, CSV restored from `.bak`, PG holds the new state, Telegram notify |
| Telegram down | logger.warning, job exit code unaffected |

The job's evidence JSON is written even on fatal error — open the file at
`crypto/data/_verification/incremental_listings_<utc-date>.json` first when
diagnosing a failure.
