# Crypto Lab — Windows Task Scheduler entries

Two daily tasks back the Crypto Lab data layer (D3-1 + D3-2).

## Active tasks

| Task | Schedule (KST) | Schedule (UTC) | Job module | Writes? |
|---|---|---|---|---|
| `\Q-TRON\crypto-incremental-listings` | daily 09:30 | daily 00:30 | `crypto.jobs.incremental_listings` | yes (PG + CSV, atomic) |
| `\Q-TRON\crypto-reconcile-db-csv`     | daily 09:40 | daily 00:40 | `crypto.jobs.reconcile_listings` | **no** (read-only) |

The 10-minute gap is intentional: incremental fires first, then reconcile checks that the day's write didn't introduce drift between PG and CSV. **Order: incremental → reconcile**, never the other way.

## Safety order — manual smoke before scheduling (Jeff D3-3)

Do not register the tasks until both jobs run cleanly by hand. The `install_all_tasks.ps1` script intentionally has no auto-smoke; that's the operator's call.

```powershell
cd C:\Q-TRON-32_ARCHIVE-crypto-d1

# 1. Manual incremental — confirm exit 0 + evidence appears
& 'C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe' -X utf8 `
    .\scripts\crypto\run_incremental_listings.py --max-pages 1
Get-Content .\crypto\data\_verification\incremental_listings_*.json -Tail 50

# 2. Manual reconcile — confirm exit 0 + drift_detected=false
& 'C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe' -X utf8 `
    .\scripts\crypto\reconcile_db_csv.py --no-telegram
Get-Content .\crypto\data\_verification\reconcile_db_csv_*.json -Tail 30

# 3. Only after both look right:
.\scripts\crypto\scheduler\install_all_tasks.ps1
```

## Install (after smoke)

```powershell
# Both tasks at once (recommended)
.\scripts\crypto\scheduler\install_all_tasks.ps1

# Or each individually
.\scripts\crypto\scheduler\install_incremental_listings_task.ps1
.\scripts\crypto\scheduler\install_reconcile_db_csv_task.ps1
```

Each installer validates that `.venv64` and the worktree exist before registering, and uses `Register-ScheduledTask -Force` so reinstalling replaces the existing definition without prompting.

## On-demand smoke after install

```powershell
# Fire both tasks immediately (good first-day check)
.\scripts\crypto\scheduler\install_all_tasks.ps1 -RunOnDemand
```

After firing, evidence appears at:

```
crypto/data/_verification/incremental_listings_<utc-date>.json
crypto/data/_verification/reconcile_db_csv_<utc-date>.json
```

Telegram notification fires only on net change (incremental) or drift (reconcile) — clean runs are silent.

## Uninstall

```powershell
.\scripts\crypto\scheduler\install_all_tasks.ps1 -Uninstall
```

## Why Windows Task Scheduler

Per Jeff D3 Q1 (2026-04-28): KR/US already use Windows Task Scheduler for recurring orchestration. Adding a second runner (APScheduler / cron daemon) for crypto would complicate ops without benefit at this scale.

## Why 09:30 / 09:40 KST

- **09:30 KST (incremental)** — Upbit posts new delisting notices during KST business hours. The notice API day rollover happens at KST 00:00 = UTC 15:00. Firing at KST 09:30 gives Upbit's editors 30 minutes after KST 09:00 to publish the day's notices and runs well before the KR market opens.
- **09:40 KST (reconcile)** — 10 minutes after incremental ensures the latest PG/CSV write has settled. Reconcile is pure read; the gap is just for clarity in evidence timestamps and Telegram correlation.

## Failure handling

### Incremental (writer)

| Failure | Behavior |
|---|---|
| Lock contention (concurrent run) | exit 2, no writes, Telegram notify (best-effort) |
| Crawl error rate > events | exit 1, no writes, Telegram notify |
| PG transaction failure | exit 1, no writes (rollback), CSV untouched, Telegram notify |
| CSV write fails after PG commit | exit 1, CSV restored from `.bak`, PG holds new state, Telegram notify |
| Telegram down | `logger.warning`, job exit code unaffected |

### Reconcile (reader)

| Outcome | Exit | Action |
|---|---|---|
| CLEAN (parity perfect) | 0 | logger INFO, telegram skipped |
| DRIFT (mismatch found) | 1 | evidence written, logger WARNING, telegram (best-effort). **No auto-fix** — operator decides |
| ERROR (PG/CSV read fail) | 2 | evidence written with `fatal_error`, telegram notify |
| Telegram down | — | `logger.warning`, exit code unaffected |

The evidence JSON is written even on fatal error — open the file first when diagnosing.

## Related artifacts

- One-shot D3-3 backfill (pages 21~35): `scripts/crypto/backfill_old_delistings.py` — runs ad-hoc, NOT scheduled.
