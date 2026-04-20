#!/usr/bin/env python3
"""
Q-TRON Daily Backup — PostgreSQL + State + Reports (SQLite removed)
============================================================
Usage:
    python daily_backup.py              # Run backup
    python daily_backup.py --restore    # Restore test (separate DB)

Designed to run from tray_server.py at 17:00 KST.
"""
import logging
import os
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

# ── Config ──
BASE_DIR = Path(__file__).resolve().parent.parent
BACKUP_DIR = Path(__file__).resolve().parent
PG_DUMP = Path("C:/Program Files/PostgreSQL/15/bin/pg_dump.exe")
PG_RESTORE = Path("C:/Program Files/PostgreSQL/15/bin/pg_restore.exe")
PSQL = Path("C:/Program Files/PostgreSQL/15/bin/psql.exe")

# INT-P0-001: credentials must come from environment (kr/.env). No hardcoded fallback.
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(BASE_DIR / "kr" / ".env")
except Exception:
    pass

DB_NAME = os.getenv("DB_NAME", "qtron")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD")
if not DB_PASS:
    raise RuntimeError(
        "[DB_CONFIG_MISSING] env var 'DB_PASSWORD' not set. "
        "Set in kr/.env (see INT-P0-001).")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

KR_STATE_DIR = BASE_DIR / "kr" / "state"
US_STATE_DIR = BASE_DIR / "us" / "state"
KR_SQLITE_DIR = BASE_DIR / "kr" / "data"
KR_REPORT_DIR = BASE_DIR / "kr" / "report" / "output"

RETENTION_DAYS = 90

logger = logging.getLogger("qtron.backup")


def _setup_logging():
    log_dir = BACKUP_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    handler = logging.FileHandler(log_dir / f"backup_{today}.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)


def _send_telegram(text, severity="INFO"):
    """Best-effort Telegram notification."""
    try:
        sys.path.insert(0, str(BASE_DIR / "kr"))
        from notify.telegram_bot import send
        send(text, severity)
    except Exception:
        try:
            sys.path.insert(0, str(BASE_DIR / "us"))
            from notify.telegram_bot import send as send_us
            send_us(text, severity)
        except Exception:
            pass


def run_backup():
    """Execute full daily backup. Returns (ok, summary)."""
    today = date.today().strftime("%Y%m%d")
    ts_start = datetime.now()
    results = {}

    # ── 1. PostgreSQL dump ──
    dump_dir = BACKUP_DIR / "db"
    dump_dir.mkdir(exist_ok=True)
    dump_file = dump_dir / f"qtron_{today}.dump"

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASS

    try:
        cmd = [
            str(PG_DUMP),
            "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
            "-d", DB_NAME, "-F", "c", "-f", str(dump_file),
        ]
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=300)
        if r.returncode == 0 and dump_file.exists():
            size_mb = dump_file.stat().st_size / (1024 * 1024)
            results["pg_dump"] = f"OK ({size_mb:.1f} MB)"
            logger.info(f"[BACKUP_PG] OK: {dump_file.name} ({size_mb:.1f} MB)")
        else:
            results["pg_dump"] = f"FAIL: {r.stderr[:200]}"
            logger.error(f"[BACKUP_PG] FAIL: {r.stderr[:200]}")
    except Exception as e:
        results["pg_dump"] = f"ERROR: {e}"
        logger.error(f"[BACKUP_PG] ERROR: {e}")

    # ── 2. State files (KR) ──
    state_kr_dir = BACKUP_DIR / "state_kr"
    state_kr_dir.mkdir(exist_ok=True)
    try:
        copied = 0
        if KR_STATE_DIR.exists():
            for f in KR_STATE_DIR.glob("*.json"):
                shutil.copy2(f, state_kr_dir / f"{today}_{f.name}")
                copied += 1
        results["state_kr"] = f"OK ({copied} files)"
        logger.info(f"[BACKUP_STATE_KR] OK: {copied} files")
    except Exception as e:
        results["state_kr"] = f"ERROR: {e}"
        logger.error(f"[BACKUP_STATE_KR] ERROR: {e}")

    # ── 3. State files (US) ──
    state_us_dir = BACKUP_DIR / "state_us"
    state_us_dir.mkdir(exist_ok=True)
    try:
        copied = 0
        if US_STATE_DIR.exists():
            for f in US_STATE_DIR.glob("*.json"):
                shutil.copy2(f, state_us_dir / f"{today}_{f.name}")
                copied += 1
        results["state_us"] = f"OK ({copied} files)"
        logger.info(f"[BACKUP_STATE_US] OK: {copied} files")
    except Exception as e:
        results["state_us"] = f"ERROR: {e}"
        logger.error(f"[BACKUP_STATE_US] ERROR: {e}")

    # ── 4. SQLite DBs — REMOVED (migrated to PostgreSQL) ──
    # All data now in PostgreSQL. pg_dump covers everything.
    # .db files archived to backup/sqlite_archive/
    results["sqlite"] = "SKIPPED (migrated to PG)"
    logger.info("[BACKUP_SQLITE] SKIPPED — all data in PostgreSQL, covered by pg_dump")

    # ── 5. Report CSVs ──
    reports_dir = BACKUP_DIR / "reports"
    reports_dir.mkdir(exist_ok=True)
    try:
        copied = 0
        if KR_REPORT_DIR.exists():
            for f in KR_REPORT_DIR.glob("*.csv"):
                shutil.copy2(f, reports_dir / f"{today}_{f.name}")
                copied += 1
        results["reports"] = f"OK ({copied} files)"
        logger.info(f"[BACKUP_REPORTS] OK: {copied} files")
    except Exception as e:
        results["reports"] = f"ERROR: {e}"
        logger.error(f"[BACKUP_REPORTS] ERROR: {e}")

    # ── 6. Cleanup old backups ──
    try:
        import time
        cutoff = time.time() - RETENTION_DAYS * 86400
        removed = 0
        for sub in ["db", "state_kr", "state_us", "sqlite", "reports"]:
            d = BACKUP_DIR / sub
            if not d.exists():
                continue
            for f in d.iterdir():
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
        results["cleanup"] = f"OK (removed {removed} old files)"
        logger.info(f"[BACKUP_CLEANUP] OK: {removed} old files removed")
    except Exception as e:
        results["cleanup"] = f"ERROR: {e}"

    # ── Summary ──
    # "SKIPPED ..." 는 의도적 skip (예: sqlite PG 마이그레이션 후)이므로 FAIL 아님.
    # FAIL 판정은 실제 실패("ERROR", "FAIL", 빈값)만 대상으로 한다.
    elapsed = (datetime.now() - ts_start).total_seconds()

    def _is_success(v: str) -> bool:
        v = str(v or "")
        return ("OK" in v) or v.startswith("SKIPPED")

    all_ok = all(_is_success(v) for v in results.values())
    failed = [k for k, v in results.items() if not _is_success(v)]

    if all_ok:
        summary = f"[BACKUP_OK] {today} completed in {elapsed:.0f}s"
        for k, v in results.items():
            summary += f"\n  {k}: {v}"
        logger.info(summary)
        _send_telegram(f"<b>Backup OK</b>\n{today} ({elapsed:.0f}s)\n" +
                       "\n".join(f"  {k}: {v}" for k, v in results.items()), "INFO")
    else:
        summary = f"[BACKUP_FAIL] {today} — failed: {', '.join(failed)}"
        for k, v in results.items():
            summary += f"\n  {k}: {v}"
        logger.error(summary)
        _send_telegram(f"<b>BACKUP FAIL</b>\n{today}\n" +
                       "\n".join(f"  {k}: {v}" for k, v in results.items()), "CRITICAL")

    return all_ok, summary


def run_restore_test():
    """Restore test to a temporary database. Returns (ok, summary)."""
    today = date.today().strftime("%Y%m%d")
    dump_file = BACKUP_DIR / "db" / f"qtron_{today}.dump"

    if not dump_file.exists():
        # Try latest
        dumps = sorted((BACKUP_DIR / "db").glob("qtron_*.dump"))
        if not dumps:
            msg = "[RESTORE_TEST] No dump files found"
            logger.error(msg)
            return False, msg
        dump_file = dumps[-1]

    test_db = "qtron_restore_test"
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASS

    try:
        # Drop test DB if exists
        subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-c", f"DROP DATABASE IF EXISTS {test_db};"],
            env=env, capture_output=True, timeout=30)

        # Create test DB
        r = subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-c", f"CREATE DATABASE {test_db};"],
            env=env, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return False, f"[RESTORE_TEST] CREATE DB failed: {r.stderr[:200]}"

        # Restore
        r = subprocess.run(
            [str(PG_RESTORE), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-d", test_db, str(dump_file)],
            env=env, capture_output=True, text=True, timeout=300)

        # Verify table count
        r2 = subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-d", test_db, "-t", "-c",
             "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public';"],
            env=env, capture_output=True, text=True, timeout=30)
        table_count = r2.stdout.strip() if r2.returncode == 0 else "?"

        # Verify row counts for critical tables
        r3 = subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-d", test_db, "-t", "-c",
             "SELECT 'ohlcv=' || COUNT(*) FROM ohlcv "
             "UNION ALL SELECT 'ohlcv_us=' || COUNT(*) FROM ohlcv_us;"],
            env=env, capture_output=True, text=True, timeout=30)
        row_info = r3.stdout.strip().replace("\n", ", ") if r3.returncode == 0 else "?"

        # Drop test DB
        subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-c", f"DROP DATABASE {test_db};"],
            env=env, capture_output=True, timeout=30)

        msg = (f"[RESTORE_TEST] OK - dump={dump_file.name}, "
               f"tables={table_count}, {row_info}")
        logger.info(msg)
        _send_telegram(f"<b>Restore Test OK</b>\n{dump_file.name}\n"
                       f"Tables: {table_count}\n{row_info}", "INFO")
        return True, msg

    except Exception as e:
        # Cleanup on failure
        subprocess.run(
            [str(PSQL), "-U", DB_USER, "-h", DB_HOST, "-p", DB_PORT,
             "-c", f"DROP DATABASE IF EXISTS {test_db};"],
            env=env, capture_output=True, timeout=30)
        msg = f"[RESTORE_TEST] ERROR: {e}"
        logger.error(msg)
        return False, msg


if __name__ == "__main__":
    _setup_logging()

    if "--restore" in sys.argv:
        ok, summary = run_restore_test()
    else:
        ok, summary = run_backup()

    print(summary)
    sys.exit(0 if ok else 1)
