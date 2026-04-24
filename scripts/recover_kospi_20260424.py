"""
recover_kospi_20260424.py — One-off recovery for KOSPI CSV/DB divergence on 2026-04-24.

Incident:
    - inject_kospi_close appended a degraded row (O=H=L=C=6481.33, vol=0) to
      KOSPI.csv at 15:30:01 live session.
    - Subsequent injections (6475.68, 6475.63) did NOT overwrite the CSV row.
    - Batch _update_kospi_index saw CSV last_date=2026-04-24 and skipped
      DB upsert entirely → DB last_date remains 2026-04-23 → chart broken.

Recovery:
    - Replace CSV 2026-04-24 row with authoritative close 6475.63.
    - Upsert same value to DB kospi_index.
    - Write a marker file noting source=manual_recovery.

Run:
    cd C:/Q-TRON-32_ARCHIVE
    .venv64/Scripts/python.exe scripts/recover_kospi_20260424.py

Idempotent: safe to rerun (upsert semantics).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "kr"))

CSV_PATH = ROOT / "backtest" / "data_full" / "index" / "KOSPI.csv"
TARGET_DATE = "2026-04-24"
TARGET_CLOSE = 6475.63
MARKER_DIR = ROOT / "backup" / "reports" / "incidents"
MARKER_FILE = MARKER_DIR / "kospi_recovery_20260424.json"


def recover_csv() -> dict:
    if not CSV_PATH.exists():
        return {"status": "FAIL", "reason": "csv_missing", "path": str(CSV_PATH)}

    df = pd.read_csv(CSV_PATH)
    date_col = "index" if "index" in df.columns else "date"
    df[date_col] = df[date_col].astype(str)

    before_rows = len(df)
    before_target_rows = int((df[date_col] == TARGET_DATE).sum())
    df = df[df[date_col] != TARGET_DATE]

    row = {c: 0 for c in df.columns}
    row[date_col] = TARGET_DATE
    for c in df.columns:
        if c.lower() in ("open", "high", "low", "close"):
            row[c] = round(TARGET_CLOSE, 2)
        elif c.lower() == "volume":
            row[c] = 0

    new_df = pd.DataFrame([row], columns=df.columns)
    df = pd.concat([df, new_df], ignore_index=True)
    df = df.drop_duplicates(subset=[date_col], keep="last")
    df = df.sort_values(date_col).reset_index(drop=True)

    tmp = CSV_PATH.with_suffix(CSV_PATH.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, CSV_PATH)

    after_target_rows = int((df[date_col] == TARGET_DATE).sum())
    return {
        "status": "OK",
        "path": str(CSV_PATH),
        "rows_before": before_rows,
        "target_rows_before": before_target_rows,
        "rows_after": len(df),
        "target_rows_after": after_target_rows,
    }


def recover_db() -> dict:
    from data.db_provider import DbProvider
    push = pd.DataFrame([{
        "date": TARGET_DATE,
        "open": TARGET_CLOSE,
        "high": TARGET_CLOSE,
        "low": TARGET_CLOSE,
        "close": TARGET_CLOSE,
        "volume": 0,
    }])
    push["date"] = pd.to_datetime(push["date"])
    n = DbProvider().upsert_kospi_index(push)
    return {"status": "OK", "upserted": int(n)}


def verify_db() -> dict:
    from shared.db.pg_base import connection
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT date, close_price FROM kospi_index "
            "WHERE date = %s::date",
            (TARGET_DATE,),
        )
        row = cur.fetchone()
        cur.execute("SELECT MAX(date) FROM kospi_index")
        max_date = cur.fetchone()[0]
    return {
        "target_row": (str(row[0]), float(row[1])) if row else None,
        "max_date": str(max_date) if max_date else None,
    }


def write_marker(result: dict) -> None:
    MARKER_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "incident": "KOSPI CSV/DB divergence 2026-04-24",
        "recovered_at": datetime.utcnow().isoformat() + "Z",
        "target_date": TARGET_DATE,
        "target_close": TARGET_CLOSE,
        "source_quality": "manual_recovery",
        "reason": (
            "CSV append-only from inject_kospi_close left stale 6481.33 value; "
            "DB was never upserted for 2026-04-24 because batch skipped on "
            "CSV-only last_date check. O/H/L set equal to close as degraded "
            "fallback (no trusted OHLC snapshot for this incident)."
        ),
        "result": result,
    }
    MARKER_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                           encoding="utf-8")


def main() -> int:
    print(f"[RECOVER] CSV={CSV_PATH}")
    csv_res = recover_csv()
    print(f"[RECOVER.CSV] {csv_res}")

    db_res = recover_db()
    print(f"[RECOVER.DB]  {db_res}")

    verify = verify_db()
    print(f"[VERIFY.DB]   {verify}")

    write_marker({"csv": csv_res, "db": db_res, "verify": verify})
    print(f"[MARKER]      {MARKER_FILE}")

    ok = (
        csv_res.get("status") == "OK"
        and csv_res.get("target_rows_after") == 1
        and db_res.get("status") == "OK"
        and verify.get("target_row") is not None
        and abs(verify["target_row"][1] - TARGET_CLOSE) < 0.01
    )
    print(f"[RESULT] {'OK' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
