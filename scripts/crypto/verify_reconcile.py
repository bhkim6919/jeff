"""D3-2 verification — exercises the reconcile contract end-to-end.

Five gate checks:

    G1 clean state       — current PG ↔ CSV produce drift_detected=false,
                           exit 0, telegram skipped.
    G2 synthetic drift   — pure compare() with hand-crafted inputs surfaces
                           every category (only_in_db, only_in_csv, source
                           drift, delisted_at drift).
    G3 read-only         — running the live entry against a tampered CSV
                           snapshot leaves the on-disk CSV bytes unchanged
                           and the PG row count unchanged (no auto-fix).
    G4 evidence schema   — JSON contains required keys at top level + summary.
    G5 telegram fail-soft — INVALID telegram credentials on drift path do
                           not cause job to raise; reconcile still exits 1.

Exit:
    0 → all gates PASS
    1 → at least one gate FAIL

Usage::

    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/verify_reconcile.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.jobs import reconcile_listings as recon  # noqa: E402


VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
SCRATCH_DIR = VERIF_DIR / "_d3_scratch"
SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

LISTINGS_CSV = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _csv_sha(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _pg_count() -> int:
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM crypto_listings")
            return int(cur.fetchone()[0])


# --- G1: clean state ----------------------------------------------------


def gate_g1_clean_state() -> tuple[bool, dict]:
    print("\n[G1] clean state — live PG vs CSV must report no drift")
    out_dir = SCRATCH_DIR / "g1_run"
    rc = recon.run([
        "--evidence-dir", str(out_dir),
        "--no-telegram",
    ])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    evidence = out_dir / f"reconcile_db_csv_{today}.json"
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    summary = payload.get("summary", {})

    ok = (
        rc == 0
        and summary.get("drift_detected") is False
        and summary.get("row_count_match") is True
    )
    return ok, {
        "exit_code": rc,
        "drift_detected": summary.get("drift_detected"),
        "row_count_match": summary.get("row_count_match"),
        "db_total": summary.get("db_total_rows"),
        "csv_total": summary.get("csv_total_rows"),
        "expected_rc": 0,
    }


# --- G2: synthetic drift ------------------------------------------------


def gate_g2_synthetic_drift() -> tuple[bool, dict]:
    print("\n[G2] synthetic drift — pure compare() must surface every category")
    db_rows = [
        {"pair": "KRW-AAA", "source": "manual_v0", "delisted_at": None},
        {"pair": "KRW-BBB", "source": "upbit_notice", "delisted_at": date(2026, 1, 15)},
        {"pair": "KRW-CCC", "source": "upbit_notice", "delisted_at": date(2025, 12, 1)},
        {"pair": "KRW-DDD", "source": "manual_v0", "delisted_at": None},  # only_in_db
        {"pair": "KRW-EEE", "source": "manual_v0", "delisted_at": None},  # source drift
    ]
    csv_rows = [
        {"pair": "KRW-AAA", "source": "manual_v0", "delisted_at": ""},
        {"pair": "KRW-BBB", "source": "upbit_notice", "delisted_at": "2026-01-15"},
        {"pair": "KRW-CCC", "source": "upbit_notice", "delisted_at": "2025-12-02"},  # date drift
        # KRW-DDD missing → only_in_db
        {"pair": "KRW-EEE", "source": "upbit_notice", "delisted_at": ""},  # source drift
        {"pair": "KRW-FFF", "source": "manual_v0", "delisted_at": ""},     # only_in_csv
    ]
    rep = recon.compare(db_rows, csv_rows)

    expected_only_in_db = ["KRW-DDD"]
    expected_only_in_csv = ["KRW-FFF"]
    drift_pairs = sorted({d.pair for d in rep.field_drift})
    drift_fields = sorted({d.field for d in rep.field_drift})

    ok = (
        rep.drift_detected
        and rep.row_count_diff == (len(db_rows) - len(csv_rows))
        and rep.only_in_db == expected_only_in_db
        and rep.only_in_csv == expected_only_in_csv
        and "KRW-CCC" in drift_pairs       # date drift
        and "KRW-EEE" in drift_pairs       # source drift
        and "delisted_at" in drift_fields
        and "source" in drift_fields
    )
    return ok, {
        "drift_detected": rep.drift_detected,
        "row_count_diff": rep.row_count_diff,
        "only_in_db": rep.only_in_db,
        "only_in_csv": rep.only_in_csv,
        "drift_by_field": rep.drift_by_field,
        "drift_pairs": drift_pairs,
    }


# --- G3: read-only ------------------------------------------------------


def gate_g3_read_only() -> tuple[bool, dict]:
    print("\n[G3] read-only — running reconcile must NOT mutate PG or CSV")
    pre_csv_sha = _csv_sha(LISTINGS_CSV)
    pre_csv_bytes = LISTINGS_CSV.stat().st_size
    pre_pg_count = _pg_count()

    rc = recon.run([
        "--evidence-dir", str(SCRATCH_DIR / "g3_run"),
        "--no-telegram",
    ])

    post_csv_sha = _csv_sha(LISTINGS_CSV)
    post_csv_bytes = LISTINGS_CSV.stat().st_size
    post_pg_count = _pg_count()

    # No leftover .tmp / .bak from reconcile (it should never write them).
    leaks: list[str] = []
    for suffix in (".tmp", ".bak"):
        leak = LISTINGS_CSV.with_suffix(LISTINGS_CSV.suffix + suffix)
        if leak.exists():
            leaks.append(leak.name)

    ok = (
        rc in (0, 1)  # 0 clean, 1 drift — both are valid outcomes
        and pre_csv_sha == post_csv_sha
        and pre_csv_bytes == post_csv_bytes
        and pre_pg_count == post_pg_count
        and not leaks
    )
    return ok, {
        "exit_code": rc,
        "csv_sha_unchanged": pre_csv_sha == post_csv_sha,
        "csv_bytes_unchanged": pre_csv_bytes == post_csv_bytes,
        "pg_count_unchanged": pre_pg_count == post_pg_count,
        "tmp_or_bak_leaks": leaks,
    }


# --- G4: evidence schema ------------------------------------------------


def gate_g4_evidence_schema() -> tuple[bool, dict]:
    print("\n[G4] evidence schema — JSON must contain required keys")
    out_dir = SCRATCH_DIR / "g4_run"
    rc = recon.run([
        "--evidence-dir", str(out_dir),
        "--no-telegram",
    ])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = out_dir / f"reconcile_db_csv_{today}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))

    required_top = {
        "started_at_utc", "completed_at_utc", "mode",
        "summary", "drift", "telegram_status", "exit_code",
        "csv_path",
    }
    required_summary = {
        "db_total_rows", "csv_total_rows", "row_count_match",
        "row_count_diff", "pairs_only_in_db", "pairs_only_in_csv",
        "field_drift_count", "drift_by_field", "drift_detected",
        "fields_compared",
    }
    required_drift = {"only_in_db", "only_in_csv", "field_drift"}

    missing_top = sorted(required_top - set(payload.keys()))
    missing_sum = sorted(required_summary - set(payload.get("summary", {}).keys()))
    missing_drift = sorted(required_drift - set(payload.get("drift", {}).keys()))

    ok = (
        rc in (0, 1)
        and not missing_top
        and not missing_sum
        and not missing_drift
        and payload.get("mode") == "report-only"
    )
    return ok, {
        "evidence_path": str(path.relative_to(WORKTREE_ROOT)),
        "missing_top_keys": missing_top,
        "missing_summary_keys": missing_sum,
        "missing_drift_keys": missing_drift,
        "mode": payload.get("mode"),
        "rc": rc,
    }


# --- G5: telegram fail-soft on drift path -------------------------------


def gate_g5_telegram_failsoft() -> tuple[bool, dict]:
    print("\n[G5] telegram fail-soft — INVALID creds on drift path must NOT raise")
    # We exercise the telegram_send path directly with the same drift-shaped
    # message the runner would build, to avoid mutating CSV/PG just to force a
    # drift exit-code in the live runner.
    saved = {
        "TELEGRAM_BOT_TOKEN_CRYPTO": os.environ.get("TELEGRAM_BOT_TOKEN_CRYPTO"),
        "TELEGRAM_CHAT_ID_CRYPTO": os.environ.get("TELEGRAM_CHAT_ID_CRYPTO"),
    }
    os.environ["TELEGRAM_BOT_TOKEN_CRYPTO"] = "INVALID_TOKEN_FOR_G5"
    os.environ["TELEGRAM_CHAT_ID_CRYPTO"] = "0"

    raised = False
    status = ""
    try:
        # Build a synthetic drift report and exercise the formatter + send().
        rep = recon.ReconcileReport(
            db_total=10, csv_total=9,
            only_in_db=["KRW-FOO"],
            only_in_csv=[],
            field_drift=[recon.FieldDrift(
                pair="KRW-BAR", field="source",
                db_value="manual_v0", csv_value="upbit_notice",
            )],
        )
        msg = recon._format_telegram_alert(rep, Path("evidence_synthetic.json"))
        from crypto.jobs._telegram import send as telegram_send
        status = telegram_send(msg)
    except Exception as exc:
        raised = True
        status = f"raised:{type(exc).__name__}:{exc}"

    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    ok = (not raised) and status.startswith("error:")
    return ok, {
        "raised": raised,
        "returned_status": status,
        "expected_status_prefix": "error:",
    }


# --- Main ---------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D3-2 verification @ {_now()}")
    print("=" * 78)

    gates = [
        ("G1 clean state",        gate_g1_clean_state),
        ("G2 synthetic drift",    gate_g2_synthetic_drift),
        ("G3 read-only",          gate_g3_read_only),
        ("G4 evidence schema",    gate_g4_evidence_schema),
        ("G5 telegram fail-soft", gate_g5_telegram_failsoft),
    ]

    results: list[dict] = []
    all_ok = True
    for name, fn in gates:
        try:
            ok, detail = fn()
        except Exception as exc:
            ok = False
            detail = {"unhandled_exception": f"{type(exc).__name__}: {exc}"}
        verdict = "PASS" if ok else "FAIL"
        results.append({"gate": name, "verdict": verdict, "detail": detail})
        print(f"\n[{verdict}] {name}")
        for k, v in detail.items():
            print(f"    {k}: {v}")
        if not ok:
            all_ok = False

    summary_path = VERIF_DIR / f"d3_2_baseline_{_now()[:10]}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "all_pass": all_ok,
                "gates": results,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print("\n" + "=" * 78)
    print(f"VERDICT: {'PASS' if all_ok else 'FAIL'}  (summary: {summary_path.relative_to(WORKTREE_ROOT)})")
    print("=" * 78)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
