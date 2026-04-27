"""D3-1 verification — exercises the safety contract end-to-end.

Five gate checks (Jeff D3 보완 1~5):

    G1 idempotency       — second run with no upstream changes produces
                           zero PG row deltas and zero CSV byte deltas.
    G2 lockfile          — concurrent invocation while one holds the lock
                           exits with code 2; lock file is auto-cleaned on
                           normal exit.
    G3 partial-write     — when PG transaction fails, listings.csv is
                           untouched (no .tmp leak, no row addition).
    G4 drift report stub — evidence JSON contains baseline_before/after
                           and diff fields with the expected schema.
    G5 telegram fail-soft — job succeeds with bogus Telegram credentials.

Runs against the live PG instance (read-mostly; G3 uses an isolated
synthetic PG path with a forced exception, so no live data is affected).

Exit code:
    0 → all gates PASS
    1 → at least one gate FAIL

Usage::

    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/verify_incremental_listings.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.jobs._lockfile import FileLock, LockHeld  # noqa: E402
from crypto.jobs import incremental_listings as job  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
SCRATCH_DIR = VERIF_DIR / "_d3_scratch"
SCRATCH_DIR.mkdir(parents=True, exist_ok=True)

LISTINGS_CSV = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_pg_baseline() -> dict[str, int]:
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        return job._read_pg_baseline(conn)


def _csv_bytes(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _csv_sha(path: Path) -> str:
    import hashlib
    if not path.exists():
        return ""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


# --- G1: idempotency ----------------------------------------------------


def gate_g1_idempotency() -> tuple[bool, dict]:
    print("\n[G1] idempotency — running job twice, expecting zero deltas on the 2nd")
    # Run #1 (full path including writes). max-pages 1 keeps the API surface
    # small; whatever new events exist will be applied on this run.
    pre = _read_pg_baseline()
    pre_sha = _csv_sha(LISTINGS_CSV)
    print(f"  pre-baseline: {pre}")
    print(f"  pre-csv-sha:  {pre_sha[:16]}…")

    rc1 = job.run([
        "--max-pages", "1",
        "--evidence-dir", str(SCRATCH_DIR / "g1_run1"),
    ])
    if rc1 != 0:
        return False, {"phase": "run1", "rc": rc1}

    mid = _read_pg_baseline()
    mid_sha = _csv_sha(LISTINGS_CSV)
    print(f"  mid-baseline: {mid}")
    print(f"  mid-csv-sha:  {mid_sha[:16]}…")

    # Run #2 — no upstream change in <1s, expect zero diff.
    rc2 = job.run([
        "--max-pages", "1",
        "--evidence-dir", str(SCRATCH_DIR / "g1_run2"),
    ])
    if rc2 != 0:
        return False, {"phase": "run2", "rc": rc2}

    post = _read_pg_baseline()
    post_sha = _csv_sha(LISTINGS_CSV)
    print(f"  post-baseline: {post}")
    print(f"  post-csv-sha:  {post_sha[:16]}…")

    diff = {k: post[k] - mid[k] for k in mid}
    csv_changed = (post_sha != mid_sha)
    ok = all(v == 0 for v in diff.values()) and not csv_changed

    return ok, {
        "pre": pre,
        "mid_after_run1": mid,
        "post_after_run2": post,
        "diff_run2": diff,
        "csv_sha_unchanged": not csv_changed,
        "rc_run1": rc1,
        "rc_run2": rc2,
    }


# --- G2: lockfile -------------------------------------------------------


def gate_g2_lockfile() -> tuple[bool, dict]:
    print("\n[G2] lockfile — concurrent invocation must exit 2")
    lock_path = SCRATCH_DIR / "g2.lock"
    if lock_path.exists():
        lock_path.unlink()

    holder = FileLock(lock_path, owner="g2_test_holder")
    holder.acquire()
    try:
        contender = FileLock(lock_path, owner="g2_test_contender")
        try:
            contender.acquire()
            raised = False
        except LockHeld as exc:
            raised = True
            err_msg = str(exc)

        # Also exercise via job.run() with --lock-path pointing at same file.
        rc = job.run([
            "--max-pages", "1",
            "--lock-path", str(lock_path),
            "--evidence-dir", str(SCRATCH_DIR / "g2_run"),
            "--dry-run",
        ])
    finally:
        holder.release()

    # After holder release, file should be gone.
    file_cleaned = not lock_path.exists()

    ok = raised and rc == 2 and file_cleaned
    return ok, {
        "concurrent_acquire_raised_lockheld": raised,
        "concurrent_run_exit_code": rc,
        "lock_cleaned_after_release": file_cleaned,
        "expected_rc": 2,
    }


# --- G3: partial-write protection (PG-failure path) ---------------------


def gate_g3_partial_write() -> tuple[bool, dict]:
    print("\n[G3] partial-write — PG failure must leave CSV untouched")

    pre_sha = _csv_sha(LISTINGS_CSV)
    pre_bytes = _csv_bytes(LISTINGS_CSV)

    # Inject a PG-time failure by monkey-patching pg_apply_delistings to
    # raise AFTER reading baseline (so we hit the "PG transaction failed"
    # path inside _run_locked).
    import crypto.jobs.incremental_listings as ij

    original = ij.pg_apply_delistings

    def boom(conn, delistings):
        raise RuntimeError("synthetic PG failure for G3")

    ij.pg_apply_delistings = boom
    try:
        rc = ij.run([
            "--max-pages", "1",
            "--evidence-dir", str(SCRATCH_DIR / "g3_run"),
            "--lock-path", str(SCRATCH_DIR / "g3.lock"),
        ])
    finally:
        ij.pg_apply_delistings = original

    post_sha = _csv_sha(LISTINGS_CSV)
    post_bytes = _csv_bytes(LISTINGS_CSV)

    # No .tmp / .bak leftovers
    leaks: list[str] = []
    for suffix in (".tmp", ".bak"):
        leak = LISTINGS_CSV.with_suffix(LISTINGS_CSV.suffix + suffix)
        if leak.exists():
            leaks.append(leak.name)

    csv_unchanged = (pre_sha == post_sha) and (pre_bytes == post_bytes)
    ok = (rc == 1) and csv_unchanged and not leaks

    return ok, {
        "exit_code": rc,
        "csv_sha_unchanged": pre_sha == post_sha,
        "csv_bytes_unchanged": pre_bytes == post_bytes,
        "tmp_or_bak_leaks": leaks,
        "expected_rc": 1,
    }


# --- G4: drift report schema -------------------------------------------


def gate_g4_drift_report() -> tuple[bool, dict]:
    print("\n[G4] drift report — evidence JSON must include baseline + diff")
    out_dir = SCRATCH_DIR / "g4_run"
    rc = job.run([
        "--max-pages", "1",
        "--evidence-dir", str(out_dir),
        "--lock-path", str(SCRATCH_DIR / "g4.lock"),
        "--dry-run",
    ])
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    evidence_path = out_dir / f"incremental_listings_{today}.json"
    if not evidence_path.exists():
        return False, {"reason": f"evidence not written: {evidence_path}", "rc": rc}

    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    required_keys = {
        "started_at_utc", "completed_at_utc",
        "max_pages", "events_crawled",
        "baseline_before", "baseline_after", "diff",
        "csv_merge_stats", "pg_apply_stats",
        "idempotent", "telegram_status", "exit_code",
    }
    missing = sorted(required_keys - set(payload.keys()))

    baseline_keys = {
        "listings_total", "delisted_with_date",
        "source_upbit_notice", "source_manual_v0",
    }
    bb_missing = sorted(baseline_keys - set(payload.get("baseline_before", {})))

    ok = rc == 0 and not missing and not bb_missing
    return ok, {
        "evidence_path": str(evidence_path.relative_to(WORKTREE_ROOT)),
        "missing_top_keys": missing,
        "missing_baseline_keys": bb_missing,
        "rc": rc,
    }


# --- G5: telegram fail-soft ---------------------------------------------


def gate_g5_telegram_failsoft() -> tuple[bool, dict]:
    print("\n[G5] telegram — bogus credentials must NOT raise")
    from crypto.jobs._telegram import send as telegram_send

    saved_token = os.environ.get("TELEGRAM_BOT_TOKEN_CRYPTO")
    saved_chat = os.environ.get("TELEGRAM_CHAT_ID_CRYPTO")
    saved_token_fb = os.environ.get("TELEGRAM_BOT_TOKEN")
    saved_chat_fb = os.environ.get("TELEGRAM_CHAT_ID")

    os.environ["TELEGRAM_BOT_TOKEN_CRYPTO"] = "INVALID_TOKEN_FOR_G5"
    os.environ["TELEGRAM_CHAT_ID_CRYPTO"] = "0"

    raised = False
    status = ""
    try:
        status = telegram_send("D3-1 verify probe — should fail without raising")
    except Exception as exc:
        raised = True
        status = f"raised:{type(exc).__name__}:{exc}"

    # Restore env
    for key, val in [
        ("TELEGRAM_BOT_TOKEN_CRYPTO", saved_token),
        ("TELEGRAM_CHAT_ID_CRYPTO", saved_chat),
        ("TELEGRAM_BOT_TOKEN", saved_token_fb),
        ("TELEGRAM_CHAT_ID", saved_chat_fb),
    ]:
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val

    # Pass: send() returned an "error:..." string AND did not raise.
    ok = (not raised) and status.startswith("error:")
    return ok, {
        "raised": raised,
        "returned_status": status,
        "expected_status_prefix": "error:",
    }


# --- Main ---------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D3-1 verification @ {_now()}")
    print("=" * 78)

    gates = [
        ("G1 idempotency",           gate_g1_idempotency),
        ("G2 lockfile",              gate_g2_lockfile),
        ("G3 partial-write",         gate_g3_partial_write),
        ("G4 drift report",          gate_g4_drift_report),
        ("G5 telegram fail-soft",    gate_g5_telegram_failsoft),
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

    summary_path = VERIF_DIR / f"d3_baseline_{_now()[:10]}.json"
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
