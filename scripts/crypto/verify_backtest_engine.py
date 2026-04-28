"""D4 PR #2 (engine + Momentum 12-1) verification.

Two engine gates introduced here, plus a regression check against the
foundation gates from PR #1:

    G4  metrics — all 6 metrics produced for both NORMAL and STRESS runs;
                  values are finite (not NaN, not Inf), trades > 0,
                  exposure within 0..100.
    G6  idempotency — running run_dual twice with the same config
                      produces byte-identical canonical hashes
                      (NORMAL + STRESS).

The runs use a deliberately small window (2020-01-01 ~ 2020-06-30, top 20)
so a full dual-mode pair completes in <2 min. Full 8-year backtest is
the operator's job after PR #2 ships — this script just proves the engine
is sound. ``top_n=20`` is locked here to match Jeff E2=A and the CLI
default (``run_backtest.py --top-n 20``); changing one without the other
makes evidence non-comparable.

Exit:
    0 — both gates PASS + foundation regression PASS
    1 — at least one gate FAIL
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import CostConfig  # noqa: E402
from crypto.backtest.engine import BacktestConfig, run_dual  # noqa: E402
from crypto.backtest.strategies.momentum_12_1 import Momentum12_1  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_config() -> BacktestConfig:
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    universe = KRWStaticTop100.from_csv_and_listings(DEFAULT_TOP100_CSV, listings)
    return BacktestConfig(
        strategy=Momentum12_1(),
        universe=universe,
        start_date=date(2020, 1, 1),
        end_date=date(2020, 6, 30),
        initial_cash_krw=100_000_000.0,
        rebal_days=21,
        top_n=20,
        cost_config=CostConfig(),
    )


# --- G4 metrics ----------------------------------------------------------


_REQUIRED_METRICS = ("cagr", "mdd", "sharpe", "calmar", "trades", "exposure_pct")


def gate_g4_metrics() -> tuple[bool, dict]:
    print("\n[G4] metrics — 6 outputs for NORMAL + STRESS, finite, sane")

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    config = _build_config()
    payload = run_dual(config, connection_factory=connection)

    detail: dict = {
        "run_id": payload["run_id"],
        "rebal_executed_normal": payload["results"]["normal"]["rebal_executed_count"],
        "rebal_executed_stress": payload["results"]["stress"]["rebal_executed_count"],
    }

    issues: list[str] = []
    for mode in ("normal", "stress"):
        metrics = payload["results"][mode]["metrics"]
        for key in _REQUIRED_METRICS:
            if key not in metrics:
                issues.append(f"{mode}.{key} missing")
                continue
            v = metrics[key]
            if not isinstance(v, (int, float)) or math.isnan(v) or math.isinf(v):
                issues.append(f"{mode}.{key} non-finite ({v})")
        if metrics.get("trades", 0) <= 0:
            issues.append(f"{mode}.trades <= 0 (engine never traded)")
        exp = metrics.get("exposure_pct", -1)
        if not (0.0 <= exp <= 100.0):
            issues.append(f"{mode}.exposure_pct out of range ({exp})")
        # Sanity: STRESS CAGR <= NORMAL CAGR (cost drag should reduce returns)
        # — only assert when both are positive.
        if mode == "stress":
            ncagr = payload["results"]["normal"]["metrics"]["cagr"]
            scagr = metrics["cagr"]
            if ncagr > 0 and scagr > ncagr + 1e-9:
                issues.append(
                    f"STRESS cagr {scagr} > NORMAL cagr {ncagr} (cost drag inverted?)"
                )

    detail["normal_metrics"] = payload["results"]["normal"]["metrics"]
    detail["stress_metrics"] = payload["results"]["stress"]["metrics"]
    detail["issues"] = issues
    detail["normal_hash"] = payload["canonical_hash_normal"]
    detail["stress_hash"] = payload["canonical_hash_stress"]
    # Cache for G6 reuse
    detail["_payload_for_g6"] = payload
    return not issues, detail


# --- G6 idempotency ------------------------------------------------------


def gate_g6_idempotency(prior_payload: dict) -> tuple[bool, dict]:
    print("\n[G6] idempotency — second run must match first byte-identical")

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    config = _build_config()
    payload2 = run_dual(config, connection_factory=connection)

    n1 = prior_payload["canonical_hash_normal"]
    n2 = payload2["canonical_hash_normal"]
    s1 = prior_payload["canonical_hash_stress"]
    s2 = payload2["canonical_hash_stress"]

    # Trade-count equality (additional cross-check)
    tc_n1 = prior_payload["results"]["normal"]["trade_count"]
    tc_n2 = payload2["results"]["normal"]["trade_count"]
    tc_s1 = prior_payload["results"]["stress"]["trade_count"]
    tc_s2 = payload2["results"]["stress"]["trade_count"]

    ok = (n1 == n2) and (s1 == s2) and (tc_n1 == tc_n2) and (tc_s1 == tc_s2)
    return ok, {
        "normal_hash_run1": n1,
        "normal_hash_run2": n2,
        "stress_hash_run1": s1,
        "stress_hash_run2": s2,
        "trade_count_normal_run1": tc_n1,
        "trade_count_normal_run2": tc_n2,
        "trade_count_stress_run1": tc_s1,
        "trade_count_stress_run2": tc_s2,
        "hashes_match": (n1 == n2) and (s1 == s2),
        "trade_counts_match": (tc_n1 == tc_n2) and (tc_s1 == tc_s2),
    }


# --- Foundation regression ----------------------------------------------


def gate_foundation_regression() -> tuple[bool, dict]:
    """Re-run the PR #1 foundation verifier as a sub-process. PR #2's code
    changes (parser, engine, etc.) MUST NOT regress G1/G2/G3/G7/G9/G10.

    Hang guard: 1800s timeout — same pattern as step2's G10 patch."""
    print("\n[regression] PR #1 foundation verifier — must still PASS 6/6")

    cmd = [
        str(WORKTREE_ROOT / ".." / "Q-TRON-32_ARCHIVE" / ".venv64" / "Scripts" / "python.exe"),
        "-X", "utf8",
        str(WORKTREE_ROOT / "scripts" / "crypto" / "verify_backtest_foundation.py"),
    ]
    # The path above resolves on this Windows host; fall back to ``sys.executable``
    # if the venv path moved.
    cmd[0] = sys.executable
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8", timeout=1800)
    except subprocess.TimeoutExpired as exc:
        out_tail = "\n".join((exc.stdout or "").splitlines()[-12:]) if exc.stdout else "(empty)"
        err_tail = "\n".join((exc.stderr or "").splitlines()[-12:]) if exc.stderr else "(empty)"
        print(f"[TIMEOUT] verify_backtest_foundation.py exceeded 1800s")
        print(f"  cmd: {' '.join(cmd)}")
        print(f"  stdout tail:\n{out_tail}")
        print(f"  stderr tail:\n{err_tail}")
        return False, {
            "returncode": -1,
            "tail": (
                f"TIMEOUT after 1800s\n"
                f"--- stdout tail ---\n{out_tail}\n"
                f"--- stderr tail ---\n{err_tail}"
            ),
        }
    out_tail = "\n".join(proc.stdout.splitlines()[-12:])
    return proc.returncode == 0, {
        "returncode": proc.returncode,
        "tail": out_tail,
    }


# --- Main ----------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D4 PR #2 engine verification @ {_now()}")
    print("=" * 78)

    results: list[dict] = []
    all_ok = True

    # G4 first — the payload is reused for G6 to avoid running 4 dual-modes.
    g4_ok, g4_detail = gate_g4_metrics()
    payload_for_g6 = g4_detail.pop("_payload_for_g6")
    results.append({
        "gate": "G4 metrics",
        "verdict": "PASS" if g4_ok else "FAIL",
        "detail": g4_detail,
    })
    print(f"\n[{'PASS' if g4_ok else 'FAIL'}] G4 metrics")
    for k, v in g4_detail.items():
        print(f"    {k}: {v}")
    if not g4_ok:
        all_ok = False

    # G6
    g6_ok, g6_detail = gate_g6_idempotency(payload_for_g6)
    results.append({
        "gate": "G6 idempotency",
        "verdict": "PASS" if g6_ok else "FAIL",
        "detail": g6_detail,
    })
    print(f"\n[{'PASS' if g6_ok else 'FAIL'}] G6 idempotency")
    for k, v in g6_detail.items():
        print(f"    {k}: {v}")
    if not g6_ok:
        all_ok = False

    # Regression
    reg_ok, reg_detail = gate_foundation_regression()
    results.append({
        "gate": "PR #1 foundation regression",
        "verdict": "PASS" if reg_ok else "FAIL",
        "detail": reg_detail,
    })
    print(f"\n[{'PASS' if reg_ok else 'FAIL'}] PR #1 foundation regression")
    for k, v in reg_detail.items():
        print(f"    {k}: {v}")
    if not reg_ok:
        all_ok = False

    summary_path = VERIF_DIR / f"d4_engine_baseline_{_now()[:10]}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D4 PR #2 (engine + Momentum 12-1)",
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
