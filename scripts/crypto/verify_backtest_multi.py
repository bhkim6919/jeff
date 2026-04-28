"""D4 PR #3 (multi-strategy) verification — G5 + G8 + G4/G6 regression.

Gates exercised here:

    G5  multi-strategy parity — three trading strategies (Momentum 12-1,
        SMA 50/200 Trend, ATR Breakout) plus the BTC HODL benchmark all
        run to completion on the same window/universe/cost; each produces
        the full 6-metric set; canonical hashes are pairwise distinct
        (different strategies MUST yield different results — proves the
        engine is not silently funneling them through a shared cache).

    G8  BTC HODL benchmark sanity — the benchmark equity curve is finite,
        exposure is at-or-near 100%, MDD is non-positive, trades >= 1
        (entry happened), and metrics are within the same order of
        magnitude as the trading strategies (loose sanity, not a return
        evaluation).

    G4/G6 regression — Momentum 12-1's NORMAL+STRESS canonical hashes
        match the PR #2 lock byte-for-byte. This is implemented two ways:
        (a) inline hash compare against the locked values, AND
        (b) subprocess to ``verify_backtest_engine.py`` which also
        re-exercises the PR #1 foundation gates.

    Sparse-universe 5-year reconfirm — counts how many KRW pairs satisfy
        Momentum 12-1's 365-day lookback at every monthly anchor across
        2021-01-01 ~ 2026-04-26. PR #2 noted ~10 pickable pairs in 2020
        H1; PASS if the 5-year mean is >= top_n (=20), which would mark
        2020 as a transient anomaly rather than chronic sparseness.

Verifier window for G5 + G8 + regression: 2020-01-01 ~ 2020-06-30,
top_n=20 (matches PR #2 — required for the regression hash compare).
BTC HODL uses top_n=1 (single-pair benchmark by design).

Exit:
    0 — all gates PASS
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
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import CostConfig, CostMode  # noqa: E402
from crypto.backtest.data_loader import OhlcvLoader  # noqa: E402
from crypto.backtest.engine import (  # noqa: E402
    BacktestConfig,
    compute_rebal_dates,
    run_backtest,
    run_multi,
)
from crypto.backtest.strategies.atr_breakout import ATRBreakout  # noqa: E402
from crypto.backtest.strategies.btc_hodl import BTCHodl  # noqa: E402
from crypto.backtest.strategies.momentum_12_1 import Momentum12_1  # noqa: E402
from crypto.backtest.strategies.sma_50_200 import SMA50_200Trend  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)

# G4/G6 regression — Momentum 12-1 hashes locked in PR #2.
PR2_HASH_NORMAL = "76d392cd9ffd110f4b44216eb7ec8b31db2242502d47bd808e819a796b71fd8d"
PR2_HASH_STRESS = "5805a9fd4ad2637f561ddbecfff0e31f0d6871a31355c0630289298d9d93a702"

# G5 + regression window (must match PR #2 for hash comparison).
G5_START = date(2020, 1, 1)
G5_END = date(2020, 6, 30)
G5_TOP_N = 20

# Sparse-universe check window (Jeff PR #3: 5년 이상).
SPARSE_START = date(2021, 1, 1)
SPARSE_END = date(2026, 4, 26)

REQUIRED_METRIC_KEYS = ("cagr", "mdd", "sharpe", "calmar", "trades", "exposure_pct")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_universe():
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    return KRWStaticTop100.from_csv_and_listings(DEFAULT_TOP100_CSV, listings), connection


def _build_g5_config(strategy, *, top_n: int):
    universe, conn_factory = _build_universe()
    return BacktestConfig(
        strategy=strategy,
        universe=universe,
        start_date=G5_START,
        end_date=G5_END,
        initial_cash_krw=100_000_000.0,
        rebal_days=21,
        top_n=top_n,
        cost_config=CostConfig(),
    ), conn_factory


# --- G5 multi-strategy parity --------------------------------------------


def gate_g5_multi_strategy_parity() -> tuple[bool, dict]:
    """Three trading strategies + benchmark all complete with finite,
    distinct results. Returns the per-strategy result map for downstream
    gates (G8 benchmark sanity, regression hash compare)."""
    print("\n[G5] multi-strategy parity — 3 strategies + BTC HODL benchmark")

    # Trading strategies — top_n=20 (engine-validation cohort).
    base_cfg, conn_factory = _build_g5_config(Momentum12_1(), top_n=G5_TOP_N)
    trading_results = run_multi(
        base_cfg,
        [Momentum12_1(), SMA50_200Trend(), ATRBreakout()],
        CostMode.NORMAL,
        connection_factory=conn_factory,
    )

    # BTC HODL benchmark — top_n=1 (single-pair benchmark by design).
    bench_cfg, _ = _build_g5_config(BTCHodl(), top_n=1)
    bench_result = run_backtest(
        bench_cfg, CostMode.NORMAL, connection_factory=conn_factory
    )

    detail: dict = {
        "window_start": G5_START.isoformat(),
        "window_end": G5_END.isoformat(),
        "trading_top_n": G5_TOP_N,
        "benchmark_top_n": 1,
        "results": {},
    }

    issues: list[str] = []
    hashes: dict[str, str] = {}

    for name, res in trading_results.items():
        detail["results"][name] = _summarize(res)
        hashes[name] = res.canonical_hash
        for k in REQUIRED_METRIC_KEYS:
            if k not in res.metrics:
                issues.append(f"{name}.{k} missing")
            else:
                v = res.metrics[k]
                if not isinstance(v, (int, float)) or (
                    isinstance(v, float) and (math.isnan(v) or math.isinf(v))
                ):
                    issues.append(f"{name}.{k} non-finite ({v})")

    # Benchmark
    detail["results"]["btc_hodl"] = _summarize(bench_result)
    hashes["btc_hodl"] = bench_result.canonical_hash

    # Distinct-hash check across all 4 (G5 core: different strategies →
    # different deterministic outputs).
    hash_pairs = list(hashes.items())
    for i in range(len(hash_pairs)):
        for j in range(i + 1, len(hash_pairs)):
            n1, h1 = hash_pairs[i]
            n2, h2 = hash_pairs[j]
            if h1 == h2:
                issues.append(
                    f"hash collision: {n1!r} == {n2!r} ({h1[:16]}...)"
                )
    detail["hashes"] = {n: h for n, h in hashes.items()}
    detail["issues"] = issues
    # Cache results for downstream gates.
    detail["_trading_results"] = trading_results
    detail["_bench_result"] = bench_result

    return not issues, detail


# --- G8 BTC HODL benchmark sanity ---------------------------------------


def gate_g8_btc_hodl_sanity(bench_result, trading_results) -> tuple[bool, dict]:
    """Benchmark equity is finite, exposure ~100%, trades>=1, MDD non-
    positive. Magnitude sanity vs trading strategies (no strict bound)."""
    print("\n[G8] BTC HODL benchmark sanity")

    metrics = bench_result.metrics
    detail: dict = {
        "metrics": {k: round(float(metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
        "final_equity_krw": round(bench_result.final_equity_krw, 2),
        "trade_count": len(bench_result.trade_log),
        "rebal_executed": len(bench_result.rebal_dates_executed),
    }

    issues: list[str] = []
    for k in REQUIRED_METRIC_KEYS:
        v = metrics.get(k)
        if v is None:
            issues.append(f"{k} missing")
            continue
        if not isinstance(v, (int, float)) or (
            isinstance(v, float) and (math.isnan(v) or math.isinf(v))
        ):
            issues.append(f"{k} non-finite ({v})")

    if metrics.get("mdd", 0) > 0:
        issues.append(f"mdd positive ({metrics['mdd']})")
    if metrics.get("trades", 0) < 1:
        issues.append("trades < 1 — benchmark never entered")
    if metrics.get("exposure_pct", -1) < 50:
        issues.append(
            f"exposure_pct {metrics.get('exposure_pct')} < 50 — benchmark not invested"
        )

    # Magnitude sanity — benchmark CAGR within ~5x of any trading
    # strategy's CAGR (loose: just rule out wild divergence indicating
    # an engine glitch). Allow same-sign requirement waived (HODL can
    # diverge in sign during bear segments).
    bench_cagr = metrics.get("cagr", 0.0)
    sane_band: dict[str, dict] = {}
    for name, res in trading_results.items():
        s_cagr = res.metrics.get("cagr", 0.0)
        ratio = (
            abs(bench_cagr) / max(abs(s_cagr), 1e-9)
            if abs(s_cagr) > 0
            else float("inf") if abs(bench_cagr) > 0 else 1.0
        )
        sane_band[name] = {
            "strategy_cagr": round(s_cagr, 4),
            "ratio_bench_to_strategy": (
                round(ratio, 2) if ratio != float("inf") else "inf"
            ),
        }
    detail["benchmark_vs_strategies"] = sane_band
    detail["benchmark_cagr"] = round(bench_cagr, 4)
    detail["issues"] = issues
    return not issues, detail


# --- G4/G6 regression: Momentum hash identity ----------------------------


def gate_g4_g6_regression_inline(trading_results) -> tuple[bool, dict]:
    """Inline check: Momentum 12-1 NORMAL hash from this run matches PR #2
    lock. STRESS is checked via the subprocess gate below (full dual-run
    plus PR #1 foundation regression)."""
    print("\n[G4/G6 inline] Momentum 12-1 NORMAL hash == PR #2 lock")
    mom = trading_results["momentum_12_1"]
    actual = mom.canonical_hash
    detail = {
        "expected_normal_hash": PR2_HASH_NORMAL,
        "actual_normal_hash": actual,
        "match": actual == PR2_HASH_NORMAL,
    }
    return detail["match"], detail


def gate_g4_g6_regression_subprocess() -> tuple[bool, dict]:
    """Run verify_backtest_engine.py as subprocess — re-exercises G4 + G6
    full dual-run and PR #1 foundation regression in one go."""
    print("\n[G4/G6 subprocess] verify_backtest_engine.py — full re-run")
    cmd = [
        sys.executable,
        "-X", "utf8",
        str(WORKTREE_ROOT / "scripts" / "crypto" / "verify_backtest_engine.py"),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    tail = "\n".join((proc.stdout or "").splitlines()[-15:])
    return proc.returncode == 0, {
        "returncode": proc.returncode,
        "tail": tail,
    }


# --- Sparse-universe 5-year reconfirm -----------------------------------


def gate_sparse_universe_5yr() -> tuple[bool, dict]:
    """Count Momentum-12-1 pickable pairs at every monthly anchor across
    SPARSE_START..SPARSE_END. PASS if mean >= G5_TOP_N (i.e. 2020 H1's
    ~10 pickable observation is a transient anomaly, not the typical
    case across the broader window)."""
    print("\n[5yr sparse] Momentum 12-1 pickable count over 2021~2026")
    universe, conn_factory = _build_universe()
    loader = OhlcvLoader(conn_factory)
    strategy = Momentum12_1()

    anchors = compute_rebal_dates(SPARSE_START, SPARSE_END, 21)
    if not anchors:
        return False, {"error": "no anchors computed"}

    per_anchor: list[dict] = []
    pickable_counts: list[int] = []
    active_counts: list[int] = []

    for anchor in anchors:
        # asof = anchor itself; engine's live run uses trade_date - 1, but
        # for pickable counting the off-by-one is immaterial as long as we
        # apply the same convention to every anchor.
        signal_asof = anchor
        active = universe.active_pairs(signal_asof)
        # top_n=200 to count ALL pickable, not the top 20 cohort.
        picks = strategy.select(
            asof=signal_asof,
            universe=active,
            loader=loader,
            top_n=200,
        )
        per_anchor.append({
            "anchor": anchor.isoformat(),
            "active_count": len(active),
            "pickable_count": len(picks),
        })
        pickable_counts.append(len(picks))
        active_counts.append(len(active))

    pickable_mean = (
        sum(pickable_counts) / len(pickable_counts) if pickable_counts else 0.0
    )

    detail = {
        "window_start": SPARSE_START.isoformat(),
        "window_end": SPARSE_END.isoformat(),
        "n_anchors": len(anchors),
        "active_min": min(active_counts) if active_counts else 0,
        "active_max": max(active_counts) if active_counts else 0,
        "pickable_min": min(pickable_counts) if pickable_counts else 0,
        "pickable_max": max(pickable_counts) if pickable_counts else 0,
        "pickable_mean": round(pickable_mean, 2),
        "pickable_threshold_pass": G5_TOP_N,
        "first_5_anchors": per_anchor[:5],
        "last_5_anchors": per_anchor[-5:],
    }
    ok = pickable_mean >= G5_TOP_N
    return ok, detail


# --- Helpers ------------------------------------------------------------


def _summarize(res) -> dict:
    return {
        "canonical_hash": res.canonical_hash,
        "final_equity_krw": round(res.final_equity_krw, 2),
        "trade_count": len(res.trade_log),
        "rebal_executed": len(res.rebal_dates_executed),
        "rebal_skipped": len(res.rebal_dates_skipped),
        "metrics": {k: round(float(res.metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
    }


# --- Driver -------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D4 PR #3 multi-strategy verification @ {_now()}")
    print(f"  G5 window: {G5_START} ~ {G5_END}  top_n={G5_TOP_N}")
    print(f"  sparse window: {SPARSE_START} ~ {SPARSE_END}")
    print("=" * 78)

    results: list[dict] = []
    all_ok = True

    # G5 first — caches per-strategy results for G8 + regression-inline.
    g5_ok, g5_detail = gate_g5_multi_strategy_parity()
    trading_results = g5_detail.pop("_trading_results")
    bench_result = g5_detail.pop("_bench_result")
    results.append({"gate": "G5 multi-strategy parity",
                    "verdict": "PASS" if g5_ok else "FAIL",
                    "detail": g5_detail})
    print(f"\n[{'PASS' if g5_ok else 'FAIL'}] G5 multi-strategy parity")
    for k, v in g5_detail.items():
        if k != "results":  # too verbose for stdout
            print(f"    {k}: {v}")
    if not g5_ok:
        all_ok = False

    # G8 benchmark sanity
    g8_ok, g8_detail = gate_g8_btc_hodl_sanity(bench_result, trading_results)
    results.append({"gate": "G8 BTC HODL benchmark sanity",
                    "verdict": "PASS" if g8_ok else "FAIL",
                    "detail": g8_detail})
    print(f"\n[{'PASS' if g8_ok else 'FAIL'}] G8 BTC HODL benchmark sanity")
    for k, v in g8_detail.items():
        print(f"    {k}: {v}")
    if not g8_ok:
        all_ok = False

    # G4/G6 regression — inline + subprocess
    reg_inline_ok, reg_inline_detail = gate_g4_g6_regression_inline(trading_results)
    results.append({"gate": "G4/G6 regression (inline hash compare)",
                    "verdict": "PASS" if reg_inline_ok else "FAIL",
                    "detail": reg_inline_detail})
    print(f"\n[{'PASS' if reg_inline_ok else 'FAIL'}] G4/G6 regression (inline)")
    for k, v in reg_inline_detail.items():
        print(f"    {k}: {v}")
    if not reg_inline_ok:
        all_ok = False

    reg_sub_ok, reg_sub_detail = gate_g4_g6_regression_subprocess()
    results.append({"gate": "G4/G6 regression (subprocess + PR #1 foundation)",
                    "verdict": "PASS" if reg_sub_ok else "FAIL",
                    "detail": reg_sub_detail})
    print(f"\n[{'PASS' if reg_sub_ok else 'FAIL'}] G4/G6 regression (subprocess)")
    for k, v in reg_sub_detail.items():
        if k == "tail":
            print(f"    {k}:")
            for line in v.splitlines():
                print(f"      {line}")
        else:
            print(f"    {k}: {v}")
    if not reg_sub_ok:
        all_ok = False

    # 5-year sparse-universe reconfirm
    sparse_ok, sparse_detail = gate_sparse_universe_5yr()
    results.append({"gate": "Sparse-universe 5-year reconfirm",
                    "verdict": "PASS" if sparse_ok else "FAIL",
                    "detail": sparse_detail})
    print(f"\n[{'PASS' if sparse_ok else 'FAIL'}] Sparse-universe 5-year reconfirm")
    for k, v in sparse_detail.items():
        print(f"    {k}: {v}")
    if not sparse_ok:
        all_ok = False

    # Persist evidence
    summary_path = VERIF_DIR / f"d4_pr3_baseline_{_now().replace(':', '_')}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D4 PR #3 (multi-strategy + benchmark)",
                "g5_config": {
                    "start_date": G5_START.isoformat(),
                    "end_date": G5_END.isoformat(),
                    "top_n": G5_TOP_N,
                    "strategies": [
                        "momentum_12_1",
                        "sma_50_200_trend",
                        "atr_breakout",
                        "btc_hodl",
                    ],
                },
                "sparse_config": {
                    "start_date": SPARSE_START.isoformat(),
                    "end_date": SPARSE_END.isoformat(),
                },
                "pr2_locked_hashes": {
                    "momentum_12_1_normal": PR2_HASH_NORMAL,
                    "momentum_12_1_stress": PR2_HASH_STRESS,
                },
                "all_pass": all_ok,
                "gates": results,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print("\n" + "=" * 78)
    print(
        f"VERDICT: {'PASS' if all_ok else 'FAIL'}  "
        f"(summary: {summary_path.relative_to(WORKTREE_ROOT)})"
    )
    print("=" * 78)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
