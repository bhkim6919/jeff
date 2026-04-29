"""D5 STEP 3 verification — Volatility Pullback Experimental addition.

Gates exercised here:

    G13  Core 3 + RSI2 + VolPullback + HODL parity (6 trading + 1 benchmark)
         on the PR #2/#3/#19/#21 6-month window:
         - momentum_12_1, sma_50_200_trend, atr_breakout, donchian_20d,
           rsi2_mean_reversion, btc_hodl reproduce their PR #21 NORMAL
           canonical hashes byte-identical (proof: STEP 3 changes did
           NOT affect existing strategies' engine path).
         - volatility_pullback_experimental produces a hash distinct
           from all six.
         - All seven strategies return finite 6-metric outputs.

    G14  STEP 2 regression (inline) — rsi2_mean_reversion 6mo NORMAL
         hash matches PR #21 lock byte-for-byte. Tightens G13 with an
         explicit name for traceability.

    STEP 3 sanity 6mo (NORMAL + STRESS dual, Jeff F24=A):
         - volatility_pullback_experimental produces finite metrics
           under both modes.
         - mdd <= 0, exposure_pct in [0, 100].
         - NORMAL hash != STRESS hash *only when there are trades*.
           VolPullback is sparse — zero trades on the G13 window is
           expected and not a failure (verifier exempts the trades=0
           collision case explicitly, identical to STEP 2's RSI2 case).

    STEP 3 sanity 5y (NORMAL only, Jeff F23=A + Jeff D5 STEP 1 보완 #5):
         - volatility_pullback_experimental on 2021-01-01 ~ 2026-04-26
           produces finite metrics with trades >= 1.

    G10 PR #21 regression (subprocess `verify_backtest_d5_step2.py`):
         - exit 0 implies the full PR #21 evidence chain is preserved
           (G11 / G12 / 6mo dual / 5y / G10 / nested PR #19 verifier /
           further nested PR #3 → #2 → #1 verifiers).

Verifier window for G13 + G14 + STEP3 6mo: 2020-01-01 ~ 2020-06-30,
top_n=20 (matches PR #2/#3/#19/#21 — required for hash compare).
BTC HODL keeps top_n=1.

5-year sanity window: 2021-01-01 ~ 2026-04-26, top_n=20.

Per Jeff D5 STEP 3 lock:
    - Engine / cost_model / universe untouched.
    - VolatilityPullbackExperimental is flagged 'experimental' in both
      docstring and strategy.name. Hash carries the suffix into all
      downstream evidence so the strategy can never be confused with
      a production candidate.

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
from crypto.backtest.engine import (  # noqa: E402
    BacktestConfig,
    run_backtest,
    run_multi,
)
from crypto.backtest.strategies.atr_breakout import ATRBreakout  # noqa: E402
from crypto.backtest.strategies.btc_hodl import BTCHodl  # noqa: E402
from crypto.backtest.strategies.donchian_20d import Donchian20DBreakout  # noqa: E402
from crypto.backtest.strategies.momentum_12_1 import Momentum12_1  # noqa: E402
from crypto.backtest.strategies.rsi2_mean_reversion import RSI2MeanReversion  # noqa: E402
from crypto.backtest.strategies.sma_50_200 import SMA50_200Trend  # noqa: E402
from crypto.backtest.strategies.volatility_pullback import (  # noqa: E402
    VolatilityPullbackExperimental,
)
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)

# PR #21 NORMAL hashes — byte-identical regression target.
# (D4 4 + STEP 1 1 + STEP 2 1 = 6 strategies)
PR21_HASHES_NORMAL = {
    "momentum_12_1":        "76d392cd9ffd110f4b44216eb7ec8b31db2242502d47bd808e819a796b71fd8d",
    "sma_50_200_trend":     "d164b15c3b1d86faa21bc2e45bfdbd63efdd940bdaab2e8ef4686c1c823449e0",
    "atr_breakout":         "af24c4f64d23c38d16b2fbe626666668fc6e24d6ed901d36869374250d247529",
    "donchian_20d":         "b5a98b256c78824963ff1d9e5a17d496587372291a2759d120faed36631478d3",
    "rsi2_mean_reversion":  "e883995251d986cf936033b1b5768cf3b8366e294b217caec88fcabb094f382a",
    "btc_hodl":             "655df2ec147d50d0b53f8dfc8e3c1e1d34b44074d973289dc4b8f96586fd64e5",
}

# G13 + G14 + STEP3 6mo regression window (matches PR #2/#3/#19/#21).
G13_START = date(2020, 1, 1)
G13_END = date(2020, 6, 30)
G13_TOP_N = 20

# 5-year sanity window.
SANITY_5Y_START = date(2021, 1, 1)
SANITY_5Y_END = date(2026, 4, 26)
SANITY_TOP_N = 20

REQUIRED_METRIC_KEYS = ("cagr", "mdd", "sharpe", "calmar", "trades", "exposure_pct")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_universe():
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    return KRWStaticTop100.from_csv_and_listings(DEFAULT_TOP100_CSV, listings), connection


def _build_config(strategy, *, start: date, end: date, top_n: int):
    universe, conn_factory = _build_universe()
    return (
        BacktestConfig(
            strategy=strategy,
            universe=universe,
            start_date=start,
            end_date=end,
            initial_cash_krw=100_000_000.0,
            rebal_days=21,
            top_n=top_n,
            cost_config=CostConfig(),
        ),
        conn_factory,
    )


# --- G13 Core 3 + RSI2 + VolPullback + HODL parity ----------------------


def gate_g13_parity() -> tuple[bool, dict]:
    """6 D5 strategies (incl. VolPullback) reproduce PR #21 NORMAL hashes;
    volatility_pullback_experimental distinct.

    Sparse-signal exemption: if VolPullback produces zero trades on the
    G13 window, its flat-equity hash may collide with any other
    zero-trade strategy (e.g., RSI2 on this window). The collision
    check is skipped in that case — same exemption as STEP 3 6mo /
    STEP 2 RSI2."""
    print("\n[G13] Core 3 + RSI2 + VolPullback + HODL parity — 7 strategies on G13 window NORMAL")
    base, conn = _build_config(
        Momentum12_1(), start=G13_START, end=G13_END, top_n=G13_TOP_N
    )
    trading = run_multi(
        base,
        [
            Momentum12_1(),
            SMA50_200Trend(),
            ATRBreakout(),
            Donchian20DBreakout(),
            RSI2MeanReversion(),
            VolatilityPullbackExperimental(),
        ],
        CostMode.NORMAL,
        connection_factory=conn,
    )
    bench_cfg, _ = _build_config(BTCHodl(), start=G13_START, end=G13_END, top_n=1)
    bench = run_backtest(bench_cfg, CostMode.NORMAL, connection_factory=conn)

    actual: dict[str, str] = {}
    for name, res in trading.items():
        actual[name] = res.canonical_hash
    actual["btc_hodl"] = bench.canonical_hash

    issues: list[str] = []

    # PR #21-locked 6 hashes byte-identical
    for name, expected in PR21_HASHES_NORMAL.items():
        got = actual.get(name)
        if got != expected:
            issues.append(
                f"{name} hash drift: expected {expected[:16]}... got "
                f"{got[:16] if got else 'None'}..."
            )

    # volatility_pullback_experimental distinct from all
    vp_hash = actual.get("volatility_pullback_experimental")
    vp_trades = len(trading["volatility_pullback_experimental"].trade_log)
    if vp_hash is None:
        issues.append("volatility_pullback_experimental missing from run_multi output")
    elif vp_trades > 0:
        for n, h in actual.items():
            if n != "volatility_pullback_experimental" and h == vp_hash:
                issues.append(f"volatility_pullback_experimental hash collides with {n}")

    # Finite 6-metric outputs across the board
    for name, res in list(trading.items()) + [("btc_hodl", bench)]:
        for k in REQUIRED_METRIC_KEYS:
            if k not in res.metrics:
                issues.append(f"{name}.{k} missing")
                continue
            v = res.metrics[k]
            if not isinstance(v, (int, float)) or (
                isinstance(v, float) and (math.isnan(v) or math.isinf(v))
            ):
                issues.append(f"{name}.{k} non-finite ({v})")

    detail = {
        "hashes_actual": actual,
        "hashes_expected_pr21": PR21_HASHES_NORMAL,
        "vol_pullback_metrics": {
            k: round(float(trading["volatility_pullback_experimental"].metrics[k]), 6)
            for k in REQUIRED_METRIC_KEYS
        },
        "vol_pullback_trades": len(trading["volatility_pullback_experimental"].trade_log),
        "vol_pullback_rebal_executed": len(
            trading["volatility_pullback_experimental"].rebal_dates_executed
        ),
        "issues": issues,
    }
    return not issues, detail


# --- G14 STEP 2 regression (inline) -------------------------------------


def gate_g14_step2_regression(g13_detail: dict) -> tuple[bool, dict]:
    """Explicit inline name for the rsi2_mean_reversion hash compare."""
    print("\n[G14] STEP 2 regression — rsi2_mean_reversion hash == PR #21 lock")
    actual = g13_detail.get("hashes_actual", {}).get("rsi2_mean_reversion")
    expected = PR21_HASHES_NORMAL["rsi2_mean_reversion"]
    detail = {
        "expected_hash": expected,
        "actual_hash": actual,
        "match": actual == expected,
    }
    return detail["match"], detail


# --- STEP 3 sanity 6mo dual ---------------------------------------------


def gate_step3_sanity_6mo() -> tuple[bool, dict]:
    """VolPullback NORMAL + STRESS finite + sane on the regression window.

    Sparse-signal exemption: if both NORMAL and STRESS produce zero
    trades, both yield a flat-equity hash that will be identical (no
    cost to differentiate). The collision check is skipped in that
    case — same exemption as STEP 2 (RSI2)."""
    print("\n[STEP 3 6mo] VolPullback NORMAL + STRESS dual")
    cfg, conn = _build_config(
        VolatilityPullbackExperimental(),
        start=G13_START, end=G13_END, top_n=G13_TOP_N,
    )
    res_n = run_backtest(cfg, CostMode.NORMAL, connection_factory=conn)
    res_s = run_backtest(cfg, CostMode.STRESS, connection_factory=conn)

    issues: list[str] = []
    for mode, res in (("normal", res_n), ("stress", res_s)):
        for k in REQUIRED_METRIC_KEYS:
            v = res.metrics.get(k)
            if v is None:
                issues.append(f"{mode}.{k} missing")
                continue
            if not isinstance(v, (int, float)) or (
                isinstance(v, float) and (math.isnan(v) or math.isinf(v))
            ):
                issues.append(f"{mode}.{k} non-finite ({v})")
        if res.metrics.get("mdd", 0) > 0:
            issues.append(f"{mode}.mdd positive ({res.metrics['mdd']})")
        exp = res.metrics.get("exposure_pct", -1)
        if not (0.0 <= exp <= 100.0):
            issues.append(f"{mode}.exposure_pct out of range ({exp})")

    if (
        len(res_n.trade_log) > 0
        and res_n.canonical_hash == res_s.canonical_hash
    ):
        issues.append("normal_hash == stress_hash with trades > 0 (cost mode not applied)")

    detail = {
        "normal_metrics": {k: round(float(res_n.metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
        "stress_metrics": {k: round(float(res_s.metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
        "normal_hash": res_n.canonical_hash,
        "stress_hash": res_s.canonical_hash,
        "normal_trades": len(res_n.trade_log),
        "stress_trades": len(res_s.trade_log),
        "normal_rebal_executed": len(res_n.rebal_dates_executed),
        "stress_rebal_executed": len(res_s.rebal_dates_executed),
        "issues": issues,
    }
    return not issues, detail


# --- STEP 3 sanity 5y NORMAL --------------------------------------------


def gate_step3_sanity_5y() -> tuple[bool, dict]:
    """VolPullback over 2021-01-01 ~ 2026-04-26 NORMAL — trades >= 1, finite."""
    print("\n[STEP 3 5y] VolPullback 2021~2026 NORMAL — trades>=1, finite")
    cfg, conn = _build_config(
        VolatilityPullbackExperimental(),
        start=SANITY_5Y_START,
        end=SANITY_5Y_END,
        top_n=SANITY_TOP_N,
    )
    res = run_backtest(cfg, CostMode.NORMAL, connection_factory=conn)

    issues: list[str] = []
    for k in REQUIRED_METRIC_KEYS:
        v = res.metrics.get(k)
        if v is None:
            issues.append(f"{k} missing")
            continue
        if not isinstance(v, (int, float)) or (
            isinstance(v, float) and (math.isnan(v) or math.isinf(v))
        ):
            issues.append(f"{k} non-finite ({v})")
    if res.metrics.get("mdd", 0) > 0:
        issues.append(f"mdd positive ({res.metrics['mdd']})")
    exp = res.metrics.get("exposure_pct", -1)
    if not (0.0 <= exp <= 100.0):
        issues.append(f"exposure_pct out of range ({exp})")
    if len(res.trade_log) < 1:
        issues.append("trades < 1 across 5-year window — strategy never entered")

    detail = {
        "metrics": {k: round(float(res.metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
        "trades": len(res.trade_log),
        "rebal_executed": len(res.rebal_dates_executed),
        "rebal_skipped": len(res.rebal_dates_skipped),
        "final_equity_krw": round(res.final_equity_krw, 2),
        "canonical_hash": res.canonical_hash,
        "issues": issues,
    }
    return not issues, detail


# --- G10 PR #21 regression (subprocess) ---------------------------------


def gate_g10_pr21_regression_subprocess() -> tuple[bool, dict]:
    """verify_backtest_d5_step2.py exit 0 — proves the full PR #21
    evidence chain (which itself nests PR #19 → PR #3 → PR #2 → PR #1
    verifiers) is preserved."""
    print("\n[G10] PR #21 regression (verify_backtest_d5_step2.py subprocess)")
    cmd = [
        sys.executable,
        "-X", "utf8",
        str(WORKTREE_ROOT / "scripts" / "crypto" / "verify_backtest_d5_step2.py"),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    tail = "\n".join((proc.stdout or "").splitlines()[-15:])
    return proc.returncode == 0, {
        "returncode": proc.returncode,
        "tail": tail,
    }


# --- Driver -------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D5 STEP 3 verification @ {_now()}")
    print(f"  G13 window:    {G13_START} ~ {G13_END}  top_n={G13_TOP_N}")
    print(f"  5y sanity:     {SANITY_5Y_START} ~ {SANITY_5Y_END}  top_n={SANITY_TOP_N}")
    print("=" * 78)

    gates = [
        ("G13 Core 3 + RSI2 + VolPullback + HODL parity", gate_g13_parity),
    ]

    results: list[dict] = []
    all_ok = True

    g13_ok, g13_detail = gate_g13_parity()
    results.append({
        "gate": "G13 Core 3 + RSI2 + VolPullback + HODL parity",
        "verdict": "PASS" if g13_ok else "FAIL",
        "detail": g13_detail,
    })
    print(f"\n[{'PASS' if g13_ok else 'FAIL'}] G13 Core 3 + RSI2 + VolPullback + HODL parity")
    for k, v in g13_detail.items():
        if k not in ("hashes_expected_pr21",):
            print(f"    {k}: {v}")
    if not g13_ok:
        all_ok = False

    g14_ok, g14_detail = gate_g14_step2_regression(g13_detail)
    results.append({
        "gate": "G14 STEP 2 regression (rsi2_mean_reversion hash)",
        "verdict": "PASS" if g14_ok else "FAIL",
        "detail": g14_detail,
    })
    print(f"\n[{'PASS' if g14_ok else 'FAIL'}] G14 STEP 2 regression")
    for k, v in g14_detail.items():
        print(f"    {k}: {v}")
    if not g14_ok:
        all_ok = False

    s6_ok, s6_detail = gate_step3_sanity_6mo()
    results.append({
        "gate": "STEP 3 sanity 6mo NORMAL+STRESS",
        "verdict": "PASS" if s6_ok else "FAIL",
        "detail": s6_detail,
    })
    print(f"\n[{'PASS' if s6_ok else 'FAIL'}] STEP 3 sanity 6mo NORMAL+STRESS")
    for k, v in s6_detail.items():
        print(f"    {k}: {v}")
    if not s6_ok:
        all_ok = False

    s5_ok, s5_detail = gate_step3_sanity_5y()
    results.append({
        "gate": "STEP 3 sanity 5y NORMAL",
        "verdict": "PASS" if s5_ok else "FAIL",
        "detail": s5_detail,
    })
    print(f"\n[{'PASS' if s5_ok else 'FAIL'}] STEP 3 sanity 5y NORMAL")
    for k, v in s5_detail.items():
        print(f"    {k}: {v}")
    if not s5_ok:
        all_ok = False

    reg_ok, reg_detail = gate_g10_pr21_regression_subprocess()
    results.append({
        "gate": "G10 PR #21 regression subprocess",
        "verdict": "PASS" if reg_ok else "FAIL",
        "detail": reg_detail,
    })
    print(f"\n[{'PASS' if reg_ok else 'FAIL'}] G10 PR #21 regression subprocess")
    for k, v in reg_detail.items():
        if k == "tail":
            print(f"    {k}:")
            for line in str(v).splitlines():
                print(f"      {line}")
        else:
            print(f"    {k}: {v}")
    if not reg_ok:
        all_ok = False

    summary_path = VERIF_DIR / f"d5_step3_baseline_{_now().replace(':', '_')}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D5 STEP 3 (Volatility Pullback Experimental addition)",
                "decisions": {
                    "F15": "A — STEP 3 single PR (VolPullback only)",
                    "F16": "A — 3-day expansion > 1.8 * ATR20",
                    "F17": "A — 3% pullback from recent peak",
                    "F18": "A — Close > EMA5 bounce signal",
                    "F19": "B — Fibonacci option excluded (simplicity)",
                    "F20": "A — ATR multiple desc",
                    "F21": "C — name + docstring both flagged 'experimental'",
                    "F22": "A — KRWStaticTop100 shared (no per-strategy universe)",
                    "F23": "A — 6mo + 5y windows",
                    "F24": "A — 6mo NORMAL+STRESS dual + 5y NORMAL only",
                },
                "g13_window": {
                    "start": G13_START.isoformat(),
                    "end": G13_END.isoformat(),
                    "top_n": G13_TOP_N,
                },
                "sanity_5y_window": {
                    "start": SANITY_5Y_START.isoformat(),
                    "end": SANITY_5Y_END.isoformat(),
                    "top_n": SANITY_TOP_N,
                },
                "pr21_locked_hashes_normal": PR21_HASHES_NORMAL,
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
