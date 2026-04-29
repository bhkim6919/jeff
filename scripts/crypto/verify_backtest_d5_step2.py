"""D5 STEP 2 verification — RSI2 Mean Reversion addition.

Gates exercised here:

    G11  Core 3 + RSI2 + HODL parity (5 trading + 1 benchmark) on the
         PR #2/#3/#19 6-month window:
         - momentum_12_1, sma_50_200_trend, atr_breakout, donchian_20d,
           btc_hodl reproduce their PR #19 NORMAL canonical hashes
           byte-identical (proof: STEP 2 changes did NOT affect existing
           strategies' engine path).
         - rsi2_mean_reversion produces a hash distinct from all above.
         - All six strategies return finite 6-metric outputs.

    G12  STEP 1 regression (inline) — donchian_20d 6mo NORMAL hash matches
         PR #19 lock byte-identical. Tightens G11 with an explicit name.

    STEP 2 sanity 6mo (NORMAL + STRESS dual, Jeff F14=A):
         - rsi2_mean_reversion produces finite metrics under both modes.
         - mdd <= 0, exposure_pct in [0, 100].
         - NORMAL hash != STRESS hash (cost actually applied differently).

    STEP 2 sanity 5y (NORMAL only, Jeff F13=A + STEP 1 보완 #5):
         - rsi2_mean_reversion on 2021-01-01 ~ 2026-04-26 produces finite
           metrics with trades >= 1.

    G10 PR #3/D5 STEP 1 regression (subprocess `verify_backtest_d5_step1.py`):
         - exit 0 implies the full PR #19 evidence chain is preserved
           (G9 / 6mo dual / 5y / G10 / nested PR #3 verifier).

Verifier window for G11 + G12 + STEP2 6mo: 2020-01-01 ~ 2020-06-30,
top_n=20 (matches PR #19 — required for hash compare).
BTC HODL keeps top_n=1.

5-year sanity window: 2021-01-01 ~ 2026-04-26, top_n=20.

Per Jeff D5 STEP 2 lock:
    - Engine / cost_model / universe untouched.
    - RSI2 is a survivor candidate. The 21-day rebal-cycle engine does
      NOT match RSI2's original 3~5 day exit horizon — alpha distortion
      is expected. STEP 2 verifies deterministic execution only.

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
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)

# PR #19 NORMAL hashes — byte-identical regression target (D4 4 + STEP 1 1).
PR19_HASHES_NORMAL = {
    "momentum_12_1":    "76d392cd9ffd110f4b44216eb7ec8b31db2242502d47bd808e819a796b71fd8d",
    "sma_50_200_trend": "d164b15c3b1d86faa21bc2e45bfdbd63efdd940bdaab2e8ef4686c1c823449e0",
    "atr_breakout":     "af24c4f64d23c38d16b2fbe626666668fc6e24d6ed901d36869374250d247529",
    "donchian_20d":     "b5a98b256c78824963ff1d9e5a17d496587372291a2759d120faed36631478d3",
    "btc_hodl":         "655df2ec147d50d0b53f8dfc8e3c1e1d34b44074d973289dc4b8f96586fd64e5",
}

# G11 + G12 + STEP2 6mo regression window (matches PR #2/#3/#19).
G11_START = date(2020, 1, 1)
G11_END = date(2020, 6, 30)
G11_TOP_N = 20

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


# --- G11 Core 3 + RSI2 + HODL parity ------------------------------------


def gate_g11_parity() -> tuple[bool, dict]:
    """5 D5 strategies (incl. RSI2) reproduce PR #19 NORMAL hashes;
    rsi2_mean_reversion distinct."""
    print("\n[G11] Core 3 + RSI2 + HODL parity — 6 strategies on G11 window NORMAL")
    base, conn = _build_config(
        Momentum12_1(), start=G11_START, end=G11_END, top_n=G11_TOP_N
    )
    trading = run_multi(
        base,
        [
            Momentum12_1(),
            SMA50_200Trend(),
            ATRBreakout(),
            Donchian20DBreakout(),
            RSI2MeanReversion(),
        ],
        CostMode.NORMAL,
        connection_factory=conn,
    )
    bench_cfg, _ = _build_config(BTCHodl(), start=G11_START, end=G11_END, top_n=1)
    bench = run_backtest(bench_cfg, CostMode.NORMAL, connection_factory=conn)

    actual: dict[str, str] = {}
    for name, res in trading.items():
        actual[name] = res.canonical_hash
    actual["btc_hodl"] = bench.canonical_hash

    issues: list[str] = []

    # PR #19-locked 5 hashes byte-identical
    for name, expected in PR19_HASHES_NORMAL.items():
        got = actual.get(name)
        if got != expected:
            issues.append(
                f"{name} hash drift: expected {expected[:16]}... got "
                f"{got[:16] if got else 'None'}..."
            )

    # rsi2_mean_reversion distinct from all
    rsi_hash = actual.get("rsi2_mean_reversion")
    if rsi_hash is None:
        issues.append("rsi2_mean_reversion missing from run_multi output")
    else:
        for n, h in actual.items():
            if n != "rsi2_mean_reversion" and h == rsi_hash:
                issues.append(f"rsi2_mean_reversion hash collides with {n}")

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
        "hashes_expected_pr19": PR19_HASHES_NORMAL,
        "rsi2_metrics": {
            k: round(float(trading["rsi2_mean_reversion"].metrics[k]), 6)
            for k in REQUIRED_METRIC_KEYS
        },
        "rsi2_trades": len(trading["rsi2_mean_reversion"].trade_log),
        "rsi2_rebal_executed": len(trading["rsi2_mean_reversion"].rebal_dates_executed),
        "issues": issues,
    }
    return not issues, detail


# --- G12 STEP 1 regression (inline) -------------------------------------


def gate_g12_step1_regression(g11_detail: dict) -> tuple[bool, dict]:
    """Explicit inline name for the donchian_20d hash compare. The G11
    gate already checks every PR #19 hash; G12 surfaces the STEP 1
    contract as a named test for traceability."""
    print("\n[G12] STEP 1 regression — donchian_20d hash == PR #19 lock")
    actual = g11_detail.get("hashes_actual", {}).get("donchian_20d")
    expected = PR19_HASHES_NORMAL["donchian_20d"]
    detail = {
        "expected_hash": expected,
        "actual_hash": actual,
        "match": actual == expected,
    }
    return detail["match"], detail


# --- STEP 2 sanity 6mo dual ---------------------------------------------


def gate_step2_sanity_6mo() -> tuple[bool, dict]:
    """RSI2 NORMAL + STRESS finite + sane on the regression window."""
    print("\n[STEP 2 6mo] RSI2 NORMAL + STRESS dual")
    cfg, conn = _build_config(
        RSI2MeanReversion(), start=G11_START, end=G11_END, top_n=G11_TOP_N
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

    # NORMAL != STRESS hash only when there are actual trades — if RSI2
    # produces zero trades on this window (engine-fit mismatch) both
    # modes yield the same flat-equity hash. We surface the trade count
    # so a hash collision is interpretable, not a hidden failure.
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


# --- STEP 2 sanity 5y NORMAL --------------------------------------------


def gate_step2_sanity_5y() -> tuple[bool, dict]:
    """RSI2 over 2021-01-01 ~ 2026-04-26 NORMAL — trades >= 1, finite."""
    print("\n[STEP 2 5y] RSI2 2021~2026 NORMAL — trades>=1, finite")
    cfg, conn = _build_config(
        RSI2MeanReversion(),
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


# --- G10 PR #19 regression (subprocess) ---------------------------------


def gate_g10_pr19_regression_subprocess() -> tuple[bool, dict]:
    """verify_backtest_d5_step1.py exit 0 — proves the full PR #19
    evidence chain (which itself nests PR #3's verifier) is preserved.

    Hang guard: 1800s timeout. On TimeoutExpired the subprocess is
    killed and the gate fails with returncode=-1 + a tail string that
    embeds TIMEOUT marker plus the captured stdout/stderr tails so the
    failure point is diagnosable from the parent log alone."""
    # G10 timeout = 5400s (90 min). Empirical run on 2026-04-29 had
    # step1's chain (G9 + 6mo dual + 5y_sharded(6) + G10_nested) hit
    # ~30 min from G9~6mo alone; the original 1800s ceiling killed
    # step1 mid-shard. Raised to 5400s with the corresponding
    # step1 SHARDED_GATE_BUDGET_SEC bumped to 3000s — both still
    # bounded so a hung leaf cannot cost the whole afternoon.
    G10_SUBPROCESS_TIMEOUT_SEC = 5400

    print("\n[G10] PR #19 regression (verify_backtest_d5_step1.py subprocess)")
    cmd = [
        sys.executable,
        "-X", "utf8",
        str(WORKTREE_ROOT / "scripts" / "crypto" / "verify_backtest_d5_step1.py"),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8",
                              timeout=G10_SUBPROCESS_TIMEOUT_SEC)
    except subprocess.TimeoutExpired as exc:
        out_tail = "\n".join((exc.stdout or "").splitlines()[-15:]) if exc.stdout else "(empty)"
        err_tail = "\n".join((exc.stderr or "").splitlines()[-15:]) if exc.stderr else "(empty)"
        print(
            f"[TIMEOUT] verify_backtest_d5_step1.py exceeded "
            f"{G10_SUBPROCESS_TIMEOUT_SEC}s"
        )
        print(f"  cmd: {' '.join(cmd)}")
        print(f"  stdout tail:\n{out_tail}")
        print(f"  stderr tail:\n{err_tail}")
        return False, {
            "returncode": -1,
            "tail": (
                f"TIMEOUT after {G10_SUBPROCESS_TIMEOUT_SEC}s\n"
                f"--- stdout tail ---\n{out_tail}\n"
                f"--- stderr tail ---\n{err_tail}"
            ),
        }
    tail = "\n".join((proc.stdout or "").splitlines()[-15:])
    return proc.returncode == 0, {
        "returncode": proc.returncode,
        "tail": tail,
    }


# --- Driver -------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D5 STEP 2 verification @ {_now()}")
    print(f"  G11 window:    {G11_START} ~ {G11_END}  top_n={G11_TOP_N}")
    print(f"  5y sanity:     {SANITY_5Y_START} ~ {SANITY_5Y_END}  top_n={SANITY_TOP_N}")
    print("=" * 78)

    results: list[dict] = []
    all_ok = True

    # G11 first — its detail feeds G12's inline compare.
    g11_ok, g11_detail = gate_g11_parity()
    results.append({
        "gate": "G11 Core 3 + RSI2 + HODL parity",
        "verdict": "PASS" if g11_ok else "FAIL",
        "detail": g11_detail,
    })
    print(f"\n[{'PASS' if g11_ok else 'FAIL'}] G11 Core 3 + RSI2 + HODL parity")
    for k, v in g11_detail.items():
        if k not in ("hashes_expected_pr19",):
            print(f"    {k}: {v}")
    if not g11_ok:
        all_ok = False

    # G12 inline compare on donchian_20d
    g12_ok, g12_detail = gate_g12_step1_regression(g11_detail)
    results.append({
        "gate": "G12 STEP 1 regression (donchian_20d hash)",
        "verdict": "PASS" if g12_ok else "FAIL",
        "detail": g12_detail,
    })
    print(f"\n[{'PASS' if g12_ok else 'FAIL'}] G12 STEP 1 regression")
    for k, v in g12_detail.items():
        print(f"    {k}: {v}")
    if not g12_ok:
        all_ok = False

    # STEP 2 sanity 6mo dual
    s6_ok, s6_detail = gate_step2_sanity_6mo()
    results.append({
        "gate": "STEP 2 sanity 6mo NORMAL+STRESS",
        "verdict": "PASS" if s6_ok else "FAIL",
        "detail": s6_detail,
    })
    print(f"\n[{'PASS' if s6_ok else 'FAIL'}] STEP 2 sanity 6mo NORMAL+STRESS")
    for k, v in s6_detail.items():
        print(f"    {k}: {v}")
    if not s6_ok:
        all_ok = False

    # STEP 2 sanity 5y NORMAL
    s5_ok, s5_detail = gate_step2_sanity_5y()
    results.append({
        "gate": "STEP 2 sanity 5y NORMAL",
        "verdict": "PASS" if s5_ok else "FAIL",
        "detail": s5_detail,
    })
    print(f"\n[{'PASS' if s5_ok else 'FAIL'}] STEP 2 sanity 5y NORMAL")
    for k, v in s5_detail.items():
        print(f"    {k}: {v}")
    if not s5_ok:
        all_ok = False

    # G10 subprocess (PR #19 verifier)
    reg_ok, reg_detail = gate_g10_pr19_regression_subprocess()
    results.append({
        "gate": "G10 PR #19 regression subprocess",
        "verdict": "PASS" if reg_ok else "FAIL",
        "detail": reg_detail,
    })
    print(f"\n[{'PASS' if reg_ok else 'FAIL'}] G10 PR #19 regression subprocess")
    for k, v in reg_detail.items():
        if k == "tail":
            print(f"    {k}:")
            for line in str(v).splitlines():
                print(f"      {line}")
        else:
            print(f"    {k}: {v}")
    if not reg_ok:
        all_ok = False

    summary_path = VERIF_DIR / f"d5_step2_baseline_{_now().replace(':', '_')}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D5 STEP 2 (RSI2 Mean Reversion addition)",
                "decisions": {
                    "F9": "A — PR #19 머지 후 새 브랜치 from origin/master",
                    "F10": "B — RSI2 < 10 AND below LowerBB / SMA20-2σ",
                    "F11": "A — Close > SMA200 안전필터",
                    "F12": "B — z-score ascending",
                    "F13": "A — 6mo + 5y windows",
                    "F14": "A — 6mo NORMAL+STRESS dual + 5y NORMAL only",
                },
                "g11_window": {
                    "start": G11_START.isoformat(),
                    "end": G11_END.isoformat(),
                    "top_n": G11_TOP_N,
                },
                "sanity_5y_window": {
                    "start": SANITY_5Y_START.isoformat(),
                    "end": SANITY_5Y_END.isoformat(),
                    "top_n": SANITY_TOP_N,
                },
                "pr19_locked_hashes_normal": PR19_HASHES_NORMAL,
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
