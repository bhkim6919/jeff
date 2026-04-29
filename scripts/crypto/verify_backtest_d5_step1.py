"""D5 STEP 1 verification — Donchian 20D Breakout addition.

Gates exercised here:

    G9   Core 3 + HODL parity at the PR #3 6-month window:
         - momentum_12_1, sma_50_200_trend, atr_breakout, btc_hodl
           reproduce their PR #3 NORMAL canonical hashes byte-identical
           (proof: STEP 1 changes did NOT affect existing strategies'
           engine path).
         - donchian_20d produces a hash distinct from all of the above.
         - All five strategies return finite 6-metric outputs.

    STEP 1 sanity 6mo (NORMAL + STRESS dual, Jeff F8=B):
         - donchian_20d produces finite metrics under both modes.
         - mdd <= 0, exposure_pct in [0, 100] under both modes.
         - NORMAL hash != STRESS hash (cost is actually applied differently).

    STEP 1 sanity 5y (NORMAL only, Jeff F5=C + 보완 #5):
         - donchian_20d on 2021-01-01 ~ 2026-04-26 produces finite metrics.
         - trades >= 1  (rule applied ONLY to the 5-year window per Jeff).

    G10 PR #3 regression (subprocess):
         - ``verify_backtest_multi.py`` exits 0, which itself re-runs:
             * G5 multi-strategy parity (D4 4 strategies)
             * G8 BTC HODL sanity
             * G4/G6 inline (Momentum hash == PR #2 lock)
             * G4/G6 subprocess (verify_backtest_engine.py + foundation)
             * Sparse-universe 5-year reconfirm
         - This gate proves the STEP 1 additions don't perturb any
           previously-locked behavior.

Verifier window for G9 + STEP1 6mo: 2020-01-01 ~ 2020-06-30, top_n=20
(matches PR #2/#3 — required for the byte-identical hash compare).
BTC HODL keeps top_n=1.

5-year sanity window: 2021-01-01 ~ 2026-04-26, top_n=20.

Per Jeff D5 STEP 1 lock:
    - Engine / cost_model / universe untouched.
    - 20D Breakout is a D5 survivor candidate; momentum_12_1 stays only
      as engine canary for hash regression.

Exit:
    0 — all gates PASS
    1 — at least one gate FAIL
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
import time
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
from crypto.backtest.strategies.sma_50_200 import SMA50_200Trend  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)

# PR #3 NORMAL hashes — byte-identical regression target.
PR3_HASHES_NORMAL = {
    "momentum_12_1":    "76d392cd9ffd110f4b44216eb7ec8b31db2242502d47bd808e819a796b71fd8d",
    "sma_50_200_trend": "d164b15c3b1d86faa21bc2e45bfdbd63efdd940bdaab2e8ef4686c1c823449e0",
    "atr_breakout":     "af24c4f64d23c38d16b2fbe626666668fc6e24d6ed901d36869374250d247529",
    "btc_hodl":         "655df2ec147d50d0b53f8dfc8e3c1e1d34b44074d973289dc4b8f96586fd64e5",
}

# G9 + STEP1 6mo regression window (matches PR #2/#3).
G9_START = date(2020, 1, 1)
G9_END = date(2020, 6, 30)
G9_TOP_N = 20

# 5-year sanity window.
SANITY_5Y_START = date(2021, 1, 1)
SANITY_5Y_END = date(2026, 4, 26)
SANITY_TOP_N = 20

# Yearly-sharded 5y regression — timeout isolation topology
# (Jeff 2026-04-29: replace single 5y subprocess with yearly shards so a
# slow / hanging shard cannot block the whole verifier chain).
SHARD_TIMEOUT_SEC = 900       # per-shard hang guard (Jeff bound: 600~900)
SHARDED_GATE_BUDGET_SEC = 3000  # whole-gate budget. Empirical 2026-04-29
                                # run: 4 shards completed in ~16 min (240s
                                # avg) → 6 shards ~24 min, plus headroom
                                # for the slowest year. step2's matching
                                # G10 subprocess timeout is now 5400s so
                                # step1's full chain (G9 + 6mo dual +
                                # sharded + G10_nested) fits inside.

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


# --- G9 Core 3 + HODL parity --------------------------------------------


def gate_g9_core3_plus_hodl_parity() -> tuple[bool, dict]:
    """All 4 D4 strategies reproduce PR #3 NORMAL hashes; donchian_20d
    distinct."""
    print("\n[G9] Core 3 + HODL parity — 5 strategies on G9 window NORMAL")
    base, conn = _build_config(Momentum12_1(), start=G9_START, end=G9_END, top_n=G9_TOP_N)
    trading = run_multi(
        base,
        [Momentum12_1(), SMA50_200Trend(), ATRBreakout(), Donchian20DBreakout()],
        CostMode.NORMAL,
        connection_factory=conn,
    )
    bench_cfg, _ = _build_config(BTCHodl(), start=G9_START, end=G9_END, top_n=1)
    bench = run_backtest(bench_cfg, CostMode.NORMAL, connection_factory=conn)

    actual: dict[str, str] = {}
    for name, res in trading.items():
        actual[name] = res.canonical_hash
    actual["btc_hodl"] = bench.canonical_hash

    issues: list[str] = []

    # Existing 4 hashes byte-identical
    for name, expected in PR3_HASHES_NORMAL.items():
        got = actual.get(name)
        if got != expected:
            issues.append(
                f"{name} hash drift: expected {expected[:16]}... got "
                f"{got[:16] if got else 'None'}..."
            )

    # donchian_20d distinct from all others
    don_hash = actual.get("donchian_20d")
    if don_hash is None:
        issues.append("donchian_20d missing from run_multi output")
    else:
        for n, h in actual.items():
            if n != "donchian_20d" and h == don_hash:
                issues.append(f"donchian_20d hash collides with {n}")

    # Finite 6-metric output for every strategy + benchmark
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
        "hashes_expected_pr3": PR3_HASHES_NORMAL,
        "donchian_20d_metrics": {
            k: round(float(trading["donchian_20d"].metrics[k]), 6)
            for k in REQUIRED_METRIC_KEYS
        },
        "donchian_20d_trades": len(trading["donchian_20d"].trade_log),
        "donchian_20d_rebal_executed": len(trading["donchian_20d"].rebal_dates_executed),
        "issues": issues,
    }
    return not issues, detail


# --- STEP 1 sanity 6mo dual ---------------------------------------------


def gate_step1_sanity_6mo() -> tuple[bool, dict]:
    """Donchian 20D NORMAL + STRESS finite + sane on the regression window."""
    print("\n[STEP 1 6mo] Donchian 20D NORMAL + STRESS dual")
    cfg, conn = _build_config(
        Donchian20DBreakout(), start=G9_START, end=G9_END, top_n=G9_TOP_N
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

    if res_n.canonical_hash == res_s.canonical_hash:
        issues.append("normal_hash == stress_hash (cost should differ between modes)")

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


# --- STEP 1 sanity 5y NORMAL only (CONTINUOUS — extended/nightly gate) --


def gate_step1_sanity_5y() -> tuple[bool, dict]:
    """Donchian 20D over the full continuous 5+ year window — must trade
    at least once and produce finite metrics. Per Jeff 보완 #5 the
    trades>=1 rule applies ONLY to this 5-year window (not the 6-month
    regression window, where the strict fresh-cross rule may legitimately
    produce zero trades).

    PRESERVED — does not run in the default verifier chain because the
    single ~30+ minute subprocess hits step2's 1800s G10 timeout once
    chained with 6mo dual + G10_nested. Activated by ``--extended`` or
    ``D5_EXTENDED_5Y_CONTINUOUS=1`` for nightly / on-demand runs.

    Performance evaluation MUST use this gate, not the yearly-sharded
    one — the shards reset state at year boundaries and are timeout
    isolation evidence only.
    """
    print("\n[STEP 1 5y CONTINUOUS] Donchian 20D 2021~2026 NORMAL — trades>=1, finite")
    cfg, conn = _build_config(
        Donchian20DBreakout(),
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


# --- STEP 1 sanity 5y NORMAL — yearly sharded regression ----------------
#
# Topology change ONLY (Jeff 2026-04-29). The strategy and engine are
# untouched. The full 2021-01-01 ~ 2026-04-26 window is split into yearly
# shards, each run as an independent subprocess with its own timeout and
# its own evidence file. The aggregator records per-shard PASS / FAIL /
# TIMEOUT.
#
# CRITICAL: yearly shards are FRESH-STATE restarts. Cash, positions, peak
# equity all reset at every year boundary. Per-shard CAGR / MDD / Sharpe
# / trades are NOT comparable to the full continuous 5y backtest. This
# gate exists for **timeout isolation regression evidence only** — full
# performance evaluation continues to use the continuous gate above
# (run via --extended).


def gate_step1_yearly_shard(
    shard_start: date,
    shard_end: date,
    top_n: int = SANITY_TOP_N,
) -> tuple[bool, dict]:
    """Run a single yearly shard — invoked from subprocess shard-mode entry.

    Same finite/sane checks as gate_step1_sanity_5y, but the trades>=1
    rule is RELAXED (a single year may legitimately have zero entries
    if the breakout regime never triggers).
    """
    cfg, conn = _build_config(
        Donchian20DBreakout(),
        start=shard_start,
        end=shard_end,
        top_n=top_n,
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
    # NOTE: trades>=1 NOT enforced per shard — a single year may
    # legitimately have zero entries.

    detail = {
        "shard_start": shard_start.isoformat(),
        "shard_end": shard_end.isoformat(),
        "metrics": {k: round(float(res.metrics[k]), 6) for k in REQUIRED_METRIC_KEYS},
        "trades": len(res.trade_log),
        "rebal_executed": len(res.rebal_dates_executed),
        "rebal_skipped": len(res.rebal_dates_skipped),
        "final_equity_krw": round(res.final_equity_krw, 2),
        "canonical_hash": res.canonical_hash,
        "issues": issues,
        "topology_note": (
            "yearly shard, fresh-state restart at year boundary — "
            "NOT equivalent to continuous 5y; per-shard metrics are "
            "for timeout isolation regression only."
        ),
    }
    return not issues, detail


def _yearly_shard_windows() -> list[tuple[date, date]]:
    """Compute yearly shard windows clamped to [SANITY_5Y_START,
    SANITY_5Y_END]. The first / last shards are partial when the
    boundaries fall mid-year."""
    shards: list[tuple[date, date]] = []
    yr = SANITY_5Y_START.year
    while yr <= SANITY_5Y_END.year:
        s = date(yr, 1, 1)
        e = date(yr, 12, 31)
        if s < SANITY_5Y_START:
            s = SANITY_5Y_START
        if e > SANITY_5Y_END:
            e = SANITY_5Y_END
        shards.append((s, e))
        yr += 1
    return shards


def gate_step1_sanity_5y_yearly_sharded() -> tuple[bool, dict]:
    """Run the 5y NORMAL window as independent yearly subprocess shards.

    Each shard:
        * launched as ``python verify_backtest_d5_step1.py --shard-mode
          --shard-start YYYY-MM-DD --shard-end YYYY-MM-DD --shard-output PATH``
        * gets its own timeout (SHARD_TIMEOUT_SEC = 900s)
        * writes its own evidence JSON to crypto/data/_verification/

    Aggregator records yearly_shards_passed / failed / timeout lists.
    Once the cumulative gate budget (SHARDED_GATE_BUDGET_SEC) is
    exhausted, remaining shards are marked SKIPPED so step1 stays well
    under step2's 1800s G10 timeout even if a shard exits early.

    Gate verdict:
        PASS  iff every shard PASSed and at least one shard recorded
              a real trade (cumulative trades >= 1 across all shards)
              AND no shard FAILed / TIMEOUTed / was SKIPPED.
        FAIL  otherwise.

    The gate failure mode preserves all completed evidence files —
    callers can inspect per-shard JSONs for the partial signal even
    when one shard hangs.
    """
    print(
        "\n[STEP 1 5y SHARDED] Donchian 20D yearly subprocess shards "
        f"(timeout={SHARD_TIMEOUT_SEC}s/shard, budget={SHARDED_GATE_BUDGET_SEC}s)"
    )
    print(
        "  WARN: yearly shards are fresh-state restarts — per-shard "
        "metrics are NOT equivalent to continuous 5y. Topology change "
        "for timeout isolation only."
    )

    shards = _yearly_shard_windows()
    shard_results: list[dict] = []
    passed: list[int] = []
    failed: list[int] = []
    timeout_list: list[int] = []
    skipped: list[int] = []
    cumulative_elapsed = 0.0
    total_trades = 0

    for s, e in shards:
        year_label = s.year
        if cumulative_elapsed >= SHARDED_GATE_BUDGET_SEC:
            print(
                f"  [SKIPPED] shard {year_label} ({s} ~ {e}) — "
                f"gate budget exhausted ({cumulative_elapsed:.0f}s)"
            )
            shard_results.append({
                "shard_start": s.isoformat(),
                "shard_end": e.isoformat(),
                "status": "SKIPPED",
                "elapsed_sec": 0.0,
                "timeout": False,
                "evidence_path": None,
                "cagr": None, "mdd": None, "sharpe": None, "trades": None,
                "reason": "gate_budget_exhausted",
            })
            skipped.append(year_label)
            continue

        shard_evidence = (
            VERIF_DIR
            / f"d5_step1_yearly_shard_{year_label}_"
              f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        )
        cmd = [
            sys.executable, "-X", "utf8", str(HERE),
            "--shard-mode",
            "--shard-start", s.isoformat(),
            "--shard-end", e.isoformat(),
            "--shard-output", str(shard_evidence),
        ]
        t0 = time.monotonic()
        timeout_flag = False
        stdout_tail = ""
        stderr_tail = ""
        returncode: int | None = None
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8",
                timeout=SHARD_TIMEOUT_SEC,
            )
            returncode = proc.returncode
            stdout_tail = "\n".join((proc.stdout or "").splitlines()[-10:])
            stderr_tail = "\n".join((proc.stderr or "").splitlines()[-10:])
        except subprocess.TimeoutExpired as exc:
            timeout_flag = True
            returncode = -1
            stdout_tail = (
                "\n".join((exc.stdout or "").splitlines()[-10:])
                if exc.stdout else "(empty)"
            )
            stderr_tail = (
                "\n".join((exc.stderr or "").splitlines()[-10:])
                if exc.stderr else "(empty)"
            )
            print(
                f"  [TIMEOUT] shard {year_label} exceeded "
                f"{SHARD_TIMEOUT_SEC}s — subprocess killed"
            )
        elapsed = time.monotonic() - t0
        cumulative_elapsed += elapsed

        # Decide status
        if timeout_flag:
            status = "TIMEOUT"
            timeout_list.append(year_label)
        elif returncode == 0:
            status = "PASS"
            passed.append(year_label)
        else:
            status = "FAIL"
            failed.append(year_label)

        # Read evidence if shard wrote it
        shard_metrics: dict = {}
        if shard_evidence.exists():
            try:
                shard_metrics = json.loads(
                    shard_evidence.read_text(encoding="utf-8")
                )
            except Exception:
                pass

        m = shard_metrics.get("metrics", {}) or {}
        entry = {
            "shard_start": s.isoformat(),
            "shard_end": e.isoformat(),
            "status": status,
            "elapsed_sec": round(elapsed, 1),
            "timeout": timeout_flag,
            "returncode": returncode,
            "evidence_path": (
                str(shard_evidence.relative_to(WORKTREE_ROOT))
                if shard_evidence.exists() else None
            ),
            "cagr": m.get("cagr"),
            "mdd": m.get("mdd"),
            "sharpe": m.get("sharpe"),
            "trades": shard_metrics.get("trades"),
            "stdout_tail": stdout_tail,
        }
        if timeout_flag:
            entry["stderr_tail"] = stderr_tail
        shard_results.append(entry)
        if shard_metrics.get("trades"):
            total_trades += int(shard_metrics["trades"])

        print(
            f"  [{status}] shard {year_label} ({s} ~ {e})  "
            f"elapsed={elapsed:.1f}s  trades={shard_metrics.get('trades', 'n/a')}  "
            f"cumulative={cumulative_elapsed:.0f}s"
        )

    issues: list[str] = []
    if failed:
        issues.append(f"yearly_shards_failed: {failed}")
    if timeout_list:
        issues.append(f"yearly_shards_timeout: {timeout_list}")
    if skipped:
        issues.append(f"yearly_shards_skipped: {skipped} (gate budget)")
    if not failed and not timeout_list and not skipped and total_trades < 1:
        issues.append(
            "all shards passed but cumulative trades < 1 — "
            "strategy never entered across the full window"
        )

    detail = {
        "evidence_label": "5y_yearly_sharded_regression",
        "topology_warning": (
            "Yearly shards are independent fresh-state subprocess "
            "backtests. Cash / positions / peak equity reset at every "
            "year boundary. Per-shard CAGR / MDD / Sharpe / trades "
            "are NOT directly comparable to a continuous 5y backtest. "
            "This gate certifies timeout isolation + finite/sane "
            "execution per shard ONLY. Performance evaluation MUST "
            "use the continuous gate (run via --extended)."
        ),
        "shard_count": len(shards),
        "shard_timeout_sec": SHARD_TIMEOUT_SEC,
        "gate_budget_sec": SHARDED_GATE_BUDGET_SEC,
        "yearly_shards_passed": passed,
        "yearly_shards_failed": failed,
        "yearly_shards_timeout": timeout_list,
        "yearly_shards_skipped": skipped,
        "cumulative_elapsed_sec": round(cumulative_elapsed, 1),
        "cumulative_trades_across_shards": total_trades,
        "shards": shard_results,
        "issues": issues,
    }
    return not issues, detail


# --- G10 PR #3 regression (subprocess) ----------------------------------


def gate_g10_pr3_regression_subprocess() -> tuple[bool, dict]:
    """verify_backtest_multi.py exit 0 — proves D4 lock fully preserved.

    Hang guard: 1800s timeout — same pattern as step2's G10 patch."""
    print("\n[G10] PR #3 regression (verify_backtest_multi.py subprocess)")
    cmd = [
        sys.executable,
        "-X", "utf8",
        str(WORKTREE_ROOT / "scripts" / "crypto" / "verify_backtest_multi.py"),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              encoding="utf-8", timeout=1800)
    except subprocess.TimeoutExpired as exc:
        out_tail = "\n".join((exc.stdout or "").splitlines()[-15:]) if exc.stdout else "(empty)"
        err_tail = "\n".join((exc.stderr or "").splitlines()[-15:]) if exc.stderr else "(empty)"
        print(f"[TIMEOUT] verify_backtest_multi.py exceeded 1800s")
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
    tail = "\n".join((proc.stdout or "").splitlines()[-15:])
    return proc.returncode == 0, {
        "returncode": proc.returncode,
        "tail": tail,
    }


# --- Driver -------------------------------------------------------------


def _main_shard_mode(args: argparse.Namespace) -> int:
    """Subprocess entry point — runs a single yearly shard and writes
    its evidence JSON to ``--shard-output``. Exit 0 on PASS, 1 on FAIL.
    Invoked by ``gate_step1_sanity_5y_yearly_sharded``."""
    s = date.fromisoformat(args.shard_start)
    e = date.fromisoformat(args.shard_end)
    print(f"\n[STEP 1 SHARD] Donchian 20D {s} ~ {e}  top_n={SANITY_TOP_N}")
    print(
        "  WARN: yearly shard, fresh-state restart — NOT continuous "
        "5y equivalent. Topology change for timeout isolation only."
    )

    ok, detail = gate_step1_yearly_shard(s, e, top_n=SANITY_TOP_N)

    output_path = Path(args.shard_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(
            detail, ensure_ascii=False, indent=2, default=str, sort_keys=True
        ),
        encoding="utf-8",
    )
    try:
        rel_or_abs = output_path.relative_to(WORKTREE_ROOT)
    except ValueError:
        rel_or_abs = output_path  # outside worktree — print absolute path
    print(f"  shard evidence -> {rel_or_abs}")
    print(f"  verdict: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="D5 STEP 1 verifier (Donchian 20D Breakout addition).",
    )
    parser.add_argument(
        "--shard-mode", action="store_true",
        help="Subprocess entry — run a single yearly shard backtest and "
             "write evidence JSON. Used internally by the sharded 5y gate.",
    )
    parser.add_argument(
        "--shard-start", help="Yearly shard start date (ISO YYYY-MM-DD)",
    )
    parser.add_argument(
        "--shard-end", help="Yearly shard end date (ISO YYYY-MM-DD)",
    )
    parser.add_argument(
        "--shard-output", help="Yearly shard evidence output JSON path",
    )
    parser.add_argument(
        "--extended", action="store_true",
        help="Run the full continuous 5y NORMAL gate (in addition to "
             "the yearly-sharded one). Intended for nightly / on-demand "
             "use; default chain skips it because its single ~30+ min "
             "subprocess hits step2's 1800s G10 timeout. Equivalent to "
             "setting D5_EXTENDED_5Y_CONTINUOUS=1 in the environment.",
    )
    args = parser.parse_args(argv)

    if args.shard_mode:
        if not (args.shard_start and args.shard_end and args.shard_output):
            parser.error(
                "--shard-mode requires --shard-start, --shard-end, "
                "--shard-output"
            )
        return _main_shard_mode(args)

    extended = args.extended or os.environ.get(
        "D5_EXTENDED_5Y_CONTINUOUS"
    ) == "1"

    print("=" * 78)
    print(f"D5 STEP 1 verification @ {_now()}")
    print(f"  G9 window:    {G9_START} ~ {G9_END}  top_n={G9_TOP_N}")
    print(f"  5y sanity:    {SANITY_5Y_START} ~ {SANITY_5Y_END}  top_n={SANITY_TOP_N}")
    print(f"  5y mode:      yearly_sharded"
          + ("  (+ continuous extended)" if extended else ""))
    print("=" * 78)

    gates: list[tuple[str, callable]] = [
        ("G9 Core 3 + HODL parity", gate_g9_core3_plus_hodl_parity),
        ("STEP 1 sanity 6mo NORMAL+STRESS", gate_step1_sanity_6mo),
        ("STEP 1 sanity 5y yearly sharded", gate_step1_sanity_5y_yearly_sharded),
    ]
    if extended:
        # Nightly / on-demand only — the continuous 5y gate's single
        # long subprocess is what motivates the sharded topology in
        # the first place. Run it AFTER the sharded gate so partial
        # evidence survives even if continuous hangs.
        gates.append(
            ("STEP 1 sanity 5y CONTINUOUS (extended)", gate_step1_sanity_5y)
        )
    gates.append(
        ("G10 PR #3 regression subprocess", gate_g10_pr3_regression_subprocess)
    )

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
            if k == "tail":
                print(f"    {k}:")
                for line in str(v).splitlines():
                    print(f"      {line}")
            else:
                print(f"    {k}: {v}")
        if not ok:
            all_ok = False

    summary_path = VERIF_DIR / f"d5_step1_baseline_{_now().replace(':', '_')}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D5 STEP 1 (Donchian 20D Breakout addition)",
                "decisions": {
                    "F0": "A — STEP 1 single PR (20D only)",
                    "F1": "C — Cross condition + SMA50 filter",
                    "F2": "A — breakout strength rank",
                    "F3": "A — engine rebal exit only (no SL/trail/time)",
                    "F4": "C — momentum_12_1 = engine canary",
                    "F5": "C — 6mo regression + 5y sanity",
                    "F6": "A — lookback 21d signal core",
                    "F7": "A — BTC Risk Gate excluded (separate PR)",
                    "F8": "B — NORMAL + STRESS dual on 6mo window",
                    "F9_5y_topology": (
                        "yearly_sharded (default) — full continuous 5y "
                        "preserved via --extended for performance "
                        "evaluation only (Jeff 2026-04-29)"
                    ),
                },
                "extended_continuous_5y": extended,
                "g9_window": {
                    "start": G9_START.isoformat(),
                    "end": G9_END.isoformat(),
                    "top_n": G9_TOP_N,
                },
                "sanity_5y_window": {
                    "start": SANITY_5Y_START.isoformat(),
                    "end": SANITY_5Y_END.isoformat(),
                    "top_n": SANITY_TOP_N,
                    "topology": (
                        "yearly_sharded — performance NOT comparable "
                        "to continuous"
                    ),
                },
                "shard_timeout_sec": SHARD_TIMEOUT_SEC,
                "sharded_gate_budget_sec": SHARDED_GATE_BUDGET_SEC,
                "pr3_locked_hashes_normal": PR3_HASHES_NORMAL,
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
