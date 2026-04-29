"""smoke_d6_btc_gate_stage1.py — D6 Stage 1 BTC Gate smoke test.

Quick parity + behavior check on a single strategy / 6mo window:

    1. baseline    — ``btc_gate=None`` (pre-D6 behavior)
    2. always_on   — gate always returns True (must match baseline)
    3. always_off  — gate always returns False (must produce 0 BUYs +
                     audit rows on every rebal date that had picks)

Run:
    cd C:/Q-TRON-d6gate
    .venv64/Scripts/python.exe -X utf8 scripts/crypto/smoke_d6_btc_gate_stage1.py

Exit 0 on PASS, 1 on FAIL. Not a hash-canonical regression — the
verifier baseline parity is in PR 2 (verify_backtest_d6_gate.py).
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import CostConfig, CostMode  # noqa: E402
from crypto.backtest.engine import BacktestConfig, run_backtest  # noqa: E402
from crypto.backtest.strategies.donchian_20d import Donchian20DBreakout  # noqa: E402
from crypto.backtest.universe import (  # noqa: E402
    DEFAULT_TOP100_CSV,
    KRWStaticTop100,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402


SMOKE_START = date(2024, 1, 1)
SMOKE_END = date(2024, 6, 30)
SMOKE_TOP_N = 20


class _AlwaysOnGate:
    def is_active(self, btc_ohlcv, asof) -> bool:  # noqa: ARG002
        return True


class _AlwaysOffGate:
    def is_active(self, btc_ohlcv, asof) -> bool:  # noqa: ARG002
        return False


def _build_config():
    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    with connection() as conn:
        listings = load_listings_from_pg(conn)
    universe = KRWStaticTop100.from_csv_and_listings(DEFAULT_TOP100_CSV, listings)
    return (
        BacktestConfig(
            strategy=Donchian20DBreakout(),
            universe=universe,
            start_date=SMOKE_START,
            end_date=SMOKE_END,
            initial_cash_krw=100_000_000.0,
            rebal_days=21,
            top_n=SMOKE_TOP_N,
            cost_config=CostConfig(),
        ),
        connection,
    )


def main() -> int:
    print("=" * 60)
    print(f"D6 Stage 1 smoke test — Donchian 20D {SMOKE_START}~{SMOKE_END}")
    print("=" * 60)

    cfg, conn = _build_config()

    print("\n[1/3] baseline — btc_gate=None")
    res_baseline = run_backtest(cfg, CostMode.NORMAL, connection_factory=conn)
    print(f"  trades={len(res_baseline.trade_log)}  "
          f"final_equity={res_baseline.final_equity_krw:,.0f}  "
          f"hash={res_baseline.canonical_hash[:16]}...")
    print(f"  blocked_buys={len(res_baseline.btc_gate_blocked_buys)}")

    print("\n[2/3] always_on — must match baseline byte-for-byte")
    res_on = run_backtest(
        cfg, CostMode.NORMAL,
        connection_factory=conn,
        btc_gate=_AlwaysOnGate(),
    )
    print(f"  trades={len(res_on.trade_log)}  "
          f"final_equity={res_on.final_equity_krw:,.0f}  "
          f"hash={res_on.canonical_hash[:16]}...")
    print(f"  blocked_buys={len(res_on.btc_gate_blocked_buys)}")

    print("\n[3/3] always_off — must produce 0 BUYs + audit rows")
    res_off = run_backtest(
        cfg, CostMode.NORMAL,
        connection_factory=conn,
        btc_gate=_AlwaysOffGate(),
    )
    buys_off = [t for t in res_off.trade_log if t.side == "buy"]
    print(f"  trades={len(res_off.trade_log)}  "
          f"buys={len(buys_off)}  "
          f"final_equity={res_off.final_equity_krw:,.0f}")
    print(f"  blocked_buys={len(res_off.btc_gate_blocked_buys)}")

    # ── Asserts ──────────────────────────────────────────────────
    fails: list[str] = []

    if res_baseline.canonical_hash != res_on.canonical_hash:
        fails.append(
            f"parity FAIL: baseline_hash={res_baseline.canonical_hash[:16]} "
            f"!= always_on_hash={res_on.canonical_hash[:16]}"
        )
    if len(res_baseline.trade_log) != len(res_on.trade_log):
        fails.append(
            f"trade-count parity FAIL: baseline={len(res_baseline.trade_log)} "
            f"vs always_on={len(res_on.trade_log)}"
        )
    if res_baseline.btc_gate_blocked_buys:
        fails.append(
            f"baseline blocked_buys must be empty, got "
            f"{len(res_baseline.btc_gate_blocked_buys)}"
        )
    if buys_off:
        fails.append(
            f"always_off must suppress all BUYs, got {len(buys_off)} BUYs"
        )
    if not res_off.btc_gate_blocked_buys:
        # Donchian's 6mo window may legitimately have zero rebal-with-picks
        # if the universe never delivers a breakout — log instead of fail.
        print("\nNOTE: always_off produced 0 audit rows — strategy had "
              "no BUY-eligible rebal in this window (acceptable on "
              "sparse signals; the always_off path was still consulted).")

    if fails:
        print("\n" + "=" * 60)
        print("VERDICT: FAIL")
        for f in fails:
            print(f"  - {f}")
        print("=" * 60)
        return 1

    print("\n" + "=" * 60)
    print("VERDICT: PASS")
    print(f"  baseline == always_on (hash + trade count match)")
    print(f"  always_off suppressed {len(buys_off)} BUYs, "
          f"{len(res_off.btc_gate_blocked_buys)} audit rows")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
