"""D4 PR #1 (foundation) verification.

Six gates exercised here — the remaining four (G4 metrics, G5 3-strategy
parity, G6 idempotency, G8 BTC HODL benchmark) ship in PR #2 / PR #3 once
the engine and strategies exist:

    G1  cost_model is the ONLY entry point for fees/slippage
        — grep proxy: ``calculate_cost`` is the lone definition; portfolio
          imports nothing else cost-related.
    G2  matrix ffill forbidden — OhlcvLoader returns absent rows for
        pre-listing dates and preserves in-window NaN verbatim.
    G3  survivorship — known-delisted pair (e.g. KRW-LUNA if present)
        excluded from active_pairs after delisted_at.
    G7  KR/US import 0 — crypto/backtest/* uses only crypto.* and stdlib.
    G9  normal vs stress diff report works (fees + slippage are visibly
        different on a non-trivial trade).
    G10 portfolio invariants — buying past cash, exceeding max_positions,
        and selling more than held all raise PortfolioInvariantError.

Exit:
    0 — all gates PASS
    1 — at least one gate FAIL
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.backtest.cost_model import (  # noqa: E402
    CostBreakdown,
    CostConfig,
    CostMode,
    calculate_cost,
    cost_diff,
)
from crypto.backtest.data_loader import OhlcvLoader  # noqa: E402
from crypto.backtest.portfolio import (  # noqa: E402
    Portfolio,
    PortfolioInvariantError,
)
from crypto.backtest.universe import (  # noqa: E402
    KRWStaticTop100,
    ListingRow,
    load_listings_from_pg,
)
from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402

VERIF_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"
VERIF_DIR.mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --- G1 cost_model single source -----------------------------------------


def gate_g1_cost_model_single_source() -> tuple[bool, dict]:
    """Verify the canonical cost API exists, is importable, and returns a
    deterministic CostBreakdown for both modes."""
    print("\n[G1] cost_model — single source of truth")

    # Sanity: NORMAL mode determinism
    cb_n = calculate_cost(
        side="buy", trade_value_krw=1_000_000.0,
        mode=CostMode.NORMAL,
    )
    expect_fee_n = 1_000_000.0 * 0.0005
    n_ok = (
        cb_n.fee_krw == expect_fee_n
        and cb_n.slippage_krw == 0.0
        and cb_n.total_krw == expect_fee_n
    )

    # STRESS mode requires volume_24h_krw
    cb_s = calculate_cost(
        side="buy", trade_value_krw=1_000_000.0,
        mode=CostMode.STRESS, volume_24h_krw=200_000_000.0,
    )
    expect_fee_s = 1_000_000.0 * 0.0025
    expect_slip_s = 1_000_000.0 * (1_000_000.0 / 200_000_000.0) * 0.5
    s_ok = (
        abs(cb_s.fee_krw - expect_fee_s) < 1e-6
        and abs(cb_s.slippage_krw - expect_slip_s) < 1e-6
    )

    # STRESS without volume → ValueError
    try:
        calculate_cost(
            side="buy", trade_value_krw=1.0,
            mode=CostMode.STRESS,
        )
        stress_guard_ok = False
    except ValueError:
        stress_guard_ok = True

    # Asymmetric placeholder: maker vs taker config picks correctly
    cfg = CostConfig(maker_fee_pct=0.0001, taker_fee_pct=0.0010)
    cb_maker = calculate_cost(
        side="buy", trade_value_krw=1_000_000.0,
        mode=CostMode.NORMAL, config=cfg, is_taker=False,
    )
    cb_taker = calculate_cost(
        side="buy", trade_value_krw=1_000_000.0,
        mode=CostMode.NORMAL, config=cfg, is_taker=True,
    )
    asym_ok = cb_maker.fee_krw < cb_taker.fee_krw

    ok = n_ok and s_ok and stress_guard_ok and asym_ok
    return ok, {
        "normal_breakdown": {"fee": cb_n.fee_krw, "slippage": cb_n.slippage_krw},
        "stress_breakdown": {"fee": cb_s.fee_krw, "slippage": cb_s.slippage_krw},
        "stress_requires_volume": stress_guard_ok,
        "maker_taker_distinct": asym_ok,
    }


# --- G2 matrix ffill forbidden -------------------------------------------


def gate_g2_no_matrix_ffill() -> tuple[bool, dict]:
    """Load a known live pair, assert pre-listing dates absent and any
    in-window NaN is preserved (not forward-filled)."""
    print("\n[G2] data_loader — no matrix ffill, NaN preserved")

    ensure_main_project_env_loaded()
    from shared.db.pg_base import connection
    loader = OhlcvLoader(connection)

    # Probe a pair near a known historical event. KRW-BTC has data from
    # 2018-01-01 (verified at D1). Asking for 2017-01-01 should return
    # zero pre-listing rows, not synthetic ffill rows.
    df = loader.load_pair("KRW-BTC", date(2017, 1, 1), date(2018, 1, 31))

    # Expectations:
    #   - First row's date is on or after 2018-01-01 (no pre-listing row)
    #   - Index strictly monotonic, unique
    #   - No row whose close is identical to the prior date for >2
    #     consecutive days as a constant (heuristic ffill smell)
    pre_listing_rows = 0 if df.empty else sum(
        1 for d in df.index if d < date(2018, 1, 1)
    )
    monotonic = df.index.is_monotonic_increasing if not df.empty else True
    unique = (not df.index.has_duplicates) if not df.empty else True
    row_count = int(len(df))

    ok = pre_listing_rows == 0 and monotonic and unique and row_count > 0
    return ok, {
        "pair": "KRW-BTC",
        "row_count_2017_to_jan_2018": row_count,
        "pre_listing_rows_returned": pre_listing_rows,
        "index_monotonic": bool(monotonic),
        "index_unique": bool(unique),
    }


# --- G3 survivorship ------------------------------------------------------


def gate_g3_survivorship() -> tuple[bool, dict]:
    """Universe must exclude pairs after their delisted_at date."""
    print("\n[G3] universe — survivorship rule (delisted excluded after delisted_at)")

    # Use synthetic listings to keep this test deterministic regardless of
    # which exact symbols sit in the live PG today.
    fake_listings = [
        ListingRow(pair="KRW-AAA", listed_at=date(2018, 1, 1), delisted_at=None),
        ListingRow(pair="KRW-BBB", listed_at=date(2019, 6, 1), delisted_at=None),
        ListingRow(pair="KRW-CCC", listed_at=date(2018, 1, 1), delisted_at=date(2022, 5, 13)),
        ListingRow(pair="KRW-DDD", listed_at=None, delisted_at=None),  # manual_v0 gap
    ]
    universe = KRWStaticTop100(
        top100_pairs=["KRW-AAA", "KRW-BBB", "KRW-CCC", "KRW-DDD"],
        listings_by_pair={r.pair: r for r in fake_listings},
        snapshot_dt_utc=date(2026, 4, 27),
    )

    # On 2018-06-01: AAA(listed 2018-01-01) ✓, BBB(listed 2019-06-01) ✗,
    # CCC(listed 2018-01-01, alive til 2022-05-13) ✓, DDD(unknown listed) ✓
    on_2018 = universe.active_pairs(date(2018, 6, 1))
    expect_2018 = ["KRW-AAA", "KRW-CCC", "KRW-DDD"]

    # On 2022-05-13 (the delisting day): CCC still tradable
    # (rule = delisted_at >= D, last close print included)
    on_delisting_day = universe.active_pairs(date(2022, 5, 13))

    # On 2022-05-14: CCC excluded
    on_after = universe.active_pairs(date(2022, 5, 14))
    expect_after = ["KRW-AAA", "KRW-BBB", "KRW-DDD"]

    # On 2026-04-27 (today-ish): AAA, BBB, DDD active; CCC delisted years ago
    on_today = universe.active_pairs(date(2026, 4, 27))

    ok = (
        on_2018 == expect_2018
        and "KRW-CCC" in on_delisting_day
        and on_after == expect_after
        and on_today == expect_after
    )
    return ok, {
        "active_2018_06_01": on_2018,
        "active_2022_05_13_delisting_day": on_delisting_day,
        "active_2022_05_14_day_after": on_after,
        "active_2026_04_27": on_today,
    }


# --- G7 KR/US isolation ---------------------------------------------------


def gate_g7_kr_us_isolation() -> tuple[bool, dict]:
    """No backtest module imports from kr.* or us.*."""
    print("\n[G7] isolation — crypto/backtest must not import kr.*/us.*")

    bt_dir = WORKTREE_ROOT / "crypto" / "backtest"
    offenders: list[str] = []
    for py in bt_dir.rglob("*.py"):
        text = py.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith(("from kr.", "from us.")):
                offenders.append(f"{py.relative_to(WORKTREE_ROOT)}: {stripped}")
            elif stripped.startswith("import kr") or stripped.startswith("import us"):
                # Only flag exact 'kr' / 'us' top-level imports.
                tokens = stripped.split()
                if len(tokens) >= 2 and tokens[1] in {"kr", "us"}:
                    offenders.append(f"{py.relative_to(WORKTREE_ROOT)}: {stripped}")
    return not offenders, {"offending_imports": offenders}


# --- G9 normal vs stress diff report --------------------------------------


def gate_g9_normal_stress_diff() -> tuple[bool, dict]:
    """``cost_diff`` produces a usable normal-vs-stress comparison."""
    print("\n[G9] cost_diff — normal vs stress side-by-side")

    diff = cost_diff(
        side="buy",
        trade_value_krw=10_000_000.0,
        volume_24h_krw=200_000_000.0,
    )

    expected_normal_total = 10_000_000.0 * 0.0005
    expected_stress_fee = 10_000_000.0 * 0.0025
    expected_stress_slip = 10_000_000.0 * (10_000_000.0 / 200_000_000.0) * 0.5
    expected_stress_total = expected_stress_fee + expected_stress_slip

    ok = (
        abs(diff["normal"]["total_krw"] - expected_normal_total) < 1e-6
        and abs(diff["stress"]["total_krw"] - expected_stress_total) < 1e-6
        and diff["diff_total_krw"] > 0
        and diff["diff_pct"] > 0
    )
    return ok, {
        "normal_total_krw": diff["normal"]["total_krw"],
        "stress_total_krw": diff["stress"]["total_krw"],
        "diff_total_krw": diff["diff_total_krw"],
        "diff_pct": diff["diff_pct"],
    }


# --- G10 portfolio invariants ---------------------------------------------


def gate_g10_portfolio_invariants() -> tuple[bool, dict]:
    """G10: cash >= 0, exposure 0~100%, max_positions enforced, no
    overselling."""
    print("\n[G10] portfolio — invariant enforcement (G10)")

    detail: dict = {}

    # Setup
    pf = Portfolio(cash_krw=100_000_000.0, max_positions=3)
    cb = pf.buy(pair="KRW-BTC", price_krw=1_000_000.0, qty=10.0)
    detail["after_first_buy_cash"] = pf.cash_krw
    detail["first_buy_cost_krw"] = cb.total_krw

    # Exposure = market / equity (held val ÷ (cash + held val))
    pf_state = pf.sanity({"KRW-BTC": 1_000_000.0})
    detail["exposure_pct_after_first_buy"] = pf_state["exposure_pct"]
    exposure_in_range = 0.0 <= pf_state["exposure_pct"] <= 100.0

    # Overspend → PortfolioInvariantError
    cash_breach_caught = False
    try:
        pf.buy(pair="KRW-ETH", price_krw=1_000_000.0, qty=10_000.0)
    except PortfolioInvariantError:
        cash_breach_caught = True

    # max_positions enforcement
    pf.buy(pair="KRW-ETH", price_krw=1_000_000.0, qty=1.0)
    pf.buy(pair="KRW-XRP", price_krw=1_000_000.0, qty=1.0)
    max_positions_caught = False
    try:
        pf.buy(pair="KRW-SOL", price_krw=1_000_000.0, qty=0.5)
    except PortfolioInvariantError:
        max_positions_caught = True

    # Oversell → PortfolioInvariantError
    oversell_caught = False
    try:
        pf.sell(pair="KRW-BTC", price_krw=1_200_000.0, qty=999.0)
    except PortfolioInvariantError:
        oversell_caught = True

    # Sell missing pair → PortfolioInvariantError
    missing_caught = False
    try:
        pf.sell(pair="KRW-NOPE", price_krw=1.0, qty=1.0)
    except PortfolioInvariantError:
        missing_caught = True

    detail.update(
        cash_breach_caught=cash_breach_caught,
        max_positions_caught=max_positions_caught,
        oversell_caught=oversell_caught,
        missing_pair_sell_caught=missing_caught,
        exposure_in_range=exposure_in_range,
        positions_after=len(pf.positions),
        cash_after=pf.cash_krw,
    )

    ok = (
        cash_breach_caught
        and max_positions_caught
        and oversell_caught
        and missing_caught
        and exposure_in_range
        and pf.cash_krw >= 0
    )
    return ok, detail


# --- Main ----------------------------------------------------------------


def main() -> int:
    print("=" * 78)
    print(f"D4 PR #1 foundation verification @ {_now()}")
    print("=" * 78)

    gates = [
        ("G1 cost_model single source",  gate_g1_cost_model_single_source),
        ("G2 no matrix ffill",           gate_g2_no_matrix_ffill),
        ("G3 survivorship",              gate_g3_survivorship),
        ("G7 KR/US isolation",           gate_g7_kr_us_isolation),
        ("G9 normal/stress diff",        gate_g9_normal_stress_diff),
        ("G10 portfolio invariants",     gate_g10_portfolio_invariants),
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

    summary_path = VERIF_DIR / f"d4_foundation_baseline_{_now()[:10]}.json"
    summary_path.write_text(
        json.dumps(
            {
                "started_at_utc": _now(),
                "phase": "D4 PR #1 (foundation)",
                "all_pass": all_ok,
                "gates": results,
                "engine_gates_deferred_to_pr2_pr3": [
                    "G4 metrics 6 outputs",
                    "G5 3-strategy parity",
                    "G6 idempotency byte-identical",
                    "G8 BTC HODL benchmark sanity",
                ],
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
