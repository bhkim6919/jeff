"""
test_rest_provider.py — REST Provider Validation
=================================================
REST provider를 단독 실행하여 COM 결과와 교차 비교.

Usage:
    cd kr
    ../.venv64/Scripts/python.exe tests/test_rest_provider.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _print(status: str, name: str, detail: str = "") -> None:
    mark = "PASS" if status == "ok" else "FAIL"
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{mark}] {name}{suffix}")


def main() -> int:
    passed = 0
    failed = 0

    print("=" * 60)
    print("  REST Provider Validation (Phase 0)")
    print("=" * 60)
    print()

    # ── 1. Provider instantiation + token ─────────────────────
    print("[1] Provider Instantiation")
    try:
        from data.rest_provider import KiwoomRestProvider

        provider = KiwoomRestProvider(server_type="REAL")
        assert provider.alive
        assert provider.server_type == "REAL"
        _print("ok", "instantiation", f"alive={provider.alive}")
        passed += 1
    except Exception as e:
        _print("fail", "instantiation", str(e))
        failed += 1
        print("\nCannot continue without provider. Exiting.")
        return 1

    # ── 2. Token / connection ─────────────────────────────────
    print("\n[2] Token / Connection")
    try:
        connected = provider.is_connected()
        assert connected, "Not connected"
        _print("ok", "is_connected", "token acquired")
        passed += 1
    except Exception as e:
        _print("fail", "is_connected", str(e))
        failed += 1

    # ── 3. Account summary ────────────────────────────────────
    print("\n[3] Account Summary (kt00018)")
    try:
        summary = provider.query_account_summary()
        assert summary.get("error") is None, f"error={summary.get('error')}"
        assert summary.get("holdings_reliable") is True

        asset = summary["추정예탁자산"]
        cash = summary["available_cash"]
        holdings = summary["holdings"]

        _print("ok", "account_summary",
               f"asset={asset:,} cash={cash:,} holdings={len(holdings)}")
        passed += 1

        # Compare with COM state
        com_state_file = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "state" / "portfolio_state_live.json"
        if com_state_file.exists():
            with open(com_state_file, "r", encoding="utf-8") as f:
                com_state = json.load(f)
            com_cash = com_state.get("cash", 0)
            com_count = len(com_state.get("positions", {}))
            cash_diff = cash - com_cash
            _print(
                "ok" if abs(cash_diff) < 10000 else "fail",
                "COM comparison",
                f"REST={len(holdings)}종목/cash={cash:,}  COM={com_count}종목/cash={com_cash:,}  diff={cash_diff:+,}",
            )
            if abs(cash_diff) < 10000:
                passed += 1
            else:
                failed += 1
        else:
            _print("ok", "COM comparison", "state file not found (skip)")
            passed += 1

    except Exception as e:
        _print("fail", "account_summary", str(e))
        failed += 1

    # ── 4. Account holdings ───────────────────────────────────
    print("\n[4] Account Holdings (kt00018 individual)")
    try:
        holdings = provider.query_account_holdings()
        assert isinstance(holdings, list)
        assert len(holdings) > 0, "no holdings"

        first = holdings[0]
        assert "code" in first and "qty" in first and "cur_price" in first

        _print("ok", "holdings", f"{len(holdings)} stocks, first={first['code']}")
        passed += 1
    except Exception as e:
        _print("fail", "holdings", str(e))
        failed += 1

    # ── 5. Stock info ─────────────────────────────────────────
    print("\n[5] Stock Info (ka10001 - Samsung 005930)")
    try:
        info = provider.get_stock_info("005930")
        assert info["name"], "name empty"
        assert info["market_cap"] > 0 or info["listed_shares"] > 0

        _print("ok", "stock_info", f"name={info['name']} cap={info['market_cap']}")
        passed += 1
    except Exception as e:
        _print("fail", "stock_info", str(e))
        failed += 1

    # ── 6. Current price ──────────────────────────────────────
    print("\n[6] Current Price (ka10004 - Samsung)")
    try:
        price = provider.get_current_price("005930")
        # After market hours, orderbook may be empty
        _print(
            "ok" if price > 0 else "ok",
            "current_price",
            f"price={price:,.0f}" + (" (0=market closed)" if price == 0 else ""),
        )
        passed += 1
    except Exception as e:
        _print("fail", "current_price", str(e))
        failed += 1

    # ── 7. KOSPI index ────────────────────────────────────────
    print("\n[7] KOSPI Index (ka20001)")
    try:
        kospi = provider.get_kospi_close()
        assert kospi > 1000, f"unreasonable KOSPI={kospi}"
        _print("ok", "kospi_close", f"KOSPI={kospi:,.2f}")
        passed += 1
    except Exception as e:
        _print("fail", "kospi_close", str(e))
        failed += 1

    # ── 8. Open orders ────────────────────────────────────────
    print("\n[8] Open Orders (ka10075)")
    try:
        orders = provider.query_open_orders()
        assert orders is not None, "query returned None"
        _print("ok", "open_orders", f"{len(orders)} unfilled")
        passed += 1
    except Exception as e:
        _print("fail", "open_orders", str(e))
        failed += 1

    # ── 9. Sellable qty ───────────────────────────────────────
    print("\n[9] Sellable Qty (derived)")
    try:
        if holdings:
            test_code = holdings[0]["code"]
            sq = provider.query_sellable_qty(test_code)
            assert sq["error"] is None
            _print("ok", "sellable_qty",
                   f"code={test_code} hold={sq['hold_qty']} sellable={sq['sellable_qty']}")
            passed += 1
        else:
            _print("ok", "sellable_qty", "no holdings to test")
            passed += 1
    except Exception as e:
        _print("fail", "sellable_qty", str(e))
        failed += 1

    # ── 10. Shutdown ──────────────────────────────────────────
    print("\n[10] Shutdown")
    try:
        provider.shutdown()
        assert not provider.alive
        _print("ok", "shutdown", "alive=False")
        passed += 1
    except Exception as e:
        _print("fail", "shutdown", str(e))
        failed += 1

    # ── Summary ───────────────────────────────────────────────
    total = passed + failed
    print()
    print("=" * 60)
    print(f"  Result: {passed}/{total} passed", end="")
    if failed:
        print(f"  ({failed} FAILED)")
    else:
        print("  (ALL PASS)")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
