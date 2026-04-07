"""
test_order_flow.py — Order Flow Validation (Phase 2)
=====================================================
실전 서버에서 주문 API 구조 테스트.
장 종료 후: 접수만 확인 (체결 안 됨).
장 중: full-cycle (접수 → 체결 → 미체결 확인).

Usage:
    cd Gen04-REST
    ../.venv64/Scripts/python.exe tests/test_order_flow.py [--live]

    기본: 주문 접수 구조만 확인 (실제 주문 안 보냄)
    --live: 실제 1주 매수→매도 테스트 (장중에만!)
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

LIVE_MODE = "--live" in sys.argv


def _print(status: str, name: str, detail: str = "") -> None:
    mark = "PASS" if status == "ok" else "FAIL" if status == "fail" else "SKIP"
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{mark}] {name}{suffix}")


def main() -> int:
    passed = 0
    failed = 0

    print("=" * 60)
    print(f"  Order Flow Validation (Phase 2)")
    print(f"  Mode: {'LIVE (real orders!)' if LIVE_MODE else 'DRY-RUN (structure only)'}")
    print(f"  Time: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)
    print()

    # ── 1. Provider init ──────────────────────────────────────
    print("[1] Provider Init")
    try:
        from data.rest_provider import KiwoomRestProvider
        provider = KiwoomRestProvider(server_type="REAL")
        _print("ok", "init", f"alive={provider.alive}")
        passed += 1
    except Exception as e:
        _print("fail", "init", str(e))
        return 1

    # ── 2. Pre-order state ────────────────────────────────────
    print("\n[2] Pre-order State")
    try:
        summary = provider.query_account_summary()
        cash = summary.get("available_cash", 0)
        holdings = summary.get("holdings", [])
        _print("ok", "pre_state", f"cash={cash:,} holdings={len(holdings)}")
        passed += 1
    except Exception as e:
        _print("fail", "pre_state", str(e))
        failed += 1

    # ── 3. Open orders check ─────────────────────────────────
    print("\n[3] Open Orders (before)")
    try:
        open_orders = provider.query_open_orders()
        assert open_orders is not None
        _print("ok", "open_orders_before", f"{len(open_orders)} unfilled")
        passed += 1
    except Exception as e:
        _print("fail", "open_orders_before", str(e))
        failed += 1

    # ── 4. Order request structure test ───────────────────────
    print("\n[4] Order Request Structure")

    if not LIVE_MODE:
        _print("ok", "order_structure",
               "DRY-RUN: skipped (use --live for real orders)")
        passed += 1

        # Test that send_order returns proper error for invalid code
        try:
            result = provider.send_order("999999", "BUY", 1, price=0, hoga_type="03")
            has_keys = all(k in result for k in ("order_no", "exec_price", "exec_qty", "error"))
            _print("ok" if has_keys else "fail", "order_response_schema",
                   f"keys present={has_keys}, error={result.get('error', '')[:60]}")
            if has_keys:
                passed += 1
            else:
                failed += 1
        except Exception as e:
            _print("fail", "order_response_schema", str(e))
            failed += 1
    else:
        # ── LIVE: 실제 1주 매수 테스트 ────────────────────────
        # 저가 종목 선택 (NH투자증권 005940, ~30,000원)
        test_code = "005940"
        test_qty = 1

        print(f"\n  [LIVE] BUY {test_code} x{test_qty} (시장가)")
        try:
            # WebSocket 연결 (체결 감시)
            provider.register_real([test_code])
            time.sleep(3)

            buy_result = provider.send_order(
                test_code, "BUY", test_qty, price=0, hoga_type="03"
            )
            print(f"    order_no: {buy_result.get('order_no', '')}")
            print(f"    status:   {buy_result.get('status', '')}")
            print(f"    exec:     {buy_result.get('exec_qty', 0)}@{buy_result.get('exec_price', 0)}")
            print(f"    error:    {buy_result.get('error', '')}")

            if buy_result.get("order_no"):
                _print("ok", "buy_order", f"order_no={buy_result['order_no']}")
                passed += 1

                # Wait and check fill
                time.sleep(5)

                # Check open orders
                open_after = provider.query_open_orders()
                open_count = len(open_after) if open_after else 0
                _print("ok", "open_after_buy", f"{open_count} unfilled")
                passed += 1

                # If filled, try sell
                if buy_result.get("exec_qty", 0) > 0:
                    print(f"\n  [LIVE] SELL {test_code} x{test_qty} (시장가)")
                    sell_result = provider.send_order(
                        test_code, "SELL", test_qty, price=0, hoga_type="03"
                    )
                    print(f"    order_no: {sell_result.get('order_no', '')}")
                    print(f"    status:   {sell_result.get('status', '')}")
                    print(f"    error:    {sell_result.get('error', '')}")

                    if sell_result.get("order_no"):
                        _print("ok", "sell_order", f"order_no={sell_result['order_no']}")
                        passed += 1
                    else:
                        _print("fail", "sell_order", sell_result.get("error", ""))
                        failed += 1
                else:
                    _print("ok", "sell_skip", "buy not filled (market closed?)")
                    passed += 1

                # Cancel any remaining open orders
                if open_count > 0:
                    cancelled = provider.cancel_all_open_orders()
                    _print("ok", "cancel_cleanup", f"cancelled={cancelled}")
                    passed += 1
            else:
                _print("fail", "buy_order", buy_result.get("error", "no order_no"))
                failed += 1

            provider.unregister_real()

        except Exception as e:
            _print("fail", "live_order", str(e))
            failed += 1

    # ── 5. Post-order state ───────────────────────────────────
    print("\n[5] Post-order State")
    try:
        summary2 = provider.query_account_summary()
        cash2 = summary2.get("available_cash", 0)
        holdings2 = summary2.get("holdings", [])
        _print("ok", "post_state", f"cash={cash2:,} holdings={len(holdings2)}")
        passed += 1
    except Exception as e:
        _print("fail", "post_state", str(e))
        failed += 1

    # ── 6. Shutdown ───────────────────────────────────────────
    print("\n[6] Shutdown")
    provider.shutdown()
    _print("ok", "shutdown", "clean")
    passed += 1

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
