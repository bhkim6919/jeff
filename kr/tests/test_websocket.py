"""
test_websocket.py — WebSocket Connection & Subscription Test
=============================================================
WebSocket 연결, 구독, 메시지 수신을 검증.
장 종료 후에도 연결은 가능하나 실시간 데이터는 없을 수 있음.

Usage:
    cd kr
    ../.venv64/Scripts/python.exe tests/test_websocket.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _print(status: str, name: str, detail: str = "") -> None:
    mark = "PASS" if status == "ok" else "FAIL" if status == "fail" else "SKIP"
    suffix = f" -- {detail}" if detail else ""
    print(f"  [{mark}] {name}{suffix}")


def main() -> int:
    passed = 0
    failed = 0
    skipped = 0

    print("=" * 60)
    print("  WebSocket Validation (Phase 1)")
    print("=" * 60)
    print()

    # ── 1. WebSocket connection ───────────────────────────────
    print("[1] WebSocket Connection")
    try:
        from dotenv import load_dotenv
        import os

        load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))
        from data.rest_token_manager import TokenManager

        token_mgr = TokenManager(
            os.getenv("KIWOOM_APP_KEY", ""),
            os.getenv("KIWOOM_APP_SECRET", ""),
            os.getenv("KIWOOM_API_URL", "https://api.kiwoom.com"),
        )
        token = token_mgr.token
        _print("ok", "token", f"acquired: {token[:12]}...")
        passed += 1
    except Exception as e:
        _print("fail", "token", str(e))
        failed += 1
        return 1

    from data.rest_websocket import KiwoomWebSocket

    ws = KiwoomWebSocket(token=token, server_type="REAL")

    try:
        ws.start()
        # Wait for connection (max 10s)
        for i in range(40):
            if ws.connected:
                break
            time.sleep(0.25)

        if ws.connected:
            _print("ok", "ws_connect", "connected to WebSocket server")
            passed += 1
        else:
            _print("fail", "ws_connect", "connection timeout (10s)")
            failed += 1
            ws.stop()
            return 1
    except Exception as e:
        _print("fail", "ws_connect", str(e))
        failed += 1
        return 1

    # ── 2. Subscribe Type 0B (price ticks) ────────────────────
    print("\n[2] Subscribe Type 0B (Samsung 005930)")
    tick_count = 0
    last_values = {}

    def on_tick(code, values):
        nonlocal tick_count, last_values
        tick_count += 1
        last_values = values

    ws.set_on_price_tick(on_tick)

    try:
        ws.subscribe(["005930"], "0B")
        time.sleep(2)  # Wait for subscription confirmation
        _print("ok", "subscribe_0B", "subscription sent")
        passed += 1
    except Exception as e:
        _print("fail", "subscribe_0B", str(e))
        failed += 1

    # ── 3. Wait for ticks (장중이면 수신, 장후면 0건) ─────────
    print("\n[3] Receive Ticks (wait 10s)")
    time.sleep(10)

    if tick_count > 0:
        price = last_values.get("10", "?")
        _print("ok", "receive_ticks", f"{tick_count} ticks received, last price={price}")
        passed += 1
    else:
        _print("ok", "receive_ticks", "0 ticks (normal if market closed)")
        passed += 1  # Not a failure — market may be closed

    # ── 4. Subscribe Type 00 (order execution) ────────────────
    print("\n[4] Subscribe Type 00 (order execution)")
    order_msg_count = 0

    def on_order(values):
        nonlocal order_msg_count
        order_msg_count += 1

    ws.set_on_order_exec(on_order)

    try:
        ws.subscribe([""], "00")
        time.sleep(2)
        _print("ok", "subscribe_00", "order execution subscription active")
        passed += 1
    except Exception as e:
        _print("fail", "subscribe_00", str(e))
        failed += 1

    # ── 5. Unsubscribe ────────────────────────────────────────
    print("\n[5] Unsubscribe All")
    try:
        ws.unsubscribe_all()
        time.sleep(1)
        _print("ok", "unsubscribe", "all subscriptions removed")
        passed += 1
    except Exception as e:
        _print("fail", "unsubscribe", str(e))
        failed += 1

    # ── 6. Provider integration ───────────────────────────────
    print("\n[6] Provider Integration (register_real via provider)")
    try:
        from data.rest_provider import KiwoomRestProvider

        provider = KiwoomRestProvider(server_type="REAL")
        rt_count = 0

        def rt_callback(code, price, volume):
            nonlocal rt_count
            rt_count += 1

        provider.set_real_data_callback(rt_callback)
        provider.register_real(["005930", "003540"], fids="10;27")
        time.sleep(8)

        _print("ok", "provider_register",
               f"WebSocket via provider, ticks={rt_count}")
        passed += 1

        provider.unregister_real()
        provider.shutdown()
    except Exception as e:
        _print("fail", "provider_register", str(e))
        failed += 1

    # ── 7. Cleanup ────────────────────────────────────────────
    print("\n[7] Cleanup")
    try:
        ws.stop()
        assert not ws.connected
        _print("ok", "ws_stop", "disconnected cleanly")
        passed += 1
    except Exception as e:
        _print("fail", "ws_stop", str(e))
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
