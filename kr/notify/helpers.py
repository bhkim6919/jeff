"""
Notification helper functions.
Telegram notify wrappers + stock name cache management.
(카카오 → 텔레그램 전환 완료 2026-04-11)
"""
from __future__ import annotations
import json
import logging
from pathlib import Path

# ── Telegram notifier (failure never blocks trading) ───────────
try:
    from notify.telegram_bot import (
        notify_buy as _tg_buy,
        notify_sell as _tg_sell,
        notify_trail_triggered as _tg_trail,
        send as _tg_send,
    )
    _NOTIFY_OK = True
except Exception:
    _NOTIFY_OK = False


def _load_name_cache(base_dir: Path) -> dict:
    p = base_dir / "data" / "stock_name_cache.json"
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except Exception:
        return {}

def _save_name_cache(base_dir: Path, cache: dict) -> None:
    p = base_dir / "data" / "stock_name_cache.json"
    try:
        p.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _enrich_name_cache(cache: dict, codes, provider) -> bool:
    """Fill missing names from broker master data. Returns True if cache was updated."""
    updated = False
    for code in codes:
        if code in cache and cache[code]:
            continue
        try:
            info = provider.get_stock_info(code)
            name = info.get("name", "")
            if name:
                cache[code] = name
                updated = True
        except Exception:
            pass
    return updated

def _kname(code: str, cache: dict) -> str:
    return cache.get(code, code)

def _notify_buy(code: str, name_cache: dict, qty: int, price: float):
    if not _NOTIFY_OK: return
    try: _tg_buy(code, _kname(code, name_cache), qty, price)
    except Exception as e:
        logging.getLogger("gen4.notify").warning(f"[NOTIFY_BUY_FAIL] {code}: {e}")

def _notify_sell(code: str, name_cache: dict, qty: int, price: float,
                 pnl_pct: float = 0.0, reason: str = "", avg_price: float = 0.0):
    if not _NOTIFY_OK: return
    try: _tg_sell(code, _kname(code, name_cache), qty, price, pnl_pct, avg_price)
    except Exception as e:
        logging.getLogger("gen4.notify").warning(f"[NOTIFY_SELL_FAIL] {code}: {e}")

def _notify_trail(code: str, name_cache: dict, price: float,
                  hwm: float, drop_pct: float,
                  qty: int = 0, avg_price: float = 0.0):
    if not _NOTIFY_OK: return
    try: _tg_trail(code, _kname(code, name_cache), price, hwm * (1 - 0.12))
    except Exception as e:
        logging.getLogger("gen4.notify").warning(f"[NOTIFY_TRAIL_FAIL] {code}: {e}")

def _notify_advisor(alerts: list, recommendations: list):
    """Advisor 결과를 텔레그램으로 발송."""
    if not _NOTIFY_OK: return
    try:
        # HIGH 경고만 즉시
        high_alerts = [a for a in alerts if a.get("priority") == "HIGH"]
        if high_alerts:
            lines = [f"• {a['message']}" for a in high_alerts[:5]]
            _tg_send(f"<b>Advisor 경고</b>\n" + "\n".join(lines), "CRITICAL")

        # 추천이 있으면
        if recommendations:
            lines = [f"• [{r.get('confidence','')}] {r.get('parameter','')}: "
                     f"{r.get('rationale','')[:60]}" for r in recommendations[:3]]
            _tg_send(f"<b>Advisor 추천</b>\n" + "\n".join(lines), "INFO")
    except Exception as e:
        logging.getLogger("gen4.notify").warning(f"[NOTIFY_ADVISOR_FAIL] {e}")
