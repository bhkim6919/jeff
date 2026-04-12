"""
Notification helper functions extracted from main.py.
Telegram-first + Kakao fallback. Stock name cache management.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path

logger = logging.getLogger("gen4.notify")

# ── Telegram notifier (primary — failure never blocks trading) ────────────────
try:
    from notify.telegram_notify import notify_buy as _tg_buy
    from notify.telegram_notify import notify_sell as _tg_sell
    from notify.telegram_notify import notify_trail_stop as _tg_trail
    _TELEGRAM_OK = True
except Exception:
    _TELEGRAM_OK = False

# ── Kakao notifier (fallback — failure never blocks trading) ──────────────────
try:
    from notify.kakao_notify import notify_buy as _kakao_buy
    from notify.kakao_notify import notify_sell as _kakao_sell
    from notify.kakao_notify import notify_trail_stop as _kakao_trail
    _KAKAO_OK = True
except Exception:
    _KAKAO_OK = False


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
    """Fill missing names from Kiwoom master data. Returns True if cache was updated."""
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
    """텔레그램 우선, 카카오 fallback. 예외 전파 절대 금지."""
    name = _kname(code, name_cache)
    if _TELEGRAM_OK:
        try:
            _tg_buy(code, name, qty, price)
        except Exception as e:
            logger.warning(f"[NOTIFY_BUY_TG_FAIL] {code}: {e}")
    if _KAKAO_OK:
        try:
            _kakao_buy(code, name, qty, price)
        except Exception as e:
            logger.warning(f"[NOTIFY_BUY_KAKAO_FAIL] {code}: {e}")

def _notify_sell(code: str, name_cache: dict, qty: int, price: float,
                 pnl_pct: float = 0.0, reason: str = "", avg_price: float = 0.0):
    """텔레그램 우선, 카카오 fallback. 예외 전파 절대 금지."""
    name = _kname(code, name_cache)
    if _TELEGRAM_OK:
        try:
            _tg_sell(code, name, qty, price, pnl_pct, reason, avg_price=avg_price)
        except Exception as e:
            logger.warning(f"[NOTIFY_SELL_TG_FAIL] {code}: {e}")
    if _KAKAO_OK:
        try:
            _kakao_sell(code, name, qty, price, pnl_pct, reason, avg_price=avg_price)
        except Exception as e:
            logger.warning(f"[NOTIFY_SELL_KAKAO_FAIL] {code}: {e}")

def _notify_trail(code: str, name_cache: dict, price: float,
                  hwm: float, drop_pct: float,
                  qty: int = 0, avg_price: float = 0.0):
    """텔레그램 우선, 카카오 fallback. 예외 전파 절대 금지."""
    name = _kname(code, name_cache)
    if _TELEGRAM_OK:
        try:
            _tg_trail(code, name, price, hwm, drop_pct, qty=qty, avg_price=avg_price)
        except Exception as e:
            logger.warning(f"[NOTIFY_TRAIL_TG_FAIL] {code}: {e}")
    if _KAKAO_OK:
        try:
            _kakao_trail(code, name, price, hwm, drop_pct, qty=qty, avg_price=avg_price)
        except Exception as e:
            logger.warning(f"[NOTIFY_TRAIL_KAKAO_FAIL] {code}: {e}")
