"""
Notification helper functions extracted from main.py.
Kakao notify wrappers + stock name cache management.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path

# ── Kakao notifier (optional — failure never blocks trading) ─────────────────
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
    if not _KAKAO_OK: return
    try: _kakao_buy(code, _kname(code, name_cache), qty, price)
    except Exception as e:
        logging.getLogger("gen4.kakao").warning(f"[NOTIFY_BUY_FAIL] {code}: {e}")

def _notify_sell(code: str, name_cache: dict, qty: int, price: float,
                 pnl_pct: float = 0.0, reason: str = "", avg_price: float = 0.0):
    if not _KAKAO_OK: return
    try: _kakao_sell(code, _kname(code, name_cache), qty, price, pnl_pct, reason,
                     avg_price=avg_price)
    except Exception as e:
        logging.getLogger("gen4.kakao").warning(f"[NOTIFY_SELL_FAIL] {code}: {e}")

def _notify_trail(code: str, name_cache: dict, price: float,
                  hwm: float, drop_pct: float,
                  qty: int = 0, avg_price: float = 0.0):
    if not _KAKAO_OK: return
    try: _kakao_trail(code, _kname(code, name_cache), price, hwm, drop_pct,
                      qty=qty, avg_price=avg_price)
    except Exception as e:
        logging.getLogger("gen4.kakao").warning(f"[NOTIFY_TRAIL_FAIL] {code}: {e}")
