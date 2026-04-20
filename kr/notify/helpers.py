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


# ── Data Pipeline Failure Alert ───────────────────────────────
import atexit
import threading

_ALERT_MAX_CTX_LINES   = 3
_ALERT_MAX_CTX_VAL_LEN = 60
_ALERT_MAX_REASON_LEN  = 200
_pending_alert_threads: list = []   # atexit flush용


def _build_data_alert_msg(source: str, reason: str, context: dict | None) -> str:
    msg = (
        f"🚨 <b>Data Pipeline Failure</b>\n"
        f"Source: <code>{source}</code>\n"
        f"Reason: {reason[:_ALERT_MAX_REASON_LEN]}"
    )
    if context:
        items = list(context.items())[:_ALERT_MAX_CTX_LINES]
        ctx_lines = [f"{k}: {str(v)[:_ALERT_MAX_CTX_VAL_LEN]}" for k, v in items]
        if ctx_lines:
            msg += "\n" + "\n".join(ctx_lines)
    msg += "\n\n⚠ 후속 배치/EOD가 stale 데이터로 진행될 수 있음"
    return msg


def _send_data_alert_async(event_key: str, msg: str, reason: str) -> None:
    """비동기 fire-and-forget. atexit handler가 최대 2초 join으로 완충."""
    def _worker():
        try:
            from notify.telegram_bot import send
            from notify.alert_state import record_sent
            ok = send(msg, severity="CRITICAL")
            if ok:
                record_sent(event_key, "CRITICAL", state=reason[:100])
                logging.getLogger("gen4.notify").warning(f"[DATA_ALERT_SENT] {event_key}")
            else:
                logging.getLogger("gen4.notify").warning(f"[DATA_ALERT_SEND_FAIL] {event_key}")
        except Exception as e:
            logging.getLogger("gen4.notify").warning(f"[DATA_ALERT_THREAD_FAIL] {event_key}: {e}")

    t = threading.Thread(target=_worker, daemon=True)
    _pending_alert_threads.append(t)
    t.start()


def _flush_alert_threads(timeout: float = 2.0) -> None:
    """atexit: 종료 직전 최대 timeout초 대기해 delivery 보장."""
    for t in _pending_alert_threads:
        if t.is_alive():
            t.join(timeout=timeout)


atexit.register(_flush_alert_threads)


def _reason_key(reason: str) -> str:
    """reason 앞 30자의 MD5 해시 8자 → event_key 세분화용."""
    import hashlib
    return hashlib.md5(reason[:30].encode("utf-8", errors="ignore")).hexdigest()[:8]


def alert_data_failure(source: str, reason: str, context: dict = None) -> bool:
    """
    외부 데이터 소스 실패 CRITICAL 알림.

    Dedup 정책: 같은 날 같은 source + 같은 장애 유형 1회.
    event_key  = "data_failure:{source}:{reason_hash8}:{today_date}"
    category   = "data_pipeline:{source}"  (source 단위 독립 burst)

    같은 source라도 다른 장애(HTTP 429 vs empty response)는 각각 독립 alert.

    Args:
        source:  시스템/공급원 이름 (e.g. "yfinance", "fundamental", "lab_live")
        reason:  장애 설명 (200자 자동 truncate)
        context: 부가 정보 dict (최대 3 키, 값 60자 truncate)
    Returns: True=alert 큐잉됨, False=dedup skip or notify disabled
    """
    if not _NOTIFY_OK:
        return False
    try:
        from notify.alert_state import can_send
        from datetime import date as _date
        category  = f"data_pipeline:{source}"
        event_key = f"data_failure:{source}:{_reason_key(reason)}:{_date.today()}"
        if not can_send(event_key, "CRITICAL", category=category):
            logging.getLogger("gen4.notify").info(f"[DATA_ALERT_DEDUP_SKIP] {event_key}")
            return False
        msg = _build_data_alert_msg(source, reason, context)
        _send_data_alert_async(event_key, msg, reason)
        return True
    except Exception as e:
        logging.getLogger("gen4.notify").warning(f"[DATA_ALERT_FAIL] {source}: {e}")
        return False
