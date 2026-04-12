"""
telegram_notify.py — Q-TRON Telegram notification module
==========================================================
sendMessage API (plain text, no parse_mode).
.env에서 TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 로드.
실패 시 False + warning log. 절대 예외 전파 없음. timeout 2초.

REST(kr)와 동일 .env 공유. kr 전환 시 import 경로만 변경.

Usage:
    from notify.telegram_notify import notify, notify_buy, notify_sell
    notify("[Q-TRON] 테스트 알림")
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger("gen4.telegram")

# ── .env 로딩 (3단계 fallback) ───────────────────────────────────────────────

_BOT_TOKEN: Optional[str] = None
_CHAT_ID: Optional[str] = None


def _parse_env_file(path: Path) -> dict:
    """key=value 파싱. # 주석, 빈줄 무시."""
    env = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    except Exception:
        pass
    return env


def _load_credentials() -> tuple:
    """Returns (bot_token, chat_id). 3단계: kr/.env → kr-legacy/.env → os.environ."""
    base = Path(__file__).resolve().parent.parent  # kr-legacy/

    # 1) kr/.env (REST와 공유)
    env_rest = base.parent / "kr" / ".env"
    if env_rest.exists():
        env = _parse_env_file(env_rest)
        token = env.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = env.get("TELEGRAM_CHAT_ID", "")
        if token and chat_id:
            logger.info(f"[TELEGRAM] loaded token from {env_rest}")
            return token, chat_id
        logger.warning(f"[TELEGRAM] .env found at {env_rest} but missing keys")

    # 2) kr-legacy/.env
    env_local = base / ".env"
    if env_local.exists():
        env = _parse_env_file(env_local)
        token = env.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = env.get("TELEGRAM_CHAT_ID", "")
        if token and chat_id:
            logger.info(f"[TELEGRAM] loaded token from {env_local}")
            return token, chat_id
        logger.warning(f"[TELEGRAM] .env found at {env_local} but missing keys")

    # 3) os.environ fallback
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if token and chat_id:
        logger.info("[TELEGRAM] loaded token from os.environ")
        return token, chat_id

    logger.error("[TELEGRAM] no token available, notifications disabled")
    return "", ""


def _ensure_credentials() -> bool:
    global _BOT_TOKEN, _CHAT_ID
    if _BOT_TOKEN and _CHAT_ID:
        return True
    _BOT_TOKEN, _CHAT_ID = _load_credentials()
    return bool(_BOT_TOKEN and _CHAT_ID)


# ── 전송 ─────────────────────────────────────────────────────────────────────

def _send(text: str) -> bool:
    """sendMessage (plain text). timeout 2초. 실패 → False + warning."""
    try:
        if not _ensure_credentials():
            return False

        url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
        payload = json.dumps({
            "chat_id": _CHAT_ID,
            "text": text,
            "disable_web_page_preview": True,
        }, ensure_ascii=False).encode("utf-8")

        req = urllib.request.Request(
            url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        r = urllib.request.urlopen(req, timeout=2)
        result = json.loads(r.read().decode())
        if result.get("ok"):
            logger.debug(f"[TELEGRAM] sent: {text[:40]}")
            return True
        logger.warning(f"[TELEGRAM] send failed: {result}")
        return False
    except Exception as e:
        logger.warning(f"[TELEGRAM] send error: {e}")
        return False


# ── 공개 API (kakao_notify.py 미러) ─────────────────────────────────────────

def notify(message: str) -> bool:
    return _send(message)


def notify_buy(code: str, name: str, qty: int, price: float) -> bool:
    text = (
        f"[Q-TRON] 매수 체결\n"
        f"{name}({code})\n"
        f"{qty:,}주 @ {price:,.0f}원\n"
        f"총 {qty * price:,.0f}원"
    )
    return _send(text)


def notify_sell(code: str, name: str, qty: int, price: float,
                pnl_pct: float = 0.0, reason: str = "",
                avg_price: float = 0.0) -> bool:
    reason_str = f"\n사유: {reason}" if reason else ""
    pnl_str = f"{pnl_pct:+.1%}" if pnl_pct != 0 else "-"
    if avg_price > 0:
        pnl_amt = (price - avg_price) * qty
        cost_str = f"\n매수단가: {avg_price:,.0f}원"
        amt_str = f"\n손익금액: {pnl_amt:+,.0f}원"
    else:
        cost_str = ""
        amt_str = ""
    text = (
        f"[Q-TRON] 매도 체결\n"
        f"{name}({code})\n"
        f"{qty:,}주 @ {price:,.0f}원"
        f"{cost_str}\n"
        f"수익률: {pnl_str}"
        f"{amt_str}"
        f"{reason_str}"
    )
    return _send(text)


def notify_trail_stop(code: str, name: str, price: float,
                      hwm: float, drop_pct: float,
                      qty: int = 0, avg_price: float = 0.0) -> bool:
    if avg_price > 0:
        pnl_pct = (price - avg_price) / avg_price
        pnl_amt = (price - avg_price) * qty
        entry_str = (f"\n매수단가: {avg_price:,.0f}원"
                     f"\n수익률: {pnl_pct:+.1%}"
                     f"\n손익금액: {pnl_amt:+,.0f}원")
    else:
        entry_str = ""
    text = (
        f"[Q-TRON] Trail Stop\n"
        f"{name}({code})\n"
        f"{qty:,}주 @ {price:,.0f}원\n"
        f"고점대비: {drop_pct:.1%}"
        f"{entry_str}"
    )
    return _send(text)


def notify_recon(corrections: int, safe_mode: bool = False) -> bool:
    flag = " → SAFE MODE" if safe_mode else ""
    text = (
        f"[Q-TRON] RECON 이상\n"
        f"보정 {corrections}건{flag}"
    )
    return _send(text)


def notify_eod(equity: float, daily_pnl: float, n_positions: int) -> bool:
    text = (
        f"[Q-TRON] EOD 요약\n"
        f"평가액: {equity:,.0f}원\n"
        f"일수익: {daily_pnl:+.2%}\n"
        f"보유: {n_positions}종목"
    )
    return _send(text)


def notify_rebalance_done(sells: int, buys: int, mode: str = "") -> bool:
    mode_str = f"[{mode}] " if mode else ""
    text = (
        f"[Q-TRON] {mode_str}리밸런스 완료\n"
        f"매도 {sells}건 / 매수 {buys}건"
    )
    return _send(text)


def notify_safe_mode(level: int, reason: str) -> bool:
    impact = {0: "해제 → 정상", 1: "알림만", 2: "BUY 축소", 3: "리밸 보류"}
    text = (
        f"[Q-TRON] SAFE MODE L{level}\n"
        f"원인: {reason}\n"
        f"영향: {impact.get(level, '?')}\n"
        f"Trail/SELL: {'허용' if level < 3 else '금지(주문불확실)'}"
    )
    return _send(text)


def notify_buy_blocked(reason: str) -> bool:
    text = (
        f"[Q-TRON] REBAL BLOCKED\n"
        f"원인: {reason}\n"
        f"포지션: 현재 유지 (주문 0건)\n"
        f"조치: HTS에서 미체결/포지션 확인"
    )
    return _send(text)


# ── 토큰 확인용 ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    ok = notify("[Q-TRON] 텔레그램 알림 테스트 - 연동 성공!")
    print("Result:", "OK" if ok else "FAILED")
