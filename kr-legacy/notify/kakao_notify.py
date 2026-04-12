"""
kakao_notify.py — Q-TRON Kakao notification module
====================================================
나에게 보내기 API 사용 (talk_message scope).
access_token 만료 시 refresh_token으로 자동 갱신.

Usage:
    from notify.kakao_notify import notify_buy, notify_sell, notify

    notify_buy("005930", "삼성전자", qty=10, price=75000)
    notify_sell("005930", "삼성전자", qty=10, price=76000, pnl_pct=0.013)
    notify("[RECON] 보정 12건 -> SAFE MODE")
"""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("gen4.kakao")

# ── 경로 ─────────────────────────────────────────────────────────────────────
_BASE = Path(__file__).resolve().parent.parent
TOKEN_FILE = _BASE / "kakao_tokens.json"
REST_API_KEY = "8cccc51e3df9442bee0c0f21e6c5f059"

# ── API ──────────────────────────────────────────────────────────────────────
_TOKEN_URL = "https://kauth.kakao.com/oauth/token"
_MEMO_URL  = "https://kapi.kakao.com/v2/api/talk/memo/default/send"


# ── 토큰 관리 ─────────────────────────────────────────────────────────────────

def _load_tokens() -> dict:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    return {}


def _save_tokens(tokens: dict) -> None:
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2), encoding="utf-8")


def _refresh_access_token(refresh_token: str) -> Optional[str]:
    data = urllib.parse.urlencode({
        "grant_type":    "refresh_token",
        "client_id":     REST_API_KEY,
        "refresh_token": refresh_token,
    }).encode()
    req = urllib.request.Request(
        _TOKEN_URL, data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        r = urllib.request.urlopen(req, timeout=10)
        result = json.loads(r.read().decode())
        tokens = _load_tokens()
        tokens["access_token"] = result["access_token"]
        if "refresh_token" in result:
            tokens["refresh_token"] = result["refresh_token"]
        tokens["refreshed_at"] = datetime.now().isoformat()
        _save_tokens(tokens)
        logger.info("[KAKAO] access_token refreshed")
        return result["access_token"]
    except Exception as e:
        logger.error(f"[KAKAO] token refresh failed: {e}")
        return None


def _get_access_token() -> Optional[str]:
    tokens = _load_tokens()
    if not tokens:
        logger.error("[KAKAO] kakao_tokens.json not found")
        return None
    return tokens.get("access_token")


# ── 전송 ─────────────────────────────────────────────────────────────────────

def _send(text: str, retry: bool = True) -> bool:
    """나에게 보내기. 실패 시 토큰 갱신 후 1회 재시도."""
    access_token = _get_access_token()
    if not access_token:
        return False

    template = json.dumps({
        "object_type": "text",
        "text": text,
        "link": {"web_url": "", "mobile_web_url": ""},
    }, ensure_ascii=False)

    data = urllib.parse.urlencode({"template_object": template}).encode("utf-8")
    req = urllib.request.Request(
        _MEMO_URL, data=data,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
    )
    try:
        r = urllib.request.urlopen(req, timeout=10)
        result = json.loads(r.read().decode())
        if result.get("result_code") == 0:
            logger.debug(f"[KAKAO] sent: {text[:40]}")
            return True
        logger.warning(f"[KAKAO] send failed: {result}")
        return False
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code == 401 and retry:
            # 토큰 만료 → 갱신 후 재시도
            tokens = _load_tokens()
            new_token = _refresh_access_token(tokens.get("refresh_token", ""))
            if new_token:
                return _send(text, retry=False)
        logger.error(f"[KAKAO] HTTP {e.code}: {body}")
        return False
    except Exception as e:
        logger.error(f"[KAKAO] send error: {e}")
        return False


# ── 공개 API ─────────────────────────────────────────────────────────────────

def notify(message: str) -> bool:
    """범용 알림."""
    return _send(message)


def notify_buy(code: str, name: str, qty: int, price: float) -> bool:
    """매수 체결 알림."""
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
    """매도 체결 알림."""
    reason_str = f"\n사유: {reason}" if reason else ""
    pnl_str = f"{pnl_pct:+.1%}" if pnl_pct != 0 else "-"
    # 수익금액 = (매도가 - 매수가) * 수량
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
    """Trail stop 발동 알림."""
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
    """RECON 이상 알림."""
    flag = " → SAFE MODE" if safe_mode else ""
    text = (
        f"[Q-TRON] RECON 이상\n"
        f"보정 {corrections}건{flag}"
    )
    return _send(text)


def notify_eod(equity: float, daily_pnl: float, n_positions: int) -> bool:
    """EOD 요약 알림."""
    text = (
        f"[Q-TRON] EOD 요약\n"
        f"평가액: {equity:,.0f}원\n"
        f"일수익: {daily_pnl:+.2%}\n"
        f"보유: {n_positions}종목"
    )
    return _send(text)


def notify_rebalance_done(sells: int, buys: int, mode: str = "") -> bool:
    """리밸런스 완료 알림."""
    mode_str = f"[{mode}] " if mode else ""
    text = (
        f"[Q-TRON] {mode_str}리밸런스 완료\n"
        f"매도 {sells}건 / 매수 {buys}건"
    )
    return _send(text)


def notify_safe_mode(level: int, reason: str) -> bool:
    """SAFE MODE 레벨 변화 알림."""
    impact = {0: "해제 → 정상", 1: "알림만", 2: "BUY 축소", 3: "리밸 보류"}
    text = (
        f"[Q-TRON] SAFE MODE L{level}\n"
        f"원인: {reason}\n"
        f"영향: {impact.get(level, '?')}\n"
        f"Trail/SELL: {'허용' if level < 3 else '금지(주문불확실)'}"
    )
    return _send(text)


def notify_buy_blocked(reason: str) -> bool:
    """리밸 전체 보류 알림 (BLOCKED 상태)."""
    text = (
        f"[Q-TRON] REBAL BLOCKED\n"
        f"원인: {reason}\n"
        f"포지션: 현재 유지 (주문 0건)\n"
        f"조치: HTS에서 미체결/포지션 확인"
    )
    return _send(text)


# ── 토큰 파일 위치 확인용 ─────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print(f"Token file: {TOKEN_FILE}")
    ok = notify("[Q-TRON] 알림 테스트 - 연동 성공!")
    print("Result:", "OK" if ok else "FAILED")
