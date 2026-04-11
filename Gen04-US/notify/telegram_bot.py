# -*- coding: utf-8 -*-
"""
telegram_bot.py — Telegram alerts & commands for Q-TRON US
===========================================================
.env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
All messages prefixed with [US] for market context separation.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger("qtron.us.telegram")

_BOT_TOKEN: Optional[str] = None
_CHAT_ID: Optional[str] = None
_INITIALIZED = False

SEVERITY_EMOJI = {"INFO": "ℹ️", "WARN": "⚠️", "CRITICAL": "🚨"}


def _init() -> bool:
    global _BOT_TOKEN, _CHAT_ID, _INITIALIZED
    if _INITIALIZED:
        return bool(_BOT_TOKEN and _CHAT_ID)

    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)
    except ImportError:
        pass

    _BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    _CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
    _INITIALIZED = True

    if not _BOT_TOKEN or not _CHAT_ID:
        logger.warning("[Telegram] Token or chat ID not set")
        return False
    return True


def send(text: str, severity: str = "INFO") -> bool:
    """Send [US] prefixed message. Non-blocking, never raises."""
    try:
        if not _init():
            return False

        import requests
        emoji = SEVERITY_EMOJI.get(severity, "")
        full = f"{emoji} [US] {text}" if emoji else f"[US] {text}"

        url = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": _CHAT_ID,
            "text": full,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)

        if resp.status_code == 200:
            return True
        logger.warning(f"[Telegram] HTTP {resp.status_code}")
        return False
    except Exception as e:
        logger.warning(f"[Telegram] Send failed: {e}")
        return False


# ── Formatted Messages ───────────────────────────────────────

def notify_buy(symbol: str, qty: int, price: float) -> bool:
    return send(
        f"🔴 <b>BUY</b> {symbol} x{qty} @ ${price:,.2f}",
        "INFO"
    )


def notify_sell(symbol: str, qty: int, price: float,
                pnl: float = 0, pnl_pct: float = 0) -> bool:
    return send(
        f"🔵 <b>SELL</b> {symbol} x{qty} @ ${price:,.2f}\n"
        f"P&L: ${pnl:+,.2f} ({pnl_pct:+.2f}%)",
        "INFO"
    )


def notify_trail_near(symbol: str, margin_pct: float) -> bool:
    return send(
        f"<b>Trail Stop Near</b>\n{symbol}: {margin_pct:.1f}% to trigger",
        "WARN"
    )


def notify_trail_triggered(symbol: str, price: float, trigger: float) -> bool:
    return send(
        f"<b>Trail Stop Triggered</b>\n{symbol}: ${price:,.2f} ≤ ${trigger:,.2f}",
        "CRITICAL"
    )


def notify_rebal_complete(n_sell: int, n_buy: int) -> bool:
    return send(
        f"<b>Rebalance Complete</b>\nSold: {n_sell} | Bought: {n_buy}",
        "INFO"
    )


def notify_batch_complete(n_stocks: int, n_target: int) -> bool:
    return send(
        f"<b>Batch Complete</b>\nUniverse: {n_stocks} | Target: {n_target}",
        "INFO"
    )


def notify_snapshot_mismatch(scoring: str, execution: str) -> bool:
    return send(
        f"<b>SNAPSHOT MISMATCH</b>\n"
        f"Scoring: {scoring}\nExecution: {execution}\n→ Orders BLOCKED",
        "CRITICAL"
    )


def notify_error(msg: str) -> bool:
    return send(f"<b>ERROR</b>\n{msg}", "CRITICAL")
