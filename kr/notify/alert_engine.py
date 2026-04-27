# -*- coding: utf-8 -*-
"""
alert_engine.py — 단일 evaluator (read-only)
==============================================
snapshot을 받아 알림 이벤트 목록 생성.
SSE에서 직접 호출하지 않음 → background task에서만 호출.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from notify.alert_state import can_send, record_sent, get_last_state
from notify import telegram_bot as tg

logger = logging.getLogger("gen4.notify.alert_engine")


@dataclass
class AlertEvent:
    event_key: str
    category: str
    severity: str   # INFO, WARN, CRITICAL
    message: str
    state: str = ""


def evaluate(snapshot: Dict[str, Any]) -> List[AlertEvent]:
    """
    Evaluate snapshot and return alert events.
    Read-only — does not modify any state.
    """
    events: List[AlertEvent] = []

    # ── 1. Regime Change ──
    _check_regime(snapshot, events)

    # ── 2. Trail Stop ──
    _check_trail_stops(snapshot, events)

    # ── 3. DD Warning ──
    _check_dd(snapshot, events)

    # ── 4. System Stale ──
    _check_stale(snapshot, events)

    # ── 5. RECON Unsafe ──
    _check_recon(snapshot, events)

    # ── 6. Rebalance Countdown ──
    _check_rebal_countdown(snapshot, events)

    return events


def send_alerts(events: List[AlertEvent]) -> int:
    """Send alerts via Telegram with dedup/burst filtering. Returns count sent."""
    sent = 0
    for ev in events:
        if can_send(ev.event_key, ev.severity, ev.category):
            ok = tg.send(ev.message, ev.severity)
            if ok:
                record_sent(ev.event_key, ev.severity, ev.state)
                sent += 1
    return sent


# ── Event Checkers ────────────────────────────────────────────

def _check_regime(snapshot: Dict, events: List[AlertEvent]) -> None:
    actual = snapshot.get("regime_actual") or {}
    label = actual.get("actual_label", "")
    if not label:
        return

    prev = get_last_state("regime_level")
    if prev and prev != label:
        events.append(AlertEvent(
            event_key="regime_level",
            category="regime",
            severity="WARN",
            message=f"<b>레짐 변경</b>\n{prev} → {label}\n점수: {actual.get('scores', {}).get('total', '--')}",
            state=label,
        ))
    elif not prev:
        # First time — record state without sending
        record_sent("regime_level", "INFO", label)


def _check_trail_stops(snapshot: Dict, events: List[AlertEvent]) -> None:
    trail_data = snapshot.get("trail_stops") or {}
    stops = trail_data.get("stops", [])

    for s in stops:
        code = s.get("code", "")
        if not code:
            continue
        current = s.get("current_price", 0)
        trail = s.get("trail_price", 0)
        if not current or not trail or trail <= 0:
            continue

        margin = (current / trail) - 1

        if margin <= 0:
            # TRIGGERED
            events.append(AlertEvent(
                event_key=f"trail_triggered_{code}",
                category="trail",
                severity="CRITICAL",
                message=f"🚨 <b>Trail Stop 발동</b>\n{code}\n현재 {current:,.0f} ≤ 트리거 {trail:,.0f}",
                state="TRIGGERED",
            ))
        elif margin <= 0.02:
            # NEAR — 2% 이내
            events.append(AlertEvent(
                event_key=f"trail_near_{code}",
                category="trail",
                severity="WARN",
                message=f"⚠️ <b>Trail Stop 근접</b>\n{code}\n트리거까지 {margin*100:.1f}%",
                state="NEAR",
            ))


def _check_dd(snapshot: Dict, events: List[AlertEvent]) -> None:
    dd = snapshot.get("dd_guard") or {}

    daily = dd.get("daily_dd")
    if daily is not None and daily < -0.03:
        prev = get_last_state("dd_daily_warn")
        if prev != "TRIGGERED":
            events.append(AlertEvent(
                event_key="dd_daily_warn",
                category="dd",
                severity="WARN",
                message=f"<b>일간 DD 경고</b>\n{daily*100:.1f}% (임계: -3%)",
                state="TRIGGERED",
            ))
    elif daily is not None and daily >= -0.03:
        prev = get_last_state("dd_daily_warn")
        if prev == "TRIGGERED":
            events.append(AlertEvent(
                event_key="dd_daily_warn",
                category="dd",
                severity="INFO",
                message=f"<b>일간 DD 복구</b>\n{daily*100:.1f}%",
                state="CLEAR",
            ))

    monthly = dd.get("monthly_dd")
    if monthly is not None and monthly < -0.05:
        prev = get_last_state("dd_monthly_warn")
        if prev != "TRIGGERED":
            events.append(AlertEvent(
                event_key="dd_monthly_warn",
                category="dd",
                severity="CRITICAL",
                message=f"<b>월간 DD 경고</b>\n{monthly*100:.1f}% (임계: -5%)",
                state="TRIGGERED",
            ))
    elif monthly is not None and monthly >= -0.05:
        prev = get_last_state("dd_monthly_warn")
        if prev == "TRIGGERED":
            events.append(AlertEvent(
                event_key="dd_monthly_warn",
                category="dd",
                severity="INFO",
                message=f"<b>월간 DD 복구</b>\n{monthly*100:.1f}%",
                state="CLEAR",
            ))


def _check_stale(snapshot: Dict, events: List[AlertEvent]) -> None:
    age = snapshot.get("data_age_max_sec", 0)
    if age > 300:
        prev = get_last_state("system_stale")
        if prev != "STALE":
            events.append(AlertEvent(
                event_key="system_stale",
                category="system",
                severity="WARN",
                message=f"<b>데이터 지연</b>\n최대 {age:.0f}초 경과",
                state="STALE",
            ))
    else:
        prev = get_last_state("system_stale")
        if prev == "STALE":
            events.append(AlertEvent(
                event_key="system_stale",
                category="system",
                severity="INFO",
                message="<b>데이터 지연 복구</b>",
                state="OK",
            ))


def _check_recon(snapshot: Dict, events: List[AlertEvent]) -> None:
    recon = snapshot.get("recon") or {}
    unreliable = recon.get("unreliable", False)

    if unreliable:
        prev = get_last_state("recon_unsafe")
        if prev != "UNSAFE":
            events.append(AlertEvent(
                event_key="recon_unsafe",
                category="recon",
                severity="CRITICAL",
                message="<b>RECON 비신뢰</b>\n브로커 동기화 불안정",
                state="UNSAFE",
            ))
    else:
        prev = get_last_state("recon_unsafe")
        if prev == "UNSAFE":
            events.append(AlertEvent(
                event_key="recon_unsafe",
                category="recon",
                severity="INFO",
                message="<b>RECON 복구</b>\n브로커 동기화 정상",
                state="OK",
            ))


def _check_rebal_countdown(snapshot: Dict, events: List[AlertEvent]) -> None:
    """Emit a one-shot D-7 / D-3 / D-1 alert per next-rebalance cycle.

    Jeff 2026-04-27: the prior implementation relied solely on the 30-min
    DEDUP_TTL in alert_state.py. While d_day stayed at the target value
    (e.g., D-7 across an entire trading day) the event was re-emitted on
    every evaluator tick that fell after the TTL window — the dashboard
    showed 5 "📅 리밸런싱 D-7" entries inside 2 hours. Now we treat the
    state field ("D-7" / "D-3" / "D-1") as a "have we already announced
    this milestone for THIS next_rebal?" flag. event_key embeds
    next_rebal so the next cycle's milestones are not blocked by a stale
    state row.
    """
    rebal = snapshot.get("rebalance") or {}
    last = rebal.get("last", "")
    cycle = rebal.get("cycle", 21)
    if not last:
        return

    try:
        from datetime import date, timedelta
        last_date = date(int(last[:4]), int(last[4:6]), int(last[6:8]))
        # Approximate next rebal (weekdays only)
        from regime.calendar import next_trading_day
        d = last_date
        for _ in range(cycle):
            d = next_trading_day(d)
        next_rebal = d
        today = date.today()
        d_day = (next_rebal - today).days

        for target in [7, 3, 1]:
            if d_day != target:
                continue
            ev_key = f"rebal_d{target}_{next_rebal}"
            target_state = f"D-{target}"
            # One-shot gate: skip if we already announced this milestone
            # for this specific next_rebal date. A new rebalance cycle
            # produces a different next_rebal → different event_key →
            # state lookup misses → alert fires again as intended.
            if get_last_state(ev_key) == target_state:
                continue
            events.append(AlertEvent(
                event_key=ev_key,
                category="rebal",
                severity="INFO",
                message=f"📅 <b>리밸런싱 D-{target}</b>\n예정일: {next_rebal}",
                state=target_state,
            ))
    except Exception:
        pass
