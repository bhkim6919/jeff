# -*- coding: utf-8 -*-
"""kr/pipeline/mode.py — Execution mode detection and trade_date resolution.

Two responsibilities:
1. `detect_mode()` — which execution context is running right now
   (live / paper_forward / lab / backtest). Falls back to paper_forward
   as the safest default (no real orders possible).
2. `resolve_trade_date(now)` — the KR market trade_date the pipeline
   should be scoped to. Uses pykrx's business-day API and falls back to
   a plain Mon–Fri heuristic if pykrx is unreachable (offline dev env).

Resolution NEVER returns a future date: if now() is Saturday, we resolve
to the prior Friday. This matches Jeff-approved open issue #2
(last_trading_day, not calendar_today).
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

from .schema import (
    ALL_MODES,
    MODE_BACKTEST,
    MODE_LAB,
    MODE_LIVE,
    MODE_PAPER_FORWARD,
)

_log = logging.getLogger("gen4.pipeline.mode")

_MODE_ENV_VAR = "QTRON_MODE"


def detect_mode(*, default: str = MODE_PAPER_FORWARD) -> str:
    """Return the active execution mode.

    Precedence:
        1. `QTRON_MODE` env var (explicit override — wins over everything)
        2. `default` kwarg (caller-specified)

    Process-scanning heuristics (checking for `main.py --live` etc.) are
    intentionally deferred: they require extra deps (psutil) and are
    brittle under Windows. Callers that need stricter detection should
    pass `default=MODE_LIVE` from inside the live entry point.
    """
    override = os.environ.get(_MODE_ENV_VAR, "").strip().lower()
    if override:
        if override in ALL_MODES:
            return override
        _log.warning(
            "[PIPELINE_MODE_INVALID] env=%s not in %s; falling back to default=%s",
            override, sorted(ALL_MODES), default,
        )
    if default not in ALL_MODES:
        raise ValueError(f"invalid default mode: {default!r}")
    return default


def resolve_trade_date(now: Optional[datetime] = None) -> date:
    """Return the KR market trade_date for the pipeline today.

    - Weekdays that are also KR trading days → today's date
    - Weekends / KR public holidays → most recent prior trading day

    Never returns a future date. Uses pykrx when available; falls back to
    a pure weekday heuristic (Mon–Fri = trading) when pykrx import or
    network call fails, with a warning log so the degradation is visible.
    """
    if now is None:
        now = datetime.now()
    today = now.date()

    try:
        from pykrx import stock as _pykrx_stock  # type: ignore

        start = (today - timedelta(days=10)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        # get_previous_business_day returns the last KRX business day
        # on or before `date` (string YYYYMMDD).
        last_bd_str = _pykrx_stock.get_nearest_business_day_in_a_week(end)
        if last_bd_str:
            last_bd = datetime.strptime(last_bd_str, "%Y%m%d").date()
            # Guard: nearest_business_day can return a future date if
            # `end` itself isn't a business day but the next one is.
            # We want on-or-before only.
            if last_bd > today:
                last_bd = _fallback_last_weekday(today)
            _log.debug(
                "[PIPELINE_TRADE_DATE] resolved=%s source=pykrx window=%s..%s",
                last_bd, start, end,
            )
            return last_bd
    except Exception as e:  # pykrx missing or offline
        _log.warning(
            "[PIPELINE_TRADE_DATE_FALLBACK] pykrx unavailable (%s); "
            "using Mon-Fri heuristic", e,
        )

    return _fallback_last_weekday(today)


def _fallback_last_weekday(d: date) -> date:
    """Last Mon–Fri on or before d. Used when pykrx is unavailable."""
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d
