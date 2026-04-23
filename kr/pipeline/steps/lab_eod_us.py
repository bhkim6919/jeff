# -*- coding: utf-8 -*-
"""kr/pipeline/steps/lab_eod_us.py — HTTP POST wrapper for US Lab Forward EOD.

Wraps the US FastAPI endpoint on port 8081:
    POST /api/lab/forward/eod  { "date": "YYYY-MM-DD", "force": false }

The underlying handler calls `us.lab.forward.ForwardTrader.run_eod(...)`.
Mirroring the KR wrapper pattern — no engine internals are touched.

Precondition: `bootstrap_env`. Unlike KR, the US chain does NOT depend on
the KR batch (US uses Alpaca directly, not pykrx). This lets KR and US
pipelines advance independently.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Callable, Optional

from ..state import PipelineState
from .base import StepBase, StepRunResult
from .time_window import TimeWindow, _TIME_WINDOW_UNSET

_log = logging.getLogger("gen4.pipeline.steps.lab_eod_us")

DEFAULT_PORT = 8081


class LabEodUsStep(StepBase):
    """Pipeline step: US Lab Forward daily EOD (10-strategy paper trading)."""

    name = "lab_eod_us"
    # US chain runs independently of the KR batch — only bootstrap_env
    # is a hard prerequisite (tzdata + data_dir writable).
    preconditions = ("bootstrap_env",)

    backoff_min_wait_sec = 300
    backoff_max_fails = 3

    # Legacy tray window: US_LAB_EOD_HOUR=16, MINUTE=5 US/Eastern —
    # i.e. ~5 minutes after US close. Critical: never fire while the US
    # market is still open, so the tz must be US/Eastern (NOT Asia/Seoul).
    # Window widened from 60s → 600s (10 min) after 2026-04-22 incident
    # where a single skipped tick cost the whole EOD window.
    time_window = TimeWindow(tz="US/Eastern", hour=16, minute=5, window_sec=600)

    def __init__(
        self,
        *,
        port: int = DEFAULT_PORT,
        host: str = "localhost",
        http_get: Optional[Callable[..., Any]] = None,
        http_post: Optional[Callable[..., Any]] = None,
        health_timeout: float = 3.0,
        eod_timeout: float = 180.0,
        force: bool = False,
        eod_date: Optional[str] = None,
        clock: Any = None,
        time_window: Any = _TIME_WINDOW_UNSET,
    ):
        super().__init__(clock=clock, time_window=time_window)
        self._port = int(port)
        self._host = str(host)
        self._http_get = http_get
        self._http_post = http_post
        self._health_timeout = float(health_timeout)
        self._eod_timeout = float(eod_timeout)
        self._force = bool(force)
        self._eod_date = eod_date

    def _get(self):
        if self._http_get is not None:
            return self._http_get
        import requests  # noqa: WPS433 — lazy
        return requests.get

    def _post(self):
        if self._http_post is not None:
            return self._http_post
        import requests  # noqa: WPS433 — lazy
        return requests.post

    def _url(self, path: str) -> str:
        return f"http://{self._host}:{self._port}{path}"

    def _resolve_date(self) -> str:
        if self._eod_date:
            return str(self._eod_date)
        return date.today().isoformat()

    def _execute(self, state: PipelineState) -> StepRunResult:
        get = self._get()
        post = self._post()
        t0 = time.monotonic()

        # 1. Health check
        try:
            h = get(self._url("/api/health"), timeout=self._health_timeout)
            h.raise_for_status()
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"health_fail: {e!r}",
                details={"phase": "health"},
            )

        # 2. EOD
        eod_date = self._resolve_date()
        try:
            r = post(
                self._url("/api/lab/forward/eod"),
                json={"date": eod_date, "force": self._force},
                timeout=self._eod_timeout,
            )
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"eod_fail: {e!r}",
                details={"phase": "eod"},
            )

        status_code = getattr(r, "status_code", None)
        ok_flag = getattr(r, "ok", None)
        if ok_flag is False:
            return StepRunResult(
                ok=False,
                error=f"http_{status_code}",
                details={"phase": "eod", "status_code": status_code},
            )

        try:
            body = r.json()
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"json_decode_fail: {e!r}",
                details={"phase": "eod"},
            )

        elapsed = round(time.monotonic() - t0, 2)

        # US forward_eod returns {"error": "..."} on failure, or a dict
        # with "strategies_processed" on success. No explicit skipped flag.
        if isinstance(body, dict) and body.get("error"):
            return StepRunResult(
                ok=False,
                error=str(body["error"])[:500],
                details={"phase": "eod", "body": body},
            )

        strategies = body.get("strategies_processed") if isinstance(body, dict) else None
        return StepRunResult(
            ok=True,
            details={
                "eod_date": eod_date,
                "strategies_processed": strategies,
                "strategy_count": len(strategies) if strategies else 0,
                "duration_sec": elapsed,
            },
        )
