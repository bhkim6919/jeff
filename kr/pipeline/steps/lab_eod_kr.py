# -*- coding: utf-8 -*-
"""kr/pipeline/steps/lab_eod_kr.py — HTTP POST wrapper for KR Lab Live EOD.

Wraps the localhost REST calls that tray_server currently makes:
    1. GET  /api/health                     — liveness gate
    2. POST /api/lab/live/start             — idempotent simulator init
    3. POST /api/lab/live/run-daily         — 9-strategy EOD close-out

The wrapper does NOT touch the lab_live engine directly — per the Engine
Protection rule and Jeff-approved open issue #4, this is a black-box POST.
If the FastAPI response says `skipped=True` the step records SKIPPED
rather than DONE (idempotency already handled upstream).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from ..state import PipelineState
from .base import StepBase, StepRunResult

_log = logging.getLogger("gen4.pipeline.steps.lab_eod_kr")

# Default port. Tray uses a PORT constant; expose as arg for injection/test.
DEFAULT_PORT = 8080


class LabEodKrStep(StepBase):
    """Pipeline step: KR Lab Live daily EOD (9-strategy forward trading)."""

    name = "lab_eod_kr"
    preconditions = ("batch",)  # Lab EOD needs today's target/snapshot

    # Lab EOD is the slowest KR task (~60–120s). Retry window matches the
    # legacy LAB_EOD_MAX_FAILS/5min backoff in tray_server.
    backoff_min_wait_sec = 300
    backoff_max_fails = 3

    def __init__(
        self,
        *,
        port: int = DEFAULT_PORT,
        host: str = "localhost",
        http_get: Optional[Callable[..., Any]] = None,
        http_post: Optional[Callable[..., Any]] = None,
        health_timeout: float = 3.0,
        start_timeout: float = 30.0,
        run_daily_timeout: float = 180.0,
        update_ohlcv: bool = True,
        clock: Any = None,
    ):
        super().__init__(clock=clock)
        self._port = int(port)
        self._host = str(host)
        self._http_get = http_get
        self._http_post = http_post
        self._health_timeout = float(health_timeout)
        self._start_timeout = float(start_timeout)
        self._run_daily_timeout = float(run_daily_timeout)
        self._update_ohlcv = bool(update_ohlcv)

    # ---------- lazy requests import ----------

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

    # ---------- step execution ----------

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

        # 2. Simulator init (idempotent)
        try:
            post(
                self._url("/api/lab/live/start"),
                json={},
                timeout=self._start_timeout,
            )
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"start_fail: {e!r}",
                details={"phase": "start"},
            )

        # 3. Run daily
        try:
            r = post(
                self._url("/api/lab/live/run-daily"),
                json={"update_ohlcv": self._update_ohlcv},
                timeout=self._run_daily_timeout,
            )
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"run_daily_fail: {e!r}",
                details={"phase": "run_daily"},
            )

        status_code = getattr(r, "status_code", None)
        ok_flag = getattr(r, "ok", None)
        if ok_flag is False:
            return StepRunResult(
                ok=False,
                error=f"http_{status_code}",
                details={"phase": "run_daily", "status_code": status_code},
            )

        try:
            body = r.json()
        except Exception as e:
            return StepRunResult(
                ok=False,
                error=f"json_decode_fail: {e!r}",
                details={"phase": "run_daily"},
            )

        elapsed = round(time.monotonic() - t0, 2)

        if body.get("skipped"):
            # Lab engine already ran today — treat as intentional no-op.
            return StepRunResult(
                ok=False,
                skipped=True,
                error=body.get("reason") or "already_run",
                details={
                    "reason": body.get("reason"),
                    "duration_sec": elapsed,
                },
            )

        if body.get("ok"):
            return StepRunResult(
                ok=True,
                details={
                    "trades": body.get("trades", 0),
                    "strategies": body.get("strategies"),
                    "trade_date": body.get("trade_date"),
                    "duration_sec": elapsed,
                },
            )

        # Neither ok nor skipped — an explicit failure payload from the API.
        return StepRunResult(
            ok=False,
            error=body.get("error") or "run_daily_not_ok",
            details={"phase": "run_daily", "body": body},
        )
