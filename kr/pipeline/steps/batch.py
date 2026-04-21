# -*- coding: utf-8 -*-
"""kr/pipeline/steps/batch.py — Thin wrapper around lifecycle.batch.run_batch.

Wraps the existing KR batch entry point so the pipeline can track its
completion centrally. Logic is NOT duplicated — we call the same
`run_batch(config, fast=True)` that tray_server currently invokes.

Returns a StepRunResult with details preserving:
    target_count        — number of target stocks selected
    snapshot_version    — idempotency key from target dict
    selected_source     — "DB" | "CSV" (data freshness trace)
    data_last_date      — last trading day of the loaded OHLCV
    trade_date          — batch trade_date (from target dict)
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Optional

from ..state import PipelineState
from .base import StepBase, StepRunResult
from .time_window import TimeWindow, _TIME_WINDOW_UNSET

_log = logging.getLogger("gen4.pipeline.steps.batch")


class BatchStep(StepBase):
    """Pipeline step: KR daily batch (OHLCV update + universe + scoring)."""

    name = "batch"
    # bootstrap_env and ohlcv_sync (once Phase 2b+ adds it) are natural
    # prerequisites. For now we only require bootstrap_env — ohlcv_sync is
    # embedded inside run_batch itself (step1_ohlcv checkpoint).
    preconditions = ("bootstrap_env",)

    # Batch can be slow (~30–60s) and pykrx has its own internal retries;
    # keep the outer backoff window conservative but not aggressive.
    backoff_min_wait_sec = 300   # 5 min between outer retries
    backoff_max_fails = 3

    # Legacy tray window: BATCH_HOUR=16, BATCH_MINUTE=5, wide (~55 min)
    # acceptance so a slow pykrx start still lands inside.
    time_window = TimeWindow(tz="Asia/Seoul", hour=16, minute=5, window_sec=3300)

    def __init__(
        self,
        *,
        config_factory: Optional[Callable[[], Any]] = None,
        run_batch_fn: Optional[Callable[..., Any]] = None,
        fast: bool = True,
        clock: Any = None,
        time_window: Any = _TIME_WINDOW_UNSET,
    ):
        """Create a BatchStep.

        Parameters
        ----------
        config_factory : Callable[[], config], optional
            Returns a ready `Gen4Config` instance. Default: import and
            construct `Gen4Config` lazily (avoids pulling heavy deps at
            pipeline import time).
        run_batch_fn : Callable, optional
            Injection point for tests. Default: `lifecycle.batch.run_batch`.
        fast : bool
            Passed through to run_batch. Default True (matches tray behavior).
        """
        super().__init__(clock=clock, time_window=time_window)
        self._config_factory = config_factory
        self._run_batch_fn = run_batch_fn
        self._fast = fast

    # ---------- default dependency resolvers ----------

    def _load_config(self):
        if self._config_factory is not None:
            return self._config_factory()
        # Mirror tray_server._run_batch import-guard logic (Gen2 shadow)
        import sys as _sys
        _stale = _sys.modules.pop("config", None)
        if _stale is not None:
            _log.debug("[BATCH_STEP_IMPORT_GUARD] evicted stale `config` module")
        from config import Gen4Config  # noqa: WPS433 — lazy by design
        cfg = Gen4Config()
        cfg.ensure_dirs()
        return cfg

    def _load_run_batch(self) -> Callable[..., Any]:
        if self._run_batch_fn is not None:
            return self._run_batch_fn
        from lifecycle.batch import run_batch  # noqa: WPS433 — lazy
        return run_batch

    # ---------- step execution ----------

    def _execute(self, state: PipelineState) -> StepRunResult:
        config = self._load_config()
        run_batch = self._load_run_batch()

        t0 = time.monotonic()
        try:
            target = run_batch(config, fast=self._fast)
        except Exception as e:  # underlying task crash
            return StepRunResult(ok=False, error=f"run_batch_crash: {e!r}")
        elapsed = time.monotonic() - t0

        # run_batch returns None on soft-failure (e.g. empty universe).
        if target is None:
            return StepRunResult(
                ok=False,
                error="run_batch_returned_none",
                details={"duration_sec": round(elapsed, 2)},
            )

        target_count = len(target.get("target_tickers") or [])
        details = {
            "target_count": target_count,
            "snapshot_version": target.get("snapshot_version"),
            "selected_source": target.get("selected_source"),
            "data_last_date": target.get("data_last_date"),
            "trade_date": target.get("date"),
            "duration_sec": round(elapsed, 2),
            "fast": bool(self._fast),
        }

        # An empty target list after a clean return is suspect — record as
        # fail so the orchestrator retries once backoff opens, rather than
        # marking DONE on a zero-stock portfolio.
        if target_count == 0:
            return StepRunResult(
                ok=False,
                error="empty_target_tickers",
                details=details,
            )

        return StepRunResult(ok=True, details=details)
