# -*- coding: utf-8 -*-
"""Lab realtime / simulate result persistence to PostgreSQL.

Companion to ``lab_simulator._save_result`` (which writes the JSON
+ CSV file pair). This module owns the PG triple-write leg.

Schema is defined in ``shared/db/migrations/v018_lab_realtime_persist.py``.

Failure policy:
    PG insert failures are SWALLOWED — the caller (``_save_result``)
    is the file-side authority and must not be blocked by an
    unavailable database. Failures emit ``[LAB_RT_PG_*]`` warnings.

Disable at runtime:
    QTRON_LAB_REALTIME_PG=0 → save_result_pg() returns early without
    touching PG. JSON + CSV continue to work.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def _is_disabled() -> bool:
    """Operator kill-switch. Defaults to enabled."""
    return os.environ.get("QTRON_LAB_REALTIME_PG", "1") == "0"


def _build_summary(strategies: List[Dict]) -> Dict:
    """Strategy-level summary for the run-level ``summary`` JSONB.
    Keeps run row compact while preserving the metrics analysts
    actually filter on (total_pnl, win_rate, trade_count, cash)."""
    summary: Dict[str, Dict] = {}
    for s in strategies or []:
        name = s.get("name") or s.get("strategy") or "?"
        summary[str(name)] = {
            "total_pnl":   s.get("total_pnl"),
            "win_count":   s.get("win_count"),
            "loss_count":  s.get("loss_count"),
            "win_rate":    s.get("win_rate"),
            "cash":        s.get("cash"),
            "trade_count": len(s.get("trades", []) or []),
        }
    return summary


def _coerce_ts(v: Any) -> Optional[str]:
    """Pass through ISO/datetime strings, return None for empty/missing
    so PG accepts NULL for unset values."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def save_result_pg(result: Dict) -> Optional[int]:
    """Persist ``result`` (the dict produced by ``_build_result``) to PG.

    Returns the inserted ``run_id`` on success, ``None`` on disabled
    or failure. Never raises — the file sinks are authoritative.
    """
    if _is_disabled():
        return None

    try:
        from shared.db.pg_base import connection
    except Exception as e:
        logger.warning(f"[LAB_RT_PG_IMPORT] {e!r}")
        return None

    sim_ts = _coerce_ts(result.get("stopped_at") or result.get("timestamp"))
    if not sim_ts:
        logger.warning("[LAB_RT_PG_SKIP] no stopped_at/timestamp on result")
        return None

    started_at = _coerce_ts(result.get("started_at"))
    stopped_at = _coerce_ts(result.get("stopped_at") or result.get("timestamp"))
    mode = result.get("mode") or "simulate"
    elapsed_sec = result.get("elapsed_sec")
    tick_count = result.get("tick_count")
    initial_cash = result.get("initial_cash")
    ranking_count = result.get("ranking_count")
    params = result.get("params") or {}
    strategies = result.get("strategies") or []
    summary = _build_summary(strategies)

    try:
        with connection() as conn:
            cur = conn.cursor()
            # Run row — ON CONFLICT keeps the migration idempotent
            # under a manual re-save (sim_ts is the natural key).
            cur.execute(
                """
                INSERT INTO lab_realtime_runs (
                    sim_ts, mode, started_at, stopped_at,
                    elapsed_sec, tick_count, initial_cash, ranking_count,
                    params, summary
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                )
                ON CONFLICT (sim_ts) DO UPDATE SET
                    mode          = EXCLUDED.mode,
                    started_at    = EXCLUDED.started_at,
                    stopped_at    = EXCLUDED.stopped_at,
                    elapsed_sec   = EXCLUDED.elapsed_sec,
                    tick_count    = EXCLUDED.tick_count,
                    initial_cash  = EXCLUDED.initial_cash,
                    ranking_count = EXCLUDED.ranking_count,
                    params        = EXCLUDED.params,
                    summary       = EXCLUDED.summary
                RETURNING run_id
                """,
                (
                    sim_ts, mode, started_at, stopped_at,
                    elapsed_sec, tick_count, initial_cash, ranking_count,
                    json.dumps(params, ensure_ascii=False, default=str),
                    json.dumps(summary, ensure_ascii=False, default=str),
                ),
            )
            row = cur.fetchone()
            run_id = int(row[0]) if row else None
            if run_id is None:
                conn.rollback()
                cur.close()
                logger.warning("[LAB_RT_PG_RUN] no run_id returned")
                return None

            # Replace trade rows for the run so a re-save is fully
            # idempotent (run_id stays stable, trades refresh).
            cur.execute(
                "DELETE FROM lab_realtime_trades WHERE run_id = %s",
                (run_id,),
            )

            trade_rows: List[tuple] = []
            for s in strategies:
                strategy_name = s.get("name") or s.get("strategy") or "?"
                for t in s.get("trades", []) or []:
                    trade_rows.append((
                        run_id, sim_ts, str(strategy_name),
                        t.get("rank"),
                        t.get("code"), t.get("name"),
                        t.get("side"),
                        t.get("entry_price"),
                        t.get("exit_price") or t.get("price"),
                        t.get("qty"),
                        t.get("pnl"), t.get("pnl_pct"),
                        t.get("exit_reason") or t.get("reason"),
                        str(t.get("entry_time") or "") or None,
                        str(t.get("exit_time") or t.get("timestamp") or "") or None,
                    ))

            if trade_rows:
                cur.executemany(
                    """
                    INSERT INTO lab_realtime_trades (
                        run_id, sim_ts, strategy,
                        rank_value, code, name, side,
                        entry_price, exit_price, qty,
                        pnl, pnl_pct, exit_reason,
                        entry_time, exit_time
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    trade_rows,
                )
            conn.commit()
            cur.close()
            logger.info(
                f"[LAB_RT_PG] run_id={run_id} mode={mode} "
                f"trades={len(trade_rows)} sim_ts={sim_ts}"
            )
            return run_id
    except Exception as e:
        logger.warning(f"[LAB_RT_PG_FAIL] {e!r}")
        return None
