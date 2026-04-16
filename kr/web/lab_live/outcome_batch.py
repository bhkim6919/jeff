"""
outcome_batch.py -- Forward outcome 후행 계산 (T+5 이후)
=====================================================
look-ahead contamination 방지: trade_date <= today - 5 대상만.
snapshot 정합성: data_snapshot_id 기반 검증.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("lab.meta.outcome")


def compute_pending_outcomes(min_delay_days: int = 5) -> dict:
    """T+N일 이상 경과한 trade_date 중 outcome 미계산 건을 처리.

    Returns: {"computed": N, "skipped": N, "invalid": N}
    """
    result = {"computed": 0, "skipped": 0, "invalid": 0}

    with connection() as conn:
      try:
        cutoff = (datetime.now() - timedelta(days=min_delay_days)).strftime("%Y-%m-%d")
        cur = conn.cursor()

        # trade_date that have strategy_daily but no outcome yet
        cur.execute("""
            SELECT DISTINCT sd.trade_date
            FROM meta_strategy_daily sd
            LEFT JOIN meta_strategy_outcome so
                ON sd.trade_date = so.trade_date AND sd.strategy = so.strategy
            WHERE sd.trade_date <= %s
                AND so.trade_date IS NULL
            ORDER BY sd.trade_date
        """, (cutoff,))
        rows = cur.fetchall()

        pending_dates = [r[0] for r in rows]

        if not pending_dates:
            logger.info("[OUTCOME_BATCH] No pending dates")
            return result

        logger.info(f"[OUTCOME_BATCH] {len(pending_dates)} dates to compute: "
                    f"{pending_dates[:5]}{'...' if len(pending_dates) > 5 else ''}")

        for trade_date in pending_dates:
            _compute_one_date(conn, cur, trade_date, min_delay_days, result)

        conn.commit()
        cur.close()
        logger.info(
            f"[OUTCOME_BATCH] Done: computed={result['computed']}, "
            f"skipped={result['skipped']}, invalid={result['invalid']}"
        )

      except Exception as e:
        logger.error(f"[OUTCOME_BATCH] Failed: {e}", exc_info=True)
        conn.rollback()

    return result


def _compute_one_date(conn, cur, trade_date: str,
                       min_delay: int, result: dict) -> None:
    """Compute outcomes for one trade_date."""
    # Get run quality for this date
    cur.execute(
        "SELECT snapshot_version, data_snapshot_id, sync_status, degraded_flag "
        "FROM meta_run_quality WHERE trade_date = %s "
        "ORDER BY created_at DESC LIMIT 1",
        (trade_date,),
    )
    rq = cur.fetchone()

    snapshot_version = rq[0] if rq else ""
    data_snapshot_id_t0 = rq[1] if rq else None
    sync_ok = rq and rq[2] == "OK" and rq[3] == 0

    # Get all strategies for this date
    cur.execute(
        "SELECT strategy, daily_return, cumul_return "
        "FROM meta_strategy_daily WHERE trade_date = %s",
        (trade_date,),
    )
    strategies = cur.fetchall()

    if not strategies:
        result["skipped"] += 1
        return

    # Get future equity data
    # We need equity_history from state files — but in batch context
    # we use strategy_daily rows for future dates
    td = datetime.strptime(trade_date, "%Y-%m-%d")

    for strat_row in strategies:
        sname = strat_row[0]

        # Get future daily returns
        cur.execute(
            """SELECT trade_date, daily_return, cumul_return
            FROM meta_strategy_daily
            WHERE strategy = %s AND trade_date > %s
            ORDER BY trade_date
            LIMIT %s""",
            (sname, trade_date, min_delay + 5),
        )
        future_rows = cur.fetchall()

        if len(future_rows) < 1:
            result["skipped"] += 1
            continue

        # Forward returns from daily_return chain
        fwd_returns = [r[1] for r in future_rows if r[1] is not None]

        fwd_1d = fwd_returns[0] if len(fwd_returns) >= 1 else None
        fwd_3d = _compound(fwd_returns[:3]) if len(fwd_returns) >= 3 else None
        fwd_5d = _compound(fwd_returns[:5]) if len(fwd_returns) >= 5 else None

        # MDD and max runup over 5d
        fwd_5d_mdd = None
        fwd_5d_max_runup = None
        fwd_vol_5d = None
        fwd_sharpe_5d = None

        if len(fwd_returns) >= 5:
            r5 = fwd_returns[:5]
            # Cumulative equity curve
            cum = [1.0]
            for r in r5:
                cum.append(cum[-1] * (1 + r))

            peak = cum[0]
            max_dd = 0
            max_ru = 0
            for c in cum[1:]:
                if c > peak:
                    peak = c
                dd = (c - peak) / peak if peak > 0 else 0
                if dd < max_dd:
                    max_dd = dd
                ru = (c - 1) / 1  # from start
                if ru > max_ru:
                    max_ru = ru

            fwd_5d_mdd = round(max_dd, 6)
            fwd_5d_max_runup = round(max_ru, 6)

            import numpy as np
            fwd_vol_5d = round(float(np.std(r5)) * (252 ** 0.5), 6)
            mean_r = np.mean(r5)
            std_r = np.std(r5)
            fwd_sharpe_5d = round(float(mean_r / std_r * (252 ** 0.5)), 4) if std_r > 0 else None

        # Cost adjusted (BUY 0.115% + SELL 0.295% = ~41 bps round trip)
        cost_bps = 41  # default KR
        cost_adj_1d = round(fwd_1d - cost_bps / 10000, 6) if fwd_1d is not None else None
        cost_adj_5d = round(fwd_5d - cost_bps / 10000, 6) if fwd_5d is not None else None

        # Snapshot mismatch check
        # T+5 시점의 data_snapshot_id와 T 시점을 비교
        rq_t5 = None
        if len(future_rows) >= 5:
            t5_date = future_rows[4][0]
            cur.execute(
                "SELECT data_snapshot_id FROM meta_run_quality "
                "WHERE trade_date = %s ORDER BY created_at DESC LIMIT 1",
                (t5_date,),
            )
            rq_t5_row = cur.fetchone()
            rq_t5 = rq_t5_row[0] if rq_t5_row else None

        is_valid = 1
        if not sync_ok:
            is_valid = 0
        if data_snapshot_id_t0 and rq_t5 and data_snapshot_id_t0 != rq_t5:
            logger.warning(
                f"[OUTCOME_SNAPSHOT_MISMATCH] {trade_date} {sname}: "
                f"t0={data_snapshot_id_t0} t5={rq_t5}"
            )
            # Note: different dates having different snapshots is EXPECTED
            # Mismatch means data was retroactively changed — only flag if same date re-run
            # For now, keep is_valid=1 (snapshot change between dates is normal)

        run_ts = now_utc()
        cur.execute(
            """INSERT INTO meta_strategy_outcome
            (trade_date, strategy, snapshot_version,
             fwd_1d_return, fwd_3d_return, fwd_5d_return,
             fwd_vol_5d, fwd_sharpe_5d,
             fwd_to_next_rebal_return,
             fwd_5d_mdd, fwd_5d_max_runup,
             cost_adjusted_fwd_1d, cost_adjusted_fwd_5d,
             label_win_1d, label_win_5d,
             is_valid, anchor_execution_date,
             computed_at, run_ts, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date, strategy) DO UPDATE SET
                fwd_1d_return = EXCLUDED.fwd_1d_return,
                fwd_3d_return = EXCLUDED.fwd_3d_return,
                fwd_5d_return = EXCLUDED.fwd_5d_return,
                run_ts = EXCLUDED.run_ts
            WHERE meta_strategy_outcome.run_ts < EXCLUDED.run_ts""",
            (
                trade_date, sname, snapshot_version,
                fwd_1d, fwd_3d, fwd_5d,
                fwd_vol_5d, fwd_sharpe_5d,
                None,
                fwd_5d_mdd, fwd_5d_max_runup,
                cost_adj_1d, cost_adj_5d,
                (1 if fwd_1d and fwd_1d > 0 else 0) if fwd_1d is not None else None,
                (1 if fwd_5d and fwd_5d > 0 else 0) if fwd_5d is not None else None,
                is_valid, trade_date,
                datetime.now().isoformat(timespec="seconds"),
                run_ts,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        result["computed"] += 1
        if is_valid == 0:
            result["invalid"] += 1


def _compound(returns: list) -> Optional[float]:
    """Compound a list of daily returns."""
    if not returns:
        return None
    cum = 1.0
    for r in returns:
        cum *= (1 + r)
    return round(cum - 1, 6)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    logging.basicConfig(level=logging.INFO)
    result = compute_pending_outcomes()
    print(f"Result: {result}")
