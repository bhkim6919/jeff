"""
meta_db.py -- Gen5 Meta Layer Phase 0: PostgreSQL helpers
==========================================================
Observer-only. 시장 컨텍스트 + 전략 성과를 구조화 저장.
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("lab.meta")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Save ──────────────────────────────────────────────────────

def save_market_context(data: dict) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO meta_market_context
                (trade_date, kospi_return, adv_ratio, small_vs_large,
                 sector_dispersion, breakout_ratio, regime_score, regime_label,
                 data_snapshot_id, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date) DO UPDATE SET
                    kospi_return = EXCLUDED.kospi_return,
                    adv_ratio = EXCLUDED.adv_ratio,
                    small_vs_large = EXCLUDED.small_vs_large,
                    sector_dispersion = EXCLUDED.sector_dispersion,
                    breakout_ratio = EXCLUDED.breakout_ratio,
                    regime_score = EXCLUDED.regime_score,
                    regime_label = EXCLUDED.regime_label,
                    data_snapshot_id = EXCLUDED.data_snapshot_id,
                    run_ts = EXCLUDED.run_ts,
                    created_at = EXCLUDED.created_at
                WHERE meta_market_context.run_ts < EXCLUDED.run_ts
            """, (
                data["trade_date"],
                data.get("kospi_return"), data.get("adv_ratio"),
                data.get("small_vs_large"), data.get("sector_dispersion"),
                data.get("breakout_ratio"), data.get("regime_score"),
                data.get("regime_label"), data.get("data_snapshot_id"),
                run_ts, _now_iso(),
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_market_context failed: {e}")


def save_strategy_daily(rows: List[dict]) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            for r in rows:
                cur.execute("""
                    INSERT INTO meta_strategy_daily
                    (trade_date, strategy, strategy_version, daily_return,
                     cumul_return, position_count, win_count, loss_count,
                     turnover, cash_ratio, gross_exposure, run_ts, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, strategy) DO UPDATE SET
                        daily_return = EXCLUDED.daily_return,
                        cumul_return = EXCLUDED.cumul_return,
                        position_count = EXCLUDED.position_count,
                        run_ts = EXCLUDED.run_ts,
                        created_at = EXCLUDED.created_at
                    WHERE meta_strategy_daily.run_ts < EXCLUDED.run_ts
                """, (
                    r["trade_date"], r["strategy"], r["strategy_version"],
                    r.get("daily_return"), r.get("cumul_return"),
                    r.get("position_count"), r.get("win_count"),
                    r.get("loss_count"), r.get("turnover"),
                    r.get("cash_ratio"), r.get("gross_exposure"),
                    run_ts, _now_iso(),
                ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_strategy_daily failed: {e}")


def save_strategy_exposure(rows: List[dict]) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            for r in rows:
                cur.execute("""
                    INSERT INTO meta_strategy_exposure
                    (trade_date, strategy, avg_market_cap, top1_weight,
                     top5_weight, sector_top1, sector_top1_weight,
                     sector_dispersion, run_ts, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, strategy) DO UPDATE SET
                        avg_market_cap = EXCLUDED.avg_market_cap,
                        top1_weight = EXCLUDED.top1_weight,
                        run_ts = EXCLUDED.run_ts
                    WHERE meta_strategy_exposure.run_ts < EXCLUDED.run_ts
                """, (
                    r["trade_date"], r["strategy"],
                    r.get("avg_market_cap"), r.get("top1_weight"),
                    r.get("top5_weight"), r.get("sector_top1"),
                    r.get("sector_top1_weight"), r.get("sector_dispersion"),
                    run_ts, _now_iso(),
                ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_strategy_exposure failed: {e}")


def save_run_quality(data: dict) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO meta_run_quality
                (trade_date, snapshot_version, market, sync_status,
                 synced_count, failed_count, expected_count, completeness_ratio,
                 selected_source, csv_last_date, db_last_date,
                 data_snapshot_id, degraded_flag, ohlc_invariant_warn_count,
                 run_id, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, snapshot_version) DO NOTHING
            """, (
                data["trade_date"], data["snapshot_version"],
                data.get("market", "KR"),
                data.get("sync_status"), data.get("synced_count"),
                data.get("failed_count"), data.get("expected_count"),
                data.get("completeness_ratio"),
                data.get("selected_source"), data.get("csv_last_date"),
                data.get("db_last_date"), data.get("data_snapshot_id"),
                data.get("degraded_flag", 0),
                data.get("ohlc_invariant_warn_count", 0),
                data.get("run_id"), run_ts, _now_iso(),
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_run_quality failed: {e}")


def save_strategy_risk_daily(rows: List[dict]) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            for r in rows:
                cur.execute("""
                    INSERT INTO meta_strategy_risk
                    (trade_date, strategy, snapshot_version,
                     daily_mdd, rolling_5d_return, rolling_20d_return,
                     rolling_20d_mdd, realized_vol_20d, hit_rate_20d,
                     avg_hold_days, slippage_bps_est, cost_bps_est,
                     run_ts, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, strategy) DO UPDATE SET
                        daily_mdd = EXCLUDED.daily_mdd,
                        rolling_5d_return = EXCLUDED.rolling_5d_return,
                        run_ts = EXCLUDED.run_ts
                    WHERE meta_strategy_risk.run_ts < EXCLUDED.run_ts
                """, (
                    r["trade_date"], r["strategy"], r.get("snapshot_version"),
                    r.get("daily_mdd"), r.get("rolling_5d_return"),
                    r.get("rolling_20d_return"), r.get("rolling_20d_mdd"),
                    r.get("realized_vol_20d"), r.get("hit_rate_20d"),
                    r.get("avg_hold_days"), r.get("slippage_bps_est"),
                    r.get("cost_bps_est"), run_ts, _now_iso(),
                ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_strategy_risk_daily failed: {e}")


def save_universe_snapshot(data: dict) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO meta_universe_snapshot
                (trade_date, snapshot_version,
                 universe_count_raw, universe_count_filtered,
                 missing_data_count, tradable_count,
                 excluded_reasons_json, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, snapshot_version) DO NOTHING
            """, (
                data["trade_date"], data["snapshot_version"],
                data.get("universe_count_raw"),
                data.get("universe_count_filtered"),
                data.get("missing_data_count"),
                data.get("tradable_count"),
                data.get("excluded_reasons_json"),
                run_ts, _now_iso(),
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_universe_snapshot failed: {e}")


def save_recommendation_log(data: dict) -> None:
    run_ts = now_utc()
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO meta_recommendation
                (trade_date, snapshot_version, recommendation_date,
                 recommended_weights_json, top_strategy, top3_strategies_json,
                 confidence_score, regime_label, regime_persistence_days,
                 market_fit_summary, perf_health_summary,
                 data_quality_status, is_valid, reason_codes,
                 selected_source, run_ts, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, snapshot_version) DO NOTHING
            """, (
                data["trade_date"], data["snapshot_version"],
                data.get("recommendation_date", data["trade_date"]),
                data.get("recommended_weights_json"),
                data.get("top_strategy"), data.get("top3_strategies_json"),
                data.get("confidence_score"), data.get("regime_label"),
                data.get("regime_persistence_days"),
                data.get("market_fit_summary"), data.get("perf_health_summary"),
                data.get("data_quality_status"), data.get("is_valid", 1),
                data.get("reason_codes"), data.get("selected_source"),
                run_ts, _now_iso(),
            ))
            conn.commit()
            cur.close()
    except Exception as e:
        logger.error(f"[META_DB] save_recommendation_log failed: {e}")


# ── Query ─────────────────────────────────────────────────────

def get_market_context(trade_date: str) -> Optional[dict]:
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM meta_market_context WHERE trade_date=%s",
            (trade_date,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        cur.close()
    return dict(zip(cols, row))


def get_strategy_daily(trade_date: str) -> List[dict]:
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM meta_strategy_daily "
            "WHERE trade_date=%s ORDER BY strategy",
            (trade_date,),
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


# ── Verification ──────────────────────────────────────────────

def verify_row_counts(trade_date: str, expected_strategies: int = 9) -> dict:
    """Verify all expected rows exist for a date."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM meta_market_context WHERE trade_date=%s",
            (trade_date,),
        )
        mc = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM meta_strategy_daily WHERE trade_date=%s",
            (trade_date,),
        )
        sd = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM meta_strategy_exposure WHERE trade_date=%s",
            (trade_date,),
        )
        se = cur.fetchone()[0]
        cur.close()

    missing = []
    if mc == 0:
        missing.append("market_context")
    if sd < expected_strategies:
        missing.append(f"strategy_daily({sd}/{expected_strategies})")
    if se < expected_strategies:
        missing.append(f"exposure_daily({se}/{expected_strategies})")

    return {
        "market_context": mc,
        "strategy_daily": sd,
        "exposure_daily": se,
        "missing_strategies": missing,
    }
