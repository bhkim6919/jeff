# -*- coding: utf-8 -*-
"""
migrate_sqlite_to_pg.py — SQLite → PostgreSQL 일괄 이전
========================================================
기존 SQLite .db 파일의 데이터를 PostgreSQL로 이전.
idempotent: ON CONFLICT DO NOTHING. 여러 번 실행해도 안전.

Usage:
    python -m shared.db.migrate_sqlite_to_pg
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("qtron.migrate")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ── Column Mappings (SQLite table → PG table) ────────────────

MIGRATIONS = [
    # (sqlite_db_path, sqlite_table, pg_table, column_mapping, pk_cols)
    # column_mapping: {sqlite_col: pg_col} or None (same names)

    # regime.db
    (
        "kr/data/regime/regime.db", "predictions", "regime_predictions",
        {
            "feature_date": "feature_date", "target_date": "target_date",
            "predicted_regime": "predicted_regime", "predicted_label": "predicted_label",
            "composite_score": "composite_score",
            "global_score": "global_score", "global_avail": "global_avail",
            "vol_score": "vol_score", "vol_avail": "vol_avail",
            "domestic_score": "domestic_score", "domestic_avail": "domestic_avail",
            "micro_score": "micro_score", "micro_avail": "micro_avail",
            "fx_score": "fx_score", "fx_avail": "fx_avail",
            "available_weight": "available_weight",
            "confidence_flag": "confidence_flag",
            "source_health": "source_health",
            "created_at": "created_at",
        },
        ["target_date"],
    ),
    (
        "kr/data/regime/regime.db", "actuals", "regime_actuals",
        {
            "market_date": "market_date", "actual_regime": "actual_regime",
            "actual_label": "actual_label", "kospi_change": "kospi_change",
            "actual_method": "actual_method", "created_at": "created_at",
        },
        ["market_date"],
    ),
    (
        "kr/data/regime/regime.db", "scores", "regime_scores",
        {
            "predicted": "predicted", "actual": "actual",
            "distance": "distance", "raw_confidence": "raw_confidence",
            "adjusted_confidence": "adjusted_confidence",
            "available_weight": "available_weight",
            "confidence_flag": "confidence_flag",
            "created_at": "created_at",
        },
        ["target_date"],
    ),

    # theme_regime.db
    (
        "kr/data/regime/theme_regime.db", "theme_daily", "regime_theme_daily",
        None, ["market_date", "theme_code"],
    ),

    # dashboard.db
    (
        "kr/data/dashboard/dashboard.db", "market_snapshots", "dashboard_snapshots",
        {
            "market_date": "market_date", "ts": "ts", "epoch": "epoch",
            "kospi_price": "kospi_price", "kospi_change_pct": "kospi_change_pct",
            "kosdaq_price": "kosdaq_price", "kosdaq_change_pct": "kosdaq_change_pct",
            "portfolio_equity": "portfolio_equity",
            "portfolio_pnl_pct": "portfolio_pnl_pct",
            "portfolio_cash": "portfolio_cash",
            "holdings_count": "holdings_count",
            "source": "source",
        },
        ["market_date", "epoch"],
    ),
]


def _open_sqlite(db_path: str) -> Optional[sqlite3.Connection]:
    """SQLite DB 열기 (없으면 None)."""
    full = PROJECT_ROOT / db_path
    if not full.exists():
        logger.warning(f"[MIGRATION] sqlite not found: {full}")
        return None
    conn = sqlite3.connect(str(full))
    conn.row_factory = sqlite3.Row
    return conn


def _get_sqlite_columns(sq_conn, table: str) -> List[str]:
    """SQLite 테이블의 컬럼 목록."""
    cur = sq_conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row["name"] for row in cur.fetchall()]
    cur.close()
    return cols


def migrate_table(
    sqlite_path: str,
    sqlite_table: str,
    pg_table: str,
    col_map: Optional[Dict[str, str]],
    pk_cols: List[str],
) -> Dict[str, int]:
    """
    단일 테이블 이전. ON CONFLICT DO NOTHING (idempotent).

    Returns: {"inserted": N, "skipped": N, "total": N}
    """
    sq_conn = _open_sqlite(sqlite_path)
    if sq_conn is None:
        return {"inserted": 0, "skipped": 0, "total": 0, "error": "file not found"}

    try:
        sq_cols = _get_sqlite_columns(sq_conn, sqlite_table)
        if col_map:
            src_cols = [c for c in col_map.keys() if c in sq_cols]
            dst_cols = [col_map[c] for c in src_cols]
        else:
            src_cols = sq_cols
            dst_cols = sq_cols

        cur = sq_conn.cursor()
        cur.execute(f"SELECT {', '.join(src_cols)} FROM {sqlite_table}")
        rows = cur.fetchall()
        cur.close()
        total = len(rows)

        if total == 0:
            logger.info(f"[MIGRATION] {sqlite_table} → {pg_table}: empty, skip")
            return {"inserted": 0, "skipped": 0, "total": 0}

        inserted = 0
        with connection() as pg_conn:
            pg_cur = pg_conn.cursor()
            placeholders = ", ".join(["%s"] * len(dst_cols))
            cols_str = ", ".join(dst_cols)
            sql = (
                f"INSERT INTO {pg_table} ({cols_str}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING"
            )

            for row in rows:
                values = [row[c] for c in src_cols]
                pg_cur.execute(sql, values)
                if pg_cur.rowcount > 0:
                    inserted += 1

            pg_conn.commit()
            pg_cur.close()

        skipped = total - inserted
        logger.info(
            f"[MIGRATION] {sqlite_table} → {pg_table}: "
            f"inserted={inserted}, skipped={skipped}, total={total}"
        )
        return {"inserted": inserted, "skipped": skipped, "total": total}

    finally:
        sq_conn.close()


def run_all() -> Dict[str, Any]:
    """전체 SQLite → PG 이전 실행."""
    results = {}
    for sqlite_path, sq_table, pg_table, col_map, pk_cols in MIGRATIONS:
        key = f"{sq_table} → {pg_table}"
        try:
            results[key] = migrate_table(
                sqlite_path, sq_table, pg_table, col_map, pk_cols
            )
        except Exception as e:
            logger.error(f"[MIGRATION] FAILED {key}: {e}", exc_info=e)
            results[key] = {"error": str(e)}
    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    print("=== SQLite → PostgreSQL Migration ===")

    # 1. Run migrations first
    from shared.db.migration import MigrationRunner
    runner = MigrationRunner()
    applied = runner.apply_pending()
    if applied:
        print(f"Applied migrations: {applied}")

    # 2. Migrate data
    results = run_all()
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("=== Done ===")
