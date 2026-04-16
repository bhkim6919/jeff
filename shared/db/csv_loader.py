# -*- coding: utf-8 -*-
"""
csv_loader.py — CSV → PostgreSQL Loader + Quality Gate
=======================================================
장중 CSV append → EOD 후 PostgreSQL bulk insert.

[규칙]
- CSV는 DB가 아니다. 반드시 PG 적재 후 PG에서 조회.
- Quality gate 실패 시 적재 거부.
- Partial insert 금지 (전체 트랜잭션).
- Stale overwrite 방지 (run_ts 비교).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from shared.db.pg_base import connection
from shared.db.run_id import compute_file_hash, make_ingest_run_id, now_utc

logger = logging.getLogger("qtron.csv_loader")

# ── Conflict Policies ────────────────────────────────────────

TABLE_POLICIES = {
    "intraday_bars": {
        "conflict": "DO_UPDATE",
        "stale_check": True,
        "update_cols": ["open", "high", "low", "close", "volume", "status", "ingest_run_id", "run_ts"],
    },
    "micro_orderbook": {
        "conflict": "DO_UPDATE",
        "stale_check": True,
        "update_cols": [
            "price", "best_ask", "best_bid", "ask_qty_1", "bid_qty_1",
            "total_ask", "total_bid", "net_bid", "volume",
            "ingest_run_id", "run_ts",
        ],
    },
    "swing_ranking": {
        "conflict": "DO_NOTHING",
        "stale_check": False,
    },
    "lab_trades_history": {
        "conflict": "DO_NOTHING",
        "stale_check": False,
    },
}


# ── Quality Gate ─────────────────────────────────────────────

def csv_quality_gate(
    trade_date: str,
    dataset: str,
    csv_files: List[Path],
    conn,
) -> Tuple[bool, List[str]]:
    """
    CSV 적재 전 품질 검증. 실패 시 적재 거부.

    Returns:
        (ok, issues) — ok=False이면 적재 금지
    """
    issues = []

    if not csv_files:
        issues.append("NO_FILES")
        return False, issues

    # 1. stale file 감지: 이전 적재보다 오래된 파일
    prev_load = _get_last_load(conn, trade_date, dataset)
    if prev_load:
        prev_ts = prev_load["loaded_at"].timestamp()
        for f in csv_files:
            if f.stat().st_mtime < prev_ts:
                issues.append(f"STALE_FILE: {f.name} older than previous load")

    # 2. row count 급감: 이전 대비 50% 이하
    new_count = sum(_count_csv_rows(f) for f in csv_files)
    if prev_load and prev_load["row_count"] and prev_load["row_count"] > 0:
        ratio = new_count / prev_load["row_count"]
        if ratio < 0.5:
            issues.append(
                f"ROW_DROP: {new_count} vs prev {prev_load['row_count']} "
                f"({ratio:.1%})"
            )

    # 3. 종목 수 과소 (intraday 기준)
    if dataset == "intraday":
        code_count = len(csv_files)  # 파일 1개 = 종목 1개
        if code_count < 5:
            issues.append(f"CODE_SPARSE: only {code_count} codes")

    # 4. 빈 파일 검출
    for f in csv_files:
        if f.stat().st_size == 0:
            issues.append(f"EMPTY_FILE: {f.name}")

    if issues:
        logger.error(
            f"[CSV_QUALITY_REJECT] {dataset} {trade_date}: {issues}"
        )
        return False, issues

    return True, []


def _count_csv_rows(path: Path) -> int:
    """CSV 행 수 (헤더 제외)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return max(0, sum(1 for _ in f) - 1)
    except Exception:
        return 0


def _get_last_load(conn, trade_date: str, dataset: str) -> Optional[Dict]:
    """이전 적재 기록 조회."""
    cur = conn.cursor()
    cur.execute(
        "SELECT row_count, loaded_at FROM csv_load_log "
        "WHERE trade_date = %s AND dataset = %s",
        (trade_date, dataset),
    )
    row = cur.fetchone()
    cur.close()
    if row:
        return {"row_count": row[0], "loaded_at": row[1]}
    return None


# ── Loader ───────────────────────────────────────────────────

def load_csv_to_pg(
    trade_date: str,
    dataset: str,
    csv_dir: Path,
    pg_table: str,
    parse_fn: Callable[[Path], pd.DataFrame],
    file_pattern: str = "*.csv",
) -> Dict[str, Any]:
    """
    EOD 후 당일 CSV → PostgreSQL bulk insert.

    Args:
        trade_date: 거래일 (YYYY-MM-DD)
        dataset: 데이터셋 이름 ('intraday', 'micro', 'swing')
        csv_dir: CSV 디렉토리
        pg_table: 대상 PostgreSQL 테이블명
        parse_fn: CSV → DataFrame 변환 함수
        file_pattern: CSV 파일 패턴

    Returns:
        {"status": "DONE"|"SKIP"|"REJECT"|"FAIL", "rows": int, ...}
    """
    csv_files = sorted(csv_dir.glob(file_pattern))
    if not csv_files:
        logger.info(f"[CSV_LOAD_SKIP] {dataset} {trade_date}: no files")
        return {"status": "SKIP", "rows": 0, "reason": "no files"}

    with connection() as conn:
        # 이미 적재 확인
        prev = _get_last_load(conn, trade_date, dataset)
        if prev:
            logger.info(
                f"[CSV_LOAD_SKIP] {dataset} {trade_date}: already loaded"
            )
            return {"status": "SKIP", "rows": prev["row_count"], "reason": "already loaded"}

        # Quality gate
        ok, issues = csv_quality_gate(trade_date, dataset, csv_files, conn)
        if not ok:
            return {"status": "REJECT", "rows": 0, "issues": issues}

        # ingest_run_id 생성
        combined_hash = compute_file_hash(str(csv_files[0]))
        ingest_id = make_ingest_run_id(trade_date, dataset, combined_hash)
        run_ts = now_utc()

        policy = TABLE_POLICIES.get(pg_table, {"conflict": "DO_NOTHING", "stale_check": False})

        try:
            total_rows = 0
            for f in csv_files:
                df = parse_fn(f)
                if df.empty:
                    continue

                df["ingest_run_id"] = ingest_id
                df["run_ts"] = run_ts

                total_rows += _batch_upsert(conn, pg_table, df, policy)

            # 적재 기록
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO csv_load_log (trade_date, dataset, ingest_run_id, row_count) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (trade_date, dataset) DO UPDATE SET "
                "ingest_run_id = EXCLUDED.ingest_run_id, "
                "row_count = EXCLUDED.row_count, "
                "loaded_at = NOW()",
                (trade_date, dataset, ingest_id, total_rows),
            )
            cur.close()
            conn.commit()

            logger.info(
                f"[CSV_LOAD_DONE] {dataset} {trade_date}: "
                f"{total_rows} rows, id={ingest_id}"
            )
            return {"status": "DONE", "rows": total_rows, "ingest_run_id": ingest_id}

        except Exception as e:
            conn.rollback()
            logger.error(
                f"[CSV_LOAD_FAIL] {dataset} {trade_date}",
                exc_info=e,
            )
            return {"status": "FAIL", "rows": 0, "error": str(e)}


def _batch_upsert(conn, table: str, df: pd.DataFrame, policy: Dict) -> int:
    """
    DataFrame을 테이블에 batch upsert.
    트랜잭션 내에서 호출 (commit은 호출자가).
    """
    if df.empty:
        return 0

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    cols_str = ", ".join(cols)

    cur = conn.cursor()
    rows_affected = 0

    if policy["conflict"] == "DO_NOTHING":
        sql = (
            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )
    elif policy["conflict"] == "DO_UPDATE":
        update_cols = policy.get("update_cols", [])
        update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

        if policy.get("stale_check"):
            sql = (
                f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
                f"ON CONFLICT DO UPDATE SET {update_set} "
                f"WHERE {table}.run_ts < EXCLUDED.run_ts"
            )
        else:
            sql = (
                f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
                f"ON CONFLICT DO UPDATE SET {update_set}"
            )
    else:
        sql = (
            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )

    for row in df.itertuples(index=False, name=None):
        cur.execute(sql, row)
        rows_affected += cur.rowcount

    cur.close()
    return rows_affected
