# -*- coding: utf-8 -*-
"""
cross_check.py — Deterministic Cross-Check
============================================
PG 기준으로 SQLite 비교 (방향 고정).
sample hash는 deterministic: PK 정렬 + normalized 직렬화.

[규칙]
- random sample 금지
- PK 기준 상위 N + 하위 N (boundary rows)
- timestamp → ISO UTC, float → round(8), None → 'NULL'
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("qtron.cross_check")

BOUNDARY_SIZE = 50  # 상위 50 + 하위 50 = 100행


@dataclass
class CrossCheckResult:
    ok: bool
    reason: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)


# ── Normalization ────────────────────────────────────────────

def _normalize_value(v: Any) -> str:
    """값을 결정론적 문자열로 변환."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return f"{round(v, 8):.8f}"
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def hash_rows(rows: List[Dict[str, Any]], col_order: Sequence[str]) -> str:
    """
    행 목록의 결정론적 SHA-256 해시.

    Args:
        rows: dict 리스트 (각 행)
        col_order: 컬럼 순서 (고정, 알파벳 또는 스키마 정의순)
    """
    lines = []
    for row in rows:
        parts = [_normalize_value(row.get(c)) for c in col_order]
        lines.append("|".join(parts))
    content = "\n".join(lines)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


# ── Query Helpers ────────────────────────────────────────────

def _query_meta(conn, table: str, date_col: str = "trade_date") -> Dict[str, Any]:
    """테이블 메타 정보: count, min_date, max_date."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {table}"  # noqa: S608
    )
    row = cur.fetchone()
    cur.close()
    return {
        "count": row[0],
        "min_date": str(row[1]) if row[1] else None,
        "max_date": str(row[2]) if row[2] else None,
    }


def _query_boundary_rows(
    conn,
    table: str,
    pk_cols: Sequence[str],
    check_cols: Sequence[str],
    n: int = BOUNDARY_SIZE,
    is_pg: bool = True,
) -> List[Dict[str, Any]]:
    """
    PK 기준 상위 N + 하위 N 행 조회 (deterministic).
    """
    all_cols = list(pk_cols) + [c for c in check_cols if c not in pk_cols]
    cols_str = ", ".join(all_cols)
    order_asc = ", ".join(f"{c} ASC" for c in pk_cols)
    order_desc = ", ".join(f"{c} DESC" for c in pk_cols)

    cur = conn.cursor()

    # 상위 N
    cur.execute(
        f"SELECT {cols_str} FROM {table} ORDER BY {order_asc} LIMIT {n}"  # noqa: S608
    )
    top_rows = cur.fetchall()

    # 하위 N
    cur.execute(
        f"SELECT {cols_str} FROM {table} ORDER BY {order_desc} LIMIT {n}"  # noqa: S608
    )
    bottom_rows = cur.fetchall()

    cur.close()

    # dict 변환
    def to_dict(row):
        return {col: row[i] for i, col in enumerate(all_cols)}

    combined = [to_dict(r) for r in top_rows]
    combined.extend(to_dict(r) for r in reversed(bottom_rows))

    # 중복 제거 (테이블이 100행 이하면 top/bottom 겹침)
    seen = set()
    unique = []
    for row in combined:
        pk_key = tuple(str(row[c]) for c in pk_cols)
        if pk_key not in seen:
            seen.add(pk_key)
            unique.append(row)

    return unique


# ── Main Check ───────────────────────────────────────────────

def deterministic_cross_check(
    pg_conn,
    sqlite_conn,
    table: str,
    pk_cols: Sequence[str],
    check_cols: Sequence[str],
    date_col: str = "trade_date",
    today_only: bool = False,
    today_value: Optional[str] = None,
) -> CrossCheckResult:
    """
    PG 기준으로 SQLite 비교.

    Args:
        pg_conn: PostgreSQL connection
        sqlite_conn: SQLite connection
        table: 테이블명 (양쪽 동일 가정)
        pk_cols: PK 컬럼 리스트
        check_cols: 비교 대상 컬럼 리스트
        date_col: 날짜 컬럼 (meta 비교용)
        today_only: True이면 당일분만 비교 (대량 테이블 최적화)
        today_value: 당일 날짜 문자열 (today_only=True 시 필수)
    """
    try:
        # 1차: 메타 비교
        pg_meta = _query_meta(pg_conn, table, date_col)
        sq_meta = _query_meta(sqlite_conn, table, date_col)

        if pg_meta["count"] != sq_meta["count"]:
            return CrossCheckResult(
                ok=False,
                reason="META_COUNT_MISMATCH",
                detail={"pg": pg_meta, "sqlite": sq_meta},
            )
        if pg_meta["min_date"] != sq_meta["min_date"]:
            return CrossCheckResult(
                ok=False,
                reason="META_MIN_DATE_MISMATCH",
                detail={"pg": pg_meta, "sqlite": sq_meta},
            )
        if pg_meta["max_date"] != sq_meta["max_date"]:
            return CrossCheckResult(
                ok=False,
                reason="META_MAX_DATE_MISMATCH",
                detail={"pg": pg_meta, "sqlite": sq_meta},
            )

        if today_only:
            # 대량 테이블: 당일분만 hash 비교
            return _check_today_rows(
                pg_conn, sqlite_conn, table, pk_cols, check_cols,
                date_col, today_value,
            )

        # 2차: boundary rows hash 비교
        col_order = sorted(set(list(pk_cols) + list(check_cols)))

        pg_rows = _query_boundary_rows(
            pg_conn, table, pk_cols, check_cols, BOUNDARY_SIZE
        )
        sq_rows = _query_boundary_rows(
            sqlite_conn, table, pk_cols, check_cols, BOUNDARY_SIZE
        )

        pg_hash = hash_rows(pg_rows, col_order)
        sq_hash = hash_rows(sq_rows, col_order)

        if pg_hash != sq_hash:
            return CrossCheckResult(
                ok=False,
                reason="SAMPLE_HASH_MISMATCH",
                detail={"pg_hash": pg_hash, "sqlite_hash": sq_hash},
            )

        return CrossCheckResult(ok=True)

    except Exception as e:
        return CrossCheckResult(
            ok=False,
            reason="CHECK_ERROR",
            detail={"error": str(e)},
        )


def _check_today_rows(
    pg_conn, sqlite_conn, table, pk_cols, check_cols,
    date_col, today_value,
) -> CrossCheckResult:
    """당일분만 hash 비교 (대량 테이블 최적화)."""
    all_cols = sorted(set(list(pk_cols) + list(check_cols)))
    cols_str = ", ".join(all_cols)
    order_str = ", ".join(f"{c} ASC" for c in pk_cols)

    def fetch_today(conn):
        cur = conn.cursor()
        cur.execute(
            f"SELECT {cols_str} FROM {table} "  # noqa: S608
            f"WHERE {date_col} = %s ORDER BY {order_str}",
            (today_value,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {col: row[i] for i, col in enumerate(all_cols)}
            for row in rows
        ]

    pg_rows = fetch_today(pg_conn)
    sq_rows = fetch_today(sqlite_conn)

    if len(pg_rows) != len(sq_rows):
        return CrossCheckResult(
            ok=False,
            reason="TODAY_COUNT_MISMATCH",
            detail={"pg_count": len(pg_rows), "sqlite_count": len(sq_rows)},
        )

    pg_hash = hash_rows(pg_rows, all_cols)
    sq_hash = hash_rows(sq_rows, all_cols)

    if pg_hash != sq_hash:
        return CrossCheckResult(
            ok=False,
            reason="TODAY_HASH_MISMATCH",
            detail={"pg_hash": pg_hash, "sqlite_hash": sq_hash},
        )

    return CrossCheckResult(ok=True)
