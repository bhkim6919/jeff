# -*- coding: utf-8 -*-
"""
pg_base.py — PostgreSQL 단일 접근 계층
=======================================
Q-TRON 전체에서 PostgreSQL 접속은 반드시 이 모듈을 통해서만 수행한다.

[강제 규칙]
- psycopg2.connect() 직접 호출 금지
- 개별 모듈에서 retry 로직 구현 금지 (여기서 일괄 처리)
- PostgreSQL 실패 시 SQLite/파일 fallback 금지
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("qtron.db")

# ── ENV 로딩 ─────────────────────────────────────────────────

_env_loaded = False


def _load_env():
    """kr/.env 또는 us/.env에서 환경변수 로드 (최초 1회)."""
    global _env_loaded
    if _env_loaded:
        return
    try:
        from dotenv import load_dotenv
        # 프로젝트 루트 기준 탐색
        for env_path in [
            Path(__file__).resolve().parent.parent.parent / "kr" / ".env",
            Path(__file__).resolve().parent.parent.parent / "us" / ".env",
            Path(__file__).resolve().parent.parent.parent / ".env",
        ]:
            if env_path.exists():
                load_dotenv(env_path)
                break
    except ImportError:
        pass
    _env_loaded = True


def _require_env(key: str, default: Optional[str] = None) -> str:
    """필수 환경변수 조회. 없으면 RuntimeError."""
    _load_env()
    v = os.getenv(key, default)
    if v is None or v == "":
        raise RuntimeError(
            f"[DB_CONFIG_MISSING] env var '{key}' not set. "
            f"Set in kr/.env or us/.env (INT-P0-001)."
        )
    return v


def get_db_config() -> Dict[str, Any]:
    """PostgreSQL 연결 설정 dict 반환."""
    _load_env()
    return {
        "dbname": os.getenv("DB_NAME", "qtron"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": _require_env("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
    }


# ── Connection Manager ───────────────────────────────────────

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 0.5  # seconds


@contextmanager
def connection(config: Optional[Dict] = None, autocommit: bool = False):
    """
    PostgreSQL connection context manager with retry.

    Usage:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.commit()

    - 예외 발생 시 자동 rollback
    - 연결 실패 시 3회 retry (0.5s, 1.0s, 1.5s backoff)
    - 최종 실패 시 raise (fallback 없음)
    """
    import psycopg2

    cfg = config or get_db_config()
    conn = None
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            conn = psycopg2.connect(**cfg)
            if autocommit:
                conn.autocommit = True
            break
        except psycopg2.OperationalError as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE * (attempt + 1)
                logger.warning(
                    f"[PG_RETRY] attempt {attempt + 1}/{MAX_RETRIES}, "
                    f"wait {wait:.1f}s: {e}"
                )
                time.sleep(wait)
            else:
                logger.error(
                    f"[PG_FAIL] max retries ({MAX_RETRIES}) exceeded",
                    exc_info=e,
                )
                raise

    try:
        yield conn
    except Exception:
        if conn and not conn.closed:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            conn.close()


def get_conn(config: Optional[Dict] = None):
    """
    단순 connection 반환 (레거시 호환).
    신규 코드는 connection() context manager 사용 권장.
    """
    import psycopg2

    cfg = config or get_db_config()
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            return psycopg2.connect(**cfg)
        except psycopg2.OperationalError as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE * (attempt + 1)
                logger.warning(
                    f"[PG_RETRY] attempt {attempt + 1}/{MAX_RETRIES}, "
                    f"wait {wait:.1f}s: {e}"
                )
                time.sleep(wait)
    logger.error(f"[PG_FAIL] max retries exceeded", exc_info=last_error)
    raise last_error


# ── Health Check ──────────────────────────────────────────────

def health_check(config: Optional[Dict] = None) -> Dict[str, Any]:
    """
    PostgreSQL 연결 + 테이블 상태 확인.

    Returns:
        {
            "status": "OK" | "ERROR",
            "latency_ms": float,
            "tables": [{"name": str, "rows": int}],
            "error": str (optional)
        }
    """
    import time as _time

    start = _time.monotonic()
    try:
        with connection(config) as conn:
            latency = (_time.monotonic() - start) * 1000
            cur = conn.cursor()

            # 모든 사용자 테이블 row count
            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            tables = []
            for (tbl,) in cur.fetchall():
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")  # noqa: S608
                (cnt,) = cur.fetchone()
                tables.append({"name": tbl, "rows": cnt})
            cur.close()

            return {
                "status": "OK",
                "latency_ms": round(latency, 1),
                "tables": tables,
            }
    except Exception as e:
        return {
            "status": "ERROR",
            "latency_ms": -1,
            "tables": [],
            "error": str(e),
        }
