"""
meta_db.py -- Gen5 Meta Layer Phase 0: DB schema + helpers
============================================================
Observer-only. 시장 컨텍스트 + 전략 성과를 구조화 저장.
추천/비중조절 구현 금지 — 수집만.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("lab.meta")

DB_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "data" / "lab_live" / "meta_strategy.db"
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS market_context (
    trade_date         TEXT PRIMARY KEY,
    kospi_return       REAL,
    adv_ratio          REAL,
    small_vs_large     REAL,
    sector_dispersion  REAL,
    breakout_ratio     REAL,
    regime_score       REAL,
    regime_label       TEXT,
    data_snapshot_id   TEXT,
    created_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_daily (
    trade_date         TEXT NOT NULL,
    strategy           TEXT NOT NULL,
    strategy_version   TEXT NOT NULL,
    daily_return       REAL,
    cumul_return       REAL,
    position_count     INTEGER,
    win_count          INTEGER,
    loss_count         INTEGER,
    turnover           REAL,
    cash_ratio         REAL,
    gross_exposure     REAL,
    created_at         TEXT NOT NULL,
    PRIMARY KEY (trade_date, strategy)
);
CREATE INDEX IF NOT EXISTS idx_sd_date ON strategy_daily(trade_date);

CREATE TABLE IF NOT EXISTS strategy_exposure_daily (
    trade_date           TEXT NOT NULL,
    strategy             TEXT NOT NULL,
    avg_market_cap       REAL,
    top1_weight          REAL,
    top5_weight          REAL,
    sector_top1          TEXT,
    sector_top1_weight   REAL,
    sector_dispersion    REAL,
    created_at           TEXT NOT NULL,
    PRIMARY KEY (trade_date, strategy)
);
CREATE INDEX IF NOT EXISTS idx_sed_date ON strategy_exposure_daily(trade_date);
"""


def _ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.close()


def _conn() -> sqlite3.Connection:
    _ensure_db()
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── Save ──────────────────────────────────────────────────────

def save_market_context(data: dict) -> None:
    """INSERT OR REPLACE market_context row."""
    conn = _conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO market_context
            (trade_date, kospi_return, adv_ratio, small_vs_large,
             sector_dispersion, breakout_ratio, regime_score, regime_label,
             data_snapshot_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["trade_date"],
                data.get("kospi_return"),
                data.get("adv_ratio"),
                data.get("small_vs_large"),
                data.get("sector_dispersion"),
                data.get("breakout_ratio"),
                data.get("regime_score"),
                data.get("regime_label"),
                data.get("data_snapshot_id"),
                _now_iso(),
            ),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"[META_DB] save_market_context failed: {e}")
        conn.rollback()
    finally:
        conn.close()


def save_strategy_daily(rows: List[dict]) -> None:
    """INSERT OR REPLACE strategy_daily rows (9 rows per day)."""
    conn = _conn()
    try:
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO strategy_daily
                (trade_date, strategy, strategy_version, daily_return,
                 cumul_return, position_count, win_count, loss_count,
                 turnover, cash_ratio, gross_exposure, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["trade_date"], r["strategy"], r["strategy_version"],
                    r.get("daily_return"), r.get("cumul_return"),
                    r.get("position_count"), r.get("win_count"),
                    r.get("loss_count"), r.get("turnover"),
                    r.get("cash_ratio"), r.get("gross_exposure"),
                    _now_iso(),
                ),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"[META_DB] save_strategy_daily failed: {e}")
        conn.rollback()
    finally:
        conn.close()


def save_strategy_exposure(rows: List[dict]) -> None:
    """INSERT OR REPLACE strategy_exposure_daily rows (9 rows per day)."""
    conn = _conn()
    try:
        for r in rows:
            conn.execute(
                """INSERT OR REPLACE INTO strategy_exposure_daily
                (trade_date, strategy, avg_market_cap, top1_weight,
                 top5_weight, sector_top1, sector_top1_weight,
                 sector_dispersion, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    r["trade_date"], r["strategy"],
                    r.get("avg_market_cap"), r.get("top1_weight"),
                    r.get("top5_weight"), r.get("sector_top1"),
                    r.get("sector_top1_weight"), r.get("sector_dispersion"),
                    _now_iso(),
                ),
            )
        conn.commit()
    except Exception as e:
        logger.error(f"[META_DB] save_strategy_exposure failed: {e}")
        conn.rollback()
    finally:
        conn.close()


# ── Query ─────────────────────────────────────────────────────

def get_market_context(trade_date: str) -> Optional[dict]:
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM market_context WHERE trade_date = ?", (trade_date,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_strategy_daily(trade_date: str) -> List[dict]:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM strategy_daily WHERE trade_date = ? ORDER BY strategy",
            (trade_date,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Verification ──────────────────────────────────────────────

def verify_row_counts(trade_date: str, expected_strategies: int = 9) -> dict:
    """Verify all expected rows exist for a date."""
    conn = _conn()
    try:
        mc = conn.execute(
            "SELECT COUNT(*) FROM market_context WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()[0]
        sd = conn.execute(
            "SELECT COUNT(*) FROM strategy_daily WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()[0]
        se = conn.execute(
            "SELECT COUNT(*) FROM strategy_exposure_daily WHERE trade_date = ?",
            (trade_date,),
        ).fetchone()[0]

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
    finally:
        conn.close()
