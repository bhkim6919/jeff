# -*- coding: utf-8 -*-
"""
storage.py — SQLite persistence for regime predictions
========================================================
WAL mode for multi-process safety. Connection per-call.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.regime.storage")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "regime" / "regime.db"
LATEST_PATH = Path(__file__).resolve().parent.parent / "data" / "regime" / "latest.json"


def _ensure_db() -> None:
    """Create tables if they don't exist."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feature_date TEXT NOT NULL,
            target_date TEXT NOT NULL,
            predicted_regime INTEGER NOT NULL,
            predicted_label TEXT NOT NULL,
            composite_score REAL NOT NULL,
            global_score REAL, global_avail INTEGER,
            vol_score REAL, vol_avail INTEGER,
            domestic_score REAL, domestic_avail INTEGER,
            micro_score REAL, micro_avail INTEGER,
            fx_score REAL, fx_avail INTEGER,
            available_weight REAL,
            confidence_flag TEXT,
            source_health TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS actuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_date TEXT NOT NULL UNIQUE,
            actual_regime INTEGER NOT NULL,
            actual_label TEXT NOT NULL,
            kospi_change REAL,
            actual_method TEXT DEFAULT 'kospi_change',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id INTEGER,
            actual_id INTEGER,
            predicted INTEGER NOT NULL,
            actual INTEGER NOT NULL,
            distance INTEGER NOT NULL,
            raw_confidence REAL NOT NULL,
            adjusted_confidence REAL NOT NULL,
            available_weight REAL,
            confidence_flag TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (prediction_id) REFERENCES predictions(id),
            FOREIGN KEY (actual_id) REFERENCES actuals(id)
        );
    """)
    conn.close()


def _conn() -> sqlite3.Connection:
    """Get connection with WAL mode and row factory."""
    _ensure_db()
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c


def save_prediction(record: Dict[str, Any]) -> int:
    """Insert prediction record. Returns row id."""
    conn = _conn()
    try:
        cur = conn.execute("""
            INSERT INTO predictions (
                feature_date, target_date, predicted_regime, predicted_label,
                composite_score, global_score, global_avail, vol_score, vol_avail,
                domestic_score, domestic_avail, micro_score, micro_avail,
                fx_score, fx_avail, available_weight, confidence_flag,
                source_health, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record["feature_date"], record["target_date"],
            record["predicted_regime"], record["predicted_label"],
            record["composite_score"],
            record.get("global_score"), record.get("global_avail", 0),
            record.get("vol_score"), record.get("vol_avail", 0),
            record.get("domestic_score"), record.get("domestic_avail", 0),
            record.get("micro_score"), record.get("micro_avail", 0),
            record.get("fx_score"), record.get("fx_avail", 0),
            record.get("available_weight", 0),
            record.get("confidence_flag", ""),
            json.dumps(record.get("source_health", {}), ensure_ascii=False),
            record.get("created_at", time.strftime("%Y-%m-%dT%H:%M:%S")),
        ))
        conn.commit()
        row_id = cur.lastrowid
        # Also save latest.json
        _save_latest(record)
        return row_id
    finally:
        conn.close()


def save_actual(record: Dict[str, Any]) -> int:
    """Insert or replace actual regime for a date. Stores full record as JSON."""
    conn = _conn()
    try:
        # Ensure extra_data column exists
        try:
            conn.execute("ALTER TABLE actuals ADD COLUMN extra_data TEXT")
        except Exception:
            pass  # column already exists

        cur = conn.execute("""
            INSERT OR REPLACE INTO actuals (
                market_date, actual_regime, actual_label, kospi_change,
                actual_method, extra_data, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            record["market_date"], record["actual_regime"], record["actual_label"],
            record.get("kospi_change"), record.get("actual_method", "jeff_v2"),
            json.dumps(record, ensure_ascii=False, default=str),
            time.strftime("%Y-%m-%dT%H:%M:%S"),
        ))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def save_score(record: Dict[str, Any]) -> int:
    """Insert score record."""
    conn = _conn()
    try:
        cur = conn.execute("""
            INSERT INTO scores (
                prediction_id, actual_id, predicted, actual, distance,
                raw_confidence, adjusted_confidence, available_weight,
                confidence_flag, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.get("prediction_id"), record.get("actual_id"),
            record["predicted"], record["actual"], record["distance"],
            record["raw_confidence"], record["adjusted_confidence"],
            record.get("available_weight", 0), record.get("confidence_flag", ""),
            time.strftime("%Y-%m-%dT%H:%M:%S"),
        ))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def load_latest_prediction(target_date: Optional[str] = None) -> Optional[Dict]:
    """Load most recent prediction (optionally for a specific target_date)."""
    conn = _conn()
    try:
        if target_date:
            row = conn.execute(
                "SELECT * FROM predictions WHERE target_date=? ORDER BY id DESC LIMIT 1",
                (target_date,)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM predictions ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def load_actual(market_date: str) -> Optional[Dict]:
    """Load actual regime for a date. Parses extra_data JSON if present."""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT * FROM actuals WHERE market_date=?", (market_date,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        # Parse extra_data JSON to get full record (scores, breadth, etc.)
        extra = result.pop("extra_data", None)
        if extra:
            try:
                full = json.loads(extra)
                result.update(full)
            except Exception:
                pass
        return result
    finally:
        conn.close()


def load_recent_scores(n: int = 20) -> List[Dict]:
    """Load most recent N score records."""
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM scores ORDER BY id DESC LIMIT ?", (n,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def load_history(days: int = 30) -> List[Dict]:
    """Load prediction + actual + score history for last N days."""
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT p.*, a.actual_regime, a.actual_label, a.kospi_change,
                   s.distance, s.raw_confidence, s.adjusted_confidence
            FROM predictions p
            LEFT JOIN actuals a ON p.target_date = a.market_date
            LEFT JOIN scores s ON s.prediction_id = p.id
            ORDER BY p.id DESC LIMIT ?
        """, (days,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _save_latest(record: Dict) -> None:
    """Save latest prediction to JSON for quick SSE reads."""
    try:
        LATEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        LATEST_PATH.write_text(
            json.dumps(record, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8"
        )
    except Exception as e:
        logger.warning(f"[Storage] Failed to save latest.json: {e}")


def load_latest_json() -> Optional[Dict]:
    """Read latest.json for SSE. Returns None on failure."""
    try:
        if LATEST_PATH.exists():
            return json.loads(LATEST_PATH.read_text("utf-8"))
    except Exception:
        pass
    return None
