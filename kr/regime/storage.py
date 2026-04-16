# -*- coding: utf-8 -*-
"""
storage.py — PostgreSQL persistence for regime predictions
============================================================
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("gen4.regime.storage")

LATEST_PATH = Path(__file__).resolve().parent.parent / "data" / "regime" / "latest.json"


def save_prediction(record: Dict[str, Any]) -> int:
    """Insert prediction record. Returns row id."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO regime_predictions (
                feature_date, target_date, model_name,
                predicted_regime, predicted_label, composite_score,
                global_score, global_avail, vol_score, vol_avail,
                domestic_score, domestic_avail, micro_score, micro_avail,
                fx_score, fx_avail, available_weight, confidence_flag,
                source_health, run_ts, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (target_date, model_name) DO UPDATE SET
                feature_date = EXCLUDED.feature_date,
                predicted_regime = EXCLUDED.predicted_regime,
                predicted_label = EXCLUDED.predicted_label,
                composite_score = EXCLUDED.composite_score,
                global_score = EXCLUDED.global_score,
                global_avail = EXCLUDED.global_avail,
                vol_score = EXCLUDED.vol_score,
                vol_avail = EXCLUDED.vol_avail,
                domestic_score = EXCLUDED.domestic_score,
                domestic_avail = EXCLUDED.domestic_avail,
                micro_score = EXCLUDED.micro_score,
                micro_avail = EXCLUDED.micro_avail,
                fx_score = EXCLUDED.fx_score,
                fx_avail = EXCLUDED.fx_avail,
                available_weight = EXCLUDED.available_weight,
                confidence_flag = EXCLUDED.confidence_flag,
                source_health = EXCLUDED.source_health,
                run_ts = EXCLUDED.run_ts,
                created_at = EXCLUDED.created_at
            RETURNING id
        """, (
            record["feature_date"], record["target_date"],
            record.get("model_name", "composite"),
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
            now_utc(),
            record.get("created_at", time.strftime("%Y-%m-%dT%H:%M:%S")),
        ))
        row_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

    _save_latest(record)
    return row_id


def save_actual(record: Dict[str, Any]) -> int:
    """Insert or update actual regime for a date."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO regime_actuals (
                market_date, actual_regime, actual_label,
                kospi_change, actual_method, extra_data,
                run_ts, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (market_date) DO NOTHING
            RETURNING id
        """, (
            record["market_date"], record["actual_regime"], record["actual_label"],
            record.get("kospi_change"), record.get("actual_method", "jeff_v2"),
            json.dumps(record, ensure_ascii=False, default=str),
            now_utc(),
            time.strftime("%Y-%m-%dT%H:%M:%S"),
        ))
        result = cur.fetchone()
        row_id = result[0] if result else 0
        conn.commit()
        cur.close()
    return row_id


def save_score(record: Dict[str, Any]) -> int:
    """Insert score record."""
    with connection() as conn:
        cur = conn.cursor()
        target_date = record.get("target_date", "")
        cur.execute("""
            INSERT INTO regime_scores (
                target_date, model_name,
                prediction_id, actual_id,
                predicted, actual, distance,
                raw_confidence, adjusted_confidence,
                available_weight, confidence_flag,
                run_ts, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (target_date, model_name) DO UPDATE SET
                predicted = EXCLUDED.predicted,
                actual = EXCLUDED.actual,
                distance = EXCLUDED.distance,
                raw_confidence = EXCLUDED.raw_confidence,
                adjusted_confidence = EXCLUDED.adjusted_confidence,
                run_ts = EXCLUDED.run_ts
            RETURNING id
        """, (
            target_date, record.get("model_name", "composite"),
            record.get("prediction_id"), record.get("actual_id"),
            record["predicted"], record["actual"], record["distance"],
            record["raw_confidence"], record["adjusted_confidence"],
            record.get("available_weight", 0), record.get("confidence_flag", ""),
            now_utc(),
            time.strftime("%Y-%m-%dT%H:%M:%S"),
        ))
        row_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    return row_id


def load_latest_prediction(target_date: Optional[str] = None) -> Optional[Dict]:
    """Load most recent prediction."""
    with connection() as conn:
        cur = conn.cursor()
        if target_date:
            cur.execute(
                "SELECT * FROM regime_predictions WHERE target_date=%s "
                "ORDER BY id DESC LIMIT 1",
                (target_date,),
            )
        else:
            cur.execute(
                "SELECT * FROM regime_predictions ORDER BY id DESC LIMIT 1"
            )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        cur.close()
    return dict(zip(cols, row))


def load_actual(market_date: str) -> Optional[Dict]:
    """Load actual regime for a date. Parses extra_data JSON if present."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM regime_actuals WHERE market_date=%s",
            (market_date,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None
        cols = [d[0] for d in cur.description]
        result = dict(zip(cols, row))
        cur.close()

    extra = result.pop("extra_data", None)
    if extra:
        try:
            full = json.loads(extra)
            result.update(full)
        except Exception:
            pass
    return result


def load_recent_scores(n: int = 20) -> List[Dict]:
    """Load most recent N score records."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM regime_scores ORDER BY id DESC LIMIT %s", (n,)
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


def load_history(days: int = 30) -> List[Dict]:
    """Load prediction + actual + score history."""
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.*, a.actual_regime, a.actual_label, a.kospi_change,
                   s.distance, s.raw_confidence, s.adjusted_confidence
            FROM regime_predictions p
            LEFT JOIN regime_actuals a ON p.target_date = a.market_date
            LEFT JOIN regime_scores s ON s.target_date = p.target_date
            ORDER BY p.id DESC LIMIT %s
        """, (days,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        cur.close()
    return [dict(zip(cols, r)) for r in rows]


def _save_latest(record: Dict) -> None:
    """Save latest prediction to JSON for quick SSE reads."""
    try:
        LATEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        LATEST_PATH.write_text(
            json.dumps(record, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
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
