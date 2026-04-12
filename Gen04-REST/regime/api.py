# -*- coding: utf-8 -*-
"""
api.py — FastAPI Router for Regime Prediction
===============================================
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date
from typing import Any

from fastapi import APIRouter, Query

from regime.calendar import (
    next_trading_day, previous_trading_day, is_after_market_close, is_calendar_stale,
)
from regime.models import INDEX_SOURCE, FEATURE_WEIGHTS, REGIME_LABELS, REGIME_COLORS
from regime.scorer import compute_rolling_stats
from regime.storage import load_latest_prediction, load_actual, load_history, load_latest_json

logger = logging.getLogger("gen4.regime.api")

router = APIRouter(prefix="/api/regime", tags=["regime"])

# Use app.py's shared global provider (avoid duplicate instances)
def _get_provider():
    try:
        from web.app import _get_global_provider
        return _get_global_provider()
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from data.rest_provider import KiwoomRestProvider
        return KiwoomRestProvider(server_type="REAL")


@router.get("/current")
async def regime_current():
    """Latest prediction + rolling accuracy stats + intraday actual + history."""
    latest = load_latest_json()
    stats = compute_rolling_stats(20)

    # Read from SSE cache (no TR call — shared with dashboard)
    actual = None
    history_points = []
    try:
        from web.app import _portfolio_cache, _regime_history, _regime_lock
        actual = _portfolio_cache.get("regime_actual")

        # Extract 10m / 30m / 1h history points (lock: P0-1)
        with _regime_lock:
            history_snapshot = list(_regime_history)

        now = time.time()
        for offset_min, ago_label in [(10, "10m"), (30, "30m"), (60, "1h")]:
            target = now - offset_min * 60
            if history_snapshot:
                closest = min(history_snapshot, key=lambda h: abs(h["ts"] - target))
                if abs(closest["ts"] - target) < 360:  # within 6 min
                    history_points.append({"ago": ago_label, **closest})
    except Exception:
        pass

    # Fallback: load from DB (post-market or first load)
    if not actual:
        try:
            actual = load_actual(date.today().isoformat())
        except Exception:
            pass

    resp = {
        "prediction": latest,
        "actual": actual,
        "history": history_points,
        "stats": stats,
        "colors": REGIME_COLORS,
    }
    if latest:
        resp["labels"] = REGIME_LABELS
    return resp


@router.get("/history")
async def regime_history(days: int = Query(30, ge=1, le=365)):
    """Historical prediction + actual + score records."""
    return load_history(days)


@router.get("/diagnostics")
async def regime_diagnostics():
    """Test all data sources + report time alignment."""
    today = date.today()
    target = next_trading_day(today)
    feature = previous_trading_day(target)

    sources = {}

    # Test global sources
    try:
        from regime.collector_global import fetch_sp500, fetch_nasdaq, fetch_vix, fetch_usdkrw
        for name, fn in [("sp500", fetch_sp500), ("nasdaq", fetch_nasdaq),
                         ("vix", fetch_vix), ("usdkrw", fetch_usdkrw)]:
            try:
                result = await asyncio.to_thread(fn, feature)
                sources[name] = {
                    "ok": result.get("ok", False),
                    "stale": result.get("stale", True),
                    "from_cache": result.get("from_cache", False),
                    "source": result.get("source", ""),
                    "error": result.get("error"),
                    "data_date": result.get("data", {}).get("market_date") if result.get("data") else None,
                }
            except Exception as e:
                sources[name] = {"ok": False, "error": str(e)}
    except ImportError as e:
        sources["global"] = {"ok": False, "error": f"import failed: {e}"}

    # Test domestic sources
    try:
        provider = _get_provider()
        from regime.collector_domestic import collect_kospi, collect_kosdaq, collect_trade_strength
        for name, fn in [("kospi", collect_kospi), ("kosdaq", collect_kosdaq),
                         ("strength", collect_trade_strength)]:
            try:
                result = await asyncio.to_thread(fn, provider, feature)
                raw_val = None
                if result.get("data"):
                    raw_val = result["data"].get("raw_cur_prc") or result["data"].get("close") or result["data"].get("strength")
                sources[name] = {
                    "ok": result.get("ok", False),
                    "stale": result.get("stale", True),
                    "source": result.get("source", ""),
                    "raw_value": raw_val,
                    "error": result.get("error"),
                }
            except Exception as e:
                sources[name] = {"ok": False, "error": str(e)}
    except Exception as e:
        sources["domestic"] = {"ok": False, "error": str(e)}

    # Time alignment
    available_count = sum(1 for s in sources.values() if s.get("ok", False))
    total_count = len(sources)

    return {
        "sources": sources,
        "time_alignment": {
            "now": time.strftime("%Y-%m-%dT%H:%M:%S+09:00"),
            "today": str(today),
            "feature_date": str(feature),
            "target_date": str(target),
            "is_after_close": is_after_market_close(),
            "calendar_stale": is_calendar_stale(),
        },
        "index_source": INDEX_SOURCE,
        "available_count": f"{available_count}/{total_count}",
        "feature_weights": FEATURE_WEIGHTS,
    }


@router.post("/predict")
async def regime_predict():
    """Force new prediction. Runs collectors in thread pool."""
    try:
        provider = _get_provider()
        from regime.predictor import predict_regime
        result = await asyncio.to_thread(predict_regime, provider)
        return result
    except Exception as e:
        logger.error(f"[API] Predict failed: {e}")
        return {"error": str(e)}


@router.post("/score")
async def regime_score():
    """Force actual calculation + scoring. Only after market close."""
    if not is_after_market_close():
        return {"error": "장 마감 전입니다 (15:30 이후 실행)", "is_after_close": False}

    try:
        provider = _get_provider()

        # 1. Calculate actual
        from regime.actual import calculate_actual
        actual_result = await asyncio.to_thread(calculate_actual, provider, True)

        if actual_result.get("unavailable"):
            return {"error": "actual 계산 실패", "detail": actual_result}

        # 2. Find matching prediction
        target_date = str(date.today())
        prediction = load_latest_prediction(target_date=target_date)

        if not prediction:
            return {
                "actual": actual_result,
                "score": None,
                "note": f"No prediction found for target_date={target_date}",
            }

        # 3. Score
        from regime.scorer import score_prediction
        global_avail = bool(prediction.get("global_avail", 0))
        score_result = score_prediction(
            predicted=prediction["predicted_regime"],
            actual=actual_result["actual_regime"],
            available_weight=prediction.get("available_weight", 1.0),
            global_available=global_avail,
            prediction_id=prediction.get("id"),
        )

        return {
            "actual": actual_result,
            "prediction_used": {
                "feature_date": prediction.get("feature_date"),
                "target_date": prediction.get("target_date"),
                "predicted_regime": prediction.get("predicted_regime"),
                "predicted_label": prediction.get("predicted_label"),
            },
            "score": score_result,
        }

    except Exception as e:
        logger.error(f"[API] Score failed: {e}")
        return {"error": str(e)}
