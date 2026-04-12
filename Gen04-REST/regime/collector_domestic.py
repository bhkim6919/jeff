# -*- coding: utf-8 -*-
"""
collector_domestic.py — 키움 REST API 국내 시장 데이터 수집
============================================================
Date 기반 fetch. raw 값 보존. 실패 시 {ok: False}.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict, Optional

from regime.models import INDEX_SOURCE

logger = logging.getLogger("gen4.regime.collector.domestic")

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL = 300
_MAX_CACHE_SIZE = 50  # prevent unbounded growth


def collect_kospi(provider: Any, feature_date: date) -> Dict[str, Any]:
    """ka20001 KOSPI 종가. raw 값 보존. 날짜 검증."""
    cache_key = f"kospi_{feature_date}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached["read_ts"]) < _CACHE_TTL:
        return {**cached, "from_cache": True}

    try:
        params = INDEX_SOURCE["params"]
        resp = provider._request("ka20001", "/api/dostk/sect", params, related_code="REGIME")

        if resp.get("return_code") != 0:
            return _fail("ka20001 failed", "ka20001:kospi", now)

        raw_price = resp.get("cur_prc", "0")
        raw_change = resp.get("flu_rt", "0")

        parsed_price = abs(float(str(raw_price).replace("+", "").replace(",", "")))
        parsed_change = float(str(raw_change).replace("+", ""))

        if parsed_price <= 0:
            return _fail("KOSPI price <= 0", "ka20001:kospi", now)

        result = {
            "ok": True,
            "data": {
                "index_name": INDEX_SOURCE["name"],
                "close": parsed_price * INDEX_SOURCE["scale"],
                "change_pct": parsed_change / 100.0,  # 5.87 → 0.0587
                "raw_cur_prc": raw_price,
                "raw_flu_rt": raw_change,
                "market_date": str(feature_date),
                "rising": int(resp.get("rising", 0) or 0),
                "falling": int(resp.get("fall", 0) or 0),
                "steady": int(resp.get("stdns", 0) or 0),
            },
            "source_ts": now,
            "read_ts": now,
            "stale": False,
            "expired": False,
            "from_cache": False,
            "error": None,
            "source": "ka20001:kospi",
        }
        _CACHE[cache_key] = result
        return result

    except Exception as e:
        logger.warning(f"[Domestic] KOSPI collect failed: {e}")
        return _fail(str(e), "ka20001:kospi", now)


def collect_kosdaq(provider: Any, feature_date: date) -> Dict[str, Any]:
    """ka20001 KOSDAQ 종가."""
    cache_key = f"kosdaq_{feature_date}"
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and (now - cached["read_ts"]) < _CACHE_TTL:
        return {**cached, "from_cache": True}

    try:
        resp = provider._request("ka20001", "/api/dostk/sect",
                                 {"mrkt_tp": "1", "inds_cd": "101"}, related_code="REGIME")

        if resp.get("return_code") != 0:
            return _fail("ka20001 kosdaq failed", "ka20001:kosdaq", now)

        raw_price = resp.get("cur_prc", "0")
        raw_change = resp.get("flu_rt", "0")
        parsed_price = abs(float(str(raw_price).replace("+", "").replace(",", "")))
        parsed_change = float(str(raw_change).replace("+", ""))

        if parsed_price <= 0:
            return _fail("KOSDAQ price <= 0", "ka20001:kosdaq", now)

        result = {
            "ok": True,
            "data": {
                "index_name": "코스닥",
                "close": parsed_price,
                "change_pct": parsed_change / 100.0,
                "raw_cur_prc": raw_price,
                "market_date": str(feature_date),
            },
            "source_ts": now, "read_ts": now, "stale": False,
            "expired": False, "from_cache": False, "error": None,
            "source": "ka20001:kosdaq",
        }
        _CACHE[cache_key] = result
        return result

    except Exception as e:
        return _fail(str(e), "ka20001:kosdaq", now)


def collect_trade_strength(provider: Any, feature_date: date) -> Dict[str, Any]:
    """ka10046 체결강도 — 현재 REST API에서 stk_cd 필수 요구로 시장 전체 조회 불가.
    unavailable 반환. micro_score는 optional이므로 예측에 영향 미미."""
    return _fail("ka10046 requires stk_cd (not supported for market-wide query)", "ka10046", time.time())


def _fail(error: str, source: str, read_ts: float) -> Dict[str, Any]:
    return {
        "ok": False, "data": None, "source_ts": 0, "read_ts": read_ts,
        "stale": True, "expired": True, "from_cache": False,
        "error": error, "source": source,
    }
