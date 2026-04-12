# -*- coding: utf-8 -*-
"""
collector_global.py — 글로벌 시장 데이터 수집 (다중 소스)
============================================================
1순위: Yahoo Finance v8 JSON (직접 호출, rate limit 느슨)
2순위: yfinance 라이브러리 (fallback)
Date 기반 fetch. 10분 TTL 캐시. 실패 시 {ok: False}.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any, Dict

logger = logging.getLogger("gen4.regime.collector.global")

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL = 600   # 10분 (rate limit 방지)
_MAX_CACHE_SIZE = 30

# Yahoo Finance v8 JSON API (yfinance가 내부적으로 사용하는 것과 동일)
_YF_V8_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
_YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def _fetch_yahoo_v8(symbol: str, feature_date: date, name: str) -> Dict[str, Any]:
    """Yahoo v8 JSON API 직접 호출. yfinance 라이브러리 불필요."""
    read_ts = time.time()
    try:
        import requests

        # 10일 범위 조회 (feature_date 포함)
        start_dt = feature_date - timedelta(days=10)
        end_dt = feature_date + timedelta(days=2)
        period1 = int(time.mktime(start_dt.timetuple()))
        period2 = int(time.mktime(end_dt.timetuple()))

        url = f"{_YF_V8_BASE}/{symbol}"
        params = {
            "period1": period1,
            "period2": period2,
            "interval": "1d",
            "includePrePost": "false",
        }
        resp = requests.get(url, params=params, headers=_YF_HEADERS, timeout=10)

        if resp.status_code == 429:
            return _fail(f"Yahoo v8 rate limited (429)", f"yahoo_v8:{symbol}", read_ts)
        if resp.status_code != 200:
            return _fail(f"Yahoo v8 HTTP {resp.status_code}", f"yahoo_v8:{symbol}", read_ts)

        data = resp.json()
        chart = data.get("chart", {}).get("result", [])
        if not chart:
            return _fail(f"Yahoo v8 empty result", f"yahoo_v8:{symbol}", read_ts)

        result_data = chart[0]
        timestamps = result_data.get("timestamp", [])
        closes = result_data.get("indicators", {}).get("quote", [{}])[0].get("close", [])

        if not timestamps or not closes or len(closes) < 2:
            return _fail(f"Yahoo v8 insufficient data", f"yahoo_v8:{symbol}", read_ts)

        # Find feature_date or nearest prior date
        from datetime import datetime
        dates = [datetime.fromtimestamp(ts).date() for ts in timestamps]

        target_idx = None
        for i, d in enumerate(dates):
            if d <= feature_date and closes[i] is not None:
                target_idx = i

        if target_idx is None or target_idx < 1:
            return _fail(f"No matching date for {feature_date}", f"yahoo_v8:{symbol}", read_ts)

        # Find previous valid close
        prev_idx = None
        for i in range(target_idx - 1, -1, -1):
            if closes[i] is not None:
                prev_idx = i
                break

        if prev_idx is None:
            return _fail(f"No prev close found", f"yahoo_v8:{symbol}", read_ts)

        close_val = float(closes[target_idx])
        prev_val = float(closes[prev_idx])
        actual_date = dates[target_idx]
        change_pct = (close_val - prev_val) / prev_val if prev_val != 0 else 0

        date_diff = abs((actual_date - feature_date).days)

        return {
            "ok": True,
            "data": {
                "close": round(close_val, 4),
                "prev_close": round(prev_val, 4),
                "change_pct": round(change_pct, 6),
                "market_date": str(actual_date),
                "requested_date": str(feature_date),
                "date_diff_days": date_diff,
            },
            "source_ts": time.mktime(actual_date.timetuple()),
            "read_ts": read_ts,
            "stale": date_diff > 3,
            "expired": False,
            "from_cache": False,
            "error": None,
            "source": f"yahoo_v8:{symbol}",
        }

    except Exception as e:
        return _fail(str(e), f"yahoo_v8:{symbol}", read_ts)


def _fetch_yfinance(symbol: str, feature_date: date, name: str) -> Dict[str, Any]:
    """yfinance 라이브러리 (fallback)."""
    read_ts = time.time()
    try:
        import yfinance as yf
        start = feature_date - timedelta(days=10)
        end = feature_date + timedelta(days=2)
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))

        if hist.empty or len(hist) < 2:
            return _fail(f"yfinance insufficient data", f"yfinance:{symbol}", read_ts)

        hist.index = hist.index.tz_localize(None) if hist.index.tz else hist.index
        available_dates = [d.date() for d in hist.index]

        target_idx = None
        for i, d in enumerate(available_dates):
            if d <= feature_date:
                target_idx = i

        if target_idx is None or target_idx < 1:
            return _fail(f"No matching date", f"yfinance:{symbol}", read_ts)

        close_val = float(hist["Close"].iloc[target_idx])
        prev_val = float(hist["Close"].iloc[target_idx - 1])
        actual_date = available_dates[target_idx]
        change_pct = (close_val - prev_val) / prev_val if prev_val != 0 else 0
        date_diff = abs((actual_date - feature_date).days)

        return {
            "ok": True,
            "data": {
                "close": round(close_val, 4),
                "prev_close": round(prev_val, 4),
                "change_pct": round(change_pct, 6),
                "market_date": str(actual_date),
                "requested_date": str(feature_date),
                "date_diff_days": date_diff,
            },
            "source_ts": time.mktime(actual_date.timetuple()),
            "read_ts": read_ts,
            "stale": date_diff > 3,
            "expired": False,
            "from_cache": False,
            "error": None,
            "source": f"yfinance:{symbol}",
        }
    except Exception as e:
        return _fail(str(e), f"yfinance:{symbol}", read_ts)


def _fetch_with_fallback(symbol: str, feature_date: date, name: str) -> Dict[str, Any]:
    """1순위 Yahoo v8 → 2순위 yfinance. 캐시 적용."""
    cache_key = f"{symbol}_{feature_date}"
    now = time.time()

    # Cache hit
    cached = _CACHE.get(cache_key)
    if cached and (now - cached["read_ts"]) < _CACHE_TTL:
        return {**cached, "from_cache": True}

    # 1순위: Yahoo v8 JSON 직접 호출
    result = _fetch_yahoo_v8(symbol, feature_date, name)
    if result["ok"]:
        _cache_store(cache_key, result)
        return result

    v8_error = result.get("error", "")
    logger.info(f"[Global] {name} v8 failed ({v8_error}), trying yfinance...")

    # 2순위: yfinance 라이브러리
    result = _fetch_yfinance(symbol, feature_date, name)
    if result["ok"]:
        _cache_store(cache_key, result)
        return result

    yf_error = result.get("error", "")
    logger.warning(f"[Global] {name} all sources failed: v8={v8_error}, yf={yf_error}")

    return _fail(f"v8: {v8_error} | yf: {yf_error}", f"fallback:{symbol}", now)


def _cache_store(key: str, result: Dict) -> None:
    if len(_CACHE) >= _MAX_CACHE_SIZE:
        oldest = min(_CACHE, key=lambda k: _CACHE[k].get("read_ts", 0))
        del _CACHE[oldest]
    _CACHE[key] = result


def _fail(error: str, source: str, read_ts: float) -> Dict[str, Any]:
    return {
        "ok": False, "data": None, "source_ts": 0, "read_ts": read_ts,
        "stale": True, "expired": True, "from_cache": False,
        "error": error, "source": source,
    }


# ── Public API ────────────────────────────────────────────────

def fetch_sp500(feature_date: date) -> Dict[str, Any]:
    return _fetch_with_fallback("^GSPC", feature_date, "S&P500")


def fetch_nasdaq(feature_date: date) -> Dict[str, Any]:
    return _fetch_with_fallback("^IXIC", feature_date, "NASDAQ")


def fetch_vix(feature_date: date) -> Dict[str, Any]:
    return _fetch_with_fallback("^VIX", feature_date, "VIX")


def fetch_usdkrw(feature_date: date) -> Dict[str, Any]:
    return _fetch_with_fallback("KRW=X", feature_date, "USD/KRW")
