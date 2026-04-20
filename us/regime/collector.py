# -*- coding: utf-8 -*-
"""
collector.py — US Market Data Collector (Alpaca-first)
=======================================================
Alpaca Data API for SPY, QQQ, sector ETFs, UUP (DXY proxy).
Yahoo Finance fallback for VIX only (not on Alpaca).

Design: broker TR 최대 활용, 계산 최소화.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("qtron.us.regime.collector")

# ── Cache ───────────────────────────────────────────────
_cache: Dict[str, Tuple[dict, float]] = {}
CACHE_TTL = 300  # 5 minutes


def _get_cached(key: str) -> Optional[dict]:
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
    return None


def _set_cached(key: str, data: dict):
    _cache[key] = (data, time.time())


# ── Alpaca Snapshot (batch) ─────────────────────────────

def fetch_alpaca_snapshots(symbols: List[str], provider=None) -> Dict[str, dict]:
    """
    Fetch latest snapshots from Alpaca Data API.
    Returns {symbol: {price, prev_close, change_pct, volume}}.
    Uses provider._session for auth headers.
    """
    cache_key = f"snap_{'_'.join(sorted(symbols))}"
    cached = _get_cached(cache_key)
    if cached:
        return cached

    results = {}

    if provider:
        # Use Alpaca multi-snapshot endpoint
        try:
            url = f"{provider._data_url}/v2/stocks/snapshots"
            resp = provider._session.get(url, params={
                "symbols": ",".join(symbols),
                "feed": "iex",
            }, timeout=10)

            if resp.status_code == 200:
                data = resp.json()
                for sym, snap in data.items():
                    daily = snap.get("dailyBar", {})
                    prev = snap.get("prevDailyBar", {})
                    latest = snap.get("latestTrade", {})

                    cur_price = float(daily.get("c", 0)) or float(latest.get("p", 0))
                    prev_close = float(prev.get("c", 0))
                    volume = int(daily.get("v", 0))

                    change_pct = 0
                    if prev_close > 0 and cur_price > 0:
                        change_pct = (cur_price / prev_close - 1) * 100

                    results[sym] = {
                        "price": cur_price,
                        "open": float(daily.get("o", 0)),
                        "high": float(daily.get("h", 0)),
                        "low": float(daily.get("l", 0)),
                        "prev_close": prev_close,
                        "change_pct": change_pct,
                        "volume": volume,
                    }
                logger.info(f"[SNAP] Fetched {len(results)}/{len(symbols)} snapshots")
            else:
                logger.warning(f"[SNAP] HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"[SNAP] Error: {e}")

    # Fallback for missing symbols
    for sym in symbols:
        if sym not in results:
            results[sym] = _fetch_single_yahoo(sym)

    _set_cached(cache_key, results)
    return results


def _fetch_single_yahoo(symbol: str) -> dict:
    """Yahoo Finance fallback for a single symbol."""
    try:
        import requests
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        resp = requests.get(url, params={"range": "2d", "interval": "1d"}, timeout=10,
                           headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("chart", {}).get("result", [{}])[0]
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            if len(closes) >= 2 and closes[-1] and closes[-2]:
                cur = closes[-1]
                prev = closes[-2]
                return {
                    "price": cur,
                    "prev_close": prev,
                    "change_pct": (cur / prev - 1) * 100,
                    "volume": 0,
                }
    except Exception as e:
        logger.warning(f"[YAHOO] {symbol}: {e}")
    return {"price": 0, "prev_close": 0, "change_pct": 0, "volume": 0}


# ── VIX (Yahoo only — Alpaca doesn't have it) ──────────

def fetch_vix() -> dict:
    """Fetch VIX level and change from Yahoo Finance."""
    cached = _get_cached("vix")
    if cached:
        return cached

    result = {"level": 0, "change_pct": 0, "available": False}
    try:
        import requests
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX"
        resp = requests.get(url, params={"range": "2d", "interval": "1d"}, timeout=10,
                           headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            r = data.get("chart", {}).get("result", [{}])[0]
            closes = r.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            if closes and closes[-1]:
                result["level"] = closes[-1]
                result["available"] = True
                if len(closes) >= 2 and closes[-2]:
                    result["change_pct"] = (closes[-1] / closes[-2] - 1) * 100
    except Exception as e:
        logger.warning(f"[VIX] Fetch error: {e}")

    if result["available"]:
        _set_cached("vix", result)
    return result


# ── High-Level Collectors ──────────────────────────────

def collect_market_data(provider=None) -> dict:
    """
    Collect all market data needed for regime prediction.
    Returns structured dict with index, vix, sectors, fx data.
    """
    from .models import SECTOR_ETFS

    # Index + FX (Alpaca)
    index_symbols = ["SPY", "QQQ", "UUP"]
    sector_symbols = list(SECTOR_ETFS.keys())
    all_symbols = index_symbols + sector_symbols

    snapshots = fetch_alpaca_snapshots(all_symbols, provider)

    # VIX (Yahoo)
    vix = fetch_vix()

    # Structure results
    spy = snapshots.get("SPY", {})
    qqq = snapshots.get("QQQ", {})
    uup = snapshots.get("UUP", {})

    sectors = {}
    for sym, name in SECTOR_ETFS.items():
        s = snapshots.get(sym, {})
        sectors[sym] = {
            "name": name,
            "price": s.get("price", 0),
            "change_pct": s.get("change_pct", 0),
            "volume": s.get("volume", 0),
        }

    # Sector breadth: count advancing vs declining
    advancing = sum(1 for s in sectors.values() if s["change_pct"] > 0)
    declining = sum(1 for s in sectors.values() if s["change_pct"] < 0)
    total_sectors = len(sectors)
    breadth_ratio = advancing / total_sectors if total_sectors > 0 else 0.5

    return {
        "spy": {
            "price": spy.get("price", 0),
            "change_pct": spy.get("change_pct", 0),
            "available": spy.get("price", 0) > 0,
        },
        "qqq": {
            "price": qqq.get("price", 0),
            "change_pct": qqq.get("change_pct", 0),
            "available": qqq.get("price", 0) > 0,
        },
        "vix": vix,
        "fx": {
            "uup_change_pct": uup.get("change_pct", 0),
            "available": uup.get("price", 0) > 0,
        },
        "sectors": sectors,
        "breadth": {
            "advancing": advancing,
            "declining": declining,
            "total": total_sectors,
            "ratio": breadth_ratio,
        },
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
