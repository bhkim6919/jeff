# -*- coding: utf-8 -*-
"""
universe_builder.py — US Stock Universe
========================================
Operating: S&P 500 (기존)
Research:  Russell 1000 / Russell 3000 (Lab용, snapshot 고정)

유니버스는 snapshot CSV로 저장, scraping 즉시 사용 금지.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger("qtron.us.universe")

UNIVERSES_DIR = Path(__file__).resolve().parent / "universes"
UNIVERSES_DIR.mkdir(parents=True, exist_ok=True)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_R1000_URL = "https://en.wikipedia.org/wiki/Russell_1000_Index"
_R3000_WIKI = "https://en.wikipedia.org/wiki/Russell_3000_Index"


# ── S&P 500 (Operating) ────────────────────────────────

def get_sp500_tickers() -> List[str]:
    """Fetch S&P 500 member tickers from Wikipedia."""
    try:
        import requests as _req
        from io import StringIO
        headers = {"User-Agent": "Mozilla/5.0 (Q-TRON US 1.0)"}
        resp = _req.get(_SP500_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        df = tables[0]
        tickers = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        logger.info(f"[UNIVERSE] S&P 500: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        logger.error(f"[UNIVERSE] Failed to fetch S&P 500: {e}")
        return []


# ── Russell 1000 (Research) ─────────────────────────────

def get_russell1000_tickers() -> List[str]:
    """
    Fetch Russell 1000 constituents.
    Strategy: Wikipedia table → IWB ETF holdings fallback → S&P 500 + MidCap.
    """
    # Try Wikipedia first
    tickers = _fetch_wiki_russell1000()
    if len(tickers) >= 800:
        logger.info(f"[UNIVERSE] Russell 1000 (wiki): {len(tickers)} tickers")
        return tickers

    # Fallback: S&P 500 + some midcap (approximate R1000)
    logger.warning("[UNIVERSE] R1000 wiki failed, using S&P 500 as fallback")
    return get_sp500_tickers()


def _fetch_wiki_russell1000() -> List[str]:
    """Try fetching R1000 from Wikipedia."""
    try:
        import requests as _req
        from io import StringIO
        headers = {"User-Agent": "Mozilla/5.0 (Q-TRON US 1.0)"}
        resp = _req.get(_R1000_URL, headers=headers, timeout=15)
        resp.raise_for_status()
        tables = pd.read_html(StringIO(resp.text))
        # Find the largest table with Symbol/Ticker column
        for t in tables:
            if t.shape[0] < 500:
                continue
            cols = [str(c).lower() for c in t.columns]
            for col_name in ["symbol", "ticker", "ticker symbol"]:
                if col_name in cols:
                    idx = cols.index(col_name)
                    raw = t.iloc[:, idx].dropna().astype(str).tolist()
                    tickers = [s.strip().replace(".", "-") for s in raw
                              if isinstance(s, str) and 0 < len(s.strip()) <= 5 and s.strip().isalpha()]
                    if len(tickers) >= 500:
                        return tickers
    except Exception as e:
        logger.warning(f"[UNIVERSE] R1000 wiki error: {e}")
    return []


# ── Russell 3000 (Research, Phase C) ────────────────────

def get_russell3000_tickers() -> List[str]:
    """
    Russell 3000 = Russell 1000 + Russell 2000 (small cap).
    Full list not on Wikipedia → use yfinance screener or IWV ETF.
    For now: R1000 + additional small caps from Alpaca assets API.
    """
    r1000 = get_russell1000_tickers()

    # Try to expand with Alpaca tradable assets (US, active, equity)
    try:
        from data.alpaca_provider import AlpacaProvider
        from config import USConfig
        p = AlpacaProvider(USConfig())
        resp = p._get("/v2/assets?status=active&asset_class=us_equity")
        if resp:
            all_symbols = [a["symbol"] for a in resp
                          if a.get("tradable") and a.get("exchange") in ("NYSE", "NASDAQ", "ARCA", "BATS")]
            # Combine: R1000 + additional tradeable (capped at 3000)
            r1000_set = set(r1000)
            additional = [s for s in all_symbols if s not in r1000_set and len(s) <= 5]
            combined = r1000 + additional[:2000]
            logger.info(f"[UNIVERSE] Russell 3000 (approx): {len(combined)} ({len(r1000)} R1000 + {len(additional[:2000])} small)")
            return combined
    except Exception as e:
        logger.warning(f"[UNIVERSE] R3000 expansion failed: {e}")

    return r1000


# ── Snapshot Management ─────────────────────────────────

def _compute_hash(tickers: List[str]) -> str:
    """SHA256 of sorted ticker list."""
    h = hashlib.sha256()
    for t in sorted(tickers):
        h.update(t.encode())
    return h.hexdigest()[:16]


def save_universe_snapshot(tickers: List[str], name: str,
                           source_name: str = "wiki") -> Path:
    """
    Save universe as snapshot CSV with metadata.
    Returns path to saved file.
    """
    today = date.today().strftime("%Y%m%d")
    filename = f"{name}_snapshot_{today}.csv"
    filepath = UNIVERSES_DIR / filename

    df = pd.DataFrame({
        "ticker": sorted(tickers),
        "source_name": source_name,
        "fetched_at": pd.Timestamp.now().isoformat(),
        "effective_date": today,
    })
    df.to_csv(filepath, index=False)

    # Update meta
    meta_path = UNIVERSES_DIR / "universe_meta.json"
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text())
        except Exception:
            pass

    meta[name] = {
        "file": filename,
        "count": len(tickers),
        "universe_hash": _compute_hash(tickers),
        "fetched_at": pd.Timestamp.now().isoformat(),
    }
    meta_path.write_text(json.dumps(meta, indent=2))

    logger.info(f"[SNAPSHOT] Saved {name}: {len(tickers)} tickers → {filename}")
    return filepath


def load_universe_snapshot(name: str) -> List[str]:
    """Load tickers from latest snapshot for given universe name.

    Returns [] on any missing-data condition. All such conditions emit
    a [UNIVERSE_EMPTY] warning so callers and alerting see them — a
    silent empty list cost us a full R3000 strategy outage on 2026-04-21.
    """
    meta_path = UNIVERSES_DIR / "universe_meta.json"
    if not meta_path.exists():
        logger.warning(
            f"[UNIVERSE_EMPTY] {name}: no universe_meta.json at {meta_path}"
        )
        return []

    meta = json.loads(meta_path.read_text())
    entry = meta.get(name)
    if not entry:
        logger.warning(
            f"[UNIVERSE_EMPTY] {name}: not registered in universe_meta.json "
            f"(known={sorted(meta.keys())})"
        )
        return []

    filepath = UNIVERSES_DIR / entry["file"]
    if not filepath.exists():
        logger.warning(
            f"[UNIVERSE_EMPTY] {name}: snapshot file missing at {filepath}"
        )
        return []

    df = pd.read_csv(filepath)
    tickers = df["ticker"].tolist()
    if not tickers:
        logger.warning(
            f"[UNIVERSE_EMPTY] {name}: snapshot file {entry['file']} "
            f"has zero tickers"
        )
        return []
    logger.info(
        f"[SNAPSHOT] Loaded {name}: {len(tickers)} tickers "
        f"(hash={entry.get('universe_hash', '?')[:8]})"
    )
    return tickers


def get_universe_snapshot_id(name: str) -> str:
    """Get snapshot file + hash for job tracking."""
    meta_path = UNIVERSES_DIR / "universe_meta.json"
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        entry = meta.get(name, {})
        return f"{entry.get('file', '?')}:{entry.get('universe_hash', '?')[:8]}"
    return "unknown"


def build_universe(db, config) -> List[str]:
    """
    Build tradeable universe:
    1. S&P 500 members
    2. Filter: min close >= $5, avg amount >= $10M, min history >= 260d
    3. Cap at 300 (sorted by liquidity)
    """
    # Get S&P 500 tickers
    sp500 = get_sp500_tickers()
    if not sp500:
        logger.error("[UNIVERSE] No tickers available")
        return []

    # Load close data from DB
    close_dict = db.load_close_dict(min_history=config.UNIV_MIN_HISTORY)
    if not close_dict:
        logger.warning("[UNIVERSE] No OHLCV data in DB yet")
        return sp500[:config.UNIV_MAX_CANDIDATES]  # return raw list if no data

    # Filter
    candidates = []
    for sym in sp500:
        if sym not in close_dict:
            continue
        closes = close_dict[sym]
        if len(closes) < config.UNIV_MIN_HISTORY:
            continue

        last_close = closes.iloc[-1]
        if last_close < config.UNIV_MIN_CLOSE:
            continue

        # TODO: daily traded value filter (need volume data)
        candidates.append((sym, last_close, len(closes)))

    # Sort by history length (proxy for quality), cap
    candidates.sort(key=lambda x: -x[2])
    result = [c[0] for c in candidates[:config.UNIV_MAX_CANDIDATES]]

    logger.info(
        f"[UNIVERSE] {len(result)} stocks "
        f"(from {len(sp500)} S&P 500, {len(close_dict)} with data)"
    )
    return result
