# -*- coding: utf-8 -*-
"""
db_provider.py — PostgreSQL data provider for Q-TRON US
========================================================
US 전용 테이블 (ohlcv_us, sector_map_us, ...) 사용.
KR DB (ohlcv, sector_map, ...) 와 완전 분리.
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from shared.db.pg_base import connection as pg_connection, get_conn as _pg_get_conn, get_db_config

logger = logging.getLogger("qtron.us.db")

# ── Connection (pg_base 경유) ────────────────────────────────
# INT-P0-001: credentials must come from environment (.env). No hardcoded fallback.

_DB_CONFIG = get_db_config()
_pool = None


def get_conn():
    """Get a PostgreSQL connection (pg_base 경유, retry 내장)."""
    return _pg_get_conn(_DB_CONFIG)


def get_db() -> "DbProviderUS":
    """Singleton DbProviderUS."""
    global _pool
    if _pool is None:
        _pool = DbProviderUS()
    return _pool


class DbProviderUS:
    """PostgreSQL-based data provider for US market. Separate tables from KR."""

    def __init__(self, config: dict = None):
        self._config = config or _DB_CONFIG

    def _conn(self):
        return _pg_get_conn(self._config)

    # ── OHLCV ────────────────────────────────────────────────

    def get_ohlcv(self, symbol: str, start: str = "", end: str = "") -> pd.DataFrame:
        """Single stock OHLCV DataFrame."""
        conn = self._conn()
        query = "SELECT date, open, high, low, close, volume FROM ohlcv_us WHERE symbol = %s"
        params: list = [symbol]
        if start:
            query += " AND date >= %s"
            params.append(start)
        if end:
            query += " AND date <= %s"
            params.append(end)
        query += " ORDER BY date"
        df = pd.read_sql(query, conn, params=params, parse_dates=["date"])
        conn.close()
        return df

    def load_ohlcv_dict(self, min_history: int = 60,
                        symbols: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Load {symbol: DataFrame} for all stocks with enough history."""
        conn = self._conn()
        cur = conn.cursor()

        if symbols:
            placeholders = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"SELECT symbol, COUNT(*) as cnt FROM ohlcv_us "
                f"WHERE symbol IN ({placeholders}) GROUP BY symbol HAVING COUNT(*) >= %s",
                symbols + [min_history]
            )
        else:
            cur.execute(
                "SELECT symbol, COUNT(*) as cnt FROM ohlcv_us "
                "GROUP BY symbol HAVING COUNT(*) >= %s",
                (min_history,)
            )
        valid = [r[0] for r in cur.fetchall()]
        cur.close()

        if not valid:
            conn.close()
            return {}

        placeholders = ",".join(["%s"] * len(valid))
        query = (f"SELECT symbol, date, open, high, low, close, volume "
                 f"FROM ohlcv_us WHERE symbol IN ({placeholders}) ORDER BY symbol, date")
        df = pd.read_sql(query, conn, params=valid, parse_dates=["date"])
        conn.close()

        result = {}
        for sym, group in df.groupby("symbol"):
            result[sym] = group.drop(columns=["symbol"]).reset_index(drop=True)

        logger.info(f"[DB_US] Loaded {len(result)} stocks ({len(df):,} rows)")
        return result

    def load_close_dict(self, min_history: int = 252) -> Dict[str, pd.Series]:
        """close_dict for scoring. {symbol: Series(index=date, values=close)}."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT symbol FROM ohlcv_us GROUP BY symbol HAVING COUNT(*) >= %s",
            (min_history,)
        )
        valid = [r[0] for r in cur.fetchall()]
        cur.close()

        if not valid:
            conn.close()
            return {}

        placeholders = ",".join(["%s"] * len(valid))
        query = (f"SELECT symbol, date, close FROM ohlcv_us "
                 f"WHERE symbol IN ({placeholders}) ORDER BY symbol, date")
        df = pd.read_sql(query, conn, params=valid, parse_dates=["date"])
        conn.close()

        result = {}
        for sym, group in df.groupby("symbol"):
            s = group.set_index("date")["close"]
            s = pd.to_numeric(s, errors="coerce").fillna(0)
            result[sym] = s

        return result

    # ── Sector Map ───────────────────────────────────────────

    def get_sector_map(self) -> Dict:
        """sector_map dict."""
        conn = self._conn()
        df = pd.read_sql("SELECT * FROM sector_map_us", conn)
        conn.close()
        result = {}
        for _, r in df.iterrows():
            result[r["symbol"]] = {
                "name": r["name"],
                "sector": r["sector"],
                "exchange": r.get("exchange", ""),
            }
        return result

    # ── Index (SPY) ──────────────────────────────────────────

    def get_index(self, symbol: str = "SPY") -> pd.DataFrame:
        """Index DataFrame (SPY/QQQ/IWM)."""
        conn = self._conn()
        df = pd.read_sql(
            "SELECT date, open, high, low, close, volume "
            "FROM index_us WHERE symbol = %s ORDER BY date",
            conn, params=[symbol], parse_dates=["date"])
        conn.close()
        return df

    # ── Target Portfolio ─────────────────────────────────────

    def get_target_portfolio(self, target_date: str = "") -> Optional[Dict]:
        """Latest target portfolio."""
        conn = self._conn()
        if target_date:
            df = pd.read_sql(
                "SELECT * FROM target_portfolio_us WHERE date = %s ORDER BY rank",
                conn, params=[target_date])
        else:
            df = pd.read_sql(
                "SELECT * FROM target_portfolio_us WHERE date = "
                "(SELECT MAX(date) FROM target_portfolio_us) ORDER BY rank", conn)
        conn.close()

        if df.empty:
            return None
        return {
            "date": str(df["date"].iloc[0]),
            "snapshot_id": df["snapshot_id"].iloc[0] if "snapshot_id" in df.columns else "",
            "target_tickers": df["symbol"].tolist(),
            "scores": {
                r["symbol"]: {"vol_12m": r["vol_12m"], "mom_12_1": r["mom_12_1"]}
                for _, r in df.iterrows()
            },
        }

    # ── Write ────────────────────────────────────────────────

    def get_ohlcv_last_date(self) -> Optional[date]:
        """Return max(date) across ohlcv_us, or None if empty/error.
        US-P0-003: used by batch quality gate to detect stale OHLCV DB."""
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM ohlcv_us;")
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row and row[0] is not None:
                return row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
            return None
        except Exception as e:
            logger.warning(f"[OHLCV_LAST_DATE_FAIL] {e}")
            return None

    def upsert_ohlcv(self, symbol: str, df: pd.DataFrame) -> int:
        """OHLCV upsert. Returns row count."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, r in df.iterrows():
            cur.execute(
                "INSERT INTO ohlcv_us (symbol, date, open, high, low, close, volume) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (symbol, date) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, volume=EXCLUDED.volume",
                (symbol, r["date"],
                 float(r["open"]), float(r["high"]),
                 float(r["low"]), float(r["close"]),
                 int(r.get("volume", 0) or 0))
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    def upsert_sector_map(self, records: List[Dict]) -> int:
        """Sector map bulk upsert."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for r in records:
            cur.execute(
                "INSERT INTO sector_map_us (symbol, name, sector, exchange, market_cap) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (symbol) DO UPDATE SET "
                "name=EXCLUDED.name, sector=EXCLUDED.sector, exchange=EXCLUDED.exchange, "
                "market_cap=EXCLUDED.market_cap",
                (r["symbol"], r.get("name", ""), r.get("sector", ""),
                 r.get("exchange", ""), r.get("market_cap", 0))
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    # ── Research OHLCV (Lab용, 운영 데이터와 분리) ────────

    def ensure_research_table(self):
        """Create ohlcv_us_research table if not exists."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_us_research (
                symbol VARCHAR(10),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                universe_tag VARCHAR(20) DEFAULT 'R1000',
                PRIMARY KEY (symbol, date)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[DB_US] ohlcv_us_research table ensured")

    def upsert_ohlcv_research(self, symbol: str, df: pd.DataFrame,
                               universe_tag: str = "R1000") -> int:
        """OHLCV upsert to research table."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, r in df.iterrows():
            cur.execute(
                "INSERT INTO ohlcv_us_research (symbol, date, open, high, low, close, volume, universe_tag) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (symbol, date) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, volume=EXCLUDED.volume, universe_tag=EXCLUDED.universe_tag",
                (symbol, r["date"],
                 float(r["open"]), float(r["high"]),
                 float(r["low"]), float(r["close"]),
                 int(r.get("volume", 0) or 0), universe_tag)
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    def load_close_dict_research(self, min_history: int = 252,
                                  symbols: List[str] = None) -> Dict[str, pd.Series]:
        """Load close_dict from BOTH operating + research tables (union)."""
        conn = self._conn()

        # Union operating + research
        query_parts = []
        params = []

        # Operating table
        query_parts.append("SELECT symbol, date, close FROM ohlcv_us")

        # Research table (if exists)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name='ohlcv_us_research'")
            has_research = cur.fetchone() is not None
            cur.close()
        except Exception:
            has_research = False

        if has_research:
            query_parts.append("SELECT symbol, date, close FROM ohlcv_us_research")

        union_query = " UNION ".join(query_parts)

        # Filter by symbols if provided
        if symbols:
            placeholders = ",".join(["%s"] * len(symbols))
            full_query = (
                f"SELECT symbol, date, close FROM ({union_query}) AS combined "
                f"WHERE symbol IN ({placeholders}) ORDER BY symbol, date"
            )
            params = list(symbols)
        else:
            full_query = f"SELECT symbol, date, close FROM ({union_query}) AS combined ORDER BY symbol, date"

        df = pd.read_sql(full_query, conn, params=params or None, parse_dates=["date"])
        conn.close()

        result = {}
        for sym, group in df.groupby("symbol"):
            s = group.set_index("date")["close"]
            s = pd.to_numeric(s, errors="coerce").fillna(0)
            if len(s) >= min_history:
                result[sym] = s

        return result

    def load_ohlcv_dict_research(self, min_history: int = 60,
                                  symbols: List[str] = None) -> Dict[str, pd.DataFrame]:
        """Load {symbol: DataFrame} from BOTH tables (union)."""
        conn = self._conn()
        cur = conn.cursor()

        # Check research table exists
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name='ohlcv_us_research'")
        has_research = cur.fetchone() is not None
        cur.close()

        union = "SELECT symbol, date, open, high, low, close, volume FROM ohlcv_us"
        if has_research:
            union += " UNION SELECT symbol, date, open, high, low, close, volume FROM ohlcv_us_research"

        if symbols:
            placeholders = ",".join(["%s"] * len(symbols))
            query = (
                f"SELECT * FROM ({union}) AS combined "
                f"WHERE symbol IN ({placeholders}) ORDER BY symbol, date"
            )
            df = pd.read_sql(query, conn, params=symbols, parse_dates=["date"])
        else:
            query = f"SELECT * FROM ({union}) AS combined ORDER BY symbol, date"
            df = pd.read_sql(query, conn, parse_dates=["date"])
        conn.close()

        result = {}
        for sym, group in df.groupby("symbol"):
            sub = group.drop(columns=["symbol"]).reset_index(drop=True)
            if len(sub) >= min_history:
                result[sym] = sub

        logger.info(f"[DB_US] Research loaded {len(result)} stocks")
        return result

    def research_health(self) -> dict:
        """Research table stats."""
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(DISTINCT symbol), COUNT(*), MAX(date) FROM ohlcv_us_research")
            row = cur.fetchone()
            cur.close()
            conn.close()
            return {"symbols": row[0], "rows": row[1], "latest": str(row[2]) if row[2] else ""}
        except Exception:
            return {"symbols": 0, "rows": 0, "latest": ""}

    def save_target_portfolio(self, target: dict, snapshot_id: str = "") -> int:
        """Save target portfolio to DB."""
        conn = self._conn()
        cur = conn.cursor()
        dt = target.get("date", "")
        count = 0
        for rank, sym in enumerate(target.get("target_tickers", []), 1):
            scores = target.get("scores", {}).get(sym, {})
            cur.execute(
                "INSERT INTO target_portfolio_us (date,symbol,vol_12m,mom_12_1,rank,snapshot_id) "
                "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (date, symbol) DO UPDATE SET "
                "vol_12m=EXCLUDED.vol_12m, mom_12_1=EXCLUDED.mom_12_1, "
                "rank=EXCLUDED.rank, snapshot_id=EXCLUDED.snapshot_id",
                (dt, sym, scores.get("vol_12m"), scores.get("mom_12_1"), rank, snapshot_id)
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    def save_trade(self, trade: dict) -> None:
        """Save trade record."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO trades_us (date,symbol,side,quantity,price,cost,reason,mode,order_id,fill_key) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (trade.get("date"), trade.get("symbol"), trade.get("side"),
             trade.get("quantity"), trade.get("price"), trade.get("cost"),
             trade.get("reason"), trade.get("mode"),
             trade.get("order_id"), trade.get("fill_key"))
        )
        conn.commit()
        cur.close()
        conn.close()

    def save_equity_snapshot(self, date_str: str, cash: float,
                             equity: float, n_pos: int, spy: float = 0) -> None:
        """Daily equity snapshot."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO equity_history_us (date,cash,equity,n_positions,spy_close) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (date) DO UPDATE SET "
            "cash=EXCLUDED.cash, equity=EXCLUDED.equity, "
            "n_positions=EXCLUDED.n_positions, spy_close=EXCLUDED.spy_close",
            (date_str, cash, equity, n_pos, spy)
        )
        conn.commit()
        cur.close()
        conn.close()

    def upsert_index(self, symbol: str, df: pd.DataFrame) -> int:
        """Index data upsert (SPY/QQQ)."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, r in df.iterrows():
            cur.execute(
                "INSERT INTO index_us (symbol, date, open, high, low, close, volume) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (symbol, date) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, volume=EXCLUDED.volume",
                (symbol, r["date"],
                 float(r["open"]), float(r["high"]),
                 float(r["low"]), float(r["close"]),
                 int(r.get("volume", 0) or 0))
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    # ── Health ───────────────────────────────────────────────

    def health_check(self) -> Dict:
        """DB health check."""
        try:
            conn = self._conn()
            cur = conn.cursor()
            tables = ["ohlcv_us", "sector_map_us", "target_portfolio_us",
                       "trades_us", "equity_history_us", "index_us"]
            result = {"status": "OK", "tables": []}
            # Tables with date column
            date_tables = {"ohlcv_us", "target_portfolio_us", "trades_us",
                           "equity_history_us", "index_us"}
            for t in tables:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                rows = cur.fetchone()[0]
                latest = None
                if t in date_tables:
                    cur.execute(f"SELECT MAX(date) FROM {t}")
                    latest = cur.fetchone()[0]
                result["tables"].append({
                    "table": t,
                    "rows": rows,
                    "latest": str(latest) if latest else "empty",
                    "status": "OK" if rows > 0 else "EMPTY",
                })
            cur.close()
            conn.close()
            return result
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
