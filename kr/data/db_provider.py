"""
db_provider.py -- PostgreSQL data provider for Q-TRON
======================================================
CSV 파일 대신 DB에서 OHLCV/Fundamental/Sector/Index 조회.
CSV 로드 30초 → DB 쿼리 0.1초.

Usage:
    from data.db_provider import get_db, DbProvider
    db = DbProvider()
    close_dict = db.load_ohlcv_dict(min_history=260)
    df = db.get_ohlcv("005930", start="2026-01-01")
"""
from __future__ import annotations
import logging
import json
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("gen4.db")

# ── Connection ───────────────────────────────────────────────

_DB_CONFIG = {
    "dbname": "qtron",
    "user": "postgres",
    "password": "!!@@123123qw",
    "host": "localhost",
    "port": 5432,
}

_pool = None


def get_conn():
    """Get a PostgreSQL connection."""
    import psycopg2
    return psycopg2.connect(**_DB_CONFIG)


def get_db() -> "DbProvider":
    """Singleton DbProvider."""
    global _pool
    if _pool is None:
        _pool = DbProvider()
    return _pool


class DbProvider:
    """PostgreSQL-based data provider. Drop-in replacement for CSV loading."""

    def __init__(self, config: dict = None):
        self._config = config or _DB_CONFIG

    def _conn(self):
        import psycopg2, warnings
        warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")
        return psycopg2.connect(**self._config)

    # ── OHLCV ────────────────────────────────────────────────

    def get_ohlcv(self, code: str, start: str = "", end: str = "") -> pd.DataFrame:
        """단일 종목 OHLCV DataFrame 반환."""
        conn = self._conn()
        query = "SELECT date, open, high, low, close, volume FROM ohlcv WHERE code = %s"
        params = [code]
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
                        codes: List[str] = None) -> Dict[str, pd.DataFrame]:
        """CSV load_ohlcv() 대체. {code: DataFrame} 반환."""
        conn = self._conn()
        cur = conn.cursor()

        # Get codes with enough history
        if codes:
            placeholders = ",".join(["%s"] * len(codes))
            cur.execute(
                f"SELECT code, COUNT(*) as cnt FROM ohlcv "
                f"WHERE code IN ({placeholders}) GROUP BY code HAVING COUNT(*) >= %s",
                codes + [min_history]
            )
        else:
            cur.execute(
                "SELECT code, COUNT(*) as cnt FROM ohlcv "
                "GROUP BY code HAVING COUNT(*) >= %s",
                (min_history,)
            )
        valid_codes = [r[0] for r in cur.fetchall()]
        cur.close()

        if not valid_codes:
            conn.close()
            return {}

        # Bulk load all valid codes
        placeholders = ",".join(["%s"] * len(valid_codes))
        query = (f"SELECT code, date, open, high, low, close, volume "
                 f"FROM ohlcv WHERE code IN ({placeholders}) ORDER BY code, date")
        df = pd.read_sql(query, conn, params=valid_codes, parse_dates=["date"])
        conn.close()

        # Split into dict
        result = {}
        for code, group in df.groupby("code"):
            result[code] = group.drop(columns=["code"]).reset_index(drop=True)

        logger.info(f"[DB] Loaded {len(result)} stocks ({len(df):,} rows)")
        return result

    def load_close_dict(self, min_history: int = 252) -> Dict[str, pd.Series]:
        """close_dict 반환 (scoring용). {code: Series(index=date, values=close)}."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT code FROM ohlcv GROUP BY code HAVING COUNT(*) >= %s",
            (min_history,)
        )
        valid_codes = [r[0] for r in cur.fetchall()]
        cur.close()

        if not valid_codes:
            conn.close()
            return {}

        placeholders = ",".join(["%s"] * len(valid_codes))
        query = (f"SELECT code, date, close FROM ohlcv "
                 f"WHERE code IN ({placeholders}) ORDER BY code, date")
        df = pd.read_sql(query, conn, params=valid_codes, parse_dates=["date"])
        conn.close()

        result = {}
        for code, group in df.groupby("code"):
            s = group.set_index("date")["close"]
            s = pd.to_numeric(s, errors="coerce").fillna(0)
            result[code] = s

        return result

    def build_matrices(self, codes: List[str] = None
                       ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame,
                                  pd.DataFrame, pd.DataFrame, pd.Series]:
        """build_matrices() 대체. close/open/high/low/vol matrices + dates 반환."""
        conn = self._conn()

        if codes:
            placeholders = ",".join(["%s"] * len(codes))
            query = (f"SELECT code, date, open, high, low, close, volume "
                     f"FROM ohlcv WHERE code IN ({placeholders}) ORDER BY date")
            df = pd.read_sql(query, conn, params=codes, parse_dates=["date"])
        else:
            df = pd.read_sql(
                "SELECT code, date, open, high, low, close, volume FROM ohlcv ORDER BY date",
                conn, parse_dates=["date"]
            )
        conn.close()

        if df.empty:
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty, pd.Series()

        pivot = df.pivot(index="date", columns="code")
        close = pivot["close"].ffill()
        opn = pivot["open"]
        high = pivot["high"]
        low = pivot["low"]
        vol = pivot["volume"].fillna(0)
        dates = close.index.to_series().reset_index(drop=True)

        return close, opn, high, low, vol, dates

    # ── Fundamental ──────────────────────────────────────────

    def get_fundamental(self, target_date: str = "") -> Optional[pd.DataFrame]:
        """최신 fundamental 스냅샷 반환."""
        conn = self._conn()
        if target_date:
            query = ("SELECT * FROM fundamental WHERE date <= %s "
                     "ORDER BY date DESC LIMIT 1")
            cur = conn.cursor()
            cur.execute(query, (target_date,))
            latest_date = cur.fetchone()
            cur.close()
            if latest_date:
                dt = latest_date[0]
                df = pd.read_sql(
                    "SELECT * FROM fundamental WHERE date = %s", conn, params=[dt])
            else:
                df = None
        else:
            df = pd.read_sql(
                "SELECT * FROM fundamental WHERE date = "
                "(SELECT MAX(date) FROM fundamental)", conn)
        conn.close()
        return df if df is not None and not df.empty else None

    # ── Sector Map ───────────────────────────────────────────

    def get_sector_map(self) -> Dict:
        """sector_map dict 반환."""
        conn = self._conn()
        df = pd.read_sql("SELECT * FROM sector_map", conn)
        conn.close()
        result = {}
        for _, r in df.iterrows():
            result[r["code"]] = {
                "name": r["name"], "sector": r["sector"], "market": r["market"]
            }
        return result

    # ── KOSPI Index ──────────────────────────────────────────

    def get_kospi_index(self) -> pd.DataFrame:
        """KOSPI index DataFrame 반환."""
        conn = self._conn()
        df = pd.read_sql(
            "SELECT date, open_price as open, high_price as high, "
            "low_price as low, close_price as close, volume "
            "FROM kospi_index ORDER BY date", conn, parse_dates=["date"])
        conn.close()
        return df

    # ── Target Portfolio ─────────────────────────────────────

    def get_target_portfolio(self, target_date: str = "") -> Optional[Dict]:
        """최신 target portfolio 반환."""
        conn = self._conn()
        if target_date:
            df = pd.read_sql(
                "SELECT * FROM target_portfolio WHERE date = %s ORDER BY rank",
                conn, params=[target_date])
        else:
            df = pd.read_sql(
                "SELECT * FROM target_portfolio WHERE date = "
                "(SELECT MAX(date) FROM target_portfolio) ORDER BY rank", conn)
        conn.close()

        if df.empty:
            return None
        return {
            "date": str(df["date"].iloc[0]),
            "target_tickers": df["code"].tolist(),
            "scores": {
                r["code"]: {"vol_12m": r["vol_12m"], "mom_12_1": r["mom_12_1"]}
                for _, r in df.iterrows()
            },
        }

    # ── Write (Batch 연동) ───────────────────────────────────

    def upsert_ohlcv(self, code: str, df: pd.DataFrame) -> int:
        """OHLCV 데이터 upsert. 반환: 삽입/갱신 행 수."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, r in df.iterrows():
            cur.execute(
                "INSERT INTO ohlcv (code, date, open, high, low, close, volume) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (code, date) DO UPDATE SET "
                "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                "close=EXCLUDED.close, volume=EXCLUDED.volume",
                (code, r["date"], int(r["open"]), int(r["high"]),
                 int(r["low"]), int(r["close"]), int(r["volume"]))
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    def upsert_fundamental(self, date_str: str, df: pd.DataFrame) -> int:
        """Fundamental 데이터 upsert."""
        conn = self._conn()
        cur = conn.cursor()
        count = 0
        for _, r in df.iterrows():
            tk = str(r.get("ticker", "")).zfill(6)
            cur.execute(
                "INSERT INTO fundamental (date,code,per,pbr,eps,bps,div_yield,market_cap,foreign_ratio) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (date, code) DO UPDATE SET "
                "per=EXCLUDED.per, pbr=EXCLUDED.pbr, eps=EXCLUDED.eps, "
                "bps=EXCLUDED.bps, div_yield=EXCLUDED.div_yield",
                (date_str, tk, r.get("per"), r.get("pbr"), r.get("eps"),
                 r.get("bps"), r.get("div_yield"),
                 int(r.get("market_cap", 0) or 0), r.get("foreign_ratio"))
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    def save_target_portfolio(self, target: dict) -> int:
        """Target portfolio DB 저장."""
        conn = self._conn()
        cur = conn.cursor()
        dt = target.get("date", "")
        if len(dt) == 8:
            dt = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
        count = 0
        for rank, tk in enumerate(target.get("target_tickers", []), 1):
            scores = target.get("scores", {}).get(tk, {})
            cur.execute(
                "INSERT INTO target_portfolio (date,code,vol_12m,mom_12_1,rank) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (date, code) DO UPDATE SET "
                "vol_12m=EXCLUDED.vol_12m, mom_12_1=EXCLUDED.mom_12_1, rank=EXCLUDED.rank",
                (dt, tk, scores.get("vol_12m"), scores.get("mom_12_1"), rank)
            )
            count += 1
        conn.commit()
        cur.close()
        conn.close()
        return count

    # ── Report Tables (report_* prefix) ────────────────────────

    def ensure_report_tables(self) -> None:
        """Create report_* tables if they don't exist."""
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS report_trades (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                code VARCHAR(20),
                side VARCHAR(10),
                quantity INTEGER,
                price FLOAT,
                cost FLOAT,
                slippage_pct VARCHAR(20),
                mode VARCHAR(20),
                event_id VARCHAR(60),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(mode, event_id)
            );
            CREATE TABLE IF NOT EXISTS report_close_log (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                code VARCHAR(20),
                exit_reason VARCHAR(40),
                quantity INTEGER,
                entry_price FLOAT,
                exit_price FLOAT,
                entry_date VARCHAR(20),
                hold_days INTEGER,
                pnl_pct FLOAT,
                pnl_amount FLOAT,
                mode VARCHAR(20),
                event_id VARCHAR(60),
                entry_rank INTEGER,
                score_mom FLOAT,
                max_hwm_pct FLOAT,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(mode, event_id)
            );
            CREATE TABLE IF NOT EXISTS report_equity_log (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                equity FLOAT,
                cash FLOAT,
                n_positions INTEGER,
                daily_pnl_pct FLOAT,
                monthly_dd_pct FLOAT,
                risk_mode VARCHAR(30),
                rebalance_executed VARCHAR(5),
                price_fail_count INTEGER,
                reconcile_corrections INTEGER,
                monitor_only VARCHAR(5),
                kospi_close FLOAT,
                kosdaq_close FLOAT,
                regime VARCHAR(20),
                kospi_ma200 FLOAT,
                breadth FLOAT,
                mode VARCHAR(20) DEFAULT 'LIVE',
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(mode, date)
            );
            CREATE TABLE IF NOT EXISTS report_decision_log (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(60),
                date VARCHAR(20),
                code VARCHAR(20),
                side VARCHAR(10),
                reason VARCHAR(40),
                score_vol FLOAT,
                score_mom FLOAT,
                rank INTEGER,
                target_weight FLOAT,
                price FLOAT,
                cash_before FLOAT,
                high_watermark FLOAT,
                trail_stop_price FLOAT,
                pnl_pct FLOAT,
                hold_days INTEGER,
                regime VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(event_id)
            );
            CREATE TABLE IF NOT EXISTS report_reconcile_log (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                time VARCHAR(20),
                code VARCHAR(20),
                diff_type VARCHAR(30),
                engine_qty INTEGER,
                broker_qty INTEGER,
                engine_avg FLOAT,
                broker_avg FLOAT,
                resolution VARCHAR(40),
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS report_daily_positions (
                id SERIAL PRIMARY KEY,
                date VARCHAR(20),
                code VARCHAR(20),
                quantity INTEGER,
                avg_price FLOAT,
                current_price FLOAT,
                market_value FLOAT,
                pnl_pct FLOAT,
                pnl_amount FLOAT,
                est_cost_pct FLOAT,
                net_pnl_pct FLOAT,
                high_watermark FLOAT,
                trail_stop_price FLOAT,
                entry_date VARCHAR(20),
                hold_days INTEGER,
                hwm_pct FLOAT,
                mode VARCHAR(20) DEFAULT 'LIVE',
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(mode, date, code)
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[DB] report_* tables ensured")

    def insert_report_trade(self, date_str, code, side, qty, price,
                            cost, slippage_pct, mode, event_id):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_trades "
            "(date,code,side,quantity,price,cost,slippage_pct,mode,event_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (date_str, code, side, qty, price, cost, slippage_pct, mode, event_id)
        )
        conn.commit()
        cur.close()
        conn.close()

    def insert_report_close(self, date_str, code, exit_reason, quantity,
                            entry_price, exit_price, entry_date, hold_days,
                            pnl_pct, pnl_amount, mode, event_id,
                            entry_rank, score_mom, max_hwm_pct):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_close_log "
            "(date,code,exit_reason,quantity,entry_price,exit_price,"
            "entry_date,hold_days,pnl_pct,pnl_amount,mode,event_id,"
            "entry_rank,score_mom,max_hwm_pct) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (date_str, code, exit_reason, quantity, entry_price, exit_price,
             entry_date, hold_days, pnl_pct, pnl_amount, mode, event_id,
             entry_rank, score_mom, max_hwm_pct)
        )
        conn.commit()
        cur.close()
        conn.close()

    def insert_report_equity(self, date_str, equity, cash, n_positions,
                             daily_pnl_pct, monthly_dd_pct, risk_mode,
                             rebalance_executed, price_fail_count,
                             reconcile_corrections, monitor_only,
                             kospi_close, kosdaq_close, regime,
                             kospi_ma200, breadth, mode="LIVE"):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_equity_log "
            "(date,equity,cash,n_positions,daily_pnl_pct,monthly_dd_pct,"
            "risk_mode,rebalance_executed,price_fail_count,"
            "reconcile_corrections,monitor_only,"
            "kospi_close,kosdaq_close,regime,kospi_ma200,breadth,mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (mode, date) DO UPDATE SET "
            "equity=EXCLUDED.equity, cash=EXCLUDED.cash, "
            "n_positions=EXCLUDED.n_positions, daily_pnl_pct=EXCLUDED.daily_pnl_pct",
            (date_str, equity, cash, n_positions, daily_pnl_pct, monthly_dd_pct,
             risk_mode, rebalance_executed, price_fail_count,
             reconcile_corrections, monitor_only,
             kospi_close, kosdaq_close, regime, kospi_ma200, breadth, mode)
        )
        conn.commit()
        cur.close()
        conn.close()

    def insert_report_decision(self, event_id, date_str, code, side, reason,
                               score_vol, score_mom, rank, target_weight,
                               price, cash_before, high_watermark,
                               trail_stop_price, pnl_pct, hold_days, regime):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_decision_log "
            "(event_id,date,code,side,reason,score_vol,score_mom,rank,"
            "target_weight,price,cash_before,high_watermark,"
            "trail_stop_price,pnl_pct,hold_days,regime) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (event_id, date_str, code, side, reason, score_vol, score_mom,
             rank, target_weight, price, cash_before, high_watermark,
             trail_stop_price, pnl_pct, hold_days, regime)
        )
        conn.commit()
        cur.close()
        conn.close()

    def insert_report_reconcile(self, date_str, time_str, code, diff_type,
                                engine_qty, broker_qty, engine_avg,
                                broker_avg, resolution):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_reconcile_log "
            "(date,time,code,diff_type,engine_qty,broker_qty,"
            "engine_avg,broker_avg,resolution) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (date_str, time_str, code, diff_type, engine_qty, broker_qty,
             engine_avg, broker_avg, resolution)
        )
        conn.commit()
        cur.close()
        conn.close()

    def insert_report_daily_position(self, date_str, code, quantity,
                                     avg_price, current_price, market_value,
                                     pnl_pct, pnl_amount, est_cost_pct,
                                     net_pnl_pct, high_watermark,
                                     trail_stop_price, entry_date,
                                     hold_days, hwm_pct, mode="LIVE"):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO report_daily_positions "
            "(date,code,quantity,avg_price,current_price,market_value,"
            "pnl_pct,pnl_amount,est_cost_pct,net_pnl_pct,"
            "high_watermark,trail_stop_price,entry_date,hold_days,hwm_pct,mode) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (mode, date, code) DO UPDATE SET "
            "quantity=EXCLUDED.quantity, current_price=EXCLUDED.current_price, "
            "market_value=EXCLUDED.market_value, pnl_pct=EXCLUDED.pnl_pct",
            (date_str, code, quantity, avg_price, current_price, market_value,
             pnl_pct, pnl_amount, est_cost_pct, net_pnl_pct,
             high_watermark, trail_stop_price, entry_date, hold_days, hwm_pct, mode)
        )
        conn.commit()
        cur.close()
        conn.close()
