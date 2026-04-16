# -*- coding: utf-8 -*-
"""v003: theme_regime.db → PostgreSQL."""
VERSION = 3
DESCRIPTION = "Create regime_theme_daily table"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS regime_theme_daily (
        market_date TEXT NOT NULL,
        theme_code  TEXT NOT NULL,
        theme_name  TEXT,
        stock_count INTEGER DEFAULT 0,
        change_pct  REAL DEFAULT 0,
        regime      TEXT DEFAULT 'SIDEWAYS',
        streak_days INTEGER DEFAULT 0,
        run_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (market_date, theme_code)
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_rtd_date
    ON regime_theme_daily(market_date)
    """)
    cur.close()
