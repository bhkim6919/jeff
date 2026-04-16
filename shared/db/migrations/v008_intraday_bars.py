# -*- coding: utf-8 -*-
"""v008: intraday CSV → PostgreSQL."""
VERSION = 8
DESCRIPTION = "Create intraday_bars table (CSV EOD bulk insert)"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS intraday_bars (
        code            TEXT NOT NULL,
        bar_datetime    TIMESTAMPTZ NOT NULL,
        open            REAL,
        high            REAL,
        low             REAL,
        close           REAL,
        volume          BIGINT,
        status          TEXT DEFAULT 'HOLD',
        ingest_run_id   TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (code, bar_datetime)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ib_date ON intraday_bars(bar_datetime)")
    cur.close()
