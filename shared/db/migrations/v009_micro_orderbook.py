# -*- coding: utf-8 -*-
"""v009: micro orderbook CSV → PostgreSQL."""
VERSION = 9
DESCRIPTION = "Create micro_orderbook table (CSV EOD bulk insert)"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS micro_orderbook (
        code            TEXT NOT NULL,
        ts              TIMESTAMPTZ NOT NULL,
        price           REAL,
        best_ask        REAL,
        best_bid        REAL,
        ask_qty_1       BIGINT,
        bid_qty_1       BIGINT,
        total_ask       BIGINT,
        total_bid       BIGINT,
        net_bid         BIGINT,
        volume          BIGINT,
        ingest_run_id   TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (code, ts)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mo_ts ON micro_orderbook(ts)")
    cur.close()
