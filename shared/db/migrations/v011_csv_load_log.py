# -*- coding: utf-8 -*-
"""v011: CSV 적재 추적 테이블."""
VERSION = 11
DESCRIPTION = "Create csv_load_log table (EOD ingest tracking)"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS csv_load_log (
        trade_date      TEXT NOT NULL,
        dataset         TEXT NOT NULL,
        ingest_run_id   TEXT NOT NULL,
        row_count       INTEGER,
        loaded_at       TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, dataset)
    )
    """)
    cur.close()
