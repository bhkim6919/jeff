# -*- coding: utf-8 -*-
"""v010: swing ranking CSV → PostgreSQL."""
VERSION = 10
DESCRIPTION = "Create swing_ranking table (CSV EOD bulk insert)"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS swing_ranking (
        snapshot_time   TIMESTAMPTZ NOT NULL,
        code            TEXT NOT NULL,
        rank            INTEGER,
        name            TEXT,
        price           REAL,
        change_pct      REAL,
        ingest_run_id   TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (snapshot_time, code)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sr_time ON swing_ranking(snapshot_time)")
    cur.close()
