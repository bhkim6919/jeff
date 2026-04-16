# -*- coding: utf-8 -*-
"""v012: lab trades history CSV → PostgreSQL."""
VERSION = 12
DESCRIPTION = "Create lab_trades_history table + dual_write_mismatch_log"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lab_trades_history (
        trade_date      TEXT NOT NULL,
        strategy        TEXT NOT NULL,
        code            TEXT NOT NULL,
        side            TEXT NOT NULL,
        qty             INTEGER,
        price           REAL,
        cost            REAL,
        reason          TEXT,
        eod_run_id      TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, strategy, code, side)
    )
    """)

    # dual_write_mismatch_log (Phase 2에서 사용)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dual_write_mismatch_log (
        mismatch_id       SERIAL PRIMARY KEY,
        context           TEXT NOT NULL,
        table_name        TEXT NOT NULL,
        pk_repr           TEXT NOT NULL,
        occurred_at       TIMESTAMPTZ DEFAULT NOW(),
        pg_ok             BOOLEAN NOT NULL,
        sqlite_ok         BOOLEAN NOT NULL,
        error_message     TEXT,
        retry_count       INTEGER DEFAULT 0,
        resolved_at       TIMESTAMPTZ,
        resolution_status TEXT
    )
    """)

    cur.close()
