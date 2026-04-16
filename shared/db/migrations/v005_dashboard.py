# -*- coding: utf-8 -*-
"""v005: dashboard.db → PostgreSQL."""
VERSION = 5
DESCRIPTION = "Create dashboard tables (snapshots, alert_state)"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dashboard_snapshots (
        market_date         TEXT NOT NULL,
        epoch               REAL NOT NULL,
        ts                  TEXT NOT NULL,
        kospi_price         REAL,
        kospi_change_pct    REAL,
        kosdaq_price        REAL,
        kosdaq_change_pct   REAL,
        portfolio_equity    REAL,
        portfolio_pnl_pct   REAL,
        portfolio_cash      REAL,
        holdings_count      INTEGER,
        source              TEXT DEFAULT 'sse',
        snapshot_id         TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (market_date, epoch)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ds_date ON dashboard_snapshots(market_date)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dashboard_alert_state (
        alert_key   TEXT PRIMARY KEY,
        last_sent   TIMESTAMPTZ,
        send_count  INTEGER DEFAULT 0,
        suppressed  INTEGER DEFAULT 0,
        run_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)

    cur.close()
