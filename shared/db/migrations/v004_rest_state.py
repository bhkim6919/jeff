# -*- coding: utf-8 -*-
"""v004: rest_state.db → PostgreSQL."""
VERSION = 4
DESCRIPTION = "Create REST state tables (positions, equity, validation)"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rest_positions (
        code            TEXT NOT NULL,
        name            TEXT,
        qty             INTEGER NOT NULL,
        avg_price       REAL NOT NULL,
        entry_date      TEXT,
        current_price   REAL DEFAULT 0,
        high_watermark  REAL DEFAULT 0,
        trail_stop_price REAL DEFAULT 0,
        invested_total  REAL DEFAULT 0,
        is_active       INTEGER DEFAULT 1,
        snapshot_id     TEXT NOT NULL,
        snapshot_date   TEXT NOT NULL,
        asof_ts         TEXT NOT NULL,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (snapshot_date, code)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rp_snapshot ON rest_positions(snapshot_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rp_active ON rest_positions(is_active)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rest_equity_snapshots (
        id                  SERIAL,
        market_date         TEXT NOT NULL,
        snapshot_seq        INTEGER NOT NULL DEFAULT 0,
        asof_ts             TEXT NOT NULL,
        is_eod              INTEGER DEFAULT 0,
        close_equity        REAL NOT NULL,
        prev_close_equity   REAL DEFAULT 0,
        peak_equity         REAL DEFAULT 0,
        cash                REAL DEFAULT 0,
        holdings_count      INTEGER DEFAULT 0,
        rebalance_cycle_id  INTEGER DEFAULT 0,
        snapshot_id         TEXT NOT NULL,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (market_date, snapshot_seq)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_res_date ON rest_equity_snapshots(market_date)")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rest_validation_log (
        id          SERIAL,
        check_time  TEXT NOT NULL,
        check_type  TEXT NOT NULL,
        snapshot_id TEXT,
        gen4_value  TEXT,
        rest_value  TEXT,
        broker_value TEXT,
        diff_pct    REAL,
        status      TEXT NOT NULL,
        detail      TEXT,
        run_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (check_time, check_type)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rvl_type ON rest_validation_log(check_type)")

    cur.close()
