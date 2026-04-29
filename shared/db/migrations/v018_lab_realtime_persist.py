# -*- coding: utf-8 -*-
"""v018: Lab realtime / simulate result persistence in PostgreSQL.

Background (Jeff 2026-04-29):
    Lab realtime (kr/web/lab_realtime.py — Conservative / Aggressive /
    Dynamic) and Lab simulate (kr/web/lab_simulator.py) currently
    persist their results to:
      - kr/data/lab_results/sim_<YYYYMMDD_HHMMSS>.json  (one per run)
      - kr/data/lab_results/trades_history.csv         (all trades)

    The CSV is the de-facto source of truth for downstream analysis
    (back-data for the eventual live-trading expansion of Lab), and
    the JSON carries the run-level params + summary metrics. Two
    file-based stores are fragile and hard to query at scale.

    This migration adds a pair of relational tables so the same
    information becomes queryable from PG directly.

Schema decisions:
    * lab_realtime_runs: one row per ``stop()`` (or per
      ``run_simulation()``) call. ``params`` is JSONB so the 12+
      tunable knobs are queryable individually
      (``params->>'top_n'`` etc.) without ALTER TABLE every time
      a new param is added.
    * lab_realtime_trades: one row per BUY or SELL leg.
      ``run_id`` FK gives O(1) join back to the run-level meta
      and a ``ON DELETE CASCADE`` keeps the table consistent if
      a run is ever pruned.
    * sim_ts is duplicated on the trade table (denormalized) so
      the common "all trades for date X" query doesn't need a
      JOIN. Indexed.
    * mode column distinguishes ``'realtime'`` (event-driven over
      WebSocket) from ``'simulate'`` (instant backtest); shared
      schema lets both modes accumulate into the same back-data
      pool with a 1-character filter.
    * NOT enrolled in dual_write — the file sinks already cover
      the durability layer, and PG is the queryable secondary.
      Insert failures are swallowed in lab_simulator._save_result.

Idempotency:
    CREATE TABLE IF NOT EXISTS — re-running the migration is a no-op.
    Column additions in future migrations should use IF NOT EXISTS.

Disabling the PG sink at runtime:
    Operators can set QTRON_LAB_REALTIME_PG=0 to skip the PG insert
    in _save_result(). The file sinks (JSON + CSV) keep functioning.
"""
VERSION = 18
DESCRIPTION = "Lab realtime/simulate persistence: lab_realtime_runs + lab_realtime_trades"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lab_realtime_runs (
        run_id        SERIAL PRIMARY KEY,
        sim_ts        TIMESTAMPTZ NOT NULL UNIQUE,
        mode          TEXT NOT NULL,
        started_at    TIMESTAMPTZ,
        stopped_at    TIMESTAMPTZ,
        elapsed_sec   NUMERIC,
        tick_count    BIGINT,
        initial_cash  NUMERIC,
        ranking_count INTEGER,
        params        JSONB NOT NULL,
        summary       JSONB,
        created_at    TIMESTAMPTZ DEFAULT NOW()
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_lab_realtime_runs_mode
        ON lab_realtime_runs(mode)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lab_realtime_trades (
        trade_id      SERIAL PRIMARY KEY,
        run_id        INTEGER NOT NULL
                      REFERENCES lab_realtime_runs(run_id) ON DELETE CASCADE,
        sim_ts        TIMESTAMPTZ NOT NULL,
        strategy      TEXT NOT NULL,
        rank_value    INTEGER,
        code          TEXT,
        name          TEXT,
        side          TEXT,
        entry_price   NUMERIC,
        exit_price    NUMERIC,
        qty           NUMERIC,
        pnl           NUMERIC,
        pnl_pct       NUMERIC,
        exit_reason   TEXT,
        entry_time    TEXT,
        exit_time     TEXT,
        created_at    TIMESTAMPTZ DEFAULT NOW()
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_lab_realtime_trades_run
        ON lab_realtime_trades(run_id)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_lab_realtime_trades_sim_ts
        ON lab_realtime_trades(sim_ts)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_lab_realtime_trades_strategy_code
        ON lab_realtime_trades(strategy, code)
    """)

    cur.close()
