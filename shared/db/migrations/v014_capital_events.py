# -*- coding: utf-8 -*-
"""v014: Capital events table for real-live account deposits/withdrawals.

Tracks external capital changes (deposit / withdraw / fee / interest /
dividend) separately from trading P&L so cumulative return calculations
can subtract / adjust for non-trading equity changes.

Use case (2026-04-20):
  Jeff plans to deposit additional cash to Kiwoom account before 5월
  초 rebalance. Without tracking, engine's equity_log would record the
  deposit as "gain" and distort cumulative return.

Tables:
  - capital_events: one row per external capital event (mode-scoped)

Query patterns:
  - SELECT SUM(amount) WHERE mode='live' AND event_type='deposit'
    AND event_date <= :date  → cumulative deposits up to that date
  - Used by equity adjustment: adjusted_equity = raw_equity - cum_deposits
"""
VERSION = 14
DESCRIPTION = "Capital events table (deposits/withdrawals/dividends — equity adjustment)"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS capital_events (
        id           BIGSERIAL PRIMARY KEY,
        mode         TEXT NOT NULL
            CHECK (mode IN ('live','paper','paper_forward','backtest')),
        market       TEXT NOT NULL
            CHECK (market IN ('KR','US')),
        event_date   DATE NOT NULL,
        event_type   TEXT NOT NULL
            CHECK (event_type IN (
                'deposit','withdraw','dividend','interest','fee','adjustment'
            )),
        amount       NUMERIC(18,2) NOT NULL,
        currency     TEXT NOT NULL DEFAULT 'KRW',
        note         TEXT NOT NULL DEFAULT '',
        recorded_by  TEXT NOT NULL DEFAULT 'jeff',
        recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        source       TEXT NOT NULL DEFAULT 'manual',
        external_ref TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_capital_events_mode_market_date
        ON capital_events(mode, market, event_date DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_capital_events_type
        ON capital_events(event_type, event_date DESC)
    """)

    cur.close()
