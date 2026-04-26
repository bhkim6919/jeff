# -*- coding: utf-8 -*-
"""v017: Add `state` column to dashboard_alert_state (2026-04-26).

Root cause for noisy Telegram alerts (Jeff report 2026-04-26):
`kr/notify/alert_state.py:record_sent(event_key, severity, state)` accepted
a `state` argument and `get_last_state(event_key)` was supposed to return
the previously-recorded state — but neither column existed in the schema,
and `get_last_state` was SELECT-ing `alert_key` (the literal event key)
instead of any state value.

Effect: `_check_regime` compared `prev = "regime_level"` (the literal key)
against the current label like "NEUTRAL", which always differed → an alert
fired every evaluation cycle, throttled only by the 30-min DEDUP_TTL. Same
bug pattern affected dd_daily_warn, dd_monthly_warn, system_stale, and
recon_unsafe transition checks.

This migration adds a nullable `state` TEXT column (idempotent —
ADD COLUMN IF NOT EXISTS). Existing rows get NULL state; alert_state.py
treats NULL as "no prior state recorded" and the next eval will record
the current state without firing an alert (matches the `elif not prev`
branch in _check_regime).
"""
VERSION = 17
DESCRIPTION = "Add state column to dashboard_alert_state (fix transition tracking)"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE dashboard_alert_state
        ADD COLUMN IF NOT EXISTS state TEXT
    """)
    # Backfill NULL → '' so get_last_state returns a consistent
    # falsy-empty value (vs Python None) for pre-existing rows.
    # Matches Jeff's R-spec (2026-04-26): state TEXT, no NULLs.
    cur.execute("""
        UPDATE dashboard_alert_state
        SET state = ''
        WHERE state IS NULL
    """)
    cur.close()
