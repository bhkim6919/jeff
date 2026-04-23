# -*- coding: utf-8 -*-
"""v016: Add severity column to dashboard_alert_state (R19, 2026-04-23).

Root cause: kr/notify/alert_state.py:30 reads `last_sent, severity` but the
v005 schema never created a `severity` column. Every call to can_send()
raised psycopg2 UndefinedColumn → dedup/burst bypassed silently.

This migration adds the column (idempotent — ADD COLUMN IF NOT EXISTS).
Existing rows get NULL severity; code treats `None or ""` as no prior
severity, which preserves current behavior on migrated data.
"""
VERSION = 16
DESCRIPTION = "R19: add severity column to dashboard_alert_state"


def up(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE dashboard_alert_state
        ADD COLUMN IF NOT EXISTS severity TEXT
    """)
    cur.close()
