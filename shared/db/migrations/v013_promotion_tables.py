# -*- coding: utf-8 -*-
"""v013: Promotion artifacts → PostgreSQL.

Migrates 3 promotion-system storages from file (JSON/JSONL) to PG tables:
  - promotion_regime_history  ← regime_history.jsonl
  - promotion_ops_snapshot    ← ops_metrics.json (current state)
  - promotion_ops_events      ← ops_events.jsonl (append-only audit)
  - promotion_transition_log  ← transition_log.jsonl

File-based storage is retained as failure fallback + historical archive.
"""
VERSION = 13
DESCRIPTION = "Promotion artifacts tables (regime history, ops snapshot, transition log)"


def up(conn):
    cur = conn.cursor()

    # ── Regime History (EOD-confirmed per strategy) ─────────────────
    # snapshot_version 기반 idempotency (trade_date + strategy + snapshot_version 유일).
    # snapshot_version이 다른 rerun은 새 row로 append 허용.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS promotion_regime_history (
        id                    BIGSERIAL PRIMARY KEY,
        trade_date            DATE NOT NULL,
        strategy_name         TEXT NOT NULL,
        regime_label          TEXT NOT NULL
            CHECK (regime_label IN ('BULL','BEAR','SIDEWAYS','UNKNOWN')),
        regime_source_version TEXT NOT NULL DEFAULT 'REGIME_V1',
        confidence            NUMERIC(6,4) NOT NULL DEFAULT 0,
        snapshot_version      TEXT NOT NULL DEFAULT '',
        recorded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (trade_date, strategy_name, snapshot_version)
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_prh_date
        ON promotion_regime_history(trade_date DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_prh_strategy
        ON promotion_regime_history(strategy_name, trade_date DESC)
    """)

    # ── Ops Snapshot (latest value per field; UPSERT target) ────────
    # value=NULL 은 UNKNOWN (evidence missing) — 0과 반드시 구분.
    # promotion.evidence collector 가 field_name 별로 조회.
    # Column 이름: "window" 은 PG 예약어 → window_scope 으로 명명.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS promotion_ops_snapshot (
        field_name   TEXT PRIMARY KEY,
        value        INTEGER,
        source       TEXT NOT NULL,
        window_scope TEXT NOT NULL,
        ts           TIMESTAMPTZ NOT NULL,
        write_origin TEXT NOT NULL DEFAULT 'eod_finalize'
    )
    """)

    # ── Ops Events (append-only audit log) ──────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS promotion_ops_events (
        id         BIGSERIAL PRIMARY KEY,
        event_type TEXT NOT NULL,
        payload    JSONB NOT NULL,
        ts         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_poe_type_ts
        ON promotion_ops_events(event_type, ts DESC)
    """)

    # ── Transition Log (status change history) ──────────────────────
    # 중복 방지는 application layer (직전 new_status가 같으면 skip).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS promotion_transition_log (
        id            BIGSERIAL PRIMARY KEY,
        strategy      TEXT NOT NULL,
        evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        old_status    TEXT,
        new_status    TEXT NOT NULL,
        reason        TEXT NOT NULL DEFAULT '',
        blockers      JSONB NOT NULL DEFAULT '[]'::jsonb,
        score         INTEGER,
        versions      JSONB NOT NULL DEFAULT '{}'::jsonb
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ptl_strategy_ts
        ON promotion_transition_log(strategy, evaluated_at DESC)
    """)

    cur.close()
