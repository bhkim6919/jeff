# -*- coding: utf-8 -*-
"""v002: regime.db → PostgreSQL (predictions, actuals, scores)."""
VERSION = 2
DESCRIPTION = "Create regime tables (predictions, actuals, scores)"


def up(conn):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS regime_predictions (
        id              SERIAL,
        feature_date    TEXT NOT NULL,
        target_date     TEXT NOT NULL,
        model_name      TEXT NOT NULL DEFAULT 'composite',
        predicted_regime INTEGER NOT NULL,
        predicted_label TEXT NOT NULL,
        composite_score REAL NOT NULL,
        global_score    REAL,
        global_avail    INTEGER,
        vol_score       REAL,
        vol_avail       INTEGER,
        domestic_score  REAL,
        domestic_avail  INTEGER,
        micro_score     REAL,
        micro_avail     INTEGER,
        fx_score        REAL,
        fx_avail        INTEGER,
        available_weight REAL,
        confidence_flag TEXT,
        source_health   TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (target_date, model_name)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS regime_actuals (
        id              SERIAL,
        market_date     TEXT NOT NULL,
        actual_regime   INTEGER NOT NULL,
        actual_label    TEXT NOT NULL,
        kospi_change    REAL,
        actual_method   TEXT DEFAULT 'kospi_change',
        extra_data      TEXT,
        run_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (market_date)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS regime_scores (
        id                  SERIAL,
        target_date         TEXT NOT NULL,
        model_name          TEXT NOT NULL DEFAULT 'composite',
        prediction_id       INTEGER,
        actual_id           INTEGER,
        predicted           INTEGER NOT NULL,
        actual              INTEGER NOT NULL,
        distance            INTEGER NOT NULL,
        raw_confidence      REAL NOT NULL,
        adjusted_confidence REAL NOT NULL,
        available_weight    REAL,
        confidence_flag     TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (target_date, model_name)
    )
    """)

    cur.close()
