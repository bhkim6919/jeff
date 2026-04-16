# -*- coding: utf-8 -*-
"""v007: meta_strategy.db US → PostgreSQL (9 tables + views)."""
VERSION = 7
DESCRIPTION = "Create US meta strategy tables (_us suffix)"


def up(conn):
    cur = conn.cursor()

    # ── market_context (US: index_return, no small_vs_large/regime) ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_market_context_us (
        trade_date          TEXT PRIMARY KEY,
        index_return        REAL,
        adv_ratio           REAL,
        sector_dispersion   REAL,
        breakout_ratio      REAL,
        data_snapshot_id    TEXT,
        eod_run_id          TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """)

    # ── strategy_daily (동일) ────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_strategy_daily_us (
        trade_date          TEXT NOT NULL,
        strategy            TEXT NOT NULL,
        strategy_version    TEXT NOT NULL,
        daily_return        REAL,
        cumul_return        REAL,
        position_count      INTEGER,
        win_count           INTEGER,
        loss_count          INTEGER,
        turnover            REAL,
        cash_ratio          REAL,
        gross_exposure      REAL,
        eod_run_id          TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, strategy)
    )
    """)

    # ── strategy_exposure (US: no avg_market_cap) ────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_strategy_exposure_us (
        trade_date          TEXT NOT NULL,
        strategy            TEXT NOT NULL,
        top1_weight         REAL,
        top5_weight         REAL,
        sector_top1         TEXT,
        sector_top1_weight  REAL,
        sector_dispersion   REAL,
        eod_run_id          TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, strategy)
    )
    """)

    # ── run_quality (동일) ───────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_run_quality_us (
        trade_date              TEXT NOT NULL,
        snapshot_version        TEXT NOT NULL,
        market                  TEXT NOT NULL,
        sync_status             TEXT,
        synced_count            INTEGER,
        failed_count            INTEGER,
        expected_count          INTEGER,
        completeness_ratio      REAL,
        selected_source         TEXT,
        csv_last_date           TEXT,
        db_last_date            TEXT,
        data_snapshot_id        TEXT,
        degraded_flag           INTEGER DEFAULT 0,
        ohlc_invariant_warn_count INTEGER DEFAULT 0,
        run_id                  TEXT,
        eod_run_id              TEXT,
        run_ts                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, snapshot_version)
    )
    """)

    # ── risk (동일) ──────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_strategy_risk_us (
        trade_date          TEXT NOT NULL,
        strategy            TEXT NOT NULL,
        snapshot_version    TEXT,
        daily_mdd           REAL,
        rolling_5d_return   REAL,
        rolling_20d_return  REAL,
        rolling_20d_mdd     REAL,
        realized_vol_20d    REAL,
        hit_rate_20d        REAL,
        avg_hold_days       REAL,
        slippage_bps_est    REAL,
        cost_bps_est        REAL,
        eod_run_id          TEXT,
        run_ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, strategy)
    )
    """)

    # ── outcome (동일) ───────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_strategy_outcome_us (
        trade_date                  TEXT NOT NULL,
        strategy                    TEXT NOT NULL,
        snapshot_version            TEXT,
        fwd_1d_return               REAL,
        fwd_3d_return               REAL,
        fwd_5d_return               REAL,
        fwd_vol_5d                  REAL,
        fwd_sharpe_5d               REAL,
        fwd_to_next_rebal_return    REAL,
        fwd_5d_mdd                  REAL,
        fwd_5d_max_runup            REAL,
        cost_adjusted_fwd_1d        REAL,
        cost_adjusted_fwd_5d        REAL,
        label_win_1d                INTEGER,
        label_win_5d                INTEGER,
        is_valid                    INTEGER DEFAULT 1,
        anchor_execution_date       TEXT,
        computed_at                 TEXT,
        eod_run_id                  TEXT,
        run_ts                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, strategy)
    )
    """)

    # ── recommendation (동일) ────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_recommendation_us (
        trade_date                  TEXT NOT NULL,
        snapshot_version            TEXT NOT NULL,
        recommendation_date         TEXT,
        recommended_weights_json    TEXT,
        top_strategy                TEXT,
        top3_strategies_json        TEXT,
        confidence_score            REAL,
        regime_label                TEXT,
        regime_persistence_days     INTEGER,
        market_fit_summary          TEXT,
        perf_health_summary         TEXT,
        data_quality_status         TEXT,
        is_valid                    INTEGER DEFAULT 1,
        reason_codes                TEXT,
        selected_source             TEXT,
        eod_run_id                  TEXT,
        run_ts                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, snapshot_version)
    )
    """)

    # ── execution_decision (동일) ────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_execution_decision_us (
        trade_date                  TEXT NOT NULL,
        execution_date              TEXT NOT NULL,
        snapshot_version            TEXT,
        recommended_strategy        TEXT,
        recommended_weights_json    TEXT,
        executed_strategy           TEXT,
        executed_weights_json       TEXT,
        changed_flag                INTEGER DEFAULT 0,
        previous_strategy           TEXT,
        change_reason               TEXT,
        blocked_by                  TEXT,
        switch_cost_estimate        REAL,
        actual_switch_cost_bps      REAL,
        turnover_at_switch          REAL,
        post_switch_drawdown        REAL,
        cooldown_active             INTEGER DEFAULT 0,
        hysteresis_state            TEXT,
        approved_by                 TEXT,
        eod_run_id                  TEXT,
        run_ts                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, execution_date)
    )
    """)

    # ── universe_snapshot (동일) ─────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS meta_universe_snapshot_us (
        trade_date                  TEXT NOT NULL,
        snapshot_version            TEXT NOT NULL,
        universe_count_raw          INTEGER,
        universe_count_filtered     INTEGER,
        missing_data_count          INTEGER,
        tradable_count              INTEGER,
        excluded_reasons_json       TEXT,
        eod_run_id                  TEXT,
        run_ts                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (trade_date, snapshot_version)
    )
    """)

    # ── Views ────────────────────────────────────────────────
    cur.execute("""
    CREATE OR REPLACE VIEW meta_strategy_outcome_clean_us AS
    SELECT so.* FROM meta_strategy_outcome_us so
    JOIN meta_run_quality_us mrq
        ON so.trade_date = mrq.trade_date
        AND so.snapshot_version = mrq.snapshot_version
    WHERE so.is_valid = 1 AND mrq.sync_status = 'OK' AND mrq.degraded_flag = 0
    """)

    cur.execute("""
    CREATE OR REPLACE VIEW meta_strategy_outcome_latest_us AS
    SELECT so.* FROM meta_strategy_outcome_us so
    INNER JOIN (
        SELECT trade_date, strategy, MAX(created_at) AS latest_ts
        FROM meta_strategy_outcome_us GROUP BY trade_date, strategy
    ) latest ON so.trade_date = latest.trade_date
        AND so.strategy = latest.strategy
        AND so.created_at = latest.latest_ts
    """)

    cur.execute("""
    CREATE OR REPLACE VIEW meta_recommendation_clean_us AS
    SELECT * FROM meta_recommendation_us WHERE is_valid = 1
    """)

    cur.execute("""
    CREATE OR REPLACE VIEW meta_recommendation_latest_us AS
    SELECT srl.* FROM meta_recommendation_us srl
    INNER JOIN (
        SELECT trade_date, MAX(created_at) AS latest_ts
        FROM meta_recommendation_us GROUP BY trade_date
    ) latest ON srl.trade_date = latest.trade_date
        AND srl.created_at = latest.latest_ts
    """)

    cur.close()
