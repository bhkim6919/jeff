-- =============================================================================
-- Q-TRON Crypto Lab — D1 PostgreSQL Schema
-- =============================================================================
-- Owner       : Jeff (final authority)
-- Branch      : feature/crypto-lab-d1-data
-- Phase       : D1 (Data Infrastructure)
-- Reference   : crypto/DESIGN.md §11 PostgreSQL Schema, §4.4 Atomic Write,
--               §5 시간축 (KST 캔들 vs UTC 스냅샷 분리)
-- =============================================================================
--
-- Tables created (3):
--   1. crypto_ohlcv              — Upbit KRW 일봉 OHLCV (canonical truth)
--   2. crypto_listings           — 상장/상폐 추적 (survivorship 방지)
--   3. crypto_universe_top100    — 24h 거래대금 Top 100 snapshot (UTC 기준)
--
-- 핵심 설계 (Jeff G1 보완):
--   #1 snapshot_version 단순화      → universe_top100.snapshot_dt_utc 만 (universe_count 별도 필드 ❌)
--   #2 candle_dt KST/UTC 분리       → crypto_ohlcv 에 candle_dt_kst + candle_dt_utc 두 컬럼
--   #3 atomic write (DB ↔ parquet) → row_checksum BYTEA NOT NULL (SHA256)
--   #4 bulk_fetch checkpoint        → 본 schema 무관 (scripts/crypto/checkpoint.json)
--
-- Idempotent: IF NOT EXISTS + 트랜잭션. 재실행 안전.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. crypto_ohlcv — Upbit KRW 일봉 OHLCV
-- -----------------------------------------------------------------------------
-- PRIMARY KEY  : (pair, candle_dt_kst) — Upbit 일봉의 KST 거래일이 자연 키
-- KST/UTC 분리 : candle_dt_kst (자연 키) + candle_dt_utc (보조, mismatch 검증)
-- Atomic write : row_checksum 으로 PG ↔ parquet 정합성 검증 (DESIGN.md §4.4)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_ohlcv (
    pair            TEXT        NOT NULL,
    candle_dt_kst   DATE        NOT NULL,
    candle_dt_utc   DATE        NOT NULL,
    open            NUMERIC(20, 8),
    high            NUMERIC(20, 8),
    low             NUMERIC(20, 8),
    close           NUMERIC(20, 8),
    volume          NUMERIC(28, 8),
    value_krw       NUMERIC(28, 2),
    row_checksum    BYTEA       NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_crypto_ohlcv PRIMARY KEY (pair, candle_dt_kst),
    CONSTRAINT chk_crypto_ohlcv_pair_format
        CHECK (pair LIKE 'KRW-%'),
    CONSTRAINT chk_crypto_ohlcv_ohlc_nonneg
        CHECK (
            (open    IS NULL OR open    >= 0) AND
            (high    IS NULL OR high    >= 0) AND
            (low     IS NULL OR low     >= 0) AND
            (close   IS NULL OR close   >= 0) AND
            (volume  IS NULL OR volume  >= 0) AND
            (value_krw IS NULL OR value_krw >= 0)
        ),
    CONSTRAINT chk_crypto_ohlcv_high_low
        CHECK (high IS NULL OR low IS NULL OR high >= low),
    CONSTRAINT chk_crypto_ohlcv_checksum_size
        CHECK (octet_length(row_checksum) = 32)        -- SHA256 = 32 bytes
);

CREATE INDEX IF NOT EXISTS idx_crypto_ohlcv_pair_dt_kst
    ON crypto_ohlcv (pair, candle_dt_kst DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_ohlcv_pair_dt_utc
    ON crypto_ohlcv (pair, candle_dt_utc DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_ohlcv_dt_kst
    ON crypto_ohlcv (candle_dt_kst);

COMMENT ON TABLE crypto_ohlcv IS
    'Crypto Lab D1: Upbit KRW spot 일봉 OHLCV. Canonical truth. DESIGN.md §11 참조.';
COMMENT ON COLUMN crypto_ohlcv.pair IS
    '거래쌍 (예: KRW-BTC). KRW 마켓 only.';
COMMENT ON COLUMN crypto_ohlcv.candle_dt_kst IS
    'Upbit 응답 candle_date_time_kst 의 DATE 부분 (자연 키, PRIMARY KEY). 2026-04-27 S4 가설 B 확정: UTC trade day = KST 09:00~익일 09:00. DESIGN.md §5.2.';
COMMENT ON COLUMN crypto_ohlcv.candle_dt_utc IS
    'Upbit 응답 candle_date_time_utc 의 DATE 부분 (보조). D1 daily 단계에서는 candle_dt_kst 와 항상 동치 (불변량, PASS #13).';
COMMENT ON COLUMN crypto_ohlcv.value_krw IS
    '거래대금 (KRW). Universe Top 100 산출 입력.';
COMMENT ON COLUMN crypto_ohlcv.row_checksum IS
    'SHA256(pair||candle_dt_kst||open||high||low||close||volume||value_krw). Atomic write 검증 (PASS #11).';
COMMENT ON COLUMN crypto_ohlcv.fetched_at IS
    'Upbit fetch 완료 시각. Phase 2 incremental 에서 staleness 판정에 사용.';


-- -----------------------------------------------------------------------------
-- 2. crypto_listings — 상장/상폐 추적 (survivorship bias 방지)
-- -----------------------------------------------------------------------------
-- delisted_at NULL = active. D1 PASS #5 = ≥ 30 종목 등록.
-- D2 부터 Upbit notice 자동 크롤로 source='auto_diff' 추가 예정.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_listings (
    pair              TEXT        PRIMARY KEY,
    symbol            TEXT        NOT NULL,
    listed_at         DATE,
    delisted_at       DATE,
    delisting_reason  TEXT,
    source            TEXT        NOT NULL,
    notes             TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_crypto_listings_pair_format
        CHECK (pair LIKE 'KRW-%'),
    CONSTRAINT chk_crypto_listings_source
        CHECK (source IN ('upbit_notice', 'manual_v0', 'auto_diff')),
    CONSTRAINT chk_crypto_listings_dates
        CHECK (
            delisted_at IS NULL OR listed_at IS NULL
            OR delisted_at >= listed_at
        )
);

CREATE INDEX IF NOT EXISTS idx_crypto_listings_active
    ON crypto_listings (pair) WHERE delisted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_crypto_listings_delisted
    ON crypto_listings (delisted_at) WHERE delisted_at IS NOT NULL;

COMMENT ON TABLE crypto_listings IS
    'Crypto Lab D1: 상장/상폐 추적. Survivorship bias 방지 (DESIGN.md §2.2). delisted_at NULL = active.';
COMMENT ON COLUMN crypto_listings.source IS
    '데이터 출처: manual_v0 (D1 수동), upbit_notice (D2 단발 크롤), auto_diff (D3+ cron diff).';
COMMENT ON COLUMN crypto_listings.delisting_reason IS
    '상폐 사유 (예: 유의종목 → 거래지원 종료, 자체 사유 등).';


-- -----------------------------------------------------------------------------
-- 3. crypto_universe_top100 — 24h 거래대금 Top 100 snapshot
-- -----------------------------------------------------------------------------
-- snapshot_dt_utc = UTC 기준 (DESIGN.md §5.3 snapshot_version 정합).
-- D1 단계는 1회 snapshot, Phase 2 부터 일자별 누적.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_universe_top100 (
    snapshot_dt_utc   DATE        NOT NULL,
    rank              INT         NOT NULL,
    pair              TEXT        NOT NULL,
    value_krw_24h     NUMERIC(28, 2),
    captured_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_crypto_universe_top100 PRIMARY KEY (snapshot_dt_utc, rank),
    CONSTRAINT chk_crypto_universe_rank
        CHECK (rank BETWEEN 1 AND 100),
    CONSTRAINT chk_crypto_universe_pair_format
        CHECK (pair LIKE 'KRW-%'),
    CONSTRAINT chk_crypto_universe_value_nonneg
        CHECK (value_krw_24h IS NULL OR value_krw_24h >= 0)
);

-- 동일 snapshot 안에서 같은 pair 가 두 rank 에 등장 방지
CREATE UNIQUE INDEX IF NOT EXISTS uniq_crypto_universe_dt_pair
    ON crypto_universe_top100 (snapshot_dt_utc, pair);

COMMENT ON TABLE crypto_universe_top100 IS
    'Crypto Lab D1: KRW 마켓 24h 거래대금 Top 100 snapshot (UTC 기준). DESIGN.md §5.3 snapshot_version 과 정합.';
COMMENT ON COLUMN crypto_universe_top100.snapshot_dt_utc IS
    'snapshot 산출 UTC 일자 (snapshot_version 의 첫 필드와 동일).';
COMMENT ON COLUMN crypto_universe_top100.value_krw_24h IS
    '캡처 시점 24h 누적 거래대금 (KRW). rank 결정 기준.';

COMMIT;


-- =============================================================================
-- 검증 쿼리 (schema apply 후 수동 실행)
-- =============================================================================
-- 1. 테이블 존재 확인 (3개)
--    SELECT tablename FROM pg_tables
--    WHERE schemaname='public' AND tablename LIKE 'crypto_%'
--    ORDER BY tablename;
--    Expected: crypto_listings, crypto_ohlcv, crypto_universe_top100
--
-- 2. 컬럼 검증 (DESIGN.md §11 정합)
--    SELECT column_name, data_type, is_nullable
--    FROM information_schema.columns
--    WHERE table_schema='public' AND table_name='crypto_ohlcv'
--    ORDER BY ordinal_position;
--    Expected: candle_dt_kst (DATE NOT NULL), candle_dt_utc (DATE NOT NULL),
--              row_checksum (BYTEA NOT NULL) 모두 존재
--
-- 3. 제약 조건 검증
--    SELECT conname, contype FROM pg_constraint
--    WHERE conrelid = 'crypto_ohlcv'::regclass;
--    Expected: pk_crypto_ohlcv, chk_crypto_ohlcv_* (5개)
--
-- 4. 인덱스 검증
--    SELECT indexname FROM pg_indexes
--    WHERE schemaname='public' AND tablename LIKE 'crypto_%'
--    ORDER BY indexname;
-- =============================================================================
