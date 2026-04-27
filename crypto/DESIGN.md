# Q-TRON Crypto Lab — Design Document

**Status**: D1 (Data Infrastructure) — In progress
**Branch**: `feature/crypto-lab-d1-data`
**Worktree**: `C:/Q-TRON-32_ARCHIVE-crypto-d1` (격리)
**Last updated**: 2026-04-27
**Owner**: Jeff (final authority) / Claude Code (implementation)

---

## 1. 목적 (Why)

Q-TRON 을 "자동매매 시스템" → "전략 검증 플랫폼" 으로 확장하기 위한 Crypto Lab 모듈.

KR Gen3 의 -57.8% / MDD -61.9% 라이브 사고의 직접 원인은 **백테스트 게이트 부재**였음. 크립토는 변동성 3~5x → 게이트 없이 진입 시 손실 비대칭. KR Gen4 가 +208% 백테스트 검증 후 라이브 진입한 패턴을 그대로 따른다.

**Crypto = 엔진 개발 대상이 아니라 검증 대상.** D1~D7 의 산출물은 모두 GO/NO-GO 의사결정을 위한 게이트 자료.

---

## 2. 범위 (Scope)

### 2.1 Crypto Lab 전체 (Phase 1~7)

| Phase | 산출물 | 게이트 |
|---|---|---|
| Phase 1 (D1) ✅ | Data 인프라 (OHLCV bulk + PG schema + parquet fallback + listings v0 + quality report) | G3 PASS (2026-04-27, PR #11 merged) |
| **Phase 2 D2** ✅ | **Listings 자동 크롤 (Upbit `/v1/announcements`) + fill-in-the-blanks UPSERT** | **G4 PASS (2026-04-27)** |
| Phase 2 D3 | Incremental cron + DB ↔ CSV 정합 | 미착수 |
| Phase 3 (D4) | Backtester (cost model 단일, matrix ffill 금지) + 전략 3개 우선 | G5 |
| Phase 4 (D5) | 전략 9개 + Defensive overlay + 동시 비교 | G6 |
| Phase 5 (D6) | Paper Simulation engine (backtest 와 동일 엔진) | G7 |
| Phase 6 (D7) | Walk-forward + slippage stress + survivorship 검증 | G8 |
| Phase 7 (D8) | UI (포트 8082) + Crypto Lab 대시보드 | G9 |
| Phase 8 (D9) | GO/NO-GO 의사결정 (Jeff + JUG) | — |

### 2.2 D1 In-scope (이번 단계 한정)

- [x] Upbit Quotation REST API 클라이언트 (`crypto/data/upbit_provider.py`)
- [x] PostgreSQL schema 3 테이블 (`crypto/db/schema.sql`)
- [x] PG R/W + parquet fallback (`crypto/db/repository.py`)
- [x] KRW Top 100 universe 결정 로직
- [x] 2018-01-01 ~ 현재 일봉 bulk fetch (BTC/ETH 선검증 후 Top 98)
- [x] 데이터 품질 검증 (gap / coverage / duplicate / outlier)
- [x] `listings.csv` v0 (수동 큐레이션 ≥ 30 종목)
- [x] `quality_report.html`

### 2.3 D1 Out-of-scope (절대 금지)

- ❌ Upbit Exchange API (실거래)
- ❌ 주문 기능 (orders, cancel, market/limit)
- ❌ 잔고 조회 (accounts, balance)
- ❌ KR/US engine 수정 (`kr/`, `us/` 폴더 touch 0)
- ❌ Crypto live engine (Phase 8 GO 이후 검토)
- ❌ Backtester / 전략 / Simulation (Phase 3~5)
- ❌ Spread 검증 (호가 데이터 필요, D2~D3 이후)
- ❌ Listings 자동 크롤 (D2)
- ❌ Funding rate / derivatives / basis (Phase 7 이후)

---

## 3. 격리 원칙 (Isolation)

### 3.1 KR/US 와 import 0

- `crypto/` 내부 어떤 파일도 `from kr.` 또는 `from us.` import 금지.
- 공유 가능한 것: `shared/db/pg_base.py` 만 (PostgreSQL 단일 접근 계층).
- D1 PASS 검증 항목 #7: `grep -r "from kr\." crypto/` 결과 = 0건.

### 3.2 디렉토리 격리

- KR/US 운영 디렉토리: `C:/Q-TRON-32_ARCHIVE/`
- Crypto Lab 디렉토리: `C:/Q-TRON-32_ARCHIVE-crypto-d1/` (git worktree)
- 두 디렉토리는 git 만 공유, working tree 완전 분리.

### 3.3 포트 격리

- KR Lab Live: `:8080`
- US Lab: `:8081`
- Crypto Lab UI: `:8082` (Phase 5 진입 시 활성, D1 단계에서는 미사용)

---

## 4. 데이터 계층 (Data Layer)

### 4.1 데이터 소스

| 항목 | 값 |
|---|---|
| Provider | Upbit Quotation REST API (공개 시세) |
| Endpoint | `https://api.upbit.com/v1/candles/days` 등 |
| 인증 | 불필요 (Quotation 은 public) |
| Rate limit | 10 req/sec, 600 req/min (REST 기준) |
| 시장 | KRW spot only (BTC, USDT 마켓 ❌) |
| 타임프레임 | 일봉 (`/v1/candles/days`) — D1 단일 |

### 4.2 Universe (Top 100)

- 기준: **24h 누적 거래대금 (KRW)** 내림차순 Top 100
- 결정 시점: D1 작업 시작일 (2026-04-27 기준 Top 100 snapshot)
- 정적 리스트로 저장 (`crypto/data/universe_top100.csv`), D2 이후 동적 재계산 검토
- 신규 상장 / 상장 폐지로 Top 100 구성 변동 시 → `listings.csv` 와 교차 검증

### 4.3 Storage (Dual-write)

| Store | 역할 | 정합성 |
|---|---|---|
| PostgreSQL `crypto_ohlcv` | Canonical truth (서빙) | 마스터 |
| Parquet `crypto/data/ohlcv/*.parquet` | Performance cache + fallback | PG 와 row 일치 필수 |
| `listings.csv` | Survivorship bias 방지 | 수동 v0 → D2 자동화 |
| `universe_top100.csv` | 백테스트 universe 입력 | 정적 (D1) |

KR R7 (2026-04-23) 결정 패턴 차용: **DB = canonical, file = cache**. D1 PASS 기준 #4 = PG ↔ parquet row mismatch 0.

### 4.4 Atomic Write 프로토콜 (DB ↔ parquet)

Jeff G1 보완사항 #3 — bulk fetch 도중 race / partial write 발생 시 DB 만 write 또는 parquet 만 write 되어 mismatch 가 누적되는 시나리오 차단.

**페어 단위 atomic write 규칙** (`crypto/db/repository.py::write_pair_ohlcv`):

```
1. Upbit 응답 수신 → 메모리 DataFrame 구성
2. SHA256 checksum 산출 (df 의 정렬된 row tuple 해시)
3. 임시 parquet 작성: {pair}.parquet.tmp (디스크 flush + fsync)
4. PG 트랜잭션 시작
5. PG INSERT ... ON CONFLICT DO UPDATE (페어 단위 upsert)
6. PG SELECT 로 동일 페어의 row checksum 재산출
7. parquet checksum == PG checksum 검증
   - mismatch → PG ROLLBACK + tmp parquet 삭제 + fail (재시도 X, 명시적 인지 필요)
8. PG COMMIT
9. parquet rename: {pair}.parquet.tmp → {pair}.parquet (atomic move)
10. 실패 시 cleanup: tmp 파일 잔존 0
```

**보장 사항**:
- (a) 페어 단위로 PG 와 parquet 가 동시 갱신되거나 둘 다 갱신되지 않음
- (b) 중간 프로세스 kill 발생 시: tmp parquet 만 잔존 (재시작 시 cleanup), PG 상태 무관
- (c) checksum mismatch = 즉시 fail = D1 PASS 기준 #4 (mismatch=0) 보장

**검증 항목 (D1 PASS #10)**: 동일 페어 bulk fetch 2회 실행 → DB row count + parquet row count + checksum 모두 동일.

### 4.5 NaN ffill 금지

KR Gen4 의 비용 모델 사고 (validate_gen4 +472% vs backtest_gen4_core +28.9%) 의 핵심 원인 중 하나가 **매트릭스 ffill** 이었음. Crypto 에서는 D1 부터 차단:

- `crypto_ohlcv` 에 NaN 보존 (forward fill ❌)
- 백테스트 단계 (Phase 3) 에서도 페어별 개별 조회, 매트릭스 ffill 금지
- 비용 모델 단일 함수 (`crypto/backtest/cost_model.py`) 에서 호출

---

## 5. 시간축 (Time Axes) — KST 캔들 vs UTC 스냅샷 분리

KR snapshot_version (`{trade_date}:...`) 은 24/7 시장에 부적합. 또한 Upbit 일봉의 KST 거래일 경계와 UTC 스냅샷 시각을 **명시적으로 분리**해야 backtest 왜곡을 막을 수 있음 (Jeff G1 보완사항 #2).

### 5.1 두 개의 시간축 정의

| 축 | 의미 | 사용처 | 저장 필드 |
|---|---|---|---|
| **`candle_dt_kst`** | Upbit 응답 `candle_date_time_kst` 의 DATE 부분 | OHLCV 행의 자연 키 (PRIMARY KEY) | `crypto_ohlcv.candle_dt_kst` |
| **`candle_dt_utc`** | Upbit 응답 `candle_date_time_utc` 의 DATE 부분 | mismatch 검증 (D1 PASS #13) | `crypto_ohlcv.candle_dt_utc` |
| **`snapshot_dt_utc`** | snapshot 산출 시점 UTC 일자 | snapshot_version 산출, idempotency | per-snapshot metadata |

**원칙 (S4 가설 B 확정 후)**:

- D1 daily 단계에서 `candle_dt_kst` ≡ `candle_dt_utc` (불변량). 두 컬럼을 모두 저장하는 이유: **불변량 위반 감지** = 데이터 품질 알람 (D1 PASS #13).
- OHLCV 자연 키 = `candle_dt_kst` (사용자 친숙성). 변경 시 Phase 7 hourly 진입 단계에서 재검토.
- 스냅샷 (Phase 3+) 은 UTC 일자 기준 산출. 24/7 시장의 글로벌 EOD 컷오프 = **UTC 00:00 = KST 09:00** (§5.2 가설 B).
- 두 축 간 매핑 (Phase 3+): 스냅샷 `snapshot_dt_utc=D` 사용 가능 데이터 = `candle_dt_utc ≤ D-1` (전일까지 마감된 캔들 only).

### 5.2 Upbit 일봉 boundary 확정 (S4 검증 결과)

**검증 일자**: 2026-04-27 (S4 실행)
**검증 스크립트**: [`scripts/crypto/verify_upbit_boundary.py`](../scripts/crypto/verify_upbit_boundary.py)
**대상**: KRW-BTC, 100일치 (Upbit `/v1/candles/days`)
**Summary 파일**: [`crypto/data/_verification/upbit_boundary_2026-04-27.json`](data/_verification/upbit_boundary_2026-04-27.json)
**판정**: **가설 B 확정** (PASS, 100/100 candles 일관)

#### 확정 결과

| 항목 | 값 |
|---|---|
| 일봉 시간 범위 | **UTC 00:00 ~ UTC 23:59** (1 UTC trade day = 1 candle) |
| KST 환산 | 동일 candle 의 KST 09:00 ~ 익일 KST 09:00 |
| Upbit 응답 `candle_date_time_utc` | 캔들 시작 UTC 시각, 예: `2026-04-27T00:00:00` |
| Upbit 응답 `candle_date_time_kst` | 동일 시각의 KST 표현, 예: `2026-04-27T09:00:00` |
| KST − UTC 오프셋 | **+9h 고정 (32400s), no DST** — 100/100 일관 |
| `date(candle_date_time_kst)` vs `date(candle_date_time_utc)` | **항상 동일** (시작 KST 시각이 09:00 < 24:00 → 날짜 변경 없음) |

#### 의의 / 운용 규칙

- Upbit 일봉의 자연 키 = **UTC trade day**. D1 daily 단계에서 `candle_dt_kst` 와 `candle_dt_utc` 는 **항상 같은 DATE 값** (Phase 7 hourly 진입 시 분리 의미 발생, 그 시점에 본 절 재검증 필수).
- D1 PASS #13 (mismatch=0) = 본 가설 B 의 **불변량 검증** (페어 단위 inner join row count = 단일 테이블 row count).
- `snapshot_dt_utc` (§5.3) = 그 UTC trade day 자체. **EOD 컷오프 = UTC 00:00 = KST 09:00**.
- 자연 키로 `candle_dt_kst` 를 PRIMARY KEY 에 둔 이유: 사용자/운영자에게 KST 가 친숙하고, daily 단계에서는 두 값이 동치이므로 의미 손실 없음.

#### 기각된 가설

- 가설 A (KST 00:00 ~ 24:00 = UTC 15:00 전일 ~ UTC 15:00 당일) — 실제 응답에서 `candle_date_time_kst` 가 `09:00:00` 이므로 불일치, 기각.
- 가설 C (기타) — 검증 불필요, 가설 B 가 100/100 일관.

### 5.3 snapshot_version 정의 (단순화)

Jeff G1 보완사항 #1 적용: `universe_count` 제거 (matrix_hash 가 이미 universe 변동을 반영).

```
{snapshot_dt_utc}:{bucket}:{provider}:{matrix_hash}
```

| 필드 | 의미 | 예시 |
|---|---|---|
| `snapshot_dt_utc` | 스냅샷 산출 시점 UTC 일자 | `2026-04-27` |
| `bucket` | 타임프레임 식별자 | `daily` (D1), `hourly` (Phase 7+) |
| `provider` | 데이터 출처 | `upbit_krw` |
| `matrix_hash` | OHLCV 매트릭스 SHA256 short (universe 변동 자동 반영) | `8f3c2a1b` |

### 5.4 Idempotency 규칙 (Phase 2+ 적용)

- 동일 `snapshot_dt_utc` + 동일 `snapshot_version` → skip
- 동일 `snapshot_dt_utc` + 다른 `snapshot_version` → 재실행 + 경고 로그 (`[CRYPTO_SNAPSHOT_VERSION_DRIFT]`)
- D1 단계는 idempotency 미적용 (bulk fetch 1회용), Phase 2 incremental 부터 적용
- **검증**: 동일 입력 (universe, candle 범위) → 동일 `snapshot_version` 산출 (deterministic hash) — D1 PASS 기준 #11

---

## 6. 비용 모델 (Cost Model)

D1 단계는 schema 에만 반영, 실제 사용은 Phase 3 백테스터 부터.

| Mode | Maker | Taker | Slippage | 적용 |
|---|---|---|---|---|
| **normal** | 0.05% | 0.05% | 0.0% | Upbit KRW 기본 수수료 (2026-04 기준) |
| **stress** | 0.25% | 0.25% | 거래대금/24h 거래대금 × 0.5 | 보수적 검증용 |

**원칙**:
- `cost_model.py` 단일 모듈, validate / backtest / simulation 동일 함수 호출
- KR Gen4 사고 (두 구현체 간 비용 차이로 +472% vs +28.9%) 재발 방지
- 수수료는 거래소 정책 변경 가능 → config 파일에 외부화, hardcoded 금지

---

## 7. 전략 구성 (참고 — D1 무관)

### 7.1 9 전략 + 1 Overlay (Phase 4 진입 시 구현)

| # | 전략 | 비고 |
|---|---|---|
| 1 | Momentum (12-1) | KR Gen4 인자 차용 |
| 2 | LowVol + Momentum | KR Gen4 메인 전략 (이식 검증 대상) |
| 3 | Trend Following | 이동평균 기반 |
| 4 | Breakout | N일 고점 돌파 |
| 5 | Mean Reversion | RSI / Z-score |
| 6 | Volume Momentum | 거래대금 + 가격 |
| 7 | Volatility Breakout | ATR 기반 |
| 8 | BTC/ETH Core | 코어 자산 집중 |
| 9 | Risk Parity | 변동성 역가중 |
| **+1** | **Defensive Overlay** | **독립 전략 ❌, 모든 전략에 부착되는 risk overlay** |

Defensive overlay 는 KR `exposure_guard.py` 의 DD-block 패턴 차용 (예: 일 -8%, 월 -15% 도달 시 신규 진입 차단, 강제 청산은 trail stop 으로).

---

## 8. D1 PASS 게이트 (G3) — Jeff 확정 기준

### 8.1 데이터 품질 (8개)

| # | 기준 | 임계값 |
|---|---|---|
| 1 | Bulk fetch 성공 페어 수 | ≥ 80 / 100 |
| 2 | Coverage (listing_date 이후 ~ 어제) | ≥ 95% |
| 3 | 페어별 최대 연속 결측일 | ≤ 7일 (신규 상장 직후 제외) |
| 4 | PG ↔ parquet row mismatch | = 0 |
| 5 | `listings.csv` v0 등록 종목 수 | ≥ 30 (≥ 50 은 D2 이동) |
| 6 | `quality_report.html` 생성 | gap / coverage / duplicate / outlier 4섹션 (spread 제외) |
| 7 | `crypto/` 내부 KR/US import | 0건 (`grep -r "from kr\.\|from us\." crypto/`) |
| 8 | 주문/잔고/Exchange API 코드 | 0건 (정적 검증) |

### 8.2 운영 안정성 (5개, Jeff G1 보완) — §4.4 atomic + §13 checkpoint 검증

| # | 기준 | 검증 방법 |
|---|---|---|
| 9 | 동일 bulk fetch 2회 실행 → row count 동일 | 1회 실행 후 row count 기록, 2회 실행 후 비교 (UPSERT idempotency) |
| 10 | 중간 실패 후 재시작 → 데이터 누락 0 | bulk fetch 50% 지점에서 강제 kill → 재시작 → 최종 row count = 단일 실행 row count |
| 11 | DB ↔ parquet checksum 일치 | 페어별 SHA256(row tuple) 비교, 100/100 일치 |
| 12 | `snapshot_version` deterministic | 동일 (universe, candle 범위, provider) 입력 → 동일 hash 산출 (Phase 2+ 적용 대비 D1 단위 테스트 1건) |
| 13 | `candle_dt_kst` ↔ `candle_dt_utc` mismatch 없음 | Upbit 응답의 두 timestamp 필드 사용, 페어별 inner join row count = 단일 테이블 row count |

1개라도 FAIL 시 D1 NO-GO, D2 진입 금지. 총 13개.

---

## 9. PR 분할 전략

| PR | 내용 | 추정 LOC | 게이트 | 머지 의존 |
|---|---|---|---|---|
| PR #1 | scaffolding + DESIGN.md + PG schema | ~350 | G1 | base |
| PR #2 | upbit_provider + repository + universe builder | ~600 | G2 | PR #1 머지 후 |
| PR #3 | bulk fetch + listings v0 + quality report | ~500 | G3 | PR #2 머지 후 |

**수정된 PR 정책 (Jeff 승인)**: PR #1 리뷰 대기 중 PR #2 작업 가능 (별도 커밋), 단 머지 순서는 #1 → #2 → #3 엄수.

---

## 10. 디렉토리 구조

```
C:/Q-TRON-32_ARCHIVE-crypto-d1/   # git worktree
└── crypto/
    ├── DESIGN.md                  # 본 문서
    ├── data/
    │   ├── upbit_provider.py      # Quotation REST 클라이언트 (Exchange API ❌)
    │   ├── universe.py            # Top 100 결정 로직
    │   ├── quality.py             # gap/coverage/duplicate/outlier 검증
    │   ├── universe_top100.csv    # 정적 universe (D1)
    │   ├── listings.csv           # 상폐 종목 v0 (수동, ≥30)
    │   └── ohlcv/                 # parquet fallback cache
    │       ├── KRW-{symbol}.parquet
    │       └── KRW-{symbol}.parquet.tmp  # atomic write 임시 파일 (정상 종료 시 잔존 0)
    ├── db/
    │   ├── schema.sql             # PG 테이블 정의 (3개)
    │   └── repository.py          # PG R/W + parquet sync (§4.4 atomic write)
    └── (Phase 3+ 추가 예정)
        ├── backtest/
        ├── strategies/
        ├── simulation/
        ├── validation/
        └── web/                   # :8082 UI

scripts/crypto/
├── bulk_fetch_d1.py              # 2018-01 ~ 현재 일봉 bulk (§13 checkpoint 적용)
├── bulk_fetch_checkpoint.json    # 진행 상태 저장 (per-pair last_completed_kst)
└── data_quality_report.py        # quality_report.html 생성
```

---

## 11. PostgreSQL Schema (preview, S3 에서 확정)

```sql
CREATE TABLE crypto_ohlcv (
    pair            TEXT      NOT NULL,            -- 'KRW-BTC' 형식
    candle_dt_kst   DATE      NOT NULL,            -- Upbit 일봉 KST 거래일 (자연 키, S4 boundary 확정)
    candle_dt_utc   DATE      NOT NULL,            -- 동일 캔들 UTC 거래일 (보조, mismatch 검증용)
    open            NUMERIC(20, 8),
    high            NUMERIC(20, 8),
    low             NUMERIC(20, 8),
    close           NUMERIC(20, 8),
    volume          NUMERIC(28, 8),                -- 코인 수량
    value_krw       NUMERIC(28, 2),                -- 거래대금 (KRW)
    row_checksum    BYTEA     NOT NULL,            -- SHA256(pair||candle_dt_kst||open||...) for atomic write 검증
    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (pair, candle_dt_kst)
);
CREATE INDEX idx_crypto_ohlcv_pair_dt_kst ON crypto_ohlcv (pair, candle_dt_kst DESC);
CREATE INDEX idx_crypto_ohlcv_pair_dt_utc ON crypto_ohlcv (pair, candle_dt_utc DESC);

CREATE TABLE crypto_listings (
    pair             TEXT      PRIMARY KEY,        -- 'KRW-LUNA' 등
    symbol           TEXT      NOT NULL,           -- 'LUNA'
    listed_at        DATE,
    delisted_at      DATE,                         -- NULL = active
    delisting_reason TEXT,
    source           TEXT,                         -- 'upbit_notice' / 'manual_v0'
    notes            TEXT
);

CREATE TABLE crypto_universe_top100 (
    snapshot_dt_utc   DATE      NOT NULL,          -- UTC 일자 (snapshot_version 과 정합)
    rank              INT       NOT NULL,
    pair              TEXT      NOT NULL,
    value_krw_24h     NUMERIC(28, 2),
    PRIMARY KEY (snapshot_dt_utc, rank)
);
```

**Schema 변경 사항 (Jeff G1 보완)**:
- `candle_dt` → `candle_dt_kst` + `candle_dt_utc` 분리 (§5.1)
- `row_checksum` 필드 신규 (§4.4 atomic write 검증용)
- `crypto_universe_top100.snapshot_dt` → `snapshot_dt_utc` (§5.3 snapshot_version 정합)

(상세 정의 + 인덱스 + 코멘트는 `crypto/db/schema.sql` 에서 확정)

---

## 12. 의사결정 로그

| 날짜 | 결정 | 결정자 | 비고 |
|---|---|---|---|
| 2026-04-27 | Crypto Lab GO 방향 적합 (data + backtest first, live ❌) | Jeff | 본 문서 §1 |
| 2026-04-27 | 포트 8082, snapshot_version UTC bucket, 9+1 전략, Upbit KRW spot, 비용 normal/stress, Quotation REST | Jeff (6 amendments) | §3.3, §5, §7, §6 |
| 2026-04-27 | 일봉 단일, Top 100, 2018-01 시작, worktree 격리 | Jeff (decisions) | §4.2, §3.2 |
| 2026-04-27 | PR 정책 완화 (병렬 작업 OK, 머지 순서 엄수) | Jeff (correction 1) | §9 |
| 2026-04-27 | listings.csv 30개 (D1) / 50개 (D2 이동) | Jeff (correction 2) | §8 #5 |
| 2026-04-27 | Coverage 기준 = listing_date 이후 ≥ 95% | Jeff (correction 3) | §8 #2 |
| 2026-04-27 | Spread 검증 D1 제외 (호가 데이터 필요) | Jeff (correction 4) | §8 #6, §2.3 |
| 2026-04-27 | Bulk fetch BTC/ETH 선검증 후 Top 98 단계적 | Jeff (Q3) | §2.2, S6 |
| 2026-04-27 | Bulk fetch 낮 시간 실행 (별도 worktree, KR/US import 0 전제) | Jeff (Q2) | §3 |
| 2026-04-27 | C 옵션 확정 — main dir 이동 ❌, worktree 유지 | Jeff | §3.2 |
| 2026-04-27 | **G1 조건부 승인** — 4개 보완 후 S3 진행 | Jeff (G1) | §5, §4.4, §8.2, §13 |
| 2026-04-27 | snapshot_version 단순화 (`universe_count` 제거) | Jeff (G1 #1) | §5.3 |
| 2026-04-27 | `candle_dt_kst` (KST) ↔ `snapshot_dt_utc` (UTC) 분리, S4 에서 boundary 확정 | Jeff (G1 #2) | §5.1, §5.2, §11 |
| 2026-04-27 | DB ↔ parquet atomic write 프로토콜 + checksum 검증 | Jeff (G1 #3) | §4.4 |
| 2026-04-27 | bulk_fetch checkpoint 메커니즘 추가 | Jeff (G1 #4) | §13 |
| 2026-04-27 | D1 PASS 기준 13개로 확장 (데이터 8 + 운영 5) | Jeff (G1) | §8.1, §8.2 |
| 2026-04-27 | C 옵션 (worktree 유지) 확정 + sparse-checkout cone mode 적용 (crypto, scripts/crypto, shared/db) | Jeff | §3.2 |
| 2026-04-27 | .venv64 재사용 결정, pyarrow 1개만 추가 예정 (US Live 종료 후) | Jeff | — |
| 2026-04-27 | PR #1 local commit (`23819ebf`), push/PR 보류, S4 별도 커밋 진행 | Jeff (B안) | §9 |
| 2026-04-27 | **S4 PASS — Upbit 일봉 boundary 가설 B 확정** (UTC 00:00~23:59, KST 09:00~익일 09:00, +9h 고정, 100/100 일관) | Claude (S4) + Jeff (검증) | §5.2 |
| 2026-04-27 | D1 완료 — 13/13 PASS, PR #11 master 머지 (`6147be7e`) | Jeff | §2.1 |
| 2026-04-27 | D2 조건부 승인 — 5개 보완 반영 (PASS 분리, partial-write 금지, structure hash, capability test, source priority) | Jeff | §14 |
| 2026-04-27 | **D2 PASS — 자동 크롤 36 events / +30 new + 6 filled / fill ratio 64.8%** | Jeff (검증 대기) | §14 |
| 2026-04-28 | D3 GO — Q1=A(WTS), Q2=A(21~35 전체), Q3=A+(logger 1차 + Telegram best-effort), Q4=B(3 PR), Q5=A(UTC 00:30) | Jeff | §15.1 |
| 2026-04-28 | D3 보완 5조건 — idempotency / lockfile / partial-write 금지 / drift report / Telegram best-effort | Jeff | §15.2 |
| 2026-04-28 | **D3-1 PASS — 5/5 게이트 (G1 idempotency, G2 lockfile, G3 partial-write, G4 drift report, G5 telegram fail-soft)** | Jeff (검증 대기) | §15.4 |

---

## 13. Bulk Fetch Checkpoint 메커니즘 (Jeff G1 보완 #4)

100 페어 × 8년 일봉 ≈ 1,250 API 호출 + atomic write. 중간 실패 (Upbit API 일시 장애 / 네트워크 / 프로세스 kill) 시 처음부터 재시작은 비효율 + rate limit 낭비.

### 13.1 Checkpoint 파일 구조

`scripts/crypto/bulk_fetch_checkpoint.json`:

```json
{
  "started_at": "2026-04-27T13:00:00+09:00",
  "universe_source": "crypto/data/universe_top100.csv@sha256:...",
  "target_start_kst": "2018-01-01",
  "target_end_kst": "2026-04-26",
  "pairs": {
    "KRW-BTC": {
      "status": "completed",
      "last_completed_kst": "2026-04-26",
      "row_count": 3037,
      "row_checksum": "8f3c2a1b...",
      "fetched_at": "2026-04-27T13:05:42+09:00"
    },
    "KRW-ETH": {
      "status": "completed",
      "last_completed_kst": "2026-04-26",
      ...
    },
    "KRW-LUNA": {
      "status": "in_progress",
      "last_completed_kst": "2021-03-01",
      "row_count": 1149
    },
    "KRW-DOGE": {
      "status": "pending"
    }
  }
}
```

### 13.2 재시작 로직

```
on bulk_fetch_d1.py 실행:
1. checkpoint.json 존재 시 로드
2. 각 페어:
   - status="completed" → skip
   - status="in_progress" → last_completed_kst+1 부터 재시작
   - status="pending" 또는 unknown → target_start_kst 부터 fetch
3. atomic write (§4.4) 통과 후 status="completed" + row_checksum 갱신
4. checkpoint 매 페어 완료 후 즉시 저장 (fsync)
```

### 13.3 검증 (D1 PASS #10)

- bulk fetch 50% 지점에서 강제 kill (`Ctrl+C` 또는 `kill -9`)
- 재시작 → 모든 페어 status="completed" 도달
- 최종 row count = 단일 실행 row count (±0)
- 동일 페어 row_checksum = 단일 실행 checksum

### 13.4 안전 장치

- checkpoint 파일 손상 시 자동 백업 (`.checkpoint.json.bak.{timestamp}`) 후 처음부터 재시작
- universe_source SHA256 변경 감지 → 재시작 거부 (universe 변경 = 의도된 새 fetch 가 맞는지 명시적 확인 필요, `--force-restart` 플래그 필요)

---

## 14. D2 Listings 자동 크롤 (Phase 2 D2 — 2026-04-27)

### 14.1 데이터 소스 — Upbit notice API

S2 inspection (2026-04-27) 결과 `upbit.com/service_center/notice` 는 JS-rendered SPA. 직접 호출 가능한 underlying API:

- **List**: `GET https://api-manager.upbit.com/api/v1/announcements?os=web&category=trade&page=N&per_page=20`
- **Detail**: `GET https://api-manager.upbit.com/api/v1/announcements/{id}`

JSON 응답, 인증 불필요, JS 렌더링 우회. Playwright 등 헤드리스 브라우저 의존성 0.

응답 schema SHA256: `crypto/data/_verification/notice_struct_hash_2026-04-27.txt` 에 baseline 저장. 후속 실행 시 schema 변경 감지 alert.

### 14.2 필터 + 파서

- **Title 필터**: `"거래지원 종료"` 키워드 (제외: `"신규 거래지원"`, `"유의종목"`)
- **Symbol 추출**: `\(([A-Z0-9]{2,15})\)\s*거래지원\s*종료` regex (예: `리졸브(RESOLV)` → `RESOLV`)
- **날짜 anchor-based 파싱** (4 포맷 지원):
  | 포맷 | 예시 |
  |---|---|
  | `YYYY-MM-DD` | `2022-05-13` |
  | `YYYY.MM.DD` | `2022.05.13` |
  | `YYYY년 M월 D일` | `2022년 5월 13일` |
  | `YYYY/MM/DD` | `2022/05/13` |
- 우선 anchor (`거래지원 종료 일시/일자/일`) 근처 200자 윈도우 → 못 찾으면 listed_at 이후 가장 이른 날짜
- **KRW 마켓 필터**: body 에 `KRW 마켓`, `원화`, `전 마켓` 토큰 검색 (모두 부재 + BTC/USDT 마켓 명시 → 제외)

### 14.3 D2 PASS 기준 (Jeff 5 보완 반영)

| # | 기준 | 임계값 | 결과 |
|---|---|---|---|
| 1 | 자동 크롤 raw delistings | ≥ 30 | 36 events |
| 2 | listings 총 entries | ≥ 50 (delisted 기준) | 54 |
| 3-1 | delisted candidate 추가량 | ≥ 50 | 54 (확장성 PASS) |
| 3-2 | `delisted_at` 채워진 비율 | ≥ 50% | 64.8% (35/54, 정확성 PASS) |
| 4 | KR/US 코드 / 테이블 touch | 0 | 0 |
| 5 | Exchange API / 주문 / 잔고 코드 | 0 | 0 |
| 6 | `crypto_listings` PG row count | ≥ 250 | 306 |
| 7 | partial write 발생 | 0 | 0 (atomic CSV/PG, 단일 transaction) |

### 14.4 Fill-in-the-blanks UPSERT 정책 (Jeff #5)

크롤 결과를 기존 `crypto_listings` 에 병합할 때:

- 신규 pair → INSERT, source = `upbit_notice`
- 기존 pair, `delisted_at` NULL + 크롤 결과 date 있음 → UPDATE date / reason / source = `upbit_notice`
- 기존 pair, `delisted_at` 이미 채워짐 → **보존** (절대 덮어쓰지 않음)
- 기존 manual_v0 entry 의 `delisting_reason` / 수동 큐레이션 노트 → 보존
- Source priority: `upbit_notice > manual_v0` (NULL 채울 때만 source 갱신)

PG 한 transaction 내에서 INSERT + UPDATE 모두 실행, 실패 시 ROLLBACK → CSV 도 atomic .tmp → rename.

### 14.5 G2 sample verification (Jeff #4)

S4 진입 전 자동 검증:
- **Live**: 페이지 1~2 샘플에서 ≥ 5 dates 파싱 성공
- **Capability**: synthetic 4 포맷 입력 → ≥ 2 포맷 정확 매치 (실제 응답에 없어도 파서 capability 확인)

2026-04-27 결과: live 8/8 (YYYY-MM-DD), capability 4/4 (YYYY-MM-DD, YYYY.MM.DD, YYYY년 M월 D일, YYYY/MM/DD) → PASS.

### 14.6 크롤 범위 제한 (Jeff fallback)

- 최대 20 페이지 (≈ 400 notices). Upbit 전체 35 페이지 archive 일부만 처리 — 시간 / rate limit 안전 우선.
- `--max-pages` CLI 옵션으로 조정. D2 단발 실행 시 default 20 충분.
- 더 오래된 delistings (예: 2018~2020) 은 D3 incremental cron + 별도 backfill 작업으로 추가.

### 14.7 산출물

```
crypto/data/listings_crawler.py                                     (~400 lines)
scripts/crypto/crawl_upbit_notices.py                              (~400 lines)
crypto/data/listings.csv                                           (276 → 306 entries)
crypto/data/_verification/notice_api_probe_2026-04-27.json
crypto/data/_verification/notice_struct_hash_2026-04-27.txt
crypto/data/_verification/listings_crawl_2026-04-27.json
requirements.freeze.before_crypto_d2.txt
```

### 14.8 D3 (Phase 2 잔여)

- Daily incremental cron (스케줄러 + 새 delistings 자동 발견)
- DB ↔ CSV 정합 정기 검증
- Backfill: 더 오래된 delistings (2018~2020) — Upbit notice 가 35 페이지 archive 갖고 있으나 D2 는 20 페이지로 제한
- 별도 PR로 진행

---

## 15. D3 — Daily Incremental + Reconcile + Backfill (2026-04-28~)

### 15.1 Jeff 결정값 (2026-04-28)

| Q | 결정 | 비고 |
|---|---|---|
| Q1 cron 스케줄러 | **Windows Task Scheduler** | KR/US 가 이미 사용 중 |
| Q2 backfill 페이지 | **21~35 전체** | 1회용, 한 번에 끝내기 |
| Q3 mismatch 알람 채널 | **logger + JSON evidence (1차) + Telegram best-effort (2차)** | Telegram 실패는 D3 FAIL 사유 ❌ |
| Q4 PR 분할 | **3 PR** | incremental / reconcile / backfill |
| Q5 cron 실행 시각 | **UTC 00:30 / KST 09:30** | Upbit 새 KST day + 30분 |

### 15.2 D3 보완 조건 (Jeff 추가)

| # | 요구사항 | 구현 |
|---|---|---|
| 1 | **Idempotency** — 같은 notice 재수집 시 row count 불변 | `INSERT ... ON CONFLICT DO NOTHING` + `UPDATE ... WHERE delisted_at IS NULL` (`crypto/data/listings_merge.py`) |
| 2 | **Lock 파일** — 중복 실행 방지 | `crypto/jobs/_lockfile.py::FileLock` (PID + age stale 검출, 2h auto-reclaim) |
| 3 | **Partial-write 금지** | PG 단일 transaction → CSV `.tmp` → fsync → rename (atomic). PG 실패 → CSV unchanged. CSV 실패 (PG commit 후) → CSV `.bak` 복원 |
| 4 | **Drift report** | Evidence JSON `crypto/data/_verification/incremental_listings_<utc-date>.json` — `baseline_before/after` + `diff` 기록 (PR #2 에서 reconcile 강화) |
| 5 | **Telegram best-effort** | `crypto/jobs/_telegram.py::send` — credentials 없으면 skip, network/HTTP 에러 시 logger.warning 후 status string 반환, 절대 raise 안함 |

### 15.3 PR 분할

| PR | 범위 | 산출물 |
|---|---|---|
| **#1 (D3-1)** | `incremental_listings` 잡 + Task Scheduler 등록 | `crypto/jobs/__init__.py`, `_lockfile.py`, `_telegram.py`, `incremental_listings.py`; `crypto/data/listings_merge.py` (D2 헬퍼 추출); `scripts/crypto/run_incremental_listings.py`, `scripts/crypto/verify_incremental_listings.py`, `scripts/crypto/scheduler/{incremental_listings_task.xml,install_*.ps1,README.md}` |
| **#2 (D3-2)** | DB ↔ CSV 정합 reconcile + drift 알람 | `scripts/crypto/reconcile_db_csv.py`, drift report JSON, Telegram 통합 |
| **#3 (D3-3)** | 페이지 21~35 backfill + DESIGN §15 종결 | `scripts/crypto/backfill_old_delistings.py`, listings.csv +N rows, Phase 2 완료 표시 |

### 15.4 D3-1 PASS 게이트 (5개)

`scripts/crypto/verify_incremental_listings.py` 실행 결과 (2026-04-28):

| # | 게이트 | 검증 방법 | 결과 |
|---|---|---|---|
| G1 | Idempotency | 잡 2회 실행 → PG row count 0 변화 + CSV SHA 동일 | **PASS** |
| G2 | Lockfile | 1번째 lock 보유 중 2번째 acquire → `LockHeld` raise + 잡 exit 2 + 정상 release 후 파일 정리 | **PASS** |
| G3 | Partial-write | `pg_apply_delistings` 강제 raise → exit 1 + CSV bytes/SHA 동일 + `.tmp`/`.bak` leak 0 | **PASS** |
| G4 | Drift report | Evidence JSON `baseline_before/after` + `diff` + `pg_apply_stats` 등 13개 키 모두 존재 | **PASS** |
| G5 | Telegram fail-soft | INVALID 토큰으로 `send()` 호출 → `error:http:404` 반환 + raise 0 | **PASS** |

증거: `crypto/data/_verification/d3_baseline_<utc-date>.json`

### 15.5 운영 메타

- **Task name**: `\Q-TRON\crypto-incremental-listings`
- **실행 시각**: 매일 KST 09:30 (= UTC 00:30)
- **실행 단위**: `C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe scripts\crypto\run_incremental_listings.py`
- **Working dir**: `C:\Q-TRON-32_ARCHIVE-crypto-d1` (worktree)
- **MultipleInstancesPolicy**: `IgnoreNew` (lockfile 보강)
- **ExecutionTimeLimit**: 15분 (정상 1초 내 완료, 비정상 시 강제 종료)

### 15.6 후속 (D3-2 / D3-3)

- D3-2: `scripts/crypto/reconcile_db_csv.py` 추가, mismatch 발견 시 drift JSON + Telegram alert
- D3-3: 페이지 21~35 backfill 1회용 스크립트, listings 50+ → 70+ 목표

---

## 16. 참조

- KR CLAUDE.md (`C:/Q-TRON-32_ARCHIVE/CLAUDE.md`) — Engine Protection Rules, Data Quality Rules, Idempotency Rules 패턴 참조
- KR Gen4 비용 모델 사고: MEMORY.md `cost_comparison.md`
- KR R7 (DB = Canonical Truth): `CLAUDE.md` §Data Source 원칙
- Upbit Quotation API 공식 문서 (Jeff 제공 링크)
- Upbit notice API: `https://api-manager.upbit.com/api/v1/announcements` (D2 발견, undocumented public)

---

**현재 상태**: D1 PR #11 merged (`6147be7e`), D2 PR #12 진행 중.
