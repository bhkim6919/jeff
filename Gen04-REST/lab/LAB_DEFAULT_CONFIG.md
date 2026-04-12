# LAB_DEFAULT_CONFIG — 기본값 (변경 가능)

이 문서의 값은 `LabConfig` 기본값이며 CLI 인자로 override 가능.
BASELINE_SPEC과 달리 변경해도 재현성 규칙을 위반하지 않음.

## Capital

- **INITIAL_CASH**: 100,000,000 (1억원)
- **CASH_BUFFER**: 0.95

## Universe Filters

- **UNIV_MIN_CLOSE**: 2,000 (KRW)
- **UNIV_MIN_AMOUNT**: 2,000,000,000 (20일 평균 거래대금)
- **UNIV_MIN_HISTORY**: 260 (거래일)

## Scoring Defaults

- **VOL_LOOKBACK**: 252
- **VOL_PERCENTILE**: 0.30
- **MOM_LOOKBACK**: 252
- **MOM_SKIP**: 22
- **N_STOCKS**: 20 (전략별 override 가능)
- **REBAL_DAYS**: 21
- **TRAIL_PCT**: 0.12

## Date Range

- **START_DATE**: 2026-03-01
- **END_DATE**: (최신 데이터)
- **LOOKBACK_DAYS**: 252 (지표 warmup)
