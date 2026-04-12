# LAB_BASELINE_SPEC — 검증 기준 (재현성 규칙)

이 문서는 Lab 시뮬레이션의 **재현성**을 보장하는 고정 규칙이다.
변경 시 validation 결과가 달라지므로 변경 금지 (별도 버전 관리).

## 체결 규칙

- **Signal timing**: EOD (종가 기준)
- **Fill timing (기본)**: NEXT_OPEN — T-1 close 신호 → T open 체결
- **Fill timing (실험)**: SAME_DAY_CLOSE — `--experimental-same-day` 플래그 필요
- **Partial fill**: 없음 (v1)
- **Execution order**: SELL 먼저 → BUY 나중 (sell-then-buy)

## 비용 모델

- **BUY_COST**: 0.00115 (fee 0.015% + slippage 0.10%)
- **SELL_COST**: 0.00295 (fee 0.015% + slippage 0.10% + tax 0.18%)
- **Entry cost**: `entry_price * (1 + BUY_COST)`
- **Exit proceeds**: `qty * exit_price * (1 - SELL_COST)`
- **PnL**: `(net_proceeds - invested) / invested`
  - `invested = qty * entry_price + buy_cost_total`

## Quantity 규칙

- **Qty**: `floor(available / buy_cost_total)`
- **Price rounding**: 없음 (float)
- **Cash buffer**: `equity * CASH_BUFFER (0.95)`

## Ranking 규칙

- **Tie-break**: ticker 오름차순 (sorted)
- **Buy queue ordering**: signal priority → ticker 정렬
- **Positive momentum only**: momentum <= 0 인 종목 제외 (LowVol/Mom 전략)

## 데이터 처리

- **Close**: ffill (MTM continuity)
- **Open/High/Low**: NO ffill — NaN = 거래 불가
- **Volume**: fillna(0)
- **Suspended (open=NaN)**: 해당일 매수 불가
- **Delisted / all-NaN row**: skip
- **Duplicate ticker/date**: last 우선
- **Partial history**: `min_history` 미달 → universe 제외
- **Split/merge adjusted**: 미적용 (raw price 사용)
- **close=0**: universe에서 자동 제외 (c > 0 필터)

## Look-ahead 방어

- **Level 1**: `close_matrix.shape[0] == day_idx + 1` assert
- **Level 2**: 지표 계산 시 `safe_slice(matrix, day_idx)` — 당일 제외
