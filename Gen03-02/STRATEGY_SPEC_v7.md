# Q-TRON Gen3 v7 Strategy Specification

> 백테스트 이식용 전략 명세. 2026-03-12 기준 실 코드에서 추출.

---

## 1. 전체 파이프라인

```
[배치 (장 마감 후)]
  OHLCV 수집 → 유니버스 필터 → RS Composite 계산 → signals_YYYYMMDD.csv 생성

[런타임 (09:00~15:30)]
  레짐 판단 → RAL 모드 → 청산 점검 → Stage A 진입 → Stage B 진입 → 상태 저장
```

---

## 2. 유니버스 필터

| 조건 | 값 |
|------|---|
| 최소 종가 | 2,000원 |
| 최소 20일 평균 거래대금 | 20억원 |
| 최소 OHLCV 행 수 | 125일 (RS120 계산에 필요) |
| 대상 시장 | KOSPI 전종목 (2,622개 중 필터 통과분) |

---

## 3. 지표 계산

### 3.1 RS (Relative Strength) 수익률

```
rs20_raw  = close[-1] / close[-21] - 1     (20일 수익률)
rs60_raw  = close[-1] / close[-61] - 1     (60일 수익률)
rs120_raw = close[-1] / close[-121] - 1    (120일 수익률)
```

### 3.2 RS 순위 (백분위)

유니버스 전체 종목 대상으로 각 rs_raw를 0~1 백분위 순위 변환:
```
rs20_rank  = rank(rs20_raw,  pct=True)   # 1.0 = 가장 강함
rs60_rank  = rank(rs60_raw,  pct=True)
rs120_rank = rank(rs120_raw, pct=True)
```

### 3.3 RS Composite

```
rs_composite = rs20_rank × 0.30 + rs60_rank × 0.50 + rs120_rank × 0.20
```
- rs20/60/120 중 하나라도 NaN이면 rs_composite = NaN → 시그널 제외

### 3.4 ATR (Average True Range)

- Wilder EMA 방식, period=20
```python
TR[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
ATR[0] = mean(TR[0:20])
ATR[n] = ATR[n-1] × (1 - 1/20) + TR[n] × (1/20)
```

### 3.5 ATR 순위

```
atr_rank = rank(atr20, pct=True)   # 1.0 = 가장 변동성 큼
```

### 3.6 기타 지표

| 지표 | 계산 |
|------|------|
| above_ma20 | close > MA(close, 20) → 1, else 0 |
| is_52w_high | close >= max(high, 252일) × 0.95 → 1 (5% 허용) |
| breakout | close >= max(high[-21:-1]) → 1 (20일 신고가 돌파) |
| gap_pct | (today_open / yesterday_close) - 1 |
| vol_ratio | today_volume / mean(volume[-21:-1]) |
| gap_blocked | gap_pct > 8% AND vol_ratio < 1.3 → 1 |
| pb_score | 52주 고점 대비 3~7% 하락 시 +5점, 그 외 0 |

---

## 4. 시그널 선정

### 4.1 진입 시그널 조건 (signal_entry=1)

3가지 **모두** 충족:
```
breakout == 1           (20일 신고가 돌파)
rs_composite >= 0.80    (RS_ENTRY_MIN)
gap_blocked == 0        (갭 필터 미차단)
```

### 4.2 점수 & 정렬

```
score = rs_composite × 100 + pb_score
```
- signal_entry=1인 종목을 score 내림차순 정렬
- 상위 50개(기본값) 저장

### 4.3 Stage 분류

| 레짐 | Stage A (Early Entry) 조건 | Stage B |
|------|---------------------------|---------|
| BULL | (is_52w_high=1 AND rs_composite≥0.80) OR (breakout=1 AND rs_composite≥0.92) | 나머지 |
| BEAR | 없음 (전부 B) | 전부 |

### 4.4 TP/SL 계산 (시그널 단계)

```
SL = entry - ATR20 × SL_MULT
TP = entry + (entry - SL) × 2.0     (RR = 2.0)
```
| 레짐 | SL_MULT |
|------|---------|
| BULL | 4.0 |
| BEAR | 1.0 |

- SL 최소 거리 1% 미만이면 시그널 제외

---

## 5. 레짐 판단

### 5.1 MA200 기본 레짐
```
KOSPI 종가 > MA(KOSPI, 200) → BULL
KOSPI 종가 ≤ MA(KOSPI, 200) → BEAR
```
- KOSPI 지수 대신 KODEX 200 ETF(069500) 종가 사용 (프록시)

### 5.2 Breadth 보완
```
breadth = (MA20 상회 종목 수) / (유니버스 전체 종목 수)

MA200 BULL이지만 breadth < 0.35 → BEAR 강제 전환
```

### 5.3 REGIME_FLIP_GATE = 2

레짐이 전환될 때 2일 연속 같은 방향이어야 실제 전환. 노이즈 방지.

---

## 6. RAL (Runtime Adaptive Layer)

전일 KOSPI 지수 수익률 기준:

| 모드 | 조건 | 효과 |
|------|------|------|
| CRASH | idx_ret < -2.0% | SL 강화 + 신규 진입 전량 차단 + RS<0.45 강제청산 |
| SURGE | idx_ret > +1.5% | Trailing Stop SL 완화 (SL - 0.5×ATR) |
| NORMAL | 그 외 | 기본 동작 |

### CRASH SL 강화
```
new_sl = avg_price - (ATR_MULT_BEAR × RAL_CRASH_SL_MULT) × ATR
       = avg_price - (1.0 × 0.60) × ATR
       = avg_price - 0.60 × ATR
pos.sl = max(현재_sl, new_sl)   # 더 타이트한 쪽 적용
```

### CRASH 강제청산
```
보유 중 rs_composite < 0.45 → 즉시 청산
```

### SURGE Trailing Stop 완화
```
new_sl = pos.sl - 0.50 × ATR    # SL을 더 느슨하게
pos.sl = min(현재_sl, new_sl)
```

---

## 7. 청산 로직 (우선순위 순)

| 순위 | 유형 | 조건 | 비고 |
|------|------|------|------|
| 1 | **SL** | current_price ≤ pos.sl | 최우선 |
| 2 | **RAL_CRASH** | ral_mode=="CRASH" AND rs_composite < 0.45 | signals에 없으면 청산 안 함 |
| 3 | **RS_EXIT** | 월초(1~7일) AND rs_composite < 0.40 | 월 1회 정리 |
| 4 | **MAX_HOLD** | held_days ≥ 60 | 달력일 기준 |

- TP 청산 **없음** (v7에서 제거 — 추세추종 설계)
- MA20 청산 **없음** (v7에서 제거 — RS 청산으로 대체)

---

## 8. 진입 로직

### 8.1 Stage A — Early Entry (BULL 레짐 한정)

| 항목 | 값 |
|------|---|
| 조건 | regime == BULL |
| 대상 | stage=A 시그널 (is_52w_high=1 AND rs_composite≥0.80) |
| 최대 Early 동시 보유 | MAX_EARLY = 3 |
| 활성 섹터 수 최소 | SECTOR_DIVERSITY_MIN = 3 (시그널 내 고유 섹터) |
| 동일 섹터 Early 한도 | SECTOR_CAP_EARLY = 1 |
| ATR 순위 상한 | atr_rank < 80%ile (ATR_STAGE_A) |
| 갭 필터 | 현재가 > entry × 1.08 → 스킵 |
| 비중 | 총자산 × 5% (EARLY_WEIGHT) |

### 8.2 Stage B — Main Strategy

| 항목 | BULL | BEAR |
|------|------|------|
| 최대 동시 포지션 | MAX_POS_BULL = 20 | MAX_POS_BEAR = 8 |
| ATR 순위 상한 | atr_rank < 70%ile | atr_rank < 40%ile |
| 최소 RS | 없음 | rs_composite ≥ 0.90 (BEAR_RS_MIN) |
| 동일 섹터 종목 한도 | SECTOR_CAP_TOTAL = 4 (기타 섹터는 ×2 = 8) |
| 섹터 노출도 한도 | SECTOR_MAX_PCT = 20% |
| 비중 | 총자산 × 7% | 총자산 × 5% |

### 8.3 공통 진입 조건

- 이미 보유 중인 종목 → 스킵
- 당일 이미 진입한 종목 → 스킵 (재시작 시 중복 방지)
- 총 노출도 > 95% → 거부
- 종목당 비중 > 10% → 거부

---

## 9. 포지션 사이징

```
per_pos_amount = equity × weight
shares = min(per_pos_amount, cash) // current_price
```

| 진입 유형 | weight |
|-----------|--------|
| Early (Stage A) | 5% |
| Main BULL (Stage B) | 7% |
| Main BEAR (Stage B) | 5% |

### TP/SL 런타임 재계산

시그널의 TP/SL이 유효하지 않거나, 갭업으로 현재가 > 시그널 entry 시:
```
SL = current_price - ATR × SL_MULT
TP = current_price + (current_price - SL) × 2.0
```
- TP ≤ current_price 또는 SL ≤ 0 → 진입 스킵

---

## 10. 리스크 관리 (6중 게이트)

### 10.1 포트폴리오 수준

| 게이트 | 조건 | 액션 |
|--------|------|------|
| HARD_STOP | 월간 DD < -7% | 전 포지션 강제 청산 |
| DAILY_KILL | 일간 DD < -4% | 신규 진입 완전 차단 (포지션 유지) |
| SOFT_STOP | 일간 DD < -2% | 손실 최대 1개 청산 + 신규 진입 중단 |

### 10.2 개별 주문 게이트 (can_enter)

```
1. 일일 손실 한도 (-2%) 초과 → 거부
2. 월간 DD 한도 (-7%) 초과 → 거부
3. 최대 보유 종목 수 (BULL=20, BEAR=8) 초과 → 거부
4. 종목당 최대 비중 (10%) 초과 → 거부
5. 섹터 노출 한도 (30%) 초과 → 거부
6. 총 노출도 (95%) 초과 → 거부
```

### 10.3 MarginState 전파

Stage A에서 증거금 부족 감지 시 → Stage B 전체 스킵 (cycle 단위)

---

## 11. 비용 모델

| 항목 | 값 |
|------|---|
| 수수료 (편도) | 0.015% |
| 슬리피지 | 0.10% (기본) |
| 세금 (매도) | 0.18% |

### 슬리피지 상세 모델 (paper trading)

| 5일 평균 거래대금 | 기본 | 상한 |
|------------------|------|------|
| 200억+ (대형주) | 0.3% | 0.7% |
| 50~200억 (중형주) | 0.7% | 1.5% |
| 50억 미만 (소형주) | 1.5% | 3.0% |

```
liquidity_penalty = (주문금액 / 일거래대금) × 10%
slippage = min(base + liquidity_penalty, cap)
```

---

## 12. 전체 파라미터 요약

```python
# 자본
initial_cash        = 100,000,000  # 1억

# 포지션 수
MAX_POS_BULL        = 20
MAX_POS_BEAR        = 8
MAX_EARLY           = 3

# 비중
EARLY_WEIGHT        = 0.05   # 5%
MAIN_WEIGHT_BULL    = 0.07   # 7%
MAIN_WEIGHT_BEAR    = 0.05   # 5%

# 리스크
daily_loss_limit    = -0.02  # SOFT_STOP
daily_kill_limit    = -0.04  # DAILY_KILL
monthly_dd_limit    = -0.07  # HARD_STOP
max_exposure        = 0.95
max_per_stock       = 0.10
max_sector_exp      = 0.30

# ATR 배수 (SL 계산)
ATR_MULT_BULL       = 4.0
ATR_MULT_BEAR       = 1.0

# ATR 순위 상한
ATR_STAGE_A         = 80     # Early ATR 상한 (80%ile)
ATR_STAGE_B         = 70     # Main BULL ATR 상한 (70%ile)
ATR_BEAR_MAX        = 40     # Main BEAR ATR 상한 (40%ile)

# 레짐
REGIME_MA           = 200
BREADTH_BEAR_THRESH = 0.35
BREADTH_BULL_THRESH = 0.55
REGIME_FLIP_GATE    = 2

# RAL
RAL_CRASH_THRESH    = -0.020
RAL_SURGE_THRESH    = +0.015
RAL_CRASH_CLOSE_RS  = 0.45
RAL_CRASH_SL_MULT   = 0.60
RAL_SURGE_TS_RELAX  = 0.50

# RS 기반
RS_ENTRY_MIN        = 0.80
RS_EXIT_THRESH      = 0.40
BEAR_RS_MIN         = 0.90

# 진입 필터
GAP_THRESH          = 0.08
GAP_VOL_MIN         = 1.30
SECTOR_DIVERSITY_MIN = 3

# 섹터 한도
SECTOR_CAP_TOTAL    = 4
SECTOR_CAP_EARLY    = 1
SECTOR_MAX_PCT      = 0.20

# 청산
MAX_HOLD_DAYS       = 60    # 달력일

# 비용
FEE                 = 0.00015
SLIPPAGE            = 0.001
TAX                 = 0.0018
```

---

## 13. 백테스트 이식 시 주의사항

1. **RS Composite는 cross-sectional 순위** — 날짜별로 유니버스 전체 종목의 순위를 매겨야 함. 개별 종목 단독 계산 불가.
2. **레짐은 look-ahead 금지** — 오늘 레짐 판단에 오늘 종가 사용 가능 (장 마감 후 배치). 내일 런타임에서 적용.
3. **RAL은 전일 수익률** — 오늘 런타임에서 어제 지수 수익률로 판단. look-ahead 없음.
4. **TP 청산 없음** — v7에서 의도적으로 제거. 추세추종 설계이므로 SL만 사용.
5. **월초 RS 청산** — 매월 1~7일에만 rs_composite < 0.40 종목 청산.
6. **슬리피지** — 거래대금 기반 모델 사용. 단순 고정 슬리피지(0.1%)로 대체해도 무방.
7. **섹터 한도는 종목수 + 금액비율 이중** — SECTOR_CAP_TOTAL(4개) AND SECTOR_MAX_PCT(20%) 모두 체크.
8. **Stage A는 BULL에서만** — BEAR/SIDEWAYS 레짐에서는 Early Entry 없음.
9. **갭업 시 TP/SL 재계산** — 시그널 entry 대비 현재가가 8%+ 갭업이면 런타임에서 현재가 기준으로 TP/SL 재산정.
10. **held_days = 달력일** — 거래일이 아닌 calendar days. MAX_HOLD_DAYS=60 달력일 ≈ 42 거래일.
