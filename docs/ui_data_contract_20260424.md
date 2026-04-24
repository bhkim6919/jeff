# UI Data Contract — Q-TRON KR/US Dashboard

**작성일**: 2026-04-24
**상태**: **FROZEN (P0-3)** — IA 실행 전 고정. 이 문서 변경은 IA 승인 체크리스트 재진입 필요.
**기준 문서**: [`ui_unification_ia_20260424.md`](./ui_unification_ia_20260424.md)

---

## 0. 계약의 원칙

1. **Broker = Truth**. Engine/state가 broker와 다르면 broker 기준.
2. **Calc은 하나의 함수**. 같은 값이 두 곳에서 다르게 계산되지 않는다.
3. **Source 명시 필수**. 모든 표시 값은 (source_table, source_column) 또는 (api_endpoint, field_path) 로 1:1 매핑.
4. **Fallback은 명시적**. primary 실패 시 secondary 사용은 코드와 문서 모두에 명시 + 뱃지 표시.
5. **허용 오차 0%**. derived 값이 broker 대비 0%가 아니면 표시 금지 또는 WARN.

---

## 1. Holdings (보유종목)

### 1.1 Schema

| 필드 | 타입 | 의미 | 단위 |
|------|------|------|------|
| `symbol` | string | 종목 코드 | KR: 6자리 숫자 / US: 티커 |
| `qty` | int | 현 보유 수량 | 주 |
| `avg_price` | float | 매입 평균가 | KR: KRW / US: USD |
| `last_price` | float | 현재가 | KR: KRW / US: USD |
| `market_value` | float | 평가금액 = `qty * last_price` | |
| `unrealized_pnl` | float | 평가손익 = `(last_price - avg_price) * qty` | |
| `unrealized_pnl_pct` | float | 평가수익률 = `(last_price / avg_price - 1) * 100` | % |
| `data_quality` | enum | `OK` / `DEGRADED` / `STALE` | |

### 1.2 Source of Truth

| 시장 | Primary | Fallback | Badge |
|------|---------|----------|-------|
| **KR** | `kiwoom_provider.query_balance()` (REST) | `report_daily_positions` 최신 row | `DEGRADED` when fallback |
| **US** | Alpaca `list_positions()` API | `runtime_state_us_paper.json.positions` | `DEGRADED` when fallback |

### 1.3 계산식 규칙

- **`unrealized_pnl_pct`**: 반드시 `(last_price / avg_price - 1) * 100`. NOT `(market_value - cost_basis) / cost_basis`. 두 방식은 수수료 반영 유무로 다를 수 있음 → 전자로 통일 (수수료 제외 순수 가격 변동).
- **`qty=0`** 항목은 Holdings 카드에 표시하지 않음. Rebal 내역에만 표시.
- **`last_price` 소스**: KR/US 각자 market data API. stale > 5분 이면 `STALE` 뱃지.

### 1.4 UI 표시 규약

- 행 정렬: `unrealized_pnl_pct desc` (기본), 옵션으로 `symbol asc`
- 색상: `unrealized_pnl_pct > 0` → 녹색, `< 0` → 빨강, `= 0` → 기본 텍스트
- 표시 자릿수: price 소수점 2자리 (US는 USD, KR은 원 단위 정수 표시)
- qty=0 → UI 숨김 (Rebal 섹션에서만 노출)

---

## 2. Summary (요약 카드)

### 2.1 Schema

| 필드 | 타입 | 의미 |
|------|------|------|
| `equity` | float | 총 자산 = `cash + sum(market_value)` |
| `cash` | float | 현금 (broker available) |
| `buying_power` | float | 매수 가능 금액 (cash 또는 margin 포함, US 한정) |
| `unrealized_pnl` | float | `sum(Holdings.unrealized_pnl)` |
| `realized_pnl_today` | float | 오늘 확정 손익 (체결 거래 기준) |
| `realized_pnl_total` | float | 누적 확정 손익 (시작일부터) |
| `total_pnl` | float | `unrealized_pnl + realized_pnl_total` |
| `fees_taxes_today` | float | 오늘 누적 수수료+세금 |
| `equity_prev` | float | 전일 equity (전일대비 비교용) |

### 2.2 Source of Truth

| 필드 | KR Primary | US Primary |
|------|-----------|-----------|
| `equity` | `kiwoom.query_balance().evaluated_total` | `alpaca.get_account().portfolio_value` |
| `cash` | `kiwoom.query_balance().deposit` | `alpaca.get_account().cash` |
| `buying_power` | (동일, KR은 cash와 동일) | `alpaca.get_account().buying_power` |
| `unrealized_pnl` | Holdings 합산 (§1.3 공식) | 동일 |
| `realized_pnl_today` | `report_trades` where `date=today AND side='SELL' AND status='FILLED'` 집계 | 동일 (DB 테이블 대신 US 로컬) |
| `realized_pnl_total` | `report_equity_log` 누적 delta | 동일 |
| `equity_prev` | `report_equity_log` `WHERE date < today ORDER BY date DESC LIMIT 1` | 동일 |

### 2.3 계산식 규칙

- **`total_pnl = unrealized + realized_total`**. UI 상 "총손익"은 이 값.
- **"전일대비"** = `(equity - equity_prev) / equity_prev * 100`. equity는 broker 기준. **평가손익 전일대비와 혼동 금지** (그건 unrealized만의 delta).
- **수수료+세금**: `report_trades.fees + report_trades.taxes` 당일 합계. KR 세금 = 매도 0.18% (증권거래세). US 세금 = 별도 연말정산 (UI는 미표시).

### 2.4 일관성 체크

- Dashboard Summary의 `equity` == Analytics Equity Curve의 마지막 점 == broker API 반환값.
- 세 값이 다르면 `data_quality=DEGRADED` 뱃지 + 로그.

---

## 3. Rebalance

### 3.1 Schema

| 필드 | 타입 | 의미 |
|------|------|------|
| `snapshot_version` | string | 타겟이 기반으로 하는 배치 snapshot ID |
| `target_date` | date | 타겟 대상 거래일 |
| `target_tickers` | list[string] | 타겟 종목 20개 |
| `new` | list[row] | 신규 편입 (target ∖ holdings) |
| `exit` | list[row] | 제외 예정 (holdings ∖ target) |
| `keep` | list[row] | 유지 (target ∩ holdings) |
| `scores` | dict[symbol→score] | 선정 근거 스코어 |
| `batch_fresh` | bool | 오늘 배치인지 (P0-1 로직) |
| `execute_result` | enum | `READY` / `SUCCESS` / `FAILED` / `REJECTED` / `MARKET_CLOSED` |

### 3.2 Source of Truth

| 시장 | target 소스 (primary) | fallback |
|------|----------------------|----------|
| **KR** | `signals/target_portfolio_{date}.json` | `target_portfolio` PG 테이블 |
| **US** | `us/data/signals/target_portfolio_{date}.json` | Alpaca `last_batch_*` state |

Holdings 소스는 §1.2 동일. `new/exit/keep` 계산은 UI 레벨에서:
```
new  = target_tickers - holdings_symbols
exit = holdings_symbols - target_tickers
keep = target_tickers ∩ holdings_symbols
```

### 3.3 snapshot_version 형식 (시장별 상이)

- **KR**: `"{trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash}"`
  - 예: `2026-04-24:DB:2026-04-24:2770:0f8fb80e7d75b0fa`
- **US**: `"{trade_date}_batch_{epoch}_{phase}"`
  - 예: `2026-04-23_batch_1776996109_POST_CLOSE`

각 시장 Dashboard는 자기 시장 snapshot만 본다. **Unified 페이지에서만** 양쪽 snapshot을 동시 표시하며, 비교 시 `trade_date` 또는 `data_last_date`로만 매칭 (raw version string 비교 금지).

### 3.4 계산식 규칙

- `batch_fresh` 판정: **P0-1 로직 그대로** (ET 16:00 이후 + `last_batch_business_date == business_date` + `snapshot_created_at` ET 날짜 일치).
- `execute_result` 상태머신:
  - `READY` (batch_fresh + market OK) → 실행 가능
  - `MARKET_CLOSED` → 장 오픈 대기
  - `REJECTED` (+reason) → 가드 차단 (ALREADY_EXECUTED_TODAY / SAME_SNAPSHOT / BATCH_NOT_FRESH)
  - `SUCCESS` / `FAILED` → 최종 결과

### 3.5 주문 수량 계산 (버그 방지)

**AAPL x-1 REJECTED 사고 재발 방지**:
```
qty_delta = target_qty - current_qty
if qty_delta > 0: side=BUY,  qty=abs(qty_delta)
if qty_delta < 0: side=SELL, qty=abs(qty_delta)   # ← abs() 필수
if qty_delta == 0: skip
```
Alpaca/Kiwoom API에 **음수 qty 전달 금지**. 주문 제출 직전 레이어에서 검증.

---

## 4. Regime (레짐)

### 4.1 Schema

| 필드 | 타입 | 의미 |
|------|------|------|
| `today` | object | 오늘 레짐 관측값 |
| `tomorrow` | object | 내일 예측값 |
| → `label` | enum | `STRONG_BULL` / `BULL` / `NEUTRAL` / `BEAR` / `STRONG_BEAR` |
| → `regime` | int | 1~5 (숫자 등급) |
| → `confidence` | float | 0.0~1.0 |
| → `computed_at` | datetime | 계산 시점 |
| → `market_fit` | enum | `HIGH` / `MID` / `LOW` |
| → `axes` | dict | EMA, Momentum, Volatility 등 축별 스코어 |
| → `ema_value` | float | EMA 값 (KR 한정) |
| → `kospi_change` | float | 실제 KOSPI 변동 (actuals만, 예측엔 없음) |

### 4.2 Source of Truth

| 시장 | today source | tomorrow source |
|------|-------------|-----------------|
| **KR** | `regime_actuals` (market_date=today) | `regime_predictions` (target_date=today+1 biz day) |
| **US** | (동일 개념, 별도 `regime_actuals_us` 등 — 현재 US는 today만, tomorrow 미구현) | **부재** (Phase 4에서 이식 결정) |

### 4.3 계산 시점 규칙

- **Today's Regime**: 장중에는 `STREAMING` 상태로 업데이트, 장 마감 후 `FINALIZED`
- **Tomorrow's Forecast**: 장 마감 후 EOD 배치에서 계산 (= `regime_predictions` 테이블 insert 시점)
- UI는 `computed_at`이 stale > 1일이면 `STALE` 뱃지

### 4.4 일관성 체크

- Regime 카드의 `kospi_change` == Analytics Equity Curve의 KOSPI line 오늘 delta.
- 값이 다르면 kospi_index 테이블의 2026-04-24 행 확인 (이미 R7 KOSPI dual-sink로 해결됨).

### 4.5 Sector Regime

별도 테이블 `regime_theme_daily`. 스키마:
- `market_date`, `theme_code`, `theme_name`, `stock_count`, `change_pct`, `regime`
- UI는 top N 테마 heatmap. breadth = `count(regime >= 4) / count(all)`.

---

## 5. Status Badges (상태 뱃지)

### 5.1 BATCH Badge

**컴포넌트**: `qc.badges.batch(state)` (Phase 3 구현)

**결정 함수** (P0-1 로직):
```js
function isBatchDone(state) {
    if (!state.last_batch_business_date) return false;
    if (state.last_batch_business_date !== state.business_date) return false;
    if (!state.snapshot_created_at) return false;

    const created = new Date(state.snapshot_created_at);
    const fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', hour12: false,
    });
    const parts = Object.fromEntries(
        fmt.formatToParts(created).map(p => [p.type, p.value])
    );
    const createdEtDate = `${parts.year}-${parts.month}-${parts.day}`;
    const createdEtHour = parseInt(parts.hour, 10);
    return (createdEtDate === state.business_date) && (createdEtHour >= 16);
}
```

**KR/US 공통**. 시장 데이터 소스만 다름:
- KR: `/api/batch/status` → `{ kr_done: boolean, business_date, snapshot_created_at }`
- US: `/api/rebalance/status` → `{ last_batch_business_date, business_date, snapshot_created_at }`

→ 두 API 응답 형식을 **공통으로 맞춰야** (Phase 3 backend refactor). 임시로 어댑터 함수만 제공.

### 5.2 AUTO GATE Badge

**결정 함수**:
```js
function autoGateLabel(state) {
    if (state.blocked) return { text: 'AUTO GATE: BLOCKED', color: 'red' };
    if (state.enforcing) return { text: 'AUTO GATE: ENFORCING', color: 'red' };
    if (state.enabled) return { text: 'AUTO GATE: ADVISORY (OK)', color: 'green' };
    return { text: 'AUTO GATE: ADVISORY', color: 'gray' };
}
// + stale 접미사
if (computed_at_age > 5 * 60) text += ' · STALE';
```

**Source**:
- KR: `/api/auto_gate/status`
- US: `/api/auto_gate/status` (동일 경로, 별도 서버)

### 5.3 Market Open/Close Badge

**KR**: 09:00~15:30 KST → `OPEN`, 그 외 `CLOSED`. 공휴일은 `holidays` 테이블 조회.
**US**: 09:30~16:00 ET → `OPEN`, 그 외 `CLOSED`. Alpaca `get_clock()` API.

UI: `OPEN`은 녹색 펄스, `CLOSED`는 회색.

### 5.4 Mode Badge

`PAPER` (기본) 또는 `LIVE`. state의 `mode` 필드 직접 표시.
- `PAPER` → 파랑
- `LIVE` → 빨강 (강조)

---

## 6. 공통 API 응답 규약 (Phase 3 통일 대상)

Phase 3 이전에 다음 엔드포인트는 **응답 스키마가 KR/US 다름**. Phase 3에서 공통화:

| 엔드포인트 | KR 현재 | US 현재 | 통일안 |
|-----------|---------|---------|--------|
| `/api/batch/status` | `{kr_done, ...}` | `/api/rebalance/status` 로 대체됨 | `{batch_done, business_date, snapshot_created_at, snapshot_version}` |
| `/api/holdings` | `{positions: [...]}` | `{positions: [...]}` | 스키마 공통화 (§1.1) |
| `/api/summary` | (각 필드 분산) | (각 필드 분산) | 단일 엔드포인트 `/api/summary` 신설 |
| `/api/regime/current` | `{today, tomorrow, axes}` | `{today, axes}` | tomorrow 필드 US도 도입 |
| `/api/rebalance/preview` | KR 로직 | US 로직 | new/exit/keep 공통 계산 |

---

## 7. 검증 방법 (3단계)

### 7.1 Cross-Market Consistency Test

같은 개념 필드가 양 시장에서 같은 의미를 갖는지 자동 검사:
```
test_holdings_schema_identical(kr_response, us_response)
test_summary_calc_same(kr_response, us_response)  # fee 제외 공식
test_regime_label_enum_match(kr, us)
```

### 7.2 Snapshot Consistency Test

Dashboard 상단 `snapshot_version`이 다음과 동일해야:
- BATCH 뱃지 상태
- Target Portfolio 섹션의 snapshot_id
- Rebalance 섹션의 snapshot_version

세 곳이 다르면 테스트 실패.

### 7.3 UI vs Engine Diff Test

```
ui_equity = scrape(Dashboard.Summary.equity)
broker_equity = broker_api.get_account().portfolio_value
assert abs(ui_equity - broker_equity) < 0.01  # 0% 오차
```

허용 오차:
- equity, cash: **0%** (1센트 수준)
- unrealized_pnl: **0%**
- positions qty: **0** (정수 일치)
- 최신 가격: stale ≤ 5분 이면 통과

불허 케이스:
- derived mismatch (예: UI가 계산식 A로, engine이 계산식 B로 구한 값)
- 시점 차이로 인한 oscillation이 분 단위 이상 지속

---

## 8. 변경 관리

이 문서는 **FROZEN**. 변경 시 요구 사항:
1. IA 문서 `§8 승인 체크리스트` 재통과
2. 영향받는 컴포넌트/엔드포인트 마이그레이션 계획 재수립
3. 회귀 테스트 셋 업데이트
4. 결정 로그 추가 (§10)

---

## 9. 알려진 우려/미해결

| 항목 | 내용 | Phase |
|------|------|-------|
| US tomorrow regime 부재 | KR에만 `regime_predictions`, US는 오늘만 | Phase 4에서 결정 |
| snapshot_version 형식 이원화 | 공통화 시 양쪽 consumer 전부 영향 | Phase 4~5 |
| realized_pnl 일관성 | KR은 DB 집계, US는 state 파일 | Phase 4 API 통일 시 해결 |
| KOSPI index 이중쓰기 | 이미 수정 (d7bfb47b) | 해결됨 |
| AAPL qty=-1 버그 | rebalancer에서 abs() 누락 | P2 (별건) |

---

## 10. 결정 로그

| 날짜 | 결정 | 근거 |
|------|------|------|
| 2026-04-24 | `unrealized_pnl_pct` 공식 = `(last/avg - 1)*100` 으로 통일 | 수수료 반영 공식과 분리하여 정의 명확화 |
| 2026-04-24 | Broker = Truth 원칙 재확인 | CLAUDE.md Global Safety Rule §1 |
| 2026-04-24 | snapshot_version은 시장별 형식 유지, Unified만 매칭 | 과도한 통일은 rebal/batch 로직 침범 |
| 2026-04-24 | qty=-1 사고 방지 공식 (§3.5) 공식화 | 실제 incident 반영 |
| 2026-04-24 | `total_pnl = unrealized + realized_total` 정의 | 기존 분산 계산 통일 |

*다음 결정 업데이트: Phase 3 API 공통화 시점.*
