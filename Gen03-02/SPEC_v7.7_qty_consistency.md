# Q-TRON Gen3 v7.7 Spec: Quantity & Price Consistency
> Created: 2026-03-17 | Status: DRAFT | Based on: LIVE session 2026-03-17 log analysis

---

## 목표
LIVE/모의투자 공통으로 주문-체결-포지션-평가금액의 기준값 일관성을 확보한다.

---

## 문제 (2026-03-17 LIVE 세션에서 관측)

### P1. qty 불일치 — reconcile 후에도 체결 수량 괴리
- RECON이 7개 전종목 qty를 보정함 (예: 131970 engine=560 → broker=480)
- 매도 시 `req_qty=480`으로 주문했으나 `CLOSED qty=522`로 기록됨
- **영향**: 포지션 잔량 계산 오류, realized PnL 왜곡

### P2. 체결가 source 불명확
- `EXECUTE SELL price=72,800` (decision price) vs `FILLED SELL price=82,104` (broker fill)
- slippage가 +12.78%로 기록 — 실제로는 decision/fill price 혼선
- **영향**: slippage 통계 무의미, PnL 계산 불신뢰

### P3. 장중 equity 고정 (stale snapshot)
- 10:30~15:20 전 구간 `equity=505,007,645` 변동 없음
- 모의서버 시세 미제공이 원인이나, 실서버에서도 캐시 문제 시 동일 현상 발생 가능
- **영향**: RiskEval/Heartbeat/Dashboard가 stale 기준으로 판단, SL/TP 트리거 지연

### P4. EOD 일간손익 reset
- 장중 `+2.02%` → EOD `+0.00%`로 표시
- `end_of_day_update()`에서 prev_close를 갱신한 후 summary를 출력하여 diff=0
- **영향**: EOD 리포트 PnL 항목 무의미

### P5. report_daily 누락 (v7.6.1에서 수정 완료)
- 재시작 시 `run_entries()` 스킵 → `report_daily()` 미호출
- **수정 완료**: `end_of_day()`에 EOD daily report 호출 추가

---

## 원인 추정

| 문제 | 추정 원인 |
|------|----------|
| P1 | `close_position()`이 broker `filled_qty`가 아니라 기존 engine `pos.quantity`를 사용 |
| P1 | reconcile 후 qty만 수정하고 관련 캐시/파생 상태(cash, exposure 등)를 재생성하지 않음 |
| P2 | fill price 기록 시 broker 체결가 대신 내부 decision_price 또는 avg_price 사용 |
| P3 | mark-to-market snapshot 갱신이 호출되지 않거나 stale cache를 반환 |
| P4 | EOD에서 prev_close 갱신 → summary 계산 순서 (baseline이 먼저 갱신되어 diff=0) |

---

## 수정 제안

### A. qty 분리 모델 (P1 해결)
```
order/request_qty    — 주문 요청 수량
broker/filled_qty    — 브로커 체결 수량 (chejan callback)
position/qty_before  — 체결 전 보유 수량
position/qty_after   — 체결 후 보유 수량 (= qty_before - filled_qty)
```
- 포지션 감소는 **filled_qty 기준으로만** 수행
- partial fill: `qty_after > 0`이면 PARTIAL_CLOSED, CLOSED가 아님
- 거부/미체결: qty 변동 없음

### B. price source 분리 (P2 해결)
```
decision_price    — 엔진이 판단한 매도 호가 (SL/TP/시장가 등)
fill_price        — 브로커 실제 체결가 (chejan 9901 필드)
avg_entry_price   — 기존 매수 평균 단가
slippage          — (fill_price - decision_price) / decision_price
realized_pnl      — (fill_price - avg_entry_price) * filled_qty - costs
```
- 모든 trade log에 5개 필드 분리 기록
- slippage 계산은 반드시 `fill_price vs decision_price`

### C. reconcile 후 상태 재생성 (P1/P3 해결)
reconcile 직후 아래 순서로 재생성:
```
1. rebuild_portfolio_snapshot()  — 전 종목 qty/price 재계산
2. rebuild_cash_state()          — 현금 = 예탁자산 - 평가금액
3. rebuild_exposure()            — 노출도 재계산
4. rebuild_risk_cache()          — RiskEval 기준값 갱신
```
- reconcile pre/post diff snapshot을 별도 저장 (원인 은폐 방지)

### D. mark-to-market 강제 갱신 (P3 해결)
- 60초마다 `mark_to_market()` 강제 호출
- RiskEval/Heartbeat/Dashboard는 **동일 snapshot**만 참조 (캐시 공유)
- 10분 이상 equity 고정 시 `[WARN:STALE_PRICE]` 경고 발생
- LIVE에서 tick 0건이면 state를 `DEGRADED_LIVE`로 두고 신규 진입 비활성화

### E. EOD baseline 순서 수정 (P4 해결)
```
현재 (잘못됨):
  1. end_of_day_update()  → prev_close = current_equity  ← baseline 갱신
  2. summary()            → day_pnl = current - prev_close = 0%

수정 후:
  1. summary()            → day_pnl = current - prev_close (= 장중 PnL 유지)
  2. report_daily()       → 정확한 PnL 포함 리포트 생성
  3. end_of_day_update()  → prev_close = current_equity  ← baseline 갱신 (마지막)
```

### F. partial fill/close 상태 분리
- CLOSED는 `qty_after == 0`일 때만 사용
- `qty_after > 0`이면 PARTIAL_CLOSED 상태 유지
- PARTIAL_CLOSED 포지션은 다음 사이클에서 잔량 청산 재시도

---

## 검증 시나리오

### T1. qty 분리 검증
- engine qty=522, broker qty=480, sell fill=480
- 기대: `qty_after = 522 - 480 = 42` (잔량 유지, PARTIAL_CLOSED)
- 실패 조건: qty_after=0 (전량 청산 오류) 또는 qty_after=522 (감소 누락)

### T2. price/slippage 일관성 검증
- decision_price=72,800, fill_price=82,104, avg_entry=81,618
- 기대: slippage = (82,104-72,800)/72,800 = +12.78%
- 기대: realized_pnl = (82,104-81,618) * 480 - costs
- 실패 조건: fill_price에 decision_price가 들어감

### T3. stale equity 경고 검증
- 장중 10분 이상 equity 변동 없음
- 기대: `[WARN:STALE_PRICE]` 로그 출력
- 실패 조건: 경고 없이 NORMAL 리스크 모드 유지

### T4. EOD PnL 일관성 검증
- 장중 마지막 snapshot PnL = +2.02%
- 기대: EOD summary day_pnl = +2.02% (동일)
- 실패 조건: EOD day_pnl = +0.00%

### T5. DEGRADED_LIVE 모드 검증
- LIVE 모드에서 tick 수신 0건
- 기대: state=DEGRADED_LIVE, entry_disabled=True
- 실패 조건: NORMAL 상태로 진입 시도

---

## 주의사항

1. **partial fill/partial close를 CLOSED로 처리하지 말 것**
   - CLOSED는 잔량 0일 때만 허용
   - 모의서버의 즉시 전량체결 가정을 실서버에 그대로 적용 금지

2. **reconcile이 원인 은폐 수단이 되지 않도록 할 것**
   - pre/post diff snapshot을 `data/logs/reconcile_YYYYMMDD.json`에 저장
   - diff가 qty ±10% 이상이면 `[WARN:LARGE_RECON]` 경고

3. **risk mode는 stale equity일 경우 신뢰 불가 상태를 명시할 것**
   - `risk_confidence: HIGH | DEGRADED`
   - DEGRADED 상태에서는 신규 진입 차단, 청산만 허용

---

## 파일 영향 범위 (예상)

| 파일 | 수정 내용 |
|------|----------|
| `core/position_tracker.py` | qty_before/qty_after 필드, PARTIAL_CLOSED 상태 |
| `core/portfolio_manager.py` | rebuild_*() 메서드, close_position filled_qty 기준 |
| `core/state_manager.py` | 신규 필드 직렬화 |
| `runtime/order_executor.py` | filled_qty/fill_price 분리, partial fill 처리 |
| `runtime/runtime_engine.py` | mark_to_market(), DEGRADED_LIVE, EOD 순서 |
| `strategy/exit_logic.py` | decision_price vs fill_price 분리 |
| `data/kiwoom_provider.py` | chejan fill_price 필드 매핑 |
| `report/reporter.py` | 5-field trade log, stale 경고 표시 |
| `core/risk_manager.py` | risk_confidence, stale equity 감지 |

---

## 구현 우선순위

1. **E. EOD baseline 순서** — 즉시 적용 가능, 리스크 없음
2. **B. price source 분리** — trade log 신뢰도 확보
3. **A. qty 분리 모델** — 핵심 정합성 보장
4. **C. reconcile 후 재생성** — 상태 일관성
5. **D. mark-to-market + stale 경고** — 운영 안정성
6. **F. partial fill/close** — 실서버 전환 필수

---

## 변경 이력

| 날짜 | 버전 | 내용 |
|------|------|------|
| 2026-03-17 | v7.7-draft | 초안 작성 (LIVE 세션 로그 기반) |
