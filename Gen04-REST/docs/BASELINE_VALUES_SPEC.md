# Q-TRON 기준값 정의서 v1.0
**작성일**: 2026-04-09
**승인**: JUG
**적용 범위**: Gen4 LIVE, Gen04-REST, Dashboard, Telegram, 모든 리포트

---

## 공통 원칙

### Source of Truth 우선순위
```
broker > REST_DB(snapshot) > cache > UI
```
- broker 값 = 현재 상태 (실시간)
- REST_DB = 과거 기준값/계산값 저장 (영속)
- cache = 일시적 참조 (TTL 한정)
- UI = 표시 전용 (판단 기준으로 사용 금지)

### 시점 구분
| 구분 | 정의 | 시간 |
|------|------|------|
| INTRADAY | 장중 tick 기반 | 09:00~15:30 |
| EOD | 장마감 확정 기준 | 15:30 이후 |
| T+1 | 익일 정산 반영 | 다음 거래일 |

### 계산 규칙
- 모든 금액: KRW 정수 (원 단위, 소수점 없음)
- 모든 비율: 소수점 6자리까지 저장, 표시는 2자리
- rounding: 금액은 `int()` (버림), 비율은 `round(x, 6)`

### 금지 사항
- broker 값과 REST_DB 값 혼용 계산 금지
- 서로 다른 snapshot 시점의 값 교차 사용 금지
- 장중 계산값으로 EOD 확정값 대체 금지

---

## 기준값 15개 정의

### 1. qty (보유 수량)
| 항목 | 값 |
|------|---|
| 정의 | 현재 보유 주식 수량 |
| source | broker (kt00018) |
| 갱신 | 체결 이벤트 / RECON |
| 기준 | INTRADAY 실시간 |
| fallback | 없음 (broker 불가 시 거래 차단) |
| 검증 | broker qty == REST_DB qty (완전 일치) |

### 2. avg_price (평균 매입가)
| 항목 | 값 |
|------|---|
| 정의 | 보유 포지션의 가중 평균 매입가 |
| source | REST trade_log (primary) / broker (참고) |
| 갱신 | 매수 체결 시 재계산 |
| 수식 | `(기존_invested + 신규_cost) / (기존_qty + 신규_qty)` |
| 기준 | 체결 기준 |
| 주의 | broker avg_price는 근사값 → REST ledger 기준 사용 |
| 검증 | broker avg_price와 1% 이내 |

### 3. entry_date (진입일)
| 항목 | 값 |
|------|---|
| 정의 | 포지션 최초 진입일 |
| source | REST trade_log |
| 갱신 | 신규 포지션 생성 시 (첫 매수 체결) |
| 기준 | T+0 (체결일 기준) |
| fallback | 없음 (반드시 REST 유지) |
| 검증 | Gen4 entry_date와 동일 |

### 4. current_price (현재가)
| 항목 | 값 |
|------|---|
| 정의 | 현재 평가 가격 |
| source | WS tick (0B) 우선 / REST quote fallback |
| 갱신 | tick 수신 시 |
| 기준 | INTRADAY last price |
| fallback | provider cache (stale 표시) |
| 검증 | WS vs REST quote 차이 < 1호가 |

### 5. market_value (시장가치)
| 항목 | 값 |
|------|---|
| 정의 | 보유 종목의 현재 평가금 |
| source | 계산값 |
| 수식 | `qty * current_price` |
| 갱신 | current_price 변경 시 |
| 기준 | INTRADAY |

### 6. invested_total (총 투자금)
| 항목 | 값 |
|------|---|
| 정의 | 매수 원금 + 매수 수수료 |
| source | REST trade_log |
| 수식 | `sum(buy_qty * buy_price * (1 + BUY_FEE_RATE))` |
| 갱신 | 매수 체결 시 증가 |
| 기준 | 체결 기준 |
| 주의 | 수수료 포함 (BUY_FEE_RATE = 0.00115) |

### 7. realized_pnl (실현 손익)
| 항목 | 값 |
|------|---|
| 정의 | 확정된 매매 손익 |
| source | REST trade_log |
| 수식 | `sell_proceeds - proportional_buy_cost - sell_fee - tax` |
| 갱신 | 매도 체결 시 |
| 기준 | 체결 기준 |
| 인식 시점 | 체결 확인 즉시 |

### 8. unrealized_pnl (미실현 손익)
| 항목 | 값 |
|------|---|
| 정의 | 보유 중인 포지션의 평가 손익 |
| source | 계산값 |
| 수식 | `(current_price - avg_price) * qty` |
| 갱신 | current_price 변경 시 |
| 기준 | INTRADAY |

### 9. high_watermark (HWM)
| 항목 | 값 |
|------|---|
| 정의 | 포지션 보유 기간 중 최고 종가 |
| source | REST_DB |
| 갱신 | EOD close > 기존 HWM 시 업데이트 |
| 기준 | **EOD close 기준** (tick high 아님) |
| reset | 신규 진입 시 entry_price로 초기화 |
| 주의 | Gen4와 동일하게 close-based. tick-based로 바꾸면 trail 판단 달라짐 |
| 검증 | Gen4 HWM == REST HWM (완전 일치) |

### 10. trail_stop_price (트레일링 스톱 가격)
| 항목 | 값 |
|------|---|
| 정의 | trailing stop 발동 가격 |
| source | 계산값 (REST_DB 저장) |
| 수식 | `HWM * (1 - TRAIL_RATIO)` (TRAIL_RATIO = 0.12) |
| 갱신 | HWM 변경 시 재계산 |
| 기준 | **계산은 INTRADAY, 실행은 EOD close 기준** |
| 주의 | 장중에 trail 이하로 내려가도 실행 안 함 — EOD close에서만 판단 |
| 검증 | Gen4 trail == REST trail (완전 일치) |

### 11. prev_close_equity (전일 종가 equity)
| 항목 | 값 |
|------|---|
| 정의 | 전일 장마감 시점의 총 자산 |
| source | REST_DB snapshot |
| 수식 | `cash + sum(qty * close_price)` at EOD T-1 |
| 갱신 | **EOD 확정 후 1회만** (15:30 이후) |
| 기준 | EOD |
| 주의 | 장중 계산값으로 대체 금지 |
| 검증 | Gen4 prev_close_equity와 괴리 < 0.5% |

### 12. peak_equity (최고 equity)
| 항목 | 값 |
|------|---|
| 정의 | 현재 리밸 주기 내 최고 총 자산 |
| source | REST_DB |
| 수식 | `max(peak_equity, total_equity)` at EOD |
| 갱신 | **EOD 기준** (장중 peak는 참고만) |
| reset | 리밸 실행 시 현재 equity로 초기화 |
| 주의 | 리밸 주기 내 peak임 (전체 기간 아님) |
| 검증 | Gen4 peak_equity와 괴리 < 0.5% |

### 13. total_equity (총 자산)
| 항목 | 값 |
|------|---|
| 정의 | 현금 + 전체 보유 종목 시장가치 |
| source | 계산값 |
| 수식 | `cash + sum(market_value)` |
| 갱신 | INTRADAY |
| 주의 | broker `total_asset`(추정예탁자산)과 차이 가능 → 로그 감지만 |

### 14. dd_level (drawdown level)
| 항목 | 값 |
|------|---|
| 정의 | peak 대비 하락률 |
| source | 계산값 |
| 수식 | `(total_equity - peak_equity) / peak_equity` |
| 갱신 | INTRADAY (total_equity 변경 시) |
| 등급 | NORMAL > -2%, CAUTION > -3%, WARNING > -4%, CRITICAL > -5%, SEVERE |
| 용도 | buy_permission 판단 기준 |

### 15. rebalance_cycle_id (리밸 주기 ID)
| 항목 | 값 |
|------|---|
| 정의 | 현재 리밸런싱 주기 식별자 |
| source | REST_DB |
| 갱신 | 리밸 실행 완료 시 +1 |
| 용도 | peak_equity reset 기준, 주기별 성과 분리 |

---

## 장중 vs EOD 기준 매트릭스

| 항목 | INTRADAY | EOD |
|------|----------|-----|
| current_price | tick (실시간) | close (확정) |
| HWM | 갱신 안 함 | close > HWM 시 갱신 |
| trail_stop_price | 재계산 (표시용) | 실행 판단 기준 |
| total_equity | 실시간 계산 | 확정 기록 |
| prev_close_equity | 읽기 전용 | 생성 (1회) |
| peak_equity | 읽기 전용 | 갱신 판단 |
| dd_level | 실시간 계산 | 확정 기록 |
| realized_pnl | 체결 시 즉시 | 일일 합산 |

---

## 검증 규칙

| 검증 항목 | 기준 | 빈도 | 실패 시 |
|----------|------|------|--------|
| broker equity vs REST equity | 괴리 < 0.5% | 매 5분 | [EQUITY_MISMATCH] 경고 |
| HWM 일치 | Gen4 == REST (정수 일치) | EOD | 로그 + 조사 |
| trail_stop 일치 | Gen4 == REST (정수 일치) | EOD | 로그 + 조사 |
| qty/code 일치 | broker == REST (완전) | 시작 시 + EOD | 거래 차단 |
| entry_date 일치 | Gen4 == REST | 체결 시 | 로그 |
| realized PnL 누적 | 차이 < 1,000원 | EOD | 로그 + 조사 |
| avg_price | broker vs REST < 1% | EOD | 로그 |
| prev_close_equity | Gen4 vs REST < 0.5% | T+1 시작 시 | 로그 + 조사 |
| peak_equity | Gen4 vs REST < 0.5% | 리밸 시 | 로그 + 조사 |

---

## 비용 모델 (고정)

| 항목 | 값 | 적용 시점 |
|------|---|----------|
| 매수 수수료 | 0.115% (0.00115) | 매수 체결 시 |
| 매도 수수료 | 0.015% (0.00015) | 매도 체결 시 |
| 거래세 | 0.18% (0.0018) | 매도 체결 시 |
| 농특세 | 0.10% (0.001) | KOSPI 매도 시 |
| 매도 총비용 | 0.295% (0.00295) | 매도 시 합산 |
| rounding | 수수료: 원 미만 버림 | 체결 즉시 |

---

## 다음 단계

- [ ] 상태 전이 정의서 (STATE_TRANSITION_SPEC.md)
- [ ] 교차검증 로직 구현 (cross_validator.py 확장)
- [ ] REST_DB 스키마 설계 (rest_positions, rest_equity_snapshots, rest_trades)

---

*이 문서는 Phase 1 진입 전 필수 선행 조건입니다.*
*수정 시 JUG 승인 필요.*
