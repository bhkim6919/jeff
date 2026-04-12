# Q-TRON 상태 전이 정의서 v1.1
**작성일**: 2026-04-09
**개정**: v1.1 — JUG P1 리뷰 7건 반영
**승인**: JUG (조건부 → 승인)
**적용 범위**: Gen4 LIVE, kr, 모든 상태 판단 로직

---

## 0. 매수 허용 최종 판정 (Master Gate)

**모든 매수 판단의 단일 진입점. 아래 조건 중 하나라도 True면 매수 차단.**

```python
def is_buy_allowed() -> bool:
    """매수 허용 최종 판정 — 모든 매수 로직은 이 함수만 호출"""
    if buy_permission in (BLOCKED, RECOVERING):
        return False
    if dd_level in (CRITICAL, SEVERE, SAFE_MODE):
        return False
    if safe_mode_active:           # RECON 또는 DD 기원 무관
        return False
    if session_monitor_only:
        return False
    if recon_unreliable:
        return False
    return True
```

**buy_scale 결정 (허용 시):**
```python
def get_buy_scale() -> float:
    scale = 1.0
    if buy_permission == REDUCED:
        scale *= 0.5
    if dd_level == WARNING:
        scale *= 0.5
    if dd_level == CAUTION:
        scale *= 0.7
    return scale
```

**우선순위**: safe_mode > BuyPermission > DD Guard > session flag
- safe_mode는 **출처 무관** (DD 기원이든 RECON 기원이든 동일 차단)
- BuyPermission과 DD Guard가 충돌 시: **더 엄격한 쪽** 적용

---

## 1. BuyPermission (매수 허용 상태)

### 상태
| 상태 | 의미 | 매수 | 매도 | Trail Stop |
|------|------|------|------|-----------|
| `NORMAL` | 정상 운영 | O (100%) | O | O |
| `REDUCED` | 축소 운영 | O (50%) | O | O |
| `RECOVERING` | 복구 관찰 중 | X | O | X |
| `BLOCKED` | 완전 차단 | X | O only | X |

### 전이
```
NORMAL ──→ REDUCED ──→ RECOVERING ──→ BLOCKED
  ↑           ↑            ↑             │
  └───────────┴────────────┴─────────────┘
         (조건 해소 시 역방향 전이)
```

| From | To | 조건 | 부수 효과 |
|------|----|------|----------|
| Any | BLOCKED | safe_mode >= 3 OR opt10075_fail >= 2 OR critical_pending_external | 로그, 알림 |
| BLOCKED | RECOVERING | opt10075_success >= 2 AND !critical_pending AND recon_ok | 관찰 시작 |
| RECOVERING | REDUCED | 관찰 세션 2회 이상 통과 (아래 정의 참조) | buy_scale=0.5 |

### RECOVERING 관찰 세션 정의
- **1세션** = Gen4 LIVE 1회 시작~종료 사이클 (장중 전체)
- **2회 통과** = 2 거래일 연속 BLOCKED 재진입 없이 완주
- 장중 opt10075 실패 시 세션 카운터 리셋
- 기준: **거래일** (달력일 아님)
| REDUCED | NORMAL | 모든 reduced 조건 해소 | buy_scale=1.0 |

### 저장
- **Memory only** (세션 로컬) — 의도적 설계
- 매 세션 NORMAL에서 시작, 실시간 신호로 재평가
- **Stateless 복원 원칙**: 재시작 시 NORMAL에서 시작하되,
  opt10075 / RECON / DD 신호를 즉시 수집하여 실제 상태로 전이
- **안전장치**: 재시작 후 첫 RECON 완료 전까지 리밸 차단 (recovery-first)
- **위험 인지**: SAFE_MODE 중 재시작 → NORMAL 리셋됨.
  단, DD Guard가 즉시 재계산되어 CRITICAL 이상이면 매수 차단 복원됨.
  opt10075 실패도 즉시 감지 → BLOCKED 복원.
  **잔여 위험**: DD가 경계값 근처일 때 ~1분간 false NORMAL 가능 → 수용

### REST 적용
- REST는 BuyPermission을 **읽기 전용**으로 표시
- REST에서 BuyPermission을 변경하지 않음
- 표시: Dashboard DD Guard 섹션의 "BUY STATUS" 배지

---

## 2. DD Guard Level (Drawdown 등급)

### 등급
| 등급 | monthly_dd 임계 | buy_scale | trim_ratio | 의미 |
|------|----------------|-----------|------------|------|
| NORMAL | > -5% | 100% | 0% | 정상 |
| CAUTION | > -10% | 70% | 0% | 주의 |
| WARNING | > -15% | 50% | 0% | 경고 |
| CRITICAL | > -20% | 0% | 0% | 위험 (매수 차단) |
| SEVERE | > -25% | 0% | 20% | 심각 (일부 강제 청산) |
| SAFE_MODE | <= -25% | 0% | 50% | 안전 모드 |

### Safe Mode 히스테리시스
- 진입: DD <= -25%
- 해제: DD > -20% (별도 임계, 반복 방지)
- 당일 해제 금지

### Safe Mode 이중 소스 정의 (P1-6)
safe_mode는 **2개 소스**에서 발생 가능:
| 소스 | 트리거 | 해제 조건 |
|------|--------|----------|
| DD Guard | monthly_dd <= -25% | DD > -20% (히스테리시스) |
| RECON | 보정 > 10건 OR cash_spike | 다음 세션 RECON 정상 (보정 <= 2건) |

**합산 규칙**: OR (어느 쪽이든 활성이면 safe_mode)
**우선순위**: 해제도 OR — **양쪽 모두** 해소되어야 safe_mode 해제
**단일 플래그**: `safe_mode_active = dd_safe_mode OR recon_safe_mode`

### REST 적용
- `_compute_dd_guard_from()` 에서 동일 로직 사용
- config_version: `gen4_v4.1_trail12_rebal21_dd4m7`
- Gen4 config 변경 시 REST도 동기화 필수

---

## 3. OrderStatus (주문 상태)

### 상태
| 상태 | 의미 | Terminal? |
|------|------|----------|
| `NEW` | 생성됨, 미제출 | N |
| `SUBMITTED` | 브로커 제출 완료 | N |
| `PARTIAL_FILLED` | 부분 체결 | N |
| `FILLED` | 전량 체결 | **Y** |
| `TIMEOUT_UNCERTAIN` | 브로커 응답 없음 | N |
| `PENDING_EXTERNAL` | 브로커에 살아있을 수 있음 | N (대기) |
| `CANCELLED` | 취소됨 | **Y** |
| `REJECTED` | 거부됨 | **Y** |

### 전이
```
NEW → SUBMITTED → FILLED
                → PARTIAL_FILLED → FILLED
                → TIMEOUT_UNCERTAIN → PENDING_EXTERNAL → FILLED
                                                       → CANCELLED
                → REJECTED
                → CANCELLED
```

### 금지 전이
- Terminal 상태(FILLED, REJECTED, CANCELLED)에서 다른 상태로 전이 금지
- PENDING_EXTERNAL에서 SUBMITTED로 역전이 금지
- 동일 fill에 대한 중복 전이 금지 (fill_ledger 멱등성 키 사용)

### Fill Ledger 멱등성
- 키: `{order_no}_{side}_{cumulative_qty}`
- 소스: CHEJAN (실시간), GHOST (잔여), RECONCILE (브로커 조회)

### OrderStatus → Sell Status 연결 (P1-4)
리밸 매도 시 개별 주문의 OrderStatus가 sell_status를 결정:
```
sell_status 결정 규칙:
- 전체 FILLED              → COMPLETE
- 일부 FILLED + 나머지 대기 → PARTIAL
- TIMEOUT_UNCERTAIN 1건+   → UNCERTAIN
- PENDING_EXTERNAL 1건+    → UNCERTAIN (PENDING_EXTERNAL → FILLED 전환 대기)
- 전체 REJECTED/CANCELLED  → FAILED
```

### RECON → OrderStatus 연결 (P1-5)
PENDING_EXTERNAL 상태의 주문은 RECON에서 해소:
```
PENDING_EXTERNAL 해소 흐름:
1. RECON 시 broker 포지션 조회
2. broker에 해당 종목 매도 반영됨 → mark_ghost_settled() → FILLED
3. broker에 미반영 → mark_reconcile_settled(terminal="CANCELLED") → CANCELLED
4. 해소 전까지 sell_status = UNCERTAIN 유지
```

### REST 적용
- REST Surge/Lab은 **가상 주문**이므로 이 상태기계 미사용
- REST Dashboard는 Gen4의 OrderStatus를 읽기 전용 표시
- Crosscheck 시 PENDING_EXTERNAL 건수 감시

---

## 4. Shutdown Reason (종료 사유)

### 값
| 값 | 의미 | 정상 종료? |
|----|------|-----------|
| `running` | 세션 활성 중 (dirty exit 감지용) | N |
| `normal` | 사용자 정상 종료 | Y |
| `sigint` | Ctrl+C / 시그널 인터럽트 | Y |
| `eod_complete` | EOD 정산 완료 후 종료 | Y |
| `unknown` | 최초 실행 또는 손상 | N |

### Dirty Exit 판단
```python
is_dirty = shutdown_reason in ("running", "unknown")
```

### Recovery 흐름 + Runtime 상태 연결 (P1-7)
```
시작
  ↓
shutdown_reason 확인
  ├─ dirty ("running"/"unknown")
  │  → recovery-first startup
  │  → opt10075 검증 2회
  │  ├─ 성공 → recovery_ok=True, 정상 진행
  │  └─ 실패 → recovery_ok=False
  │            → session_monitor_only = True
  │            → pending_buys 보존 (실행 안 함)
  │            → recon_unreliable = True
  │            → BuyPermission → BLOCKED
  │
  └─ clean ("normal"/"sigint"/"eod_complete")
     → 정상 startup
     → pending_buys 확인 → sell_status == COMPLETE면 실행
     → recovery_ok = True
```

**Runtime 상태 결합 테이블:**
| shutdown_reason | recovery_ok | session_monitor_only | pending_buys | recon_unreliable |
|----------------|-------------|---------------------|--------------|-----------------|
| eod_complete | True | False | 실행 가능 | False |
| normal/sigint | True | False | 실행 가능 | False |
| running (dirty) + opt10075 OK | True | False | 실행 가능 | False |
| running (dirty) + opt10075 FAIL | False | **True** | **보존** | **True** |
| unknown | False | **True** | **보존** | **True** |

### REST 적용
- REST는 `runtime_state_live.json`에서 `shutdown_reason` 읽기
- Dashboard System Risk에 표시 (stale 판단 기준)
- REST 자체 shutdown은 별도 (WS cleanup, SSE 종료)

---

## 5. Rebalance Phase (리밸런싱 단계)

### 실행 흐름
```
Phase 0: Startup
    ↓
Phase 1: RECON (broker 동기화)
    ↓
Phase 1.5: Pending Buy 실행 (T+1)
    ↓
Phase 2: 리밸 필요 여부 판단
    ├─ 불필요 → Phase 3 (모니터)
    └─ 필요 → Phase 2.5
        ↓
Phase 2.5: 리밸 실행
    ├─ Sell Phase → sell_status 결정
    └─ Buy Phase → pending_buys 생성
        ↓
Phase 3: Monitor Loop (trail stop + 실시간 감시)
    ↓
Phase 4: EOD Wait (15:30 대기)
    ↓
Phase 5: EOD 평가 + 리포트 + 종료
```

### Sell Status
| 값 | 의미 | Buy Phase 진행? |
|----|------|----------------|
| `COMPLETE` | 전체 매도 성공 | O |
| `PARTIAL` | 일부 성공 | X (다음 세션) |
| `UNCERTAIN` | 타임아웃 발생 | X (다음 세션) |
| `FAILED` | 전체 실패 | X (다음 세션) |

### Pending Buy 실행 조건
```
모두 충족 시에만 실행:
├─ recovery_ok == True
├─ safe_mode 미활성
├─ BuyPermission not in (BLOCKED, RECOVERING)
├─ sell_status == "COMPLETE"
└─ session_monitor_only == False
```

### REST 적용
- REST는 `runtime_state_live.json`에서 `last_rebalance_date` 읽기
- Dashboard에 "다음 리밸까지 N일" 표시
- REST가 리밸 실행하지 않음 (읽기 전용)

---

## 6. RECON (Reconciliation) 상태

### 보정 유형
| 유형 | 조건 | 조치 |
|------|------|------|
| ENGINE_ONLY | 엔진에만 있는 포지션 | 엔진에서 제거 |
| BROKER_ONLY | 브로커에만 있는 포지션 | 엔진에 추가 |
| QTY_MISMATCH | 수량 불일치 | 브로커 기준 동기화 |
| AVGPRICE_MISMATCH | 매입가 불일치 | 브로커 기준 동기화 |
| CASH_MISMATCH | 현금 불일치 | 브로커 기준 동기화 |

### 안전 임계
- 보정 > 10건 → safe_mode 활성
- cash spike + 보정 > 0건 → safe_mode 활성
- recon_unreliable → session_monitor_only

### REST 적용
- REST Crosscheck (`cross_validator.py`): Gen4 state vs 브로커 비교
- RECON 상태는 Dashboard에 표시
- REST 자체 RECON: Phase 1에서 구현 예정 (REST_DB vs 브로커)

---

## 7. Surge StockState (급등주 시뮬 상태)

### 상태
```
SCANNED → WATCHING → READY_TO_BUY → BUY_PENDING → BOUGHT → SELL_PENDING → CLOSED
    └─→ SKIPPED        └─→ SKIPPED        └─→ SKIPPED                     (terminal)
                                                                    SKIPPED (terminal)
```

| 상태 | 의미 | 진입 조건 |
|------|------|----------|
| SCANNED | TR 스캔 감지 | 등락률 상위 |
| WATCHING | 필터 통과, 감시 중 | Lane별 조건 충족 |
| READY_TO_BUY | 진입 신호 | 틱 조건 충족 |
| BUY_PENDING | 매수 대기 | 주문 제출 |
| BOUGHT | 보유 중 | 체결 완료 |
| SELL_PENDING | 매도 대기 | TP/SL/시간 |
| CLOSED | 청산 완료 | 체결 완료 (terminal) |
| SKIPPED | 건너뜀 | 필터/실패 (terminal) |

### Lane별 진입 필터
| Lane | 조건 |
|------|------|
| A | 모든 후보 허용 |
| B | volume_surge == True (ka10023) |
| C | volume_surge AND strength_pass (ka10023 + ka10046) |

### REST 적용
- Surge 전용 (Gen4 LIVE와 무관)
- REST 내부에서만 사용

---

## 8. API Health Status (REST 서버 상태)

### 상태
| 상태 | 색상 | 의미 |
|------|------|------|
| GREEN | 초록 | 정상 |
| YELLOW | 노랑 | 경고 (stale, 지연) |
| RED | 빨강 | 오류 (인증 실패, 반복 에러) |
| BLACK | 검정 | 사망 (연결 불가) |

### 전이
```
GREEN → YELLOW (stale > 60s OR latency > 3s)
YELLOW → RED (error_count > 3 OR auth failure)
RED → BLACK (no connection > 5min)
Any → GREEN (정상 응답 수신 시)
```

---

## 상태 저장 위치 요약

| 상태 | 저장 | 영속? | 세션 초기값 |
|------|------|-------|-----------|
| BuyPermission | Memory | N | NORMAL |
| DD Level | 계산값 | N | 실시간 계산 |
| OrderStatus | Memory (+ journal) | N (journal: Y) | NEW |
| shutdown_reason | runtime_state.json | Y | "running" |
| sell_status | runtime_state.json | Y | "" |
| pending_buys | runtime_state.json | Y | [] |
| recon_unreliable | runtime_state.json | Y | False |
| session_monitor_only | Memory | N | False |
| StockState (Surge) | Memory | N | SCANNED |
| HealthStatus | Memory | N | GREEN |

---

## 금지 사항

1. Terminal 상태에서 역전이 금지 (FILLED→SUBMITTED 등)
2. REST에서 Gen4의 BuyPermission/OrderStatus 변경 금지
3. RECON 중 주문 발행 금지
4. safe_mode 활성 중 매수 주문 금지
5. session_monitor_only에서 리밸/매매 금지
6. 동일 fill에 대한 중복 상태 전이 금지

---

## 다음 단계

- [ ] 비용/회계 모델 고정 (COST_MODEL_SPEC.md)
- [ ] REST_DB 스키마 설계
- [ ] 교차검증 로직 구현

---

---

## 변경 이력

| 버전 | 날짜 | 내용 |
|------|------|------|
| v1.0 | 2026-04-09 | 초안 작성 |
| v1.1 | 2026-04-09 | JUG P1 리뷰 7건 반영: Master Gate, Memory 리셋 안전장치, RECOVERING 기준, OrderStatus↔SellStatus 연결, RECON↔OrderStatus 연결, safe_mode 이중소스, shutdown↔runtime 결합 |

---

*Phase 1 진입 조건: 기준값 정의서 + 상태 전이 정의서 + 비용 모델 고정 완료*
*비용 모델은 BASELINE_VALUES_SPEC.md에 포함 (buy 0.115%, sell 0.295%, rounding 규칙).*
*수정 시 JUG 승인 필요.*
