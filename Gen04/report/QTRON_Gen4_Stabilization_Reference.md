# Q-TRON Gen4 Stabilization Reference
## 자동매매 시스템 안정화 교본

> **목적**: Gen4 개발 과정에서 발견된 모든 버그, 수정 패턴, 설계 결정을 역추적하여 문서화.
> 향후 Q-TRON 미국주식/코인 시장 확장 시 참고 교본으로 활용.
>
> **대상 시스템**: Q-TRON Gen4 — LowVol + Momentum 12-1 월간 리밸런싱
> **최종 수정일**: 2026-03-24

---

## 목차

1. [시스템 아키텍처 개요](#1-시스템-아키텍처-개요)
2. [실행 모드 체계 (mock / paper / live)](#2-실행-모드-체계)
3. [P0: 실거래 안전성](#3-p0-실거래-안전성)
4. [P0: 주문/체결 상태 일관성](#4-p0-주문체결-상태-일관성)
5. [P0: 상태 저장 원자성](#5-p0-상태-저장-원자성)
6. [P0: 계좌 동기화 안전장치](#6-p0-계좌-동기화-안전장치)
7. [P1: 리밸런스 커밋 원자성](#7-p1-리밸런스-커밋-원자성)
8. [P1: 주문 저널 (크래시 포렌식)](#8-p1-주문-저널)
9. [P1: CSV 스키마 마이그레이션](#9-p1-csv-스키마-마이그레이션)
10. [P1: EOD 가격 신뢰성](#10-p1-eod-가격-신뢰성)
11. [P1: 현금 추정과 매수 사이징](#11-p1-현금-추정과-매수-사이징)
12. [P2: 리스크 엔진 (검증 보류 항목)](#12-p2-리스크-엔진)
13. [P2: 전략 수치 검증 (변경 보류 항목)](#13-p2-전략-수치-검증)
14. [범시장 적용 가이드 (미국/코인)](#14-범시장-적용-가이드)
15. [로그 태그 사전](#15-로그-태그-사전)
16. [테스트 시나리오 카탈로그](#16-테스트-시나리오-카탈로그)

---

## 1. 시스템 아키텍처 개요

```
┌─────────────────────────────────────────────────────────┐
│                    main.py (Entry Point)                 │
│  --batch | --live | --mock | --backtest | --rebalance    │
└──────────┬──────────────────────────────────┬───────────┘
           │                                  │
    ┌──────▼──────┐                    ┌──────▼──────┐
    │  Batch Mode  │                    │  Live Mode   │
    │  pykrx→score │                    │  Kiwoom API  │
    │  →target.json│                    │  →orders     │
    └─────────────┘                    └──────┬───────┘
                                              │
           ┌──────────────────────────────────┼──────────────┐
           │                                  │              │
    ┌──────▼──────┐  ┌──────────────┐  ┌─────▼────┐  ┌─────▼──────┐
    │  Portfolio   │  │  Exposure    │  │  Order   │  │  Reporter  │
    │  Manager     │  │  Guard (DD)  │  │  Executor│  │  (CSV/JSONL│
    │  (state)     │  │  (risk)      │  │  (fill)  │  │   logging) │
    └──────┬──────┘  └──────────────┘  └─────┬────┘  └────────────┘
           │                                  │
    ┌──────▼──────┐                    ┌─────▼────────┐
    │  State Mgr   │                    │  Kiwoom      │
    │  (atomic I/O)│                    │  Provider    │
    │  JSON+backup │                    │  (COM/chejan)│
    └─────────────┘                    └──────────────┘
```

### 모듈별 라인 수 (코드 규모)

| 모듈 | 파일 | 라인 | 역할 |
|------|------|------|------|
| Entry | main.py | 1,369 | 모드 분기, 리밸런스, 모니터, EOD |
| Broker | kiwoom_provider.py | 1,391 | 키움 COM, 체결, 고스트 |
| Report | daily_report.py | 1,090 | 일간 리포트 |
| Config | config.py | 115 | 전 파라미터 중앙 관리 |
| Portfolio | portfolio_manager.py | 298 | 포지션, 현금, PnL |
| Risk | exposure_guard.py | 243 | DD 5단계 대응 |
| Execution | order_executor.py | 328 | paper/live 주문 |
| State | state_manager.py | 159 | 원자적 JSON 저장 |
| Tracking | order_tracker.py | 195 | 체결 이력 + 저널 |
| Strategy | rebalancer.py | 171 | 매도/매수 주문 생성 |
| Scoring | scoring.py | 89 | 변동성/모멘텀 계산 |

---

## 2. 실행 모드 체계

### 2.1 세 가지 모드 정의

| 모드 | CLI | 키움 연결 | 주문 경로 | 자금 | State 파일 |
|------|-----|----------|----------|------|-----------|
| **mock** | `--mock` | X | 내부 시뮬레이션 | 가상 (엔진) | `*_paper.json` |
| **paper** | `--live` + MOCK 서버 | O | 키움 모의투자 API | 가상 (키움) | `*_paper.json` |
| **live** | `--live` + REAL 서버 | O | 키움 실거래 API | 실자금 | `*.json` |

### 2.2 용어 혼동 사례 (Gen3→Gen4 교훈)

**Gen3 문제**: `is_paper = False` 하드코딩으로 인해 `config.PAPER_TRADING=True`여도
실제 주문이 나가는 버그 존재.

**Gen4 해결**: 세 단계 분리

```python
# main.py — 모드 판별 로직
kiwoom, server_type = create_loggedin_kiwoom()
# server_type: "MOCK" (모의투자 서버) | "REAL" (실거래 서버)

# Safety gate: PAPER_TRADING=True + REAL 서버 → 즉시 중단
if config.PAPER_TRADING and server_type == "REAL":
    logger.critical("[PAPER_SAFETY] ABORTING...")
    return

is_paper_server = (server_type == "MOCK")

# 핵심: paper/live 모두 키움 API로 주문 전송 (paper=False)
# 서버(MOCK vs REAL)가 가상/실거래를 결정
executor = OrderExecutor(provider, tracker, trade_logger, paper=False)
```

### 2.3 범시장 적용 시 모드 매핑

| Q-TRON 모드 | 한국 (키움) | 미국 (IBKR/Alpaca) | 코인 (Binance) |
|------------|-----------|-------------------|---------------|
| mock | 내부 시뮬 | 내부 시뮬 | 내부 시뮬 |
| paper | 키움 MOCK 서버 | IBKR Paper / Alpaca Paper | Binance Testnet |
| live | 키움 REAL 서버 | IBKR Live / Alpaca Live | Binance Mainnet |

**원칙**: `config.PAPER_TRADING=True` → live 서버 접속 차단. 모든 시장 동일 적용.

---

## 3. P0: 실거래 안전성

### 3.1 버그: Paper 모드 무시

**파일**: `main.py:207` (수정 전)

```python
# [수정 전] — 위험: config 무시하고 항상 실거래
is_paper = False
```

**원인**: MOCK 서버 = 키움 모의투자인데, 개발자가 "내부 시뮬레이션"과 혼동하여
모든 경우에 키움 API를 쓰도록 하드코딩.

**영향**: `PAPER_TRADING=True`로 설정해도 REAL 서버에서 실제 주문 실행 가능.

**수정**:

```python
# [수정 후] — fail-fast 가드 + 명확한 모드 분리
if config.PAPER_TRADING and server_type == "REAL":
    logger.critical("[PAPER_SAFETY] config.PAPER_TRADING=True but REAL server. ABORTING.")
    return
# paper/live 모두 키움 API 사용. 서버가 가상/실거래 결정.
executor = OrderExecutor(provider, tracker, trade_logger, paper=False)
```

**교훈**:
1. **모드 전환은 반드시 fail-fast로 구현** — warning만 띄우고 계속 진행하면 안 됨
2. **"paper"의 의미를 코드 전체에서 통일** — 내부 시뮬과 모의투자 서버를 구분
3. 모든 시장에서 동일한 패턴 적용 가능 (IBKR paper vs live, Binance testnet vs mainnet)

### 3.2 설계 원칙: 안전장치 계층

```
Layer 1: config.PAPER_TRADING (설정 파일 가드)
    → REAL 서버 + PAPER=True → abort

Layer 2: server_type 판별 (런타임 가드)
    → 키움 GetServerGubun() 리턴값으로 서버 종류 확인

Layer 3: mode_label 로깅 (관찰성)
    → 모든 주문/체결 로그에 "PAPER" 또는 "LIVE" 표시
```

---

## 4. P0: 주문/체결 상태 일관성

### 4.1 버그: Ghost Fill BUY — avg_price/cash 이중 반영

**파일**: `order_executor.py:272-296` (수정 전)

```python
# [수정 전] — 문제: avg_price에 fee 포함, cash 이중 차감
new_cost = delta_qty * exec_price * (1 + self._buy_cost)  # fee 포함
pos.avg_price = (old_cost + new_cost) / total_qty          # avg에 fee 섞임
self._portfolio.cash -= new_cost                            # cash 별도 차감

# 신규 포지션은 add_position()이 cash 차감 → 기준 불일치
```

**문제 분석**:

| 항목 | 기존 포지션 경로 | 신규 포지션 경로 | 불일치 |
|------|----------------|----------------|--------|
| avg_price | fee 포함 | fee 미포함 (순수 체결가) | O |
| cash 차감 | 직접 차감 | add_position() 내부 차감 | O |
| PnL 영향 | avg_price 부풀림 → PnL 왜곡 | 정상 | O |

**수정**:

```python
# [수정 후] — 통일 규칙: avg_price = 순수 체결가, fee는 cash에만 1회
gross_cost = delta_qty * exec_price           # 순수 비용
cash_cost = gross_cost * (1 + self._buy_cost) # fee 포함 비용

if code in self._portfolio.positions:
    pos = self._portfolio.positions[code]
    old_gross = old_qty * pos.avg_price        # 기존도 순수 기준
    pos.avg_price = (old_gross + gross_cost) / total_qty  # 순수 평균
    self._portfolio.cash -= cash_cost          # cash에서 fee 포함 1회 차감
else:
    self._portfolio.add_position(              # 내부에서 cash 차감 1회
        code, delta_qty, exec_price,
        buy_cost=self._buy_cost)               # fee 명시 전달
```

**통일 규칙**:

```
avg_price = Σ(체결수량 × 체결가) / 총수량        ← fee 미포함
cash 차감 = 체결수량 × 체결가 × (1 + buy_cost)   ← fee 포함, 1회만
PnL = (매도대금 - avg_price × 수량) / (avg_price × 수량)
```

**교훈**:
1. **회계 기준은 시스템 전체에서 한 곳에 정의** — add_position/remove_position이 유일한 진입점
2. Ghost fill, 일반 fill, 리밸런스 fill 모두 동일 경로 사용
3. **avg_price에 fee를 넣으면 PnL 계산이 전부 틀어짐** — 순수 체결가만 사용

### 4.2 버그: Overfill Guard가 정상 부분체결 차단

**파일**: `kiwoom_provider.py:934-943` (수정 전)

```python
# [수정 전] — 문제: fill_key에 누적수량 미포함
fill_key = (code, order_no, exec_qty, exec_price)
# 동일 (qty, price) 부분체결이 2번 오면 정상 체결도 무시
```

**시나리오**:

```
요청: 100주 매수
chejan 1: 30주 @ 50,000 → fill_key = ("A", "ORD1", 30, 50000)     ✓ 처리
chejan 2: 30주 @ 50,000 → fill_key = ("A", "ORD1", 30, 50000)     ✗ 무시됨!
  ↑ 동일 가격·수량의 두 번째 정상 체결이 DUP으로 처리됨
```

**수정**:

```python
# [수정 후] — 누적수량(prev_qty) 포함으로 서로 다른 부분체결 구분
fill_key = (code, order_no, exec_qty, exec_price, prev_qty)

# chejan 1: prev_qty=0  → key = ("A","ORD1",30,50000,0)   ✓ 처리
# chejan 2: prev_qty=30 → key = ("A","ORD1",30,50000,30)  ✓ 처리 (다른 key)
# chejan 1 재전송:       → key = ("A","ORD1",30,50000,0)   ✗ DUP (동일 key)
```

**추가 수정: per-order 초기화**:

```python
# 새 주문 시작 시 이전 주문의 fill key 제거
self._processed_fill_keys.clear()   # per-order fresh tracking
self._global_chejan_dedup.clear()
```

**교훈**:
1. **체결 dedup key에는 "상태 정보"를 포함해야 한다** — (qty, price)만으로는 불충분
2. 키움 API는 동일 체결 이벤트를 여러 번 전송할 수 있음 (reliability 목적)
3. **미국/코인 시장 참고**: WebSocket 기반 API도 reconnect 시 중복 이벤트 발생 가능.
   fill_id나 trade_id 기반 dedup이 필수

### 4.3 체결 처리 3중 안전장치

```
Layer 1: _processed_fill_keys
    → (code, order_no, exec_qty, exec_price, prev_qty) 정확 매칭
    → per-order 초기화

Layer 2: Overfill Guard
    → remaining = max(0, requested - prev_qty)
    → remaining <= 0이면 무시 (요청 이상 체결 방지)

Layer 3: _global_chejan_dedup
    → pre-capture 이벤트 fallback dedup
```

---

## 5. P0: 상태 저장 원자성

### 5.1 버그: Windows에서 파일 유실

**파일**: `state_manager.py:130-134` (수정 전)

```python
# [수정 전] — 위험: unlink 후 rename 실패 시 데이터 소실
if os.name == "nt":
    if path.exists():
        path.unlink()      # ← 여기서 기존 파일 삭제
os.rename(str(tmp), str(path))  # ← 여기서 실패하면 파일 없음!
```

**실패 시나리오**:

```
1. path.unlink() 성공 → 기존 state 삭제됨
2. os.rename() 실패 (권한, AV 스캔, 파일 잠금)
3. 결과: state 파일 없음 → 다음 시작 시 초기 상태로 리셋
4. 포지션/현금 정보 전부 소실
```

**수정**:

```python
# [수정 후] — os.replace()는 모든 플랫폼에서 atomic
os.replace(str(tmp), str(path))
# Windows: 대상 파일 존재해도 덮어쓰기 (atomic)
# Linux/Mac: 원래 atomic
```

**전체 atomic write 패턴**:

```python
def _atomic_write(self, path, data):
    tmp = path.with_suffix(".tmp")
    bak = path.with_suffix(".bak")

    # 1. tmp에 쓰기
    tmp.write_text(json.dumps(data))

    # 2. tmp가 유효한 JSON인지 검증
    verify = json.loads(tmp.read_text())
    if not isinstance(verify, dict):
        raise ValueError("Verification failed")

    # 3. 기존 파일 백업
    if path.exists():
        shutil.copy2(path, bak)

    # 4. 원자적 교체
    os.replace(str(tmp), str(path))
```

**교훈**:
1. **Windows에서 `os.rename()`은 대상 파일 존재 시 실패** — `os.replace()` 사용 필수
2. 쓰기 전 검증 → 백업 → 교체 순서는 모든 상태 파일에 적용
3. **미국/코인 참고**: 24시간 운영 시 전원 차단, OOM 킬 등에서도 상태 보존 필수.
   SQLite WAL 모드나 append-only log도 고려 가능

---

## 6. P0: 계좌 동기화 안전장치

### 6.1 버그: holdings 불신뢰 상태에서 거래 계속 진행

**파일**: `main.py` — `_reconcile_with_broker()` 내부

```python
# [수정 전] — 위험: holdings 불신뢰인데 ok=True 반환
if summary.get("holdings_reliable") is False:
    logger.warning("Broker holdings unreliable — cash only sync")
    portfolio.cash = broker_cash
    return {"ok": True, "corrections": 0, "safe_mode": False}
    # ↑ 이후 리밸런스/매수/매도가 정상 진행됨!
```

**영향**:

```
holdings_reliable=False (키움 msg_rejected 등)
→ 포지션 정보 신뢰 불가
→ 그런데 리밸런스 진행
→ 없는 포지션 매도 시도 / 이미 보유 중인데 매수 판단
→ 중복 매수, 빈 매도, 수량 불일치
```

**수정**:

```python
# [수정 후] — safe_mode=True 반환 → 세션 전체 monitor-only
if summary.get("holdings_reliable") is False:
    logger.critical("[BROKER_STATE_UNRELIABLE] Holdings unreliable. "
                    "Rebalance and orders BLOCKED for this session.")
    return {"ok": True, "corrections": 0,
            "safe_mode": True,
            "safe_mode_reason": "holdings_unreliable — monitor-only session"}

# 호출부에서:
if recon.get("safe_mode") and "holdings_unreliable" in reason:
    session_monitor_only = True  # 리밸런스/매매 전면 차단

# 리밸런스 진입 전 가드:
if need_rebalance and session_monitor_only:
    logger.critical("[MONITOR_ONLY] Session is MONITOR-ONLY. Skipping rebalance.")
    need_rebalance = False
```

### 6.2 Reconciliation 전체 흐름

```
                  broker.query_account_summary()
                            │
                 ┌──────────▼──────────┐
                 │ holdings_reliable?   │
                 └──┬───────────────┬──┘
                    │ False         │ True
                    ▼               ▼
         [MONITOR-ONLY]    ┌───────────────┐
         cash만 sync       │ Cash sync      │
         매매 전면 차단     │ (spike 감지)    │
                           └───────┬───────┘
                                   ▼
                          ┌────────────────┐
                          │ Holdings sync   │
                          │ ENGINE-ONLY:삭제│
                          │ BROKER-ONLY:추가│
                          │ BOTH: qty/avg   │
                          │   sync (spike   │
                          │   감지 포함)     │
                          └───────┬────────┘
                                  ▼
                         corrections > 10?
                         cash_spike + corr > 5?
                              │ Yes
                              ▼
                       [SAFE MODE 진입]
                       신규 매수 차단
                       보호 매도만 허용
```

### 6.3 Spike Detection 임계값

| 항목 | 임계값 | 동작 |
|------|--------|------|
| 현금 변동 | 50% 이상 | CRITICAL 로그 + 카운트 |
| 수량 변동 | 100% 이상 | 교정 차단 (수동 확인 필요) |
| 총 교정 수 | 10건 초과 | SAFE MODE 진입 |
| 현금 spike + 교정 | 50% + 5건 | SAFE MODE 진입 |

**교훈**:
1. **브로커가 진실의 원천(source of truth)** — 내부 상태가 아니라 브로커 데이터 기준으로 동기화
2. 하지만 **무조건 믿으면 안 됨** — 브로커 데이터도 일시적 오류 가능
3. Spike detection으로 "너무 큰 변화"를 차단하고 수동 확인 요구
4. **미국/코인 참고**: REST API 응답 지연, 일부 데이터만 반환 등 동일 문제 발생.
   항상 "데이터 신뢰도" 플래그를 체크해야 함

---

## 7. P1: 리밸런스 커밋 원자성

### 7.1 버그: 리밸런스 마킹과 포트폴리오 저장의 분리

**파일**: `main.py:388-398` (수정 전)

```python
# [수정 전] — 두 저장이 독립적
state_mgr.set_last_rebalance_date(today_str)    # runtime.json 저장
_safe_save(state_mgr, portfolio, context="...")   # portfolio.json 저장
# 문제: runtime 성공 + portfolio 실패 → "리밸런스 완료"인데 포트 미반영
```

**수정**:

```python
# [수정 후] — 포트폴리오 먼저, 성공 시에만 마킹
portfolio_saved = _safe_save(state_mgr, portfolio,
                             context="rebalance_commit/portfolio")
if portfolio_saved:
    state_mgr.set_last_rebalance_date(today_str)
    logger.info("[REBALANCE_COMMIT_OK]")
else:
    logger.critical("[REBALANCE_COMMIT_PARTIAL_FAIL] "
                    "Rebalance date NOT marked — will retry next session.")
```

**상태 다이어그램**:

```
리밸런스 실행
    │
    ├─ 매도 완료 → [POST-SELL CHECKPOINT] 포트 저장 (날짜 미마킹)
    │   ↑ 여기서 크래시 → 다음 세션에서 리밸런스 재시도
    │     이미 매도된 종목은 compute_orders에서 set diff로 제외됨
    │
    ├─ 매수 완료 → [REBALANCE_COMMIT]
    │   ├─ 포트 저장 성공 → 날짜 마킹 → [COMMIT_OK]
    │   └─ 포트 저장 실패 → 날짜 미마킹 → [PARTIAL_FAIL] → 다음 세션 재시도
    │
    └─ 크래시 → 포트 저장 (매도 결과 보존) + 날짜 미마킹 → 재시도
```

**교훈**:
1. **두 개의 상태 파일을 "트랜잭션"처럼 묶어야 함** — DB가 아닌 파일 기반이라 완전한 ACID는 불가하지만, 순서로 일관성 확보
2. **멱등성(idempotency) 설계**: 재시도 시 이미 매도된 종목이 다시 매도되지 않도록 compute_orders가 set diff 사용
3. **미국/코인 참고**: 특히 24시간 시장에서 리밸런스 중간 크래시 확률이 높음.
   모든 중간 상태를 복구 가능하게 설계해야 함

---

## 8. P1: 주문 저널 (크래시 포렌식)

### 8.1 설계: JSONL Append-Only Log

**파일**: `order_tracker.py`

```python
class OrderTracker:
    def __init__(self, journal_dir=None):
        self._journal_path = journal_dir / f"order_journal_{session_id}.jsonl"

    def _journal_write(self, event, **kwargs):
        """Best-effort append — never raises (trading 차단 금지)."""
        entry = {"ts": datetime.now().isoformat(),
                 "session_id": self._session_id,
                 "event": event, **kwargs}
        with open(self._journal_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
```

### 8.2 이벤트 타입

| 이벤트 | 발생 시점 | 기록 필드 |
|--------|----------|----------|
| `SUBMIT_ATTEMPT` | 주문 등록 | order_id, code, side, requested_qty, reason |
| `SUBMITTED` | 키움 접수 | order_id, code, side |
| `FILLED` | 전량 체결 | order_id, code, side, exec_qty, exec_price |
| `PARTIAL_FILLED` | 부분 체결 | order_no, code, side, exec_qty, cumulative_qty |
| `REJECTED` | 주문 거부 | order_id, code, side, reason |
| `TIMEOUT_UNCERTAIN` | 체결 미확인 | order_id, code, side, requested_qty |
| `GHOST_FILLED` | 지연 체결 | order_no, code, side, exec_qty, exec_price |

### 8.3 사용법: 크래시 후 복구

```bash
# 마지막 세션의 미확인 주문 확인
cat logs/order_journal_20260324_*.jsonl | \
  python -c "
import json, sys
for line in sys.stdin:
    e = json.loads(line)
    if e['event'] in ('TIMEOUT_UNCERTAIN', 'GHOST_FILLED'):
        print(f\"{e['ts']} {e['event']} {e.get('code','')} qty={e.get('exec_qty', e.get('requested_qty', '?'))}\")
"
```

**교훈**:
1. **저널은 절대 트레이딩을 차단하면 안 됨** — `except: pass` 처리
2. JSONL은 append-only라 corrupt 위험 최소화
3. 세션별 파일 분리로 로그 관리 용이
4. **미국/코인 참고**: 24시간 시장에서 저널 파일 크기 관리 필요.
   일별 rotation 또는 max size 제한 추가 고려

---

## 9. P1: CSV 스키마 마이그레이션

### 9.1 버그: Silent Padding으로 데이터 오염

**파일**: `reporter.py:96-128` (수정 전)

```python
# [수정 전] — 컬럼 수 불일치 시 빈 문자열로 패딩
if old_data.shape[1] < len(expected_columns):
    for i in range(old_data.shape[1], len(expected_columns)):
        old_data[i] = ""  # ← 의미 없는 빈 값으로 채움
old_data = old_data.iloc[:, :len(expected_columns)]  # ← 초과 컬럼 잘라냄
```

**문제**: 9컬럼 데이터에 14컬럼 헤더 적용 → 5개 컬럼이 빈 문자열 → 분석 시 0으로 해석되거나 파싱 에러

**수정**:

```python
# [수정 후] — 백업 + 새 파일 생성 (데이터 보존, 오염 방지)
if len(header) != len(expected_columns):
    backup = path.with_suffix(".mismatch_backup")
    shutil.copy2(path, backup)
    logger.error("[CSV_HEADER_MISMATCH_FATAL] %s: %d→%d cols. "
                 "Backed up to %s. Starting fresh.", ...)
    self._write_header(path, expected_columns)
```

**교훈**:
1. **데이터 무결성 > 연속성** — 오염된 데이터보다 빈 파일이 낫다
2. 원본은 반드시 백업 (`.mismatch_backup`)
3. 스키마 변경은 명시적으로 관리 — version 필드 추가 고려
4. **미국/코인 참고**: 실시간 데이터 양이 많아 CSV 대신 Parquet/SQLite 고려

---

## 10. P1: EOD 가격 신뢰성

### 10.1 설계: 가격 소스 계층 + 실행 제한

**파일**: `main.py` — EOD trail stop 처리부

```python
# 가격 소스 우선순위
Priority 1: intraday_last_close (당일 검증된 장중 종가)
Priority 2: provider_cached (키움 캐시 가격)
Priority 3: position_fallback (포지션 마지막 가격)
Priority 4: unavailable (가격 없음)

# 실행 제한: 공식 종가가 아니면 trail stop 실행 안 함
if price_source in ("provider_cached", "position_fallback"):
    logger.warning("[EOD_SKIP_NO_OFFICIAL_CLOSE] trail stop SKIPPED")
    # HWM만 업데이트 (관찰용), 청산 결정 안 함
    continue
```

### 10.2 가격 소스별 신뢰도

| 소스 | 신뢰도 | 의사결정 허용 | 설명 |
|------|--------|-------------|------|
| `intraday_last_close` | 높음 | trail stop 실행 O | 당일 분봉 마지막 close |
| `provider_cached` | 중간 | HWM 업데이트만 | 키움 마스터 가격 (언제 것인지 불명확) |
| `position_fallback` | 낮음 | HWM 업데이트만 | 장중 마지막 갱신 가격 |
| `unavailable` | 없음 | 전부 스킵 | 가격 조회 실패 |

**[행동 변경]**: 이 수정은 일부 청산 타이밍에 영향을 줄 수 있음.
Cached 가격으로 trail stop이 발동했을 실제 사례가 있었다면, 그 매도가 발생하지 않게 됨.
**실전 기준으로는 더 안전한 방향**.

**교훈**:
1. **가격 소스의 "품질"을 추적해야 함** — 단순 float가 아니라 (price, source, timestamp) 튜플
2. 의사결정에 사용할 가격과 관찰용 가격을 명확히 분리
3. **미국 참고**: 15분 지연 데이터 vs 실시간 데이터 구분 필요
4. **코인 참고**: 거래소별 가격 차이 → 어느 거래소 가격으로 판단할지 정책 필요

---

## 11. P1: 현금 추정과 매수 사이징

### 11.1 설계: 보수적 현금 추정 + 버퍼

**파일**: `rebalancer.py`

```python
# 매도 대금 추정 (현재가 기반 — 실제와 다를 수 있음)
estimated_cash = current_cash
for sell_order in sell_orders:
    estimated_cash += qty * price * (1 - sell_cost)

# 매수 사이징: estimated_cash * buffer 이내로 제한
alloc = min(target_alloc, estimated_cash * cash_buffer)
qty = int(alloc / (price * (1 + buy_cost)))
```

### 11.2 CASH_BUFFER_RATIO

**파일**: `config.py`

```python
CASH_BUFFER_RATIO: float = 0.95  # 매수 할당을 추정 현금의 95%로 제한
```

**존재 이유**:
- 매도 → 매수 사이 가격 변동
- 부분 체결로 예상보다 적은 대금 수령
- 슬리피지
- 수수료 차이

**교훈**:
1. 현금 추정은 **항상 근사치** — 정확한 값은 매도 체결 후에만 알 수 있음
2. 버퍼는 config로 관리하여 시장별 조정 가능
3. 로그에 "추정치임"을 명시: `[REB_CASH_ESTIMATE] ... NOTE: approximate`
4. **코인 참고**: maker/taker 수수료 차이가 크므로 buffer를 더 넉넉하게 설정 필요

---

## 12. P2: 리스크 엔진 (검증 보류 항목)

### 12.1 DD Graduated Response (5단계)

```python
DD_LEVELS = (
    (-0.25, 0.00, 0.20, "DD_SAFE_MODE"),   # -25% 이하: 매수 0%, 트림 20%
    (-0.20, 0.00, 0.20, "DD_SEVERE"),       # -20% 이하: 매수 0%, 트림 20%
    (-0.15, 0.00, 0.00, "DD_CRITICAL"),     # -15% 이하: 매수 0%
    (-0.10, 0.50, 0.00, "DD_WARNING"),      # -10% 이하: 매수 50%
    (-0.05, 0.70, 0.00, "DD_CAUTION"),      # -5% 이하:  매수 70%
)
```

### 12.2 검증 필요 항목 (즉시 수정 금지)

| # | 항목 | 현재 동작 | 우려 | 상태 |
|---|------|----------|------|------|
| 1 | 월초 DD 리셋 | `peak_equity = 현재 equity` | 월초 하락 시 DD=0%로 시작 | 테스트 추가 완료 |
| 2 | Trim dedup | 같은 날 1회 제한 | 더 심각한 레벨 trim도 차단 | 검증 필요 |
| 3 | Safe mode 해제 | 당일 해제 금지 | 과도하게 제한적일 수 있음 | 검증 필요 |

**원칙**: 리스크 엔진 동작 변경은 전략 영향 검증 후 결정.
테스트로 현재 동작을 문서화한 후 변경 여부 판단.

### 12.3 Safe Mode Hysteresis

```
진입: monthly_dd <= -25% (DD_SAFE_MODE 레벨)
해제: monthly_dd > -20% (별도 release threshold)
      + 당일 진입이 아닐 것 (anti-flapping)

    DD%
    0% ─────────────────────────────────────
   -5% ─ ─ ─ CAUTION ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
  -10% ─ ─ ─ WARNING ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
  -15% ─ ─ ─ CRITICAL ─ ─ ─ ─ ─ ─ ─ ─ ─
  -20% ═══════ RELEASE ═══════════════════  ← 해제 기준
  -25% ═══════ ENTRY ════════════════════   ← 진입 기준
       │     │         │              │
       진입    유지      유지→해제        재진입
```

---

## 13. P2: 전략 수치 검증 (변경 보류 항목)

### 13.1 Momentum 12-1 인덱싱

**파일**: `scoring.py:62-63`

```python
# 현재 구현
c_skip = c[-skip]      # skip=22 → c[-22] (끝에서 22번째)
c_12m  = c[-lookback]  # lookback=252 → c[-252] (끝에서 252번째)
momentum = c_skip / c_12m - 1
```

**의도**: "최근 1개월 제외 12개월 모멘텀"
- c[-22] = 22거래일 전 가격 (≈ 1개월 전)
- c[-252] = 252거래일 전 가격 (≈ 12개월 전)
- momentum = (1개월 전 가격 / 12개월 전 가격) - 1

**검증 테스트 (test_stability.py Test 9)**:
```python
# 300일 선형 시계열로 검증
prices = pd.Series(np.linspace(100, 200, 300))
mom = calc_momentum(prices, lookback=252, skip=22)
expected = prices.values[-22] / prices.values[-252] - 1
assert abs(mom - expected) < 1e-10  # ✓ PASS
```

**결론**: 현재 구현은 standard 12-1 정의와 일치.
off-by-one 의심은 해소되었으나, **생산 코드 변경 없이 테스트로 확인 완료**.

### 13.2 Backtest vs Live 실행 기준 차이

| 항목 | Backtest | Live |
|------|----------|------|
| 매수 시점 | T+1 open | 당일 시장가 |
| 종가 기준 | 일봉 close | intraday last close |
| Trail stop | 일봉 close 기반 | EOD 15:30 close 기반 |

**현재 정책**: 차이를 인지하되, 구조 변경 없이 로그/테스트로 차이를 명시.
라이브 성과가 백테스트보다 약간 다를 수 있음을 전제로 운영.

---

## 14. 범시장 적용 가이드 (미국/코인)

### 14.1 시장별 차이점과 대응

| 항목 | 한국 (키움) | 미국 (IBKR/Alpaca) | 코인 (Binance) |
|------|-----------|-------------------|---------------|
| **거래시간** | 09:00-15:30 | 09:30-16:00 ET | 24/7 |
| **체결 통보** | COM chejan 이벤트 | WebSocket / REST poll | WebSocket |
| **모의투자** | 키움 MOCK 서버 | Paper Trading API | Testnet |
| **수수료** | 0.015% + 세금 0.18% | $0 (주요 브로커) | 0.1% (maker/taker) |
| **슬리피지** | 0.10% | 종목별 상이 | 유동성에 따라 큼 |
| **결제 주기** | T+2 | T+1 (2024~) | 즉시 |
| **API 한도** | TR 0.5초 제한 | Rate limit (분당) | Weight 기반 제한 |
| **재시작** | 일 1회 세션 | 일 1회 | 연속 운영 |

### 14.2 반드시 재구현해야 할 패턴

#### (1) 실행 모드 안전장치 (TRADING_MODE 체계)

```python
# 모든 시장 공통 패턴 — validate_trading_mode()
# TRADING_MODE is the operator's intended mode.
# server_type is the broker's actual connected environment.
# If they do not match, abort immediately.
#
# 모드 정책 표:
# ┌──────────┬────────────┬───────────┬──────────┬────────────┐
# │ MODE     │ broker연결  │ server    │ simulate │ 주문가능    │
# ├──────────┼────────────┼───────────┼──────────┼────────────┤
# │ mock     │ X          │ N/A       │ True     │ 내부시뮬만  │
# │ paper    │ O          │ MOCK      │ False    │ 모의투자    │
# │ live     │ O          │ REAL      │ False    │ 실거래      │
# └──────────┴────────────┴───────────┴──────────┴────────────┘
#
# 불일치 시: [MODE_MISMATCH_ABORT]
validate_trading_mode(config.TRADING_MODE, server_type, broker_connected)
    logger.critical("[PAPER_SAFETY] ABORTING")
    return
```

#### (2) 체결 Dedup

```python
# 한국: chejan 이벤트 중복
# 미국: WebSocket reconnect 시 중복 메시지
# 코인: WebSocket 재연결 + REST 폴링 중복

# 범시장 dedup 패턴
fill_key = (order_id, exec_qty, exec_price, cumulative_qty)
if fill_key in seen_fills:
    return  # 중복
seen_fills.add(fill_key)
```

#### (3) avg_price 통일 규칙

```python
# 범시장 공통: avg_price = 순수 체결가 (fee 미포함)
avg_price = Σ(체결수량 × 체결가) / 총수량
cash_deduction = 체결수량 × 체결가 × (1 + fee_rate)  # 1회만
```

#### (4) 상태 저장 원자성

```python
# 범시장 공통: tmp → verify → backup → os.replace
# 24시간 시장은 더 빈번한 저장 + 저널 필수
```

#### (5) 브로커 동기화 가드

```python
# 범시장 공통: 브로커 데이터 신뢰도 체크
if not broker.holdings_reliable():
    session.set_monitor_only()
    logger.critical("[BROKER_STATE_UNRELIABLE]")
```

### 14.3 시장별 추가 고려사항

#### 미국 시장

```
- Pre/After Market 시간대 처리
- Fractional shares (소수점 주식) 지원
- 배당/스플릿 자동 반영 (Corporate Actions)
- PDT Rule (Pattern Day Trader) 가드
- T+1 결제: 매도 후 당일 매수 가능 여부 확인
```

#### 코인 시장

```
- 24/7 운영: 리밸런스 시점 정의 필요 (UTC 00:00? 한국 시간?)
- 극단적 변동성: trail stop 12% → 더 넓게? 더 좁게?
- 거래소 장애: 다중 거래소 fallback
- 네트워크 수수료 (Gas fee): 온체인 전송 시 추가 비용
- Funding rate: 선물 포지션 유지 비용
- Decimal precision: 8자리 소수점 처리
```

### 14.4 확장 시 아키텍처 권장사항

```
┌──────────────────────────────────────────┐
│              Strategy Layer              │  ← 시장 독립
│  scoring, factor_ranker, rebalancer      │
│  trail_stop, exposure_guard              │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│           Execution Layer                │  ← 시장별 구현
│  ┌─────────┐ ┌──────────┐ ┌───────────┐ │
│  │ Kiwoom  │ │  IBKR    │ │  Binance  │ │
│  │Provider │ │Provider  │ │ Provider  │ │
│  └─────────┘ └──────────┘ └───────────┘ │
└──────────────┬───────────────────────────┘
               │
┌──────────────▼───────────────────────────┐
│           Infrastructure Layer           │  ← 시장 독립
│  state_manager, order_tracker,           │
│  reporter, config                        │
└──────────────────────────────────────────┘
```

---

## 15. 로그 태그 사전

### Critical (즉시 확인 필요)

| 태그 | 의미 | 대응 |
|------|------|------|
| `[PAPER_SAFETY]` | PAPER 설정 + REAL 서버 → 중단 | config 확인 |
| `[BROKER_STATE_UNRELIABLE]` | 계좌 데이터 불신뢰 | 키움 HTS 확인 |
| `[REBALANCE_COMMIT_PARTIAL_FAIL]` | 포트 저장 실패 → 날짜 미마킹 | 디스크/권한 확인 |
| `[RECON_CASH_SPIKE]` | 현금 50%+ 변동 | HTS에서 수동 확인 |
| `[RECON_QTY_SPIKE]` | 수량 100%+ 변동 → 교정 차단 | 수동 확인 후 재실행 |
| `[RECON_SAFETY]` | 교정 10건+ → SAFE MODE | 다음 세션에서 확인 |
| `[GHOST_STATE_SAVE_FAIL]` | 고스트 체결 후 저장 3회 실패 | 즉시 확인 |

### Error (운영 영향)

| 태그 | 의미 | 대응 |
|------|------|------|
| `[CSV_HEADER_MISMATCH_FATAL]` | CSV 스키마 불일치 → 백업+새파일 | 백업 파일 확인 |
| `[GHOST_FILL_ERROR]` | 고스트 체결 처리 예외 | 로그 상세 확인 |
| `[OVERFILL_IGNORED]` | 요청 초과 체결 무시 | 정상 (dedup 동작) |

### Warning (관찰 필요)

| 태그 | 의미 | 대응 |
|------|------|------|
| `[EOD_SKIP_NO_OFFICIAL_CLOSE]` | 비공식 가격 → trail stop 스킵 | 정상 (안전 우선) |
| `[MONITOR_ONLY]` | 세션 관찰 전용 | 다음 세션에서 해소 |
| `[REB_CASH_ESTIMATE]` | 현금 추정치 로그 | 참고용 |
| `[STALE_PRICE_WARNING]` | 10분+ 가격 미갱신 | 시세 연결 확인 |
| `[EQUITY_STALE]` | 3+ 사이클 동일 equity | 시세 연결 확인 |
| `[REB_WARN]` | 일부 매수 스킵 | 현금 부족 또는 가격 미조회 |

### Info (정상 운영)

| 태그 | 의미 |
|------|------|
| `[REBALANCE_COMMIT_OK]` | 리밸런스 정상 완료 |
| `[GHOST_FILL_APPLIED]` | 고스트 체결 반영 완료 |
| `[GHOST_PORTFOLIO_SYNCED]` | 고스트 체결 후 포트/상태 동기화 완료 |
| `[STATE_SAVE_OK]` | 상태 저장 성공 |
| `[DD_TRIM_DONE]` | DD 트림 완료 |
| `[FILL]` / `[PARTIAL]` | 정상 체결 / 부분 체결 |
| `[EOD_PRICE_SOURCE]` | EOD 가격 소스 분포 |

---

## 16. 테스트 시나리오 카탈로그

### 필수 테스트 (P0 안전)

| # | 시나리오 | 검증 항목 | 파일 |
|---|---------|----------|------|
| 1 | PAPER_TRADING=True + REAL 서버 | abort 발생, 주문 없음 | test_stability.py |
| 2 | Ghost BUY 기존 보유 (10주@10k + 5주@12k) | avg=(10*10k+5*12k)/15, cash 1회 차감 | test_stability.py |
| 3 | Ghost BUY 신규 진입 | add_position 1회로 처리, 이중 차감 없음 | test_stability.py |
| 4 | Windows atomic write | os.replace 사용, unlink 없음, 실패 시 기존 보존 | test_stability.py |
| 5 | 부분체결 3회 (30+40+30=100) | 전량 반영, overfill 없음, DUP 재전송만 무시 | test_stability.py |
| 6 | holdings_reliable=False | safe_mode=True, monitor-only, 매매 차단 | test_stability.py |

### 필수 테스트 (P1 복원)

| # | 시나리오 | 검증 항목 | 파일 |
|---|---------|----------|------|
| 7 | 리밸런스 포트 저장 실패 | 날짜 미마킹, 다음 세션 재시도 | test_stability.py |
| 8 | CSV 헤더 컬럼 수 불일치 | auto-padding 금지, 백업+새파일 | test_stability.py |

### 권장 테스트 (P2 검증)

| # | 시나리오 | 검증 항목 | 파일 |
|---|---------|----------|------|
| 9 | Momentum 12-1 인덱싱 | 합성 시계열로 기대값 확인 | test_stability.py |
| 10 | Monthly DD 월초 리셋 | peak_equity 리셋 동작 문서화 | test_stability.py |

### 미구현 테스트 (향후 추가)

| # | 시나리오 | 우선순위 |
|---|---------|---------|
| 11 | Ghost order 매칭 시간 경계 (order_no 재사용) | 중간 |
| 12 | Chejan 전일 체결 도착 (delay 음수) | 중간 |
| 13 | Trim dedup — 같은 날 더 심각한 레벨 | 검증 후 |
| 14 | Safe mode 당일 해제 시도 | 검증 후 |
| 15 | 동시 ghost fill 2건 (reentrant lock) | 높음 |

---

## 부록 A: 수정 이력 요약

| 일자 | 수정 | 파일 | 분류 | 전략 영향 |
|------|------|------|------|----------|
| 2026-03-24 | Paper 모드 fail-fast | main.py | P0 | 없음 |
| 2026-03-24 | Ghost fill avg_price/cash 통일 | order_executor.py | P0 | 없음 (회계 수정) |
| 2026-03-24 | Atomic write os.replace | state_manager.py | P0 | 없음 |
| 2026-03-24 | Holdings unreliable 차단 | main.py | P0 | 없음 (안전장치) |
| 2026-03-24 | Overfill guard 보강 | kiwoom_provider.py | P0 | 없음 |
| 2026-03-24 | Rebalance commit 순서 | main.py | P1 | 없음 |
| 2026-03-24 | Order journal JSONL | order_tracker.py | P1 | 없음 |
| 2026-03-24 | CSV silent padding 제거 | reporter.py | P1 | 없음 |
| 2026-03-24 | EOD fallback price 스킵 | main.py | P1 | **있음** (청산 타이밍) |
| 2026-03-24 | Cash buffer config 승격 | config.py, rebalancer.py | P1 | 없음 |
| 2026-03-24 | 모드 용어 통일 (mock/paper/live) | main.py | P0 | 없음 |
| 2026-03-24 | Backtest --end 동적화 | main.py | 편의 | 없음 |
| 2026-03-24 | **TRADING_MODE 체계 도입** | config, main, executor, state_mgr, tracker | P0 | **있음** (모드 불일치 abort) |
| 2026-03-24 | config.TRADING_MODE="mock"\|"paper"\|"live" | config.py | P0 | 없음 |
| 2026-03-24 | PAPER_TRADING deprecated → TRADING_MODE | config.py | P0 | 없음 |
| 2026-03-24 | validate_trading_mode() hard gate | main.py | P0 | **있음** (불일치 즉시 중단) |
| 2026-03-24 | OrderExecutor paper= deprecated → simulate= | order_executor.py | P0 | 없음 |
| 2026-03-24 | OrderExecutor._check_broker_gate() 2차 gate | order_executor.py | P0 | **있음** (주문 직전 검증) |
| 2026-03-24 | 상태파일 3분리 (mock/paper/live) | state_manager.py | P0 | **있음** (파일 분리) |
| 2026-03-24 | 레거시 상태파일 자동 migration | state_manager.py | P0 | 없음 |
| 2026-03-24 | order journal에 trading_mode 필드 추가 | order_tracker.py | P1 | 없음 |

## 부록 B: 코드 관례 (Conventions)

### 로그 형식

```python
# 태그는 대괄호, 대문자, 밑줄 구분
logger.critical("[PAPER_SAFETY] 설명")
logger.error("[CSV_HEADER_MISMATCH_FATAL] 설명")
logger.warning("[EOD_SKIP_NO_OFFICIAL_CLOSE] 설명")
logger.info("[REBALANCE_COMMIT_OK] 설명")
```

### 상태 저장 패턴

```python
# 항상 _safe_save() 사용 (재시도 포함)
saved = _safe_save(state_mgr, portfolio, context="어디서/왜")
if not saved:
    logger.error("...")
```

### 비용 처리 관례

```python
# avg_price: 순수 체결가 (fee 미포함)
# cash 차감: 체결가 × (1 + buy_cost)  — 1회만
# PnL 계산: (매도대금 - cost_basis) / cost_basis
#   매도대금 = qty × price × (1 - sell_cost)
#   cost_basis = qty × avg_price
```

### 안전장치 설계 원칙

```
1. Fail-fast > Warning — 위험한 상태에서 warning만 띄우고 계속 진행하지 말 것
2. 브로커 = 진실의 원천 — 내부 상태가 아니라 브로커 기준으로 동기화
3. 상태 저장 = 원자적 — tmp → verify → backup → replace
4. 재시도 가능 설계 — 중간 크래시 후 다음 세션에서 복구 가능
5. 로그 = silent failure 금지 — 모든 비정상 경로에 명시적 로그
6. 의사결정 가격 ≠ 관찰 가격 — 공식 종가만 청산 판단에 사용
7. Dedup은 다중 레이어 — 단일 dedup으로는 모든 중복을 잡을 수 없음
8. 모드 불일치 = 즉시 중단 — TRADING_MODE와 server_type 불일치 시 abort
9. 상태 격리 — mock/paper/live 상태 파일을 분리하여 상호 오염 방지
10. 2중 gate — 세션 시작 시 + 주문 직전 모두 모드 검증
```

---

---

## 2026-03-26 안정화 세션 — 리밸 사이클 검증 + Ghost Fill 수정

### 개요
- **목표**: 리밸런스 → 매도 → 매수 풀 사이클을 PAPER_TEST 환경에서 검증하고, ghost fill / partial timeout 상태기계 결함을 수정
- **결과**: 풀 사이클 완주 성공 (리밸 → 매도 4건 → 5분 대기 → 매수 4건 → EOD 리포트)

---

### FIX-1: 거래일 카운팅 오류 (KOSPI.csv 의존)

**근거**: 리밸 조건이 21거래일인데, `_count_trading_days()`가 KOSPI.csv를 캘린더로 사용. 이 파일이 EOD에서만 업데이트되어 장 시작 시점에 항상 1일 이상 부족. 실제 23거래일인데 19일로 오판 → 리밸 미트리거.

**조치**: `_count_trading_days()`에 pykrx 캘린더 fallback 추가. KOSPI.csv 부족 시 `pykrx.stock.get_market_trading_days()` 호출.

**결과**: `[TRADING_DAYS] pykrx: 23 days` 정상 인식 → 리밸 트리거 성공.

---

### FIX-2: collector NameError (리밸 경로에서만 크래시)

**근거**: `IntradayCollector`가 리밸 이후(line 632)에 초기화되는데, trail pre-check 코드(line 543)가 리밸 내부에서 `collector.get_last_prices()` 호출. 비리밸 날에는 이 경로를 안 타서 미발견 → 리밸 날에만 `UnboundLocalError`.

**조치**: `try/except NameError`로 collector 미초기화 시 빈 dict 반환.

**결과**: 리밸 경로에서 크래시 없이 정상 진행.

---

### FIX-3: PARTIAL_TIMEOUT 후 ghost fill 미반영 (핵심 버그)

**근거**: PARTIAL_TIMEOUT 발생 시 `_order_state["status"]`가 `"PARTIAL"`로 남아있어, 이후 chejan 이벤트가 active path에서 처리됨. ghost path를 우회하여 portfolio에 반영 안 됨. 결과: engine holdings ≠ broker holdings.

로그 증거 (069960):
```
timeout: applied=17/271
이후 chejan: cum_filled=271 remain=0 → 하지만 [GHOST_FILL] 로그 없음
재시작 시: [RECON] ENGINE_ONLY 069960: 201 → 0
```

**조치**:
1. `kiwoom_provider.py`: PARTIAL_TIMEOUT 시 `_order_state["status"] = "PARTIAL_TIMEOUT"` 설정 → active matching 조건(`"REQUESTED", "ACCEPTED", "PARTIAL"`) 통과 불가 → 이후 chejan은 ghost path로만 라우팅
2. Ghost path에 `GHOST_FILLING` → `GHOST_FILLED` 상태 추가, delta 기반 portfolio 반영
3. `applied_qty` 추적으로 중복 반영 방지
4. Terminal 시 `[GHOST_FILL_FINALIZED]` 로그 + `is_terminal` 플래그 callback 전달

**결과**:
```
[GHOST_FILL] SELL 003690 delta=140 applied=157 status=GHOST_FILLING
[GHOST_FILL] SELL 003690 delta=267 applied=424 status=GHOST_FILLING
...
[GHOST_FILL_FINALIZED] SELL 003690 filled=1893/1893
```

---

### FIX-4: sell_status PARTIAL → COMPLETE 자동 승격

**근거**: ghost fill이 전량 완료되어도 `rebal_sell_status`가 `PARTIAL`로 저장됨 → 다음 세션에서 pending buy가 영구 블로킹.

로그 증거:
```
[PENDING_BUY_BLOCKED_UNSETTLED_REBAL] sell_status=PARTIAL, 7 buys blocked
```

**조치**:
1. `order_executor.py`: ghost terminal 시 `_try_upgrade_sell_status()` 호출
2. 모든 ghost가 `GHOST_FILLED` → runtime state에 `rebal_sell_status = COMPLETE` 저장
3. pending_buys 파일의 sell_status도 동기화 (`_reconcile_sell_status_on_load`)

**결과**: `[PENDING_BUY_LOAD] 7 buys, sell_status=COMPLETE` → 매수 정상 실행.

---

### FIX-5: logging 포맷 에러

**근거**: `logger.critical('[RECON_CASH_SPIKE] %,.0f -> %,.0f')` — Python logging의 `%` 포매팅은 콤마 구분자 미지원 → `ValueError: unsupported format character ','`.

**조치**: f-string으로 변경.

**결과**: 로그 에러 제거.

---

### ADD-1: PAPER_TEST 모드 + Fast Reentry (테스트 인프라)

**근거**: 리밸 → 매도 → T+1 매수 사이클을 테스트하려면 최소 2일 필요. 개발 속도를 위해 동일 세션 내 5분 후 매수 실행 필요.

**조치**:
1. `--paper-test` 모드: test CSV + test state 파일 사용
2. `--cycle full/sell_only/buy_only`: 매도/매수 레이어 분리
3. `--fresh`: state 파일 자동 삭제 → broker sync clean start
4. `PAPER_TEST_FAST_REENTRY`: 매도 완료 후 300초 대기 → 모니터 루프에서 매수 실행
5. `force_rebalance`: 같은 날 리밸 dedup 스킵 (paper_test 전용)
6. LIVE 보호 가드: test 잔재 감지 시 `[LIVE_BLOCKED_TEST_RESIDUE]` → 즉시 종료

**결과**: 풀 사이클 10분 내 완주.
```
13:11:02 매도 시작 → 13:11:57 매도 완료 (COMPLETE)
13:16:59 5분 후 매수 시작 → 13:17:41 매수 완료
13:17:41 [PENDING_BUY_COMPLETE] cash_remaining=490,511
15:30:10 EOD complete. Daily report generated.
```

---

### ADD-2: EOD 가격 Prefetch (fb 종목 trail skip 방지)

**근거**: fast reentry로 추가된 종목은 실시간 tick 미등록 → fb(fallback) → `TRAIL_SKIP_1D`. 오늘 로그에서 4종목 trail 평가 스킵됨.

**조치**:
1. EOD 직전 fb 종목을 `GetMasterLastPrice`로 재조회
2. 장 마감 후 조회 = 당일 종가 → `eod_master_close` source로 승격
3. trail 평가에서 verified source로 인정 (skip 리스트에 미포함)

**결과**: `TRAIL_SKIP_1D` → 0건 예상 (다음 실행에서 검증).

---

### ADD-3: 로그 품질 개선

**근거**: ghost fill 로그가 `CRITICAL` → 정상 동작이므로 과도. Chejan fallthrough 로그가 매 이벤트마다 출력 → spam.

**조치**:
1. Ghost fill 로그: `CRITICAL → WARNING`
2. RECON BROKER_ONLY 로그: `CRITICAL → WARNING`
3. Chejan fallthrough 카운터: 첫 5회 + 이후 20회마다만 출력, 50회 도달 시 `[CHEJAN_FALLTHROUGH_ALERT]`

**결과**: 로그 가독성 향상, 핵심 이벤트 식별 용이.

---

### ADD-4: 리포트 EOD 가격 소스 품질 섹션

**근거**: EOD 가격이 어느 source에서 왔는지 리포트에서 확인 불가.

**조치**: daily report에 `EOD 가격 소스 품질` 카드 추가 — verified %, 소스별 종목 수 테이블.

**결과**: 리포트에서 데이터 품질 즉시 확인 가능.

---

### 운영 영향 평가

| 구분 | 변경 | live/paper 영향 |
|------|------|-----------------|
| FIX-1 | pykrx 캘린더 fallback | 개선 (거래일 정확도 향상) |
| FIX-2 | collector NameError 방어 | 개선 (리밸 날 크래시 방지) |
| FIX-3 | ghost fill 상태기계 | 개선 (holdings 정합성 보장) |
| FIX-4 | sell_status 자동 승격 | 개선 (pending buy 영구 블로킹 방지) |
| FIX-5 | logging 포맷 | 버그픽스 |
| ADD-1 | PAPER_TEST 모드 | 격리됨 (paper_test 가드) |
| ADD-2 | EOD prefetch | 개선 (trail skip 감소) |
| ADD-3 | 로그 레벨 조정 | 개선 (가독성) |
| ADD-4 | 리포트 source 품질 | 추가 기능 |

### 남은 리스크

1. **PARTIAL_TIMEOUT → ghost → FINALIZED 전체 경로**: 오늘 풀 사이클에서는 전량 체결되어 ghost 미발생. 저유동성 종목에서의 실전 검증 필요.
2. **RECON 정합성**: fresh start 시 21 corrections는 정상이지만, 일반 재시작 시 correction 2건 이하 유지 확인 필요.

---

> **이 문서는 Q-TRON Gen4의 실전 운영 과정에서 발견된 문제들의 원인, 수정, 교훈을 기록한 것입니다.
> 미국주식/코인 시장 확장 시 섹션 14의 범시장 적용 가이드와 부록 B의 코드 관례를 기반으로
> 시장별 Provider만 교체하고, 동일한 안전장치 패턴을 적용하면 됩니다.**
