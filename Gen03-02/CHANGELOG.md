# Q-TRON Gen3 Changelog

---

## [2026-03-10] Gen3 v7 — 이번 세션 변경사항

### 1. Mock HARD_STOP 수정 (`runtime/runtime_engine.py`, `main.py`)

**문제:** `--mock` 실행 시 `portfolio_state.json`에 누적된 손실이 로드되어 HARD_STOP 발생.

**수정:**
- `RuntimeEngine.__init__`에 `fresh_state: bool = False` 파라미터 추가
- `fresh_state=True`이면 `restore_portfolio()` 스킵 → 클린 1억원 상태로 시작
- `run_mock()`에서 `fresh_state=True` 전달

```python
# runtime_engine.py
def __init__(self, config, provider, skip_market_hours=False, fresh_state=False):
    ...
    if not fresh_state:
        restored = self.state_mgr.restore_portfolio(self.portfolio)
    else:
        print("[RuntimeEngine] fresh_state=True — 포지션 복원 스킵 (클린 시작)")
```

---

### 2. 종목명 표시 (`data/name_lookup.py`, `data/stock_names.json`)

**추가:** 2622개 KOSPI 종목코드 → 한국어 종목명 매핑

- `data/stock_names.json`: pykrx `get_market_ticker_name()` 기반 2622개 매핑
- `data/name_lookup.py`: 싱글턴 lazy-load 유틸리티
  - `get_name(code)` → 한국어 종목명 (없으면 코드 그대로)
  - `fmt(code, width)` → `"삼성전자(005930)"` 형식
- `runtime/order_executor.py`, `runtime/position_monitor.py`: 종목명 적용

**출력 전후:**
```
# Before
[BUY] 448900 88주 @ 56,724원

# After
[BUY] 한국피아이엠(448900) 88주 @ 56,724원
```

---

### 3. signals.csv 3일 fallback (`strategy/entry_signal.py`)

**추가:** 오늘 signals 파일이 없으면 최근 3일 이내 파일 자동 사용

```python
for delta in range(4):  # 오늘, 1일전, 2일전, 3일전
    candidate = signals_dir / f"signals_{(today - timedelta(days=delta)).strftime('%Y%m%d')}.csv"
    if candidate.exists():
        filepath = candidate
        break
```

---

### 4. 일일 HTML 리포트 — 신호 선정근거 (`report/reporter.py`)

**추가:** 신호 테이블에 선정 이유 컬럼 추가

| 컬럼 | 내용 |
|---|---|
| Stage | A=조기진입(금색) / B=메인전략(파란색) |
| RS% | 전체 상대강도 퍼센타일 (색상 코딩) |
| 모멘텀(20/60/120) | rs20/rs60/rs120 3기간 일관성 |
| 선정근거 | `52주신고` / `MA20위` / `진입신호` 배지 |
| RR | (TP-진입가)/(진입가-SL) 수익/리스크 비율 |

**새 헬퍼 메서드:** `Reporter._sig_row()`, `Reporter._rs_badge()`

---

### 5. Mock 모드 일일 리포트 자동 열기 (`main.py`)

**추가:** `--mock` 실행 후 브라우저에서 HTML 리포트 자동 오픈

```python
def run_mock():
    _run_runtime(MockProvider(), "MOCK", skip_market_hours=True,
                 open_browser=True, fresh_state=True)
```

---

### 현재 개발 완성도

| 영역 | 완성도 |
|---|---|
| 핵심 엔진 (Portfolio/Risk/State) | 100% |
| 전략 (Regime/RS/RAL/Stage A·B) | 100% |
| 신호 생성 (gen3_signal_builder) | 100% |
| 데이터 (pykrx/mock/kiwoom 데이터) | 100% |
| 페이퍼 트레이딩 시뮬레이션 | 100% |
| 일일 HTML 리포트 | 95% |
| **실주문 체결 (Kiwoom SendOrder)** | **0%** |
| 섹터 분류 ('기타' 개선) | 65% |
| 백테스트 실데이터 검증 | 70% |
| 자동 배치 스케줄링 | 0% |
