# Pipeline Orchestrator — Implementation Plan (v1.0)

작성일: 2026-04-21
작성자: Claude (TOM) — Jeff 승인 후 착수
상태: **Phase 1~4 COMPLETE · Phase 5 대기 (shadow→primary 관찰 후)**
참조 설계 문서: `kr/docs/PIPELINE_ORCHESTRATOR.md` (commit `fafdf9f6`)

---

## 0.0 Progress Tracker (Live)

| Phase | 상태 | Commit | Tests | 비고 |
|---|---|---|---|---|
| 1. Foundation (schema/state/backoff/mode/bootstrap) | ✅ DONE | `8eb3c88e` | 39 | 2026-04-21 |
| 2. Step wrappers + PG mirror | ✅ DONE | `08af90d2` | 76 (+37) | 2026-04-21 |
| 3. Orchestrator + tray shadow hook + lab_eod_us | ✅ DONE | `4e6c8afb` | 100 (+24) | 2026-04-21 |
| 4. REST API + UI banner + primary-mode gating | ✅ DONE | `a9cba508` | 111 (+11) | 2026-04-21 — Phase 5 legacy gating 선행 완료 |
| 5. Legacy 필드 제거 + advisor 전환 | ⏳ WAITING | — | — | `QTRON_PIPELINE=2` primary 1주일 무사고 후 착수 |

**현재 운영 상태**: 코드 전체 배포됨. env `QTRON_PIPELINE` 미설정이라 no-op (regression 가드).
  - `=1` shadow: orchestrator tick + state 기록 병렬, legacy trigger 그대로
  - `=2` primary: legacy batch/backup/KR-EOD/US-EOD trigger 4블럭 suppress → orchestrator 단독 scheduler

**Phase 3.5 (deferred)**: ohlcv_sync 독립 step 추출 — `run_batch` 내부 checkpoint와 tightly coupled. Phase 5 이후 `lifecycle/batch.py` 리팩토링과 함께 재평가.

---

## 0. Executive Summary

2026-04-20 Design doc §7의 **오픈 이슈 5개**를 Jeff이 전부 승인 (2026-04-21).
본 문서는 그 결정을 기반으로 **Phase 1~5 concrete implementation plan**을 확정한다.

총 소요: 5~9일 (1~1.5주). 회귀 리스크 최소화를 위해 기존 tray_server.py 스케줄러와 **병렬 운영 → Phase 5에서 전환**.

---

## 1. 결정된 오픈 이슈 (Jeff 승인 2026-04-21)

| # | 이슈 | 결정 | 구현 영향 |
|---|---|---|---|
| 1 | Pipeline state 저장 위치 | **JSON primary + 완료 시 PG mirror** | `kr/data/pipeline/state_YYYYMMDD.json` 단일 원천, step 완료 시 `pipeline_state_history` PG 테이블에 append (Phase 2 말미) |
| 2 | "오늘 trade_date" 판정 | **pykrx 기반 last_trading_day** | `mode.py::resolve_trade_date()` — 주말/공휴일에는 직전 거래일 반환. `pykrx.get_business_days` 사용 |
| 3 | `_batch_today_done` 메모리 플래그 | **1주일 병렬 유지 후 deprecate** | Phase 3에서 orchestrator가 쓰기 전환, Phase 5에서 기존 플래그 제거 |
| 4 | 실 Live 엔진 통합 범위 | **tray 주도 태스크만, live는 record_step POST 위임** | `main.py --live`는 그대로. EOD 완료 시 `POST /api/pipeline/record_step` 호출만 추가 (Phase 4) |
| 5 | 어제 abandoned → 오늘 catch-up | **No. 매일 새 state, 어제는 read-only** | `state.load_or_create_today()`는 오늘만 생성/로드. 어제 state는 쿼리/리포트용으로만 보존 |

**추가 결정 (R-6 반영)**: Phase 1에 **bootstrap 환경 검증 step** 포함 — tzdata/ZoneInfo fail-fast. 2026-04-20 22시 silent fail 재발 방지.

---

## 2. Phase 1 상세 계획 (1~2일, 이번 세션 착수)

### 2.1 파일 구조

```
kr/
├── pipeline/
│   ├── __init__.py              # 공개 API (PipelineState, BackoffTracker, detect_mode, bootstrap_env)
│   ├── state.py                 # PipelineState + atomic I/O
│   ├── backoff.py               # BackoffTracker
│   ├── mode.py                  # detect_mode() + resolve_trade_date()
│   ├── bootstrap.py             # bootstrap_env() — tzdata/ZoneInfo fail-fast
│   └── schema.py                # step status enum, schema constants
├── data/
│   └── pipeline/                # (Phase 2에서 실제 state 파일 생성)
│       └── .gitkeep
└── tests/
    └── pipeline/
        ├── __init__.py
        ├── test_state.py        # 라운드트립, 원자쓰기, 스키마 검증
        ├── test_backoff.py      # 재시도 윈도우, max_fails, reset
        ├── test_mode.py         # live/paper_forward/lab 구분, trade_date 계산
        └── test_bootstrap.py    # tzdata 누락 감지 (mock)
```

### 2.2 `pipeline/schema.py` 명세

```python
# Step status enum (string-based for JSON 호환)
STATUS_NOT_STARTED = "NOT_STARTED"
STATUS_PENDING = "PENDING"       # in-progress (background thread running)
STATUS_DONE = "DONE"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"       # precondition unmet, intentional no-run

ALL_STATUSES = frozenset({...})

# Mode enum
MODE_LIVE = "live"
MODE_PAPER_FORWARD = "paper_forward"
MODE_LAB = "lab"
MODE_BACKTEST = "backtest"

SCHEMA_VERSION = 1

# Step ordering (Phase 2에서 각 step 래퍼가 등록)
DEFAULT_STEPS = (
    "bootstrap_env",
    "ohlcv_sync",
    "batch",
    "lab_eod_kr",
    "lab_eod_us",
    "gate_observer",
    "backup",
)
```

### 2.3 `pipeline/state.py` 명세

**책임**:
- `kr/data/pipeline/state_YYYYMMDD.json` atomic read/write
- Schema v1 validation
- Step별 `started_at`/`finished_at`/`fail_count`/`last_error`/`details` 필드 관리

**공개 API**:
```python
class PipelineState:
    trade_date: date
    tz: str                      # "Asia/Seoul"
    mode: str                    # MODE_*
    last_update: datetime
    steps: dict[str, StepState]

    @classmethod
    def load_or_create_today(cls, *, data_dir: Path, mode: str) -> "PipelineState": ...
    @classmethod
    def load_date(cls, trade_date: date, *, data_dir: Path) -> "PipelineState | None": ...

    def mark_started(self, step_name: str) -> None: ...
    def mark_done(self, step_name: str, details: dict | None = None) -> None: ...
    def mark_failed(self, step_name: str, err: str) -> None: ...
    def mark_skipped(self, step_name: str, reason: str) -> None: ...

    def step(self, step_name: str) -> StepState: ...
    def is_done(self, step_name: str) -> bool: ...

    def save(self) -> None: ...   # atomic write (tmp+rename)
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState": ...


@dataclass
class StepState:
    status: str = STATUS_NOT_STARTED
    started_at: datetime | None = None
    finished_at: datetime | None = None
    fail_count: int = 0
    last_error: str | None = None
    last_failed_at: datetime | None = None
    details: dict = field(default_factory=dict)
```

**Atomic write**:
- `tmp_path = path.with_suffix('.json.tmp')` → write → `os.replace(tmp_path, path)`
- 실패 시 기존 파일 보존, 로깅 후 raise (다음 tick에서 재시도)

**Backward compat**: Phase 1에서는 신규 파일만 생성. 기존 `head.json`/`runtime_state_live.json`는 **읽기 계속 유지** (Phase 5에서 정리).

### 2.4 `pipeline/backoff.py` 명세

**단일 `BackoffTracker`로 기존 3개 재시도 패턴 통합**:
- KR batch: 30s → 5min backoff
- US batch: 30s retry
- Lab EOD: 5min backoff + MAX_FAILS=3

**공개 API**:
```python
class BackoffTracker:
    def __init__(
        self,
        step_name: str,
        *,
        min_wait_sec: int = 300,     # default 5min
        max_fails: int = 3,          # abandoned threshold
        clock: Callable[[], datetime] = datetime.now,
    ): ...

    def can_run_now(self, state: PipelineState) -> tuple[bool, str]:
        """Returns (allowed, reason). reason = 'ok' | 'abandoned' | 'backoff' | 'already_done'."""
        ...

    def record_fail(self, state: PipelineState, err: str) -> None: ...
    def record_success(self, state: PipelineState, details: dict | None = None) -> None: ...
    def reset(self, state: PipelineState) -> None: ...
```

**핵심 로직**:
- `fail_count >= max_fails` → abandoned (오늘은 포기, 다음 날 새 state에서 자동 reset)
- `last_failed_at + min_wait_sec > now` → backoff 대기
- step.status == DONE → already_done (double-run 차단)

### 2.5 `pipeline/mode.py` 명세

**책임 2가지**:

1. **`detect_mode()`**: 현재 실행 모드 감지
   - `os.environ['QTRON_MODE']` (명시적 override)
   - tray process + lab_live 활성 여부 → `paper_forward`
   - `main.py --live` 실행 중 → `live`
   - 기본값: `paper_forward` (가장 안전)

2. **`resolve_trade_date(now: datetime | None = None) -> date`**:
   - `pykrx.get_business_days(year, month)` 사용
   - 주말/공휴일이면 **직전 거래일** 반환
   - pykrx 실패 시 fallback: `weekday() < 5 ? today : 마지막 월~금`
   - **로깅**: `[PIPELINE_TRADE_DATE] resolved=2026-04-21 source=pykrx|fallback`

**공개 API**:
```python
def detect_mode() -> str: ...
def resolve_trade_date(now: datetime | None = None) -> date: ...
```

### 2.6 `pipeline/bootstrap.py` 명세 (R-6 대응)

**책임**: 환경 전제 fail-fast 검증. tray/orchestrator 시작 시 **반드시** 먼저 호출.

**검증 항목**:
1. `import tzdata` 성공
2. `ZoneInfo("Asia/Seoul")` lookup + `datetime.now(ZoneInfo(...))` 성공
3. `kr/data/pipeline/` 쓰기 권한

**공개 API**:
```python
class BootstrapError(RuntimeError): ...

def bootstrap_env(*, data_dir: Path | None = None, strict: bool = True) -> dict:
    """
    Returns checks dict {'tzdata': True, 'zoneinfo_seoul': True, 'data_dir_writable': True}.
    strict=True (default): raise BootstrapError on any failure.
    strict=False: 반환 dict에 실패 기록 + 로깅 (개발 환경용).
    """
    ...
```

**호출 위치** (Phase 3):
- `tray_server.py` 최상단 (BOOTSTRAP_AUDIT 직후)
- orchestrator tick 진입 전 1회

### 2.7 단위 테스트 (`kr/tests/pipeline/`)

**test_state.py** (6 cases):
1. Create today → save → load round-trip
2. Atomic write: tmp 파일 생성 중 크래시 시 기존 파일 보존
3. Schema version mismatch → raise ValueError
4. mark_failed 후 fail_count 증가 + last_failed_at 기록
5. mark_done 후 finished_at + details 저장
6. load_date(yesterday) → 없으면 None 반환 (catch-up 금지 검증)

**test_backoff.py** (5 cases):
1. 첫 호출 → can_run_now = (True, 'ok')
2. fail 1회 + min_wait_sec 안 → (False, 'backoff')
3. fail 1회 + min_wait_sec 경과 → (True, 'ok')
4. fail 3회 → (False, 'abandoned')
5. record_success 후 fail_count reset

**test_mode.py** (4 cases):
1. `QTRON_MODE=live` env → "live"
2. env 없음 + default → "paper_forward"
3. `resolve_trade_date(금요일 17시)` → 금요일 date
4. `resolve_trade_date(토요일 10시)` → 금요일 date

**test_bootstrap.py** (3 cases):
1. 정상 환경 → 모든 체크 True
2. tzdata 미설치 mock → `BootstrapError` (strict=True)
3. strict=False → 실패 기록 + dict 반환

### 2.8 Phase 1 Done 기준

- [ ] 10개 파일 생성 (5 pipeline/ + 1 schema + 4 tests)
- [ ] `cd kr && ../.venv64/Scripts/python.exe -m pytest tests/pipeline/ -v` 전부 PASS
- [ ] 기존 tray/engine 코드 **무변경** (회귀 zero)
- [ ] Commit: `feat(pipeline): Phase 1 — state/backoff/mode/bootstrap foundation`

---

## 3. Phase 2~5 요약

### Phase 2: Step 래퍼 (1~2일)

각 기존 태스크를 Step 인터페이스로 래핑.

```python
# pipeline/steps/__init__.py
class Step(Protocol):
    name: str
    def precondition_met(self, state: PipelineState) -> tuple[bool, str]: ...
    def run(self, state: PipelineState) -> None: ...  # BackoffTracker 내장

# 래핑 대상
steps/ohlcv_sync.py   # lifecycle.batch.step1 재사용
steps/batch.py        # lifecycle.batch.run_batch 재사용
steps/lab_eod_kr.py   # POST /api/lab/live/run-daily 호출
steps/lab_eod_us.py   # us/lab/forward.py 호출
steps/gate_observer.py # tools.gate_observer 호출
steps/backup.py       # tools.daily_backup 재사용
```

Phase 2 말미: PG mirror 테이블 `pipeline_state_history` 스키마 + DDL 추가.

### Phase 3: Orchestrator (1일)

```python
# pipeline/orchestrator.py
class Orchestrator:
    def __init__(self, data_dir: Path, steps: list[Step]): ...
    def tick(self) -> None:
        state = PipelineState.load_or_create_today(...)
        for step in self._steps:
            if state.is_done(step.name) or state.step(step.name).status == STATUS_PENDING:
                continue
            ok, reason = step.precondition_met(state)
            if not ok:
                continue
            tracker = BackoffTracker(step.name)
            allowed, br = tracker.can_run_now(state)
            if not allowed:
                continue
            threading.Thread(target=self._run_step, args=(step, state), daemon=True).start()
```

`tray_server.py` 변경:
- 기존 `_run_scheduled_batch`, `_run_kr_lab_eod_auto` 등 auto-trigger 블록 → `orchestrator.tick()` 1줄 호출로 교체
- 기존 플래그 (`_batch_today_done` 등) **1주일 유지** (Phase 5까지 병렬)

### Phase 4: Observability + Advisor (1일)

- `POST /api/pipeline/record_step` — live 엔진 연동 endpoint (오픈 이슈 #4)
- `GET /api/pipeline/status` — UI용 current day state 덤프
- `lab_live.html` 상단 배너 — "Pipeline: OHLCV ✓ · Batch ✓ · Lab EOD KR ⏳"
- Advisor 체크 로직을 pipeline_state 기반으로 전환 (mode-aware — 오픈 이슈 #4 후속)

### Phase 5: Legacy 정리 + 관찰 (2~3일)

- 1주일 병렬 운영 (Phase 3 종료 후)
- 로그 모니터링: `[PIPELINE_TICK]`, `[STEP_DONE]`, `[STEP_FAIL]`
- Legacy 필드 deprecation:
  - `_batch_today_done`, `_kr_lab_eod_last_done_date` 제거
  - `head.json`의 중복 필드 정리 (lab_live 내부 state는 유지)
  - `runtime_state_live.json` 이중 timestamp 필드 (R-1 원인) 단일화

---

## 4. 회귀 리스크 평가

| 영역 | 리스크 | 완화 |
|---|---|---|
| Phase 1 (신규 모듈만) | **Zero** | 기존 코드 무변경 |
| Phase 2 (step 래퍼) | Low | step 내부는 기존 함수 재사용, 래퍼만 추가 |
| Phase 3 (tray 스케줄러 교체) | **High** | 환경변수 `QTRON_PIPELINE=1` 로 new/old 토글. Off 시 기존 로직 그대로 |
| Phase 4 (live 엔진 연동) | Low | record_step은 fire-and-forget, 실패해도 live 주문 영향 없음 |
| Phase 5 (legacy 제거) | Medium | 1주일 관찰 후 제거, 언제든 revert 가능한 소규모 commit |

---

## 5. 롤백 전략

- 각 Phase 끝에 **단일 commit** (squash 아님, revert 쉽게)
- Phase 3 전환 시 `QTRON_PIPELINE=0` 환경변수로 즉시 old path 복귀
- Phase 5 legacy 제거는 별도 feature branch → 1주일 staging 후 main merge

---

## 6. 검증 기준 (Phase별 PASS 조건)

| Phase | PASS 조건 |
|---|---|
| 1 | 단위 테스트 100% PASS, 기존 기능 무변경 확인 (smoke: `tray_server` 부팅 + `kr/main.py --batch` 정상) |
| 2 | 각 step 독립 실행 시 pipeline state 기록됨, 기존 결과물 (top20 리포트, CSV 등) 바이트 동일 |
| 3 | `QTRON_PIPELINE=1`로 1일 운영 → 기존과 동일 결과 + pipeline state 완결 |
| 4 | `/api/pipeline/status` UI 배너 표시, advisor 오탐 3건 소멸 |
| 5 | Legacy 필드 제거 후 1주일 무사고 |

---

## 7. 이번 세션 작업 범위 (2026-04-21)

- [x] 계획서 작성 (본 문서)
- [ ] Phase 1 전체 구현 + 테스트
- [ ] commit + next session 대기

Phase 2 착수는 Phase 1 commit review 후 Jeff 승인 재확인.

---

## Appendix A. 결정 근거 (오픈 이슈 세부)

### A.1 JSON primary + PG mirror (이슈 #1)
- JSON primary 이유: 로컬 복구 쉬움, tray 프로세스 단독으로 읽기/쓰기, PG 장애와 독립
- PG mirror 이유: 일자별 history 집계 (30일 backtest 등), advisor/dashboard 쿼리 편의
- 구현 순서: Phase 1은 JSON only, Phase 2 말미 PG append 추가

### A.2 pykrx last_trading_day (이슈 #2)
- `calendar_today` 의 문제: 토/일/공휴일에 tray 돌면 빈 date의 state 파일 생성 → 의미 없음
- `last_trading_day`: 주말 tray 부팅 시 금요일 state 로드 → catch-up 아님 (이슈 #5 준수), 단순히 "오늘은 새 거래일 아님" 명시

### A.3 메모리 플래그 1주일 병렬 (이슈 #3)
- 즉시 deprecate 리스크: Phase 3 전환 실패 시 롤백 포인트 없음
- 1주일 병렬: 두 방식 로그 대조 가능, 이상 감지 시 즉시 old path 복귀

### A.4 Live 엔진 record_step POST (이슈 #4)
- Live 엔진 내부 수정 금지 원칙 (CLAUDE.md Engine Protection) 준수
- record_step POST 실패 = pipeline state 미기록뿐, live 주문 흐름 무영향
- UNKNOWN 상태가 될 수 있으므로 Advisor는 mode-aware 체크 필수

### A.5 어제 state는 read-only (이슈 #5)
- Catch-up 허용 시 장 마감 24시간 후 자동 주문 위험 (실수 주문 원천 차단)
- 어제 fail → 수동 복구 필요 (Jeff 검토 가능), 자동 복구 금지
- History 보존은 유지 (리포트/advisor 참조용)
