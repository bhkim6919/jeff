# Pipeline Orchestrator — Design Doc (Draft v0.1)

작성일: 2026-04-20
작성자: Claude (JUG/TOM 협업 초안)
상태: **DRAFT — Jeff 검토 대기**

---

## 1. 문제 정의

### 1.1 재발 이력 (4/10~4/20)

| 날짜 | 증상 | 임시 패치 | 재발 지점 |
|---|---|---|---|
| 4/14~16 | US skip 로그 30s 간격 반복 (60~234건/일) | 없음 (관찰만) | 매일 동일 |
| 4/18 | US 같은 로그 560건 | 없음 | — |
| 4/20 | KR Gen4Config import 에러 (32회) | `sys.modules.pop` defensive fix | tray 재시작 시 재발 가능 |
| 4/20 | KR Lab EOD 15:35 윈도우 놓침 → abandoned | post-batch retry arm 추가 | 배치가 17시 이후 완료되면 또 놓침 |
| 4/20 | `gen4.batch` 로그 파일 누락 (`[BATCH_DONE]` 추적 불가) | `gen4` parent handler 부착 | 신규 `gen4.xxx` 로거 생길 때마다 수정 필요 |
| 4/20 | Backup FAIL 오탐 (`SKIPPED` 포함) | 성공 판정 로직 수정 | 신규 status 값 생길 때마다 수정 필요 |
| 4/20 | AI Advisor "portfolio state 314.9h old" | — | paper forward 전환 후에도 Gen4 live state 체크 |
| 4/20 | AI Advisor "timestamp gap 6138min" | — | runtime_state_live.json 이중 timestamp 필드 |

**공통 패턴**: 매번 지역 패치 → 1~N일 뒤 인접 영역에서 유사 증상 재발.

### 1.2 진짜 원인 (5가지)

#### R-1. State Fragmentation
같은 사실(오늘 배치/EOD 완료 여부)이 **4~5 곳**에 분산되어 각자 업데이트:

| 사실 | 저장 위치 |
|---|---|
| KR 오늘 배치 완료 | `tray._batch_today_done` (mem) + `tray._batch_last_done_date` (mem+file) + `head.json.last_run_date` + rest_api log grep |
| US 오늘 배치 완료 | `tray._us_batch_last_done_date` (mem+file) + FastAPI `/api/rebalance/status.phase` + `/api/rebalance/status.snapshot_created_at` |
| KR Lab EOD 오늘 완료 | `tray._kr_lab_eod_last_done_date` (mem) + `tray._kr_lab_eod_fail_count` (mem) + `head.json` |
| Live trading 상태 | `state/portfolio_state_live.json` (legacy) + `state/runtime_state_live.json` (tray) + `rest_state_db` (PG) + `report/equity_log.csv` |

결과:
- 특정 쓰기가 실패/지연 → 다른 곳은 모름 → 스케줄러가 "아직 미완료"로 오판 → 재시도 spam
- 특정 읽기가 stale 필드 참조 → advisor 같은 downstream이 잘못된 경보

#### R-2. Time-Window 기반 스케줄링
모든 auto-trigger가 **시간 윈도우**에 매달림:

```
KR batch:      16:05~16:59 (55min 윈도우)
KR Lab EOD:    15:35±60s (2분 윈도우)
US batch:      US_BATCH_TIME 범위
Backup:        17:00~17:59
```

- 상류가 지연되면 하류 윈도우 내에 실행 못 함 → 당일 포기
- 포기 후 상류 복구돼도 하류는 다음 날까지 대기 (오늘 Lab EOD 사례)
- 시간 윈도우 ≠ **데이터 준비 상태** (실제 필요 조건)

#### R-3. Retry/Backoff 설계가 각자
동일 개념이 3가지로 구현:

| 위치 | 패턴 |
|---|---|
| KR batch | 30s 무한 retry → 오늘 5min backoff 추가 |
| US batch | 30s retry + skip 판정 |
| Lab EOD | MAX_FAILS=3 + 5min backoff + abandoned 플래그 |

결과: 버그도 3배, 수정도 3배, 각 재시도 정책이 서로 다른 동작.

#### R-4. 로거 부착의 명시 리스트
`rest_logger.py`가 5개 자식 로거에만 handler 부착:
```python
for name in ("gen4.rest", "gen4.live", "gen4.state", "gen4.crosscheck", "gen4.dual_read"):
```
신규 `gen4.*` 로거가 생기면 **매번 수동 추가 필요**. 오늘 `gen4` parent 부착으로 우회했지만 근본은 "allowlist 방식".

#### R-5. 구조적 중복 (config/main/backtest)
- `C:\Q-TRON-32_ARCHIVE\config.py` (Gen2 legacy `QTronConfig`)
- `C:\Q-TRON-32_ARCHIVE\kr\config.py` (Gen4 `Gen4Config`)
- Root `main.py`, `backtest/`, `core/`, `stage4_risk/` 모두 Gen2 legacy

결과: `sys.modules` 캐시에 Gen2가 선점되면 tray의 `from config import Gen4Config`가 실패 (오늘 증상).

### 1.3 2026-04-20 구체적 증거 (5 root causes 매핑)

2026-04-20 세션에서 발견/겪은 증상들을 5 root causes에 매핑.
commit hash와 로그 라인 증거 포함 — 재현 불가능한 맥락을 영구 기록.

| 발견 증상 | 날짜/시각 | 매핑 | 증거 (commit/로그) |
|---|---|---|---|
| US auto-retry 30s 폭주 (4/18: 560회/일) | 4/14~4/18 | R-1 R-3 | `b02c2f6e` tray_server.py `_run_us_batch_and_rebal` skip cache 누락 |
| KR Lab EOD 15:35 윈도우 놓침 → abandoned | 오늘 15:52 | R-2 R-3 | `[KR_LAB_EOD_AUTO_ABANDONED] fail=3/3` in rest_api_20260420.log; 수동 복구 필요 |
| Gen4Config import 32회 반복 실패 | 오늘 16:06~16:22 | R-5 | `[BATCH_FAIL] cannot import name 'Gen4Config' from 'config' (C:\Q-TRON-32_ARCHIVE\config.py)`; `b02c2f6e` sys.modules.pop defensive fix |
| gen4.batch 로그 rest_api_*.log에 누락 | 관측 전부터 | R-4 | rest_logger.py allowlist 5개만; `b02c2f6e` gen4 parent 부착 |
| Backup "[KR] BACKUP FAIL" 오탐 (SKIPPED sqlite) | 매일 | R-1 | `d41d9bf0` daily_backup.py _is_success 추가 |
| Promotion UNKNOWN이 safe 처리 | 설계 초기부터 | (정확성) | `9f5b3bf2` 17 files; 수정 후 BLOCKED 정확 표시 |
| Pre-commit stash 사고 (50+ files 손실) | 오늘 19:35 | 인접 R-5 | `.venv/Scripts/python.exe` file lock → rollback 실패; `backup/precommit_stash_20260420.patch` 217MB 보존 + `.pre-commit-config.yaml` exclude `^(\.venv|\.venv64)/` 추가 |
| tray restart 시 lab_live head.json rollback | 오늘 19:35 | R-1 | `committed_version_seq 11→1` 관측; state/*.json 일부는 최신 유지하면서 head만 롤백 → 원인 미확인. Pipeline state 단일화가 해결 경로 |
| Lab Live catch-up 없음 → 4/11~4/19 equity_history 구멍 | 오늘 | R-2 | `run_daily()` 가 오늘 하루만 처리; 매일 실행 안 하면 빠짐. Backfill script `backup/lab_live_equity_backfill.py` 로 일회 복구. 근본 해결은 Orchestrator step 재실행 idempotency |
| AI Advisor "portfolio state 314h old" 오탐 | 상시 | R-1 | `portfolio_state_live.json`은 Gen4 live state, Jeff은 paper_forward 사용 → 읽는 source와 실제 운영 mode 불일치. mode-aware 체크로 해결 |
| Advisor timestamp gap 6138min (~4일) | 상시 | R-1 | `runtime_state_live.json`에 `timestamp`(stale 4/3) vs `_write_ts`(최신 4/20) 이중 필드; advisor가 stale 필드 참조. 단일 `pipeline_state.last_update` 로 해결 |

### 1.4 오늘 작업 범위 (증상 치료 한계 확인)

2026-04-20 commit 이력 (master 병합 완료):
- `9f5b3bf2` feat(promotion): UNKNOWN-safe ops evidence + PG
- `d41d9bf0` fix(backup): treat SKIPPED as success
- `b02c2f6e` fix(tray): batch import guard + Lab EOD recovery + fail backoff + gen4 logger
- `fafdf9f6` docs: Pipeline Orchestrator design draft (v0.1) — 본 문서
- `7b93dcf1` fix(us): add _bootstrap_path import to web/app.py
- `cefba0ea` feat: restore uncommitted work lost during 2026-04-20 pre-commit incident (53 files)

**전부 증상 레벨**. Pipeline Orchestrator 실장만이 5 root causes 전부 해결 경로.

---

## 2. 목표

### 2.1 설계 원칙

1. **Single source of truth**: "오늘 X 완료 여부"는 단 한 곳에서 읽기/쓰기.
2. **Event-driven > time-driven**: step은 upstream 완료로 트리거, 시간 윈도우 폐기.
3. **Idempotent by default**: 모든 step 재실행 안전 (snapshot_version 기반).
4. **Unified retry/backoff**: 단일 `BackoffTracker` 헬퍼로 모든 자동 재시도 통합.
5. **Logger hierarchy**: `gen4` parent 단일 부착, allowlist 폐기.
6. **Observable**: 파이프라인 상태를 UI/CLI에서 하나의 뷰로 조회.

### 2.2 Out of scope (이번 설계 범위 제외)

- 실 Live 트레이딩 엔진(`main.py --live`)의 내부 로직 — 그대로 유지
- Lab Live 전략 로직(`web/lab_live/engine.py`의 `_run_daily_locked` 내부)
- Promotion evidence 수집 (별도 시스템, 최근 완료)
- Root/Gen2 legacy 코드 정리 — 별도 작업으로 분리

---

## 3. 제안 아키텍처

### 3.1 파일 구조

```
kr/
├── pipeline/
│   ├── __init__.py
│   ├── state.py          # PipelineState read/write (atomic)
│   ├── steps.py          # Step definitions (batch, lab_eod, backup, gate_obs)
│   ├── orchestrator.py   # Scheduler (precondition-based, event-driven)
│   ├── backoff.py        # 단일 BackoffTracker 구현
│   └── mode.py           # 현재 실행 모드 감지 (live/lab/paper)
└── data/
    └── pipeline/
        └── state_YYYYMMDD.json   # Daily pipeline state (single source of truth)
```

### 3.2 Pipeline State 스키마

```json
{
  "schema_version": 1,
  "trade_date": "2026-04-20",
  "tz": "Asia/Seoul",
  "last_update": "2026-04-20T17:55:00+09:00",
  "mode": "paper_forward",
  "steps": {
    "ohlcv_sync": {
      "status": "DONE",
      "started_at": "2026-04-20T17:08:14",
      "finished_at": "2026-04-20T17:08:34",
      "fail_count": 0,
      "last_error": null,
      "details": {
        "snapshot_version": "2026-04-20:DB:2026-04-20:2769:0bf88e15",
        "universe_count": 2769,
        "kospi_last_date": "2026-04-20"
      }
    },
    "batch": {
      "status": "DONE",
      "started_at": "...",
      "finished_at": "...",
      "fail_count": 0,
      "last_error": null,
      "details": {
        "target_count": 20,
        "top20_report": "report/output/top20_ma_20260420.html"
      }
    },
    "lab_eod_kr": {
      "status": "DONE",
      "started_at": "...",
      "finished_at": "...",
      "fail_count": 0,
      "last_error": null,
      "details": {
        "n_lanes": 18,
        "total_trades": 25
      }
    },
    "lab_eod_us": { "status": "PENDING", ... },
    "gate_observer": { "status": "NOT_STARTED", ... },
    "backup": { "status": "NOT_STARTED", ... }
  }
}
```

**Status enum**: `NOT_STARTED` | `PENDING` (in progress) | `DONE` | `FAILED` | `SKIPPED` (precondition not met)

### 3.3 Step Precondition Graph

```
                  ┌─────────────┐
                  │ ohlcv_sync  │  (always runs first on weekday)
                  └──────┬──────┘
                         ↓
                  ┌──────▼──────┐
                  │    batch    │  (precondition: ohlcv_sync.status=DONE)
                  └──────┬──────┘
                         ↓
            ┌────────────┴────────────┐
            ↓                         ↓
     ┌──────▼──────┐           ┌──────▼──────┐
     │ lab_eod_kr  │           │lab_eod_us   │
     │(pre: batch  │           │(pre: us_batch
     │  =DONE)     │           │  =DONE)     │
     └──────┬──────┘           └─────────────┘
            ↓
     ┌──────▼──────┐
     │gate_observer│  (pre: lab_eod_kr.status=DONE)
     └──────┬──────┘
            ↓
     ┌──────▼──────┐
     │   backup    │  (pre: 모든 critical step DONE or SKIPPED)
     └─────────────┘
```

각 step은 **자신의 precondition**만 알면 됨. 시간 윈도우 개념 제거.

### 3.4 Orchestrator Tick Logic

```python
# Pseudo-code
def tick():
    state = PipelineState.load_or_create_today()

    for step in ORDERED_STEPS:
        if step.status == DONE or step.status == SKIPPED:
            continue
        if step.status == PENDING:
            continue  # already running in background thread
        if not step.precondition_met(state):
            continue
        if step.in_backoff():  # unified BackoffTracker
            continue

        step.status = PENDING
        state.save()
        threading.Thread(target=step.run, args=(state,), daemon=True).start()
```

- **30초 tick 유지** (다른 tooltip/health 체크용)
- **step별 윈도우 제거**: upstream 완료 즉시 하류 트리거
- **파이프라인 상태 파일**이 사실상 캐시 역할 (probe 불필요)

### 3.5 Backoff/Retry 통합

```python
class BackoffTracker:
    """Step별 fail backoff. 메모리 state + pipeline_state 기록."""

    def __init__(self, step_name, min_wait_sec=300, max_fails=3):
        ...

    def can_retry_now(self, state) -> bool:
        step = state.steps[self.step_name]
        if step.fail_count >= self.max_fails:
            return False  # abandoned
        if step.last_failed_at:
            elapsed = now() - step.last_failed_at
            return elapsed >= timedelta(seconds=self.min_wait_sec)
        return True

    def record_fail(self, state, err): ...
    def record_success(self, state, details): ...
    def reset(self, state): ...
```

모든 step이 동일한 tracker 사용 → 버그 수정도 한 곳.

### 3.6 Restart 복구

```python
# tray 부팅 시
state = PipelineState.load_or_create_today()
# 무조건 어제자가 아니라 오늘 trade_date 기준 — 어제 미완료면 자동으로 NOT_STARTED

# 이후 tick이 돌면서 precondition 만족하는 step부터 catch-up
```

- 시간 윈도우 없으므로 **언제 재시작해도 자동 이어서 진행**
- 오늘 사례: 17:37 tray 재시작해도 batch 완료 감지 → lab_eod_kr 즉시 트리거

### 3.7 Observability

- CLI: `python -m pipeline.status` → 오늘 파이프라인 상태 덤프
- REST: `GET /api/pipeline/status` → UI에서 단일 뷰
- UI 배너: 상단에 "Pipeline: OHLCV ✓ · Batch ✓ · Lab EOD KR ⏳ · Backup ⏳"

### 3.8 Advisor 연동

현재 advisor가 개별 파일 timestamp를 읽는 로직을 pipeline_state 기반으로 교체:

```python
# Before (fragmented)
pf_ts = read_json("portfolio_state_live.json")["timestamp"]
rt_ts = read_json("runtime_state_live.json")["timestamp"]
age = now - pf_ts
# → "portfolio state 314h old" (mode mismatch)

# After (unified)
state = PipelineState.load_today()
if state.mode == "live":
    step = state.steps["live_eod"]
    if step.finished_at and (now - step.finished_at).hours > 2:
        alert("Live EOD stale")
# Paper mode면 이 alert 자체가 비활성화
```

---

## 4. 마이그레이션 계획

### 4.1 Phase 1: 기반 구현 (1~2일)

- [ ] `pipeline/state.py` — PipelineState I/O (atomic write, schema validation)
- [ ] `pipeline/backoff.py` — BackoffTracker
- [ ] `pipeline/mode.py` — mode detection (live/paper_forward/lab)
- [ ] 단위 테스트

### 4.2 Phase 2: Step 래퍼 (1~2일)

- [ ] `pipeline/steps/ohlcv_sync.py` — lifecycle.batch.step1 wrap
- [ ] `pipeline/steps/batch.py` — lifecycle.batch.run_batch wrap
- [ ] `pipeline/steps/lab_eod_kr.py` — `/api/lab/live/run-daily` wrap
- [ ] `pipeline/steps/backup.py` — daily_backup.py wrap
- [ ] `pipeline/steps/gate_observer.py` — tools.gate_observer wrap
- [ ] 각 step별 precondition / record_success 구현

### 4.3 Phase 3: Orchestrator (1일)

- [ ] `pipeline/orchestrator.py` — tick loop
- [ ] tray_server.py의 기존 auto-trigger 블록을 orchestrator 호출로 교체
- [ ] 기존 시간 윈도우 상수(`BATCH_HOUR`, `KR_LAB_EOD_HOUR`) 제거 or "earliest start" 힌트로만 남김

### 4.4 Phase 4: Observability + Advisor (1일)

- [ ] REST 엔드포인트 `/api/pipeline/status`
- [ ] UI 배너 (lab_live.html top bar)
- [ ] Advisor에서 pipeline_state 기반 체크로 전환

### 4.5 Phase 5: Backward compat + 관찰 (2~3일)

- [ ] 기존 `head.json`, `runtime_state_live.json` 병렬 유지 (읽기 compat)
- [ ] 며칠 관찰 후 legacy 필드 deprecation

**총 소요 예상**: 5~9일 (1~1.5주)

---

## 5. 회귀 리스크 평가

| 영역 | 리스크 | 완화 |
|---|---|---|
| tray_server 스케줄러 전체 재작성 | **High** | Phase 3에서 old/new 병렬 운영 후 스위치 |
| head.json 의존 코드가 많음 | Medium | 쓰기는 계속 유지 (Phase 5에서 정리) |
| 실 Live 엔진은 건드리지 않음 | Low | Out-of-scope |
| 파이프라인 state 파일 손상 | Low | atomic write + daily 파일이라 손상 범위 제한 |

---

## 6. 이번 설계로 해결되는 것 (재발 이력 매핑)

| 4/10~4/20 문제 | 해결 경로 |
|---|---|
| US skip 로그 폭주 | `us_batch.status=DONE` 기록 후 precondition 불통과 → tick skip |
| KR Lab EOD 윈도우 miss | 시간 윈도우 제거, upstream(batch) 완료 즉시 트리거 |
| Gen4Config import 에러 | Out-of-scope이지만 Phase 5에서 root/config.py 제거 제안 |
| `[BATCH_DONE]` 로그 추적 불가 | pipeline_state의 `finished_at` 필드 = 단일 진실 |
| Backup FAIL 오탐 | step.status 기반 판정 (SKIPPED 별도 분리) |
| Advisor "314h old" 오탐 | mode-aware 체크 (paper_forward에서는 live_eod 미체크) |
| Advisor timestamp gap | 단일 pipeline_state.last_update 하나만 봄 |

---

## 7. 오픈 이슈 (Jeff 결정 필요)

1. **Pipeline state 저장 위치**: JSON 파일 vs PG 테이블?
   - JSON: 단순, 디버깅 쉬움, 로컬 복구 용이
   - PG: 쿼리/집계 편함, 하지만 의존성 증가
   - **제안**: JSON primary + 완료 시 PG mirror (history 보존)

2. **"오늘 trade_date" 판정 기준**: calendar_today vs last_trading_day?
   - 주말/공휴일에 tray 돌면 어느 날짜를 파이프라인 state로?
   - **제안**: `pykrx.get_business_days` 기반 "가장 최근 거래일"

3. **기존 `_batch_today_done` 같은 메모리 플래그 유지?**
   - Phase 3에서 orchestrator로 이전 후 메모리 플래그는 **deprecate**
   - backward compat 기간 1주일 동안 둘 다 세팅

4. **실 Live 엔진과의 통합 범위**:
   - 이번 설계는 "tray 주도 자동 태스크"만 대상
   - `main.py --live`의 RECON/EOD는 기존대로, pipeline_state에는 외부 결과만 기록
   - **제안**: live 엔진이 EOD 끝나면 `/api/pipeline/record_step` POST로 기록 위임

5. **당일 abandoned → 다음날 자동 catch-up?**
   - 어제 backup이 fail로 끝남 → 오늘 tray 부팅 시 어제 pipeline_state를 보고 재시도?
   - **제안**: No. 매일 새 pipeline_state, 어제는 읽기 전용 기록으로만 보존.

---

## 8. 다음 단계 (JUG 승인 대기)

1. Jeff 본 문서 검토
2. 오픈 이슈 4가지 결정
3. Phase 1 PR 기반 디자인 리뷰 (`pipeline/state.py` + 테스트 먼저)
4. Phase 2~5 순차 진행

**이번 주 (4/20~4/24)**: 본 문서 작업만, 구현 없음.
**다음 주 (4/27~5/01)**: Phase 1 착수 결정.
