# State Management v2 Roadmap

## 완료 (2026-04-12)

### Gap #8 + #9: Lab Live Per-Strategy State + Committed HEAD

**변경 파일:**
- `web/lab_live/state_store.py` — v2 함수군 (save/load/migrate/recover/archive)
- `web/lab_live/config.py` — states_dir, head_file, equity_json_file, state_io_lock_file
- `web/lab_live/engine.py` — call site 전환
- `tests/test_state_v2.py` — 9개 시나리오 검증

**구현 내용:**
- Per-strategy 파일 분리 (9개 전략 × 개별 JSON)
- Committed HEAD (head.json) — 전체 세트의 atomic commit marker
- state/trades/equity 전부 동일 version_seq 보장
- All-or-nothing recovery: primary → .bak → archive → CORRUPTED
- FileLock (cross-process, msvcrt/fcntl)
- Archive rotation (keep_last_n=10)
- Legacy migration (monolithic state.json → per-strategy, temp dir 기반)
- CORRUPTED status 반환 (UI 표시용)

**테스트 결과 (9/9 PASS):**
1. Round-trip save/load
2. Partial crash → .bak rollback
3. .bak inconsistency → CORRUPTED status
4. Legacy migration
5. Archive content verification
6. Equity version consistency
7. Partial crash → archive fallback
8. HEAD corruption → .bak recovery
9. 31-day multi-day run + archive rotation

---

## 남은 작업

### 1순위: Gen04-REST Core StateManager

**파일:** `Gen04-REST/core/state_manager.py` (376줄)

**현재 구조:**
```
portfolio_state_{mode}.json  ← version_seq = N
runtime_state_{mode}.json    ← version_seq = N  (같은 값이지만 보장 안 됨)
```

**문제:**
- `save_all()`에서 portfolio → runtime 순차 write. 중간 crash 시 split 가능
- cross-process lock 없음 (REST 서버 + LIVE 엔진 동시 접근)
- archive fallback 없음
- CORRUPTED 상태 없음

**수정 계획:**

A. **version_seq 일치 검증** (load 시)
```python
def load_all(self) -> Optional[Tuple[dict, dict]]:
    p = self._atomic_read(self._portfolio_path)
    r = self._atomic_read(self._runtime_path)
    if p and r:
        if p.get("_version_seq") != r.get("_version_seq"):
            logger.error("[STATE] Version split detected")
            # .bak fallback 시도
            ...
    return p, r
```

B. **Committed HEAD 도입**
```
state/
├── head_{mode}.json           ← NEW: committed version pointer
├── portfolio_state_{mode}.json
├── portfolio_state_{mode}.json.bak
├── runtime_state_{mode}.json
├── runtime_state_{mode}.json.bak
└── archive/
```

Write protocol:
```
1. FileLock acquire
2. next_ver = head.committed_version_seq + 1
3. atomic_write(portfolio, version_seq=next_ver)
4. atomic_write(runtime, version_seq=next_ver)
5. atomic_write(head, committed_version_seq=next_ver)  ← LAST
6. FileLock release
```

C. **FileLock 적용**
- Lab Live v2의 FileLock 재사용
- REST 서버 ↔ LIVE 엔진 경쟁 방지

D. **Archive fallback chain**
- primary → .bak → archive → CORRUPTED
- Lab Live v2와 동일 패턴

**영향 범위:**
- `lifecycle/startup_phase.py` — load 경로
- `lifecycle/utils.py` — save 경로
- `runtime/order_executor.py` — 즉시 save 경로
- `web/rebalance_api.py` — REST API save 경로
- `main.py` — startup/shutdown save

**주의:** Engine Protection Rules 적용 대상. CONFIRMED + 회귀 테스트 통과 필요.

---

### 2순위: Gen04-US Core StateManagerUS

**파일:** `Gen04-US/core/state_manager.py` (184줄)

**현재 구조:**
- Gen04-REST Core와 동일한 two-file 패턴
- `save_all()`에서 paired version_seq

**리스크가 낮은 이유:**
- Alpaca REST API → broker가 truth, RECON으로 복원 가능
- `was_dirty_exit()` + FORCE_SYNC 경로 있음
- 단일 프로세스 (REST 서버 분리 안 됨)

**수정 계획:**

A. **Load 시 version_seq 일치 검증**
- split 감지 → was_dirty_exit=True 강제 → RECON 경로

B. **Archive fallback** (선택)
- broker RECON이 이미 truth 복원하므로 우선순위 낮음

---

### 3순위: Gen04-US Lab Forward

**파일:** `Gen04-US/lab/forward.py` (617줄)

**이미 성숙한 설계:**
- `meta.json:last_committed_run_id` = HEAD pointer
- `VERSIONS_DIR/{run_id}/{strategy}.json` = per-strategy
- Version directory로 이력 보존

**남은 작업:**

A. **VERSIONS_DIR rotation**
- 무한 증가 방지, keep_last_n=20 (Lab Live v2의 `_rotate_archives` 재사용)

B. **meta.json .bak fallback**
- meta.json 손상 시 복구 경로 없음
- `_read_head()` 패턴 적용

---

### 4순위: 공유 유틸 추출

**현재:** 3개 시스템이 atomic_write/read를 독립 구현

**목표:**
```
Gen04-REST/shared/
├── atomic_io.py        ← atomic_write_json, safe_read_json, _read_bak
├── file_lock.py        ← FileLock (msvcrt/fcntl)
├── committed_writer.py ← CommittedWriter (HEAD + multi-file)
└── archive.py          ← archive, rotation, recovery
```

**적용:**
- Lab Live state_store.py → shared import
- Gen04-REST Core state_manager.py → shared import
- Gen04-US → 별도 패키지 또는 복사 (다른 Python 환경)

---

## 비교표

| 기능 | Lab Live v2 | REST Core (현재) | REST Core (목표) | US Core | US Lab |
|------|:---:|:---:|:---:|:---:|:---:|
| Atomic write | ✅ | ✅ | ✅ | ✅ | ✅ |
| version_seq | ✅ | ✅ | ✅ | ✅ | ✅ |
| .bak fallback | ✅ | ✅ | ✅ | ✅ | ❌→✅ |
| Committed HEAD | ✅ | ❌ | ✅ | ⚠️검증만 | ✅ |
| All-or-nothing | ✅ | ❌ | ✅ | ⚠️RECON | ⚠️ |
| FileLock | ✅ | ❌ | ✅ | 불필요 | 불필요 |
| Archive fallback | ✅ | ❌ | ✅ | 선택 | ❌→✅ |
| Rotation | ✅ | ❌ | ✅ | 선택 | ❌→✅ |
| CORRUPTED status | ✅ | ❌ | ✅ | 불필요 | ❌ |

---

## 맥 이전 관련

state_store.py의 FileLock은 이미 macOS 호환 (fcntl). 전체 state 관리 계층에서 Windows 전용 코드는 없음.
Kiwoom COM 제거(REST 전환) 완료 후 state 계층은 macOS에서 그대로 동작.

---

## 커밋 이력

| Commit | 내용 |
|--------|------|
| 988cbd13 | date lock, missing data filter, snapshot ID |
| c150f2fb | same-day re-entry block, hold_days, archive on reset |
| (미커밋) | **Gap #8+#9: per-strategy state + committed HEAD v2** |
