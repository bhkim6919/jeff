# P0-2 — Debug 분리 Dependency Map

**작성일**: 2026-04-24
**목적**: KR `index.html` 내부 debug 섹션 16개를 `/debug` 라우트로 이전하기 전 **모든 의존성** 정리.
**기준 문서**: [`ui_unification_ia_20260424.md`](./ui_unification_ia_20260424.md) §5 Phase 0

---

## 0. 왜 map 작성이 필요한가 (Jeff 지적)

> "13개 debug panel이 dashboard.js, DOM id, hidden class, mode-debug 조건에 물려 있을 가능성이 높아서 단순 템플릿 이동으로 끝나지 않을 수 있습니다."

실제 조사 결과: **16개 섹션** (TOC 13 + `sec-batch-log`, `sec-qobs`, `sec-debug-toc` 자체) + **SSE 데이터 파이프라인 공유** + **모드 전환 로직** + **7개 init 함수** + **10+ 업데이트 함수**가 얽혀 있음.

이 map 없이 템플릿만 옮기면 dashboard.js가 null element에 append하다 터지거나, 모드 전환 시 화면이 깨짐.

---

## 1. 섹션 인벤토리 (16개)

모두 `kr/web/templates/index.html` 내부, `mode-debug` 클래스 + `hidden` 속성 기본.

| # | section ID | TOC 라벨 | 기능 | 복잡도 |
|---|-----------|---------|------|--------|
| 1 | `sec-debug-toc` | D-TOC | 네비게이션 chip | Low |
| 2 | `sec-db-health` | D-DB | PostgreSQL 상태 | Mid (`loadDbHealth()`) |
| 3 | `sec-control` | D-SYS | 시스템 컨트롤 카드 | Mid (`updateControlCards`) |
| 4 | `sec-freshness` | D-FRESH | 데이터 신선도 그리드 | Mid (`updateFreshnessGrid`) |
| 5 | `sec-traces` | D-TRACE | 요청 추적 테이블 | High (filter + `updateTracesTable`) |
| 6 | `sec-market-ctx` | D-MCTX | 시장 컨텍스트 | Mid (`loadMarketContext`) |
| 7 | `sec-data-events` | D-EVT | 데이터 공급 이벤트 | Mid |
| 8 | `sec-test-order` | D-TEST | 테스트 주문 (Paper) | High (form + 실제 API) |
| 9 | `sec-ws-sync` | D-WS | WebSocket 타임스탬프 | Mid (`updateWSCard`, `updateTimestampsCard`) |
| 10 | `sec-sync` | D-SYNC | REST vs COM 비교 | Mid (`updateSyncTable`) |
| 11 | `sec-raw-json` | D-RAW | Raw JSON 덤프 | Low (`updateRawJson`) |
| 12 | `sec-histogram` | D-HIST | 응답시간 분포 | Mid (`updateLatencyHistogram`) |
| 13 | `sec-logs` | D-LOG | 로그 스트림 | High (`fetchLogs`, auto-refresh) |
| 14 | `sec-diff` | D-DIFF | 상태 변경 추적 | High (`updateStateDiff`, `computeDiff`) |
| 15 | `sec-batch-log` | (D-BLOG?) | 배치 로그 패널 | High (`initBatchLogPanel`, `fetchBatchLog`, autorefresh) |
| 16 | `sec-qobs` | (D-QOBS?) | Qobs 패널 | High (`initQobsPanel`, `fetchQobs`, autorefresh) |

→ **16개 중 High 복잡도 6개 주의**: traces, test-order, logs, diff, batch-log, qobs.

---

## 2. DOM 요소 ID 의존성

옮길 때 함께 이동해야 할 인라인 element ID (dashboard.js가 `getElementById`로 직접 참조):

### 2.1 Freshness Grid
- `freshness-grid`

### 2.2 Traces
- `trace-filter-status`, `trace-filter-search`
- 테이블 `tbody` (ID 별도 확인 필요)

### 2.3 WS Sync
- `ws-connected`, `ws-last-msg`, `ws-msg-count`, `ws-reconnects`
- timestamps 관련 element들

### 2.4 Raw JSON
- `raw-json`

### 2.5 Diff
- `diff-container`

### 2.6 Batch Log
- `btn-refresh-batch-log`, `batch-log-autorefresh`
- `batch-log-container`, `batch-log-source`

### 2.7 Qobs
- `btn-refresh-qobs`, `qobs-autorefresh`
- `qobs-container`

### 2.8 Histogram
- (후속 확인 — `renderHistogram` 내부 element ID)

### 2.9 Logs
- (후속 확인 — `renderLogs` 내부 element ID)

### 2.10 Test Order
- (후속 확인 — form input ID들)

### 2.11 DB Health, Control Cards, Market Context, Data Events
- 각 카드 내부 element IDs (후속 확인)

→ **행동 지침**: 이동 시 섹션 HTML을 통째로 복사 (element ID 유지).

---

## 3. dashboard.js 함수 의존성 (90개 중 debug 관련 20개)

### 3.1 Mode 전환 (L1724~1770)
```
initModeSwitcher()              — Mode 초기화
switchMode(mode)                — mode-operator / mode-debug 토글 (L1752-1760)
  → .mode-debug 요소 hidden 제어
  → mode==='debug'일 때 fetchLogs(), updateLatencyHistogram(), loadDbHealth() 호출
```

### 3.2 SSE 업데이트 파이프라인 (L153~189)
```
updateDashboard(data)
  ├ 항상 호출:
  │   ├ updateControlCards(data)        ← sec-control
  │   └ updateFreshnessGrid(data.freshness) ← sec-freshness
  │
  ├ if operator OR debug:
  │   ├ updateTracesTable(data.traces)     ← sec-traces
  │   ├ updateWSCard(data.websocket)       ← sec-ws-sync
  │   ├ updateTimestampsCard(data.timestamps) ← sec-ws-sync
  │   └ updateSyncTable(data.sync)         ← sec-sync
  │
  └ if debug:
      ├ updateRawJson(data)             ← sec-raw-json
      ├ updateLatencyHistogram()        ← sec-histogram
      └ updateStateDiff(data)           ← sec-diff
```

**이동 시 결정 필요**:
- SSE 스트림 `/api/dashboard/stream` 은 하나만 유지 (양 페이지 구독)
- dashboard.js를 **양 페이지 모두** 로드 → `getElementById` 결과 null일 때 skip 방어 로직 추가
- 또는 debug 전용 함수만 debug.js 로 분리

### 3.3 Init 함수들 (일회성 setup)
| 함수 | 호출 시점 | 대상 섹션 |
|------|----------|----------|
| `initModeSwitcher()` | DOMContentLoaded | 전체 모드 |
| `initAlertClose()` | DOMContentLoaded | 일반 alert (debug 무관) |
| `initTraceFilters()` | DOMContentLoaded | `sec-traces` |
| `initCopyJson()` | DOMContentLoaded | `sec-raw-json` |
| `initBatchLogPanel()` | DOMContentLoaded | `sec-batch-log` |
| `initQobsPanel()` | DOMContentLoaded | `sec-qobs` |
| `initLogRefresh()` | DOMContentLoaded | `sec-logs` |

→ 이동 시 debug.html 쪽에서 `DOMContentLoaded` 호출이 제대로 실행되는지 확인. 공통 `dashboard.js`를 debug.html에도 로드하면 같은 init 함수들이 동작.

### 3.4 Debug 전용 fetch/update 함수 (14개)
```
loadDbHealth()                 L1869
fetchLogs() + renderLogs()     L1689, L1699
initLogRefresh()               L1684
updateLatencyHistogram()       L1275
renderHistogram(buckets)       L1285
updateStateDiff(data)          L1314
computeDiff(old, new, prefix)  L1343
initBatchLogPanel()            L1383
fetchBatchLog()                L1405
renderBatchLog(data)           L1417
initQobsPanel()                L1534
fetchQobs()                    L1553
renderQobs(data)               L1565
loadMarketContext()            L2962
updateControlCards(data)       L1052
updateFreshnessGrid(freshness) L1088
updateWSCard(ws)               L1133
updateTimestampsCard(ts)       L1148
updateTracesTable(traces)      L1160
initTraceFilters()             L1212
updateSyncTable(syncItems)     L1221
updateRawJson(data)            L1249
initCopyJson()                 L1259
```

**분류**:
- **데이터 표시만** (SSE에서 자동 호출): updateControlCards, updateFreshnessGrid, updateWSCard, updateTimestampsCard, updateTracesTable, updateSyncTable, updateRawJson, updateStateDiff — 대상 DOM 없으면 safe skip 필요
- **사용자 상호작용**: initTraceFilters, initCopyJson, initBatchLogPanel, initQobsPanel, initLogRefresh, loadMarketContext — 별도 페이지에서 재호출
- **모드 전환 시 ad-hoc**: loadDbHealth, fetchLogs, updateLatencyHistogram — `/debug` 페이지 로드 시 호출로 변경

---

## 4. CSS 의존성

### 4.1 `style.css`
- `mode-debug` 클래스: 6회 사용 (hidden 규칙, 레이아웃, 색상 override)
- `debug-toc-chip`, `debug-toc-chip-new` 클래스
- 섹션별 개별 클래스 (확인 필요)

### 4.2 인라인 스타일
- TOC chip `<code>` 스타일 (inline, 확인 필요)

**이동 시**: `style.css`의 debug 관련 규칙을 **그대로 유지**. `mode-debug` 대신 `/debug` 페이지에서는 기본 visible 상태.

---

## 5. 서버 측 (app.py) 의존성

### 5.1 Flask 라우트 신설
```python
@application.get("/debug")
async def debug_page(request):
    return templates.TemplateResponse("debug.html", {...})
```

### 5.2 공유 API 엔드포인트
아래 API들은 debug 페이지에서도 호출되므로 **그대로 유지**:
- `/api/dashboard/stream` (SSE)
- `/api/logs/stream`
- `/api/db-health`
- `/api/market-context`
- `/api/batch/log` (확인 필요)
- `/api/qobs/*` (확인 필요)
- `/api/traces/*`
- `/api/test-order/submit` (D-TEST)

→ **서버 측 변경 없음**. 라우트 1개만 신설.

---

## 6. 이식 전략 (권장)

### 6.1 Option A — 최소 변경 (권장)

1. `kr/web/templates/debug.html` 신설 (지금 없음)
2. `index.html`에서 `mode-debug` 섹션 16개를 **그대로 복사**하여 `debug.html`에 붙여넣기 (element ID 보존)
3. `index.html`에서는 해당 섹션들 **삭제** (단, SSE가 호출하는 common 섹션 `sec-control`/`sec-freshness`는 operator 모드에서도 쓰이므로 **남겨둘지 debug로 완전 이동할지 결정 필요** — §6.3 참조)
4. `app.py`에 `/debug` 라우트 추가
5. `dashboard.js`의 update 함수들에 **null check 추가** (getElementById 결과 null이면 skip)
6. `switchMode()` 로직 제거 또는 단순화 (이제 mode 전환 없음, /debug 진입 = debug 모드)
7. nav.js에 debug 진입 아이콘 추가 (톱니바퀴 또는 `?debug=1`)

### 6.2 Option B — 분리 JS (Phase 3 후 고려)
debug.js 신설하고 debug 전용 update 함수 이주. Phase 3에서 컴포넌트화 할 때 더 깔끔하지만 지금은 과투자.

### 6.3 sec-control / sec-freshness / sec-ws-sync / sec-traces / sec-sync 의 운명

`updateDashboard`에서 **조건부 호출**되는 섹션들. 실제 운영 모드에서도 일부 유용:
- `sec-control` — 시스템 상태 (Jeff 주용도)
- `sec-freshness` — 데이터 신선도 (운영 판정)
- `sec-ws-sync` — WebSocket (운영 중요)
- `sec-traces` — API 추적 (debug 중심)
- `sec-sync` — REST vs COM (debug 중심)

→ **결정 필요**:
- A: 전부 `/debug`로 (Jeff의 "Debug 분리" 원칙 철저)
- B: `sec-control`, `sec-freshness`, `sec-ws-sync`는 Dashboard에 남기고 나머지만 `/debug`로
- C: 선택적 노출 (?debug=1 쿼리로 숨김 토글)

**권장 B**: 운영 시에도 시스템 상태·데이터 신선도·WebSocket 헬스는 필수. Traces/Sync/RawJson/Diff/Histogram/Logs/BatchLog/Qobs/TestOrder/DataEvents/MarketCtx/DbHealth 등 **11개만 /debug 이전**.

즉:
- **이동 대상 11개**: sec-debug-toc, sec-db-health, sec-traces, sec-market-ctx, sec-data-events, sec-test-order, sec-sync, sec-raw-json, sec-histogram, sec-logs, sec-diff, sec-batch-log, sec-qobs (13개)
- **Dashboard 잔류 3개**: sec-control, sec-freshness, sec-ws-sync
- 계: 13 이동 + 3 잔류 = 16 (전체)

*주: sec-debug-toc도 이동 대상 → Dashboard에서는 완전 제거. debug.html 내부에 자체 TOC 존재.*

---

## 7. 검증 체크리스트 (P0-2 실행 후)

| # | 검증 항목 | 방법 |
|---|----------|------|
| 1 | `/debug` 페이지 200 응답 | curl |
| 2 | 13개 debug 섹션 모두 렌더 | DOM 검사 |
| 3 | SSE 스트림이 debug 페이지에서도 수신 | 콘솔 네트워크 탭 |
| 4 | dashboard.js null check 동작 (index에서 traces update skip) | 콘솔 에러 0 |
| 5 | nav.js에 debug 진입 경로 추가 | UI 확인 |
| 6 | Dashboard 기본 모드가 `operator` (mode 전환 UI 제거됨) | 기본 렌더 정상 |
| 7 | Test Order 기능 `/debug`에서 정상 | 테스트 주문 제출 |
| 8 | Batch Log autorefresh 동작 | 30s 대기 확인 |
| 9 | Log stream autorefresh 동작 | 30s 대기 확인 |
| 10 | `style.css` `mode-debug` 규칙 유지 (스타일 깨짐 방지) | 시각 비교 |
| 11 | US `:8081`는 영향 없음 (KR-only 작업) | US Dashboard 확인 |
| 12 | `/debug`에서 Dashboard 복귀 버튼 작동 | 클릭 테스트 |

---

## 8. 리스크 & 완화

| 리스크 | 영향 | 완화 |
|--------|------|------|
| `dashboard.js` null check 누락 → console error 폭증 | 높음 | 모든 `getElementById` 뒤에 `if (!el) return;` 또는 optional chaining |
| `switchMode()` 제거 시 기존 localStorage `qtron_mode` 잔재 | 낮음 | 기본 operator로 강제, 로그 남김 |
| debug 진입 경로 누락 | 중 | nav.js 아이콘 + `?debug=1` 파라미터 2중 |
| Test Order form ID 충돌 (새 페이지) | 낮음 | ID 그대로 유지, 한 페이지에 1개만 존재하게 index에서 완전 제거 |
| SSE stream 중복 구독으로 부하 증가 | 낮음 | 브라우저 탭 2개 열면 원래도 2번 연결. 기존과 동일 |
| Batch Log / Qobs autorefresh 타이머 leak | 중 | 페이지 unload 시 clearInterval, `beforeunload` 핸들러 추가 |

---

## 9. 예상 커밋 단위

P0-2는 독립 커밋 3개로 분할 권장:

1. **commit 1**: `kr/web/templates/debug.html` 신설 + `/debug` 라우트 추가 + nav 진입 경로 (최소 실행)
2. **commit 2**: `index.html`에서 13개 섹션 제거 + `dashboard.js`에 null check (§8 리스크 #1 대응)
3. **commit 3**: `switchMode()` 단순화 + mode-debug 처리 정리 + §7 검증 체크리스트 통과 보고

각 커밋 후 `:8080` 재시작 + 스모크 테스트.

---

## 10. 질문 (Jeff 결정 필요)

다음 세션 P0-2 착수 전 Jeff 확인:

1. **§6.3 잔류/이동 결정**:
   - 권장 B (sec-control/sec-freshness/sec-ws-sync 3개 Dashboard 잔류)
   - 또는 A (전부 `/debug`로)?

2. **debug 진입 경로**:
   - 권장: nav 우측 톱니바퀴 아이콘 + `?debug=1` 파라미터
   - 또는: nav 메뉴에 Debug 탭 추가?

3. **`switchMode()` 제거 여부**:
   - operator/debug 2-모드 UI 완전 제거 → 기본 operator로 고정
   - 또는 basic/operator 구분은 유지?

이 3개만 답주면 P0-2 3 커밋 진행 가능.

---

*Generated by P0-2 scouting pass 2026-04-24. dashboard.js는 3080줄이라 세부 element ID는 실제 구현 시 추가 확인 필요.*
