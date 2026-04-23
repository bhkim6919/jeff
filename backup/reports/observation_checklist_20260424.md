# 2026-04-24 (금) 관찰 체크포인트 — KR + US 통합

**작성**: 2026-04-24 00:50 KST (세션 막바지, Jeff 자기 전)
**목적**: 어제 밤 배포된 R26/R5/R6/Issue 1/2/3 + 이전 누적 (R1~R25) 의
자연 관찰. KR 장중 ~ US 장 마감 까지 하루 전체.

> **전제**: 어제 밤 23:00 경 Jeff 가 tray 재실행 완료 → 모든 "deploy 대기"
> 항목이 이미 활성. 오늘은 **자연 실행 결과를 관찰만**.

---

## 🌅 1) 아침 일어나자마자 (KST 05:00 ~ 08:00)

### 1-A. US Lab Forward 자동 EOD 결과
어젯밤 Jeff 가 요청한 "22일/23일 안 돈 이유" 를 확인하는 타이밍.

**명령**:
```bash
curl -s http://localhost:8081/api/rebalance/status | python -m json.tool
```

**기대 (정상)**:
```json
"batch_fresh": true,
"last_batch_business_date": "2026-04-23",   // US 기준 수요일 close
"snapshot_version": "2026-04-23_batch_...POST_CLOSE",
"block_reasons": []
```

**이상 시**:
- `last_batch_business_date` 여전히 "2026-04-21" → 자동 EOD 또 실패
- 즉시 확인: `kr/data/pipeline/state_20260424.json` → `lab_eod_us` step 상태
  ```bash
  cat kr/data/pipeline/state_20260424.json | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('steps',{}).get('lab_eod_us'))"
  ```
- 실패 원인이 MARKET_CLOSED 면 22일 재현. 그 외면 tray/orchestrator 로그 점검.

### 1-B. US forward HEAD 복구 (Issue 3) 확인
어제 복구한 `bf33f2f8` 이 정상 HEAD 로 유지되는지.

**명령**:
```bash
cat us/lab/state/forward/meta.json | grep last_committed_run_id
```

**기대**: `"last_committed_run_id": "bf33f2f8"` 유지. 만약 빈 run_id 나 다른
run_id 로 바뀌어 있으면 자동 EOD 가 덮어쓴 것 — `us/lab/state/forward/versions/`
를 확인해 새 run 인지 또 stale 인지 판별.

### 1-C. US Lab Forward 대시보드 18 전략 카드
- [localhost:8081/lab](http://localhost:8081/lab) 접속
- base 10 전략 카드: Equity 이 $100k 에서 움직였는지, Positions > 0 인지
- HA 10 전략: 여전히 0 예상 (source 유실, config 주석 상태)

---

## 🕰️ 2) KR 장 전 (KST 08:00 ~ 09:00)

### 2-A. tray 메뉴 "(US) Run Batch Now" 활성화 확인
**재현 테스트** (Jeff 의 어제 지적):
- KST 08:00 경 (= ET 19:00 전날, post-close) → "Run Batch Now" **활성화** 돼야 정상
- 만약 여전히 "(장 마감 전)" grayed → 버그 (ZoneInfo 실패 or `_is_after_us_close` 로직 문제)

### 2-B. Issue 2 콘솔창 재현 확인
- 어제 15분 간격으로 뜨던 빈 cmd 창이 **더 이상 안 뜸** 확인
- 만약 또 뜬다면: `backup/reports/incidents/watchdog_stdout.log` 체크 (새 .bat 이 출력 리다이렉트 하는 곳)

### 2-C. KR 대시보드 Lab Forward 9전략 상태
- [localhost:8080/lab](http://localhost:8080/lab)
- R8/R9 fix 효과 — 페이지 첫 접근 시 초기화 lazy-restore 정상 동작
- 9전략 (HA 주석 처리) 실거래 반영

---

## 🟢 3) KR 장중 (KST 09:00 ~ 15:30)

### 3-A. KR Live engine 동작
- Live 창 모니터: `[LOOP] equity~... | pos=...`
- 30초마다 heartbeat (R10), trail stop (-12%), DD guard (일 -4%, 월 -7%)

### 3-B. D-BATCH / D-QOBS 패널 (R13/R14)
- [localhost:8080/#debug](http://localhost:8080/#debug)
- `heartbeat.tick_seq` 가 증가하는 것만 확인 (숫자 커지면 정상)

### 3-C. (관찰만) tray 재실행 감지
- 혹시 live engine 이 죽으면 tray 가 잡아서 재시작. 수동 개입 금지.

---

## 🔔 4) KR 장 마감 후 자연 Batch (KST 15:35 ~ 18:00)

### 4-A. 15:35 ~ — `lab_eod_kr` 자동 실행
- 9전략 EOD 반영. 
- `kr/data/lab_live/trades.json` mtime 갱신되는지.

### 4-B. 16:05 ~ — **핵심! KR_BATCH 자연 실행**

이게 **어제 배포한 모든 R-항목의 첫 실전 검증**. 꼭 확인할 로그.

**로그 경로**: `kr/logs/gen4_batch_20260424.log` (CLI) + `kr/data/logs/rest_api_20260424.log` (tray)

**필수 확인 라인**:

| 로그 패턴 | 의미 | 기대값 |
|-----------|------|--------|
| `[1/5] OHLCV CSV update` | R26(A) Step 1 분리 | "SKIP (checkpoint)" 또는 "Updated N/M stocks" |
| `[1/5] OHLCV DB sync` | R26(A) Step 1 분리 (신규 단계) | "DB synced: N stocks" |
| `[UNIVERSE_SHADOW] csv=X db=Y diff_pct=Z%` | R4 Stage 1 shadow | `diff_pct < 1%` + 샘플 정상 (3영업일 중 1일차) |
| `[BATCH_SNAPSHOT_VERSION]` | snapshot_version fingerprint | 정상 4콜론 포맷 |
| `[REGIME_SNAPSHOT] SIDE/BULL/BEAR KOSPI=6476 MA200=≈4285 ratio=≈1.51` | R11 sanity | ratio ∈ [0.5, 2.0] |
| `[5/5] Fundamental` 내부: `CSV reused` or `fetched + saved` | R26(A) Step 5 | 적절히 선택 |
| `Fundamental DB already fresh` or `DB upsert: N rows` | R26(A) Step 5 DB 독립 판정 | 상황별 |
| `[6/7] Collecting fundamental snapshot` 내부: 동일 | R26(A) Step 6 | 상황별 |
| `[R3_TRUNCATE_GUARD]` | R3 guard 경고 | **안 떠야 정상** (CSV 오염 감지 시에만) |
| `[REGIME_SNAPSHOT] KOSPI.csv corruption` | R11 sanity 오류 | **안 떠야 정상** |

**타이밍 target**:
- 16:05 KR_BATCH 시작
- 17:35 ± 5분 KR_BATCH SUCCESS (R18 deadline 18:00)
- 17:40 ± 5분 KR_EOD SUCCESS (post_batch_arm)

**D-QOBS 패널** ([localhost:8080/#debug](http://localhost:8080/#debug)):
- `KR_BATCH`: `대기 → 실행창 → SUCCESS`
- `marker.incidents`: 비어 있어야 정상
- R6 추가: **`metrics.universe_count: 901`** (± 50 변동 정상) ← **내일 첫 가시화**

---

## 🌙 5) 저녁 (KST 18:00 ~ 22:00)

### 5-A. 배치 incident 여부
- `backup/reports/incidents/` 에 오늘 날짜 새 md 파일 없어야 정상
- 있으면: stale_sweep / preflight_fail / stalled_running 등 확인

### 5-B. Telegram deadman 채널
- 알림 없어야 정상 (R5 STALE_DB/CACHE 미발동, R12 window 정합)

### 5-C. AUTO GATE Report (선택)
- tray 메뉴 "AUTO GATE Report (Latest)" → 오늘 분포 확인
- BUY 가 advisory WARN 만 쌓이는 정상 상태

---

## 🇺🇸 6) US 장중 (KST 22:30 ~ 익일 05:00)

### 6-A. US live engine
- [localhost:8081](http://localhost:8081) 에서 live mode equity 변동 확인
- `[LOOP] equity~$107,704 | pos=21` 같은 라인이 1분 주기

### 6-B. STALE 경고 해소 여부
- 어제 us_app log 에 `[STALE] GOOGL: last=2026-04-21T...` 가 연속 있었음
- 오늘 live 재시작 후에도 prev_close 업데이트 안 되면 또 stale → DB `ohlcv` 테이블에 US 티커 upsert 되는지 확인

---

## 🌅 7) 익일 새벽 (KST 04:55 ~ 05:30) — **리밸 타이밍!**

어제 Jeff 가 원했던 **22일 실패한 US 리밸 실행**의 진짜 타이밍.

### 7-A. ET 16:05 (= KST 05:05) US 자동 batch
- Primary: orchestrator `lab_eod_us` step (±10min window = KST 04:55~05:15)
- Fallback: tray 내부 (KST 05:05 initial + retry ×3, max ~24분 커버)

### 7-B. 자동 batch 성공 시
```bash
curl -s http://localhost:8081/api/rebalance/status | python -m json.tool | grep -E "batch_fresh|last_batch|block_reasons"
```
기대: `batch_fresh=true`, `block_reasons=[]`

### 7-C. 리밸 실행 (Jeff 수동)
1. 브라우저 F5 새로고침 (어제 수정한 request_id JS 로딩 확인)
2. [localhost:8081/lab](http://localhost:8081/lab) → Rebalance Preview
3. Sell Only → 체결 확인 → Buy Only (또는 Sell+Buy 한 번에)
4. "Failed: request_id required" 오류 **안 나야 정상** (Issue 1 fix)

### 7-D. 자동 batch 실패 시
1. tray 메뉴 → US Market → **Run Batch Now** (이 시각에는 활성화돼야 정상)
2. 수동 실행 후 7-B 재확인 → 7-C 진행

---

## 📋 아침 빠른 체크 3줄 요약

```bash
# 1) US batch freshness
curl -s http://localhost:8081/api/rebalance/status | python -m json.tool | grep -E "batch_fresh|last_batch"

# 2) US forward HEAD 유지
grep last_committed_run_id us/lab/state/forward/meta.json

# 3) Watchdog console 안 뜨는지 확인 (14분/29분/44분/59분 시점에 화면 체크)
```

---

## 🚨 문제 발생 시 triage 우선순위

| 증상 | 1차 확인 | 2차 액션 |
|------|---------|---------|
| D-QOBS heartbeat age > 120s | tray 죽음 | tray 재시작 |
| D-BATCH progress stuck > 10min | Step 5 fundamental hang | R16 6000s 타이머 기다림 |
| `[R3_TRUNCATE_GUARD]` 뜸 | CSV 오염 감지됨 | `scripts/restore_ohlcv_from_db.py` 실행 |
| `[UNIVERSE_SHADOW_FAIL]` | DB 연결 문제 | shadow만 fail, 배치는 CSV 로 계속 진행 |
| `[REGIME_SNAPSHOT] KOSPI.csv corruption` | KOSPI.csv 오염 | yfinance 재수집 |
| 빈 cmd 창 또 뜸 | Issue 2 fix 무효 | `scripts/install_watchdog_external.ps1 -Action install` 재실행 필요 |
| US `batch_fresh=false` 지속 | 자동 EOD 실패 누적 | `state_20260424.json::lab_eod_us` 에러 확인 |
| US 리밸 request_id 오류 재현 | 브라우저 캐시 | 강제 새로고침 (Ctrl+F5) |
| US lab forward 전부 0건 | HEAD 또 corrupt | `meta.json::last_committed_run_id` 확인 |

---

## 🗂️ 관련 파일 빠른 참조

- **오늘의 KR 로그**: `kr/logs/gen4_batch_20260424.log`
- **Marker (canonical truth)**: `kr/data/pipeline/run_completion_20260424.json`
- **Orchestrator state**: `kr/data/pipeline/state_20260424.json`
- **Incidents**: `backup/reports/incidents/` (오늘 날짜 파일 체크)
- **Watchdog stdout**: `backup/reports/incidents/watchdog_stdout.log` (Issue 2 수정 후 새로 생김)
- **US 리밸 API 베이스**: http://localhost:8081
- **US 대시보드**: http://localhost:8081/lab
- **KR 대시보드**: http://localhost:8080/#debug

---

## ✅ 오늘 "완결 판정" 조건 (Jeff 원칙 고정)

> "오늘과 같은 장애 유형 재발방지는 거의 완료되었으나, 구조적 근본 해결(R4)은
> 아직 미완이므로 완결로 선언하지 않는다."

| 계층 | 상태 |
|------|------|
| 1. 직접 원인 대응 | 대부분 완료 |
| 2. 운영 배포 | ✅ tray 재시작 완료 (어제 밤) |
| 3. 구조적 근본 해결 | R4 Stage 2 관찰 1/3일차 (오늘 diff_pct 수집) |

**따라서 오늘도 "완료" 선언 금지**. 금·월·화 3영업일 shadow 관찰 후 JUG + Jeff 승인으로 Stage 3 전환 제안.

---

## 📝 남은 작업 정리 (2026-04-24 기준)

### 🔴 긴급도 높음 — Jeff 결정 필요

| 항목 | 상태 | 결정 옵션 | 예상 시간 |
|------|------|----------|----------|
| **HA 전략 9개 경로 판정** | .py source 유실, .pyc 만 있음. 현재 config (b) 주석 처리로 운영 중 | (a) HA 필터 9전략 재작성 2~4h / (b) 영구 제거 (현상 유지) / (c) PYC_CRITICAL_MODULES 우회 허용 10min | Jeff 선택 |
| **R4 Stage 3 전환** | Stage 1 shadow 가동 중, 3영업일 diff_pct 수집 예정 (금·월·화) | 3일 모두 diff_pct < 1% + 극단치 없음 → JUG + Jeff 승인 후 전환 | 관찰 3일 후 |

### 🟡 관찰 의존 — 코드 완료, 검증 대기

| 항목 | 검증 시점 | 무엇을 본다 |
|------|----------|-----------|
| R26 (A) batch.py 3곳 CSV/DB 독립 | 오늘 16:05 batch 로그 | "CSV reused"/"DB upsert N rows"/"already fresh" 라인 |
| R26 (C) sync_guard helper | 지금은 미사용 (future writer 대비) | 신규 writer 추가 시 helper 위임 여부 |
| R5 watchdog STALE 알림 | 14분 이내 첫 tick | incident md 파일 / Telegram deadman |
| R6 MetricsBlock universe_count | 오늘 16:05 batch 후 marker | `run_completion_20260424.json::runs.KR_BATCH.metrics.universe_count` |
| R11 Regime MA200 sanity | 16:05 batch 후 | `[REGIME_SNAPSHOT] ratio` ∈ [0.5, 2.0] |
| R18 deadline 현실화 | 17:35 ~ 18:00 | KR_BATCH SUCCESS 가 18:00 전에 |
| R24 Rebalance D-day | 아무 때나 | 대시보드 Next/D-day 표시 정확 |
| Issue 1 request_id | 내일 새벽 리밸 | Preview/Sell/Buy 클릭 시 오류 없음 |
| Issue 2 콘솔창 제거 | 매 15분 tick | cmd 창 미출현 |
| Issue 3 US HEAD 복구 | 내일 새벽 EOD | meta.json 이 덮어쓰이지 않고 새 run_id 로 progress |

### 🟢 코드 미완 — 선택적 추가 (급하지 않음)

| 항목 | 설명 | 비고 |
|------|------|------|
| **R26 (B)** | Explore 조사 결과 `fundamental_collector.py:330,394` / `swing_collector.py:225` 는 CSV-exists-skip 아닌 다른 용도로 확인 → 실제 수정 대상 **없음** | **실질 완료로 판정** |
| **tray menu 라벨 명시** | "(장 마감 전)" → "(US 장 마감 전)" / "(KR 장 마감 전)" 로 혼동 제거 | 10초 수정. 원하시면 내일 추가 |
| **`_is_after_us_close` except 블록 로그** | 현재는 조용히 return False → 버그 시 영구 grayed | 로그 한 줄 추가 (5분) |
| **scripts/install_watchdog_external.ps1 재실행 가이드** | Jeff 가 재설치 때 `-Hidden` + pythonw 자동 적용하도록 문서화 | install/uninstall 절차 README 보강 |
| **US state directory 재설계** | 현재 `us/lab/state/forward/` 만 사용, `us/data/lab_live/` 는 비어 있음 (KR 와 parity 깨짐) | 의도된 설계인지 Jeff 확인 필요 |

### 🔵 문서 / 메모리 정리 (내일 이후)

- [ ] `work_plan_20260423.md` 의 "완료" 표기 최신화
- [ ] `work_plan_20260424_continuation.md` 에 이번 세션 완결분 정리
- [ ] `CLAUDE.md` §Q-TRON Project Rules 에 새 R-items 간단 언급 (지금은 R7 까지만 명시)
- [ ] `docs/PIPELINE_ORCHESTRATOR.md` 에 MetricsBlock 스키마 추가

### 🟣 장기 — 진행 중이거나 뒤에 다룰 것

| 항목 | 상태 | 비고 |
|------|------|------|
| **R4 S2/S3** | 관찰 중 | 3영업일 shadow → default 전환 |
| **US batch 22/23 안 돈 이유 RCA** | 내일 아침 state_20260424 보고 판단 | lab_eod_us step 상태 / MARKET_CLOSED 재현? |
| **US Lab Forward HA 10전략** | 계속 0건 예상 (source 없음) | HA 경로 판정 (a/b/c) 후에 결정 |
| **Kiwoom Provider 통합** | Gen4 main TODO (memory.md) | LIVE 모드 완성 |
| **분봉 Phase 2~5** | 데이터 축적 후 판단 | 리밸 테스트 통과 후 |
| **자동화 로드맵** (BIOS Wake-on-RTC) | 미착수 | Step 3 안정 확인 후 |

---

## 🎯 한 줄 요약

> **오늘의 미션 = 어제 배포한 R-items + Issues 가 자연스럽게 작동하는 걸
> 관찰 기록**. 건드리지 말고 로그·마커·대시보드만 본다. 문제 발생 시
> §Triage 우선순위 따름. 내일 새벽 05:00~05:30 에 US 리밸 수동 실행 가능.
