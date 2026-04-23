# PR: R1-R26 batch 재발방지 + 3 urgent hotfixes

**Branch**: `feature/gen3-v7` → `main`
**Create PR**: https://github.com/bhkim6919/jeff/compare/main...feature/gen3-v7?expand=1

---

## Title

```
feat(batch+lab): R1-R26 batch 재발방지 대책 + 3 urgent hotfixes
```

## Summary

- 2026-04-23 KR batch universe=0 장애의 구조적 재발방지 (R1~R26) 와 당일 밤 Jeff 3건 보고 (US manual rebalance 실패 / 빈 cmd 창 / US lab-forward 0건 거래) 을 한 번에 배포.
- 이번 세션 신규: R26(A) CSV-exists skip 근본 수정, R26(C) shared/db/sync_guard 범용 helper, R5 watchdog STALE_DB/CACHE, R6 MetricsBlock+universe_count.
- 이전 세션 누적 deploy 대기분 (R3/R4S1/R8-R25 + HA(b)) 전부 이 커밋과 함께 Jeff tray 재실행 (2026-04-24) 으로 활성화.

## What's New (이번 세션)

| Area | 변경 | 위치 | 테스트 |
|------|------|------|--------|
| R26(A) | batch.py Step 1 (OHLCV) checkpoint csv/db 분리 + Step 5/6 Fundamental CSV/DB 독립 판정 | `kr/lifecycle/batch.py`, `kr/data/db_provider.py` helpers | 10 tests |
| R26(C) | 범용 `db_sync_guard` helper (9-cell × CSV/DB/fetch 진실 테이블 + 4 resilience) | `shared/db/sync_guard.py` | 11 tests |
| R5 | watchdog stdlib-only STALE_DB (marker snapshot_version 역산) + STALE_OHLCV_CACHE (CSV mtime) | `scripts/watchdog_external.py` | 25 tests |
| R6 | MetricsBlock + universe_count 일별 trend → 2026-04-23 사고의 10일 조기 가시화 | `kr/pipeline/completion_marker.py`, `marker_integration.py`, `preflight.py` | 14 tests |
| Issue 1 | US `/api/rebalance/execute` request_id UUID 주입 | `us/web/templates/index.html:860` | — |
| Issue 2 | CREATE_NO_WINDOW 플래그 + watchdog `.bat` → `pythonw.exe` + Install `-Hidden` + Task 재등록 | `kr/tray_server.py`, `kr/web/app.py`, `scripts/watchdog_external.bat`, `scripts/install_watchdog_external.ps1` | — |
| Issue 3 | US forward HEAD 포인터 복구 (2d90c9fa empty → bf33f2f8 정상, backup/us_state_backup/ 에 사본) | `backup/us_state_backup/meta_20260424_before_head_restore.json` | — |

## 이전 세션 누적 Deploy 대기 (이번 PR 에 함께)

R1, R2, R3, R4 Stage 1, R7, R8/R9, R10, R11 (+KOSPI.csv 재생성), R12, R13, R14, R15, R16, R17, R18, R19, R20, R21, R22, R23, R24, R25, HA(b)

## 남은 관찰/결정 항목 (PR 범위 밖)

- **R4 Stage 2 → Stage 3 전환**: 3영업일 `[UNIVERSE_SHADOW] diff_pct < 1%` 관찰 → Jeff + JUG 승인 필요
- **HA 전략 9개 경로 (a/b/c) 판정**: Jeff 결정 대기 (현재 config 주석 (b) 로 운영)
- **16:05 배치 자연 관찰**: D-BATCH, D-QOBS, `[UNIVERSE_SHADOW]`, `[REGIME_SNAPSHOT]` 로그 체크

## Test plan
- [x] `pytest kr/tests scripts/test_watchdog_external.py` — 485 passed, 0 regression
- [x] Neu tests: R26(A) 10 + R26(C) 11 + R6 14 + R5 25 = 60 신규
- [ ] 2026-04-24 16:05 자연 KR_BATCH 관찰 → D-BATCH / D-QOBS 정상, `[CSV reused|fetched]` + `[DB upsert|already fresh]` 로그 확인
- [ ] Jeff: US 대시보드에서 Rebalance Execute 3개 버튼 클릭 시 request_id 필요 오류 사라짐 확인
- [ ] 다음 watchdog tick (15분 후) 에 빈 cmd 창 미출현 확인
- [ ] US Lab Forward 대시보드 18전략 카드 정상 복구 (equity/positions 복귀)
