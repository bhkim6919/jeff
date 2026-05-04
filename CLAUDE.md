# Q-TRON Project Rules

## Multi-Agent System

Command chain: **USER → JUG → TOM → Team Leads**

### Directors & Authority

| Agent | Role | Scope | Code Modify |
|-------|------|-------|-------------|
| **ALEX** | Strategy Director | 전략 분석/제안, 백테스트 해석, 팩터 변경 제안 | NO |
| **TOM** | Engineering Lead | 팀 오케스트레이션, 이슈 라우팅, P1 에스컬레이션 | NO (라우팅만) |
| **JUG** | Final Authority + QA | P1 승인, 안전 차단, QA gate, cross-team 일관성 | NO (게이트만) |
| **Demimure** | REST Design Lead | API/REST 인터페이스 설계, 상태 가시성 아키텍처 | NO (설계만) |

### Team Leads (Code Modification 권한)

| Agent | 담당 | 파일 범위 | 자율 수정 |
|-------|------|----------|----------|
| **Coral** | Core (State/Portfolio) | state_manager, portfolio_manager | P2/P3 자율 |
| **David** | Data (Provider/OHLCV) | kiwoom_provider, pykrx_provider, db_provider | P2/P3 자율 |
| **Olive** | Orchestrator (Main/Report) | main.py, reporter, GUI, lifecycle | P2/P3 자율 |
| **Ricky** | Risk (Guard/Safety) | exposure_guard, safety_checks, risk_management | P2/P3 자율, P1 LOG/GUARD 즉시 |

### Read-Only Agents

| Agent | 역할 |
|-------|------|
| **Evidence Validator** | 로그 cross-check 검증 (실행/청산/리밸/상태/TR) |
| **Log Collector** | 로그 수집/파싱, 파일 존재 확인, 누락 감지 |

## Global Safety Rules

1. **Broker = Truth** — RECON 결과가 최종 기준. Engine 상태 < Broker 조회.
2. **SELL doctrine** (JUG 확정 2026-05-04, audit 결과 분류):
   - **Safety SELL** — Trail Stop / DD trim. **항상 허용**. broker state 불확실해도 last known qty 로 fire (over-sell이 hold 보다 안전). `BuyPermission` / `monitor_only` 체크 우회.
   - **Rebalance SELL** — 포트폴리오 재구성 (target 변경). **broker reliability 필요**. `BuyPermission BLOCKED/RECOVERING` / `monitor_only_reason` / batch stale 시 차단.
   - 모든 BUY 는 차단 가능 (uncertainty 시 보수적).
3. **NEVER trust single log source** — 반드시 복수 소스 cross-check
4. **State must be backward-compatible** — old JSON → new JSON 로드 가능 필수
5. **Engine layer is protected** — 아래 Engine Protection Rules 참조
6. **No P0 execution without USER approval** — JUG → USER 승인 경로만 허용
7. **Meta/Regime = advisory only** — 엔진 override 금지 (USER 승인 없이)
8. **monitor_only clear policy** (JUG 확정 2026-05-04):
   - 자동 클리어 금지 / 수동 web endpoint 금지 / **재시작만**.
   - dashboard 가시성 + 30분 주기 CRITICAL 알림으로 운영자 인지 유도.

## Engine Protection Rules

### LOCKED (절대 수정 불가, USER 명시 지시 필요)

| File | Protected Content |
|------|-------------------|
| `kr/strategy/scoring.py` | 팩터 계산 로직 전체 |
| `kr/config.py` | 전략 파라미터: trail -12%, rebal 21일, position 20, LowVol/Mom window |

### PROTECTED RUNTIME (behavior 변경은 JUG 승인 필요)

| File | 보호 범위 |
|------|----------|
| `kr/web/lab_live/engine.py` | order/count/cash/pending/data source/idempotency 로직 |
| `kr/web/lab_live/state_store.py` | 상태 저장/복원/버전 관리 |
| `kr/web/lab_live/daily_runner.py` | OHLCV sync, DB upsert |
| `us/lab/forward.py` | EOD 실행, 포지션/체결/상태 커밋 |
| `us/lab/meta_collector.py` | equity/return 계산, qty 키 해석 |

### PROTECTED (CONFIRMED + 회귀 테스트 통과 시 수정 가능)

| File | Condition |
|------|-----------|
| `kr/core/portfolio_manager.py` | 하위 호환 + 테스트 |
| `kr/core/state_manager.py` | 하위 호환 + 백업 필수 |
| `kr/risk/exposure_guard.py` | DD guard 임계값 변경 금지 |

### Guard Classification Rule

BUY/SELL 조건, count, cash, signal, retry, data source, snapshot 변경은
**LOG/RETRY/GUARD가 아닌 behavior change**로 분류 → JUG 승인 필요.

### Order Flow Protection

- RECON 중 주문 발행 금지
- 동일 종목 BUY→SELL→BUY 3연속 감지 시 HALT

### State Protection

- State 파일 삭제 금지 (백업 후 신규 생성만)
- RECON 결과 = state 최종 truth

## Data Quality Rules

### OHLCV Sync Status

| Status | 정의 | Engine 동작 |
|--------|------|------------|
| **OK** | 전체 sync 성공 + DB last_date >= CSV last_date | 정상 실행 |
| **PARTIAL** | 일부 종목 실패 또는 날짜 불일치 (failed_ratio ≤ 10%) | 실행 허용 + 경고 로그 |
| **FAIL** | DB 연결 실패 또는 failed_ratio > 10% | DB blind trust 금지 → CSV fallback |

- **Degraded mode (O=H=L=C 등)는 warning + normal 취급 금지** → `[US_OHLC_SOURCE]` 로그 필수
- OUTLIER daily_return (>50%) → None 설정 + `[META_RETURN_DEBUG]` 로그
- SNAPSHOT_MISMATCH (position>0, exposure=0) → data_quality=BAD

### Data Source 원칙

- **KR**: raw ingestion = CSV, serving = DB, DB stale 시 CSV fallback
- **US**: Alpaca API(primary) → DB(serving), DB fallback 시 OHLC 불가 경고

### DB = Canonical Truth, CSV = Performance Cache (R7, 2026-04-23)

**원칙**: DB는 단일 진실 기준. CSV는 성능 캐시로만 간주.

| 상황 | 권장 경로 | 이유 |
|------|----------|------|
| Universe 구성 (batch step 2) | CSV primary (R4 Stage 1 shadow) | 과거 관행 유지, R4 Stage 3 전환 대기 |
| 가격 조회 (서빙) | DB primary | 2026-04-16 DB 통합 기준 |
| OHLCV 증분 업데이트 | CSV append → DB upsert | 원천 이중 쓰기 (`kr/data/pykrx_provider.py::update_ohlcv_incremental`) |
| CSV 복구 | DB → CSV dump | `scripts/restore_ohlcv_from_db.py` (R2) |

**사건 기록**:
- 2026-04-22 universe=0 사고 원인 = CSV 가 30일로 truncated, DB 는 정상 → DB 를 truth 로 삼았다면 발생 불가
- 대책: R3 truncate guard (`pykrx_provider.py:229`), R4 Stage 1 shadow (DB 병행 + diff 로그)

**R4 관찰 프로토콜** (Jeff 고정):
- Default 전환 금지 — 3영업일 `[UNIVERSE_SHADOW] diff_pct < 1%` + 극단치 없음 + JUG 승인 후 Stage 3
- CSV 실사용 유지, DB 는 shadow diff 로그만

## Idempotency Rules

### snapshot_version 구성

```
{trade_date}:{selected_source}:{data_last_date}:{universe_count}:{matrix_hash}
```

- **trade_date 단독은 idempotent 아님** — 입력 데이터 버전이 동일해야 함
- 동일 trade_date + 동일 snapshot_version → skip
- 동일 trade_date + 다른 snapshot_version → 재실행 + `[EOD_IDEMPOTENCY]` 경고
- snapshot_version은 head.json에 persist

## Meta / Regime Policy

- **Meta Layer = observer only** — 추천/비중조절 금지
- **적합도(fitness) 3레이어**: market_fit + perf_health + data_quality → final_score
- `score`/`score_value` = market_fit 의미 보존 (하위호환)
- `final_score` = UI 표시용 최종 판정
- data_quality BAD → final_score=WARN, rankable=False
- UNKNOWN + market_fit HIGH → final_score=MID (과대낙관 방지)
- **Regime = advisory** — 엔진 자동 override 금지, USER 승인 필요

## Execution Policy

| Severity | Fix Type | 승인 |
|----------|----------|------|
| P0 | 모든 타입 | JUG + USER 승인 |
| P1 | CODE_FIX | JUG 승인 |
| P1 | LOG/RETRY/GUARD (behavior 무변경) | TOM 즉시 실행 |
| P2 | CODE_FIX (Engine Protection 비침범 + 로컬) | TOM 자율 |
| P2 | 기타 | TOM 자율 |
| P3 | 모든 타입 | TOM 자율 |

## Project Structure

```
Q-TRON/
├── kr/                  # KR market (REST API, Kiwoom, :8080)
│   ├── lifecycle/       # 거래 단계 (startup, eod, monitor, rebalance, reconcile)
│   ├── web/lab_live/    # Forward Trading 엔진 + 메타분석
│   ├── strategy/        # 팩터/리밸/트레일스탑/레짐
│   ├── regime/          # 시장 레짐 예측 (domestic/global/theme)
│   ├── risk/            # DD guard, safety checks
│   ├── lab/strategies/  # 9전략 실험 프레임
│   ├── advisor/         # 자문 파이프라인
│   └── core/            # 포트폴리오/상태 관리
├── us/                  # US market (Alpaca, :8081)
│   ├── lab/             # Forward Trading (10전략)
│   ├── regime/          # US 레짐 + Alpaca snapshot
│   └── strategy/        # US 팩터
├── backtest/            # 공용 백테스트 데이터 + 엔진
├── docs/                # 매뉴얼 (KR/US/Backup PDF)
└── .claude/skills/      # 16개 운영 skill
```

## Python Environment

### Current Runtime
```
KR: C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe (Python 3.12.9 64-bit — REST 전환 완료)
US: C:\Q-TRON-32_ARCHIVE\us\.venv\Scripts\python.exe (Python 3.12.9 64-bit)
```

### Runtime Notes
- KR/US 모두 Python 3.12 64-bit 통일 (2026-04-16 전환)
- KR 구 `.venv` (Python 3.9 32-bit) 는 `.venv39_deprecated/` 로 rename 완료 (2026-04-21), 활성 경로 미사용
- 32-bit Python은 사용하지 않음 (시스템 설치 잔존, venv 미사용)
- PostgreSQL 접근: psycopg2-binary (shared/db/pg_base.py 경유)

## Key Commands

```bash
# KR (3.12 64-bit)
cd kr && ../.venv64/Scripts/python.exe main.py --batch
cd kr && ../.venv64/Scripts/python.exe main.py --live

# US
cd us && .venv/Scripts/python.exe main.py --batch
cd us && .venv/Scripts/python.exe main.py --live

# Lab Forward Trading
# KR: localhost:8080/lab → Forward Trading → 수동 EOD 실행
# US: localhost:8081/lab → Forward Trading → Run EOD
```

## Lab Live Configuration (KR)

- 9전략 독립 포트폴리오 (각 1억 원)
- 전략 그룹: rebal(4), event(3), macro(1), regime(1)
- 비용: BUY 0.115%, SELL 0.295%
- 포지션: 전략별 5~20종목
- 유니버스: ~2,700종목 (최소 종가 2,000원, 최소 거래대금 20억)
- EOD 자동 실행: 16:05 KST

## Available Skills

| Skill | 용도 |
|-------|------|
| q-debug | 3에이전트 디버깅 시스템 |
| system-health | KR/US 시스템 상태 모니터링 |
| trade-auditor | 거래 실행 품질 감사 |
| backtest-validator | 백테스트 결과 검증 |
| regime-analyst | 레짐 예측 정확도 분석 |
| portfolio-analytics | 포트폴리오 성과 측정 |
| portfolio-manager | Alpaca 포트폴리오 분석 |
| incident-commander | 장애 대응 |
| adversarial-reviewer | 적대적 코드 리뷰 |
| code-reviewer | 코드 리뷰 자동화 |
| edge-strategy-reviewer | 전략 초안 품질 검증 |
| regime-detection | 시장 레짐 식별 |
| senior-backend | 백엔드 설계/구현 |
| senior-security | 보안 분석 |
| webapp-testing | 웹앱 테스트 (Playwright) |
| skill-creator | 스킬 생성/수정 |
