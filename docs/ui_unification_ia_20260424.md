# UI Unification — Target IA & Migration Plan

**작성일**: 2026-04-24
**기준 문서**: [`ui_unification_inventory_20260424.md`](./ui_unification_inventory_20260424.md)
**범위**: Q-TRON KR/US Dashboard + Lab + Unified + Debug

---

## 0. 확정된 5개 원칙 (Jeff 2026-04-24)

| # | 원칙 | 결정 |
|---|------|------|
| 1 | 언어 | **C: 자동 전환** (Phase 1은 EN 기준 고정, KR 번역은 Phase 2 이후) |
| 2 | 페이지 구조 | **A+C 혼합** — Dashboard/Lab/Unified 유지, Debug는 `/debug` 분리, Surge는 Dashboard 섹션 흡수 |
| 3 | 카드 구성 | **C: 공통 최소 + 시장별 확장** |
| 4 | 스타일 | **C: `style.css` 공통 + `style.kr.css`/`style.us.css` override** |
| 5 | Rebal UX | **C: dual-mode** — default `preview → confirm`, option으로 direct-execute |

### 0.1 P0 선결 항목 (IA 실행 전 반드시 고정)

| P0 | 내용 | 규모 | 상태 |
|----|------|------|------|
| **P0-1** | BATCH 판정 로직 통일 — US의 ET wall-clock 버전을 KR nav.js로 역포팅 | 작음 (nav.js 한 파일) | ✅ **완료** (2026-04-24 sync) |
| **P0-2** | Debug 구조 분리 — KR `index.html` 내부 13개 debug 섹션을 `/debug` 라우트로 이전 | 큼 (템플릿 + 라우팅 + JS 참조 정리) | 대기 |
| **P0-3** | **Data Contract Freeze** — Holdings/Summary/Rebal/Regime/Badges의 source·schema·calc 고정 | 중 (문서) | ✅ **완료** — [`ui_data_contract_20260424.md`](./ui_data_contract_20260424.md) |

### 0.2 P0-3 추가 근거 (Jeff 지적 2026-04-24)

IA 방향은 완벽하지만 **"데이터 계약 먼저 고정 안 하면 UI만 맞고 내부는 계속 틀어진다"**. 핵심 리스크 3개:
1. `qc-*` 컴포넌트는 공통인데 데이터는 market별 제각각
2. `snapshot`/`batch`/`gate` 형식만 다르고 의미 동일하다고 가정 → Unified 단계에서 폭발
3. Phase 3 전에 UI 구조 변경 → 중복 구조 + 기술 부채 증가

**대응**: P0-3로 데이터/상태 contract를 freeze하고, Phase 순서를 재배열하여 **컴포넌트 추출을 구조 정렬보다 먼저** 수행.

---

## 1. 용어 사전 (Terminology Lock)

모든 라벨은 이 표 기준으로 통일. Phase 1에서 전 페이지 검색·치환.

### 1.1 포지션 & 실행

| 의미 | EN (기본) | KR | 배지 형태 | 비고 |
|------|-----------|-----|----------|------|
| 보유 | Holdings | 보유종목 | - | 섹션 타이틀 |
| 매수 (신규) | Buy / New | 매수 / 신규 | `+` 녹색 | Rebal 그룹 |
| 매도 (제외) | Sell / Exit | 매도 / 제외 | `-` 빨강 | Rebal 그룹 |
| 유지 | Keep | 유지 | `=` 회색 | Rebal 그룹 |
| 수량 | Qty | 수량 | `×N` | 라벨 x{n} 폐기 → `× N` |
| 현금 | Cash | 현금 | - | `Buying Power` 폐기 |
| 평균가 | Avg | 평균가 | - | |
| 평가손익 | Unrealized P&L | 평가손익 | 색상(±) | |
| 실현손익 | Realized P&L | 실현손익 | | |
| 총손익 | Total P&L | 총손익 | | |
| 수수료+세금 | Fees & Taxes | 수수료+세금 | | |
| 버튼: 미리보기 | Preview | 미리보기 | | |
| 버튼: 실행 | Execute | 실행 | | |
| 버튼: 매도만 | Sell Only | 매도만 | | |
| 버튼: 매수만 | Buy Only | 매수만 | | |
| 버튼: 매도+매수 | Sell + Buy | 매도+매수 | | |

### 1.2 상태 뱃지

| 의미 | EN 표기 | 색상 | 조건 |
|------|---------|------|------|
| 종가 확정 배치 완료 | `BATCH ✓` | 녹색 | P0-1 로직 (ET 16:00 이후 + last_batch_business_date == business_date + snapshot_created_at ET 날짜 일치) |
| 배치 오늘자 아님 | `BATCH STALE` | 회색 | 위 조건 미충족 |
| 장 오픈 | `OPEN` | 녹색 펄스 | |
| 장 마감 | `CLOSED` | 회색 | |
| Paper 모드 | `PAPER` | 파랑 | |
| Live 모드 | `LIVE` | 빨강 | |
| AUTO GATE: 실행 허용 | `AUTO GATE: ADVISORY (OK)` | 녹색 | |
| AUTO GATE: 권고만 | `AUTO GATE: ADVISORY` | 회색 | |
| AUTO GATE: 차단 | `AUTO GATE: BLOCKED` | 빨강 | |
| AUTO GATE: stale | `... · STALE` | 노랑 | computed_at > 5min |

### 1.3 레짐

| 의미 | EN | KR |
|------|-----|-----|
| 레짐 카드 — 오늘 | `Today's Regime` | 오늘 레짐 |
| 레짐 카드 — 내일 예측 | `Tomorrow's Forecast` | 내일 예측 |
| BULL / BEAR / NEUTRAL | 그대로 | 상승/하락/중립 (괄호 표기) |
| Market Regime | `Market Regime` | 시장 레짐 |
| Sector Regime | `Sector Regime` | 섹터 레짐 |
| Sector Breadth | `Sector Breadth` | 섹터 브레드스 |

---

## 2. 목표 IA (Target Information Architecture)

### 2.1 페이지 맵

```
/          → Dashboard  (market 토글로 KR/US 전환)
/lab       → Lab
/unified   → Unified (market-neutral 관찰 뷰)
/debug     → Debug    (P0-2로 분리 신설 — KR)
           (US는 이미 /debug 존재, 구조는 유지하되 디자인만 KR과 정렬)
```

**삭제/흡수**:
- `/surge` → Dashboard의 Extensions 섹션에 `Surge Monitor` 카드로 흡수 (KR 전용)

**네비게이션 탭** (상단 qnav-menu):
- Dashboard / Lab / Unified — 3개 유지
- Debug는 메뉴가 아닌 **우측 유틸리티 아이콘** (톱니바퀴) 또는 `?debug=1` 쿼리 진입

### 2.2 Dashboard 섹션 배치 (공통 최소 + 시장별 확장)

```
┌─────────────────────────────────────────────────────────┐
│  Navigation Bar (공통)                                   │
│  [Q-TRON] [KR|US] [상태뱃지] [BATCH] [AUTO GATE] [Nav]   │
├─────────────────────────────────────────────────────────┤
│  ▷ Core                                                  │
│     • Summary        (평가손익 / 실현 / 총 / 수수료+세금)   │
│     • Holdings       (Symbol / Qty / Avg / P&L%)         │
│     • Rebalance      (Preview · Sell Only · Buy Only ·   │
│                       Sell+Buy, dual-mode UX)            │
│     • Target Portfolio (Latest Batch, 20 symbols)        │
│                                                          │
│  ▷ Market                                                │
│     • Market Regime  (Today / Tomorrow 2 cards)          │
│     • Sector Regime  (breadth heatmap)                   │
│                                                          │
│  ▷ Extensions (conditional, market별 다름)                │
│     [KR only]                                            │
│       • DD Guard                                         │
│       • AI Advisor                                       │
│       • Surge Monitor (기존 /surge 흡수)                  │
│     [US only]                                            │
│       • Exchange Rate & Tax                              │
│                                                          │
│  ▷ Analytics (Phase 2에서 US 부분 이식)                   │
│     • Equity Curve (Gen4 LIVE + 9전략 + KOSPI/SPY 통합)  │
│     • Trade History                                      │
│     • Risk Metrics                                       │
│     • Rebalance History                                  │
│     • Alert History                                      │
│                                                          │
│  ▷ Test Order (Paper only, 디버그 아닌 실거래 준비)        │
└─────────────────────────────────────────────────────────┘
```

### 2.3 Debug 페이지 (`/debug`, KR·US 공통 구조)

현재 KR index.html 내부에 있는 13 panels를 독립 라우트로 이전. 구조는 다음 순서로 TOC 포함:

```
D-SYS    시스템 상태
D-DB     PostgreSQL DB
D-FRESH  데이터 신선도
D-MCTX   시장 컨텍스트
D-EVT    데이터 공급 이벤트
D-TRACE  요청 추적
D-WS     WebSocket & 타임스탬프
D-SYNC   REST vs COM
D-RAW    Raw JSON
D-HIST   응답시간 분포
D-LOG    로그 스트림
D-DIFF   상태 변경 추적
D-TEST   테스트 주문 (Paper)
```

Debug 진입:
- nav 우측 톱니바퀴 아이콘 (기본 hidden, `?debug=1` 또는 로컬스토리지 플래그)
- Debug 페이지에서 Dashboard 복귀 버튼 항상 상단

### 2.4 Lab 페이지

현 상태 유지 + 용어만 §1 따라 정리. IA 대상은 아님 (별도 작업).
- KR Lab: 9전략 Forward Trading 중심
- US Lab: 독자 구조 (Phase 3 이후 구조 재검토)

### 2.5 Unified 페이지

market-neutral 관찰 뷰. KR 서버 `:8080/unified` 단일 유지.
- 공통 요약만 (KR+US 합산 equity, regime, alert)
- Phase 2에서 equity_history 통합 후 구체화

---

## 3. 공통 컴포넌트 라이브러리 (Phase 3 대상)

재사용 가능한 "부품" 단위. 이름은 `qc-*` prefix (Q-TRON Component).

| 컴포넌트 | 파일 (예정) | 용도 |
|---------|------------|------|
| `qc-summary-card` | `components/summary.js` | 평가/실현/총손익/수수료 4칸 요약 |
| `qc-holdings-table` | `components/holdings.js` | Symbol/Qty/Avg/P&L 테이블 |
| `qc-rebal-panel` | `components/rebal.js` | 신규/유지/제외 3그룹 + 실행 버튼 (dual-mode) |
| `qc-regime-card` | `components/regime.js` | Today/Tomorrow 2 카드 |
| `qc-sector-regime` | `components/sector_regime.js` | breadth heatmap |
| `qc-analytics-chart` | `components/analytics_chart.js` | unified equity curve (이미 만듦, refactor) |
| `qc-test-order` | `components/test_order.js` | paper 모드 테스트 주문 |

### 3.1 컴포넌트 계약 (예: `qc-holdings-table`)

```js
qc.holdings.render(host, {
  market: 'KR' | 'US',
  columns: ['symbol', 'qty', 'avg', 'pnl_pct'],  // 기본
  data: [...],
  onRowClick: (row) => {...}
});
```

- **데이터 소스는 market별 분리** (KR: report_daily_positions, US: alpaca/state)
- **표시 로직은 공통**
- 테이블 컬럼 구성만 `market` prop에 따라 약간 다를 수 있음

---

## 4. 스타일 아키텍처

```
kr/web/static/
├── style.css            ← 공통 (컴포넌트 기본 스타일)
├── style.kr.css         ← KR override (한글 폰트, 한국 세금 색상 등)
├── themes.css           ← CSS 변수 (다크/라이트, 이미 공유 중)
└── nav.css              ← nav bar (공유, P0-1 싱크)

us/web/static/
├── style.css            ← 공통 (KR 파일을 symlink 또는 빌드 복사)
├── style.us.css         ← US override
├── themes.css           ← 공유
└── nav.css              ← 공유
```

### 4.1 공통 파일 공유 방법

- **Option A**: `shared/web/static/` 디렉터리 신설, KR/US는 Flask가 해당 경로를 serve
- **Option B**: KR 원본 유지, US는 빌드 시점에 copy (pre-commit hook 또는 수동 scripts/sync_ui.py)
- **Option C (현실적)**: Git에는 두 벌 유지하되, 변경 시 같이 업데이트. drift 방지용 CI 체크 (`diff` 실패 시 PR 차단)

→ **권장: Option A** (shared 디렉터리). 실행 시점에 Flask `static_folder` 경로만 조정.

### 4.2 CSS 변수 규칙

- 색상/간격/폰트는 `themes.css`의 CSS 변수로만 접근
- 인라인 `style="color: #xxx"` 금지 (Phase 1에서 제거)
- 컴포넌트 단위 class (`qc-holdings-row`, `qc-regime-badge` 등)

---

## 5. 마이그레이션 계획 (Phase별) — **순서 재배열 (Jeff 2026-04-24)**

**재배열 이유**: 구조(Phase 2)를 컴포넌트(Phase 3)보다 먼저 바꾸면 중복 구조 + 기술 부채 증가. 컴포넌트 일부를 먼저 추출하고, 그 후 구조 정렬.

**새 순서**: `Phase 0 (P0-1 + P0-2) → Phase 0.5 (P0-3 contract freeze) → Phase 1 (용어) → Phase 3 (컴포넌트 일부) → Phase 2 (구조) → Phase 4 (기능 이식) → Phase 5 (i18n) → Phase 6 (style)`

### Phase 0 — P0-1 + P0-2 (Structural Fixes)

**목표**: IA 실행 전 구조적 리스크 제거.

| 작업 | 파일 | 규모 | 상태 |
|------|------|------|------|
| **P0-1** BATCH 판정 US 로직 → KR nav.js 역포팅 | `kr/web/static/nav.js` | +24/-2 라인 | ✅ 완료 |
| **P0-2** KR `/debug` 라우트 신설 + 13개 debug 섹션 이전 | `kr/web/app.py`, `kr/web/templates/debug.html` 신설, `kr/web/static/dashboard.js` debug 참조 분리 | +500/-500 | 대기 |

**완료 조건**: nav.js 로직 싱크 ✓ + `/debug` 페이지 동작 + Dashboard에서 debug 섹션 흔적 0.

### Phase 0.5 — P0-3 (Data Contract Freeze)

**목표**: `ui_data_contract_20260424.md` 확정. UI 값의 source·schema·calc 고정 → 이후 모든 Phase가 이 계약 기준.

| 작업 | 산출물 | 상태 |
|------|--------|------|
| Holdings/Summary/Rebal/Regime 계약 정의 | `docs/ui_data_contract_20260424.md` | ✅ 완료 |
| 상태 뱃지 결정 함수 공식 | 위 문서 §5 | ✅ 완료 |
| 검증 규칙 3단계 (cross-market, snapshot, UI vs engine) | 위 문서 §7 | ✅ 완료 |

**완료 조건**: Jeff 문서 승인 후 **FROZEN**. 이 문서 변경은 IA 승인 체크리스트 재통과 필요.

### Phase 1 — 용어/라벨 통일

**목표**: §1 용어 사전 기준으로 **텍스트만** 일괄 치환. 로직 변경 없음. **P0-3 계약 스키마와 필드 이름 정렬**.

- KR `index.html`, `lab.html`, `unified.html` 텍스트 치환
- US `index.html`, `lab.html`, `debug.html` 텍스트 치환
- 한글/영문 혼용 정리 (US → 영문으로, KR → `data-i18n` 속성 예비)
- 버튼 라벨: `Preview / Sell Only / Buy Only / Sell + Buy / Execute` 등
- 컬럼 헤더: `Symbol / Qty / Avg / P&L%` 등 (§1 + P0-3 §1.1 schema)

**완료 조건**: §1 테이블에 있는 모든 용어가 전 페이지에서 일관 표기 + P0-3 필드명과 일치.

### Phase 3 — 공통 컴포넌트 일부 추출 (구조 정렬 전에 먼저)

**목표**: §3 `qc-*` 컴포넌트 중 **데이터 계약이 단순한 것부터 먼저** 추출. 구조 변경 없이 "DOM만 같게" 만들어 Phase 2에서 자리만 옮기면 됨.

**1차 추출 대상 (contract 단순한 것)**:
- `qc-summary-card` — P0-3 §2 계약
- `qc-holdings-table` — P0-3 §1 계약
- `qc-regime-card` — P0-3 §4 계약 (KR에만 tomorrow 있음 → 조건부)
- `qc-badges` — P0-3 §5 계약 (BATCH / AUTO GATE / Market / Mode)

**2차 (Phase 4 이후)**:
- `qc-rebal-panel` — dual-mode UX 포함
- `qc-sector-regime` — breadth heatmap

**디렉터리**:
```
kr/web/static/components/   (KR 전용)
us/web/static/components/   (US 전용, symlink 또는 copy)
shared/web/static/components/  (Phase 4에서 완전 통합 시 승격)
```

먼저 **각 시장 내부에서 컴포넌트화**하고, Phase 4에서 `shared/`로 승격.

**완료 조건**: KR/US 각각 4개 컴포넌트 호출 형태로 전환. 컴포넌트 바깥에서 직접 DOM 조작 없음.

### Phase 2 — 카드 배치·섹션 구조 정렬 (컴포넌트화 후)

**목표**: §2.2 레이아웃으로 양 시장 Dashboard 재배치. 이제 컴포넌트는 "이동 가능한 블록" 이라 구조 변경이 안전.

- **KR**: Extensions 그룹 도입 (DD Guard, Advisor, Surge 흡수), Debug 섹션 삭제 (Phase 0에서 이미 분리됨)
- **US**: Core → Market → Extensions → Analytics(빈 자리) → Test Order 순서 정렬, Exchange Rate & Tax는 Extensions에
- **Target Portfolio**를 독립 카드로 분리 (US는 이미 분리됨, KR 현재 Rebal에 통합 → 분리)
- `mode-operator` / `mode-debug` 클래스 제거 또는 재정의 (Debug 분리됐으므로 operator-only가 default)

**완료 조건**: 양쪽 Dashboard 섹션 순서 동일, 없는 섹션은 빈 placeholder 대신 완전 생략.

### Phase 4 — 기능 이식 / Analytics 통합 / API 응답 공통화

**목표**: §2.2 Analytics 섹션을 US로 이식 + P0-3 §6 공통 API 엔드포인트 통일.

- US에 Analytics 섹션 활성화 (Equity Curve, Trade History, Risk Metrics, Rebalance History, Alert History)
- US `/api/charts/equity-unified` 버전 신설 (이미 KR에 있는 로직 복제)
- **API 응답 공통화** (P0-3 §6 표): `/api/holdings`, `/api/summary`, `/api/batch/status`, `/api/regime/current` 스키마 양 서버 통일
- 각 컴포넌트 (Phase 3에서 만든) 를 `shared/web/static/components/`로 승격
- 기존 "Exchange Rate & Tax"는 Extensions에서 유지
- US Lab 구조 점검, 필요 시 9전략 모델 도입 여부 결정

**완료 조건**: 양 시장이 **기능 측면에서 최소 동급** + API 응답 스키마 일치. 시장별 고유 섹션만 `Extensions`에 남음.

### Phase 5 — 언어 자동 전환 (i18n)

**목표**: 원칙 1의 C(자동 전환) 구현. Phase 1에서 심어둔 `data-i18n` 속성 활성.

- `shared/web/static/i18n.js` + `i18n/{ko,en}.json` 리소스
- 언어 토글 (KR/US market 토글과 분리, 별도 아이콘)
- 로컬스토리지 `qtron_lang` 키
- 날짜/통화 포맷 `Intl` 사용

**완료 조건**: 모든 UI 텍스트가 KR/EN 양쪽 전환 가능, 숫자/날짜는 로케일 맞게 포맷.

### Phase 6 — style.css 공통화

**목표**: 원칙 4의 C(공통 + override) 실현. US 인라인 제거.

- `shared/web/static/style.css` 신설 (KR `style.css` 기반)
- `style.kr.css`, `style.us.css` override 2개만 유지
- US `index.html`의 `<style>` 블록 제거, 클래스명으로 마이그레이션
- 인라인 `style=` 속성 금칙, ESLint-style 가드 추가

**완료 조건**: US index.html `<style>` 블록 사라짐, 인라인 `style=` 10개 미만 (점진 감축 허용).

---

## 6.bis 검증 (Jeff 2026-04-24 추가 지시)

Phase 진행 시 각 단계에서 다음 3가지 검증을 통과해야 다음 Phase로.

### 6.bis.1 Cross-Market Consistency Test
```
test_holdings_schema_identical(kr_response, us_response)   # 필드 이름/타입 동일
test_summary_calc_same(kr, us)                              # 계산식 동일 (§P0-3 기준)
test_regime_label_enum_match(kr, us)                        # enum 값 집합 동일
test_badge_decision_identical(kr_state, us_state)           # 같은 state → 같은 badge
```

### 6.bis.2 Snapshot Consistency Test
Dashboard 상단 `snapshot_version`이 다음과 동일해야:
- BATCH 뱃지 상태 (P0-1 로직에서 사용한 snapshot)
- Target Portfolio 섹션의 snapshot_id
- Rebalance 섹션의 snapshot_version

세 곳이 다르면 테스트 실패 → 해당 Phase 미승인.

### 6.bis.3 UI vs Engine Diff Test
```
ui_equity = scrape(Dashboard.Summary.equity)
broker_equity = broker_api.get_account().portfolio_value
assert abs(ui_equity - broker_equity) < 0.01   # 0% 오차
```

허용 오차:
- equity, cash, unrealized_pnl: **0%** (센트 수준)
- positions qty: 정수 일치
- 최신 가격: stale ≤ 5분

불허:
- derived mismatch (UI와 engine이 다른 공식으로 계산)
- 시점 차이 oscillation 분 단위 이상 지속

**테스트 파일 예정 위치**: `tests/ui_contract/` (Phase 3과 동시 작성)

---

## 6. §4 IA 검증 포인트 (Jeff 확인)

### 6.1 데이터 의미 일관성
- 모든 `Equity` 수치는 **broker 기준** (Alpaca/Kiwoom 조회값 우선, state는 fallback)
- `Holdings` 카드의 `qty`는 broker qty, state qty가 아닐 경우 WARN 뱃지
- `Target Portfolio`의 `snapshot_version`은 Dashboard 상단 BATCH 뱃지와 같은 source

### 6.2 상태 뱃지 정의 단일화
§1.2 테이블이 **유일한 소스**. 구현 시 각 페이지에서 동일한 컴포넌트/함수 호출:
```js
qc.badges.batch(state)         // 'BATCH ✓' | 'BATCH STALE'
qc.badges.autoGate(state)      // 'AUTO GATE: ...'
qc.badges.market(state)        // 'OPEN' | 'CLOSED'
qc.badges.mode(state)          // 'PAPER' | 'LIVE'
```

### 6.3 BATCH / AUTO GATE 동일 조건
- BATCH: P0-1 로직 (ET wall-clock)
- AUTO GATE: `/api/auto_gate/status` 응답의 `{enabled, blocked, stale, top_violation, computed_at}` 필드만 참조

### 6.4 snapshot_id 기준
- KR: `"{trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash}"`
- US: `"{trade_date}_batch_{epoch}_{phase}"`
- → 서로 다른 형식이지만 **각자의 Dashboard는 자기 시장 snapshot만 본다**. Unified에서만 양쪽 비교.

---

## 7. 잔여 리스크 대응

### 7.1 기능 과잉 이식
- US에 KR의 Advisor/Profit/Trades 무조건 이식하지 않음 (원칙 3: 공통 최소 + 시장별 확장)
- 이식 대상은 §5 Phase 4에서 **Analytics 5개만** (Equity / Trade / Risk / Rebal / Alert History)
- 나머지는 **KR Extensions로 고정**

### 7.2 US 인라인 제거 시 스타일 깨짐
- Phase 6 시작 전 US Dashboard 스크린샷 baseline 확보
- 인라인 → 클래스 마이그레이션 1 섹션씩 진행, 각 섹션 후 스크린샷 diff
- 완전 제거 대신 점진 감축 허용 (인라인 10개 미만이면 Phase 6 완료 간주)

### 7.3 Debug 분리 시 접근성 저하
- P0-2에서 nav 우측 아이콘 신설 (톱니바퀴 + 로컬스토리지 플래그)
- Dashboard 푸터 또는 Debug 페이지 상단에 **복귀 버튼** 항상 표시
- `?debug=1` URL 파라미터 진입도 지원 (북마크용)

### 7.4 Phase 3 컴포넌트 추출 실패 리스크
- `shared/web/static/` Option A가 Flask static_folder 설정과 충돌할 가능성
- Option B/C로 fallback 가능: Git에 두 벌 유지 + `scripts/sync_ui.py` + CI drift 체크

---

## 8. 승인 요청 체크리스트

Jeff가 이 IA 문서에 확정해야 진행 가능한 항목:

- [ ] §0 5개 원칙 + 2 P0 그대로 수용
- [ ] §1 용어 사전 — 추가/수정할 용어 있는지
- [ ] §2.1 페이지 맵 — Surge 흡수, Debug 분리 OK
- [ ] §2.2 Dashboard 섹션 그룹핑 (Core/Market/Extensions/Analytics/Test) OK
- [ ] §3 컴포넌트 이름 (`qc-*`) OK 또는 다른 prefix 선호
- [ ] §4 스타일 공유 Option A/B/C 중 선택
- [ ] §5 Phase 순서 OK (0→1→2→3→4→5→6)
- [ ] §6 데이터 일관성 검증 포인트 OK
- [ ] §7 리스크 대응 방식 OK

### 8.1 Phase 실행 우선순위 제안 (2026-04-24 업데이트)

**이번 세션 내 완료**:
- ✅ Phase 0 P0-1 (BATCH 로직 싱크, nav.js) — 10분
- ✅ Phase 0.5 P0-3 (Data Contract Freeze, 문서) — 40분

**다음 작업 세션**:
- Phase 0 P0-2 (Debug 분리) — 1~2시간
- Phase 1 (용어 통일) — 2~3시간

**이후 각 Phase 세션별 1~2개씩**:
- Phase 3 (컴포넌트 일부 추출) — 세션 2~3회
- Phase 2 (구조 정렬) — 세션 1~2회
- Phase 4 (기능 이식 + API 공통화) — 세션 3~4회
- Phase 5 (i18n) — 세션 2회
- Phase 6 (style 공통화) — 세션 2회

---

## 9. 부록: 결정 로그

| 날짜 | 주제 | 결정 |
|------|------|------|
| 2026-04-24 | UI 통합 원칙 5개 | Jeff 확정 (§0) |
| 2026-04-24 | P0 선결 항목 2개 | Jeff 지시 (§0.1) |
| 2026-04-24 | IA 초안 작성 | 본 문서 |
| 2026-04-24 | **P0-3 추가 (Data Contract Freeze)** | Jeff 지시 (§0.2) → 별도 문서 `ui_data_contract_20260424.md` |
| 2026-04-24 | **Phase 순서 재배열** (2와 3 순서 교체) | Jeff 지시: 컴포넌트화 먼저, 구조 정렬 나중 — 중복 구조/기술 부채 방지 |
| 2026-04-24 | **검증 3단계 추가** (§6.bis) | Jeff 지시: cross-market / snapshot / UI vs engine |
| 2026-04-24 | P0-1 실행 완료 | `kr/web/static/nav.js` ET wall-clock 포팅 |

*다음 결정 로그 업데이트: P0-2 실행, Phase 1 착수 시 커밋 링크 병기.*
