# UI Unification — Inventory & Asymmetry Report

**작성일**: 2026-04-24
**대상**: KR Dashboard (`:8080`) vs US Dashboard (`:8081`)
**목적**: UI/메뉴 통일 전 현 상태 대조 + 차이 식별 (1~2단계)

---

## 1. 파일 구조 비교

### 1.1 Templates

| 페이지 | KR | US | 비고 |
|--------|-----|-----|------|
| Dashboard | `kr/web/templates/index.html` (895줄) | `us/web/templates/index.html` (1059줄) | US는 인라인 CSS 다량 |
| Lab | `kr/web/templates/lab.html` (787줄) | `us/web/templates/lab.html` (946줄) | 내용 크게 다름 |
| Unified | `kr/web/templates/unified.html` (581줄) | **없음** | US는 KR로 리디렉트 |
| Debug | (index.html 내부 섹션) | `us/web/templates/debug.html` (591줄, **별도 페이지**) | 구조 상이 |
| Surge | `kr/web/templates/surge.html` (91줄) | **없음** | KR 전용 |

### 1.2 Static Assets

| 파일 | KR | US | 차이 |
|------|-----|-----|------|
| **nav.js** | 399줄 | 418줄 | US에만 ET wall-clock 배치 완료 판정 (DST 보정 포함) |
| **nav.css** | 256줄 | 257줄 | 1줄 차이 (경미) |
| **themes.css/js** | 공유 | 공유 | 동일 |
| **favicon.svg** | 공유 | 공유 | 동일 |
| **style.css** | 2671줄 | **없음** | US는 인라인 스타일 |
| **dashboard.js** | 3080줄 | **없음** | US는 index.html 내 `<script>` |
| **analytics.js** | 554줄 | **없음** | US에 Analytics 기능 부재 |
| **lab.js** | 730줄 | **없음** | US lab은 인라인 |
| **lab_live.js** | 905줄 | **없음** | KR 전용 (9전략 forward) |
| **strategy_lab.js** | 318줄 | **없음** | KR 전용 |
| **surge.js / surge.css** | 360 + 333 | **없음** | KR 전용 |

### 1.3 ROUTES 정의 (nav.js)

```
dashboard: KR → :8080/       US → :8081/
lab:       KR → :8080/lab    US → :8081/lab
unified:   KR → :8080/unified  US → :8080/unified  ← US에서 unified 클릭 시 KR 서버로 이동
```

---

## 2. 상단 네비게이션 구성 비교

### 2.1 공통 (nav.js 렌더링)

```
[Q-TRON] [KR|US] [🟢 뱃지들] [BATCH ✓] [AUTO GATE: ...]     [Dashboard] [Lab] [Unified]     [INDEX] [📤 TG] [시계]
```

### 2.2 차이점

| 요소 | KR | US | 문제 |
|------|----|----|------|
| **BATCH 뱃지 판정** | `last_batch_business_date === business_date` 단순 비교 | + ET wall-clock 16:00 이후 확인 | KR은 장중에도 "완료" 표시 가능 → 오탐 위험 |
| **장중 상태 뱃지** (PAPER / OPEN/CLOSED) | dashboard.js | index.html 인라인 | 표시 로직 이원화 |
| **INDEX 표시** (KOSPI/SPY) | 각자 다른 소스 | 각자 다른 소스 | 단위·업데이트 주기 통일 필요 |

---

## 3. Dashboard 섹션 구성

### 3.1 KR `index.html` 주요 섹션 (17개)

운영 모드 (default):
1. `#hero` — 건강 상태 요약 (health-label)
2. `#summary-cards` — 평가 요약 (평가손익, 실현손익, 총손익, 수수료+세금, 등)
3. `#rebal-section` — 리밸런스 대상 (신규/유지/제외/리밸일)
4. `#rebal-actions` — 리밸 실행 버튼 + Confirm 모달
5. `#dd-guard-section` — DD Guard 상태
6. `#regime-section` — **오늘 레짐 + 내일 예측** (2 카드)
7. `#sector-regime-section` — 섹터 레짐
8. `#advisor-section` — AI Advisor
9. `#profit-section` — 수익 분석 (mode-operator hidden)
10. `#trades-section` — 거래 기록 (mode-operator hidden)
11. `#holdings-section` — 보유종목 (mode-operator hidden)

Debug 모드 (hidden + mode-debug):
12. `#sec-debug-toc` — 디버그 네비게이션 (13 panels)
13. `#sec-db-health`, `#sec-control`, `#sec-freshness`, `#sec-traces`,
    `#sec-market-ctx`, `#sec-data-events`, `#sec-test-order`,
    `#sec-ws-sync`, `#sec-sync`, `#sec-raw-json`, `#sec-histogram`,
    `#sec-logs`, `#sec-diff`

**Analytics 카드** (mode-operator): Equity Curve, Trade History, Risk Metrics, Rebalance History, Alert History

### 3.2 US `index.html` 주요 섹션 (7개)

1. **Holdings**
2. **Target Portfolio (Latest Batch)**
3. **Market Regime**
4. **Sector Regime**
5. **Rebalance**
6. **Exchange Rate & Tax** (US 전용)
7. **Test Order (Paper)**

(추가로 Mode/Phase/Batch 상태 표시 영역이 Rebalance 상단에 존재)

### 3.3 핵심 비대칭

| 기능/카드 | KR | US | 판정 |
|-----------|-----|----|------|
| Holdings | ✅ | ✅ | 용어 일치 필요 (보유종목 vs Holdings) |
| Rebal | ✅ 상세 (신규/유지/제외 분리) | ✅ 간단 | UI 형태 다름 |
| Market Regime | ✅ 오늘+내일 2 카드 | ✅ 1 카드 | 내일 예측 US 부재 |
| Sector Regime | ✅ | ✅ | 공통 |
| DD Guard | ✅ | ❌ | **US 부재** |
| AI Advisor | ✅ | ❌ | **US 부재** |
| Profit Analysis | ✅ (mode-operator) | ❌ | **US 부재** |
| Trades | ✅ (mode-operator) | ❌ | **US 부재** |
| Target Portfolio | Rebal 섹션에 통합 | ✅ 독립 카드 | UI 위치 다름 |
| Exchange Rate & Tax | ❌ | ✅ | **US 전용** (한국 해외주식 세금) |
| Test Order | ✅ (debug 내) | ✅ 카드로 노출 | 위치 다름 |
| Analytics (Equity Curve 등) | ✅ | ❌ | **US 부재 (큰 갭)** |
| Debug | 인라인 섹션 (13 panels + TOC) | 별도 `/debug` 페이지 | 구조 차이 |
| Surge | ✅ 별도 페이지 | ❌ | KR 전용 |

---

## 4. 라벨/용어 차이 (언어 통일성)

### 4.1 같은 의미, 다른 표기

| 의미 | KR 표기 | US 표기 | 권장 |
|------|---------|---------|------|
| 보유 | 보유종목 | Holdings | (Jeff 결정) |
| 매수 | BUY / 신규 | BUY / New | 통일 |
| 매도 | SELL / 제외 | SELL / Exit | 통일 |
| 유지 | 유지 | Keep | 통일 |
| 현금 | 현금 | Cash / Buying Power | 통일 |
| 수량 | x{n} | Qty | 통일 |
| 평가손익 | 평가손익 | Unrealized P&L | 통일 |
| 실현손익 | 실현손익 | Realized P&L | 통일 |
| 리밸 | 리밸런스 | Rebalance | 통일 |
| 레짐 | 레짐 | Regime | 통일 |

### 4.2 KR 전용 라벨 (30+)
`AI Advisor`, `Equity Curve`, `Analytics`, `Alert History`, `Rebalance History`, `Risk Metrics`, `CRITICAL만`, `ERROR 이상`, `20일 평균`, `5일 평균`, `180D/1Y`, `30D/60D/90D`, `DEBUG 네비게이션`, `Pipeline 스냅샷`, `API ID`, `COM 값/시각`, `P95`, `BEAR/BULL/NEUTRAL`, `BLOCKED`, `1단계내`...

### 4.3 US 전용 라벨
`250만원 공제 후`, `Another action is in progress — please wait`, `Avg Price`, `Buy Only / Sell Only`, `Click Preview to check rebalance orders`, `DB Status`, `Failed`, `Market Regime`, `No data / No positions`, `Portfolio matches target — no rebalance orders needed`, `Positions`, `Prediction Axes`, `Rebalance executed`, `Sector Breadth`, `Symbol`, `Target Stocks`, `디바이스별 저장`, `없음`, `✓ 저장됨`

→ US도 **한글/영문 혼용** 이슈 있음 (`250만원 공제 후`, `디바이스별 저장`, `없음`, `✓ 저장됨`)

---

## 5. 기술 스택 드리프트

### 5.1 스타일 전략

- **KR**: `style.css` (2671줄) + 섹션별 class 기반
- **US**: `<style>` 블록 인라인 + inline `style=` 속성 다수

→ **컴포넌트 공유 불가능**한 구조. CSS 변수만 `themes.css`에서 공유.

### 5.2 JS 전략

- **KR**: 기능별 파일 분리 (`dashboard.js`, `analytics.js`, `lab.js`, `lab_live.js`, `strategy_lab.js`, `surge.js`) + `nav.js` 공유
- **US**: index.html 내 인라인 `<script>` + `nav.js` 공유

→ **US에 Analytics/Lab Live 기능이 아예 없음**. 정책 결정 필요: 이식 vs 의도적 생략.

### 5.3 nav.js 드리프트

- 핵심 기능 같음 (market toggle, page nav, Telegram modal, AUTO gate 뱃지)
- 차이: US 버전이 **ET wall-clock 기반 배치 완료 판정** 추가 (UI-P0-001 주석) → KR 버전도 동일 로직 필요하나 적용 안 됨

---

## 6. 주요 "그때그때 올린" 흔적 (중구난방 지점)

### 6.1 모드 시스템
- KR: `mode-operator` + `mode-debug` 클래스로 섹션 hidden 제어
- US: 모드 구분 없음 (모두 한 화면)

### 6.2 레짐 카드
- KR: **오늘 + 내일 2카드**, 각각 axis scores, EMA, market_fit 등
- US: **1카드**, 상대적으로 단순
- → 내일 예측 기능이 US에 없음 (백엔드 존재 여부 별도 확인 필요)

### 6.3 Rebal 섹션
- KR: `rebal-section` + `rebal-actions` 2분리, Confirm 모달 포함
- US: 단일 Rebalance 카드 + Preview/Sell/Buy/Sell+Buy 버튼 직접 노출
- → 실행 흐름 UX 다름. KR은 미리보기→확인→실행, US는 버튼 즉시

### 6.4 Debug
- KR: index.html 내부 toggle (`mode-debug` 표시)
- US: `/debug` 라우트로 완전 분리
- → Debug 진입 경로가 다름

### 6.5 Lab
- KR `lab.html` + 3 JS (lab.js, lab_live.js, strategy_lab.js)
- US `lab.html` 단독 (인라인)
- → Lab 콘텐츠 자체가 다를 가능성 높음 (KR은 9전략 forward, US는 다른 구조)

### 6.6 언어 혼용
- KR 페이지: 대부분 한글 + Analytics/Alert History 등 일부 영문
- US 페이지: 대부분 영문 + 한글 스팟 (세금 메모 등)
- → 언어 정책 없음

### 6.7 Surge 페이지
- KR 전용 (급등주 감시)
- US에 없음 (의도인지 누락인지 불명)

---

## 7. 공통 인프라 정리 가능 후보

아래는 현재 **중복이거나 분리 구현**되어 있어 통합 기회가 있는 항목:

| 항목 | 현 상태 | 통합 방식 |
|------|---------|----------|
| BATCH 뱃지 판정 | nav.js 내 분기 | KR이 US 로직 채택 (정확한 버전) |
| 시계 (clock) | 각자 렌더 | nav.js 공통 로직 유지 (현재 OK) |
| Market Regime 카드 | 구조 다름 | 같은 컴포넌트 (오늘+내일 2카드) |
| Sector Regime | 구조 다름 | 같은 컴포넌트 |
| Holdings 테이블 | KR 상세, US 간단 | 공통 컬럼 (Symbol, Qty, Avg, P&L%) 합의 |
| Rebal preview | KR 3-groups, US single list | 기본 표시 통일 + 펼침/접힘 |
| Target Portfolio | 위치/표현 다름 | 독립 카드로 통일 |
| Analytics (Equity Curve) | KR만 있음 | US로 이식 (방금 unified chart 만듦) |
| Exchange Rate & Tax | US 전용 | KR에도 필요한가? (Jeff 결정) |

---

## 8. 권장 정리 원칙 (Jeff 결정 요청 사항)

다음 원칙들을 먼저 확정해야 마이그레이션 설계 가능:

### 8.1 언어 정책
- A: 한글 우선 (영문 용어는 배지/고유명사만)
- B: 영문 우선
- C: **자동 전환** (메뉴에서 언어 스위치)
- D: 현재 혼용 유지

### 8.2 페이지 수
- A: Dashboard / Lab / Unified 3개 유지
- B: Debug를 Dashboard 내 toggle로 (US → KR 스타일)
- C: Debug를 완전히 분리 `/debug` (KR → US 스타일)
- D: Surge를 Dashboard 섹션으로 흡수

### 8.3 카드 구성 동기화 기준
- A: **KR 풍부 버전에 US 맞추기** (Analytics, DD Guard, Advisor, Profit, Trades 전부 이식)
- B: **US 간결 버전에 KR 맞추기** (불필요 카드 접기)
- C: **공통 최소 집합 합의** + 시장별 부가 섹션 허용

### 8.4 스타일 전략
- A: US도 `style.css` 파일화 (KR 스타일)
- B: KR도 인라인 (US 스타일, 현실성 낮음)
- C: 공통 `style.css` 파일 하나 + 시장별 override

### 8.5 Rebal 실행 UX
- A: **KR 스타일** (preview → confirm 모달 → execute)
- B: **US 스타일** (버튼 직접)
- C: 양쪽 공존 (safety 옵션 토글)

---

## 9. 다음 단계 (3~6)

Jeff 돌아오면 §8의 5개 원칙 결정 후:

- **3. 목표 IA 초안**: 확정 원칙에 맞춰 양 페이지의 최종 구조 설계 (카드 이름/순서/내용)
- **4. Jeff 검토·조정**: 초안 보고 수정 반영
- **5. 마이그레이션 계획**: Phase 1 (라벨·용어 통일) → Phase 2 (카드 위치·이름) → Phase 3 (공통 컴포넌트) → Phase 4 (Analytics/missing sections 이식)
- **6. 구현**: Phase별 독립 커밋

---

*Generated by inventory pass 2026-04-24. 파일/라인 수는 당시 `feature/gen3-v7` HEAD 기준.*
