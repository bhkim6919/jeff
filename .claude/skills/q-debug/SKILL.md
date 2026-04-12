---
name: q-debug
description: Q-TRON Gen4 multi-agent debugging system. 3 isolated agents (ALEX Strategy Director, TOM Engineering Lead, JUG) collaborate to analyze, classify, and resolve trading system issues with strict evidence-based validation and tiered execution gates.
user_invocable: true
command: q-debug
---

# Q-TRON Multi-Agent Debugging System

3 isolated agents with strict separation of concerns.
**Core principle: 로그는 증거이지 사실이 아니다. 위험한 것만 막고 나머지는 흐르게.**

---

## System Architecture

```
┌───��─────────────────────���───────────────────────┐
│                  USER (최종 승인)                  │
└──────────┬──────────────────┬────────────────────┘
           │ P0 승인           │
     ┌──���──▼─────┐    ┌──────▼──────┐
     │   ALEX    │    │    TOM      │
     │ Strategy  │◄──►│ Engineering │──► P2/P3 자율처리
     ��� Director  │    │ Debug Lead  │──► LOG/RETRY/GUARD 즉시실행
     └──��──┬─────┘    └���─────┬──────┘
           │                  │ P0/P1
           │   ┌───��──────┐   │
           └──►│ DECISION │��──┘
               │  JUG   ��
               └────┬─────┘
                    │
              EXECUTE / OBSERVE / NO ACTION
```

## Agent Summary

| Agent | Role | File | Core Authority |
|-------|------|------|----------------|
| **USER** | Operator (최상위) | — | 모든 판정에 개입/변경/거부/승인 가능 |
| ALEX | Strategy Director (본부장) | `agents/alex.md` | 전략 분석/제안. 코드 수정 불가 |
| TOM | Engineering & Debug Lead (그룹장) | `agents/tom.md` | 디버깅/수정. 4개 팀 총괄 |
| JUG | Final Authority + QA Lead (겸직) | `agents/jug.md` | P0 필수 개입, P1 위험 판단, 품질관리 |

### TOM 산하 팀 구성

| Team | Lead | File | 관할 |
|------|------|------|------|
| Core | Coral | `agents/coral.md` | state_manager, portfolio_manager |
| Risk | Ricky | `agents/ricky.md` | exposure_guard, safety_checks |
| Data | David | `agents/david.md` | kiwoom_provider, pykrx_provider |
| Orchestrator | Olive | `agents/olive.md` | main.py, reporter, GUI |

### Issue Routing

```
이슈 발생 → TOM 라우팅 → 관할 팀장 분석/보고
  ├── P2/P3: 팀장 자율 처리 → TOM 보고
  ├── P1 CODE_FIX: 팀장 → TOM → JUG 승인
  ├── P0: 팀장 → TOM → JUG → USER 승인
  └── Cross-module: TOM 직접 조율
```

> **behavior-changing GUARD = CODE_FIX 간주** (전 팀 공통 원칙)

## Natural Language Routing (v2)

자연어 요청 → 자동으로 적절한 에이전트에게 라우팅:

| 자연어 요청 | 담당 | 실행 |
|------------|------|------|
| "전략 검토해줘" / "이 전략 괜찮아?" | **ALEX** | 8기준 Quality Gate (PASS/REVISE/REJECT) |
| "전략 성과 분석" / "Sharpe 구해줘" | **ALEX** | Portfolio Analytics (CAGR, MDD, Sharpe, Sortino) |
| "레짐 영향 분석" / "레짐별 수익률" | **ALEX** | Regime Detection + Analysis |
| "코드 리뷰해줘" / "PR 검토" | **TOM** | Code Review + Adversarial 3-페르소나 |
| "버그 분석해" / "디버깅해줘" | **TOM** | 5단계 Debug Flow |
| "장애 대응" / "인시던트" | **TOM** | Incident Commander 플레이북 |
| "리밸 체결 감사" / "거래 검증" | **JUG** | Trade Auditor |
| "백테스트 검증" / "OOS 확인" | **JUG** | Backtest Validator |
| "포트폴리오 리스크" / "DD 상태" | **Ricky** | Portfolio Manager + DD Guard |
| "시스템 점검" / "서버 상태" | **Coral** | System Health Monitor |
| "레짐 데이터 확인" / "EMA 효과" | **David** | Regime Data Analyst |

---

## Execution Policy

| Severity | Fix Type | 실행 주체 | 승인 |
|----------|----------|-----------|------|
| **P0** (CRITICAL) | 모든 Fix Type | TOM (worktree) | **JUG + USER 명시 승��** |
| **P1** (HIGH) | CODE_FIX | TOM (worktree) | **JUG 승인** |
| **P1** (HIGH) | LOG_ENHANCEMENT / RETRY_POLICY / GUARD_ADD | TOM | 즉시 실행 가능 (보고) |
| **P2** (MEDIUM) | 모��� Fix Type | TOM | 자율 처리 (보고만) |
| **P3** (LOW) | 모든 Fix Type | TOM | 자율 처리 (보고만) |

### Fast Path (Classification 완료 후 판��)

Classification 결과가 아래 조건 **모두** 충족 시 JUG 생략:
- INFO 판정 또는 LOW risk HYPOTHESIS
- 시스템 영향 없음 (PnL/position/state 무변동)
- Fix Type이 LOG_ENHANCEMENT 또는 OPERATIONAL_CHANGE (또는 No Fix)

→ TOM이 종료 처리, 결과만 보고

---

## Interaction Rules

```
ALEX → TOM:     feasibility check, validation request
TOM → ALEX:     reject unsafe strategy, highlight system constraints
TOM → JUG:    submit debug report (P0/P1만)
ALEX → JUG:   submit strategy proposal
JUG → USER:   P0 승인 요청 전달
TOM → USER:     P2/P3 처리 결과 보고
```

---

## Global Safety Rules

1. **Broker = Truth** (RECON authoritative)
2. **SELL always allowed**, BUY may be blocked
3. **TIMEOUT ≠ failure**
4. **NEVER trust single log source**
5. **State must be backward-compatible**
6. **Engine layer is protected** (scoring.py, config 파라미터 LOCKED)
7. **No P0 execution without USER approval**

---

## JUG Scope

| Severity | Judge 개입 | 역할 |
|----------|-----------|------|
| P0 | **필수** | 전체 검증 + 승인 + USER 승인 요청 |
| P1 | **위험 판단만** | CODE_FIX 승인 여부만 판단 |
| P2/P3 | **미개입** | TOM 자율 처리 수용 |
| Fast Path | **생략** | TOM 종료 처리 |

---

## Goal

Build a system that is: **Stable, Reproducible, Risk-controlled, Operationally safe.**
NOT optimized for short-term profit.

---

## Invocation

```
/q-debug              → Full diagnostic (all phases)
/q-debug logs         → Log collection only
/q-debug report       → Report without execution
/q-debug fix BUG-{ID} → Execute specific approved fix
```
