# Q-TRON Project Rules

## Multi-Agent System

This project uses a 3-agent debugging/development system:
- **ALEX** — Strategy Director (전략 분석/제안, 코드 수정 불가)
- **TOM** — Engineering & Debug Lead (디버깅/수정, 승인 체계 준수)
- **JUG** — Final Authority (판단 계층, P0 승인 경로)

Agent specs: `.claude/skills/q-debug/agents/`

## Global Safety Rules

1. **Broker = Truth** — RECON 결과가 최종 기준. Engine 상태 < Broker 조회.
2. **SELL always allowed**, BUY may be blocked
3. **TIMEOUT ≠ failure** — opt10075 no response는 "서버 미응답"이지 "미체결 없음"이 아님
4. **NEVER trust single log source** — 반드시 복수 소스 cross-check
5. **State must be backward-compatible** — old JSON → new JSON 로드 가능 필수
6. **Engine layer is protected** — 아래 Engine Protection Rules 참조
7. **No P0 execution without USER approval** — JUG → USER 승인 경로만 허용

## Engine Protection Rules

### LOCKED (절대 수정 불가, USER 명시 지시 필요)

| File | Protected Content |
|------|-------------------|
| `kr-legacy/strategy/scoring.py` | 팩터 계산 로직 전체 |
| `kr-legacy/config.py` | 전략 파라미터: trail -12%, rebal 21일, position 20, LowVol/Mom window |

### PROTECTED (CONFIRMED + 회귀 테스트 통과 시 수정 가능)

| File | Condition |
|------|-----------|
| `kr-legacy/core/portfolio_manager.py` | 하위 호환 + 테스트 |
| `kr-legacy/core/state_manager.py` | 하위 호환 + 백업 필수 |
| `kr-legacy/risk/exposure_guard.py` | DD guard 임계값 변경 금지 |

### Order Flow Protection

- RECON 중 주문 발행 금지
- 동일 종목 BUY→SELL→BUY 3연속 감지 시 HALT
- Chejan callback 내 동기 주문 금지

### State Protection

- State 파일 삭제 금지 (백업 후 신규 생성만)
- RECON 결과 = state 최종 truth

## Execution Policy

| Severity | Fix Type | 승인 |
|----------|----------|------|
| P0 | 모든 타입 | JUG + USER 승인 |
| P1 | CODE_FIX | JUG 승인 |
| P1 | LOG/RETRY/GUARD | TOM 즉시 실행 |
| P2 | CODE_FIX | TOM 자율 (Engine Protection 비침범 + 로컬 영향만) |
| P2 | 기타 | TOM 자율 |
| P3 | 모든 타입 | TOM 자율 |

## Project Structure

```
Q-TRON/
├── kr/              # KR market (REST API, Kiwoom, :8080)
├── us/              # US market (Alpaca, :8081)
├── kr-legacy/       # Gen4 Open API (삭제 예정)
├── backtest/        # 공용 백테스트 데이터
└── .claude/         # skills, settings, memory
```

## Python Environment

```
KR: C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe (3.9 32-bit)
US: C:\Q-TRON-32_ARCHIVE\us\.venv\Scripts\python.exe (3.12 64-bit)
```

## Key Commands

```bash
# KR
cd kr && ../.venv/Scripts/python.exe main.py --batch

# US
cd us && .venv/Scripts/python.exe main.py --batch
cd us && .venv/Scripts/python.exe main.py --live
```
