# Valuation Top20 리포트 계획 메모
> 작성일: 2026-03-25
> 상태: 브레인스토밍 / 향후 개발 예정
> 기존 Gen4 Top20과 별도 생성 (기존 리포트 유지)

## 목적
- 종목별 기업가치를 역추적 평가하여 투자전략에 반영
- 배치 실행 시 Gen4 Top20과 별도로 Valuation Top20 리포트 생성

## 리포트 포함 항목

### 가격/이평선
- 현재가, MA20 / MA60 / MA120 / MA200
- MA 배열 상태 (정배열/역배열)
- 골든크로스 / 데드크로스
- 이격도

### 가치 지표
- PER, PBR, EPS, BPS
- 배당수익률
- **동일업종 평균 PER** (업종 PER 대비 괴리율)

### 수급
- 외국인 보유율 및 보유율 변화 (1개월)
- 시가총액, 시총순위

### 제외 항목
- 투자의견 (제외 확정)
- 추정PER / 컨센서스 (과거 데이터 없어 백테스트 불가)

## 스코어링 (가중치 추후 논의)
```
Value Score = w1 * PER순위
            + w2 * PBR순위
            + w3 * 배당순위
            + w4 * 업종PER괴리율 순위
            + w5 * 외국인보유변화 순위
            + w6 * 이평선배열 점수

※ 가중치 w1~w6는 추후 논의 후 확정
```

## 데이터 소스
| 데이터 | 소스 | 과거 데이터 | 비고 |
|--------|------|------------|------|
| PER/PBR/EPS/BPS/배당 | pykrx `get_market_fundamental()` | O (일별) | 무료 |
| 시가총액 | pykrx `get_market_cap()` | O | 무료 |
| 외국인 보유율 | pykrx `get_exhaustion_rates()` | O | 무료 |
| 업종 정보 | 키움 섹터맵 (Gen4KiwoomProvider) | O | 2770종목 |
| EPS (분기별) | pykrx 또는 DART API | 분기별만 | |

## 백테스트 가능 전략 후보

### 전략 1: Value + Momentum
- 1차: PER 음수 제외 + PER 상위 20% 고평가 제외
- 2차: 기존 Vol + Mom 스코어링
- 가장 현실적, pykrx만으로 충분

### 전략 2: Quality Value
- PBR < 1.5 + PER < 15 + 배당 > 1%
- 이 풀에서 모멘텀 상위 20종목

### 전략 3: Foreign Flow + Value
- 외국인 보유율 전월 대비 증가 종목
- + PER 적정 범위 (5~25)
- + 기존 Vol + Mom

## 선행 작업
1. pykrx 펀더멘털 일별 수집 스크립트 (반나절)
2. 업종별 평균 PER 계산 로직 (반나절)
3. Value 스코어링 모듈 (1일)
4. HTML 리포트 템플릿 (1일)

## 배치 통합 구조
```
01_batch.bat 실행 시:
  Step 1: OHLCV 업데이트 (기존)
  Step 2: 유니버스 구축 (기존)
  Step 3: Vol + Mom 스코어링 → Gen4 Top20 (기존)
  Step 4: Gen4 Top20 리포트 (기존)
  Step 5: Valuation 스코어링 → Value Top20 (신규)
  Step 6: Value Top20 리포트 (신규)
```

## 주의사항
- 가치 팩터는 모멘텀과 역상관일 수 있음 (Value vs Growth)
- 백테스트 시 생존자 편향 주의 (상폐 종목 데이터)
- 추정치 데이터는 과거 시점 확보 어려움 (백테스트 한계)
- Base 안정화 → Ver.02 검증 → Live 전환 이후 착수
