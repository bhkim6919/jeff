# Q-TRON Gen3 보완사항 목록

---

## [2026-03-09] 배치 진행률 출력 개선

### 현상
`--kiwoom-batch` 실행 시 아래 두 가지 로그가 종목 수만큼 반복 출력되어 콘솔이 노이즈로 가득 참.

```
[INFO]    [CommRqData] 주식일봉차트조회(opt10081) screen=9001 ret=0
[WARNING] [KiwoomProvider] get_investor_trend(XXXXXX, 5) 미구현 -> 빈 dict 반환
```

약 2,500회(유니버스 빌더) + 1,292회(QScore) = **총 약 3,700줄 반복 출력**.

---

### 보완 방향

#### 1. `get_investor_trend` WARNING 억제
**파일**: `data/kiwoom_provider.py`
**위치**: `get_investor_trend()` 메서드

미구현 상태이므로 매번 WARNING을 찍을 필요 없음.
`logger.warning` → `logger.debug` 로 레벨만 낮추거나, 첫 1회만 출력 후 이후는 억제.

```python
# 현재
logger.warning(
    "[KiwoomProvider] get_investor_trend(%s, %d) 미구현 -> 빈 dict 반환",
    code, days,
)

# 개선안 A: debug 레벨로 낮춤 (콘솔 미출력)
logger.debug(
    "[KiwoomProvider] get_investor_trend(%s, %d) 미구현 -> 빈 dict 반환",
    code, days,
)

# 개선안 B: 최초 1회만 WARNING, 이후 억제
if not getattr(self, '_investor_trend_warned', False):
    logger.warning("[KiwoomProvider] get_investor_trend() 미구현 — 이후 출력 억제")
    self._investor_trend_warned = True
```

**권장**: 개선안 B (미구현 사실을 한 번은 명시)

---

#### 2. `[CommRqData] ret=0` INFO 로그 억제
**파일**: `data/kiwoom_provider.py`
**위치**: `_request_tr_with_retry()` 내부

정상 응답(ret=0)은 성공 확인용이지만 3,700회 반복 출력은 노이즈.
`logger.info` → `logger.debug` 로 레벨만 낮춤.

```python
# 현재
logger.info("[CommRqData] %s(%s) screen=%s ret=%s", rqname, trcode, screen_no, ret)

# 개선
logger.debug("[CommRqData] %s(%s) screen=%s ret=%s", rqname, trcode, screen_no, ret)
```

에러/재시도 로그(ret=-200, ret!=0)는 현행 WARNING/ERROR 레벨 유지.

---

#### 3. QScorePipeline 진행률 표시 (로딩바)
**파일**: `batch/qscore_pipeline.py`
**위치**: `run()` 메서드의 종목 루프

`QScoreEngine.score()` 내부에서 종목별 `print()` 대신, pipeline 레벨에서 진행률 바 출력.

```python
# 개선안 (표준 라이브러리 shutil만 사용, tqdm 불필요)
import shutil

def _progress_bar(current: int, total: int, width: int = 40) -> str:
    filled = int(width * current / total) if total else 0
    bar    = "█" * filled + "░" * (width - filled)
    pct    = current / total * 100 if total else 0
    return f"\r[{bar}] {pct:5.1f}%  {current:4d}/{total}개"

# score() 루프에서:
for i, code in enumerate(candidates, 1):
    ...
    # 기존 per-stock print 제거
    print(_progress_bar(i, total), end="", flush=True)

print()  # 완료 후 줄바꿈
```

출력 예시:
```
[████████████████████░░░░░░░░░░░░░░░░░░░░]  50.0%   646/1292개
```

---

#### 4. UniverseBuilder 진행률 표시
**파일**: `batch/universe_builder.py`
**위치**: `build()` 내 for 루프

```python
# 개선안
for i, code in enumerate(codes, 1):
    print(_progress_bar(i, total), end="", flush=True)
    ...
print()
```

---

#### 5. BatchRunner 단계별 요약 출력
**파일**: `batch/batch_runner.py`

각 Step 완료 시 한 줄 요약만 출력하는 형태 유지 (현행 OK).
추가로 전체 배치 경과 시간 표시 권장:

```python
import time
t0 = time.time()
...
elapsed = time.time() - t0
logger.info("━━━ Gen3 Batch 완료: %s (소요 %.0f초) ━━━", today, elapsed)
```

---

### 적용 우선순위

| 우선순위 | 항목 | 효과 |
|---------|------|------|
| ⭐⭐⭐ | get_investor_trend WARNING 억제 (개선안 B) | WARNING 1,292줄 → 1줄 |
| ⭐⭐⭐ | CommRqData ret=0 INFO → DEBUG | INFO 3,700줄 → 0줄 |
| ⭐⭐ | QScorePipeline 진행률 바 | 1,292줄 → 1줄(갱신) |
| ⭐⭐ | UniverseBuilder 진행률 바 | 2,400줄 → 1줄(갱신) |
| ⭐ | 배치 경과 시간 표시 | 편의 |

---

## [2026-03-09] 아키텍처 보완 우선순위 및 "지금 수정 시 위험성" 분석

### ⚠️ 지금(23:xx) 수정을 시작하면 발생하는 문제점

내일 장 시작(09:00)까지 약 9.5시간. signals_20260309.csv는 이미 생성 완료.

| 수정 대상 | 런타임 영향 | 위험도 | 판단 |
|----------|------------|--------|------|
| 로그 레벨(INFO→DEBUG, WARNING 억제) | 없음 (배치만 해당) | 🟢 낮음 | 지금 가능 |
| 진행률 바 추가 | 없음 (배치만 해당) | 🟢 낮음 | 지금 가능 |
| paper_trading 분리 | **직접 영향** | 🔴 높음 | 주말에 |
| OHLCV 공용 캐시 | provider 인터페이스 변경 | 🔴 높음 | 주말에 |
| 레짐 입력원 변경 | 레짐→QScore→signals 연쇄 | 🟡 중간 | 검증 후 |
| 가중치 재조정 | 내일 batch부터 반영 (런타임 무관) | 🟡 중간 | 내일 batch 전 |
| 조용한 pass 제거 | 배치 전용, 예외 처리 변경 | 🟢 낮음 | 지금 가능 |

#### 핵심 위험 포인트 (지금 건드리면 안 되는 이유)

**1. paper_trading 분리 → 즉시 폭발 위험**
- `order_executor.py` L114: `_send_to_kiwoom()` = `raise NotImplementedError()`
- `main.py`에서 `config.paper_trading = True` 강제 설정 제거하면
  → LIVE 모드에서 `paper_trading=False` → `_send_to_kiwoom()` → NotImplementedError 폭발
- 실거래 주문이 나갈 수 있는 상태가 아님. 분리 전 구현 완료가 선행 조건.

**2. OHLCV 공용 캐시 → 인터페이스 연쇄 변경**
- `_ohlcv_cache`가 현재 `QScoreEngine` 내부에 있음
- Provider 레벨로 올리려면 `DataProvider(ABC)` 수정 → Mock/Pykrx/Kiwoom 3개 구현체 전부 수정
- 수정 도중 실패 시 내일 `--pykrx` 런타임도 불가

**3. 레짐 변경 → signals.csv와 불일치**
- 이미 생성된 signals_20260309.csv는 **SIDEWAYS 가중치 기준**으로 계산됨
- 런타임이 레짐을 다시 감지하면 BULL/BEAR가 나올 수 있음
- signals의 TP/SL도 SIDEWAYS 기준 `sl_mult=2.5`로 고정되어 있음
- 레짐 변경은 반드시 배치와 런타임을 동시에 바꿔야 일관성 유지

---

### 보완 우선순위 — 항목별 분석 및 의견

#### 1순위: 실거래/시뮬레이션 완전 분리 ✅ 동의, 단 구현 순서가 관건

**현황 파악:**
- `main.py` `_run_runtime()`: `config.paper_trading = True` **강제** → LIVE도 항상 시뮬
- `OrderExecutor._send_to_kiwoom()`: `raise NotImplementedError()` → 실거래 미구현
- LIVE 명칭을 쓰면서 실제로는 항상 paper → **이름과 동작의 불일치**

**올바른 분리 순서:**
```
Step A. _send_to_kiwoom() 구현 완료 (SendOrder TR 연결)
Step B. LIVE / SIM 명칭 정리 (main.py 모드 이름 재정의)
Step C. paper_trading 강제 설정 제거
```
Step A 없이 Step C만 하면 NotImplementedError 폭발. 순서가 핵심.

**추가 의견:**
`paper_trading` bool 플래그보다 `ExecutionMode(Enum)` — LIVE / PAPER / BACKTEST 3단계로
분리하면 이후 백테스트 연결 시에도 구조가 깔끔해짐.

---

#### 2순위: OHLCV 공용 캐시 ✅ 강하게 동의, 오히려 더 높은 우선순위

**현재 TR 이중 소모 구조:**
```
UniverseBuilder → get_avg_daily_volume(days=5)  → opt10081 1회 (종목당)
QScoreEngine   → get_stock_ohlcv(days=252)       → opt10081 1회 (종목당, 별도 캐시)
```
같은 종목을 2번 조회. days=252 데이터 안에 days=5가 이미 포함되어 있음에도.

**실측 절감 효과:**
- 유니버스 통과 종목 약 1,292개 × opt10081 1회 = **약 1,292회 TR 절감**
- 소요 시간 약 1,300초(21분) 단축 가능

**권장 구현 방향:**
```python
# Provider 레벨에 캐시 추가 (days는 항상 최대 요청값으로 덮어씀)
def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
    cached = self._ohlcv_cache.get(code)
    if cached is not None and len(cached) >= days:
        return cached.tail(days).reset_index(drop=True)
    df = self._fetch_ohlcv(code, days)   # 실제 TR 호출
    self._ohlcv_cache[code] = df
    return df.tail(days).reset_index(drop=True)
```
DataProvider ABC에 `clear_cache()` 메서드 추가로 배치 재실행 시 초기화.

---

#### 3순위: 레짐 입력원 정리 ✅ 강하게 동의, 현재 가장 심각한 전략적 결함

**현재 구조의 문제:**
```
DISABLE_INDEX_TR = True → opt20006 호출 안 함 → 항상 SIDEWAYS 폴백
```
즉, **레짐 기반 전략이 사실상 작동하지 않음**.

**수치로 보는 영향:**
```
SIDEWAYS 가중치: technical=30%, demand=25%, price=30%, alpha=15%
BULL 가중치:     technical=50%, demand=25%, price=15%, alpha=10%

오늘 KOSPI가 상승장(BULL)임에도 SIDEWAYS 가중치 적용 중
→ technical 점수가 30%만 반영되어 모멘텀 강한 종목이 과소평가됨
→ price(52주 신고가 근접도)가 30%로 과다 반영됨
```

**권장 해결책 (현실적 순서):**
```
단기: kiwoom-batch 내에서도 pykrx로 지수 일봉 조회 (혼용)
      → kiwoom_provider.get_index_ohlcv() 내부에서 pykrx fallback
중기: opt20006 DISABLE_INDEX_TR = False 복원 후 야간 테스트
장기: 외부 지수 캐시 파일 방식 (장 마감 후 저장 → 런타임 로드)
```

---

#### 4순위: 미구현 점수 항목 정리 ✅ 동의, 수치가 예상보다 심각

**현재 실효 가중치 계산:**
```
demand score  = 항상 0.0 (get_investor_trend 미구현)
alpha score   = 항상 0.0 (명시적 return 0.0)

SIDEWAYS 실효: technical*0.30 + price*0.30 → 최대 Q-Score = 0.60
BULL    실효:  technical*0.50 + price*0.15 → 최대 Q-Score = 0.65
```
오늘 실측: [0000J0] Q=0.582 (T=1.00 D=0.00 P=0.94 A=0.00)
→ 계산 검증: 1.00*0.30 + 0.94*0.30 = 0.582 ✓ (이론 상한과 일치)

**정식 조치 방향:**
```
Option A (권장): demand/alpha 미구현 동안 가중치 재배분
  SIDEWAYS: technical=50%, price=50%  (demand/alpha 0%)
  → qscore 상한 1.00으로 정상화, 종목 간 변별력 회복

Option B: demand을 pykrx 수급 지표로 대체 구현 (거래량 증가율 등)
Option C: demand/alpha 가중치를 0으로 명시, 나머지를 normalize
```

---

#### 5순위: 조용한 pass 제거 ✅ 동의, 안전망 구축 관점에서 필수

**현재 위험한 pass 위치:**
```python
# universe_builder.py L51
except Exception as e:
    pass  # 개별 종목 오류 → 조용히 제외

# order_executor.py L69-70
except Exception:
    pass  # 유동성 조회 실패 시 통과 허용
```

**문제:**
- 배치 종료 후 "전체 2,400개 중 1,292개 통과"가 필터 때문인지 오류 때문인지 구분 불가
- 오류 누적 시 유니버스 품질 저하를 감지할 방법 없음

**권장 개선:**
```python
# universe_builder.py
fail_count = 0
fail_reasons: dict = {}
for code in codes:
    try:
        ...
    except Exception as e:
        fail_count += 1
        fail_reasons[type(e).__name__] = fail_reasons.get(type(e).__name__, 0) + 1

if fail_count:
    print(f"[UniverseBuilder] 오류 제외: {fail_count}개 — {fail_reasons}")
```

---

### 종합 실행 권고

| 타이밍 | 작업 |
|--------|------|
| **오늘 밤 (지금 가능)** | 로그 레벨 조정(INFO→DEBUG) + 진행률 바 추가 + 경과 시간 표시 + 조용한 pass 개선 |
| **내일 batch 전** | 가중치 재조정 (Option A: technical/price 각 50%) + 레짐 pykrx fallback 검토 |
| **이번 주말** | OHLCV 공용 캐시 (큰 리팩터링) + paper_trading 완전 분리 (순서 준수) |
| **중장기** | demand 점수 구현 + _send_to_kiwoom() 실거래 연결 |

---

## [2026-03-10] 내일 장 시간 run.bat 실행 가능 여부 분석

### 결론부터: 장 중에는 실행 불가, 장 마감 후(15:30+) run_pykrx.bat 권장

---

### 문제 1: run.bat이 비어있음

`run.bat` → **1줄 (내용 없음)**. 실행해도 아무 일도 일어나지 않음.

**현재 사용 가능한 bat 파일 목록:**

| 파일 | 명령 | 용도 | 장 중 사용 가능? |
|------|------|------|-----------------|
| `run_mock.bat` | `--mock` | 구조 확인 | ✅ 언제든 |
| `run_pykrx.bat` | `--pykrx` | 전일 종가 기반 테스트 | ⚠️ 장 마감 후만 안정 |
| `run_batch.bat` | `--batch` (pykrx) | 신호 생성 | 평일 18:00 이후 |
| `run_kiwoom_batch.bat` | `--kiwoom-batch` (32bit) | 신호 생성 (Kiwoom) | 오늘 검증 완료 ✅ |
| `run.bat` | **(비어있음)** | — | ❌ |

**보완 필요:** `run.bat`에 런타임 실행 명령 추가 또는 `run_live.bat` 신규 생성.

---

### 문제 2: run_pykrx.bat 09:00 실행 시 신호 파일 없어 진입 0건 (원인 수정)

**실제 1순위 원인 — 가격 조회 전에 이미 종료:**

```python
# strategy/entry_signal.py EntrySignal.load_today()
today    = date.today().strftime("%Y%m%d")   # → "20260310"
filepath = signals_dir / "signals_20260310.csv"

if not filepath.exists():          # 배치 미실행 → 파일 없음
    print("signals_20260310.csv 없음 → 신규 진입 없음")
    return []                      # 여기서 종료, 가격 조회 안 일어남
```

signals 파일은 **당일 18:00 이후 배치**가 생성함.
→ 09:00 실행 시 신호 없음 → Stage A/B 모두 후보 0건 → **에러 없이 정상 종료, 체결 0건**

```
[EntrySignal]  signals_20260310.csv 없음 → 신규 진입 없음
[StageA]       Early Entry 후보: 0개
[StageB]       Main Strategy 후보: 0개 (슬롯 여유: 20)
[RuntimeEngine] status: NORMAL / 체결 0건
```

**2순위 원인 — pykrx 장 중 가격 불안정 (신호가 있을 경우에 해당):**
만약 signals 파일이 이미 존재하는 상황이라도,
`_last_business_day()` → 오늘 날짜 반환 → `get_market_ohlcv_by_ticker(오늘)` → KRX 미확정 → 가격 0원 가능.
하지만 신호 파일 부재가 먼저 걸리므로 실제로는 이 경로에 도달하지 않음.

---

### 문제 3: signals_20260309.csv의 TP/SL 이상 징후

TP/SL 범위가 비정상적으로 좁음:

```
0043B0: entry=101,845  TP=101,939(+94원, +0.09%)  SL=101,798(-47원, -0.05%)
357870: entry=57,190   TP=57,242 (+52원, +0.09%)  SL=57,164 (-26원, -0.05%)
449170: entry=110,360  TP=110,442(+82원, +0.07%)  SL=110,319(-41원, -0.04%)
```

**원인 추정:** 위 종목들은 ETF 또는 채권형 상품 → ATR이 극히 낮음 (일 변동폭 수십 원)

```
sl_mult(SIDEWAYS) = 2.5
ATR = (entry - sl) / 2.5 = 47 / 2.5 = 18.8원  (종가 10만원 대비 0.02%)
```

→ **슬리피지(0.1%)만으로도 즉시 SL 터지는 구조**

**보완 방향:**
```python
# stage_manager.py _size_position() 또는 signal_generator.py 에서
# SL 폭이 진입가의 최소 N% 미만이면 필터링
MIN_SL_DISTANCE_PCT = 0.01  # 최소 1% SL 거리

if (current_price - sl) / current_price < MIN_SL_DISTANCE_PCT:
    return None  # 너무 좁은 SL → 진입 거부
```

---

### 내일(2026-03-10) 실행 가이드

| 시간대 | 추천 행동 |
|--------|----------|
| 09:00~15:30 (장 중) | 실행 없음. signals_20260309.csv 내용 확인만 |
| **15:30 이후** | **`run_pykrx.bat`** 실행 → 당일 종가 로드 → Stage A/B 검증 |
| 18:00 이후 | `run_kiwoom_batch.bat` or `run_batch.bat` → signals_20260310.csv 생성 |

**15:30 이후 run_pykrx.bat 실행 시 확인 포인트:**
1. `[PykrxProvider] 현재가 캐시 완료: N개` — 오늘 종가 로드 확인
2. `[StageB] Main Strategy 후보: N개` — 진입 대상 출력
3. `state/portfolio_state.json` — 가상 포지션 저장 확인
4. SL 폭이 너무 좁은 종목(ETF류) 진입 여부 수동 확인

---

### 추가 보완 사항 (IMPROVEMENTS에 추가)

| # | 항목 | 파일 | 우선순위 |
|---|------|------|---------|
| 6 | `run.bat` 내용 추가 (런타임 실행) | `run.bat` | 🟡 |
| 7 | `run_live.bat` 32bit Python 런타임 bat 신규 생성 | 신규 | 🟡 |
| 8 | SL 최소 거리 필터 추가 (`MIN_SL_DISTANCE_PCT`) | `stage_manager.py` 또는 `signal_generator.py` | 🟠 |
| 9 | `launch.json`에 `kiwoom-batch`, `kiwoom-live` 항목 추가 | `.claude/launch.json` | 🟢 |

---
