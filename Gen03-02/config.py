"""
Gen3 Config
===========
Q-TRON Gen3 v7 시스템 전체 파라미터.

v7.5 변경 (2026-03-16):
  - Trailing Stop: +5% 수익 시 SL 자동 상향 (TRAIL_*)
  - Time-Decay SL: 보유 20일 이후 SL 점진 강화 (DECAY_*)
  - Orphan Repair: SL=0 포지션 자동 ATR SL 설정 + prev_close 백필

v7 변경 (2026-03-10):
  - Breadth 레짐 추가 (BREADTH_BEAR_THRESH, BREADTH_BULL_THRESH)
  - Runtime Adaptive Layer (RAL_*) 추가
  - 포지션 수 BULL=20 / BEAR=8 분리 (MAX_POS_BULL / MAX_POS_BEAR)
  - 진입 가중치 Early=5% / Main BULL=7% / Main BEAR=5%
  - RS composite 기반 청산 (RS_EXIT_THRESH)
  - 섹터 한도 종목수 기준 (SECTOR_CAP_TOTAL=4)
"""

from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


@dataclass
class Gen3Config:
    # ── 실행 설정 ────────────────────────────────────────────────────────
    mode: str = "LIVE"          # LIVE / MOCK / BATCH
    paper_trading: bool = True
    strategy_id: str = "GEN3_V7"
    STRATEGY_VERSION: str = "7.5"       # meta.json strategy_version 검증용

    # ── 시그널 유효성 (v7.6) ──────────────────────────────────────────────
    STRICT_SIGNAL_MODE: bool = True     # True: 오늘 파일 + meta SUCCESS만 허용
    ALLOW_PREV_SIGNAL:  bool = False    # True: 어제 파일 fallback 허용 (수동 override용)
    SIGNAL_MIN_ROWS:    int  = 3        # 최소 시그널 행 수
    SIGNAL_NAN_MAX_PCT: float = 0.20    # NaN 허용 비율 상한 (20%)

    # ── 자본 ─────────────────────────────────────────────────────────────
    initial_cash: float = 500_000_000  # 5억원

    # ── 리스크 한도 ───────────────────────────────────────────────────────
    daily_loss_limit:  float = -0.02   # 일 손실 한도 -2% (SOFT_STOP → 최약 1개 청산)
    daily_kill_limit:  float = -0.04   # 일 DD -4% → 신규 진입 완전 차단
    monthly_dd_limit:  float = -0.15   # 월 DD 한도 -15% (임시 완화, 원래 -7%)
    max_exposure:      float = 0.95    # 총 노출도 (BULL 20×7%=140% 가능, 실제 현금 한도)
    MAX_EXPOSURE_BEAR: float = 0.40   # BEAR 모드 최대 노출도 40% (v7.2)
    max_per_stock:     float = 0.10    # 종목당 최대 10% (개별 안전장치)

    # ── 포지션 수 (v7: BULL/BEAR 분리) ────────────────────────────────────
    MAX_POS_BULL:  int = 20   # BULL 최대 동시 포지션
    MAX_POS_BEAR:  int = 8    # BEAR 최대 동시 포지션
    MAX_EARLY:     int = 3    # Early 최대 동시 포지션
    MAX_MAIN:      int = 17   # Main 최대 동시 포지션

    # ── 진입 가중치 (v7) ─────────────────────────────────────────────────
    EARLY_WEIGHT:       float = 0.05   # Early 종목당 5%
    MAIN_WEIGHT_BULL:   float = 0.07   # Main BULL 종목당 7%
    MAIN_WEIGHT_BEAR:   float = 0.05   # Main BEAR 종목당 5%

    # ── Gen3 핵심 파라미터 ────────────────────────────────────────────────
    ATR_MULT_BULL: float = 2.5   # BULL Stop Loss ATR 배수 (v7.3: 4.0→2.5)
    ATR_MULT_BEAR: float = 1.0   # BEAR Stop Loss ATR 배수
    MAX_LOSS_CAP:  float = -0.08  # 종목별 최대 손실 한도 -8% (ATR SL 무관 강제 청산)
    MAX_HOLD_DAYS: int   = 60    # 최대 보유 거래일
    REGIME_MA:     int   = 200   # 레짐 판단 이동평균
    MAX_PORT_DD:   float = 0.10  # 포트 MDD 방어 임계 10%

    # ── ATR 변동성 필터 순위 상한 (%) ─────────────────────────────────────
    ATR_STAGE_A:   int = 80    # Early ATR 순위 상한
    ATR_STAGE_B:   int = 70    # Main BULL ATR 순위 상한
    ATR_BEAR_MAX:  int = 40    # Main BEAR ATR 순위 상한

    # ── Breadth 레짐 보완 (v7) ────────────────────────────────────────────
    BREADTH_BEAR_THRESH: float = 0.35  # Bear 강제 전환 하한 (MA20 상회 비율)
    BREADTH_BULL_THRESH: float = 0.55  # Bull 신뢰 상한
    REGIME_FLIP_GATE:    int   = 2     # 레짐 전환 유예일

    # ── Runtime Adaptive Layer (RAL, v7) ──────────────────────────────────
    RAL_CRASH_THRESH:    float = -0.020  # CRASH 모드 지수 수익률 임계
    RAL_SURGE_THRESH:    float = +0.015  # SURGE 모드 지수 수익률 임계
    RAL_CRASH_CLOSE_RS:  float = 0.45    # CRASH 강제청산 RS 임계
    RAL_CRASH_SL_MULT:   float = 0.60    # CRASH SL 강화 배수
    RAL_SURGE_TS_RELAX:  float = 0.50    # SURGE Trailing Stop 완화 ATR 단위

    # ── 테일 컷 (v7.2, v7.5b 정의 명확화) ──────────────────────────────────
    GAP_DOWN_EXIT:   float = -0.05  # 전일종가 대비 현재가 ≤ -5% 강제 청산 (장중 매 사이클 판정)

    # ── RS 기반 청산 (v7) ─────────────────────────────────────────────────
    RS_EXIT_THRESH:  float = 0.40   # 월초 RS 청산 임계값
    BEAR_RS_MIN:     float = 0.90   # BEAR 모드 신규 진입 최소 RS

    # ── Entry 필터 (v7) ───────────────────────────────────────────────────
    RS_ENTRY_MIN:        float = 0.80   # Main 진입 최소 RS composite
    RS_STABILITY_MIN:    float = -0.20  # RS 안정성 (rs60-rs20 하한)
    GAP_THRESH:          float = 0.08   # 갭 필터 임계 8%
    GAP_VOL_MIN:         float = 1.30   # 갭 시 필요 거래량 배수
    SECTOR_DIVERSITY_MIN:int   = 3      # Early 허용 최소 활성 섹터 수

    # ── 진입 필터 (v7.4) ────────────────────────────────────────────────
    ENTRY_GAP_LIMIT:      float = 0.07     # signal entry 대비 현재가 괴리 허용 한도 (±7%)
    ENTRY_CUTOFF_HOUR:    int   = 12       # 이 시각 이후 신규 진입 차단 (0=비활성)
    ENTRY_CUTOFF_MINUTE:  int   = 0

    # ── Trailing Stop (v7.5) ────────────────────────────────────────────
    TRAIL_ENABLED:        bool  = True
    TRAIL_ACTIVATION_PCT: float = 0.05     # +5% 이상 수익 시 트레일링 활성화
    TRAIL_ATR_MULT:       float = 2.0      # trail SL = high_watermark - 2.0 * ATR
    TRAIL_MIN_LOCK_PCT:   float = 0.02     # 최소 +2% 이익 확보

    # ── Time-Decay SL Tightening (v7.5) ─────────────────────────────────
    DECAY_ENABLED:        bool  = True
    DECAY_START_DAY:      int   = 20       # 20일차부터 SL 점진 강화
    DECAY_END_DAY:        int   = 50       # 50일차에 최대 강화
    DECAY_ATR_MULT_MIN:   float = 1.0      # 최종 ATR 배수 (2.5 → 1.0)

    # ── 체결강도 Entry Timing (v7.1) ────────────────────────────────────
    TICK_ENABLED:         bool  = True     # 체결강도 필터 활성화
    TICK_OBSERVE_MINUTES: int   = 60       # 관측 시간 (분) — 09:00~10:00
    TICK_STRONG_THRESH:   float = 120.0    # 강매수 임계값 (진입)
    TICK_WEAK_THRESH:     float = 80.0     # 강매도 임계값 (DROP)
    TICK_NEUTRAL_ACTION:  str   = "ENTER"  # 중립 80-120: "ENTER" or "SKIP"

    # ── 섹터 한도 (v7: 종목수 + 금액 비율) ────────────────────────────────
    SECTOR_CAP_TOTAL: int   = 4      # 동일 섹터 최대 4개
    SECTOR_CAP_ETC:   int   = 3      # '기타' 섹터 최대 3개 (v7.2)
    SECTOR_CAP_EARLY: int   = 1      # Early 동일 섹터 최대 1개
    SECTOR_MAX_PCT:   float = 0.20   # 섹터 최대 노출도 20%

    # ── 수수료 / 슬리피지 / 세금 ─────────────────────────────────────────
    FEE:      float = 0.00015
    SLIPPAGE: float = 0.001
    TAX:      float = 0.0018

    # ── v7.8: Reconcile ─────────────────────────────────────────────────
    RECONCILE_FORCE_CLOSE_COUNT: int = 3  # broker 미보유 연속 N회 → 포지션 강제 제거

    # ── v7.9: Stale 방어 ──────────────────────────────────────────────
    STALE_THRESHOLD_SEC: int   = 600   # equity 변동 없음 감지 임계 (초)
    STALE_FORCE_REFRESH_SEC: int = 600 # stale 시 강제 재조회 시작 (초)
    FEED_STALE_SEC: float = 300.0      # 실시간 피드 미수신 임계 (초)

    # ── 데이터 경로 ───────────────────────────────────────────────────────
    signals_dir:      str = "data/signals"
    ohlcv_dir:        str = "data/ohlcv_kospi_daily"
    index_file:       str = "data/kospi_index_daily_5y.csv"
    universe_file:    str = "data/universe_kospi.csv"
    sector_map_path:  str = "data/sector_map.json"
    cache_dir:        str = "data/cache"
    market_dir:       str = "data/market"

    # ── 유니버스 필터 ─────────────────────────────────────────────────────
    UNIV_MIN_CLOSE:   int   = 2_000          # 최소 종가 2,000원
    UNIV_MIN_AMT:     float = 2_000_000_000  # 최소 일 거래대금 20억
    # 하위 호환
    min_price:        int   = 2_000
    min_market_cap:   float = 0               # 시총 필터 해제 (거래대금으로 대체)
    min_daily_volume: float = 2_000_000_000

    # ── 하위 호환: 기존 코드에서 max_positions 참조 시 ────────────────────
    @property
    def max_positions(self) -> int:
        return self.MAX_POS_BULL

    @property
    def max_sector_exp(self) -> float:
        return 0.30   # 기존 섹터 노출 비율 (sector_cap_total 으로 대체됨)

    @property
    def ENTRY_COST(self) -> float:
        return self.FEE + self.SLIPPAGE

    @property
    def EXIT_COST(self) -> float:
        return self.FEE + self.SLIPPAGE + self.TAX

    def abs_path(self, rel: str) -> Path:
        p = Path(rel)
        if p.is_absolute():
            return p
        return BASE_DIR / p

    @classmethod
    def load(cls) -> "Gen3Config":
        return cls()
