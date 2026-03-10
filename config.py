from dataclasses import dataclass


@dataclass
class QTronConfig:
    # ── 실행/전략 정보 ─────────────────────────────────────
    mode: str = "LIVE"  # LIVE / MOCK
    paper_trading: bool = True

    # Gen2 Core v1.0 전략 ID
    # (기존 전략을 쓰고 싶으면 다른 문자열로 바꾸면 됩니다.)
    strategy_id: str = "GEN2_CORE_V1"

    # ── 공통 계좌/리스크 설정 (기존 구조 유지) ─────────────
    initial_cash: float = 100_000_000   # Gen2 기본 자본 1억원

    # 계좌 리스크 한도 (RiskGovernor에서 사용)
    daily_loss_limit:  float = -0.02   # 일 손실 한도 -2%
    monthly_dd_limit:  float = -0.07   # 월 DD 한도 -7% (안정화 후 -5% 검토)

    # 노출 제한 (추가 안전장치, 필요시 RiskGovernor 등에서 사용)
    max_exposure:    float = 0.60      # 총 노출도 60% (안정화 후 80%)
    max_per_stock:   float = 0.20      # 종목당 최대 20%
    max_positions:   int   = 4         # 기존 전략용 (Gen2는 아래 GEN2_MAX_POSITIONS 사용)
    max_sector_exp:  float = 0.30      # 섹터당 최대 30%

    # 시장 판단 임계값 (MarketAnalyzer용)
    bull_threshold:  float = 2.5       # 이상 → BULL
    bear_threshold:  float = 1.5       # 이하 → BEAR

    # ── Gen2 Core v1.0 전용 파라미터 ──────────────────────
    # 포지션/레짐/ATR 기반 슬리피지/수수료
    GEN2_MAX_POSITIONS: int   = 20     # 최대 보유 종목 20개
    GEN2_ATR_MULT_BULL: float = 4.0    # 상승장 SL ATR 배수
    GEN2_ATR_MULT_BEAR: float = 1.0    # 하락장 SL ATR 배수

    GEN2_MAX_HOLD_DAYS: int   = 60     # 최대 보유일 60일
    GEN2_RS_EXIT_THRESH: float = 0.40  # RS 하위 40% 청산 (※ StopManager에는 아직 미적용)

    GEN2_REGIME_MA: int      = 200     # 레짐 판단용 MA200
    GEN2_REGIME_CONFIRM: int = 60      # 보조 MA60
    GEN2_MAX_PORT_DD: float  = 0.10    # 포트폴리오 DD 10%

    # 수수료/슬리피지/세금
    FEE:      float = 0.00015
    SLIPPAGE: float = 0.001
    TAX:      float = 0.0018

    @property
    def GEN2_ENTRY_COST(self) -> float:
        return self.FEE + self.SLIPPAGE

    @property
    def GEN2_EXIT_COST(self) -> float:
        return self.FEE + self.SLIPPAGE + self.TAX

    # ── Early Entry 전략 파라미터 ─────────────────────────
    sector_map_path:  str = "data/sector_map.json"   # 종목→섹터 매핑 파일
    early_signal_dir: str = "data/early_signals"     # Early 신호 JSON 저장 폴더
    early_signal_db:  str = "data/early_signals.db"  # Early 신호 SQLite DB
    sector_cap:       int = 4                         # 동일 섹터 최대 보유 종목 수

    # ── 설정 로더 (main.py에서 사용) ──────────────────────
    @classmethod
    def load(cls) -> "QTronConfig":
        """
        이후 .env, JSON, YAML 등을 붙이고 싶으면 여기서 확장.
        지금은 기본값 그대로 사용.
        """
        return cls()

    @classmethod
    def from_env(cls) -> "QTronConfig":
        return cls()