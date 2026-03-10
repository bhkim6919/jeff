"""
RiskManager
===========

전략 분기:

- 기본(default): 기존 Q-Score 기반 TP/SL 전략
- GEN2_CORE_V1: Gen2 Core v1.0 (전략4)
    - 균등 분산 포지션 (MAX_POSITIONS 기준)
    - 레짐(BULL/SIDEWAYS/BEAR)에 따라 ATR SL 배수 조정
    - TP는 SL 거리의 2배로 설정 (기본 R:R = 2:1)
"""

import pandas as pd
from stage1_market.market_state import MarketState
from stage4_risk.position_sizer import PositionSizer
from config import QTronConfig


SL_ATR_MULT_LEGACY = 1.0  # 기존 전략용 SL


def _tp_mult_legacy(q_score: float, market_state: MarketState) -> float:
    """
    기존 Q-Score 전략용 TP 배수.
    """
    if q_score >= 70:
        base = 3.0
    elif q_score >= 50:
        base = 2.0
    else:
        base = 1.5

    if market_state == MarketState.SIDEWAYS:
        base *= 0.8

    return base


class RiskManager:

    def __init__(self, provider, config: QTronConfig):
        self.provider = provider
        self.config   = config
        self.sizer    = PositionSizer(config)

    # ──────────────────────────────────────────────────────────────────────
    #  공용 엔트리
    # ──────────────────────────────────────────────────────────────────────
    def apply(
        self,
        scored: list[dict],
        market_state: MarketState,
        available_cash: float,
        total_asset: float,
    ) -> list[dict]:
        """
        scored       : QScorer.score() 반환값
        market_state : 현재 시장 상태
        반환         : 진입 금액 + TP/SL 이 추가된 리스트
        """
        strategy_id = getattr(self.config, "strategy_id", "")

        if strategy_id == "GEN2_CORE_V1":
            return self._apply_gen2(scored, market_state, available_cash, total_asset)
        else:
            # 기존 Q-Score ATR 전략
            return self._apply_legacy(scored, market_state, available_cash, total_asset)

    # ──────────────────────────────────────────────────────────────────────
    #  기존 전략 로직 (Q-Score 기반 TP/SL)
    # ──────────────────────────────────────────────────────────────────────
    def _apply_legacy(
        self,
        scored: list[dict],
        market_state: MarketState,
        available_cash: float,
        total_asset: float,
    ) -> list[dict]:
        allocated = self.sizer.allocate(scored, available_cash, total_asset)

        result = []
        for item in allocated:
            code        = item["code"]
            q_score     = item["q_score"]
            entry_price = self.provider.get_current_price(code)
            atr         = self._calc_atr(code)

            if entry_price <= 0 or atr <= 0:
                continue

            tp_mult = _tp_mult_legacy(q_score, market_state)
            tp = entry_price + atr * tp_mult
            sl = entry_price - atr * SL_ATR_MULT_LEGACY

            shares = int(item["amount"] // entry_price)
            if shares <= 0:
                continue

            rr = round((tp - entry_price) / (entry_price - sl), 2)

            result.append({
                "code":        code,
                "q_score":     q_score,
                "entry_price": int(entry_price),
                "shares":      shares,
                "amount":      int(entry_price * shares),
                "tp":          int(tp),
                "sl":          int(sl),
                "atr":         round(atr, 0),
                "tp_atr_mult": tp_mult,
                "rr_ratio":    rr,
                "breakdown":   item.get("breakdown"),
                "market_state": item.get("market_state", market_state),
            })

            print(
                f"  [LEGACY {code}] Q={q_score:.1f} "
                f"진입:{int(entry_price):,} "
                f"TP:{int(tp):,}(x{tp_mult}) "
                f"SL:{int(sl):,}(x{SL_ATR_MULT_LEGACY}) "
                f"수량:{shares}주  R:R={rr}"
            )

        print(f"[Stage4] (LEGACY) 포지션 확정 → {len(result)}개 종목")
        return result

    # ──────────────────────────────────────────────────────────────────────
    #  Gen2 Core v1.0 전략 (전략4)
    # ──────────────────────────────────────────────────────────────────────
    def _apply_gen2(
        self,
        scored: list[dict],
        market_state: MarketState,
        available_cash: float,
        total_asset: float,
    ) -> list[dict]:
        """
        Gen2 Core v1.0:
          - 균등 분산: 최대 GEN2_MAX_POSITIONS 종목까지 동일 금액 배분
          - SL: 레짐에 따라 ATR 배수 적용
                BULL     → GEN2_ATR_MULT_BULL
                BEAR     → GEN2_ATR_MULT_BEAR
                SIDEWAYS → 중간값
          - TP: SL 거리의 2배 (기본 R:R=2:1)
        """
        max_positions = getattr(self.config, "GEN2_MAX_POSITIONS", 20)
        bull_mult     = getattr(self.config, "GEN2_ATR_MULT_BULL", 4.0)
        bear_mult     = getattr(self.config, "GEN2_ATR_MULT_BEAR", 1.0)

        # 레짐별 SL ATR 배수
        if market_state == MarketState.BULL:
            sl_mult = bull_mult
        elif market_state == MarketState.BEAR:
            sl_mult = bear_mult
        else:
            sl_mult = (bull_mult + bear_mult) / 2.0

        print(f"[Gen2] 전략4 적용: MAX_POS={max_positions}, SL_ATR_MULT={sl_mult:.2f}")

        # Q-Score 높은 순 정렬 후 상위 max_positions만 사용
        scored_sorted = sorted(scored, key=lambda x: x.get("q_score", 0), reverse=True)
        selected = scored_sorted[:max_positions]

        if not selected:
            print("[Gen2] 진입 후보 없음")
            return []

        # 균등 분산 — 전체 자산 기준 (보수적으로 available_cash와 total_asset 중 작은 값 사용)
        invest_base = min(available_cash, total_asset)
        if invest_base <= 0:
            print("[Gen2] 투자 가능 금액 0원 → 진입 없음")
            return []

        per_pos_amount = invest_base / max_positions
        print(f"[Gen2] 1종목당 목표 금액: {int(per_pos_amount):,}원")

        result: list[dict] = []
        for item in selected:
            code    = item["code"]
            q_score = item.get("q_score", 0.0)

            entry_price = self.provider.get_current_price(code)
            atr         = self._calc_atr(code)

            if entry_price <= 0 or atr <= 0:
                continue

            # 수량 계산
            shares = int(per_pos_amount // entry_price)
            if shares <= 0:
                continue

            amount = int(entry_price * shares)

            # SL/TP 설정
            sl_price = entry_price - atr * sl_mult
            if sl_price <= 0:
                continue

            tp_price = entry_price + (entry_price - sl_price) * 2.0  # R:R = 2:1

            rr = round((tp_price - entry_price) / (entry_price - sl_price), 2)

            result.append({
                "code":        code,
                "q_score":     q_score,
                "entry_price": int(entry_price),
                "shares":      shares,
                "amount":      amount,
                "tp":          int(tp_price),
                "sl":          int(sl_price),
                "atr":         round(atr, 0),
                "tp_atr_mult": (tp_price - entry_price) / atr if atr > 0 else 0.0,
                "rr_ratio":    rr,
                "breakdown":   item.get("breakdown"),
                "market_state": item.get("market_state", market_state),
            })

            print(
                f"  [Gen2 {code}] Q={q_score:.1f} "
                f"진입:{int(entry_price):,} "
                f"TP:{int(tp_price):,} "
                f"SL:{int(sl_price):,}(x{sl_mult:.2f}) "
                f"수량:{shares}주  R:R={rr}"
            )

        print(f"[Stage4] (Gen2) 포지션 확정 → {len(result)}개 종목")
        return result

    # ──────────────────────────────────────────────────────────────────────
    #  공통 ATR 계산
    # ──────────────────────────────────────────────────────────────────────
    def _calc_atr(self, code: str, period: int = 14) -> float:
        df = self.provider.get_stock_ohlcv(code, days=30)
        if df is None or len(df) < period + 1:
            return 0.0

        high       = df["high"]
        low        = df["low"]
        close      = df["close"]
        prev_close = close.shift(1)

        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)

        atr = tr.rolling(period).mean().iloc[-1]
        return float(atr) if not pd.isna(atr) else 0.0