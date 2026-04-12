"""
regime_classifier.py — Q-TRON 4-Stage Regime Classification
=============================================================
3축 구조 (Trend + Breadth + Momentum) → 4단계 레짐 판정.
목표: 수익률 최적화가 아니라 안정성, 드로다운 제어, 실행 일관성.

Regime:
  BULL      score >= +2   적극 진입, 20종목, trail -12%
  SIDEWAYS  -1 ~ +1       선별 진입, 10~15종목
  BEAR      score <= -2   진입 제한, 5~10종목, trail -8%
  CRISIS    override      진입 금지, 현 포지션 관리만

동기화 규칙:
  - EOD close 기준, 하루 1회 확정
  - intraday는 CRISIS override만 허용
  - backtest / live 동일 로직
  - SAFE_MODE 충돌 시 SAFE_MODE 우선

Usage:
    from strategy.regime_classifier import classify_regime, RegimeResult
    result = classify_regime(kospi_closes, adv_count, total_count)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence

import numpy as np

logger = logging.getLogger("gen4.regime")


class Regime(Enum):
    BULL = "BULL"
    SIDEWAYS = "SIDEWAYS"
    BEAR = "BEAR"
    CRISIS = "CRISIS"


@dataclass
class RegimeResult:
    regime: Regime
    score: int              # -3 ~ +3
    trend_score: int        # -1, 0, +1
    breadth_score: int      # -1, 0, +1
    momentum_score: int     # -1, 0, +1
    crisis_triggered: bool  # CRISIS override 발동 여부
    crisis_reason: str      # CRISIS 사유
    detail: str             # 판정 근거 요약

    def to_dict(self) -> dict:
        return {
            "regime": self.regime.value,
            "score": self.score,
            "trend": self.trend_score,
            "breadth": self.breadth_score,
            "momentum": self.momentum_score,
            "crisis": self.crisis_triggered,
            "crisis_reason": self.crisis_reason,
            "detail": self.detail,
        }


# ── Axis 1: Trend (KOSPI vs MA200) ──────────────────────────────────────────

def _calc_trend(closes: Sequence[float], ma_window: int = 200) -> int:
    """
    +1 if close > MA200 (상승 추세)
    -1 if close < MA200 (하락 추세)
    slope 가중: slope > 0 → 유지, slope < 0 → BEAR bias (0으로 감점)
    """
    if len(closes) < ma_window:
        return 0  # 데이터 부족 → 중립

    arr = np.array(closes, dtype=float)
    ma = np.mean(arr[-ma_window:])
    current = arr[-1]

    if current > ma:
        # 추가 필터: MA slope (최근 20일 MA의 변화)
        if len(closes) >= ma_window + 20:
            ma_prev = np.mean(arr[-(ma_window + 20):-20])
            if ma < ma_prev:
                # close > MA200이지만 MA 기울기 하락 → 약화 신호
                return 0
        return 1
    else:
        return -1


# ── Axis 2: Breadth (상승종목 비율) ──────────────────────────────────────────

def _calc_breadth(adv_count: int, total_count: int,
                  bull_threshold: float = 0.60,
                  bear_threshold: float = 0.40) -> int:
    """
    상승종목 / 전체종목 비율 기반.
    > 0.60 → +1 (강세 확산)
    0.40~0.60 → 0 (중립)
    < 0.40 → -1 (약세 확산)
    """
    if total_count <= 0:
        return 0
    ratio = adv_count / total_count
    if ratio > bull_threshold:
        return 1
    elif ratio < bear_threshold:
        return -1
    return 0


# ── Axis 3: Momentum (index 20일 수익률) ─────────────────────────────────────

def _calc_momentum(closes: Sequence[float], lookback: int = 20,
                   bull_threshold: float = 0.03,
                   bear_threshold: float = -0.03) -> int:
    """
    20일 수익률 기반.
    > +3% → +1
    -3% ~ +3% → 0
    < -3% → -1
    """
    if len(closes) < lookback + 1:
        return 0
    current = closes[-1]
    past = closes[-(lookback + 1)]
    if past <= 0:
        return 0
    ret = (current / past) - 1

    if ret > bull_threshold:
        return 1
    elif ret < bear_threshold:
        return -1
    return 0


# ── CRISIS Override ──────────────────────────────────────────────────────────

def _check_crisis(closes: Sequence[float],
                  drop_5d_threshold: float = -0.08,
                  intraday_drop: Optional[float] = None,
                  intraday_threshold: float = -0.04) -> tuple:
    """
    CRISIS 조건 (하나라도 만족 시 override):
    1. index 5일 하락률 <= -8%
    2. 일중 급락 >= -4% (intraday_drop 파라미터로 전달)

    Returns: (is_crisis, reason)
    """
    reasons = []

    # 5일 하락률
    if len(closes) >= 6:
        ret_5d = (closes[-1] / closes[-6]) - 1
        if ret_5d <= drop_5d_threshold:
            reasons.append(f"5D_DROP={ret_5d:.1%}")

    # 일중 급락 (intraday — CRISIS만 override 허용)
    if intraday_drop is not None and intraday_drop <= intraday_threshold:
        reasons.append(f"INTRADAY_DROP={intraday_drop:.1%}")

    if reasons:
        return True, " + ".join(reasons)
    return False, ""


# ── Main Classifier ──────────────────────────────────────────────────────────

def classify_regime(
    kospi_closes: Sequence[float],
    adv_count: int = 0,
    total_count: int = 0,
    intraday_drop: Optional[float] = None,
    ma_window: int = 200,
    mom_lookback: int = 20,
    breadth_bull: float = 0.60,
    breadth_bear: float = 0.40,
    mom_bull: float = 0.03,
    mom_bear: float = -0.03,
    crisis_5d: float = -0.08,
    crisis_intraday: float = -0.04,
) -> RegimeResult:
    """
    3축 스코어링 → 4단계 레짐 판정.

    Args:
        kospi_closes: KOSPI 일봉 close 시계열 (최소 200+일)
        adv_count: 오늘 상승 종목 수
        total_count: 전체 종목 수
        intraday_drop: 일중 최대 하락률 (optional, CRISIS 판정용)

    Returns:
        RegimeResult
    """
    # Axis scores
    trend = _calc_trend(kospi_closes, ma_window)
    breadth = _calc_breadth(adv_count, total_count, breadth_bull, breadth_bear)
    momentum = _calc_momentum(kospi_closes, mom_lookback, mom_bull, mom_bear)

    score = trend + breadth + momentum

    # CRISIS override
    is_crisis, crisis_reason = _check_crisis(
        kospi_closes, crisis_5d, intraday_drop, crisis_intraday)

    if is_crisis:
        regime = Regime.CRISIS
    elif score >= 2:
        regime = Regime.BULL
    elif score <= -2:
        regime = Regime.BEAR
    else:
        regime = Regime.SIDEWAYS

    detail = (f"T={trend:+d} B={breadth:+d} M={momentum:+d} → "
              f"score={score:+d} → {regime.value}")
    if is_crisis:
        detail += f" [CRISIS: {crisis_reason}]"

    return RegimeResult(
        regime=regime,
        score=score,
        trend_score=trend,
        breadth_score=breadth,
        momentum_score=momentum,
        crisis_triggered=is_crisis,
        crisis_reason=crisis_reason,
        detail=detail,
    )


# ── Batch Helper: OHLCV 기반 일괄 계산 ───────────────────────────────────────

def classify_regime_from_ohlcv(
    kospi_df,
    universe_closes_today: Optional[dict] = None,
    intraday_drop: Optional[float] = None,
    **kwargs,
) -> RegimeResult:
    """
    배치/백테스트용. DataFrame에서 직접 계산.

    Args:
        kospi_df: KOSPI index DataFrame (columns: date, close)
        universe_closes_today: {code: close_today} — breadth 계산용
                              (전일 대비 상승 여부)
        intraday_drop: 일중 급락률 (live에서만)
    """
    closes = kospi_df["close"].tolist()

    # Breadth: 전일 대비 상승 종목 수 계산
    adv = 0
    total = 0
    if universe_closes_today and len(kospi_df) >= 2:
        # universe_closes_today = {code: (prev_close, today_close)}
        for code, vals in universe_closes_today.items():
            if isinstance(vals, (tuple, list)) and len(vals) >= 2:
                prev, today = vals[0], vals[1]
                if prev > 0 and today > 0:
                    total += 1
                    if today > prev:
                        adv += 1

    return classify_regime(closes, adv, total, intraday_drop, **kwargs)


# ── Standalone Test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pandas as pd
    from pathlib import Path

    logging.basicConfig(level=logging.DEBUG)

    # Load KOSPI index
    idx_path = Path(__file__).parent.parent.parent / "backtest" / "data_full" / "index" / "KOSPI.csv"
    if not idx_path.exists():
        print(f"Index file not found: {idx_path}")
        exit(1)

    df = pd.read_csv(idx_path)
    # Normalize column names: index/date → date, Close/close → close
    col_map = {}
    for c in df.columns:
        cl = c.strip().lower()
        if cl in ("index", "date"):
            col_map[c] = "date"
        elif cl == "close":
            col_map[c] = "close"
    df = df.rename(columns=col_map)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    closes = df["close"].tolist()

    print(f"KOSPI data: {len(closes)} days, {df['date'].iloc[0].date()} ~ {df['date'].iloc[-1].date()}")
    print(f"Last close: {closes[-1]:,.0f}")
    print()

    # Current regime
    result = classify_regime(closes, adv_count=450, total_count=900)
    print(f"Regime: {result.regime.value}")
    print(f"Score:  {result.score:+d}")
    print(f"Detail: {result.detail}")
    print()

    # Historical regime distribution (rolling)
    print("=== Historical Regime Distribution (last 3 years) ===")
    regime_counts = {"BULL": 0, "SIDEWAYS": 0, "BEAR": 0, "CRISIS": 0}
    start_idx = max(250, len(closes) - 756)  # ~3 years

    for i in range(start_idx, len(closes)):
        r = classify_regime(closes[:i+1], adv_count=450, total_count=900)
        regime_counts[r.regime.value] += 1

    total_days = sum(regime_counts.values())
    for regime, count in regime_counts.items():
        pct = count / total_days * 100 if total_days > 0 else 0
        print(f"  {regime:10s}: {count:4d}일 ({pct:.1f}%)")
