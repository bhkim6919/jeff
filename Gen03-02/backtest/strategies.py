# -*- coding: utf-8 -*-
"""
전략 구현체 3종
===============
1. TrendStrategy      — 기존 Gen3 RS Composite (BULL)
2. MeanReversionStrategy — RSI<30 + Bollinger 하단 (SIDEWAYS)
3. DefenseStrategy     — 포지션 축소 방어 (BEAR)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from backtest.strategy_base import Signal, Strategy


# ── 공통 지표 유틸 ──────────────────────────────────────────────────────────

def _wilder_atr(df: pd.DataFrame, period: int = 20) -> float:
    if len(df) < period + 1:
        return 0.0
    h = df["high"].values.astype(float)
    lo = df["low"].values.astype(float)
    c = df["close"].values.astype(float)
    tr = np.maximum(h[1:] - lo[1:],
                    np.maximum(np.abs(h[1:] - c[:-1]), np.abs(lo[1:] - c[:-1])))
    if len(tr) < period:
        return 0.0
    atr = float(tr[:period].mean())
    k = 1.0 / period
    for v in tr[period:]:
        atr = atr * (1 - k) + v * k
    return atr


def _rsi(closes: np.ndarray, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = np.diff(closes[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains.mean()
    avg_loss = losses.mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def _bollinger(closes: np.ndarray, period: int = 20, n_std: float = 2.0):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mid = float(window.mean())
    std = float(window.std())
    return mid, mid + n_std * std, mid - n_std * std


def _rs_returns(close_series: pd.Series, n: int) -> float:
    if len(close_series) <= n:
        return float("nan")
    prev = float(close_series.iloc[-(n + 1)])
    last = float(close_series.iloc[-1])
    return (last / prev - 1.0) if prev > 0 else float("nan")


# ═══════════════════════════════════════════════════════════════════════════
# 1. TrendStrategy — 기존 Gen3 RS Composite 재현
# ═══════════════════════════════════════════════════════════════════════════

class TrendStrategy(Strategy):
    """
    BULL 전용 추세추종.
    gen3_signal_builder.py 핵심 로직을 Strategy 인터페이스로 래핑.
    """
    name = "Trend"

    def __init__(self, *, top_n: int = 30, atr_mult: float = 2.5,
                 rs_entry_min: float = 0.80, max_hold: int = 60):
        self.top_n = top_n
        self.atr_mult = atr_mult
        self.rs_entry_min = rs_entry_min
        self.max_hold = max_hold

    def generate_signals(self, eval_date, universe, index_df, regime, sector_map):
        if regime != "BULL":
            return []

        features = []
        for ticker, df in universe.items():
            df_cut = df[df["date"] <= eval_date]
            if len(df_cut) < 130:
                continue
            close = df_cut["close"].astype(float)
            high = df_cut["high"].astype(float)
            last = float(close.iloc[-1])
            if last <= 0:
                continue

            # 유니버스 필터
            avg_amt = (close * df_cut["volume"].astype(float)).tail(20).mean()
            if last < 2000 or avg_amt < 2e9:
                continue

            rs20 = _rs_returns(close, 20)
            rs60 = _rs_returns(close, 60)
            rs120 = _rs_returns(close, 120)

            ma20 = float(close.rolling(20).mean().iloc[-1])
            above_ma20 = int(last > ma20)

            high_252 = float(high.tail(252).max())
            is_52w_high = int(last >= high_252 * 0.95)

            high_20 = float(high.tail(21).iloc[:-1].max()) if len(high) >= 21 else float("nan")
            breakout = int(last >= high_20) if not np.isnan(high_20) else 0

            atr = _wilder_atr(df_cut.tail(60))

            features.append({
                "ticker": ticker, "last_close": last,
                "rs20": rs20, "rs60": rs60, "rs120": rs120,
                "above_ma20": above_ma20, "is_52w_high": is_52w_high,
                "breakout": breakout, "atr": atr,
                "high_252": high_252,
            })

        if not features:
            return []

        fdf = pd.DataFrame(features)
        for col, out in [("rs20", "rs20_r"), ("rs60", "rs60_r"), ("rs120", "rs120_r")]:
            v = fdf[col].notna()
            fdf.loc[v, out] = fdf.loc[v, col].rank(pct=True)
            fdf.loc[~v, out] = float("nan")

        fdf["rs_composite"] = (
            fdf["rs20_r"].fillna(0) * 0.30 +
            fdf["rs60_r"].fillna(0) * 0.50 +
            fdf["rs120_r"].fillna(0) * 0.20
        )

        # signal_entry: breakout + RS >= 0.80
        fdf["signal_entry"] = (
            (fdf["breakout"] == 1) &
            (fdf["rs_composite"] >= self.rs_entry_min)
        ).astype(int)

        cands = fdf[fdf["signal_entry"] == 1].sort_values("rs_composite", ascending=False)
        cands = cands.head(self.top_n)

        signals = []
        for _, r in cands.iterrows():
            price = r["last_close"]
            atr_v = r["atr"]
            if atr_v <= 0:
                continue
            sl = price - atr_v * self.atr_mult
            tp = price + (price - sl) * 2.0
            if sl <= 0 or tp <= price:
                continue
            # MAX_LOSS_CAP clamp
            sl_floor = price * 0.92
            if sl < sl_floor:
                sl = sl_floor
                tp = price + (price - sl) * 2.0

            stage = "A" if (r["is_52w_high"] and r["rs_composite"] >= 0.80) else "B"
            signals.append(Signal(
                code=r["ticker"], entry=price, tp=tp, sl=sl,
                sector=sector_map.get(r["ticker"], "기타"),
                qscore=r["rs_composite"], stage=stage,
                strategy_name=self.name,
                extra={"max_hold": self.max_hold},
            ))
        return signals

    def exit_check(self, position, current_bar, regime):
        base = super().exit_check(position, current_bar, regime)
        if base:
            return base
        # GAP_DOWN: prev_close 대비 -5%
        prev_close = position.get("prev_close", 0)
        if prev_close > 0:
            cur = current_bar.get("close", 0)
            if cur <= prev_close * 0.95:
                return "GAP_DOWN"
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 2. MeanReversionStrategy — SIDEWAYS 전용
# ═══════════════════════════════════════════════════════════════════════════

class MeanReversionStrategy(Strategy):
    """
    SIDEWAYS 전용 평균회귀.
    진입: RSI(14) < 30 AND close < Bollinger Lower Band(20, 2σ)
    청산: TP +3~5%, SL -2%
    """
    name = "MeanReversion"

    def __init__(self, *, rsi_thresh: float = 30.0, tp_pct: float = 0.04,
                 sl_pct: float = 0.02, top_n: int = 15, max_hold: int = 20):
        self.rsi_thresh = rsi_thresh
        self.tp_pct = tp_pct
        self.sl_pct = sl_pct
        self.top_n = top_n
        self.max_hold = max_hold

    def generate_signals(self, eval_date, universe, index_df, regime, sector_map):
        if regime != "SIDEWAYS":
            return []

        candidates = []
        for ticker, df in universe.items():
            df_cut = df[df["date"] <= eval_date]
            if len(df_cut) < 30:
                continue
            close = df_cut["close"].astype(float)
            last = float(close.iloc[-1])
            if last <= 0 or last < 2000:
                continue

            avg_amt = (close * df_cut["volume"].astype(float)).tail(20).mean()
            if avg_amt < 2e9:
                continue

            closes = close.values
            rsi_val = _rsi(closes)
            mid, upper, lower = _bollinger(closes)

            if lower is None:
                continue

            if rsi_val < self.rsi_thresh and last < lower:
                # 과매도 스코어: RSI가 낮을수록, BB 하단과 거리 클수록 높은 점수
                bb_distance = (lower - last) / lower if lower > 0 else 0
                score = (self.rsi_thresh - rsi_val) + bb_distance * 100

                candidates.append({
                    "ticker": ticker, "last_close": last,
                    "rsi": rsi_val, "bb_lower": lower, "bb_mid": mid,
                    "score": score,
                })

        if not candidates:
            return []

        cdf = pd.DataFrame(candidates).sort_values("score", ascending=False)
        cdf = cdf.head(self.top_n)

        signals = []
        for _, r in cdf.iterrows():
            price = r["last_close"]
            tp = price * (1 + self.tp_pct)
            sl = price * (1 - self.sl_pct)
            signals.append(Signal(
                code=r["ticker"], entry=price, tp=tp, sl=sl,
                sector=sector_map.get(r["ticker"], "기타"),
                qscore=round(r["score"] / 100, 4), stage="MR",
                strategy_name=self.name,
                extra={
                    "rsi": round(r["rsi"], 1),
                    "bb_lower": round(r["bb_lower"], 0),
                    "max_hold": self.max_hold,
                },
            ))
        return signals

    def exit_check(self, position, current_bar, regime):
        # MR 전용: TP/SL/MAX_HOLD (짧은 보유)
        return super().exit_check(position, current_bar, regime)


# ═══════════════════════════════════════════════════════════════════════════
# 3. DefenseStrategy — BEAR 전용
# ═══════════════════════════════════════════════════════════════════════════

class DefenseStrategy(Strategy):
    """
    BEAR 방어 모드.
    - max_positions 축소 (0~5)
    - position size 30% 축소 (weight_mult)
    - 매우 보수적 진입: RS >= 0.90 + ATR < 40%ile
    """
    name = "Defense"

    def __init__(self, *, max_pos: int = 5, weight_mult: float = 0.30,
                 rs_min: float = 0.90, atr_max_pct: float = 0.40,
                 atr_mult: float = 1.0, top_n: int = 10, max_hold: int = 30):
        self.max_pos = max_pos
        self.weight_mult = weight_mult
        self.rs_min = rs_min
        self.atr_max_pct = atr_max_pct
        self.atr_mult = atr_mult
        self.top_n = top_n
        self.max_hold = max_hold

    def generate_signals(self, eval_date, universe, index_df, regime, sector_map):
        if regime != "BEAR":
            return []

        features = []
        for ticker, df in universe.items():
            df_cut = df[df["date"] <= eval_date]
            if len(df_cut) < 130:
                continue
            close = df_cut["close"].astype(float)
            last = float(close.iloc[-1])
            if last <= 0 or last < 2000:
                continue

            avg_amt = (close * df_cut["volume"].astype(float)).tail(20).mean()
            if avg_amt < 2e9:
                continue

            rs20 = _rs_returns(close, 20)
            rs60 = _rs_returns(close, 60)
            rs120 = _rs_returns(close, 120)
            atr = _wilder_atr(df_cut.tail(60))

            high_20 = float(df_cut["high"].astype(float).tail(21).iloc[:-1].max()) if len(df_cut) >= 21 else float("nan")
            breakout = int(last >= high_20) if not np.isnan(high_20) else 0

            features.append({
                "ticker": ticker, "last_close": last,
                "rs20": rs20, "rs60": rs60, "rs120": rs120,
                "atr": atr, "breakout": breakout,
            })

        if not features:
            return []

        fdf = pd.DataFrame(features)
        for col, out in [("rs20", "rs20_r"), ("rs60", "rs60_r"), ("rs120", "rs120_r")]:
            v = fdf[col].notna()
            fdf.loc[v, out] = fdf.loc[v, col].rank(pct=True)
            fdf.loc[~v, out] = float("nan")

        fdf["rs_composite"] = (
            fdf["rs20_r"].fillna(0) * 0.30 +
            fdf["rs60_r"].fillna(0) * 0.50 +
            fdf["rs120_r"].fillna(0) * 0.20
        )

        # ATR rank
        valid_atr = fdf["atr"] > 0
        fdf.loc[valid_atr, "atr_rank"] = fdf.loc[valid_atr, "atr"].rank(pct=True)
        fdf.loc[~valid_atr, "atr_rank"] = 1.0

        # 엄격 필터: RS >= 0.90 + ATR < 40%ile + breakout
        mask = (
            (fdf["rs_composite"] >= self.rs_min) &
            (fdf["atr_rank"] < self.atr_max_pct) &
            (fdf["breakout"] == 1)
        )
        cands = fdf[mask].sort_values("rs_composite", ascending=False).head(self.top_n)

        signals = []
        for _, r in cands.iterrows():
            price = r["last_close"]
            atr_v = r["atr"]
            if atr_v <= 0:
                continue
            sl = price - atr_v * self.atr_mult
            tp = price + (price - sl) * 1.5  # R:R 1.5 (보수적)
            if sl <= 0 or tp <= price:
                continue
            sl_floor = price * 0.92
            if sl < sl_floor:
                sl = sl_floor
                tp = price + (price - sl) * 1.5

            signals.append(Signal(
                code=r["ticker"], entry=price, tp=tp, sl=sl,
                sector=sector_map.get(r["ticker"], "기타"),
                qscore=r["rs_composite"], stage="DEF",
                strategy_name=self.name,
                extra={
                    "weight_mult": self.weight_mult,
                    "max_hold": self.max_hold,
                },
            ))
        return signals[:self.max_pos]

    def exit_check(self, position, current_bar, regime):
        base = super().exit_check(position, current_bar, regime)
        if base:
            return base
        # BEAR 추가: GAP_DOWN -5% 강제
        prev_close = position.get("prev_close", 0)
        if prev_close > 0:
            if current_bar.get("close", 0) <= prev_close * 0.95:
                return "GAP_DOWN"
        return None
