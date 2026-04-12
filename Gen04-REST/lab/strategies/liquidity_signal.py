"""
liquidity_signal.py — Strategy 8: Liquidity_Signal
====================================================
Alpha: 거래량 2x surge + 양봉 (price up on high volume)
Exit: 거래량 급감 / trail -10%
Group: event
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import LiquidityExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal


class LiquiditySignalStrategy(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["liquidity_signal"]
        super().__init__(
            name="liquidity_signal",
            config=cfg,
            exit_policy=LiquidityExit(vol_decay_thresh=0.5, trail_pct=0.10),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if snapshot.day_idx < 20:
            return signals

        available_slots = self.config.max_positions - len(positions)
        if available_slots <= 0:
            return signals

        candidates = []
        for tk in snapshot.universe:
            if tk in positions:
                continue

            vol_today = float(snapshot.volume.get(tk, 0))
            if vol_today <= 0:
                continue

            # 20일 평균 거래량
            vol_hist = snapshot.volume_matrix[tk].iloc[-20:] if tk in snapshot.volume_matrix.columns else None
            if vol_hist is None or len(vol_hist) < 20:
                continue
            avg_vol = vol_hist.mean()
            if avg_vol <= 0:
                continue

            vol_ratio = vol_today / avg_vol
            if vol_ratio < 2.0:
                continue

            # 양봉 확인
            today_close = float(snapshot.close.get(tk, 0))
            today_open = float(snapshot.open.get(tk, 0))
            if today_close <= 0 or pd.isna(today_close):
                continue
            if today_open <= 0 or pd.isna(today_open):
                continue
            if today_close <= today_open:
                continue  # 음봉이면 skip

            candidates.append({"tk": tk, "vol_ratio": vol_ratio})

        if not candidates:
            return signals

        cdf = pd.DataFrame(candidates)
        top = (cdf.sort_values(["vol_ratio", "tk"], ascending=[False, True])
               .head(available_slots))

        for _, row in top.iterrows():
            signals.append(Signal(
                ticker=row["tk"],
                direction="BUY",
                reason="VOLUME_SURGE",
                priority=row["vol_ratio"],
            ))

        return signals
