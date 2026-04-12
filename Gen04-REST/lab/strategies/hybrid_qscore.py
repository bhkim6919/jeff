"""
hybrid_qscore.py — Strategy 9: Hybrid_QScore
==============================================
Alpha: RS(60d) + Sector(60d) + Quality(ROE) + Trend(MA200) + LowVol 복합
Exit: Trail -12% / rebalance
Group: rebal

과최적화 위험 가장 높음 — 실험용.
"""
from __future__ import annotations
from typing import Dict, List

import numpy as np
import pandas as pd

from lab.snapshot import DailySnapshot, safe_slice
from lab.exit_policy import TrailStopExit
from lab.lab_config import STRATEGY_CONFIGS
from lab.strategies.base import BaseStrategy, Signal

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from strategy.scoring import calc_volatility


class HybridQScore(BaseStrategy):

    def __init__(self):
        cfg = STRATEGY_CONFIGS["hybrid_qscore"]
        super().__init__(
            name="hybrid_qscore",
            config=cfg,
            exit_policy=TrailStopExit(trail_pct=0.12),
        )

    def generate_signals(self, snapshot: DailySnapshot,
                         positions: Dict[str, dict]) -> List[Signal]:
        signals = []

        if not self._should_rebalance(snapshot.day_idx):
            return signals

        self._mark_rebalanced(snapshot.day_idx)

        close_hist = safe_slice(snapshot.close_matrix, snapshot.day_idx)
        if len(close_hist) < 252:
            return signals

        # Prepare fundamental data
        fund_lookup = {}
        if snapshot.fundamental is not None and not snapshot.fundamental.empty:
            fund = snapshot.fundamental.copy()
            tk_col = "ticker" if "ticker" in fund.columns else "stk_cd"
            if tk_col in fund.columns:
                for col in ["eps", "bps"]:
                    if col in fund.columns:
                        fund[col] = pd.to_numeric(fund[col], errors="coerce")
                for _, row in fund.iterrows():
                    tk = str(row.get(tk_col, ""))
                    eps = row.get("eps", 0) or 0
                    bps = row.get("bps", 0) or 0
                    roe = eps / bps if bps > 0 else 0
                    fund_lookup[tk] = roe

        # Sector returns (60d)
        sector_stocks: Dict[str, List[float]] = {}
        stock_sector: Dict[str, str] = {}
        for tk in snapshot.universe:
            info = snapshot.sector_map.get(tk, {})
            sector = info.get("sector", "") if isinstance(info, dict) else ""
            stock_sector[tk] = sector
            if not sector:
                continue
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 60:
                continue
            ret = series.iloc[-1] / series.iloc[-60] - 1
            if not np.isnan(ret):
                if sector not in sector_stocks:
                    sector_stocks[sector] = []
                sector_stocks[sector].append(ret)

        sector_returns = {s: np.mean(r) for s, r in sector_stocks.items() if r}

        # Score each stock
        scored = []
        for tk in snapshot.universe:
            if tk not in close_hist.columns:
                continue
            series = close_hist[tk].dropna()
            if len(series) < 252:
                continue

            today_close = float(snapshot.close.get(tk, 0))
            if today_close <= 0 or pd.isna(today_close):
                continue

            # RS: 60-day return
            rs = series.iloc[-1] / series.iloc[-60] - 1 if len(series) >= 60 else 0

            # Sector score
            sec = stock_sector.get(tk, "")
            sec_ret = sector_returns.get(sec, 0)

            # Quality: ROE from fundamental
            roe = fund_lookup.get(tk, 0)

            # Trend: above MA200 = 1, below = 0
            ma200 = series.iloc[-200:].mean() if len(series) >= 200 else series.mean()
            trend = 1.0 if today_close > ma200 else 0.0

            # LowVol: inverse volatility
            vol = calc_volatility(series, 252)
            inv_vol = 1.0 / vol if vol > 0 and not np.isnan(vol) else 0

            scored.append({
                "tk": tk, "rs": rs, "sec_ret": sec_ret,
                "roe": roe, "trend": trend, "inv_vol": inv_vol,
            })

        if not scored:
            return signals

        sdf = pd.DataFrame(scored)

        # Rank percentile for each component
        has_quality = any(s["roe"] != 0 for s in scored)

        sdf["rs_rank"] = sdf["rs"].rank(pct=True)
        sdf["sec_rank"] = sdf["sec_ret"].rank(pct=True)
        sdf["vol_rank"] = sdf["inv_vol"].rank(pct=True)

        if has_quality:
            sdf["roe_rank"] = sdf["roe"].rank(pct=True)
            # Weights: RS 0.25, Sector 0.20, Quality 0.20, Trend 0.15, LowVol 0.20
            sdf["q_score"] = (
                sdf["rs_rank"] * 0.25
                + sdf["sec_rank"] * 0.20
                + sdf["roe_rank"] * 0.20
                + sdf["trend"] * 0.15
                + sdf["vol_rank"] * 0.20
            )
        else:
            # No quality data — redistribute: RS 0.30, Sector 0.25, Trend 0.15, LowVol 0.30
            sdf["q_score"] = (
                sdf["rs_rank"] * 0.30
                + sdf["sec_rank"] * 0.25
                + sdf["trend"] * 0.15
                + sdf["vol_rank"] * 0.30
            )

        top = (sdf.sort_values(["q_score", "tk"], ascending=[False, True])
               .head(self.config.max_positions))
        target_codes = set(top["tk"].tolist())

        for tk in positions:
            if tk not in target_codes:
                signals.append(Signal(
                    ticker=tk, direction="SELL", reason="QSCORE_REBAL"))

        for _, row in top.iterrows():
            tk = row["tk"]
            if tk not in positions:
                signals.append(Signal(
                    ticker=tk, direction="BUY",
                    reason="QSCORE_TOP",
                    priority=row["q_score"],
                ))

        return signals
