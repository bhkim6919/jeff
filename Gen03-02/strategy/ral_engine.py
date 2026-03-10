"""
RALEngine (Runtime Adaptive Layer)
===================================
전일 KOSPI 지수 수익률을 기반으로 당일 RAL 모드를 결정.
Look-ahead 방지: 전일 종가 기준 (daily_ret = pct_change shifted by 1).

RAL 모드:
  CRASH  — 전일 idx_ret < -2.0%  → SL 강화 + 신규 진입 차단 + RS<0.45 강제청산
  SURGE  — 전일 idx_ret > +1.5%  → Trailing Stop 완화
  NORMAL — 그 외

Usage in runtime_engine.py:
  ral = RALEngine(config)
  ral_mode = ral.determine_mode()
  if ral_mode == "CRASH":
      ral.apply_crash_sl(portfolio, provider)
      return  # 신규 진입 차단
  elif ral_mode == "SURGE":
      ral.apply_surge_sl(portfolio)
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.portfolio_manager import PortfolioManager
    from config import Gen3Config


class RALEngine:

    def __init__(self, config: "Gen3Config"):
        self.config        = config
        self.signals_dir   = config.abs_path(config.signals_dir)
        self.index_file    = config.abs_path(config.index_file)

    # ── 모드 결정 ─────────────────────────────────────────────────────────────

    def determine_mode(self) -> str:
        """
        전일 KOSPI 종가 수익률 기반 RAL 모드 결정.
        1. regime_YYYYMMDD.json 에서 idx_ret 읽기 (배치가 저장)
        2. 없으면 kospi_index_daily_5y.csv 에서 직접 계산
        반환: "CRASH" | "SURGE" | "NORMAL"
        """
        idx_ret = self._load_idx_ret()
        mode    = self._classify(idx_ret)
        print(f"[RAL] 전일 KOSPI 수익률={idx_ret:+.3%} → 모드={mode}")
        return mode

    def _classify(self, idx_ret: float) -> str:
        if idx_ret < self.config.RAL_CRASH_THRESH:
            return "CRASH"
        if idx_ret > self.config.RAL_SURGE_THRESH:
            return "SURGE"
        return "NORMAL"

    def _load_idx_ret(self) -> float:
        today_str = date.today().strftime("%Y%m%d")

        # 1순위: regime_YYYYMMDD.json (gen3_signal_builder 가 저장)
        regime_file = self.signals_dir / f"regime_{today_str}.json"
        if regime_file.exists():
            try:
                with open(regime_file, encoding="utf-8") as f:
                    data = json.load(f)
                return float(data.get("idx_ret", 0.0))
            except Exception:
                pass

        # 2순위: kospi_index_daily_5y.csv 에서 직접 계산
        if self.index_file.exists():
            try:
                import pandas as pd
                df = pd.read_csv(self.index_file, parse_dates=["date"])
                df = df.sort_values("date").tail(10)
                if len(df) >= 2:
                    prev_c = float(df["close"].iloc[-2])
                    curr_c = float(df["close"].iloc[-1])
                    if prev_c > 0:
                        return curr_c / prev_c - 1.0
            except Exception:
                pass

        print("[RAL] 전일 수익률 계산 실패 → NORMAL 가정")
        return 0.0

    # ── CRASH 모드 SL 강화 ────────────────────────────────────────────────────

    def apply_crash_sl(
        self,
        portfolio: "PortfolioManager",
        provider=None,
    ) -> int:
        """
        CRASH 모드: 모든 포지션 SL = max(현재SL, 진입가 - 0.60×ATR_BEAR×ATR)
        반환: 조정된 포지션 수
        """
        adjusted = 0
        sl_mult  = self.config.ATR_MULT_BEAR * self.config.RAL_CRASH_SL_MULT  # 1.0 × 0.60 = 0.60

        for code, pos in portfolio.positions.items():
            atr = self._get_atr(code, provider)
            if atr <= 0:
                continue
            new_sl = pos.avg_price - sl_mult * atr
            if new_sl > pos.sl:
                old_sl = pos.sl
                pos.sl = new_sl
                adjusted += 1
                print(f"  [RAL-CRASH] {code} SL {old_sl:.0f} → {new_sl:.0f} (강화)")

        print(f"[RAL] CRASH SL 강화 완료: {adjusted}개 포지션")
        return adjusted

    def get_crash_close_candidates(
        self,
        portfolio: "PortfolioManager",
        signals_today: list,
    ) -> list:
        """
        CRASH 모드 강제청산 대상: rs_composite < RAL_CRASH_CLOSE_RS 포지션.
        signals_today: EntrySignal.load_today() 반환값 (rs_composite 포함)
        """
        thresh    = self.config.RAL_CRASH_CLOSE_RS
        rs_map    = {s["code"]: float(s.get("rs_composite", s.get("qscore", 1.0)))
                     for s in signals_today}
        candidates = []
        for code in list(portfolio.positions.keys()):
            rs = rs_map.get(code, 1.0)   # signals 없으면 높다고 간주 → 청산 안 함
            if rs < thresh:
                candidates.append(code)
                print(f"  [RAL-CRASH] {code} RS={rs:.3f} < {thresh} → 강제청산 대상")
        print(f"[RAL] CRASH 강제청산 후보: {len(candidates)}개")
        return candidates

    # ── SURGE 모드 Trailing Stop 완화 ─────────────────────────────────────────

    def apply_surge_sl(
        self,
        portfolio: "PortfolioManager",
        provider=None,
    ) -> int:
        """
        SURGE 모드: SL = min(현재SL, 현재SL - 0.5×ATR) → 추세 유지
        반환: 조정된 포지션 수
        """
        adjusted = 0
        relax    = self.config.RAL_SURGE_TS_RELAX  # 0.50

        for code, pos in portfolio.positions.items():
            atr = self._get_atr(code, provider)
            if atr <= 0:
                continue
            new_sl = pos.sl - relax * atr
            if new_sl < pos.sl:
                old_sl = pos.sl
                pos.sl = new_sl
                adjusted += 1
                print(f"  [RAL-SURGE] {code} SL {old_sl:.0f} → {new_sl:.0f} (완화)")

        print(f"[RAL] SURGE Trailing Stop 완화 완료: {adjusted}개 포지션")
        return adjusted

    # ── 헬퍼 ─────────────────────────────────────────────────────────────────

    def _get_atr(self, code: str, provider) -> float:
        """종목 ATR 조회. provider가 없으면 0 반환."""
        if provider is None:
            return 0.0
        try:
            import pandas as pd
            df = provider.get_stock_ohlcv(code, days=30)
            if df is None or len(df) < 21:
                return 0.0
            high  = df["high"].astype(float).values
            low   = df["low"].astype(float).values
            close = df["close"].astype(float).values
            import numpy as np
            tr = np.maximum(
                high[1:] - low[1:],
                np.maximum(np.abs(high[1:] - close[:-1]),
                           np.abs(low[1:]  - close[:-1]))
            )
            period = 20
            if len(tr) < period:
                return float(tr.mean())
            atr = float(tr[:period].mean())
            k   = 1.0 / period
            for v in tr[period:]:
                atr = atr * (1 - k) + v * k
            return atr
        except Exception:
            return 0.0
