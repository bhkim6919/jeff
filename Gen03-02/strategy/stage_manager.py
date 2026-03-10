"""
StageManager (v7)
=================
v7 진입 로직 적용.

변경 내역 (v7):
  - BULL/BEAR 최대 포지션 수 분리 (MAX_POS_BULL=20, MAX_POS_BEAR=8)
  - 진입 가중치: Early=5%, Main BULL=7%, Main BEAR=5%
  - ATR 변동성 필터: Stage A < 80%ile, Main BULL < 70%ile, Main BEAR < 40%ile
  - 섹터 한도: SECTOR_CAP_TOTAL=4, SECTOR_CAP_EARLY=1
  - BEAR 모드 최소 RS: BEAR_RS_MIN=0.90
  - Early Entry: 52주 신고가 + 활성 섹터 수 >= 3 조건
  - 갭 필터: gap > 8% AND volume < 1.3배 → 스킵 (signals에서 이미 필터됨)
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional

import pandas as pd

from strategy.regime_detector import MarketRegime
from core.portfolio_manager import PortfolioManager
from config import Gen3Config


class StageManager:

    def __init__(self, provider, portfolio: PortfolioManager, config: Gen3Config):
        self.provider  = provider
        self.portfolio = portfolio
        self.config    = config

    # ── Stage A: Early Entry ─────────────────────────────────────────────────

    def run_stage_a(self, signals: List[Dict[str, Any]], regime: MarketRegime) -> List[Dict[str, Any]]:
        """
        BULL 레짐 한정 Early Entry (v7):
          - stage=A 플래그 필터 (is_52w_high=1 AND rs_composite>=0.80)
          - MAX_EARLY=3 슬롯 제한
          - 활성 섹터 수 >= SECTOR_DIVERSITY_MIN
          - 동일 섹터 Early 최대 SECTOR_CAP_EARLY=1개
          - ATR 순위 < ATR_STAGE_A=80%ile (signals의 atr_rank 기준)
        """
        if regime != MarketRegime.BULL:
            return []

        # stage=A 신호 필터
        early_sigs = [s for s in signals if s.get("stage") == "A"]
        if not early_sigs:
            print("[StageA] stage=A 신호 없음 → Early Entry 스킵")
            return []

        # 현재 Early 포지션 수 확인
        current_early = sum(
            1 for code in self.portfolio.positions
            if self.portfolio.positions[code].__dict__.get("stage") == "A"
        )
        max_new_early = self.config.MAX_EARLY - current_early
        if max_new_early <= 0:
            print(f"[StageA] Early 슬롯 가득참 ({self.config.MAX_EARLY}개)")
            return []

        # 활성 섹터 수 체크 (signals 내 고유 섹터)
        active_sectors = len(set(s.get("sector", "기타") for s in early_sigs))
        if active_sectors < self.config.SECTOR_DIVERSITY_MIN:
            print(f"[StageA] 활성 섹터 {active_sectors}개 < {self.config.SECTOR_DIVERSITY_MIN} → Early 차단")
            return []

        # 현재 Early 섹터 카운트
        early_sector_cnt: Counter = Counter()
        for code, pos in self.portfolio.positions.items():
            if getattr(pos, "stage", None) == "A":
                sec = getattr(pos, "sector", "기타")
                early_sector_cnt[sec] += 1

        result = []
        for sig in early_sigs:
            if len(result) >= max_new_early:
                break
            code   = sig["code"]
            sector = sig.get("sector", "기타")

            if self.portfolio.has_position(code):
                continue

            # 섹터 Early 한도 (최대 1개)
            if early_sector_cnt.get(sector, 0) >= self.config.SECTOR_CAP_EARLY:
                print(f"  [StageA] {code} 섹터 Early 한도 초과 ({sector})")
                continue

            # ATR 순위 필터 (신호 파일에 atr_rank 없으면 통과)
            atr_rank_pct = float(sig.get("atr_rank", 0.0)) if "atr_rank" in sig else 0.0
            if atr_rank_pct >= self.config.ATR_STAGE_A / 100.0:
                print(f"  [StageA] {code} ATR 순위 너무 높음 ({atr_rank_pct:.0%})")
                continue

            current = self.provider.get_current_price(code)
            entry   = sig.get("entry", current)

            # 갭업 방지: 8% 초과 갭 (signal_builder에서 이미 필터하지만 런타임 재확인)
            if entry > 0 and current > entry * (1 + self.config.GAP_THRESH):
                print(f"  [StageA] {code} 갭 {(current/entry-1):.1%} > {self.config.GAP_THRESH:.0%} → 패스")
                continue

            sized = self._size_position(sig, current, regime, entry_type="EARLY")
            if sized:
                sized["stage"] = "A"
                result.append(sized)
                early_sector_cnt[sector] = early_sector_cnt.get(sector, 0) + 1

        print(f"[StageA] Early Entry 후보: {len(result)}개")
        return result

    # ── Stage B: Main Strategy ───────────────────────────────────────────────

    def run_stage_b(self, signals: List[Dict[str, Any]], regime: MarketRegime,
                    exclude_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        v7 Main Entry:
          - BULL: ATR 순위 < 70%ile, 최소 RS 없음 (signal_entry=1 이미 필터)
          - BEAR: ATR 순위 < 40%ile, rs_composite >= BEAR_RS_MIN=0.90
          - 섹터 SECTOR_CAP_TOTAL=4 한도
          - BULL=20, BEAR=8 최대 포지션
        """
        exclude      = set(exclude_codes or [])
        is_bull      = (regime == MarketRegime.BULL)
        max_pos      = self.config.MAX_POS_BULL if is_bull else self.config.MAX_POS_BEAR
        atr_max_pct  = (self.config.ATR_STAGE_B if is_bull else self.config.ATR_BEAR_MAX) / 100.0
        bear_rs_min  = self.config.BEAR_RS_MIN  # 0.90 (BEAR 모드만 적용)

        current_pos  = len(self.portfolio.positions)
        max_new      = max_pos - current_pos
        if max_new <= 0:
            print(f"[StageB] 포지션 슬롯 가득참 ({max_pos}개)")
            return []

        # 섹터별 현재 보유 수 계산
        sector_cnt: Counter = Counter()
        for pos in self.portfolio.positions.values():
            sector_cnt[getattr(pos, "sector", "기타")] += 1

        result = []
        skipped_atr    = 0
        skipped_rs     = 0
        skipped_sector = 0

        for sig in signals:
            if len(result) >= max_new:
                break
            code   = sig["code"]
            sector = sig.get("sector", "기타")
            rs_c   = float(sig.get("rs_composite", sig.get("qscore", 0.0)))

            if code in exclude or self.portfolio.has_position(code):
                continue

            # BEAR 모드 최소 RS 필터
            if not is_bull and rs_c < bear_rs_min:
                skipped_rs += 1
                continue

            # ATR 순위 필터
            atr_rank_pct = float(sig.get("atr_rank", 0.0)) if "atr_rank" in sig else 0.0
            if atr_rank_pct > 0 and atr_rank_pct >= atr_max_pct:
                skipped_atr += 1
                continue

            # 섹터 한도 (종목 수 기준)
            if sector_cnt.get(sector, 0) >= self.config.SECTOR_CAP_TOTAL:
                skipped_sector += 1
                continue

            current = self.provider.get_current_price(code)
            sized   = self._size_position(sig, current, regime, entry_type="MAIN")
            if sized:
                sized["stage"] = "B"
                result.append(sized)
                sector_cnt[sector] = sector_cnt.get(sector, 0) + 1

        if skipped_atr:
            print(f"[StageB] ATR 한도 제외: {skipped_atr}개")
        if skipped_rs:
            print(f"[StageB] BEAR RS 미달 제외: {skipped_rs}개")
        if skipped_sector:
            print(f"[StageB] 섹터 한도 제외: {skipped_sector}개")
        print(f"[StageB] Main Entry 후보: {len(result)}개 (슬롯 여유: {max_new})")
        return result

    # ── 포지션 사이징 ────────────────────────────────────────────────────────

    def _size_position(
        self,
        sig: Dict[str, Any],
        current_price: float,
        regime: MarketRegime,
        entry_type: str = "MAIN",
    ) -> Optional[Dict[str, Any]]:
        """
        v7 포지션 사이징:
          Early:     총자산 × EARLY_WEIGHT (5%)
          Main BULL: 총자산 × MAIN_WEIGHT_BULL (7%)
          Main BEAR: 총자산 × MAIN_WEIGHT_BEAR (5%)
        """
        code   = sig["code"]
        sector = sig.get("sector", "기타")
        rs_c   = float(sig.get("rs_composite", sig.get("qscore", 0.0)))

        if current_price <= 0:
            return None

        # 비중 결정
        equity = self.portfolio.get_current_equity()
        if equity <= 0:
            return None

        if entry_type == "EARLY":
            weight = self.config.EARLY_WEIGHT
        elif regime == MarketRegime.BULL:
            weight = self.config.MAIN_WEIGHT_BULL
        else:
            weight = self.config.MAIN_WEIGHT_BEAR

        per_pos_amt = equity * weight
        shares      = int(min(per_pos_amt, self.portfolio.cash) // current_price)
        if shares <= 0:
            return None

        # TP/SL 계산
        tp = int(sig.get("tp", 0))
        sl = int(sig.get("sl", 0))

        if sl <= 0 or tp <= 0:
            atr = self._calc_atr(code)
            if atr <= 0:
                return None
            sl_mult = self.config.ATR_MULT_BULL if regime == MarketRegime.BULL else self.config.ATR_MULT_BEAR
            sl = int(current_price - atr * sl_mult)
            tp = int(current_price + (current_price - sl) * 2.0)

        if sl <= 0:
            return None

        # TP 역방향 방어
        if tp <= current_price:
            print(f"  [SizePos] {code} TP({tp}) <= 현재가({int(current_price)}) → 스킵")
            return None

        rr = round((tp - current_price) / (current_price - sl), 2) if current_price > sl else 0.0

        return {
            "code":        code,
            "qscore":      rs_c,
            "sector":      sector,
            "entry_price": int(current_price),
            "shares":      shares,
            "amount":      int(current_price * shares),
            "tp":          tp,
            "sl":          sl,
            "rr_ratio":    rr,
        }

    def _calc_atr(self, code: str, period: int = 20) -> float:
        try:
            df = self.provider.get_stock_ohlcv(code, days=30)
            if df is None or len(df) < period + 1:
                return 0.0
            import numpy as np
            high  = df["high"].astype(float).values
            low   = df["low"].astype(float).values
            close = df["close"].astype(float).values
            tr    = np.maximum(
                high[1:] - low[1:],
                np.maximum(np.abs(high[1:] - close[:-1]),
                           np.abs(low[1:]  - close[:-1]))
            )
            if len(tr) < period:
                return float(tr.mean())
            atr = float(tr[:period].mean())
            k   = 1.0 / period
            for v in tr[period:]:
                atr = atr * (1 - k) + v * k
            return atr
        except Exception:
            return 0.0
