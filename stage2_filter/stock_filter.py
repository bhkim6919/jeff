"""
StockFilter (확장판)
====================
필터 탈락 원인을 filter_log에 기록할 수 있도록
rejected_log 리스트를 함께 반환한다.
"""

import pandas as pd
from stage1_market.market_state import MarketState
from stage2_filter.universe import Universe
from core.data_provider import DataProvider


class StockFilter:

    def __init__(self, provider: DataProvider,
                 min_avg_volume: float = 2_000_000_000,
                 max_candidates: int   = 50):
        self.provider       = provider
        self.min_avg_volume = min_avg_volume
        self.max_candidates = max_candidates
        self.universe       = Universe(provider)

    def filter(self, market_state: MarketState) -> list[str]:
        """후보 종목 코드 리스트만 반환 (파이프라인 호환용)."""
        candidates, _ = self.filter_with_log(market_state)
        return candidates

    def filter_with_log(self, market_state: MarketState) -> tuple[list[str], list[dict]]:
        """
        반환: (후보 코드 리스트, 탈락 로그 리스트)
        탈락 로그 형식: [{"stage":..., "reason":..., "code":...}, ...]
        """
        if market_state == MarketState.BEAR:
            print("[Stage2] BEAR 구간 — 신규 후보 없음")
            return [], []

        universe    = self.universe.get_universe()
        print(f"[Stage2] 유니버스 {len(universe)}개 → 필터 시작 ({market_state.value})")

        candidates   = []
        rejected_log = []

        for code in universe:
            try:
                passed, reason = self._check(code, market_state)
                if passed:
                    candidates.append(code)
                    if len(candidates) >= self.max_candidates:
                        break
                else:
                    rejected_log.append({
                        "stage":  f"Stage2_{reason[0]}",
                        "reason": reason[1],
                        "code":   code,
                    })
            except Exception:
                continue

        print(f"[Stage2] 필터 완료 → {len(candidates)}개 후보 / {len(rejected_log)}개 탈락")
        return candidates, rejected_log

    # ── 필터 로직 ────────────────────────────────────────────────────────────

    def _check(self, code: str, market_state: MarketState) -> tuple[bool, tuple]:
        """(통과여부, (탈락구분, 탈락사유)) 반환."""
        df = self.provider.get_stock_ohlcv(code, days=60)
        if df is None or len(df) < 20:
            return False, ("데이터", "OHLCV 데이터 부족")

        # 유동성
        avg_vol = self.provider.get_avg_daily_volume(code, days=20)
        if avg_vol < self.min_avg_volume:
            return False, ("유동성", f"20일 평균 거래대금 {avg_vol/1e8:.1f}억 < 기준 {self.min_avg_volume/1e8:.0f}억")

        # MA 추세
        close = df["close"]
        ma20  = close.rolling(20).mean().iloc[-1]
        if close.iloc[-1] <= ma20:
            return False, ("MA", f"종가 {close.iloc[-1]:,} <= MA20 {int(ma20):,}")

        # 시장 상태별 추가
        if market_state == MarketState.BULL:
            ok, reason = self._bull_check(df)
        else:
            ok, reason = self._sideways_check(df)

        if not ok:
            return False, reason
        return True, ("", "")

    def _bull_check(self, df):
        close = df["close"]
        ret20 = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20]
        if ret20 <= 0.05:
            return False, ("모멘텀", f"20일 수익률 {ret20:.1%} <= 5%")
        vol = df["volume"]
        if vol.iloc[-5:].mean() >= vol.mean() * 3:
            return False, ("거래량과열", "최근 5일 거래량이 60일 평균의 3배 초과")
        return True, ("", "")

    def _sideways_check(self, df):
        close = df["close"]
        ret20 = (close.iloc[-1] - close.iloc[-20]) / close.iloc[-20]
        if ret20 <= 0:
            return False, ("모멘텀", f"20일 수익률 {ret20:.1%} <= 0")
        atr = self._calc_atr(df).iloc[-1]
        if atr / close.iloc[-1] >= 0.05:
            return False, ("변동성", f"ATR/종가 {atr/close.iloc[-1]:.1%} >= 5%")
        return True, ("", "")

    @staticmethod
    def _calc_atr(df, period=14):
        h, l, c = df["high"], df["low"], df["close"]
        tr = pd.concat([h-l, (h-c.shift(1)).abs(), (l-c.shift(1)).abs()], axis=1).max(axis=1)
        return tr.rolling(period).mean()
