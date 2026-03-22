"""
RegimeDetector (v7)
===================
Q-TRON Gen3 v7 레짐 판단.

변경 내역 (v7):
  1. MA200 기반 단순 레짐: KOSPI 종가 > MA200 → BULL, 이하 → BEAR
  2. Breadth 보완: 유니버스 내 MA20 상회 비율 < 35% → BULL이어도 BEAR 강제 전환
  3. REGIME_FLIP_GATE = 2일 유예 (노이즈 방지)
  4. 배치 결과(regime_YYYYMMDD.json) 우선 사용 → 없으면 실시간 계산

계산 흐름:
  regime_YYYYMMDD.json 존재 → 저장된 레짐 사용 (권장)
  없으면 → kospi_index_daily_5y.csv 읽어 MA200 계산
         → ohlcv_kospi_daily/ 읽어 Breadth 계산 (선택적)
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd


class MarketRegime(Enum):
    BULL     = "BULL"
    SIDEWAYS = "SIDEWAYS"
    BEAR     = "BEAR"


class RegimeDetector:

    def __init__(self, provider, config):
        self.provider        = provider
        self.config          = config
        self.signals_dir     = config.abs_path(config.signals_dir)
        self.index_file      = config.abs_path(config.index_file)
        self._prev_regime:   Optional[MarketRegime] = None
        self._flip_count:    int = 0

    def detect(self) -> MarketRegime:
        """
        오늘 레짐 결정. 우선순위:
        1. 배치에서 저장된 regime_YYYYMMDD.json
        2. kospi_index_daily_5y.csv (MA200)
        3. 실시간 pykrx 폴백
        """
        # 1순위: 배치 결과 JSON
        regime = self._load_from_json()
        if regime is not None:
            print(f"[RegimeDetector] 배치 결과 로드: {regime.value}")
            return regime

        # 2순위: 인덱스 CSV
        regime = self._detect_from_csv()
        if regime is not None:
            print(f"[RegimeDetector] CSV 기반 레짐: {regime.value}")
            return regime

        # 3순위: 실시간 pykrx 폴백
        regime = self._detect_live()
        print(f"[RegimeDetector] 실시간 레짐: {regime.value}")
        return regime

    # ── 1. JSON 로드 ─────────────────────────────────────────────────────────

    def _load_from_json(self) -> Optional[MarketRegime]:
        today_str = date.today().strftime("%Y%m%d")
        path      = self.signals_dir / f"regime_{today_str}.json"
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            regime_str = data.get("regime", "BEAR")
            # 적응형 Breadth 임계값 반영 (v7.2)
            if "breadth_thresh" in data:
                self.config.BREADTH_BEAR_THRESH = float(data["breadth_thresh"])
            return MarketRegime[regime_str]
        except Exception:
            return None

    # ── 2. CSV 기반 MA200 ────────────────────────────────────────────────────

    def _detect_from_csv(self) -> Optional[MarketRegime]:
        if not self.index_file.exists():
            return None
        try:
            df    = pd.read_csv(self.index_file, parse_dates=["date"])
            df    = df.sort_values("date")
            if len(df) < 200:
                return None
            close = df["close"].astype(float)
            ma200 = float(close.rolling(200).mean().iloc[-1])
            last  = float(close.iloc[-1])
            base  = MarketRegime.BULL if last > ma200 else MarketRegime.BEAR

            # FLIP_GATE 적용
            return self._apply_flip_gate(base)
        except Exception:
            return None

    # ── 3. 실시간 폴백 ────────────────────────────────────────────────────────

    def _detect_live(self) -> MarketRegime:
        if self.provider is None:
            return MarketRegime.SIDEWAYS
        try:
            df    = self.provider.get_index_ohlcv("KOSPI", days=220)
            if df is None or df.empty or len(df) < 200:
                return MarketRegime.SIDEWAYS
            close = df["close"].astype(float)
            ma200 = float(close.rolling(200).mean().iloc[-1])
            last  = float(close.iloc[-1])
            base  = MarketRegime.BULL if last > ma200 else MarketRegime.BEAR
            return self._apply_flip_gate(base)
        except Exception:
            return MarketRegime.SIDEWAYS

    # ── FLIP_GATE ─────────────────────────────────────────────────────────────

    def _apply_flip_gate(self, new_regime: MarketRegime) -> MarketRegime:
        """레짐 전환 시 REGIME_FLIP_GATE일 유예."""
        gate = getattr(self.config, "REGIME_FLIP_GATE", 2)
        if self._prev_regime is None:
            self._prev_regime = new_regime
            self._flip_count  = 0
            return new_regime

        if new_regime != self._prev_regime:
            self._flip_count += 1
            if self._flip_count >= gate:
                self._prev_regime = new_regime
                self._flip_count  = 0
            else:
                # 유예 중 → 이전 레짐 유지
                return self._prev_regime
        else:
            self._flip_count = 0

        return self._prev_regime
