# -*- coding: utf-8 -*-
"""
run_comparison.py
==================
Gen3Only vs MultiStrategy 비교 백테스트 실행.

사용법:
  python -m backtest.run_comparison
  또는
  python backtest/run_comparison.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backtest.historical_provider import HistoricalProvider, BacktestRegimeDetector
from backtest.strategy_base import StrategySelector
from backtest.strategies import TrendStrategy, MeanReversionStrategy, DefenseStrategy
from backtest.backtest_loop import BacktestEngine
from backtest.performance import compare_results


def main():
    # ── 데이터 경로 ──────────────────────────────────────────────────────
    data_dir = ROOT / "data"
    ohlcv_dir = str(data_dir / "ohlcv_kospi_daily")
    index_file = str(data_dir / "kospi_index_daily_5y.csv")
    universe_file = str(data_dir / "universe_kospi.csv")
    sector_map_path = str(data_dir / "sector_map.json")

    # ── 백테스트 기간 ────────────────────────────────────────────────────
    START = "2023-06-01"
    END = "2026-03-14"

    # ── 공통 설정 ────────────────────────────────────────────────────────
    INITIAL_CASH = 100_000_000
    MAX_POSITIONS = 20
    WEIGHT = 0.07
    SIGNAL_INTERVAL = 5  # 5거래일마다 시그널 갱신

    # ── 데이터 로드 (공유) ───────────────────────────────────────────────
    print("=" * 60)
    print("  Data Loading")
    print("=" * 60)
    provider = HistoricalProvider(
        ohlcv_dir=ohlcv_dir,
        index_file=index_file,
        universe_file=universe_file,
        sector_map_path=sector_map_path,
    )
    provider.load_all(min_rows=130)

    regime_det = BacktestRegimeDetector(ma_period=200)

    # ── A) Gen3Only: TrendStrategy를 모든 레짐에서 사용 ──────────────────
    print("\n" + "=" * 60)
    print("  [A] Gen3Only - Trend only (all regimes)")
    print("=" * 60)

    trend_all = TrendStrategy(top_n=30, atr_mult=2.5, rs_entry_min=0.80, max_hold=60)

    # Gen3Only: BULL일 때만 시그널 생성 (Trend는 BULL 아니면 빈 리스트 반환)
    # → 기존 Gen3 동작 재현: SIDEWAYS/BEAR에서는 진입 없음, 청산만 수행
    selector_a = StrategySelector({
        "BULL": trend_all,
        "SIDEWAYS": trend_all,   # generate_signals에서 BULL 아니면 [] 반환
        "BEAR": trend_all,
    })

    engine_a = BacktestEngine(
        provider=provider,
        selector=selector_a,
        regime_detector=regime_det,
        initial_cash=INITIAL_CASH,
        max_positions=MAX_POSITIONS,
        weight_per_pos=WEIGHT,
        signal_interval=SIGNAL_INTERVAL,
        label="Gen3Only",
    )
    result_a = engine_a.run(START, END)

    # ── B) MultiStrategy: 레짐별 전략 자동 선택 ─────────────────────────
    print("\n" + "=" * 60)
    print("  [B] MultiStrategy - per-regime (Trend/MR/Defense)")
    print("=" * 60)

    selector_b = StrategySelector({
        "BULL": TrendStrategy(top_n=30, atr_mult=2.5, rs_entry_min=0.80, max_hold=60),
        "SIDEWAYS": MeanReversionStrategy(rsi_thresh=30.0, tp_pct=0.04, sl_pct=0.02,
                                           top_n=15, max_hold=20),
        "BEAR": DefenseStrategy(max_pos=5, weight_mult=0.30, rs_min=0.90,
                                atr_max_pct=0.40, atr_mult=1.0, top_n=10, max_hold=30),
    })

    engine_b = BacktestEngine(
        provider=provider,
        selector=selector_b,
        regime_detector=regime_det,
        initial_cash=INITIAL_CASH,
        max_positions=MAX_POSITIONS,
        weight_per_pos=WEIGHT,
        signal_interval=SIGNAL_INTERVAL,
        label="MultiStrategy",
    )
    result_b = engine_b.run(START, END)

    # ── 비교 리포트 ──────────────────────────────────────────────────────
    output_dir = str(ROOT / "backtest" / "results")
    results = [r for r in [result_a, result_b] if r]
    if results:
        compare_results(results, output_dir=output_dir)
    else:
        print("\n[ERROR] 백테스트 결과 없음")


if __name__ == "__main__":
    main()
