# backtest/run_backtest.py
from __future__ import annotations

import argparse
from pathlib import Path

from config import QTronConfig
from backtest.csv_provider import CsvProvider
from backtest.bt_engine import BacktestEngine, BtConfig
from backtest.bt_reporter import BtReporter
from backtest.bt_gen2_pipeline import Gen2BacktestPipeline


def pipeline_factory(provider, qtron_config):
    """
    BacktestEngine 에 주입할 파이프라인 생성 함수.
    (provider, qtron_config) 시그니처를 맞춰줍니다.
    """
    return Gen2BacktestPipeline(provider, qtron_config)


def main():
    parser = argparse.ArgumentParser(description="Q-TRON Gen2 Core v1.0 백테스트 실행")
    parser.add_argument("--data-dir", default="backtest/data",
                        help="CSV 데이터 디렉토리 (기본: backtest/data)")
    parser.add_argument("--start", default="20220101",
                        help="백테스트 시작일 (YYYYMMDD)")
    parser.add_argument("--end", default="20250228",
                        help="백테스트 종료일 (YYYYMMDD)")
    parser.add_argument("--label", default="gen2_core_v1",
                        help="리포트 파일명에 붙일 라벨")
    parser.add_argument("--initial-cash", type=float, default=None,
                        help="초기 자본 (미입력 시 config.INITIAL_CAPITAL 사용)")

    args = parser.parse_args()

    # 1) Gen2 Core v1.0 설정 로드
    qt_cfg = QTronConfig.load()
    initial_cash = args.initial_cash
    if initial_cash is None:
        # config.py 에 정의된 INITIAL_CAPITAL 사용
        initial_cash = getattr(qt_cfg, "INITIAL_CAPITAL", 100_000_000)

    max_positions = getattr(qt_cfg, "MAX_POSITIONS", 20)

    # 수수료 / 슬리피지는 Gen2 파라미터를 그대로 재사용
    slippage  = getattr(qt_cfg, "SLIPPAGE", 0.001)
    commission = getattr(qt_cfg, "FEE", 0.00015)
    sell_tax  = getattr(qt_cfg, "TAX", 0.0018)

    # 2) CSV 기반 DataProvider 준비
    data_dir = args.data_dir
    if not Path(data_dir).exists():
        raise SystemExit(f"[ERROR] 데이터 디렉토리 없음: {data_dir}")

    provider = CsvProvider(data_dir)

    # 3) 백테스트 엔진 설정
    bt_cfg = BtConfig(
        start        = args.start,
        end          = args.end,
        initial_cash = initial_cash,
        slippage     = slippage,
        commission   = commission,
        sell_tax     = sell_tax,
        max_positions= max_positions,
        # Train/Test 경계는 필요에 따라 조정
        train_end    = "20231231",
        test_start   = "20240101",
    )

    print("==================================================")
    print("  Q-TRON Gen2 Core v1.0 백테스트 실행")
    print("==================================================")
    print(f"데이터 디렉토리 : {data_dir}")
    print(f"기간           : {bt_cfg.start} ~ {bt_cfg.end}")
    print(f"초기자본       : {bt_cfg.initial_cash:,.0f}원")
    print(f"최대 포지션수  : {bt_cfg.max_positions}")
    print("--------------------------------------------------")

    # 4) 엔진 실행
    engine = BacktestEngine(provider, pipeline_factory, bt_cfg, qt_cfg)
    result = engine.run()

    # 5) 리포트 출력
    reporter = BtReporter(output_dir="backtest/results")
    reporter.print_summary(result, label=args.label)
    reporter.save(result, label=args.label)


if __name__ == "__main__":
    main()