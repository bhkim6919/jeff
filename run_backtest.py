"""
run_backtest.py
===============
Q-TRON 백테스트 진입점.

모드:
  --mode collect    pykrx로 과거 데이터 수집 (최초 1회)
  --mode run        단일 백테스트 실행
  --mode optimize   그리드서치 최적화
  --mode compare    Train vs Test 비교 리포트

사용 예:
  # 1단계: 데이터 수집 (최초 1회, 약 1~2시간 소요)
  python run_backtest.py --mode collect --start 20220101 --end 20251231 --top 300

  # 2단계: 단일 백테스트
  python run_backtest.py --mode run --start 20220101 --end 20231231

  # 3단계: 최적화 (시간 오래 걸림)
  python run_backtest.py --mode optimize --train-only

  # 4단계: Train/Test 비교
  python run_backtest.py --mode compare
"""

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config import QTronConfig
from backtest.csv_provider import CsvProvider
from backtest.bt_engine import BacktestEngine, BtConfig
from backtest.bt_reporter import BtReporter
from backtest.optimizer import grid_search, PARAM_GRID

DATA_DIR = ROOT / "backtest" / "data"


# ── 파이프라인 팩토리 ─────────────────────────────────────────────────────────
def pipeline_factory(provider, qt_config):
    """
    백테스트용 파이프라인 팩토리.
    기존 QTronPipeline을 그대로 활용하되,
    'positioned' 목록을 result에 포함하도록 래핑.
    """
    from core.pipeline import QTronPipeline

    class BtPipeline(QTronPipeline):
        def run(self):
            result = super().run()
            # positioned 정보를 result에 추가 (bt_engine이 사용)
            # 실제 positioned는 QTronPipeline 내부에서 생성되므로
            # 여기서는 빈 리스트 반환 (Stage 분리 후 확장 예정)
            result.setdefault("positioned", [])
            return result

    return BtPipeline(qt_config, provider)


# ── 모드별 실행 함수 ──────────────────────────────────────────────────────────

def run_collect(args):
    """데이터 수집 모드."""
    from backtest.data_collector import main as collect_main
    import sys as _sys
    _sys.argv = [
        "data_collector.py",
        "--start", args.start,
        "--end",   args.end,
        "--top",   str(args.top),
    ]
    if args.index_only:
        _sys.argv.append("--index-only")
    collect_main()


def run_single(args):
    """단일 백테스트 모드."""
    print(f"\n[단일 백테스트] {args.start} ~ {args.end}")
    provider = CsvProvider(str(DATA_DIR))
    bt_cfg   = BtConfig(
        start         = args.start,
        end           = args.end,
        initial_cash  = args.cash,
        slippage      = args.slippage,
        commission    = args.commission,
        max_positions = args.max_pos,
    )
    qt_cfg = QTronConfig()
    engine = BacktestEngine(provider, pipeline_factory, bt_cfg, qt_cfg)
    result = engine.run()

    reporter = BtReporter(str(ROOT / "backtest" / "results"))
    reporter.print_summary(result, label="single")
    path = reporter.save(result, label="single")
    print(f"\n[완료] 리포트 → {path}")


def run_optimize(args):
    """그리드서치 최적화 모드."""
    print(f"\n[최적화] 그리드서치 시작 (train_only={args.train_only})")
    provider   = CsvProvider(str(DATA_DIR))
    qt_cfg     = QTronConfig()
    result_df  = grid_search(
        provider         = provider,
        pipeline_factory = pipeline_factory,
        base_qt_config   = qt_cfg,
        param_grid       = PARAM_GRID,
        train_only       = args.train_only,
    )
    print(f"\n[완료] 최적화 결과: {len(result_df)}개 조합 탐색")


def run_compare(args):
    """Train vs Test 비교 모드: 동일 파라미터로 두 구간 리포트 생성."""
    print("\n[비교] Train vs Test 백테스트")
    provider  = CsvProvider(str(DATA_DIR))
    reporter  = BtReporter(str(ROOT / "backtest" / "results"))
    qt_cfg    = QTronConfig()

    for label, start, end in [
        ("train", "20220101", "20231231"),
        ("test",  "20240101", "20251231"),
    ]:
        print(f"\n--- {label.upper()} 구간: {start} ~ {end} ---")
        bt_cfg = BtConfig(start=start, end=end,
                          initial_cash=args.cash,
                          slippage=args.slippage,
                          commission=args.commission,
                          max_positions=args.max_pos)
        engine = BacktestEngine(provider, pipeline_factory, bt_cfg, qt_cfg)
        result = engine.run()
        reporter.print_summary(result, label=label)
        reporter.save(result, label=label)

    print("\n[완료] Train/Test 비교 리포트 저장 완료")


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Q-TRON 백테스트 실행기")
    parser.add_argument("--mode", choices=["collect","run","optimize","compare"],
                        default="run", help="실행 모드")

    # 공통
    parser.add_argument("--start",      default="20220101")
    parser.add_argument("--end",        default="20231231")
    parser.add_argument("--cash",       default=10_000_000, type=float, help="초기 자본(원)")
    parser.add_argument("--slippage",   default=0.002,  type=float, help="슬리피지 (기본 0.2%)")
    parser.add_argument("--commission", default=0.00015,type=float, help="수수료 편도 (기본 0.015%)")
    parser.add_argument("--max-pos",    default=5,      type=int,   help="최대 보유 종목 수")

    # collect 전용
    parser.add_argument("--top",        default=300,    type=int,   help="유니버스 상위 N개")
    parser.add_argument("--index-only", action="store_true")

    # optimize 전용
    parser.add_argument("--train-only", action="store_true", help="Train 구간만 최적화")

    args = parser.parse_args()

    dispatch = {
        "collect":  run_collect,
        "run":      run_single,
        "optimize": run_optimize,
        "compare":  run_compare,
    }
    dispatch[args.mode](args)


if __name__ == "__main__":
    main()
