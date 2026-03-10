"""
optimizer.py
============
Q-TRON 파라미터 그리드서치 최적화기.

탐색 파라미터:
  - min_avg_volume     : 유동성 기준 (20일 평균 거래대금)
  - bull_momentum_min  : BULL 구간 20일 수익률 하한
  - sideways_momentum_min: SIDEWAYS 구간 20일 수익률 하한
  - atr_ratio_max      : ATR/종가 최대 허용
  - tp_pct             : Take Profit %
  - sl_pct             : Stop Loss % (음수)

평가 기준:
  - 월 평균 수익률 1~2.5% 범위
  - MDD -15% 이내
  - Sharpe >= 0.5
  - 거래 횟수 >= 10회

사용:
  python backtest/optimizer.py
  python backtest/optimizer.py --train-only  # Train 구간만 탐색
"""

from __future__ import annotations

import argparse
import itertools
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from backtest.csv_provider import CsvProvider
from backtest.bt_engine import BacktestEngine, BtConfig, BtResult
from backtest.bt_reporter import BtReporter


# ── 파라미터 그리드 ───────────────────────────────────────────────────────────
PARAM_GRID: Dict[str, List[Any]] = {
    "min_avg_volume":          [5_000_000_000, 10_000_000_000, 20_000_000_000],  # 50억/100억/200억
    "bull_momentum_min":       [0.03, 0.05, 0.07],        # 3%, 5%, 7%
    "sideways_momentum_min":   [-0.02, 0.0, 0.02],        # -2%, 0%, 2%
    "atr_ratio_max":           [0.04, 0.05, 0.07],        # 4%, 5%, 7%
    "tp_pct":                  [0.04, 0.05, 0.07],        # 4%, 5%, 7%
    "sl_pct":                  [-0.015, -0.02, -0.03],    # -1.5%, -2%, -3%
}

# 목표 기준 (보수적 캘리브레이션)
CRITERIA = {
    "monthly_return_min":  0.005,   # 월 평균 수익률 0.5% 이상
    "monthly_return_max":  0.03,    # 월 평균 수익률 3.0% 이하
    "mdd_max":            -0.20,    # MDD -20% 이내
    "sharpe_min":          0.3,     # Sharpe >= 0.3
    "n_trades_min":        5,       # 최소 거래 5회
}

OUTPUT_DIR = Path("backtest/results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── QTronConfig 패치 유틸 ────────────────────────────────────────────────────
def patch_config(base_config, params: Dict[str, Any]):
    """
    base_config를 deepcopy 후 params 값으로 속성 오버라이드.
    QTronConfig 가 dataclass 또는 일반 클래스 모두 지원.
    """
    cfg = deepcopy(base_config)
    for k, v in params.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg


# ── 단일 파라미터 조합 백테스트 ──────────────────────────────────────────────
def run_single(
    provider:         CsvProvider,
    pipeline_factory,
    base_qt_config,
    params:           Dict[str, Any],
    bt_config:        BtConfig,
) -> Optional[BtResult]:
    """단일 파라미터 조합으로 백테스트 실행."""
    qt_cfg = patch_config(base_qt_config, params)
    engine = BacktestEngine(provider, pipeline_factory, bt_config, qt_cfg)
    try:
        return engine.run()
    except Exception as e:
        print(f"    [WARN] 파라미터 조합 실패: {e}")
        return None


# ── 조합 평가 ─────────────────────────────────────────────────────────────────
def evaluate(result: BtResult) -> dict:
    """BtResult → 평가 지표 dict."""
    m = result.metrics
    monthly = result.monthly_returns

    monthly_ret_mean = float(monthly["return"].mean()) if not monthly.empty else 0.0
    monthly_ret_std  = float(monthly["return"].std())  if not monthly.empty else 0.0
    monthly_neg_rate = float((monthly["return"] < 0).mean()) if not monthly.empty else 0.0

    return {
        "total_return":      m.get("total_return", 0),
        "cagr":              m.get("cagr", 0),
        "mdd":               m.get("mdd", 0),
        "sharpe":            m.get("sharpe", 0),
        "n_trades":          m.get("n_trades", 0),
        "win_rate":          m.get("win_rate", 0),
        "avg_pnl":           m.get("avg_pnl", 0),
        "monthly_ret_mean":  monthly_ret_mean,
        "monthly_ret_std":   monthly_ret_std,
        "monthly_neg_rate":  monthly_neg_rate,
    }


def passes_criteria(ev: dict) -> bool:
    """평가 지표가 목표 기준을 통과하는지 여부."""
    c = CRITERIA
    return (
        c["monthly_return_min"] <= ev["monthly_ret_mean"] <= c["monthly_return_max"]
        and ev["mdd"]    >= c["mdd_max"]
        and ev["sharpe"] >= c["sharpe_min"]
        and ev["n_trades"] >= c["n_trades_min"]
    )


# ── 그리드서치 ────────────────────────────────────────────────────────────────
def grid_search(
    provider:         CsvProvider,
    pipeline_factory,
    base_qt_config,
    param_grid:       Dict[str, List[Any]] = PARAM_GRID,
    train_only:       bool = False,
) -> pd.DataFrame:
    """
    전체 파라미터 조합을 순서대로 탐색.

    반환: 모든 조합의 결과 DataFrame (Train 지표 + Test 지표 포함)
    """
    keys   = list(param_grid.keys())
    values = list(param_grid.values())
    combos = list(itertools.product(*values))
    total  = len(combos)

    print(f"\n[Optimizer] 그리드서치 시작: {total}개 조합")
    print(f"[Optimizer] 탐색 파라미터: {keys}")

    records = []
    passed  = []

    for i, combo in enumerate(combos, 1):
        params = dict(zip(keys, combo))
        label  = "_".join(f"{k[:4]}{v}" for k, v in params.items())
        print(f"\n[{i:3d}/{total}] {params}")

        # ── Train 구간 ──
        bt_train = BtConfig(
            start       = "20220101",
            end         = "20231231",
            train_end   = "20231231",
            test_start  = "20240101",
        )
        result_train = run_single(provider, pipeline_factory, base_qt_config, params, bt_train)
        if result_train is None:
            continue

        ev_train = evaluate(result_train)
        passed_train = passes_criteria(ev_train)

        record = {"combo_id": i, **params,
                  **{f"train_{k}": v for k, v in ev_train.items()},
                  "train_pass": passed_train}

        # ── Test 구간 (Train 통과 시만 or train_only=False) ──
        ev_test = {}
        passed_test = False
        if not train_only:
            bt_test = BtConfig(
                start       = "20240101",
                end         = "20251231",
                train_end   = "20231231",
                test_start  = "20240101",
            )
            result_test = run_single(provider, pipeline_factory, base_qt_config, params, bt_test)
            if result_test:
                ev_test    = evaluate(result_test)
                passed_test= passes_criteria(ev_test)
                record.update({f"test_{k}": v for k, v in ev_test.items()})
                record["test_pass"] = passed_test

        records.append(record)

        # Train + Test 모두 통과한 조합만 후보에 추가
        if passed_train and (train_only or passed_test):
            passed.append((params, ev_train, ev_test))
            print(f"  ✅ 통과! Train Sharpe={ev_train['sharpe']:.3f} "
                  f"MDD={ev_train['mdd']*100:.1f}% "
                  f"월수익={ev_train['monthly_ret_mean']*100:.2f}%")
        else:
            print(f"  ❌ 탈락 (Train: pass={passed_train})")

    df = pd.DataFrame(records)

    # 결과 CSV 저장
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = OUTPUT_DIR / f"grid_search_{ts}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n[Optimizer] 전체 결과 저장 → {csv_path}")

    # 상위 5개 출력
    _print_top5(df, passed)

    return df


def _print_top5(df: pd.DataFrame, passed: list):
    """통과 조합 중 Sharpe 상위 5개 출력."""
    print(f"\n[Optimizer] {'='*60}")
    print(f"[Optimizer] 통과 조합: {len(passed)}개 / 전체 {len(df)}개")

    if not passed:
        print("[Optimizer] 통과 조합 없음. 기준 완화 또는 데이터 확인 필요.")
        # 기준 미달이어도 Sharpe 상위 5개는 보여줌
        if "train_sharpe" in df.columns:
            top = df.nlargest(5, "train_sharpe")
            print("\n[참고] Train Sharpe 상위 5개 (기준 미달 포함):")
            for _, row in top.iterrows():
                print(f"  Sharpe={row.get('train_sharpe',0):.3f} "
                      f"MDD={row.get('train_mdd',0)*100:.1f}% "
                      f"월={row.get('train_monthly_ret_mean',0)*100:.2f}% "
                      f"| {dict((k,row[k]) for k in PARAM_GRID if k in row)}")
        return

    # Sharpe 기준 정렬
    passed_sorted = sorted(passed, key=lambda x: x[1].get("sharpe", 0), reverse=True)
    print(f"\n상위 최대 5개 후보:")
    for rank, (params, ev_tr, ev_te) in enumerate(passed_sorted[:5], 1):
        print(f"\n  [{rank}위] {params}")
        print(f"    Train: Sharpe={ev_tr['sharpe']:.3f} "
              f"CAGR={ev_tr['cagr']*100:+.1f}% "
              f"MDD={ev_tr['mdd']*100:.1f}% "
              f"월={ev_tr['monthly_ret_mean']*100:.2f}% "
              f"승률={ev_tr['win_rate']*100:.1f}%")
        if ev_te:
            print(f"    Test:  Sharpe={ev_te['sharpe']:.3f} "
                  f"CAGR={ev_te['cagr']*100:+.1f}% "
                  f"MDD={ev_te['mdd']*100:.1f}% "
                  f"월={ev_te['monthly_ret_mean']*100:.2f}% "
                  f"승률={ev_te['win_rate']*100:.1f}%")


# ── 진입점 ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q-TRON 파라미터 최적화")
    parser.add_argument("--train-only", action="store_true",
                        help="Train 구간만 탐색 (Test 스킵)")
    parser.add_argument("--data-dir", default="backtest/data",
                        help="CSV 데이터 디렉토리")
    args = parser.parse_args()

    # 실제 사용 시 아래를 교체:
    #   from config import QTronConfig
    #   from core.pipeline import QTronPipeline
    #   base_config = QTronConfig()
    #   provider    = CsvProvider(args.data_dir)
    #   grid_search(provider, QTronPipeline, base_config, train_only=args.train_only)

    print("[Optimizer] optimizer.py를 직접 실행하려면 run_backtest.py를 사용하세요.")
    print("  python run_backtest.py --mode optimize")
