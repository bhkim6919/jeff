"""
BacktestEngine
==============
signals_YYYYMMDD.csv 기록을 기반으로 과거 성과를 시뮬레이션한다.

사용법:
  python tests/backtest_engine.py                # provider 없음 (random fallback)
  python tests/backtest_engine.py --pykrx        # PykrxProvider 실데이터

흐름:
  1. signals/ 디렉토리의 과거 signals_*.csv 를 날짜 순으로 로드
  2. 각 신호에 대해 신호일 이후 OHLCV 로드
  3. 실제 TP/SL/MAX_HOLD 청산 시뮬레이션 (provider 있을 때)
  4. 누적 수익 / MDD / 승률 계산 및 출력
"""

import sys
import csv
import random
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from config import Gen3Config


class BacktestEngine:

    def __init__(self, config: Gen3Config, provider=None):
        self.config   = config
        self.provider = provider
        self.sig_dir  = config.abs_path(config.signals_dir)

    def run(self) -> Dict[str, Any]:
        """
        signals/ 디렉토리의 모든 signals_*.csv 기반 백테스트 실행.
        반환: 성과 요약 dict
        """
        signal_files = sorted(self.sig_dir.glob("signals_*.csv"))
        if not signal_files:
            print("[Backtest] signals/ 디렉토리에 신호 파일 없음")
            return {}

        print(f"[Backtest] {len(signal_files)}개 신호 파일 로드")
        mode = "실데이터" if self.provider else "랜덤 시뮬레이션"
        print(f"[Backtest] 시뮬레이션 모드: {mode}")

        equity_curve = [self.config.initial_cash]
        trades: List[Dict] = []

        for sig_file in signal_files:
            date_str = sig_file.stem.replace("signals_", "")
            signals  = self._load_signals(sig_file)
            if not signals:
                continue

            batch_n = min(len(signals), self.config.max_positions)  # 최대 20개
            print(f"  {date_str}: {len(signals)}개 신호 (시뮬레이션 {batch_n}개)")
            for sig in signals[:batch_n]:
                trade = self._simulate_one(sig, date_str)
                if trade:
                    trades.append(trade)
                    # 균등 분산 기준: 1포지션이 전체의 1/max_positions
                    equity_curve.append(
                        equity_curve[-1] * (1 + trade["pnl_pct"] / self.config.max_positions)
                    )

        total_return = (equity_curve[-1] / equity_curve[0] - 1) if len(equity_curve) > 1 else 0.0
        mdd          = self._calc_mdd(equity_curve)
        win_rate     = self._calc_win_rate(trades)
        avg_pnl      = (sum(t["pnl_pct"] for t in trades) / len(trades)) if trades else 0.0

        result = {
            "total_return": f"{total_return:.2%}",
            "mdd":          f"{mdd:.2%}",
            "win_rate":     f"{win_rate:.2%}",
            "avg_pnl":      f"{avg_pnl:.2%}",
            "trade_count":  len(trades),
            "signal_days":  len(signal_files),
        }
        self._print_result(result, trades)
        return result

    # ── 시뮬레이션 ───────────────────────────────────────────────────────────

    def _simulate_one(self, sig: Dict, date_str: str) -> Optional[Dict]:
        """단일 신호에 대한 TP/SL/MAX_HOLD 시뮬레이션."""
        if self.provider:
            return self._simulate_with_provider(sig, date_str)
        return self._simulate_random(sig, date_str)

    def _simulate_with_provider(self, sig: Dict, date_str: str) -> Optional[Dict]:
        """
        실제 OHLCV 데이터로 TP/SL/MAX_HOLD 청산 시뮬레이션.

        - 신호일 이후 일봉을 순서대로 순회
        - 고가 >= TP  → TP 청산
        - 저가 <= SL  → SL 청산
        - MAX_HOLD_DAYS 초과 → 강제 청산 (종가 기준)
        """
        try:
            ticker = sig["ticker"]
            entry  = sig["entry"]
            tp     = sig["tp"]
            sl     = sig["sl"]

            if entry <= 0 or tp <= sl:
                return self._simulate_random(sig, date_str)

            signal_dt = datetime.strptime(date_str, "%Y%m%d")
            df = self.provider.get_stock_ohlcv(ticker, days=self.config.MAX_HOLD_DAYS + 10)
            if df is None or df.empty:
                return self._simulate_random(sig, date_str)

            # 신호일 이후 데이터만
            df = df[df["date"] > signal_dt].reset_index(drop=True)
            if df.empty:
                return self._simulate_random(sig, date_str)

            for i, row in df.iterrows():
                high  = float(row["high"])
                low   = float(row["low"])
                close = float(row["close"])

                if tp > 0 and high >= tp:
                    pnl_pct = (tp - entry) / entry
                    return {"date": date_str, "code": ticker, "qscore": sig["qscore"],
                            "pnl_pct": pnl_pct, "exit_type": "TP", "hold_days": i + 1}

                if sl > 0 and low <= sl:
                    pnl_pct = (sl - entry) / entry
                    return {"date": date_str, "code": ticker, "qscore": sig["qscore"],
                            "pnl_pct": pnl_pct, "exit_type": "SL", "hold_days": i + 1}

                if i + 1 >= self.config.MAX_HOLD_DAYS:
                    pnl_pct = (close - entry) / entry
                    return {"date": date_str, "code": ticker, "qscore": sig["qscore"],
                            "pnl_pct": pnl_pct, "exit_type": "MAX_HOLD", "hold_days": i + 1}

            # 데이터 부족 → 마지막 종가로 청산
            last_close = float(df.iloc[-1]["close"])
            pnl_pct    = (last_close - entry) / entry if entry > 0 else 0.0
            return {"date": date_str, "code": ticker, "qscore": sig["qscore"],
                    "pnl_pct": pnl_pct, "exit_type": "EOD", "hold_days": len(df)}

        except Exception as e:
            print(f"  [Backtest] {sig.get('ticker','?')} 실데이터 실패: {e} -> 랜덤 폴백")
            return self._simulate_random(sig, date_str)

    def _simulate_random(self, sig: Dict, date_str: str) -> Dict:
        """provider 없을 때 랜덤 PnL 시뮬레이션 (구조 확인용)."""
        pnl_pct = random.uniform(-0.05, 0.12)
        return {
            "date":      date_str,
            "code":      sig["ticker"],
            "qscore":    sig["qscore"],
            "pnl_pct":   pnl_pct,
            "exit_type": "RANDOM",
            "hold_days": random.randint(1, 30),
        }

    # ── 신호 로드 ─────────────────────────────────────────────────────────────

    def _load_signals(self, filepath: Path) -> List[Dict[str, Any]]:
        signals = []
        with open(filepath, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    signals.append({
                        "ticker": row["ticker"],
                        "qscore": float(row["qscore"]),
                        "entry":  int(row["entry"]),
                        "tp":     int(row["tp"]),
                        "sl":     int(row["sl"]),
                    })
                except Exception:
                    pass
        return signals

    # ── 통계 ─────────────────────────────────────────────────────────────────

    def _calc_mdd(self, equity_curve: List[float]) -> float:
        peak = equity_curve[0]
        mdd  = 0.0
        for e in equity_curve:
            peak = max(peak, e)
            mdd  = min(mdd, (e - peak) / peak if peak > 0 else 0.0)
        return mdd

    def _calc_win_rate(self, trades: List[dict]) -> float:
        if not trades:
            return 0.0
        wins = sum(1 for t in trades if t.get("pnl_pct", 0) > 0)
        return wins / len(trades)

    def _print_result(self, result: dict, trades: List[dict] = None) -> None:
        print("\n[Backtest 결과]")
        for k, v in result.items():
            print(f"  {k}: {v}")

        if trades and self.provider:
            exit_types: Dict[str, int] = {}
            for t in trades:
                et = t.get("exit_type", "?")
                exit_types[et] = exit_types.get(et, 0) + 1
            if exit_types:
                print("\n[청산 유형]")
                for et, cnt in sorted(exit_types.items()):
                    print(f"  {et}: {cnt}건")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Q-TRON Gen3 Backtest Engine")
    parser.add_argument("--pykrx", action="store_true", help="PykrxProvider 실데이터 모드")
    args = parser.parse_args()

    cfg = Gen3Config.load()

    if args.pykrx:
        from data.pykrx_provider import PykrxProvider
        engine = BacktestEngine(cfg, PykrxProvider())
    else:
        engine = BacktestEngine(cfg)

    engine.run()
