# -*- coding: utf-8 -*-
"""
backtest_compare.py
===================
Top10 스코어링 vs Batch Signal 백테스트 비교.

두 전략을 동일 기간에 시뮬레이션:
  A) Top10 Score: top20_report 스코어링 상위 10종목 등비중 매수 → N일 보유 → 청산
  B) Batch Signal: signals_YYYYMMDD.csv 종목 등비중 매수 → N일 보유 → 청산

비교 항목: 수익률, 승률, 최대손실, 평균수익

Usage:
  python backtest_compare.py                    # 기본 (5일 보유)
  python backtest_compare.py --hold 10          # 10일 보유
  python backtest_compare.py --hold 1           # 1일 보유 (당일 시가→종가)
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = Path(__file__).resolve().parent
OHLCV_DIR = BASE_DIR / "data" / "ohlcv_kospi_daily"
SIGNALS_DIR = BASE_DIR / "data" / "signals"
REPORT_DIR = BASE_DIR / "data" / "top20" / "reports"

sys.path.insert(0, str(BASE_DIR))


# ── OHLCV 로더 ──────────────────────────────────────────────────────────────

_ohlcv_cache: Dict[str, pd.DataFrame] = {}


def load_ohlcv(ticker: str) -> Optional[pd.DataFrame]:
    if ticker in _ohlcv_cache:
        return _ohlcv_cache[ticker]
    path = OHLCV_DIR / f"{ticker}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, dtype={"date": str})
        df["date"] = df["date"].str[:10].str.replace("-", "")
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
        df = df.sort_values("date").reset_index(drop=True)
        _ohlcv_cache[ticker] = df
        return df
    except Exception:
        return None


def get_price_on_date(ticker: str, date_str: str, field: str = "close") -> Optional[float]:
    """특정 날짜의 가격. 없으면 None."""
    df = load_ohlcv(ticker)
    if df is None:
        return None
    row = df[df["date"] == date_str]
    if row.empty:
        return None
    val = float(row[field].iloc[0])
    return val if val > 0 else None


def get_price_after_n_days(ticker: str, entry_date: str, n: int) -> Optional[float]:
    """entry_date 이후 n거래일 종가. 데이터 부족 시 마지막 종가."""
    df = load_ohlcv(ticker)
    if df is None:
        return None
    idx = df.index[df["date"] == entry_date]
    if len(idx) == 0:
        return None
    start = idx[0]
    exit_idx = min(start + n, len(df) - 1)
    return float(df["close"].iloc[exit_idx])


def get_trading_dates() -> List[str]:
    """OHLCV에서 거래일 목록 추출."""
    df = load_ohlcv("005930")
    if df is None:
        return []
    return df["date"].tolist()


# ── 시그널 로더 ──────────────────────────────────────────────────────────────

def load_signals(date_str: str) -> List[Dict]:
    path = SIGNALS_DIR / f"signals_{date_str}.csv"
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


# ── Top10 스코어링 (top20_report 로직 재사용) ────────────────────────────────

def compute_score_top10(date_str: str) -> List[Dict]:
    """특정 날짜 기준 OHLCV로 스코어링 → 상위 10종목 반환."""
    from top20_report import compute_full_analysis, _score_investment

    # 유동성 필터
    import re
    _pref = re.compile(r"^\d{5}[5-9]$")
    candidates = []
    for csv_path in OHLCV_DIR.glob("*.csv"):
        ticker = csv_path.stem
        if not (ticker.isdigit() and len(ticker) == 6) or _pref.match(ticker):
            continue
        df = load_ohlcv(ticker)
        if df is None:
            continue
        # 해당 날짜까지 데이터만 사용
        df_cut = df[df["date"] <= date_str].copy()
        if len(df_cut) < 120:
            continue
        close = df_cut["close"]
        volume = df_cut["volume"]
        last_close = float(close.iloc[-1])
        if last_close < 5000:
            continue
        amt_20 = float((close * volume).tail(20).mean()) / 1e8
        if amt_20 < 500:
            continue
        candidates.append((ticker, df_cut, amt_20))

    # 상위 200개만 분석
    candidates.sort(key=lambda x: -x[2])
    candidates = candidates[:200]

    scored = []
    for ticker, df_cut, _ in candidates:
        result = compute_full_analysis(ticker, df_cut)
        if result:
            inv = _score_investment(result)
            scored.append({**result, **inv})

    scored.sort(key=lambda x: -x["score"])
    return scored[:10]


# ── 백테스트 엔진 ────────────────────────────────────────────────────────────

def backtest_portfolio(picks: List[Dict], entry_date: str, hold_days: int,
                       entry_field: str = "open") -> Dict:
    """
    등비중 포트폴리오 백테스트.
    picks: [{"ticker": ..., "name": ..., "score": ...}, ...]
    entry_field: "open" (다음날 시가 매수) 또는 "close" (당일 종가 매수)
    """
    trades = []
    for p in picks:
        ticker = p["ticker"]
        # 진입: entry_date 다음 거래일 시가
        df = load_ohlcv(ticker)
        if df is None:
            continue
        idx_list = df.index[df["date"] == entry_date].tolist()
        if not idx_list:
            continue
        entry_idx = idx_list[0] + 1  # 다음 거래일
        if entry_idx >= len(df):
            continue

        entry_price = float(df[entry_field].iloc[entry_idx])
        entry_actual_date = df["date"].iloc[entry_idx]
        if entry_price <= 0:
            entry_price = float(df["close"].iloc[entry_idx])

        # 청산: entry_idx + hold_days
        exit_idx = min(entry_idx + hold_days, len(df) - 1)
        exit_price = float(df["close"].iloc[exit_idx])
        exit_date = df["date"].iloc[exit_idx]
        actual_hold = exit_idx - entry_idx

        if entry_price <= 0 or exit_price <= 0:
            continue

        ret_pct = (exit_price / entry_price - 1) * 100

        trades.append({
            "ticker": ticker,
            "name": p.get("name", ticker),
            "score": p.get("score", 0),
            "entry_date": entry_actual_date,
            "entry_price": entry_price,
            "exit_date": exit_date,
            "exit_price": exit_price,
            "hold_days": actual_hold,
            "ret_pct": round(ret_pct, 2),
        })

    if not trades:
        return {"trades": [], "avg_ret": 0, "total_ret": 0, "win_rate": 0,
                "max_loss": 0, "max_gain": 0, "n_trades": 0}

    rets = [t["ret_pct"] for t in trades]
    wins = sum(1 for r in rets if r > 0)

    return {
        "trades": trades,
        "avg_ret": round(np.mean(rets), 2),
        "total_ret": round(np.sum(rets), 2),
        "win_rate": round(wins / len(rets) * 100, 1),
        "max_loss": round(min(rets), 2),
        "max_gain": round(max(rets), 2),
        "n_trades": len(trades),
    }


# ── 메인 비교 ────────────────────────────────────────────────────────────────

def run_comparison(hold_days: int = 5) -> Dict:
    print("=" * 90)
    print(f"  Top10 Score vs Batch Signal 백테스트 비교  |  보유기간: {hold_days}일")
    print("=" * 90)

    # 시그널 날짜 목록
    signal_files = sorted(SIGNALS_DIR.glob("signals_*.csv"))
    signal_dates = []
    for sf in signal_files:
        d = sf.stem.replace("signals_", "")
        sigs = load_signals(d)
        if sigs:
            signal_dates.append(d)

    if not signal_dates:
        print("시그널 파일 없음")
        return {}

    print(f"\n시그널 날짜: {len(signal_dates)}개 ({signal_dates[0]}~{signal_dates[-1]})")

    all_results = {"score_top10": [], "batch_signal": []}

    for date_str in signal_dates:
        print(f"\n{'─'*80}")
        print(f"  {date_str}")
        print(f"{'─'*80}")

        # ── A) Top10 Score ────────────────────────────────────────────
        print(f"  [Score Top10] 스코어링 중...", end="", flush=True)
        top10 = compute_score_top10(date_str)
        if top10:
            bt_score = backtest_portfolio(top10, date_str, hold_days)
            print(f" {bt_score['n_trades']}종목 | "
                  f"avg={bt_score['avg_ret']:+.2f}% | "
                  f"승률={bt_score['win_rate']:.0f}% | "
                  f"최대손={bt_score['max_loss']:+.2f}%")
            for t in bt_score["trades"][:5]:
                print(f"    {t['ticker']} {t['name'][:8]:8s} "
                      f"score={t['score']:5.1f} | "
                      f"{t['entry_price']:>10,.0f} → {t['exit_price']:>10,.0f} "
                      f"({t['hold_days']}d) {t['ret_pct']:+6.2f}%")
            all_results["score_top10"].append({
                "date": date_str, **bt_score,
            })
        else:
            print(" 데이터 부족")

        # ── B) Batch Signal ───────────────────────────────────────────
        signals = load_signals(date_str)
        if signals:
            sig_picks = []
            for s in signals[:10]:  # 상위 10개만
                sig_picks.append({
                    "ticker": s["ticker"],
                    "name": "",
                    "score": float(s.get("qscore", 0)) * 100,
                })
            bt_signal = backtest_portfolio(sig_picks, date_str, hold_days)
            print(f"  [Batch Signal] {bt_signal['n_trades']}종목 | "
                  f"avg={bt_signal['avg_ret']:+.2f}% | "
                  f"승률={bt_signal['win_rate']:.0f}% | "
                  f"최대손={bt_signal['max_loss']:+.2f}%")
            for t in bt_signal["trades"][:5]:
                print(f"    {t['ticker']} "
                      f"qscore={t['score']:5.1f} | "
                      f"{t['entry_price']:>10,.0f} → {t['exit_price']:>10,.0f} "
                      f"({t['hold_days']}d) {t['ret_pct']:+6.2f}%")
            all_results["batch_signal"].append({
                "date": date_str, **bt_signal,
            })

    # ── 종합 비교 ─────────────────────────────────────────────────────
    print(f"\n{'='*90}")
    print(f"  종합 비교 (보유기간: {hold_days}일)")
    print(f"{'='*90}")

    for label, key in [("Score Top10", "score_top10"), ("Batch Signal", "batch_signal")]:
        results = all_results[key]
        if not results:
            continue
        all_rets = []
        all_wins = 0
        all_total = 0
        for r in results:
            for t in r["trades"]:
                all_rets.append(t["ret_pct"])
                if t["ret_pct"] > 0:
                    all_wins += 1
                all_total += 1

        if not all_rets:
            continue

        avg = np.mean(all_rets)
        med = np.median(all_rets)
        wr = all_wins / all_total * 100
        mx = max(all_rets)
        mn = min(all_rets)
        total = sum(all_rets)
        # 일별 평균 수익률
        daily_avgs = [r["avg_ret"] for r in results if r["n_trades"] > 0]
        cumul = sum(daily_avgs)

        print(f"\n  [{label}]")
        print(f"    기간:       {results[0]['date']} ~ {results[-1]['date']} ({len(results)}일)")
        print(f"    총 거래:    {all_total}건")
        print(f"    평균 수익:  {avg:+.2f}%")
        print(f"    중앙값:     {med:+.2f}%")
        print(f"    승률:       {wr:.1f}% ({all_wins}/{all_total})")
        print(f"    최대 수익:  {mx:+.2f}%")
        print(f"    최대 손실:  {mn:+.2f}%")
        print(f"    누적 수익:  {cumul:+.2f}% (일별 avg 합산)")

    # ── 일별 대결 ─────────────────────────────────────────────────────
    print(f"\n{'─'*90}")
    print(f"  {'날짜':>10}  {'Score Top10':>15}  {'Batch Signal':>15}  {'차이':>10}  승자")
    print(f"{'─'*90}")

    score_wins = 0
    signal_wins = 0
    score_map = {r["date"]: r for r in all_results["score_top10"]}
    signal_map = {r["date"]: r for r in all_results["batch_signal"]}
    all_dates = sorted(set(list(score_map.keys()) + list(signal_map.keys())))

    for d in all_dates:
        s_ret = score_map[d]["avg_ret"] if d in score_map and score_map[d]["n_trades"] > 0 else None
        b_ret = signal_map[d]["avg_ret"] if d in signal_map and signal_map[d]["n_trades"] > 0 else None
        if s_ret is not None and b_ret is not None:
            diff = s_ret - b_ret
            winner = "Score" if diff > 0 else "Signal" if diff < 0 else "Draw"
            if diff > 0:
                score_wins += 1
            elif diff < 0:
                signal_wins += 1
            print(f"  {d:>10}  {s_ret:>+14.2f}%  {b_ret:>+14.2f}%  {diff:>+9.2f}%  {winner}")
        elif s_ret is not None:
            print(f"  {d:>10}  {s_ret:>+14.2f}%  {'N/A':>15}  {'':>10}  -")
        elif b_ret is not None:
            print(f"  {d:>10}  {'N/A':>15}  {b_ret:>+14.2f}%  {'':>10}  -")

    total_matches = score_wins + signal_wins
    if total_matches > 0:
        print(f"\n  Score 승: {score_wins}회 / Signal 승: {signal_wins}회 "
              f"(Score 승률: {score_wins/total_matches*100:.0f}%)")

    return all_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Top10 Score vs Batch Signal Backtest")
    parser.add_argument("--hold", type=int, default=5, help="보유 기간 (거래일, 기본=5)")
    args = parser.parse_args()
    run_comparison(hold_days=args.hold)
