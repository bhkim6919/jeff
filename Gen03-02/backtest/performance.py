# -*- coding: utf-8 -*-
"""
PerformanceReport
==================
두 백테스트 결과를 나란히 비교하는 콘솔 + CSV 리포트.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def compare_results(results: List[Dict[str, Any]], output_dir: str = "") -> None:
    """
    여러 백테스트 결과를 나란히 비교 출력.
    results: [engine.run() 반환값, ...]
    """
    print("\n" + "=" * 80)
    print("  백테스트 비교 결과")
    print("=" * 80)

    # ── 핵심 지표 비교 ──────────────────────────────────────────────────────

    header = f"{'지표':<20}"
    for r in results:
        header += f"  {r.get('label', '?'):>18}"
    print(header)
    print("-" * 80)

    rows = [
        ("총수익률",      lambda r: f"{r['total_return']*100:+.2f}%"),
        ("MDD",           lambda r: f"{r['mdd']*100:.2f}%"),
        ("거래 수",       lambda r: f"{r['n_trades']}"),
        ("승률",          lambda r: f"{r['win_rate']*100:.1f}%"),
        ("평균 수익(승)", lambda r: f"{r['avg_win']*100:+.2f}%"),
        ("평균 손실(패)", lambda r: f"{r['avg_loss']*100:+.2f}%"),
        ("평균 보유일",   lambda r: f"{r['avg_hold_days']:.1f}일"),
        ("최종 자산",     lambda r: f"{r['final_equity']:,.0f}원"),
    ]

    for label, fmt in rows:
        line = f"{label:<20}"
        for r in results:
            try:
                line += f"  {fmt(r):>18}"
            except Exception:
                line += f"  {'N/A':>18}"
        print(line)

    # ── 레짐별 분포 ─────────────────────────────────────────────────────────

    print(f"\n{'레짐 분포':<20}", end="")
    for r in results:
        print(f"  {r.get('label', '?'):>18}", end="")
    print()
    print("-" * 80)
    all_regimes = set()
    for r in results:
        all_regimes.update(r.get("regime_counts", {}).keys())
    for regime in sorted(all_regimes):
        line = f"  {regime:<18}"
        for r in results:
            cnt = r.get("regime_counts", {}).get(regime, 0)
            line += f"  {cnt:>18}"
        print(line)

    # ── 전략별 성과 분리 ──────────────────────────────────────────────────

    for r in results:
        label = r.get("label", "?")
        trades = r.get("trades", [])
        if not trades:
            continue

        print(f"\n--- [{label}] 전략별 성과 분리 ---")
        strat_groups: Dict[str, list] = {}
        for t in trades:
            sn = t.strategy_name
            strat_groups.setdefault(sn, []).append(t)

        print(f"  {'전략':<16} {'거래수':>6} {'승률':>8} {'평균PnL':>10} "
              f"{'총PnL':>12} {'평균보유':>8}")
        print("  " + "-" * 66)
        for sn, st_trades in sorted(strat_groups.items()):
            n = len(st_trades)
            wins = sum(1 for t in st_trades if t.pnl_pct > 0)
            wr = wins / n if n else 0
            avg_pnl = sum(t.pnl_pct for t in st_trades) / n if n else 0
            total_pnl = sum(t.pnl_won for t in st_trades)
            avg_hold = sum(t.hold_days for t in st_trades) / n if n else 0
            print(f"  {sn:<16} {n:>6} {wr*100:>7.1f}% {avg_pnl*100:>+9.2f}% "
                  f"{total_pnl:>+11,.0f}원 {avg_hold:>7.1f}일")

    # ── 청산 사유 분포 ───────────────────────────────────────────────────

    for r in results:
        label = r.get("label", "?")
        trades = r.get("trades", [])
        if not trades:
            continue

        print(f"\n--- [{label}] 청산 사유 분포 ---")
        exit_types = Counter(t.exit_type for t in trades)
        print(f"  {'사유':<16} {'건수':>6} {'비율':>8}")
        print("  " + "-" * 34)
        for et, cnt in exit_types.most_common():
            pct = cnt / len(trades) * 100
            print(f"  {et:<16} {cnt:>6} {pct:>7.1f}%")

    # ── CSV 저장 ──────────────────────────────────────────────────────────

    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        for r in results:
            label = r.get("label", "result")
            # equity curve
            if r.get("equity_curve"):
                eq_df = pd.DataFrame(r["equity_curve"])
                eq_file = out_path / f"equity_{label}.csv"
                eq_df.to_csv(eq_file, index=False)

            # trades
            if r.get("trades"):
                trade_rows = []
                for t in r["trades"]:
                    trade_rows.append({
                        "code": t.code, "strategy": t.strategy_name,
                        "entry_date": str(t.entry_date.date()),
                        "exit_date": str(t.exit_date.date()),
                        "entry_price": t.entry_price, "exit_price": t.exit_price,
                        "pnl_pct": round(t.pnl_pct * 100, 2),
                        "pnl_won": round(t.pnl_won),
                        "exit_type": t.exit_type, "hold_days": t.hold_days,
                        "sector": t.sector, "regime": t.regime_at_entry,
                    })
                tr_df = pd.DataFrame(trade_rows)
                tr_file = out_path / f"trades_{label}.csv"
                tr_df.to_csv(tr_file, index=False)

        print(f"\n[CSV 저장] {out_path}/")

    print("\n" + "=" * 80)
