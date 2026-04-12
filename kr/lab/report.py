"""
report.py — Lab 메트릭스 + 비교 리포트 + 차트
===============================================
그룹 내 비교만 수행. cross-group 비교 금지.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd

from lab.lab_config import DISABLE_CROSS_GROUP_COMPARISON, EXPECTED_EXPOSURE

logger = logging.getLogger("lab.report")


def calc_metrics(eq: pd.Series, trades: list) -> dict:
    """성과 지표 계산. backtester.calc_metrics 호환."""
    if len(eq) < 2:
        return {}
    r = eq.pct_change().dropna()
    tot = eq.iloc[-1] / eq.iloc[0] - 1
    ny = len(eq) / 252
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / ny) - 1 if ny > 0 else 0
    pk = eq.expanding().max()
    dd = (eq - pk) / pk
    mdd = float(dd.min())
    sharpe = float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0

    dr = r[r < 0]
    sortino = float(r.mean() / dr.std() * np.sqrt(252)) if len(dr) > 0 and dr.std() > 0 else 0
    calmar = abs(cagr / mdd) if mdd != 0 else 0

    pnls = [t["pnl_pct"] for t in trades] if trades else []
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    wr = len(wins) / len(pnls) if pnls else 0
    pf = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else 999
    avg_win = float(np.mean(wins)) if wins else 0
    avg_loss = float(np.mean(losses)) if losses else 0
    hd = [t.get("hold_days", 0) for t in trades]
    avg_hold = float(np.mean(hd)) if hd else 0
    median_hold = float(np.median(hd)) if hd else 0

    # Exit reason breakdown
    exit_reasons = {}
    for t in trades:
        reason = t.get("exit_reason", "UNKNOWN")
        if reason not in exit_reasons:
            exit_reasons[reason] = {"count": 0, "wins": 0, "total_pnl": 0}
        exit_reasons[reason]["count"] += 1
        exit_reasons[reason]["total_pnl"] += t["pnl_pct"]
        if t["pnl_pct"] > 0:
            exit_reasons[reason]["wins"] += 1

    # Additional metrics
    return_per_trade = tot / len(trades) if trades else 0
    median_pnl = float(np.median(pnls)) if pnls else 0

    return dict(
        total_return=tot, cagr=cagr, mdd=mdd, calmar=calmar,
        sharpe=sharpe, sortino=sortino,
        profit_factor=pf, win_rate=wr,
        avg_win=avg_win, avg_loss=avg_loss,
        n_trades=len(trades), avg_hold_days=avg_hold,
        median_hold_days=median_hold,
        exit_reasons=exit_reasons,
        return_per_trade=return_per_trade,
        median_trade_pnl=median_pnl,
    )


def calc_exposure_and_turnover(state, config) -> dict:
    """Exposure + Turnover 계산."""
    eq = pd.Series(state.equity_history).sort_index()
    if len(eq) < 2:
        return {"avg_exposure": 0, "turnover": 0}

    avg_eq = eq.mean()
    avg_exposure = 1.0 - (state.cash / avg_eq) if avg_eq > 0 else 0

    # Annualized turnover
    n_days = len(eq)
    total_traded = state.total_buy_amount + state.total_sell_amount
    turnover = (total_traded / avg_eq / n_days * 252) if avg_eq > 0 and n_days > 0 else 0

    return {"avg_exposure": avg_exposure, "turnover": turnover}


def generate_report(lab_result: dict, no_charts: bool = False) -> None:
    """리포트 생성. 그룹 내 비교만 수행."""
    results = lab_result.get("results", {})
    output_dir = Path(lab_result.get("output_dir", "."))
    config = lab_result.get("config")

    if not results:
        print("[LAB_REPORT] No results to report")
        return

    print(f"\n[LAB_REPORT] Generating report to {output_dir}")

    # ── Per-strategy metrics ─────────────────────────────────────
    all_metrics = {}
    for sname, res in results.items():
        eq = res["equity"]
        trades = res["trades"]
        m = calc_metrics(eq, trades)
        et = calc_exposure_and_turnover(res["state"], config)
        m.update(et)
        m["group"] = res["group"]
        all_metrics[sname] = m

        # Save per-strategy detail
        detail_dir = output_dir / "detail" / sname
        detail_dir.mkdir(parents=True, exist_ok=True)

        eq.to_csv(detail_dir / "equity.csv", header=["equity"])
        if trades:
            pd.DataFrame(trades).to_csv(
                detail_dir / "trades.csv", index=False, encoding="utf-8-sig")

        # metrics.json (exit_reasons 제외하면 JSON serializable)
        m_json = {k: (v if not isinstance(v, (np.floating, np.integer))
                       else float(v))
                  for k, v in m.items()}
        (detail_dir / "metrics.json").write_text(
            json.dumps(m_json, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8")

    # ── Summary table ────────────────────────────────────────────
    summary_rows = []
    for sname, m in sorted(all_metrics.items(), key=lambda x: x[1].get("mdd", -1), reverse=True):
        summary_rows.append({
            "Strategy": sname,
            "Group": m.get("group", ""),
            "Return%": round(m.get("total_return", 0) * 100, 2),
            "MDD%": round(m.get("mdd", 0) * 100, 2),
            "Sharpe": round(m.get("sharpe", 0), 2),
            "Calmar": round(m.get("calmar", 0), 2),
            "Sortino": round(m.get("sortino", 0), 2),
            "WinRate%": round(m.get("win_rate", 0) * 100, 1),
            "Trades": m.get("n_trades", 0),
            "AvgHold": round(m.get("avg_hold_days", 0), 1),
            "Exposure": round(m.get("avg_exposure", 0), 2),
            "Turnover": round(m.get("turnover", 0), 2),
        })

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_dir / "summary.csv", index=False, encoding="utf-8-sig")

    # Print summary
    print(f"\n{'='*90}")
    print(f"  Strategy Lab Summary (sorted by MDD, best first)")
    print(f"{'='*90}")
    for _, row in summary_df.iterrows():
        print(f"  {row['Strategy']:20s} [{row['Group']:6s}]  "
              f"MDD={row['MDD%']:+6.1f}%  Sharpe={row['Sharpe']:5.2f}  "
              f"Calmar={row['Calmar']:5.2f}  Return={row['Return%']:+6.1f}%  "
              f"Trades={row['Trades']:3d}  Exposure={row['Exposure']:.2f}")
    print(f"{'='*90}")

    # ── Equity curves CSV ────────────────────────────────────────
    eq_dict = {}
    for sname, res in results.items():
        eq_dict[sname] = res["equity"]
    eq_df = pd.DataFrame(eq_dict)
    # Normalize to 100
    eq_norm = eq_df / eq_df.iloc[0] * 100
    eq_norm.to_csv(output_dir / "equity_curves.csv", encoding="utf-8-sig")

    # ── Overlap matrix ───────────────────────────────────────────
    # Compute pairwise stock overlap (within group only)
    groups_in_result = {}
    for sname, res in results.items():
        g = res["group"]
        if g not in groups_in_result:
            groups_in_result[g] = []
        groups_in_result[g].append(sname)

    # ── Summary JSON ─────────────────────────────────────────────
    summary_json = {
        "run_id": lab_result.get("run_id", ""),
        "period": f"{config.START_DATE} ~ {config.END_DATE or 'latest'}",
        "mode": config.LAB_MODE,
        "strategies": {
            sname: {k: v for k, v in m.items() if k != "exit_reasons"}
            for sname, m in all_metrics.items()
        },
        "groups": groups_in_result,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary_json, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8")

    # ── Charts ───────────────────────────────────────────────────
    if not no_charts:
        try:
            _generate_charts(eq_norm, all_metrics, groups_in_result, output_dir)
        except Exception as e:
            logger.warning(f"[LAB_CHART_ERROR] {e}")
            print(f"  Charts skipped: {e}")


def _generate_charts(eq_norm: pd.DataFrame, all_metrics: dict,
                     groups: dict, output_dir: Path) -> None:
    """Matplotlib 차트 생성."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  matplotlib not available, skipping charts")
        return

    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)

    # 1. Equity curves (per group)
    for group, strats in groups.items():
        if len(strats) < 2 and DISABLE_CROSS_GROUP_COMPARISON:
            continue
        fig, ax = plt.subplots(figsize=(12, 6))
        for s in strats:
            if s in eq_norm.columns:
                ax.plot(eq_norm.index, eq_norm[s], label=s, linewidth=1.5)
        ax.set_title(f"Equity Curves — {group} group")
        ax.set_ylabel("Normalized (100)")
        ax.legend(loc="upper left")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(chart_dir / f"equity_{group}.png", dpi=120)
        plt.close(fig)

    # 2. Drawdown (per group)
    for group, strats in groups.items():
        if len(strats) < 2 and DISABLE_CROSS_GROUP_COMPARISON:
            continue
        fig, ax = plt.subplots(figsize=(12, 4))
        for s in strats:
            if s in eq_norm.columns:
                series = eq_norm[s]
                dd = (series - series.expanding().max()) / series.expanding().max() * 100
                ax.fill_between(dd.index, dd, alpha=0.3, label=s)
        ax.set_title(f"Drawdown — {group} group")
        ax.set_ylabel("Drawdown %")
        ax.legend(loc="lower left")
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(chart_dir / f"drawdown_{group}.png", dpi=120)
        plt.close(fig)

    # 3. Trade stats bar chart
    strat_names = list(all_metrics.keys())
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    wr = [all_metrics[s].get("win_rate", 0) * 100 for s in strat_names]
    axes[0].barh(strat_names, wr, color="steelblue")
    axes[0].set_title("Win Rate %")

    trades = [all_metrics[s].get("n_trades", 0) for s in strat_names]
    axes[1].barh(strat_names, trades, color="coral")
    axes[1].set_title("Trade Count")

    sharpe = [all_metrics[s].get("sharpe", 0) for s in strat_names]
    axes[2].barh(strat_names, sharpe, color="forestgreen")
    axes[2].set_title("Sharpe Ratio")

    fig.tight_layout()
    fig.savefig(chart_dir / "trade_stats.png", dpi=120)
    plt.close(fig)

    print(f"  Charts saved to {chart_dir}")
