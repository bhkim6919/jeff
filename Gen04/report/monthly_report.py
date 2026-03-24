"""
monthly_report.py — Gen4 Monthly HTML Report
==============================================
월간 보고서: 전략 성과 검증 + 투자 지속 여부 판단.

Callable:
  - main.py: 월말 EOD 또는 독립 실행
  - Standalone: python -m report.monthly_report [--month YYYY-MM]
"""
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from report.kospi_utils import (
    load_kospi_close, get_kospi_period_return, compute_excess_return,
    count_outperform_days,
)

logger = logging.getLogger("gen4.monthly_report")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Loading
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str})
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
        return df
    except Exception:
        return pd.DataFrame()


def get_month_range(year: int, month: int) -> Tuple[str, str]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _filter_range(df, col, start, end):
    if df.empty or col not in df.columns:
        return pd.DataFrame()
    return df[(df[col] >= start) & (df[col] <= end)]


def _dedup_daily(df, col="date"):
    """Keep only the last row per date (EOD snapshot, discard intraday)."""
    if df.empty or col not in df.columns:
        return df
    return df.drop_duplicates(subset=[col], keep="last").reset_index(drop=True)


def load_monthly_data(report_dir: Path, month_start: str, month_end: str,
                       initial_cash: float) -> dict:
    equity_df = _load_csv(report_dir / "equity_log.csv")
    positions_df = _load_csv(report_dir / "daily_positions.csv")
    trades_df = _load_csv(report_dir / "trades.csv")
    closes_df = _load_csv(report_dir / "close_log.csv")
    decisions_df = _load_csv(report_dir / "decision_log.csv")
    reconciles_df = _load_csv(report_dir / "reconcile_log.csv")

    eq_month = _dedup_daily(_filter_range(equity_df, "date", month_start, month_end))
    trades_month = _filter_range(trades_df, "date", month_start, month_end)
    if not trades_month.empty and "code" in trades_month.columns:
        trades_month = trades_month[trades_month["code"] != "REBALANCE"]
    closes_month = _filter_range(closes_df, "date", month_start, month_end)
    decisions_month = _filter_range(decisions_df, "date", month_start, month_end)
    reconciles_month = _filter_range(reconciles_df, "date", month_start, month_end)

    # Last day positions
    pos_end = pd.DataFrame()
    if not positions_df.empty and "date" in positions_df.columns:
        pm = _filter_range(positions_df, "date", month_start, month_end)
        if not pm.empty:
            last_date = pm["date"].max()
            pos_end = pm[pm["date"] == last_date]

    # Pre-month equity
    eq_before = None
    if not equity_df.empty and "date" in equity_df.columns:
        before = equity_df[equity_df["date"] < month_start]
        if not before.empty:
            eq_before = before.iloc[-1]

    return {
        "equity_month": eq_month,
        "equity_all": equity_df,
        "equity_before": eq_before,
        "positions_end": pos_end,
        "trades": trades_month,
        "closes": closes_month,
        "decisions": decisions_month,
        "reconciles": reconciles_month,
        "initial_cash": initial_cash,
        "month_start": month_start,
        "month_end": month_end,
        "_report_dir": str(report_dir),
        "_kospi": pd.Series(dtype=float),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Computation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compute_monthly_return(data):
    eq = data["equity_month"]
    if eq.empty or "equity" not in eq.columns:
        return 0.0, data["initial_cash"], data["initial_cash"]
    eqs = pd.to_numeric(eq["equity"], errors="coerce").dropna()
    end_eq = float(eqs.iloc[-1])
    if data["equity_before"] is not None:
        start_eq = float(data["equity_before"].get("equity", data["initial_cash"]))
    else:
        start_eq = data["initial_cash"]
    ret = (end_eq / start_eq - 1) if start_eq > 0 else 0
    return ret, end_eq, start_eq


def compute_monthly_mdd(data):
    eq = data["equity_month"]
    if eq.empty or "equity" not in eq.columns:
        return 0.0
    eqs = pd.to_numeric(eq["equity"], errors="coerce").dropna()
    if len(eqs) < 2:
        return 0.0
    peak = eqs.cummax()
    return float(((eqs - peak) / peak).min())


def compute_sharpe(data):
    eq = data["equity_month"]
    if eq.empty or "daily_pnl_pct" not in eq.columns:
        return 0.0
    pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
    if len(pnls) < 5:
        return 0.0
    mean = float(pnls.mean())
    std = float(pnls.std())
    if std == 0:
        return 0.0
    return mean / std * (252 ** 0.5)  # annualized


def compute_trade_stats_monthly(data):
    closes = data["closes"]
    trades = data["trades"]

    n_buys = len(trades[trades["side"] == "BUY"]) if not trades.empty and "side" in trades.columns else 0
    n_sells = len(trades[trades["side"] == "SELL"]) if not trades.empty and "side" in trades.columns else 0

    if closes.empty or "pnl_pct" not in closes.columns:
        return {"n_trades": 0, "n_buys": n_buys, "n_sells": n_sells,
                "win_rate": 0, "avg_win": 0, "avg_loss": 0,
                "payoff": 0, "avg_hold": 0, "total_pnl": 0}

    pnl = pd.to_numeric(closes["pnl_pct"], errors="coerce").dropna()
    amt = pd.to_numeric(closes["pnl_amount"], errors="coerce").dropna()
    hold = pd.to_numeric(closes["hold_days"], errors="coerce").dropna()

    winners = pnl[pnl > 0]
    losers = pnl[pnl <= 0]
    n = len(pnl)
    wr = len(winners) / n if n > 0 else 0
    avg_w = float(winners.mean()) if len(winners) > 0 else 0
    avg_l = float(losers.mean()) if len(losers) > 0 else 0
    payoff = abs(avg_w / avg_l) if avg_l != 0 else 0

    return {
        "n_trades": n, "n_buys": n_buys, "n_sells": n_sells,
        "win_rate": wr, "avg_win": avg_w, "avg_loss": avg_l,
        "payoff": payoff,
        "avg_hold": float(hold.mean()) if len(hold) > 0 else 0,
        "total_pnl": float(amt.sum()),
    }


def compute_turnover(data):
    """Monthly turnover = unique traded codes / end positions."""
    closes = data["closes"]
    pos = data["positions_end"]
    traded = set()
    if not closes.empty and "code" in closes.columns:
        traded |= set(closes["code"].astype(str))
    n_end = len(pos) if not pos.empty else 20  # assume 20 if no data
    return len(traded) / n_end if n_end > 0 else 0


def compute_cost_monthly(data):
    trades = data["trades"]
    week_cost = 0.0
    if not trades.empty and "cost" in trades.columns:
        week_cost = float(pd.to_numeric(trades["cost"], errors="coerce").fillna(0).sum())

    report_dir = Path(data.get("_report_dir", "."))
    trades_all = _load_csv(report_dir / "trades.csv")
    cum_cost = 0.0
    if not trades_all.empty and "cost" in trades_all.columns:
        if "code" in trades_all.columns:
            trades_all = trades_all[trades_all["code"] != "REBALANCE"]
        cum_cost = float(pd.to_numeric(trades_all["cost"], errors="coerce").fillna(0).sum())

    return {"month_cost": week_cost, "cum_cost": cum_cost}


def compute_verdict_monthly(mret, mdd, sharpe, n_trades):
    if n_trades == 0:
        return ("STANDBY", "대기", "#1565c0")
    if mret > 0 and mdd > -0.15 and sharpe > 1.0:
        return ("EXPAND", "확대", "#1b5e20")
    if mret > -0.03 and mdd > -0.15:
        return ("MAINTAIN", "유지", "#2e7d32")
    if mret <= -0.03 or mdd <= -0.15:
        return ("REDUCE", "축소", "#d32f2f")
    return ("MAINTAIN", "유지", "#2e7d32")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fk(val):
    v = float(val)
    return f'{"+" if v > 0 else ""}{v:,.0f}'

def _fp(val):
    v = float(val) * 100
    return f'{"+" if v > 0 else ""}{v:.2f}%'

def _color(val):
    v = float(val)
    return "#2e7d32" if v > 0 else "#d32f2f" if v < 0 else "#78909c"

def _card(title, value, color="#333", sub=""):
    return f"""<div style="flex:1;min-width:130px;background:#fff;border-radius:8px;
        padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.12);text-align:center;">
        <div style="font-size:12px;color:#78909c;margin-bottom:4px;">{title}</div>
        <div style="font-size:20px;font-weight:700;color:{color};">{value}</div>
        {f'<div style="font-size:11px;color:#aaa;margin-top:2px;">{sub}</div>' if sub else ''}
    </div>"""

def _section(title, content):
    return f"""<div style="margin-bottom:24px;">
        <h2 style="font-size:16px;color:#1a237e;border-bottom:2px solid #1565c0;
            padding-bottom:6px;margin-bottom:12px;">{title}</h2>
        {content}
    </div>"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Section Builders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_summary(data, config, mret, verdict, verdict_kr, vcolor, stats, mdd):
    _, end_eq, start_eq = compute_monthly_return(data)
    delta = end_eq - start_eq
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    lines = [
        f"월간 수익률 <b>{_fp(mret)}</b>, 자산 변동 <b>{_fk(delta)}</b>원",
        f"총 자산 <b>{_fk(end_eq)}</b>원 (누적 {_fp(cum)})",
        f"MDD {_fp(mdd)}, 거래 {stats['n_trades']}건",
    ]

    bg_map = {"EXPAND": "#c8e6c9", "MAINTAIN": "#c8e6c9", "REDUCE": "#ffcdd2", "STANDBY": "#e3f2fd"}
    badge = (f'<span style="display:inline-block;padding:4px 14px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#f5f5f5")};">'
             f'{verdict_kr}</span>')

    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#f5f5f5;border-radius:8px;padding:16px;margin-bottom:20px;">
        <div style="font-size:14px;line-height:1.7;">{"<br>".join(lines)}</div>
        <div>{badge}</div>
    </div>"""


def build_performance(data, config, mret, mdd, sharpe):
    _, end_eq, _ = compute_monthly_return(data)
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    # Calmar = annualized return / abs(MDD)
    days = max(len(data["equity_month"]), 1)
    ann_ret = mret * (252.0 / days)
    calmar = ann_ret / abs(mdd) if mdd != 0 else 0

    cards = (
        _card("월간 수익률", _fp(mret), _color(mret)) +
        _card("누적 수익률", _fp(cum), _color(cum)) +
        _card("MDD", _fp(mdd), _color(mdd)) +
        _card("Sharpe", f"{sharpe:.2f}",
              "#2e7d32" if sharpe > 1 else "#d32f2f" if sharpe < 0.5 else "#333",
              "연환산") +
        _card("Calmar", f"{calmar:.2f}",
              "#2e7d32" if calmar > 1 else "#333")
    )
    return _section("성과 지표",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_market_comparison(data, config, mret):
    kospi = data.get("_kospi", pd.Series(dtype=float))
    ms, me = data["month_start"], data["month_end"]
    k_ret = get_kospi_period_return(kospi, ms, me)

    if k_ret is None:
        return _section("시장 대비 성과",
            '<div style="color:#aaa;padding:12px;">KOSPI 데이터 없음</div>')

    excess, label = compute_excess_return(mret, k_ret)
    out_days, total_days = count_outperform_days(data["equity_month"], kospi)
    hit = f"{out_days}/{total_days}" if total_days > 0 else "N/A"

    lc = {"Outperform": "#2e7d32", "Underperform": "#d32f2f"}.get(label, "#78909c")

    cards = (
        _card("포트폴리오", _fp(mret), _color(mret)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret)) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>') +
        _card("Outperform", hit, "#333", "일별 기준")
    )
    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_trade_stats(stats):
    if stats["n_trades"] == 0 and stats["n_buys"] == 0:
        return _section("거래 통계",
            '<div style="color:#aaa;padding:12px;">월간 거래 없음</div>')

    nt = stats["n_trades"]
    th = 'style="text-align:left;padding:8px;border-bottom:1px solid #e0e0e0;font-size:12px;color:#78909c;"'
    td = 'style="padding:8px;text-align:right;font-size:14px;font-weight:600;"'

    rows = [
        ("매수 / 매도", f'{stats["n_buys"]}건 / {stats["n_sells"]}건'),
        ("청산 건수", f'{nt}건'),
        ("승률", f'{stats["win_rate"]*100:.0f}%' if nt > 0 else "N/A"),
        ("평균 수익", _fp(stats["avg_win"]) if nt > 0 else "N/A"),
        ("평균 손실", _fp(stats["avg_loss"]) if nt > 0 else "N/A"),
        ("Payoff Ratio", f'{stats["payoff"]:.2f}' if nt > 0 else "N/A"),
        ("평균 보유기간", f'{stats["avg_hold"]:.0f}일' if nt > 0 else "N/A"),
        ("실현 손익", _fk(stats["total_pnl"])),
    ]

    trs = ""
    for label, val in rows:
        trs += f'<tr><td {th}>{label}</td><td {td}>{val}</td></tr>'

    return _section("거래 통계",
        f'<table style="width:100%;border-collapse:collapse;">{trs}</table>')


def build_cost(data, config, mret):
    costs = compute_cost_monthly(data)

    _, end_eq, start_eq = compute_monthly_return(data)
    month_pnl = end_eq - start_eq

    # Cost impact: return before vs after cost
    ret_pre_cost = mret + (costs["month_cost"] / start_eq) if start_eq > 0 else mret
    ratio = (costs["month_cost"] / abs(month_pnl) * 100) if month_pnl != 0 else 0

    cards = (
        _card("월간 비용", _fk(costs["month_cost"]) + "원", "#333") +
        _card("누적 비용", _fk(costs["cum_cost"]) + "원", "#333") +
        _card("비용 전 수익률", _fp(ret_pre_cost), _color(ret_pre_cost), "비용 차감 전") +
        _card("비용/손익", f"{ratio:.1f}%",
              "#d32f2f" if ratio > 30 else "#333")
    )
    return _section("비용 영향도",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_turnover(data, stats):
    to = compute_turnover(data)
    n_end = len(data["positions_end"]) if not data["positions_end"].empty else 0

    cards = (
        _card("Turnover", f"{to*100:.0f}%", "#333",
              "월간 교체 종목 / 보유 종목") +
        _card("월말 포지션", f"{n_end}종목", "#333", "목표: 20종목") +
        _card("월간 청산", f'{stats["n_trades"]}건', "#333")
    )
    return _section("포트폴리오 회전",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_system(data):
    eq = data["equity_month"]
    recon = data["reconciles"]

    pf = int(pd.to_numeric(eq["price_fail_count"], errors="coerce").sum()) if not eq.empty and "price_fail_count" in eq.columns else 0
    rc = int(pd.to_numeric(eq["reconcile_corrections"], errors="coerce").sum()) if not eq.empty and "reconcile_corrections" in eq.columns else 0
    mo = int((eq["monitor_only"] == "Y").sum()) if not eq.empty and "monitor_only" in eq.columns else 0
    rebal = int((eq["rebalance_executed"] == "Y").sum()) if not eq.empty and "rebalance_executed" in eq.columns else 0
    recon_n = len(recon)

    # Forensic snapshots
    state_dir = Path(data.get("_report_dir", ".")).parent / "state"
    ms_compact = data["month_start"][:7].replace("-", "")
    forensic_count = len(list(state_dir.glob(f"forensic_{ms_compact}*.json"))) if state_dir.exists() else 0

    cards = (
        _card("가격 실패", f"{pf}건", "#d32f2f" if pf > 0 else "#2e7d32") +
        _card("Broker 보정", f"{rc + recon_n}건", "#d32f2f" if (rc + recon_n) > 0 else "#2e7d32") +
        _card("Monitor Only", f"{mo}일", "#d32f2f" if mo > 0 else "#2e7d32") +
        _card("리밸런스", f"{rebal}회", "#333") +
        _card("Critical Error", f"{forensic_count}건", "#d32f2f" if forensic_count > 0 else "#2e7d32")
    )
    return _section("시스템 안정성",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_conclusion(verdict, verdict_kr, vcolor, mret, mdd, sharpe, stats):
    reasons = []
    if verdict == "STANDBY":
        reasons.append("월간 거래 없음 (초기 상태 또는 전략 비활성)")
    elif verdict == "EXPAND":
        reasons.append(f"수익률 양호 ({_fp(mret)}), MDD 안정 ({_fp(mdd)}), Sharpe {sharpe:.2f}")
    elif verdict == "MAINTAIN":
        reasons.append(f"성과 유지 범위 ({_fp(mret)})")
    else:
        if mret <= -0.03:
            reasons.append(f"월간 손실 ({_fp(mret)})")
        if mdd <= -0.15:
            reasons.append(f"MDD 과다 ({_fp(mdd)})")

    actions = {"EXPAND": "투자 비중 확대 검토", "MAINTAIN": "현행 유지",
               "REDUCE": "투자 비중 축소 검토", "STANDBY": "전략 활성화 대기"}
    action = actions.get(verdict, "현행 유지")

    bg_map = {"EXPAND": "#c8e6c9", "MAINTAIN": "#c8e6c9", "REDUCE": "#ffcdd2", "STANDBY": "#e3f2fd"}
    badge = (f'<div style="text-align:center;margin:8px 0 16px;">'
             f'<span style="display:inline-block;padding:8px 24px;border-radius:12px;'
             f'font-size:18px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#f5f5f5")};">'
             f'{verdict_kr}</span></div>')

    reason_html = "<br>".join(f"- {r}" for r in reasons)
    action_html = (f'<div style="font-size:14px;text-align:center;padding:8px;'
                   f'background:#f5f5f5;border-radius:6px;margin-top:8px;">'
                   f'다음 액션: <b>{action}</b></div>')

    return _section("결론", badge + f'<div style="font-size:13px;padding:0 8px;">{reason_html}</div>' + action_html)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Assembly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_monthly_html(data, config) -> str:
    mret, _, _ = compute_monthly_return(data)
    mdd = compute_monthly_mdd(data)
    sharpe = compute_sharpe(data)
    stats = compute_trade_stats_monthly(data)

    verdict, verdict_kr, vcolor = compute_verdict_monthly(
        mret, mdd, sharpe, stats["n_trades"])

    sections = [
        build_summary(data, config, mret, verdict, verdict_kr, vcolor, stats, mdd),
        build_performance(data, config, mret, mdd, sharpe),
        build_market_comparison(data, config, mret),
        build_cost(data, config, mret),
        build_trade_stats(stats),
        build_turnover(data, stats),
        build_system(data),
        build_conclusion(verdict, verdict_kr, vcolor, mret, mdd, sharpe, stats),
    ]

    ms = data["month_start"]
    me = data["month_end"]
    month_label = ms[:7]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(s for s in sections if s)

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gen4 Monthly Report — {month_label}</title>
<style>
body {{ font-family: 'Malgun Gothic','Segoe UI',sans-serif; background:#f0f2f5;
       margin:0; padding:20px; color:#333; }}
.container {{ max-width:800px; margin:0 auto; }}
h1 {{ font-size:20px; color:#1a237e; margin-bottom:16px; }}
</style>
</head>
<body>
<div class="container">
<h1>Gen4 월간 보고서 — {month_label} ({ms} ~ {me})</h1>
{body}
<div style="text-align:center;font-size:11px;color:#bbb;margin-top:24px;padding-top:12px;
    border-top:1px solid #e0e0e0;">
    Generated: {ts} | Q-TRON Gen4 v4.0 | Initial: {_fk(data['initial_cash'])}
</div>
</div>
</body>
</html>"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Entry Points
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_monthly_report(report_dir: Path, config,
                             month_str: str = "") -> Optional[Path]:
    """Generate monthly HTML report. month_str = 'YYYY-MM'."""
    if not month_str:
        today = date.today()
        month_str = today.strftime("%Y-%m")
    year, month = int(month_str[:4]), int(month_str[5:7])
    ms, me = get_month_range(year, month)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    try:
        data = load_monthly_data(report_dir, ms, me, config.INITIAL_CASH)
        if hasattr(config, "INDEX_FILE") and config.INDEX_FILE.exists():
            data["_kospi"] = load_kospi_close(config.INDEX_FILE)
        html = generate_monthly_html(data, config)

        fname = f"monthly_{month_str.replace('-', '')}.html"
        path = report_dir / fname
        path.write_text(html, encoding="utf-8")
        logger.info(f"Monthly report generated: {path}")
        return path
    except Exception as e:
        logger.error(f"Monthly report generation failed: {e}")
        return None


if __name__ == "__main__":
    import argparse
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import Gen4Config

    parser = argparse.ArgumentParser(description="Gen4 Monthly Report")
    parser.add_argument("--month", default=date.today().strftime("%Y-%m"),
                        help="Report month (YYYY-MM)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    config = Gen4Config()
    path = generate_monthly_report(config.REPORT_DIR, config, args.month)
    if path:
        print(f"Report: {path}")
    else:
        print("Report generation failed.")
        sys.exit(1)
