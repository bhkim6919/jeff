"""
weekly_report.py — Gen4 Weekly HTML Report
============================================
주간 보고서: 전략 정상 작동 여부 판단.

Callable:
  - main.py EOD (Friday): generate_weekly_report(config.REPORT_DIR, config)
  - Standalone: python -m report.weekly_report [--date YYYY-MM-DD]
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

logger = logging.getLogger("gen4.weekly_report")


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


def get_week_range(ref_date: date) -> Tuple[str, str]:
    """Monday~Friday range for the week containing ref_date.
    Works for all days: Mon(0)~Sun(6) → same week's Monday."""
    monday = ref_date - timedelta(days=ref_date.weekday())
    friday = monday + timedelta(days=4)
    return monday.strftime("%Y-%m-%d"), friday.strftime("%Y-%m-%d")


def _filter_range(df: pd.DataFrame, col: str, start: str, end: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return pd.DataFrame()
    return df[(df[col] >= start) & (df[col] <= end)]


def _dedup_daily(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    """Keep only the last row per date (EOD snapshot, discard intraday)."""
    if df.empty or col not in df.columns:
        return df
    return df.drop_duplicates(subset=[col], keep="last").reset_index(drop=True)


def load_weekly_data(report_dir: Path, week_start: str, week_end: str,
                      initial_cash: float) -> dict:
    equity_df = _load_csv(report_dir / "equity_log.csv")
    positions_df = _load_csv(report_dir / "daily_positions.csv")
    trades_df = _load_csv(report_dir / "trades.csv")
    closes_df = _load_csv(report_dir / "close_log.csv")
    decisions_df = _load_csv(report_dir / "decision_log.csv")
    reconciles_df = _load_csv(report_dir / "reconcile_log.csv")

    eq_week = _dedup_daily(_filter_range(equity_df, "date", week_start, week_end))
    pos_week = _filter_range(positions_df, "date", week_start, week_end)
    trades_week = _filter_range(trades_df, "date", week_start, week_end)
    if not trades_week.empty and "code" in trades_week.columns:
        trades_week = trades_week[trades_week["code"] != "REBALANCE"]
    closes_week = _filter_range(closes_df, "date", week_start, week_end)
    decisions_week = _filter_range(decisions_df, "date", week_start, week_end)
    reconciles_week = _filter_range(reconciles_df, "date", week_start, week_end)

    # Positions at start vs end of week
    pos_start_codes = set()
    pos_end_codes = set()
    if not pos_week.empty and "date" in pos_week.columns:
        dates = sorted(pos_week["date"].unique())
        if dates:
            pos_start_codes = set(pos_week[pos_week["date"] == dates[0]]["code"].astype(str))
            pos_end_codes = set(pos_week[pos_week["date"] == dates[-1]]["code"].astype(str))

    # Pre-week equity (for weekly return calc)
    eq_before = None
    if not equity_df.empty and "date" in equity_df.columns:
        before = equity_df[equity_df["date"] < week_start]
        if not before.empty:
            eq_before = before.iloc[-1]

    return {
        "equity_week": eq_week,
        "equity_all": equity_df,
        "equity_before": eq_before,
        "positions_week": pos_week,
        "trades": trades_week,
        "closes": closes_week,
        "decisions": decisions_week,
        "reconciles": reconciles_week,
        "pos_start_codes": pos_start_codes,
        "pos_end_codes": pos_end_codes,
        "initial_cash": initial_cash,
        "week_start": week_start,
        "week_end": week_end,
        "_report_dir": str(report_dir),
        "_kospi": pd.Series(dtype=float),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Computation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compute_weekly_return(data: dict) -> Tuple[float, float, float]:
    """Returns (weekly_return, end_equity, start_equity)."""
    eq = data["equity_week"]
    if eq.empty or "equity" not in eq.columns:
        return 0.0, data["initial_cash"], data["initial_cash"]

    eqs = pd.to_numeric(eq["equity"], errors="coerce").dropna()
    if eqs.empty:
        return 0.0, data["initial_cash"], data["initial_cash"]

    end_eq = float(eqs.iloc[-1])

    # Start equity: pre-week last or first of week
    if data["equity_before"] is not None:
        start_eq = float(data["equity_before"].get("equity", data["initial_cash"]))
    else:
        start_eq = data["initial_cash"]

    ret = (end_eq / start_eq - 1) if start_eq > 0 else 0
    return ret, end_eq, start_eq


def compute_weekly_volatility(data: dict) -> float:
    eq = data["equity_week"]
    if eq.empty or "daily_pnl_pct" not in eq.columns:
        return 0.0
    pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
    if len(pnls) < 2:
        return 0.0
    return float(pnls.std())


def compute_weekly_dd(data: dict) -> float:
    eq = data["equity_week"]
    if eq.empty or "equity" not in eq.columns:
        return 0.0
    eqs = pd.to_numeric(eq["equity"], errors="coerce").dropna()
    if len(eqs) < 2:
        return 0.0
    peak = eqs.cummax()
    dd = (eqs - peak) / peak
    return float(dd.min())


def compute_trade_stats(data: dict) -> dict:
    closes = data["closes"]
    trades = data["trades"]

    n_buys = 0
    n_sells = 0
    if not trades.empty and "side" in trades.columns:
        n_buys = len(trades[trades["side"] == "BUY"])
        n_sells = len(trades[trades["side"] == "SELL"])

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
        "n_trades": n,
        "n_buys": n_buys,
        "n_sells": n_sells,
        "win_rate": wr,
        "avg_win": avg_w,
        "avg_loss": avg_l,
        "payoff": payoff,
        "avg_hold": float(hold.mean()) if len(hold) > 0 else 0,
        "total_pnl": float(amt.sum()),
    }


def compute_verdict_weekly(weekly_ret: float, win_rate: float,
                            sys_errors: int, n_trades: int = 0,
                            end_positions: int = 0,
                            monitor_only_days: int = 0) -> Tuple[str, str, str]:
    # No-trade week: not a failure
    if n_trades == 0 and end_positions == 0 and sys_errors == 0 and monitor_only_days == 0:
        return ("STANDBY", "대기", "#1565c0")
    if sys_errors > 0 or monitor_only_days > 0:
        return ("WATCH", "관찰 필요", "#f57f17")
    if weekly_ret <= -0.05 or (n_trades > 0 and win_rate < 0.30):
        return ("REVIEW", "전략 점검", "#d32f2f")
    if weekly_ret <= -0.03 or (n_trades > 0 and 0.30 <= win_rate < 0.40):
        return ("WATCH", "관찰 필요", "#f57f17")
    return ("MAINTAIN", "정상 유지", "#2e7d32")


def compute_system_stats(data: dict) -> dict:
    eq = data["equity_week"]
    recon = data["reconciles"]

    pf_total = 0
    rc_total = 0
    mo_days = 0
    rebal_days = 0

    if not eq.empty:
        if "price_fail_count" in eq.columns:
            pf_total = int(pd.to_numeric(eq["price_fail_count"], errors="coerce").sum())
        if "reconcile_corrections" in eq.columns:
            rc_total = int(pd.to_numeric(eq["reconcile_corrections"], errors="coerce").sum())
        if "monitor_only" in eq.columns:
            mo_days = int((eq["monitor_only"] == "Y").sum())
        if "rebalance_executed" in eq.columns:
            rebal_days = int((eq["rebalance_executed"] == "Y").sum())

    recon_count = len(recon)

    return {
        "price_fail_total": pf_total,
        "reconcile_total": rc_total + recon_count,
        "monitor_only_days": mo_days,
        "rebalance_count": rebal_days,
        "total_errors": pf_total + rc_total + recon_count,
    }


def compute_trail_stats(data: dict) -> dict:
    closes = data["closes"]
    if closes.empty or "exit_reason" not in closes.columns:
        return {"trail_count": 0, "rebal_count": 0, "trail_ratio": 0}
    trail = len(closes[closes["exit_reason"] == "TRAIL_STOP"])
    rebal = len(closes[closes["exit_reason"] == "REBALANCE_EXIT"])
    total = trail + rebal
    return {
        "trail_count": trail,
        "rebal_count": rebal,
        "trail_ratio": trail / total if total > 0 else 0,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Helpers (same pattern as daily_report)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fk(val) -> str:
    v = float(val)
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.0f}"

def _fp(val) -> str:
    v = float(val) * 100
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"

def _color(val) -> str:
    v = float(val)
    return "#2e7d32" if v > 0 else "#d32f2f" if v < 0 else "#78909c"

def _card(title, value, color="#333", sub=""):
    return f"""<div style="flex:1;min-width:140px;background:#fff;border-radius:8px;
        padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.12);text-align:center;">
        <div style="font-size:12px;color:#78909c;margin-bottom:4px;">{title}</div>
        <div style="font-size:22px;font-weight:700;color:{color};">{value}</div>
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

def build_summary(data, config, wret, verdict, verdict_kr, vcolor, stats):
    _, end_eq, start_eq = compute_weekly_return(data)
    delta = end_eq - start_eq
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    lines = [
        f"주간 수익률 <b>{_fp(wret)}</b>, "
        f"자산 변동 <b>{_fk(delta)}</b>원",
        f"총 자산 <b>{_fk(end_eq)}</b>원 (누적 {_fp(cum)})",
        f"거래 {stats['n_trades']}건 (승률 {stats['win_rate']*100:.0f}%)",
    ]

    bg_map = {"MAINTAIN": "#c8e6c9", "WATCH": "#fff9c4", "REVIEW": "#ffcdd2",
              "STANDBY": "#e3f2fd"}
    badge = (f'<span style="display:inline-block;padding:4px 14px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#f5f5f5")};">'
             f'{verdict_kr}</span>')

    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#f5f5f5;border-radius:8px;padding:16px;margin-bottom:20px;">
        <div style="font-size:14px;line-height:1.7;">{"<br>".join(lines)}</div>
        <div>{badge}</div>
    </div>"""


def build_performance(data, config, wret, vol):
    _, end_eq, _ = compute_weekly_return(data)
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    cards = (
        _card("주간 수익률", _fp(wret), _color(wret)) +
        _card("주간 변동성", f"{vol*100:.2f}%", "#333", "일별 수익률 std") +
        _card("누적 수익률", _fp(cum), _color(cum),
              f"기준 {_fk(data['initial_cash'])}") +
        _card("총 자산", _fk(end_eq), "#333")
    )

    # Daily returns list
    eq = data["equity_week"]
    daily_list = ""
    if not eq.empty and "date" in eq.columns and "daily_pnl_pct" in eq.columns:
        rows = ""
        for _, r in eq.iterrows():
            p = float(r.get("daily_pnl_pct", 0))
            rows += (f'<span style="display:inline-block;margin:2px 4px;padding:2px 8px;'
                     f'border-radius:4px;font-size:12px;'
                     f'background:{"#e8f5e9" if p>=0 else "#ffebee"};'
                     f'color:{_color(p)};">'
                     f'{r["date"][-5:]} {_fp(p)}</span>')
        daily_list = f'<div style="margin-top:8px;">{rows}</div>'

    return _section("주간 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>{daily_list}')


def build_trade_stats(stats):
    if stats["n_trades"] == 0 and stats["n_buys"] == 0:
        return _section("거래 통계",
            '<div style="color:#aaa;padding:12px;">주간 거래 없음</div>')

    th = 'style="text-align:left;padding:8px;border-bottom:1px solid #e0e0e0;font-size:12px;color:#78909c;"'
    td = 'style="padding:8px;text-align:right;font-size:14px;font-weight:600;"'

    nt = stats["n_trades"]
    wr_str = f'{stats["win_rate"]*100:.0f}%' if nt > 0 else "평가 불가"
    payoff_str = f'{stats["payoff"]:.2f}' if nt > 0 else "N/A"
    hold_str = f'{stats["avg_hold"]:.0f}일' if nt > 0 else "N/A"

    rows = [
        ("매수 / 매도", f'{stats["n_buys"]}건 / {stats["n_sells"]}건'),
        ("청산 건수", f'{nt}건'),
        ("승률", wr_str),
        ("평균 수익 (winner)", _fp(stats["avg_win"]) if nt > 0 else "N/A"),
        ("평균 손실 (loser)", _fp(stats["avg_loss"]) if nt > 0 else "N/A"),
        ("Payoff Ratio", payoff_str),
        ("평균 보유기간", hold_str),
        ("실현 손익 합계", _fk(stats["total_pnl"])),
    ]

    trs = ""
    for label, val in rows:
        trs += f'<tr><td {th}>{label}</td><td {td}>{val}</td></tr>'

    return _section("거래 통계",
        f'<table style="width:100%;border-collapse:collapse;">{trs}</table>')


def build_portfolio_changes(data):
    start = data["pos_start_codes"]
    end = data["pos_end_codes"]
    added = sorted(end - start)
    removed = sorted(start - end)
    kept = sorted(start & end)

    closes = data["closes"]

    parts = [f'<div style="font-size:13px;">주초 {len(start)}종목 → 주말 {len(end)}종목 '
             f'(유지 {len(kept)}, 편입 {len(added)}, 제거 {len(removed)})</div>']

    if kept:
        parts.append(f'<div style="margin-top:6px;font-size:13px;">'
                     f'<span style="color:#78909c;">유지:</span> '
                     f'{", ".join(kept)}</div>')
    if added:
        parts.append(f'<div style="margin-top:6px;font-size:13px;">'
                     f'<span style="color:#1565c0;">신규 편입:</span> '
                     f'{", ".join(added)}</div>')
    if removed:
        reasons = {}
        if not closes.empty and "code" in closes.columns:
            for _, r in closes.iterrows():
                c = str(r.get("code", ""))
                if c in removed:
                    reasons[c] = str(r.get("exit_reason", ""))
        reason_parts = []
        for c in removed:
            r = reasons.get(c, "")
            reason_parts.append(f'{c}({r})' if r else c)
        parts.append(f'<div style="margin-top:4px;font-size:13px;">'
                     f'<span style="color:#d32f2f;">제거:</span> '
                     f'{", ".join(reason_parts)}</div>')

    # Closed positions (from close_log, may include mid-week closes not in position diff)
    if not closes.empty and "code" in closes.columns:
        th = 'style="text-align:left;padding:5px 8px;border-bottom:1px solid #e0e0e0;font-size:11px;color:#78909c;"'
        rows = ""
        for _, r in closes.iterrows():
            pnl = float(r.get("pnl_pct", 0))
            rows += (f'<tr><td style="padding:4px 8px;">{r.get("code","")}</td>'
                     f'<td style="padding:4px 8px;">{r.get("exit_reason","")}</td>'
                     f'<td style="padding:4px 8px;text-align:right;color:{_color(pnl)};">'
                     f'{_fp(pnl)}</td>'
                     f'<td style="padding:4px 8px;text-align:right;">{r.get("hold_days",0)}일</td></tr>')
        parts.append(f'<div style="margin-top:10px;font-size:12px;color:#78909c;">주간 청산 종목</div>'
                     f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                     f'<tr><th {th}>종목</th><th {th}>사유</th>'
                     f'<th {th} style="text-align:right;">손익률</th>'
                     f'<th {th} style="text-align:right;">보유</th></tr>'
                     f'{rows}</table>')

    if not start and not end and closes.empty:
        parts = ['<div style="color:#aaa;padding:12px;">포지션 데이터 없음</div>']

    return _section("포트폴리오 변화", "\n".join(parts))


def build_risk(data, wret, weekly_dd):
    # Overall MDD
    eq_all = data["equity_all"]
    mdd = 0.0
    if not eq_all.empty and "equity" in eq_all.columns:
        eqs = pd.to_numeric(eq_all["equity"], errors="coerce").dropna()
        if len(eqs) > 0:
            peak = eqs.cummax()
            mdd = float(((eqs - peak) / peak).min())

    # Recovery check
    _, end_eq, start_eq = compute_weekly_return(data)
    recovered = "회복" if end_eq >= start_eq else "미회복"
    rec_color = "#2e7d32" if end_eq >= start_eq else "#d32f2f"

    cards = (
        _card("주간 최대 낙폭", _fp(weekly_dd), _color(weekly_dd), "주간 내 peak→trough") +
        _card("전체 MDD", _fp(mdd), _color(mdd)) +
        _card("DD 회복", recovered, rec_color, f"주초→주말 자산 비교")
    )
    return _section("리스크 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_strategy(data, trail, sys_stats=None):
    sys = sys_stats or compute_system_stats(data)
    end_pos = len(data["pos_end_codes"])

    items = [
        f'Trail Stop 발동: <b>{trail["trail_count"]}건</b> '
        f'(전체 청산의 {trail["trail_ratio"]*100:.0f}%)',
        f'REBALANCE_EXIT: <b>{trail["rebal_count"]}건</b>',
        f'리밸런스 실행: <b>{sys["rebalance_count"]}회</b>',
        f'편입 종목 수: <b>{end_pos}/20</b> (목표 20종목)',
    ]

    # No-position explanation
    if end_pos == 0:
        reasons = []
        if sys["rebalance_count"] == 0:
            reasons.append("리밸런스 미실행")
        if sys["monitor_only_days"] > 0:
            reasons.append(f"monitor-only {sys['monitor_only_days']}일")
        if not reasons:
            reasons.append("초기 상태 또는 target 부재")
        items.append(f'무포지션 사유: {", ".join(reasons)}')

    # Factor analysis from decisions
    dec = data["decisions"]
    if not dec.empty and "score_mom" in dec.columns and "side" in dec.columns:
        buys = dec[dec["side"] == "BUY"]
        if not buys.empty:
            avg_mom = pd.to_numeric(buys["score_mom"], errors="coerce").mean()
            avg_vol = pd.to_numeric(buys["score_vol"], errors="coerce").mean()
            items.append(f'LowVol+Mom12-1 편입 평균 — Mom: {avg_mom:.4f}, Vol: {avg_vol:.6f}')

    content = ""
    for item in items:
        content += f'<div style="font-size:13px;padding:4px 0;">{item}</div>'
    return _section("전략 검증 (Gen4: LowVol + Mom12-1)", content)


def build_system_section(data):
    sys = compute_system_stats(data)

    cards = (
        _card("가격 실패", f'{sys["price_fail_total"]}건',
              "#d32f2f" if sys["price_fail_total"] > 0 else "#2e7d32") +
        _card("Broker 보정", f'{sys["reconcile_total"]}건',
              "#d32f2f" if sys["reconcile_total"] > 0 else "#2e7d32") +
        _card("Monitor Only", f'{sys["monitor_only_days"]}일',
              "#d32f2f" if sys["monitor_only_days"] > 0 else "#2e7d32") +
        _card("리밸런스", f'{sys["rebalance_count"]}회', "#333")
    )
    return _section("시스템 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_market_comparison_weekly(data, config, wret) -> str:
    """주간 KOSPI 대비 성과."""
    kospi = data.get("_kospi", pd.Series(dtype=float))
    ws, we = data["week_start"], data["week_end"]

    k_ret = get_kospi_period_return(kospi, ws, we)
    if k_ret is None:
        return _section("시장 대비 성과",
            '<div style="color:#aaa;padding:12px;">KOSPI 데이터 없음</div>')

    excess, label = compute_excess_return(wret, k_ret)
    out_days, total_days = count_outperform_days(data["equity_week"], kospi)
    hit = f"{out_days}/{total_days}" if total_days > 0 else "N/A"

    lc = {"Outperform": "#2e7d32", "Underperform": "#d32f2f"}.get(label, "#78909c")

    cards = (
        _card("포트폴리오", _fp(wret), _color(wret)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret)) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>') +
        _card("Outperform 일수", hit, "#333", "일별 KOSPI 대비")
    )
    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_cost_weekly(data, config) -> str:
    """주간 비용 분석."""
    trades = data["trades"]
    if trades.empty or "cost" not in trades.columns:
        return _section("비용 분석",
            '<div style="color:#aaa;padding:12px;">주간 거래 비용 없음</div>')

    week_cost = float(pd.to_numeric(trades["cost"], errors="coerce").fillna(0).sum())

    # Cumulative from all trades
    report_dir = Path(data.get("_report_dir", "."))
    trades_all = _load_csv(report_dir / "trades.csv")
    cum_cost = 0.0
    if not trades_all.empty and "cost" in trades_all.columns:
        if "code" in trades_all.columns:
            trades_all = trades_all[trades_all["code"] != "REBALANCE"]
        cum_cost = float(pd.to_numeric(trades_all["cost"], errors="coerce").fillna(0).sum())

    _, end_eq, start_eq = compute_weekly_return(data)
    week_pnl = end_eq - start_eq
    ratio = (week_cost / abs(week_pnl) * 100) if week_pnl != 0 else 0

    cards = (
        _card("주간 비용", _fk(week_cost) + "원", "#333") +
        _card("누적 비용", _fk(cum_cost) + "원", "#333") +
        _card("비용/손익", f"{ratio:.1f}%",
              "#d32f2f" if ratio > 50 else "#333", "주간 비용 잠식률")
    )
    return _section("비용 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_conclusion(verdict, verdict_kr, vcolor, wret, stats, sys_stats):
    reasons = []

    if verdict == "STANDBY":
        reasons.append("주간 거래 없음 (전략 비활성 또는 초기 상태)")
    elif verdict == "MAINTAIN":
        reasons.append("주간 성과 양호, 시스템 안정")
    elif verdict == "WATCH":
        if wret <= -0.03:
            reasons.append(f"주간 수익률 저조 ({_fp(wret)})")
        if stats["n_trades"] > 0 and 0.30 <= stats["win_rate"] < 0.40:
            reasons.append(f"승률 경계 ({stats['win_rate']*100:.0f}%)")
        elif stats["n_trades"] == 0:
            reasons.append("거래 없음 (승률 평가 불가)")
        if sys_stats["total_errors"] > 0:
            reasons.append(f"시스템 에러 {sys_stats['total_errors']}건")
        if sys_stats["monitor_only_days"] > 0:
            reasons.append(f"Monitor-only {sys_stats['monitor_only_days']}일")
    else:  # REVIEW
        if wret <= -0.05:
            reasons.append(f"주간 대폭 손실 ({_fp(wret)})")
        if stats["n_trades"] > 0 and stats["win_rate"] < 0.30:
            reasons.append(f"승률 부진 ({stats['win_rate']*100:.0f}%)")

    bg_map = {"MAINTAIN": "#c8e6c9", "WATCH": "#fff9c4", "REVIEW": "#ffcdd2",
              "STANDBY": "#e3f2fd"}
    badge = (f'<span style="display:inline-block;padding:6px 20px;border-radius:12px;'
             f'font-size:16px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#f5f5f5")};">'
             f'{verdict_kr}</span>')

    reason_html = "<br>".join(f"- {r}" for r in reasons) if reasons else "특이사항 없음"

    return _section("결론",
        f'<div style="text-align:center;margin:8px 0 12px;">{badge}</div>'
        f'<div style="font-size:13px;line-height:1.7;padding:0 8px;">{reason_html}</div>')


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Assembly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_weekly_html(data, config) -> str:
    wret, _, _ = compute_weekly_return(data)
    vol = compute_weekly_volatility(data)
    weekly_dd = compute_weekly_dd(data)
    stats = compute_trade_stats(data)
    trail = compute_trail_stats(data)
    sys_stats = compute_system_stats(data)

    # End positions count for verdict
    end_pos = len(data["pos_end_codes"])

    verdict, verdict_kr, vcolor = compute_verdict_weekly(
        wret, stats["win_rate"], sys_stats["total_errors"],
        n_trades=stats["n_trades"], end_positions=end_pos,
        monitor_only_days=sys_stats["monitor_only_days"])

    sections = [
        build_summary(data, config, wret, verdict, verdict_kr, vcolor, stats),
        build_performance(data, config, wret, vol),
        build_market_comparison_weekly(data, config, wret),
        build_cost_weekly(data, config),
        build_trade_stats(stats),
        build_portfolio_changes(data),
        build_risk(data, wret, weekly_dd),
        build_strategy(data, trail, sys_stats),
        build_system_section(data),
        build_conclusion(verdict, verdict_kr, vcolor, wret, stats, sys_stats),
    ]

    ws = data["week_start"]
    we = data["week_end"]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(s for s in sections if s)

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gen4 Weekly Report — {ws} ~ {we}</title>
<style>
body {{ font-family: 'Malgun Gothic','Segoe UI',sans-serif; background:#f0f2f5;
       margin:0; padding:20px; color:#333; }}
.container {{ max-width:800px; margin:0 auto; }}
h1 {{ font-size:20px; color:#1a237e; margin-bottom:16px; }}
</style>
</head>
<body>
<div class="container">
<h1>Gen4 주간 보고서 — {ws} ~ {we}</h1>
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

def generate_weekly_report(report_dir: Path, config,
                            ref_date_str: str = "") -> Optional[Path]:
    ref = datetime.strptime(ref_date_str, "%Y-%m-%d").date() if ref_date_str else date.today()
    ws, we = get_week_range(ref)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    try:
        data = load_weekly_data(report_dir, ws, we, config.INITIAL_CASH)
        if hasattr(config, "INDEX_FILE") and config.INDEX_FILE.exists():
            data["_kospi"] = load_kospi_close(config.INDEX_FILE)
        html = generate_weekly_html(data, config)

        fname = f"weekly_{ws.replace('-', '')}.html"
        path = report_dir / fname
        path.write_text(html, encoding="utf-8")
        logger.info(f"Weekly report generated: {path}")
        return path
    except Exception as e:
        logger.error(f"Weekly report generation failed: {e}")
        return None


if __name__ == "__main__":
    import argparse
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import Gen4Config

    parser = argparse.ArgumentParser(description="Gen4 Weekly Report")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"),
                        help="Reference date (YYYY-MM-DD), report covers that week")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    config = Gen4Config()
    path = generate_weekly_report(config.REPORT_DIR, config, args.date)
    if path:
        print(f"Report: {path}")
    else:
        print("Report generation failed.")
        sys.exit(1)
