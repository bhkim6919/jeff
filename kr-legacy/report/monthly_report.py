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
from report.daily_report import resolve_stock_name

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
        return ("STANDBY", "대기", "#3B82F6")
    if mret > 0 and mdd > -0.15 and sharpe > 1.0:
        return ("EXPAND", "확대", "#10B981")
    if mret > -0.03 and mdd > -0.15:
        return ("MAINTAIN", "유지", "#10B981")
    if mret <= -0.03 or mdd <= -0.15:
        return ("REDUCE", "축소", "#EF4444")
    return ("MAINTAIN", "유지", "#10B981")


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
    """Korean convention: red for profit, blue for loss."""
    v = float(val)
    if v > 0:
        return "#FF4757"   # vivid red = profit (Korean convention)
    if v < 0:
        return "#3B82F6"   # blue = loss (Korean convention)
    return "#94A3B8"

def _card(title, value, color="#1E293B", sub=""):
    # Determine gradient border color based on value color
    grad_map = {
        "#FF4757": "linear-gradient(135deg, #FF4757, #FF6B81)",
        "#3B82F6": "linear-gradient(135deg, #3B82F6, #60A5FA)",
        "#10B981": "linear-gradient(135deg, #10B981, #34D399)",
        "#F59E0B": "linear-gradient(135deg, #F59E0B, #FBBF24)",
        "#EF4444": "linear-gradient(135deg, #EF4444, #F87171)",
        "#94A3B8": "linear-gradient(135deg, #94A3B8, #CBD5E1)",
    }
    grad = grad_map.get(color, f"linear-gradient(135deg, {color}, {color})")
    # Trend arrow based on value text
    arrow = ""
    try:
        v_str = value.replace("%", "").replace(",", "").replace("+", "").replace("원", "").strip()
        v_num = float(v_str)
        if v_num > 0:
            arrow = '<span style="font-size:14px;margin-left:4px;opacity:0.7;">&#9650;</span>'
        elif v_num < 0:
            arrow = '<span style="font-size:14px;margin-left:4px;opacity:0.7;">&#9660;</span>'
    except (ValueError, TypeError):
        pass
    return f"""<div class="g4-card" style="flex:1;min-width:150px;background:#FFFFFF;
        border-radius:12px;padding:20px 16px;
        box-shadow:0 2px 8px rgba(0,0,0,0.06);text-align:center;
        border-top:2px solid transparent;background-image:{grad};
        background-size:100% 2px;background-repeat:no-repeat;background-position:top;">
        <div style="font-size:11px;color:#64748B;margin-bottom:6px;
            text-transform:uppercase;letter-spacing:0.8px;font-weight:500;">{title}</div>
        <div style="font-size:28px;font-weight:700;color:{color};line-height:1.2;">{value}{arrow}</div>
        {f'<div style="font-size:11px;color:#94A3B8;margin-top:4px;">{sub}</div>' if sub else ''}
    </div>"""

def _section(title, content):
    return f"""<div class="g4-section" style="margin-bottom:28px;padding:20px 24px;
        background:#FFFFFF;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,0.04);
        border-left:3px solid #00B4D8;">
        <h2 style="font-size:15px;color:#1E293B;font-weight:600;
            margin:0 0 14px 0;padding:0;border:none;">{title}</h2>
        {content}
    </div>"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Section Builders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_basis_line_monthly(data: dict) -> str:
    """기준 시각 / 계산 기준 (월간)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ms, me = data["month_start"], data["month_end"]
    return (f'<div style="font-size:11px;color:#94A3B8;margin-bottom:12px;padding:0 4px;">'
            f'생성: {ts} KST | '
            f'월간 수익률 = (월말 EOD 총자산 / 월초 전일 종가 총자산) - 1 | '
            f'구간: {ms} ~ {me}</div>')


def build_monthly_verdict_summary(data, config, mret, verdict_kr, mdd, sharpe, stats):
    """월간 판단 요약."""
    _, end_eq, start_eq = compute_monthly_return(data)
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    # Calmar
    days = max(len(data["equity_month"]), 1)
    ann_ret = mret * (252.0 / days)
    calmar = ann_ret / abs(mdd) if mdd != 0 else 0

    lines = [
        f"월간 수익률: <b>{_fp(mret)}</b> (누적 {_fp(cum)})",
        f"MDD: <b>{_fp(mdd)}</b> | Sharpe: {sharpe:.2f} | Calmar: {calmar:.2f}",
        f"전략 상태: <b>{verdict_kr}</b>",
        f"거래: 청산 {stats['n_trades']}건 (승률 {stats['win_rate']*100:.0f}%), "
        f"매수 {stats['n_buys']}건 / 매도 {stats['n_sells']}건",
    ]

    has_issue = mret <= -0.03 or mdd <= -0.15
    bg = "rgba(245,158,11,0.06)" if has_issue else "rgba(16,185,129,0.06)"
    border = "#F59E0B" if has_issue else "#10B981"

    items = "".join(f'<div style="padding:2px 0;font-size:12px;">{l}</div>' for l in lines)
    return (f'<div class="g4-alert" style="background:{bg};border-left:4px solid {border};'
            f'border-radius:0 8px 8px 0;padding:10px 14px;margin-bottom:16px;">'
            f'<div style="font-size:11px;font-weight:700;color:#1E293B;margin-bottom:4px;">'
            f'월간 판단</div>'
            f'{items}</div>')


def build_risk_monthly(data, mdd):
    """월간 리스크 분석: MDD, 변동성, 연속 손실."""
    eq = data["equity_month"]

    # Volatility
    vol = 0.0
    if not eq.empty and "daily_pnl_pct" in eq.columns:
        pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
        if len(pnls) >= 2:
            vol = float(pnls.std())

    # Consecutive losses
    max_consec_loss = 0
    if not eq.empty and "daily_pnl_pct" in eq.columns:
        pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
        streak = 0
        for p in pnls:
            if p < 0:
                streak += 1
                max_consec_loss = max(max_consec_loss, streak)
            else:
                streak = 0

    # Overall MDD
    overall_mdd = 0.0
    eq_all = data.get("equity_all", pd.DataFrame())
    if not eq_all.empty and "equity" in eq_all.columns:
        eqs = pd.to_numeric(eq_all["equity"], errors="coerce").dropna()
        if len(eqs) > 0:
            peak = eqs.cummax()
            overall_mdd = float(((eqs - peak) / peak).min())

    cards = (
        _card("월간 MDD", _fp(mdd), _color(mdd)) +
        _card("전체 MDD", _fp(overall_mdd), _color(overall_mdd)) +
        _card("일별 변동성", f"{vol*100:.2f}%", "#1E293B", "수익률 std") +
        _card("연속 손실", f"{max_consec_loss}일",
              "#EF4444" if max_consec_loss >= 5 else "#1E293B")
    )
    return _section("리스크 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_pnl_attribution_monthly(data):
    """포지션 분석: top/bottom 기여."""
    pos = data["positions_end"]
    closes = data["closes"]

    parts = []

    # Closed position top/bottom
    if not closes.empty and "pnl_amount" in closes.columns:
        c = closes.copy()
        c["pnl_amount"] = pd.to_numeric(c["pnl_amount"], errors="coerce").fillna(0)
        top3 = c.nlargest(3, "pnl_amount")
        bot3 = c.nsmallest(3, "pnl_amount")

        def _fmt_stock(r):
            code = str(r.get("code", "")).zfill(6)
            name = resolve_stock_name(code)
            label = f"{name} ({code})" if name != code else code
            return f'{label}: {_fk(float(r["pnl_amount"]))}원 ({_fp(float(r.get("pnl_pct",0)))})'

        if not top3.empty:
            items = [_fmt_stock(r) for _, r in top3.iterrows()]
            parts.append(f'<div style="font-size:13px;padding:3px 0;">'
                         f'수익 기여 TOP3: {" / ".join(items)}</div>')
        if not bot3.empty:
            items = [_fmt_stock(r) for _, r in bot3.iterrows()]
            parts.append(f'<div style="font-size:13px;padding:3px 0;">'
                         f'손실 기여 TOP3: {" / ".join(items)}</div>')
    else:
        parts.append('<div style="color:#94A3B8;font-size:13px;">청산 데이터 없음</div>')

    # End-of-month position count
    n_end = len(pos) if not pos.empty else 0
    parts.append(f'<div style="font-size:13px;padding:3px 0;margin-top:4px;">'
                 f'월말 보유: {n_end}종목 / 목표 20종목</div>')

    return _section("포지션 기여 분석", "\n".join(parts))


def build_strategy_structure(data, mret, mdd):
    """전략 구조 분석: 상승/하락장 성과, 노출도, 종목 수 변화."""
    eq = data["equity_month"]
    lines = []

    if not eq.empty and "daily_pnl_pct" in eq.columns:
        pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
        up_days = pnls[pnls > 0]
        dn_days = pnls[pnls < 0]
        n_up = len(up_days)
        n_dn = len(dn_days)
        avg_up = float(up_days.mean()) if n_up > 0 else 0
        avg_dn = float(dn_days.mean()) if n_dn > 0 else 0

        lines.append(f"상승일 {n_up}일 (평균 {_fp(avg_up)}) / "
                     f"하락일 {n_dn}일 (평균 {_fp(avg_dn)})")

    # Position count range
    if not eq.empty and "n_positions" in eq.columns:
        n_pos = pd.to_numeric(eq["n_positions"], errors="coerce").dropna()
        if len(n_pos) > 0:
            lines.append(f"보유 종목 수: {int(n_pos.min())}~{int(n_pos.max())} "
                         f"(평균 {n_pos.mean():.0f})")

    # Exposure (invested vs cash ratio at end)
    pos = data["positions_end"]
    if not pos.empty and "market_value" in pos.columns:
        mv_total = pd.to_numeric(pos["market_value"], errors="coerce").sum()
        mret_end, end_eq, _ = compute_monthly_return(data)
        if end_eq > 0:
            exposure = mv_total / end_eq * 100
            lines.append(f"월말 노출도: {exposure:.0f}% (현금 {100-exposure:.0f}%)")

    if not lines:
        lines.append("전략 구조 데이터 부족")

    items = "".join(f'<div style="font-size:13px;padding:3px 0;">{l}</div>' for l in lines)
    return _section("전략 구조 분석", items)


def build_issues(data, mret, mdd, sharpe, stats, sys):
    """문제점: 리스크 패턴, 전략 약점."""
    issues = []

    if mdd <= -0.15:
        issues.append(f"MDD {_fp(mdd)} — 과도한 낙폭, DD guard 설정 재검토")
    if mret <= -0.03:
        issues.append(f"월간 손실 {_fp(mret)} — 전략 유효성 점검")
    if sharpe < 0.5 and stats["n_trades"] > 0:
        issues.append(f"Sharpe {sharpe:.2f} — 리스크 대비 수익 낮음")
    if stats["win_rate"] < 0.3 and stats["n_trades"] >= 5:
        issues.append(f"승률 {stats['win_rate']*100:.0f}% — trail stop 빈도 점검")

    pf = sys.get("price_fail_total", 0)
    rc = sys.get("reconcile_total", 0)
    if pf > 5:
        issues.append(f"가격 실패 {pf}건 — 데이터 소스 안정성 점검")
    if rc > 5:
        issues.append(f"Broker 보정 {rc}건 — 체결 동기화 이슈")

    if not issues:
        return _section("문제점",
            '<div style="color:#10B981;padding:8px;font-size:13px;">특이사항 없음</div>')

    items = ""
    for issue in issues:
        items += (f'<div class="g4-alert" style="background:rgba(245,158,11,0.06);'
                  f'border-left:4px solid #F59E0B;'
                  f'padding:8px 12px;margin-bottom:6px;font-size:13px;'
                  f'border-radius:0 8px 8px 0;">{issue}</div>')
    return _section("문제점", items)


def build_next_direction(verdict, verdict_kr, vcolor, mret, mdd, sharpe, stats):
    """다음 전략 방향."""
    actions = {
        "EXPAND": [
            "투자 비중 확대 검토 가능",
            "리밸런싱 정상 운용 유지",
            "수익 고점 갱신 시 trailing 기준 재확인",
        ],
        "MAINTAIN": [
            "현행 전략 유지",
            "DD guard 기준 모니터링 지속",
        ],
        "REDUCE": [
            "투자 비중 축소 검토",
            "Trail stop 기준 재검토 (-12% 적정 여부)",
            "모멘텀 윈도우 변경 시뮬레이션 고려",
        ],
        "STANDBY": [
            "전략 활성화 조건 확인",
            "유니버스 및 데이터 정합성 점검",
        ],
    }
    action_list = actions.get(verdict, actions["MAINTAIN"])

    items = "".join(
        f'<div style="padding:3px 0;font-size:13px;">'
        f'<span style="color:#00B4D8;margin-right:6px;">→</span>{a}</div>'
        for a in action_list
    )

    bg_map = {"EXPAND": "rgba(16,185,129,0.1)", "MAINTAIN": "rgba(16,185,129,0.1)",
              "REDUCE": "rgba(239,68,68,0.1)", "STANDBY": "rgba(59,130,246,0.1)"}
    badge = (f'<div style="text-align:center;margin-bottom:12px;">'
             f'<span style="display:inline-block;padding:8px 24px;border-radius:12px;'
             f'font-size:18px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#F8FAFC")};">'
             f'{verdict_kr}</span></div>')

    return _section("다음 전략 방향", badge + items)


def _get_system_stats_monthly(data):
    """월간 시스템 통계 dict."""
    eq = data["equity_month"]
    recon = data["reconciles"]
    pf = int(pd.to_numeric(eq["price_fail_count"], errors="coerce").sum()) if not eq.empty and "price_fail_count" in eq.columns else 0
    rc = int(pd.to_numeric(eq["reconcile_corrections"], errors="coerce").sum()) if not eq.empty and "reconcile_corrections" in eq.columns else 0
    recon_n = len(recon)
    return {"price_fail_total": pf, "reconcile_total": rc + recon_n}


def build_summary(data, config, mret, verdict, verdict_kr, vcolor, stats, mdd):
    _, end_eq, start_eq = compute_monthly_return(data)
    delta = end_eq - start_eq
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    lines = [
        f"월간 수익률 <b>{_fp(mret)}</b>, 자산 변동 <b>{_fk(delta)}</b>원",
        f"총 자산 <b>{_fk(end_eq)}</b>원 (누적 {_fp(cum)})",
        f"MDD {_fp(mdd)}, 거래 {stats['n_trades']}건",
    ]

    bg_map = {"EXPAND": "rgba(16,185,129,0.1)", "MAINTAIN": "rgba(16,185,129,0.1)",
              "REDUCE": "rgba(239,68,68,0.1)", "STANDBY": "rgba(59,130,246,0.1)"}
    badge = (f'<span style="display:inline-block;padding:4px 14px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#F8FAFC")};">'
             f'{verdict_kr}</span>')

    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#F8FAFC;border-radius:12px;padding:16px 20px;margin-bottom:20px;
        border:1px solid #E2E8F0;">
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
              "#10B981" if sharpe > 1 else "#EF4444" if sharpe < 0.5 else "#1E293B",
              "연환산") +
        _card("Calmar", f"{calmar:.2f}",
              "#10B981" if calmar > 1 else "#1E293B")
    )
    return _section("성과 지표",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_market_comparison(data, config, mret):
    kospi = data.get("_kospi", pd.Series(dtype=float))
    ms, me = data["month_start"], data["month_end"]
    k_ret = get_kospi_period_return(kospi, ms, me)

    if k_ret is None:
        return _section("시장 대비 성과",
            '<div style="padding:12px;">'
            '<div style="color:#94A3B8;font-size:13px;">시장 비교 데이터 부재</div>'
            '<div style="color:#64748B;font-size:12px;margin-top:4px;">'
            '상대성과 평가 보류 — 절대수익/내부 리스크 기준으로만 해석 필요</div>'
            '</div>')

    excess, label = compute_excess_return(mret, k_ret)
    out_days, total_days = count_outperform_days(data["equity_month"], kospi)
    hit = f"{out_days}/{total_days}" if total_days > 0 else "N/A"

    lc = {"Outperform": "#10B981", "Underperform": "#EF4444"}.get(label, "#94A3B8")

    cards = (
        _card("포트폴리오", _fp(mret), _color(mret)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret)) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>') +
        _card("Outperform", hit, "#1E293B", "일별 기준")
    )
    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_trade_stats(stats):
    if stats["n_trades"] == 0 and stats["n_buys"] == 0:
        return _section("거래 통계",
            '<div style="color:#94A3B8;padding:12px;">월간 거래 없음</div>')

    nt = stats["n_trades"]
    th = 'style="text-align:left;padding:8px;border-bottom:1px solid #E2E8F0;font-size:12px;color:#64748B;"'
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
        f'<div style="border-radius:8px;overflow:hidden;border:1px solid #E2E8F0;">'
        f'<table style="width:100%;border-collapse:collapse;">{trs}</table></div>')


def build_cost(data, config, mret):
    costs = compute_cost_monthly(data)

    _, end_eq, start_eq = compute_monthly_return(data)
    month_pnl = end_eq - start_eq

    # Cost impact: return before vs after cost
    ret_pre_cost = mret + (costs["month_cost"] / start_eq) if start_eq > 0 else mret
    ratio = (costs["month_cost"] / abs(month_pnl) * 100) if month_pnl != 0 else 0

    cards = (
        _card("월간 비용", _fk(costs["month_cost"]) + "원", "#1E293B") +
        _card("누적 비용", _fk(costs["cum_cost"]) + "원", "#1E293B") +
        _card("비용 전 수익률", _fp(ret_pre_cost), _color(ret_pre_cost), "비용 차감 전") +
        _card("비용/손익", f"{ratio:.1f}%",
              "#EF4444" if ratio > 30 else "#1E293B")
    )
    return _section("비용 영향도",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_turnover(data, stats):
    to = compute_turnover(data)
    n_end = len(data["positions_end"]) if not data["positions_end"].empty else 0

    cards = (
        _card("Turnover", f"{to*100:.0f}%", "#1E293B",
              "월간 교체 종목 / 보유 종목") +
        _card("월말 포지션", f"{n_end}종목", "#1E293B", "목표: 20종목") +
        _card("월간 청산", f'{stats["n_trades"]}건', "#1E293B")
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
        _card("가격 실패", f"{pf}건", "#EF4444" if pf > 0 else "#10B981") +
        _card("Broker 보정", f"{rc + recon_n}건", "#EF4444" if (rc + recon_n) > 0 else "#10B981") +
        _card("Monitor Only", f"{mo}일", "#EF4444" if mo > 0 else "#10B981") +
        _card("리밸런스", f"{rebal}회", "#1E293B") +
        _card("Critical Error", f"{forensic_count}건", "#EF4444" if forensic_count > 0 else "#10B981")
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

    bg_map = {"EXPAND": "rgba(16,185,129,0.1)", "MAINTAIN": "rgba(16,185,129,0.1)",
              "REDUCE": "rgba(239,68,68,0.1)", "STANDBY": "rgba(59,130,246,0.1)"}
    badge = (f'<div style="text-align:center;margin:8px 0 16px;">'
             f'<span style="display:inline-block;padding:8px 24px;border-radius:12px;'
             f'font-size:18px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#F8FAFC")};">'
             f'{verdict_kr}</span></div>')

    reason_html = "<br>".join(f"- {r}" for r in reasons)
    action_html = (f'<div style="font-size:14px;text-align:center;padding:8px;'
                   f'background:#F8FAFC;border-radius:8px;margin-top:8px;">'
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

    sys_stats = _get_system_stats_monthly(data)

    sections = [
        build_summary(data, config, mret, verdict, verdict_kr, vcolor, stats, mdd),
        build_basis_line_monthly(data),                   # 기준 시각/계산 기준
        build_monthly_verdict_summary(data, config, mret, verdict_kr, mdd, sharpe, stats),  # 월간 판단
        build_performance(data, config, mret, mdd, sharpe),
        build_risk_monthly(data, mdd),                    # 리스크 분석 (보강)
        build_market_comparison(data, config, mret),
        build_strategy_structure(data, mret, mdd),        # 전략 구조 분석 (신규)
        build_cost(data, config, mret),
        build_trade_stats(stats),
        build_pnl_attribution_monthly(data),              # 포지션 기여 (신규)
        build_turnover(data, stats),
        build_system(data),
        build_issues(data, mret, mdd, sharpe, stats, sys_stats),  # 문제점 (신규)
        build_next_direction(verdict, verdict_kr, vcolor, mret, mdd, sharpe, stats),  # 다음 전략 (신규)
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
<title>Q-TRON Gen4 Monthly Report — {month_label}</title>
<style>
:root {{
    --primary: #0A1628;
    --accent: #00B4D8;
    --profit: #FF4757;
    --loss: #3B82F6;
    --success: #10B981;
    --warning: #F59E0B;
    --danger: #EF4444;
    --surface: #FFFFFF;
    --surface-alt: #F8FAFC;
    --text: #1E293B;
    --text-dim: #94A3B8;
    --border: #E2E8F0;
}}
* {{ box-sizing: border-box; }}
body {{
    font-family: 'Pretendard','Malgun Gothic',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
    background: #F1F5F9;
    margin: 0; padding: 0; color: var(--text); line-height: 1.6;
}}
.g4-header {{
    background: linear-gradient(135deg, #0A1628 0%, #1E3A5F 100%);
    padding: 32px 24px 28px;
    text-align: center;
    position: relative;
    overflow: hidden;
}}
.g4-header::before {{
    content: '';
    position: absolute; top: 0; left: 0; right: 0; bottom: 0;
    background: radial-gradient(circle at 20% 50%, rgba(0,180,216,0.12) 0%, transparent 50%),
                radial-gradient(circle at 80% 50%, rgba(255,71,87,0.08) 0%, transparent 50%);
    pointer-events: none;
}}
.g4-header h1 {{
    font-size: 24px; color: #FFFFFF; margin: 0 0 6px 0; font-weight: 700;
    letter-spacing: -0.3px; position: relative;
}}
.g4-header .g4-brand {{
    font-size: 11px; color: rgba(255,255,255,0.5); letter-spacing: 2px;
    text-transform: uppercase; margin-bottom: 8px; position: relative;
}}
.g4-header .g4-date {{
    font-size: 14px; color: rgba(255,255,255,0.7); position: relative;
}}
.container {{
    max-width: 960px; margin: -20px auto 0; padding: 0 16px 32px;
    position: relative; z-index: 1;
}}
/* Section styling */
.g4-section {{
    animation: g4FadeIn 0.35s ease-out;
}}
@keyframes g4FadeIn {{
    from {{ opacity: 0; transform: translateY(8px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
/* Card hover */
.g4-card {{
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    cursor: default;
}}
.g4-card:hover {{
    transform: translateY(-3px);
    box-shadow: 0 8px 24px rgba(0,0,0,0.1) !important;
}}
/* Table styling */
table {{
    border-radius: 8px;
    overflow: hidden;
}}
table thead tr {{
    background: #F1F5F9;
}}
table thead th {{
    color: #475569;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.3px;
}}
table tbody tr {{
    transition: background 0.15s ease;
}}
table tbody tr:nth-child(even) {{
    background: #F8FAFC;
}}
table tbody tr:nth-child(odd) {{
    background: #FFFFFF;
}}
table tbody tr:hover {{
    background: #EFF6FF !important;
}}
/* Alert box hover */
.g4-alert {{
    transition: box-shadow 0.18s ease, transform 0.18s ease;
    border-radius: 0 8px 8px 0;
}}
.g4-alert:hover {{
    box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    transform: translateX(2px);
}}
/* Footer */
.g4-footer {{
    text-align: center;
    font-size: 11px;
    color: var(--text-dim);
    margin-top: 32px;
    padding: 16px 0;
    border-top: 1px solid var(--border);
}}
.g4-footer a {{
    color: var(--accent);
    text-decoration: none;
}}
/* Responsive: Mobile */
@media (max-width: 640px) {{
    .g4-header {{ padding: 24px 16px 20px; }}
    .g4-header h1 {{ font-size: 18px; }}
    .container {{ padding: 0 8px 24px; margin-top: -12px; }}
    .g4-section {{ padding: 14px 12px !important; }}
    .g4-card {{ min-width: 120px !important; padding: 12px 10px !important; }}
    .g4-card div:nth-child(2) {{ font-size: 22px !important; }}
    table {{ font-size: 11px !important; }}
    table th, table td {{ padding: 4px 6px !important; }}
}}
/* Print-friendly */
@media print {{
    body {{ background: #fff !important; padding: 0 !important; }}
    .g4-header {{
        background: #0A1628 !important;
        -webkit-print-color-adjust: exact;
        print-color-adjust: exact;
    }}
    .container {{ max-width: 100%; margin: 0; padding: 0; }}
    .g4-section {{
        box-shadow: none !important;
        border: 1px solid #E2E8F0;
        break-inside: avoid;
        page-break-inside: avoid;
    }}
    .g4-card {{
        box-shadow: none !important;
        border: 1px solid #E2E8F0;
    }}
    .g4-card:hover, .g4-alert:hover {{
        transform: none !important;
        box-shadow: none !important;
    }}
    table tbody tr:hover {{ background: inherit !important; }}
    .g4-footer {{ margin-top: 16px; }}
    @page {{ margin: 1cm; }}
}}
</style>
</head>
<body>
<div class="g4-header">
    <div class="g4-brand">Q-TRON GEN4</div>
    <h1>월간 보고서 — {month_label}</h1>
    <div class="g4-date">{ms} ~ {me}</div>
</div>
<div class="container">
{body}
<div class="g4-footer">
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
