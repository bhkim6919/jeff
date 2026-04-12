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
        return ("STANDBY", "대기", "#3B82F6")
    if sys_errors > 0 or monitor_only_days > 0:
        return ("WATCH", "관찰 필요", "#F59E0B")
    if weekly_ret <= -0.05 or (n_trades > 0 and win_rate < 0.30):
        return ("REVIEW", "전략 점검", "#EF4444")
    if weekly_ret <= -0.03 or (n_trades > 0 and 0.30 <= win_rate < 0.40):
        return ("WATCH", "관찰 필요", "#F59E0B")
    return ("MAINTAIN", "정상 유지", "#10B981")


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

def build_basis_line_weekly(data: dict) -> str:
    """기준 시각 / 계산 기준 (주간)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ws, we = data["week_start"], data["week_end"]
    return (f'<div style="font-size:11px;color:#94A3B8;margin-bottom:12px;padding:0 4px;">'
            f'생성: {ts} KST | '
            f'주간 수익률 = (주말 EOD 총자산 / 주초 전일 종가 총자산) - 1 | '
            f'구간: {ws} ~ {we}</div>')


def build_weekly_verdict_summary(data, config, wret, verdict_kr, stats, sys_stats, weekly_dd):
    """주간 판단 5줄 요약."""
    _, end_eq, start_eq = compute_weekly_return(data)

    # Line 1: 주간 수익률
    line1 = f"주간 수익률: <b>{_fp(wret)}</b> (자산 {_fk(end_eq)}원)"
    # Line 2: MDD
    line2 = f"주간 MDD: <b>{_fp(weekly_dd)}</b>"
    # Line 3: 전략 상태
    line3 = f"전략 상태: <b>{verdict_kr}</b>"
    # Line 4: 핵심 특징
    features = []
    if stats["n_trades"] > 0:
        features.append(f"청산 {stats['n_trades']}건 (승률 {stats['win_rate']*100:.0f}%)")
    if sys_stats["rebalance_count"] > 0:
        features.append(f"리밸 {sys_stats['rebalance_count']}회")
    trail = compute_trail_stats(data)
    if trail["trail_count"] > 0:
        features.append(f"Trail {trail['trail_count']}건")
    line4 = f"핵심 특징: {', '.join(features)}" if features else "핵심 특징: 특이사항 없음"
    # Line 5: 운영 안정성
    errors = sys_stats["total_errors"]
    mo = sys_stats["monitor_only_days"]
    stab = "안정" if errors == 0 and mo == 0 else f"점검 필요 (에러 {errors}, MO {mo}일)"
    line5 = f"운영 안정성: {stab}"

    lines = [line1, line2, line3, line4, line5]
    has_issue = errors > 0 or mo > 0 or wret <= -0.03
    bg = "rgba(245,158,11,0.06)" if has_issue else "rgba(16,185,129,0.06)"
    border = "#F59E0B" if has_issue else "#10B981"

    items = "".join(f'<div style="padding:2px 0;font-size:12px;">{l}</div>' for l in lines)
    return (f'<div class="g4-alert" style="background:{bg};border-left:4px solid {border};'
            f'border-radius:0 8px 8px 0;padding:12px 16px;margin-bottom:16px;">'
            f'<div style="font-size:11px;font-weight:700;color:#1E293B;margin-bottom:4px;'
            f'text-transform:uppercase;letter-spacing:0.5px;">'
            f'주간 판단</div>'
            f'{items}</div>')


def build_risk_weekly(data, wret, weekly_dd):
    """주간 리스크 분석 (MDD, avg DD, intraday DD 평균 포함)."""
    # Overall MDD
    eq_all = data["equity_all"]
    mdd = 0.0
    if not eq_all.empty and "equity" in eq_all.columns:
        eqs = pd.to_numeric(eq_all["equity"], errors="coerce").dropna()
        if len(eqs) > 0:
            peak = eqs.cummax()
            mdd = float(((eqs - peak) / peak).min())

    # Average daily DD during the week
    eq = data["equity_week"]
    avg_dd = 0.0
    if not eq.empty and "daily_pnl_pct" in eq.columns:
        pnls = pd.to_numeric(eq["daily_pnl_pct"], errors="coerce").dropna()
        neg = pnls[pnls < 0]
        avg_dd = float(neg.mean()) if len(neg) > 0 else 0.0

    # Recovery check
    _, end_eq, start_eq = compute_weekly_return(data)
    recovered = "회복" if end_eq >= start_eq else "미회복"
    rec_color = "#10B981" if end_eq >= start_eq else "#EF4444"

    cards = (
        _card("주간 MDD", _fp(weekly_dd), _color(weekly_dd), "peak→trough") +
        _card("전체 MDD", _fp(mdd), _color(mdd)) +
        _card("평균 하락일 DD", _fp(avg_dd) if avg_dd != 0 else "N/A",
              _color(avg_dd) if avg_dd != 0 else "#94A3B8") +
        _card("DD 회복", recovered, rec_color)
    )
    return _section("리스크 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_position_analysis(data, stats):
    """포지션 분석: 평균 보유기간, 청산 사유 분포, 신규 vs 청산."""
    start = data["pos_start_codes"]
    end = data["pos_end_codes"]
    added = len(end - start)
    removed = len(start - end)

    # Exit reason breakdown
    closes = data["closes"]
    reason_counts = {}
    if not closes.empty and "exit_reason" in closes.columns:
        for reason in closes["exit_reason"]:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1

    items = [
        f'<div style="font-size:13px;padding:3px 0;">주초 {len(start)}종목 → '
        f'주말 {len(end)}종목 (편입 +{added}, 제거 -{removed})</div>',
    ]

    if stats["n_trades"] > 0:
        items.append(f'<div style="font-size:13px;padding:3px 0;">'
                     f'평균 보유기간: <b>{stats["avg_hold"]:.0f}일</b></div>')

    if reason_counts:
        parts = [f'{k}: {v}건' for k, v in sorted(reason_counts.items(),
                                                    key=lambda x: -x[1])]
        items.append(f'<div style="font-size:13px;padding:3px 0;">'
                     f'청산 사유: {", ".join(parts)}</div>')

    return _section("포지션 분석", "\n".join(items))


def build_anomaly_weekly(data, sys_stats):
    """주간 이상 탐지."""
    alerts = []
    wret, _, _ = compute_weekly_return(data)
    weekly_dd = compute_weekly_dd(data)

    if weekly_dd <= -0.05:
        alerts.append(("HIGH", f"주간 MDD {_fp(weekly_dd)} — 대폭 낙폭"))
    elif weekly_dd <= -0.03:
        alerts.append(("MED", f"주간 MDD {_fp(weekly_dd)} — 주의"))
    if wret <= -0.05:
        alerts.append(("HIGH", f"주간 수익률 {_fp(wret)} — 전략 점검 필요"))
    if sys_stats["price_fail_total"] > 3:
        alerts.append(("MED", f"가격 실패 누적 {sys_stats['price_fail_total']}건"))
    if sys_stats["monitor_only_days"] > 0:
        alerts.append(("MED", f"Monitor-only {sys_stats['monitor_only_days']}일"))
    if sys_stats["reconcile_total"] > 3:
        alerts.append(("MED", f"RECON 보정 {sys_stats['reconcile_total']}건"))

    if not alerts:
        return _section("이상 탐지",
            '<div style="color:#10B981;padding:8px;font-size:13px;">이상 없음</div>')

    items = ""
    for severity, msg in alerts:
        if severity == "HIGH":
            bg, border, icon = "rgba(239,68,68,0.06)", "#EF4444", "!!"
        else:
            bg, border, icon = "rgba(245,158,11,0.06)", "#F59E0B", "!"
        items += (f'<div class="g4-alert" style="background:{bg};border-left:4px solid {border};'
                  f'padding:8px 12px;margin-bottom:6px;font-size:13px;'
                  f'border-radius:0 8px 8px 0;">'
                  f'<span style="font-weight:700;color:{border};margin-right:6px;">'
                  f'{icon}</span>{msg}</div>')

    return _section("이상 탐지", items)


def build_strategy_eval(data, wret, stats, sys_stats):
    """전략 평가 (자동 문장)."""
    lines = []

    # 시장 대응
    if wret > 0.02:
        lines.append("주간 수익률 양호 — 전략이 시장 환경에 적절히 대응")
    elif wret > -0.02:
        lines.append("주간 수익률 보합 — 전략 방어적 운용 상태")
    else:
        lines.append("주간 수익률 부진 — 시장 환경 또는 전략 적합성 점검 필요")

    # 전략 일관성
    if stats["n_trades"] > 0 and stats["win_rate"] >= 0.5:
        lines.append(f"승률 {stats['win_rate']*100:.0f}% — 전략 일관성 유지")
    elif stats["n_trades"] > 0 and stats["win_rate"] >= 0.3:
        lines.append(f"승률 {stats['win_rate']*100:.0f}% — 전략 약세, 관찰 필요")
    elif stats["n_trades"] > 0:
        lines.append(f"승률 {stats['win_rate']*100:.0f}% — 전략 부진, 점검 필요")
    else:
        lines.append("청산 없음 — 보유 유지 상태")

    # 운영 안정성
    if sys_stats["total_errors"] == 0:
        lines.append("운영 안정 — 시스템 오류 없음")
    else:
        lines.append(f"운영 주의 — 에러 {sys_stats['total_errors']}건 발생")

    items = "".join(f'<div style="padding:3px 0;font-size:13px;">{l}</div>' for l in lines)
    return _section("전략 평가", f"""
        <div class="g4-alert" style="background:rgba(0,180,216,0.06);border-left:4px solid #00B4D8;
            border-radius:0 8px 8px 0;padding:14px 18px;">
            {items}
        </div>""")


def build_next_week_action(data, verdict, verdict_kr, wret, stats, sys_stats):
    """다음 주 액션 섹션."""
    actions = []

    if verdict == "REVIEW":
        actions.append("전략 파라미터 재검토 (모멘텀 윈도우, 변동성 필터)")
        actions.append("리밸런싱 결과 면밀히 모니터링")
    elif verdict == "WATCH":
        actions.append("일별 DD 추이 집중 관찰")
        if sys_stats["total_errors"] > 0:
            actions.append("시스템 로그 점검 및 에러 원인 파악")
    elif verdict == "MAINTAIN":
        actions.append("현행 전략 유지")
    else:  # STANDBY
        actions.append("전략 활성화 조건 확인")

    if stats["n_trades"] > 0 and stats["win_rate"] < 0.4:
        actions.append("청산 종목 패턴 분석 (trail 비중 vs rebal 비중)")

    trail = compute_trail_stats(data)
    if trail["trail_count"] >= 3:
        actions.append("Trail stop 빈도 확인 — 종목 수 감소 위험")

    items = "".join(
        f'<div style="padding:3px 0;font-size:13px;">'
        f'<span style="color:#00B4D8;margin-right:6px;">→</span>{a}</div>'
        for a in actions
    )

    color_map = {"MAINTAIN": "#10B981", "WATCH": "#F59E0B",
                 "REVIEW": "#EF4444", "STANDBY": "#3B82F6"}
    badge_bg = color_map.get(verdict, "#94A3B8")
    badge = (f'<span style="display:inline-block;padding:6px 16px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:#fff;'
             f'background:{badge_bg};">'
             f'{verdict_kr}</span>')

    return _section("다음 주 액션",
        f'<div style="text-align:center;margin-bottom:12px;">{badge}</div>'
        + items)


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

    bg_map = {"MAINTAIN": "rgba(16,185,129,0.1)", "WATCH": "rgba(245,158,11,0.1)",
              "REVIEW": "rgba(239,68,68,0.1)", "STANDBY": "rgba(59,130,246,0.1)"}
    badge = (f'<span style="display:inline-block;padding:4px 14px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#F8FAFC")};">'
             f'{verdict_kr}</span>')

    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#F8FAFC;border-radius:12px;padding:20px;margin-bottom:20px;
        box-shadow:0 1px 4px rgba(0,0,0,0.04);">
        <div style="font-size:14px;line-height:1.7;color:#1E293B;">{"<br>".join(lines)}</div>
        <div>{badge}</div>
    </div>"""


def build_performance(data, config, wret, vol):
    _, end_eq, _ = compute_weekly_return(data)
    cum = (end_eq / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    cards = (
        _card("주간 수익률", _fp(wret), _color(wret)) +
        _card("주간 변동성", f"{vol*100:.2f}%", "#1E293B", "일별 수익률 std") +
        _card("누적 수익률", _fp(cum), _color(cum),
              f"기준 {_fk(data['initial_cash'])}") +
        _card("총 자산", _fk(end_eq), "#1E293B")
    )

    # Daily returns list
    eq = data["equity_week"]
    daily_list = ""
    if not eq.empty and "date" in eq.columns and "daily_pnl_pct" in eq.columns:
        rows = ""
        for _, r in eq.iterrows():
            p = float(r.get("daily_pnl_pct", 0))
            chip_bg = "rgba(255,71,87,0.08)" if p >= 0 else "rgba(59,130,246,0.08)"
            rows += (f'<span style="display:inline-block;margin:2px 4px;padding:3px 10px;'
                     f'border-radius:6px;font-size:12px;font-weight:500;'
                     f'background:{chip_bg};'
                     f'color:{_color(p)};">'
                     f'{r["date"][-5:]} {_fp(p)}</span>')
        daily_list = f'<div style="margin-top:8px;">{rows}</div>'

    return _section("주간 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>{daily_list}')


def build_trade_stats(stats):
    if stats["n_trades"] == 0 and stats["n_buys"] == 0:
        return _section("거래 통계",
            '<div style="color:#94A3B8;padding:12px;">주간 거래 없음</div>')

    th = 'style="text-align:left;padding:10px 12px;font-size:12px;color:#64748B;font-weight:500;"'
    td = 'style="padding:10px 12px;text-align:right;font-size:14px;font-weight:600;"'

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
        f'<div style="border-radius:8px;overflow:hidden;border:1px solid #E2E8F0;">'
        f'<table style="width:100%;border-collapse:collapse;">{trs}</table></div>')


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
                     f'<span style="color:#94A3B8;">유지:</span> '
                     f'{", ".join(kept)}</div>')
    if added:
        parts.append(f'<div style="margin-top:6px;font-size:13px;">'
                     f'<span style="color:#3B82F6;">신규 편입:</span> '
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
                     f'<span style="color:#EF4444;">제거:</span> '
                     f'{", ".join(reason_parts)}</div>')

    # Closed positions (from close_log, may include mid-week closes not in position diff)
    if not closes.empty and "code" in closes.columns:
        th = 'style="text-align:left;padding:8px;font-size:11px;color:#64748B;font-weight:600;"'
        rows = ""
        for _, r in closes.iterrows():
            pnl = float(r.get("pnl_pct", 0))
            rows += (f'<tr><td style="padding:4px 8px;">{r.get("code","")}</td>'
                     f'<td style="padding:4px 8px;">{r.get("exit_reason","")}</td>'
                     f'<td style="padding:4px 8px;text-align:right;color:{_color(pnl)};">'
                     f'{_fp(pnl)}</td>'
                     f'<td style="padding:4px 8px;text-align:right;">{r.get("hold_days",0)}일</td></tr>')
        parts.append(f'<div style="margin-top:10px;font-size:12px;color:#64748B;">주간 청산 종목</div>'
                     f'<div style="border-radius:8px;overflow:hidden;border:1px solid #E2E8F0;">'
                     f'<table style="width:100%;border-collapse:collapse;font-size:13px;">'
                     f'<tr><th {th}>종목</th><th {th}>사유</th>'
                     f'<th {th} style="text-align:right;">손익률</th>'
                     f'<th {th} style="text-align:right;">보유</th></tr>'
                     f'{rows}</table></div>')

    if not start and not end and closes.empty:
        parts = ['<div style="color:#94A3B8;padding:12px;">포지션 데이터 없음</div>']

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
    rec_color = "#10B981" if end_eq >= start_eq else "#EF4444"

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
              "#EF4444" if sys["price_fail_total"] > 0 else "#10B981") +
        _card("Broker 보정", f'{sys["reconcile_total"]}건',
              "#EF4444" if sys["reconcile_total"] > 0 else "#10B981") +
        _card("Monitor Only", f'{sys["monitor_only_days"]}일',
              "#EF4444" if sys["monitor_only_days"] > 0 else "#10B981") +
        _card("리밸런스", f'{sys["rebalance_count"]}회', "#1E293B")
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
            '<div style="padding:12px;">'
            '<div style="color:#94A3B8;font-size:13px;">시장 비교 데이터 부재</div>'
            '<div style="color:#64748B;font-size:12px;margin-top:4px;">'
            '상대성과 평가 보류 — 절대수익/내부 리스크 기준으로만 해석 필요</div>'
            '</div>')

    excess, label = compute_excess_return(wret, k_ret)
    out_days, total_days = count_outperform_days(data["equity_week"], kospi)
    hit = f"{out_days}/{total_days}" if total_days > 0 else "N/A"

    lc = {"Outperform": "#10B981", "Underperform": "#EF4444"}.get(label, "#94A3B8")

    cards = (
        _card("포트폴리오", _fp(wret), _color(wret)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret)) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>') +
        _card("Outperform 일수", hit, "#1E293B", "일별 KOSPI 대비")
    )
    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_cost_weekly(data, config) -> str:
    """주간 비용 분석."""
    trades = data["trades"]
    if trades.empty or "cost" not in trades.columns:
        return _section("비용 분석",
            '<div style="color:#94A3B8;padding:12px;">주간 거래 비용 없음</div>')

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
        _card("주간 비용", _fk(week_cost) + "원", "#1E293B") +
        _card("누적 비용", _fk(cum_cost) + "원", "#1E293B") +
        _card("비용/손익", f"{ratio:.1f}%",
              "#EF4444" if ratio > 50 else "#1E293B", "주간 비용 잠식률")
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

    bg_map = {"MAINTAIN": "rgba(16,185,129,0.1)", "WATCH": "rgba(245,158,11,0.1)",
              "REVIEW": "rgba(239,68,68,0.1)", "STANDBY": "rgba(59,130,246,0.1)"}
    badge = (f'<span style="display:inline-block;padding:6px 20px;border-radius:12px;'
             f'font-size:16px;font-weight:700;color:{vcolor};'
             f'background:{bg_map.get(verdict, "#F8FAFC")};">'
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
        build_basis_line_weekly(data),                   # 기준 시각/계산 기준
        build_weekly_verdict_summary(data, config, wret, verdict_kr, stats, sys_stats, weekly_dd),  # 주간 판단 5줄
        build_performance(data, config, wret, vol),
        build_risk_weekly(data, wret, weekly_dd),        # 리스크 분석 (보강)
        build_market_comparison_weekly(data, config, wret),
        build_cost_weekly(data, config),
        build_trade_stats(stats),
        build_position_analysis(data, stats),            # 포지션 분석 (신규)
        build_portfolio_changes(data),
        build_system_section(data),
        build_anomaly_weekly(data, sys_stats),            # 이상 탐지 (신규)
        build_strategy(data, trail, sys_stats),
        build_strategy_eval(data, wret, stats, sys_stats),  # 전략 평가 (신규)
        build_next_week_action(data, verdict, verdict_kr, wret, stats, sys_stats),  # 다음 주 액션 (신규)
        build_conclusion(verdict, verdict_kr, vcolor, wret, stats, sys_stats),
    ]

    ws = data["week_start"]
    we = data["week_end"]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    body = "\n".join(s for s in sections if s)

    # ── Premium Design System (v2 — Design Studio A) ──
    try:
        from report.premium_style import get_premium_css, get_premium_js
        _css = get_premium_css()
        _js = get_premium_js()
    except ImportError:
        try:
            from premium_style import get_premium_css, get_premium_js
            _css = get_premium_css()
            _js = get_premium_js()
        except ImportError:
            _css = ":root {{ --text: #1E293B; }}"
            _js = ""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Q-TRON Gen4 Weekly Report — {ws} ~ {we}</title>
<style>
{_css}
/* Weekly-specific overrides */
.g4-card div:nth-child(2) {{ font-size: 28px; }}
td.g4-profit-cell {{ background: rgba(220,38,38,0.06); }}
td.g4-loss-cell {{ background: rgba(37,99,235,0.06); }}
</style>
<script>
{_js}
</script>
</head>
<body>
<div class="g4-header">
    <div class="g4-brand">Q-TRON GEN4 WEEKLY REPORT</div>
    <h1>Weekly Performance Report</h1>
    <div class="g4-date">{ws} ~ {we}</div>
    <div class="g4-subtitle">Initial Capital: {_fk(data['initial_cash'])}</div>
</div>
<div class="container">
{body}
<div class="g4-footer">
    <div>Generated: {ts} | Q-TRON Gen4 Automated Trading System</div>
    <div class="g4-footer-brand">Confidential &mdash; Internal Use Only</div>
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
