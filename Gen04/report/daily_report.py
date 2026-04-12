"""
daily_report.py — Gen4 Daily HTML Report
==========================================
EOD 일일 보고서: 성과/리스크/시스템 상태를 HTML로 생성.

Callable:
  - main.py EOD: generate_daily_report(config.REPORT_DIR, config)
  - Standalone:  python -m report.daily_report [--date YYYY-MM-DD]
"""
from __future__ import annotations
import csv
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

try:
    from pykrx import stock as krx
except ImportError:
    krx = None

from report.kospi_utils import (
    load_kospi_close, get_kospi_return, compute_excess_return, get_kospi_close_on,
)

logger = logging.getLogger("gen4.daily_report")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Stock Name Resolver (cached)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_name_cache: Dict[str, str] = {}


def resolve_stock_name(code: str) -> str:
    """Resolve 6-digit stock code to Korean name via pykrx (cached)."""
    code = str(code).zfill(6)
    if code in _name_cache:
        return _name_cache[code]
    if krx is None:
        return code
    try:
        name = krx.get_market_ticker_name(code)
        if name:
            _name_cache[code] = name
            return name
    except Exception:
        pass
    _name_cache[code] = code
    return code


def resolve_names_bulk(codes) -> Dict[str, str]:
    """Resolve multiple stock codes to names."""
    result = {}
    for c in codes:
        result[str(c).zfill(6)] = resolve_stock_name(c)
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Data Loading
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype={"code": str})
        # Ensure code column keeps leading zeros (e.g. 055550)
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
        return df
    except Exception:
        return pd.DataFrame()


def load_daily_data(report_dir: Path, today_str: str, initial_cash: float) -> dict:
    """Load all CSV data for a given date. today_str = 'YYYY-MM-DD'."""
    equity_df = _load_csv(report_dir / "equity_log.csv")
    positions_df = _load_csv(report_dir / "daily_positions.csv")
    trades_df = _load_csv(report_dir / "trades.csv")
    closes_df = _load_csv(report_dir / "close_log.csv")
    decisions_df = _load_csv(report_dir / "decision_log.csv")
    reconciles_df = _load_csv(report_dir / "reconcile_log.csv")

    # Filter by date
    def filt(df, col="date"):
        if df.empty or col not in df.columns:
            return pd.DataFrame()
        return df[df[col] == today_str]

    eq_today = filt(equity_df)
    pos_today = filt(positions_df)
    trades_today = filt(trades_df)
    # Exclude REBALANCE rows, GHOST partial fills, and PAPER_TEST mode
    if not trades_today.empty and "code" in trades_today.columns:
        trades_today = trades_today[trades_today["code"] != "REBALANCE"]
    if not trades_today.empty and "mode" in trades_today.columns:
        trades_today = trades_today[
            ~trades_today["mode"].isin(["GHOST", "PAPER_TEST"])]
    # Aggregate duplicate code+side rows (partial fills within same session)
    if not trades_today.empty and "code" in trades_today.columns:
        trades_today["quantity"] = pd.to_numeric(trades_today["quantity"], errors="coerce").fillna(0).astype(int)
        trades_today["price"] = pd.to_numeric(trades_today["price"], errors="coerce").fillna(0)
        trades_today["_amount"] = trades_today["quantity"] * trades_today["price"]
        # FIX-A6: cost 컬럼도 집계에 포함
        if "cost" in trades_today.columns:
            trades_today["cost"] = pd.to_numeric(trades_today["cost"], errors="coerce").fillna(0)
            agg = trades_today.groupby(["code", "side"], as_index=False).agg(
                {"quantity": "sum", "_amount": "sum", "cost": "sum", "mode": "last"})
        else:
            agg = trades_today.groupby(["code", "side"], as_index=False).agg(
                {"quantity": "sum", "_amount": "sum", "mode": "last"})
        agg["price"] = (agg["_amount"] / agg["quantity"]).where(agg["quantity"] > 0, 0)
        agg = agg.drop(columns=["_amount"])
        trades_today = agg
    closes_today = filt(closes_df)
    # Exclude PAPER_TEST closes, deduplicate by code
    if not closes_today.empty and "mode" in closes_today.columns:
        closes_today = closes_today[closes_today["mode"] != "PAPER_TEST"]
    if not closes_today.empty and "code" in closes_today.columns:
        closes_today = closes_today.drop_duplicates(subset=["code"], keep="last")
    decisions_today = filt(decisions_df)
    reconciles_today = filt(reconciles_df)

    # Previous equity (for daily comparison)
    eq_prev = None
    if not equity_df.empty and "date" in equity_df.columns:
        prev_dates = equity_df[equity_df["date"] < today_str]["date"].unique()
        if len(prev_dates) > 0:
            prev_date = sorted(prev_dates)[-1]
            eq_prev = equity_df[equity_df["date"] == prev_date].iloc[-1]

    # Previous day positions (for change tracking)
    prev_position_codes = set()
    if not positions_df.empty and "date" in positions_df.columns:
        prev_dates = positions_df[positions_df["date"] < today_str]["date"].unique()
        if len(prev_dates) > 0:
            prev_date = sorted(prev_dates)[-1]
            prev_pos = positions_df[positions_df["date"] == prev_date]
            prev_position_codes = set(prev_pos["code"].astype(str))

    # Current equity row (last row for today)
    equity_row = eq_today.iloc[-1] if not eq_today.empty else None

    return {
        "equity_row": equity_row,
        "equity_prev": eq_prev,
        "equity_all": equity_df.drop_duplicates(subset=["date"], keep="last") if not equity_df.empty and "date" in equity_df.columns else equity_df,
        "positions": pos_today,
        "trades": trades_today,
        "closes": closes_today,
        "decisions": decisions_today,
        "reconciles": reconciles_today,
        "prev_position_codes": prev_position_codes,
        "initial_cash": initial_cash,
        "today_str": today_str,
        "_report_dir": str(report_dir),
        "_kospi": pd.Series(dtype=float),  # filled by caller
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Computation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def compute_verdict(daily_pnl: float, price_fail: int,
                    reconcile: int, monitor_only: bool) -> Tuple[str, str, str]:
    """Returns (verdict_en, verdict_kr, color)."""
    if daily_pnl <= -0.04 or monitor_only:
        return ("DANGER", "위험", "#EF4444")
    if daily_pnl <= -0.02 or price_fail > 0 or reconcile > 0:
        return ("CAUTION", "주의", "#F59E0B")
    return ("NORMAL", "정상", "#10B981")


def compute_position_risk(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    cur = pd.to_numeric(df["current_price"], errors="coerce").fillna(0)
    trail = pd.to_numeric(df["trail_stop_price"], errors="coerce").fillna(0)
    # trail_gap = how far above trail stop (0% = at stop, negative = below stop)
    df["trail_gap_pct"] = ((cur / trail - 1) * 100).where(trail > 0, 99).round(2)
    df["risk_flag"] = df["trail_gap_pct"].apply(
        lambda g: "위험" if g <= 2 else "주의" if g <= 5 else "정상")
    return df.sort_values("trail_gap_pct", ascending=True)


def compute_pnl_breakdown(positions_df: pd.DataFrame,
                           closes_df: pd.DataFrame) -> dict:
    unrealized = 0.0
    if not positions_df.empty and "pnl_amount" in positions_df.columns:
        unrealized = pd.to_numeric(positions_df["pnl_amount"], errors="coerce").sum()
    realized = 0.0
    if not closes_df.empty and "pnl_amount" in closes_df.columns:
        realized = pd.to_numeric(closes_df["pnl_amount"], errors="coerce").sum()
    return {"unrealized": unrealized, "realized": realized,
            "total": unrealized + realized}


def compute_cost(trades_df: pd.DataFrame) -> dict:
    if trades_df.empty or "cost" not in trades_df.columns:
        return {"total": 0, "n_buys": 0, "n_sells": 0}
    cost = pd.to_numeric(trades_df["cost"], errors="coerce").fillna(0).sum()
    n_buys = len(trades_df[trades_df["side"] == "BUY"]) if "side" in trades_df.columns else 0
    n_sells = len(trades_df[trades_df["side"] == "SELL"]) if "side" in trades_df.columns else 0
    return {"total": cost, "n_buys": n_buys, "n_sells": n_sells}


def find_top_movers(df: pd.DataFrame, n: int = 3) -> List[dict]:
    if df.empty or "pnl_amount" not in df.columns:
        return []
    d = df.copy()
    d["pnl_amount"] = pd.to_numeric(d["pnl_amount"], errors="coerce").fillna(0)
    d["abs_pnl"] = d["pnl_amount"].abs()
    top = d.nlargest(n, "abs_pnl")
    result = []
    for _, r in top.iterrows():
        result.append({
            "code": str(r.get("code", "")),
            "pnl_pct": float(r.get("pnl_pct", 0)),
            "pnl_amount": float(r["pnl_amount"]),
        })
    return result


def detect_problems(data: dict) -> List[str]:
    problems = []
    pos = data["positions"]
    eq = data["equity_row"]

    # Excessive loss positions
    if not pos.empty and "pnl_pct" in pos.columns:
        pnl = pd.to_numeric(pos["pnl_pct"], errors="coerce")
        bad = pos[pnl < -0.10]
        for _, r in bad.iterrows():
            problems.append(f"과도 손실: {r['code']} (손익 {float(r['pnl_pct'])*100:.1f}%)")

    # Partial rebalance
    if eq is not None:
        rebal = str(eq.get("rebalance_executed", "N"))
        n_pos = int(eq.get("n_positions", 0))
        if rebal == "Y" and n_pos < 20:
            problems.append(f"Partial rebalance: {n_pos}/20 포지션만 편입됨")

    # Multiple trail stops in one day
    closes = data["closes"]
    if not closes.empty and "exit_reason" in closes.columns:
        n_trail = len(closes[closes["exit_reason"] == "TRAIL_STOP"])
        if n_trail >= 3:
            problems.append(f"다수 trail stop 발동: {n_trail}건")

    # Price failures
    if eq is not None:
        pf = int(eq.get("price_fail_count", 0))
        if pf > 0:
            problems.append(f"가격 조회 실패: {pf}건 (partial rebalance 가능성)")

    # Reconcile mismatches
    recon = data["reconciles"]
    if not recon.empty:
        problems.append(f"Broker sync 불일치: {len(recon)}건 보정")

    return problems


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _fk(val) -> str:
    """Format KRW with comma and sign."""
    v = float(val)
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.0f}"


def _fp(val) -> str:
    """Format percentage with sign."""
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


def _card(title: str, value: str, color: str = "#1E293B", sub: str = "") -> str:
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


def _section(title: str, content: str, alt_bg: bool = False) -> str:
    bg = "background:#F8FAFC;" if alt_bg else ""
    return f"""<div class="g4-section" style="margin-bottom:28px;padding:20px 24px;
        background:#FFFFFF;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,0.04);
        border-left:3px solid #00B4D8;{bg}">
        <h2 style="font-size:15px;color:#1E293B;font-weight:600;
            margin:0 0 14px 0;padding:0;border:none;">{title}</h2>
        {content}
    </div>"""


def _progress_bar(value: float, max_val: float = 100, color: str = "#10B981",
                  width: str = "60px", height: str = "6px") -> str:
    """Render a mini progress bar for trail gap visualization."""
    pct = min(max(value / max_val * 100, 0), 100) if max_val > 0 else 0
    bg = "#EF4444" if pct < 15 else "#F59E0B" if pct < 35 else "#10B981"
    if color != "#10B981":
        bg = color
    return (f'<div style="display:inline-block;width:{width};height:{height};'
            f'background:#E2E8F0;border-radius:3px;overflow:hidden;vertical-align:middle;">'
            f'<div style="width:{pct:.0f}%;height:100%;background:{bg};'
            f'border-radius:3px;transition:width 0.3s ease;"></div></div>')


def _treemap_block(code: str, name: str, weight: float, pnl: float,
                   color: str = "#10B981") -> str:
    """Render a treemap-style position block sized by weight."""
    # Size: min 60px, max 140px based on weight (0-10%)
    size = max(60, min(140, int(weight * 1400)))
    bg = "#FEE2E2" if pnl < -0.03 else "#DCFCE7" if pnl > 0.03 else "#F1F5F9"
    border_c = "#EF4444" if pnl < -0.03 else "#10B981" if pnl > 0.03 else "#E2E8F0"
    pnl_str = f"{pnl*100:+.1f}%"
    return (f'<div style="display:inline-block;width:{size}px;height:{size}px;'
            f'background:{bg};border:1px solid {border_c};border-radius:8px;'
            f'padding:6px;margin:3px;font-size:10px;vertical-align:top;'
            f'overflow:hidden;cursor:default;transition:transform 0.15s ease;"'
            f' title="{code} {name} | {weight*100:.1f}% | {pnl_str}">'
            f'<div style="font-weight:600;color:#1E293B;white-space:nowrap;'
            f'overflow:hidden;text-overflow:ellipsis;">{name}</div>'
            f'<div style="color:{_color(pnl)};font-weight:700;margin-top:2px;">'
            f'{pnl_str}</div>'
            f'<div style="color:#94A3B8;margin-top:1px;">{weight*100:.1f}%</div>'
            f'</div>')


def _timeline_event(time_str: str, label: str, level: str = "INFO",
                    detail: str = "") -> str:
    """Render a timeline event item."""
    colors = {
        "CRITICAL": ("#EF4444", "#FEE2E2"),
        "ERROR": ("#EF4444", "#FEE2E2"),
        "WARNING": ("#F59E0B", "#FEF3C7"),
        "INFO": ("#00B4D8", "#E0F2FE"),
    }
    dot_c, bg = colors.get(level, colors["INFO"])
    return (f'<div style="display:flex;align-items:flex-start;margin-bottom:8px;'
            f'position:relative;padding-left:24px;">'
            f'<div style="position:absolute;left:0;top:4px;width:10px;height:10px;'
            f'background:{dot_c};border-radius:50%;border:2px solid #FFFFFF;'
            f'box-shadow:0 0 0 2px {dot_c}40;"></div>'
            f'<div style="flex:1;background:{bg};border-radius:8px;padding:8px 12px;'
            f'font-size:12px;">'
            f'<span style="color:#64748B;margin-right:8px;font-weight:500;">{time_str}</span>'
            f'<span style="color:#1E293B;font-weight:600;">{label}</span>'
            f'{f"<div style=color:#64748B;font-size:11px;margin-top:2px;>{detail}</div>" if detail else ""}'
            f'</div></div>')


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Section Builders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_basis_line(data: dict, today_str: str) -> str:
    """기준 시각 / 계산 기준 한 줄 (상단 요약 바로 아래)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    eq = data["equity_row"]
    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0

    # Intraday high/low from equity_entries (if available from monitor logs)
    intra_hi = data.get("_intraday_peak_pct")
    intra_lo = data.get("_intraday_low_pct")
    intra_text = ""
    if intra_hi is not None and intra_lo is not None:
        intra_text = (f" | 장중 고점 {_fp(intra_hi)}, "
                      f"저점 {_fp(intra_lo)}")

    return (f'<div style="font-size:11px;color:#94A3B8;margin-bottom:12px;'
            f'padding:0 4px;">'
            f'기준: {ts} KST | '
            f'당일 수익률 = EOD 총자산 / 전일 종가 총자산 - 1'
            f'{intra_text}'
            f'</div>')


def build_today_verdict(data: dict, config, verdict_kr: str,
                         ops: dict, intraday_summary: dict) -> str:
    """오늘의 판단 3줄 요약 (상단 카드)."""
    eq = data["equity_row"]
    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0

    # Line 1: 상태 + 리밸런싱 + 운영
    if daily_pnl > 0:
        perf = "상승 마감"
    elif daily_pnl == 0:
        perf = "보합 마감"
    else:
        perf = "하락 마감"
    rebal = ops.get("rebal_status", "미실행")
    save = ops.get("state_save", "N/A")
    line1 = f"오늘 상태: {perf} ({_fp(daily_pnl)}) / 리밸런싱 {rebal} / 운영 {'정상' if save == 'OK' else '점검 필요'}"

    # Line 2: 핵심 경고
    warnings = []
    _is = intraday_summary or {}
    if _is.get("n_stocks", 0) > 0:
        vwap_below = _is.get("vwap_below_count", 0)
        n = _is["n_stocks"]
        worst_dd = _is.get("worst_dd_pct", 0)
        if vwap_below > n * 0.5:
            warnings.append(f"VWAP 하회 {vwap_below}/{n}")
        if worst_dd <= -4:
            warnings.append(f"일부 종목 DD {worst_dd:.1f}%")
    pf = int(eq.get("price_fail_count", 0)) if eq is not None else 0
    if pf > 0:
        warnings.append(f"가격 실패 {pf}건")
    line2 = f"핵심 경고: {', '.join(warnings)}" if warnings else "핵심 경고: 없음"

    # Line 3: 운영 안정성
    cash_sync = ops.get("cash_sync", "N/A")
    cash_delta = ops.get("cash_delta", "")
    rt = ops.get("price_rt", 0)
    fb = ops.get("price_fb", 0)
    fail = ops.get("price_fail", 0)
    stability = f"Cash Sync {cash_sync}"
    if cash_delta:
        stability += f" ({cash_delta})"
    stability += f", Price Feed {'안정' if fail == 0 and fb == 0 else '불안정'}"
    line3 = f"운영 안정성: {stability}"

    lines = [line1, line2, line3]

    bg = "#ECFDF5" if not warnings else "#FFFBEB"
    border = "#10B981" if not warnings else "#F59E0B"

    items = "".join(f'<div style="padding:3px 0;font-size:12px;color:#1E293B;">{l}</div>' for l in lines)

    return (f'<div style="background:{bg};border-left:4px solid {border};'
            f'border-radius:0 10px 10px 0;padding:12px 16px;margin-bottom:16px;'
            f'box-shadow:0 1px 4px rgba(0,0,0,0.04);">'
            f'<div style="font-size:11px;font-weight:700;color:#0A1628;margin-bottom:6px;'
            f'text-transform:uppercase;letter-spacing:0.5px;">'
            f'오늘의 판단</div>'
            f'{items}</div>')


def build_summary(data: dict, config, verdict, verdict_kr, vcolor) -> str:
    eq = data["equity_row"]
    movers = find_top_movers(data["positions"], 3)

    broker = data.get("broker_summary")

    if eq is None:
        equity = config.INITIAL_CASH
        daily_pnl = 0
    else:
        equity = float(eq.get("equity", config.INITIAL_CASH))
        daily_pnl = float(eq.get("daily_pnl_pct", 0))

    # Broker 기준 총자산/누적수익률 (LIVE) vs fallback (backtest)
    if broker and broker.get("추정예탁자산", 0) > 0:
        equity = broker["추정예탁자산"]
        total_buy = broker.get("총매입금액", 0)
        broker_pnl = broker.get("총평가손익금액", 0)
        cum_pnl = (broker_pnl / total_buy) if total_buy > 0 else 0
    else:
        cum_pnl = (equity / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0
    n_pos = int(eq.get("n_positions", 0)) if eq is not None else 0

    lines = [f"당일 수익률 <b>{_fp(daily_pnl)}</b>, "
             f"총 자산 <b>{_fk(equity)}</b>원 "
             f"(누적 {_fp(cum_pnl)})"]

    if movers:
        parts = [f"{resolve_stock_name(m['code'])}({_fp(m['pnl_pct'])})" for m in movers]
        lines.append(f"주요 손익: {', '.join(parts)}")

    lines.append(f"포지션 {n_pos}종목 보유")

    badge_bg = {"NORMAL": "#DCFCE7", "CAUTION": "#FEF3C7", "DANGER": "#FEE2E2"}
    badge_color = {"NORMAL": "#10B981", "CAUTION": "#F59E0B", "DANGER": "#EF4444"}
    badge = (f'<span style="display:inline-block;padding:6px 18px;border-radius:20px;'
             f'font-size:14px;font-weight:700;color:{badge_color.get(verdict, vcolor)};'
             f'background:{badge_bg.get(verdict, "#F1F5F9")};'
             f'letter-spacing:0.5px;">'
             f'{verdict_kr}</span>')

    summary_html = "<br>".join(lines)
    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#FFFFFF;border-radius:12px;padding:20px 24px;margin-bottom:20px;
        box-shadow:0 2px 8px rgba(0,0,0,0.06);border-left:3px solid #00B4D8;">
        <div style="font-size:14px;line-height:1.8;color:#1E293B;">{summary_html}</div>
        <div style="flex-shrink:0;margin-left:16px;">{badge}</div>
    </div>"""


def build_performance(data: dict, config) -> str:
    eq = data["equity_row"]
    pnl = compute_pnl_breakdown(data["positions"], data["closes"])
    broker = data.get("broker_summary")

    if eq is None:
        daily_pnl = 0.0
        equity = config.INITIAL_CASH
    else:
        daily_pnl = float(eq.get("daily_pnl_pct", 0))
        equity = float(eq.get("equity", config.INITIAL_CASH))

    # Broker 기준 (LIVE) vs Gen4 state 기준 (backtest) fallback
    if broker and broker.get("총매입금액", 0) > 0:
        total_buy = broker["총매입금액"]
        broker_pnl = broker.get("총평가손익금액", 0)
        cum_pnl = broker_pnl / total_buy
        unrealized = broker_pnl
        cum_basis = f"기준 매입 {_fk(total_buy)}"
    else:
        cum_pnl = (equity / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0
        unrealized = pnl["unrealized"]
        cum_basis = f"기준 {_fk(data['initial_cash'])}"

    cards = (
        _card("당일 수익률", _fp(daily_pnl), _color(daily_pnl)) +
        _card("누적 수익률", _fp(cum_pnl), _color(cum_pnl), cum_basis) +
        _card("실현 손익", _fk(pnl["realized"]), _color(pnl["realized"]), "청산 확정") +
        _card("미실현 손익", _fk(unrealized), _color(unrealized), "보유 중")
    )
    return _section("성과 지표",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_trades(data: dict) -> str:
    trades = data["trades"]
    closes = data["closes"]

    if trades.empty and closes.empty:
        return _section("거래 요약",
            '<div style="color:#94A3B8;padding:12px;">당일 거래 없음</div>')

    cost = compute_cost(trades)
    header = (f'<div style="margin-bottom:10px;display:flex;gap:8px;">'
              f'<span style="background:#EFF6FF;color:#3B82F6;padding:4px 12px;border-radius:16px;'
              f'font-size:12px;font-weight:600;">매수 {cost["n_buys"]}건</span>'
              f'<span style="background:#FEE2E2;color:#EF4444;padding:4px 12px;border-radius:16px;'
              f'font-size:12px;font-weight:600;">매도 {cost["n_sells"]}건</span></div>')

    # Build close_log lookup: code -> close details (for enriching sell rows)
    _close_map = {}
    if not closes.empty:
        for _, r in closes.iterrows():
            code = str(r.get("code", "")).zfill(6)
            _close_map[code] = {
                "entry_price": float(r.get("entry_price", 0)),
                "exit_price": float(r.get("exit_price", 0)),
                "pnl_pct": float(r.get("pnl_pct", 0)),
                "pnl_amount": float(r.get("pnl_amount", 0)),
                "exit_reason": str(r.get("exit_reason", "")),
                "hold_days": int(r.get("hold_days", 0)),
                "entry_rank": int(r.get("entry_rank", 0) or 0),
                "score_mom": float(r.get("score_mom", 0) or 0),
                "max_hwm_pct": float(r.get("max_hwm_pct", 0) or 0),
            }

    # Build decision_log lookup for BUY decisions: code -> {rank, score_mom}
    _buy_decision_map = {}
    decisions = data.get("decisions")
    if decisions is not None and not decisions.empty:
        _buys = decisions[decisions["side"] == "BUY"] if "side" in decisions.columns else decisions.iloc[0:0]
        for _, r in _buys.iterrows():
            code = str(r.get("code", "")).zfill(6)
            _buy_decision_map[code] = {
                "rank": int(r.get("rank", 0) or 0),
                "score_mom": float(r.get("score_mom", 0) or 0),
            }

    def _sell_row(code, name, qty, entry_p, exit_p, pnl, pnl_amt, hold_days, max_hwm, rank, reason):
        """Generate a sell row HTML."""
        rank_s = f'{rank}' if rank > 0 else '-'
        hwm_s = _fp(max_hwm) if max_hwm else '-'
        hd_s = f'{hold_days}d' if hold_days > 0 else '-'
        return (f'<tr><td>{code}</td>'
                f'<td style="font-size:11px;color:#64748B;">{name}</td>'
                f'<td style="text-align:center;color:#94A3B8;">{rank_s}</td>'
                f'<td style="text-align:right;">{qty:,}</td>'
                f'<td style="text-align:right;">{entry_p:,.0f}</td>'
                f'<td style="text-align:right;">{exit_p:,.0f}</td>'
                f'<td style="color:{_color(max_hwm)};text-align:right;">{hwm_s}</td>'
                f'<td style="color:{_color(pnl)};text-align:right;'
                f'font-weight:600;">{_fp(pnl)}</td>'
                f'<td style="color:{_color(pnl_amt)};text-align:right;">'
                f'{_fk(pnl_amt)}</td>'
                f'<td style="text-align:center;color:#94A3B8;">{hd_s}</td>'
                f'<td style="font-size:11px;color:#94A3B8;">{reason}</td></tr>')

    # Buy trades
    buy_rows = ""
    # Sell trades (enriched with close_log)
    sell_rows = ""
    if not trades.empty:
        for _, r in trades.iterrows():
            side = str(r.get("side", ""))
            code = str(r.get("code", "")).zfill(6)
            name = resolve_stock_name(code)
            price = float(r.get("price", 0))
            qty = int(r.get("quantity", 0))
            if side == "BUY":
                bd = _buy_decision_map.get(code, {})
                rank = bd.get("rank", 0)
                mom = bd.get("score_mom", 0)
                rank_s = f'{rank}' if rank > 0 else '-'
                mom_s = f'{mom:.2f}' if mom else '-'
                buy_rows += (f'<tr><td>{code}</td>'
                             f'<td style="font-size:11px;color:#64748B;">{name}</td>'
                             f'<td style="text-align:center;color:#94A3B8;">{rank_s}</td>'
                             f'<td style="text-align:right;color:#94A3B8;">{mom_s}</td>'
                             f'<td style="text-align:right;">{qty:,}</td>'
                             f'<td style="text-align:right;">{price:,.0f}</td></tr>')
            elif side == "SELL":
                cl = _close_map.get(code)
                if not cl:
                    continue  # no close_log entry → likely paper_test residue
                sell_rows += _sell_row(
                    code, name, qty,
                    cl.get("entry_price", 0), price,
                    cl.get("pnl_pct", 0), cl.get("pnl_amount", 0),
                    cl.get("hold_days", 0), cl.get("max_hwm_pct", 0),
                    cl.get("entry_rank", 0), cl.get("exit_reason", "REBAL"))

    # Closed positions not in trades (e.g. trail stop filled after monitor)
    close_only_rows = ""
    _traded_codes = set()
    if not trades.empty and "code" in trades.columns:
        _traded_codes = set(trades[trades["side"] == "SELL"]["code"].astype(str))
    if not closes.empty:
        for _, r in closes.iterrows():
            code = str(r.get("code", "")).zfill(6)
            if code in _traded_codes:
                continue
            name = resolve_stock_name(code)
            close_only_rows += _sell_row(
                code, name,
                int(r.get("quantity", 0)),
                float(r.get("entry_price", 0)),
                float(r.get("exit_price", 0)),
                float(r.get("pnl_pct", 0)),
                float(r.get("pnl_amount", 0)),
                int(r.get("hold_days", 0)),
                float(r.get("max_hwm_pct", 0) or 0),
                int(r.get("entry_rank", 0) or 0),
                str(r.get("exit_reason", "")))

    table_style = 'style="width:100%;border-collapse:collapse;font-size:13px;border-radius:8px;overflow:hidden;"'
    th_style = 'style="text-align:left;padding:8px 8px;border-bottom:1px solid #E2E8F0;color:#475569;font-size:12px;font-weight:600;background:#F1F5F9;"'
    td_base = 'style="padding:6px 8px;border-bottom:1px solid #F1F5F9;"'

    content = header
    sell_all_rows = sell_rows + close_only_rows
    if sell_all_rows:
        content += f"""<div style="margin-top:4px;font-size:13px;color:#3B82F6;font-weight:600;">매도</div>
            <table {table_style}>
            <tr><th {th_style}>종목</th><th {th_style}>종목명</th>
            <th {th_style} style="text-align:center;">순위</th>
            <th {th_style} style="text-align:right;">수량</th>
            <th {th_style} style="text-align:right;">매수가</th>
            <th {th_style} style="text-align:right;">매도가</th>
            <th {th_style} style="text-align:right;">최고↑</th>
            <th {th_style} style="text-align:right;">수익률</th>
            <th {th_style} style="text-align:right;">손익액</th>
            <th {th_style} style="text-align:center;">보유일</th>
            <th {th_style}>사유</th></tr>
            {sell_all_rows}</table>"""
    if buy_rows:
        content += f"""<div style="margin-top:12px;font-size:13px;color:#FF4757;font-weight:600;">매수</div>
            <table {table_style}>
            <tr><th {th_style}>종목</th><th {th_style}>종목명</th>
            <th {th_style} style="text-align:center;">순위</th>
            <th {th_style} style="text-align:right;">모멘텀</th>
            <th {th_style} style="text-align:right;">수량</th>
            <th {th_style} style="text-align:right;">매수가</th></tr>
            {buy_rows}</table>"""

    return _section("거래 요약", content)


def build_positions(data: dict, intraday_summary: dict = None) -> str:
    pos = data["positions"]
    if pos.empty:
        return _section("미청산 포지션",
            '<div style="color:#94A3B8;padding:12px;">미보유 (현금 100%)</div>')

    pos = compute_position_risk(pos)

    # Resolve Korean stock names
    codes = pos["code"].astype(str).tolist()
    names = resolve_names_bulk(codes)

    # Build intraday lookup: code -> {close_vs_vwap_pct, max_intraday_dd_pct}
    intra_lookup = {}
    if intraday_summary and intraday_summary.get("per_stock"):
        for ps in intraday_summary["per_stock"]:
            intra_lookup[str(ps.get("code", "")).zfill(6)] = ps

    th = 'style="text-align:left;padding:8px 8px;border-bottom:1px solid #E2E8F0;color:#475569;font-size:11px;font-weight:600;background:#F1F5F9;"'
    td = 'style="padding:6px 8px;font-size:12px;border-bottom:1px solid #F1F5F9;"'
    tdr = 'style="padding:6px 8px;text-align:right;font-size:12px;border-bottom:1px solid #F1F5F9;"'
    rows = ""
    for i, (_, r) in enumerate(pos.iterrows(), 1):
        pnl = float(r.get("pnl_pct", 0))
        amt = float(r.get("pnl_amount", 0))
        est_cost = float(r.get("est_cost_pct", 0))
        net_pnl = float(r.get("net_pnl_pct", pnl - est_cost))
        gap = float(r.get("trail_gap_pct", 99))
        hd = int(r.get("hold_days", 0))
        mv = float(r.get("market_value", 0))
        qty = int(r.get("quantity", 0))
        avg = float(r.get("avg_price", 0))
        cur = float(r.get("current_price", 0))
        code = str(r.get("code", ""))
        stock_name = names.get(code.zfill(6), code)

        # Intraday data
        intra = intra_lookup.get(code.zfill(6), {})
        vs_vwap = intra.get("close_vs_vwap_pct", None)
        intra_dd = intra.get("max_intraday_dd_pct", None)

        # Enhanced risk flag: 4-level
        vwap_below = (vs_vwap is not None and vs_vwap < 0)
        dd_severe = (intra_dd is not None and intra_dd <= -4)
        trail_near = gap <= 5

        if (trail_near and (vwap_below or dd_severe)):
            rf = "고위험"
            bg = "#FEE2E2"
            rf_color = "#EF4444"
        elif trail_near:
            rf = "경계"
            bg = "#FFF7ED"
            rf_color = "#F59E0B"
        elif vwap_below and dd_severe:
            rf = "주의"
            bg = "#FEF3C7"
            rf_color = "#F59E0B"
        elif gap <= 2:
            rf = "경계"
            bg = "#FEE2E2"
            rf_color = "#EF4444"
        else:
            rf = "정상"
            bg = "#FFFFFF"
            rf_color = "#10B981"

        # Format vs VWAP / Intraday DD
        if vs_vwap is not None:
            vwap_str = f"{vs_vwap:+.2f}%"
            vwap_color = "#3B82F6" if vs_vwap < 0 else "#FF4757" if vs_vwap > 0 else "#1E293B"
        else:
            vwap_str = "-"
            vwap_color = "#94A3B8"
        if intra_dd is not None:
            dd_str = f"{intra_dd:.2f}%"
            dd_color = "#EF4444" if intra_dd <= -4 else "#F59E0B" if intra_dd <= -2 else "#1E293B"
        else:
            dd_str = "-"
            dd_color = "#94A3B8"

        # Progress bar for trail gap (gap capped at 20% for visualization)
        gap_bar_color = "#EF4444" if gap <= 2 else "#F59E0B" if gap <= 5 else "#10B981"
        gap_bar = _progress_bar(gap, max_val=20, color=gap_bar_color, width="48px", height="5px")

        # PnL cell class for subtle background tint
        pnl_cell_cls = "g4-profit-cell" if pnl > 0 else "g4-loss-cell" if pnl < 0 else ""

        rows += (f'<tr style="background:{bg};">'
                 f'<td {td}>{i}</td>'
                 f'<td {td} style="font-weight:600;color:{_color(pnl)};">{stock_name}'
                 f'<span style="font-size:9px;color:#94A3B8;margin-left:4px;">{code}</span></td>'
                 f'<td {tdr}>{qty:,}</td>'
                 f'<td {tdr}>{avg:,.0f}</td>'
                 f'<td {tdr}>{cur:,.0f}</td>'
                 f'<td {tdr} class="{pnl_cell_cls}" style="color:{_color(pnl)};">{_fp(pnl)}</td>'
                 f'<td {tdr} class="{pnl_cell_cls}" style="color:{_color(amt)};">{amt:+,.0f}</td>'
                 f'<td {tdr} style="color:{_color(net_pnl)};font-weight:600;">{_fp(net_pnl)}</td>'
                 f'<td {tdr} style="color:{vwap_color};">{vwap_str}</td>'
                 f'<td {tdr} style="color:{dd_color};">{dd_str}</td>'
                 f'<td {tdr}>{hd}일</td>'
                 f'<td {tdr} style="color:{rf_color};">{gap:.1f}% {gap_bar}</td>'
                 f'<td {td} style="text-align:center;color:{rf_color};font-weight:600;">{rf}</td>'
                 f'</tr>')

    total_mv = pd.to_numeric(pos["market_value"], errors="coerce").sum()
    total_pnl = pd.to_numeric(pos["pnl_amount"], errors="coerce").sum()

    # Sortable header helper
    def _sh(label, col_idx, is_num=True, align="right"):
        al = f"text-align:{align};"
        return (f'<th class="g4-sortable" onclick="g4sort(\'pos-tbl\',{col_idx},{str(is_num).lower()})" '
                f'style="{al}padding:8px 8px;border-bottom:1px solid #E2E8F0;'
                f'color:#475569;font-size:11px;font-weight:600;background:#F1F5F9;">'
                f'{label}<span class="g4-arrow" style="font-size:9px;"></span></th>')

    return _section("미청산 포지션",
        f"""<div style="font-size:11px;color:#94A3B8;margin-bottom:8px;">
            헤더 클릭으로 정렬 | 상태: 정상 / 주의(VWAP+DD) / 경계(Trail) / 고위험(복합)</div>
        <div class="g4-sticky-wrap" style="overflow-x:auto;border-radius:8px;border:1px solid #E2E8F0;">
        <table id="pos-tbl" style="width:100%;border-collapse:collapse;font-size:13px;min-width:1000px;">
        <thead><tr>
        {_sh('#', 0)}
        {_sh('종목명', 1, False, 'left')}
        {_sh('수량', 2)}
        {_sh('평균가', 3)}
        {_sh('현재가', 4)}
        {_sh('손익률', 5)}
        {_sh('손익금액', 6)}
        {_sh('순손익률', 7)}
        {_sh('vs VWAP', 8)}
        {_sh('장중DD', 9)}
        {_sh('보유', 10)}
        {_sh('Trail Gap', 11)}
        {_sh('상태', 12, False, 'center')}
        </tr></thead>
        <tbody>
        {rows}
        <tr style="border-top:2px solid #00B4D8;font-weight:700;background:#F8FAFC;">
        <td colspan="6" style="padding:8px;">합계</td>
        <td style="padding:8px;text-align:right;color:{_color(total_pnl)};">{total_pnl:+,.0f}</td>
        <td colspan="3" style="padding:8px;"></td>
        <td colspan="3" style="padding:8px;text-align:right;">{total_mv:,.0f}</td></tr>
        </tbody>
        </table></div>""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NEW: Operations + Risk + Anomaly + Intraday Flow + Log Events + Verdict
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_log_events(log_dir: Path, today_str: str) -> List[dict]:
    """Parse today's live log for key events."""
    date_compact = today_str.replace("-", "")
    candidates = [
        log_dir / f"gen4_live_{date_compact}.log",
        log_dir / f"gen4_mock_{date_compact}.log",
        log_dir / f"gen4_paper_test_{date_compact}.log",
        log_dir / f"gen4_paper_{date_compact}.log",
    ]
    log_path = None
    for p in candidates:
        if p.exists():
            log_path = p
            break
    if not log_path:
        return []

    patterns = {
        "TRADING_MODE": "LOGIN",
        "RECON": "RECON",
        "STATE_SAVE_OK": "STATE_SAVE",
        "STATE_SAVE_FAIL": "STATE_SAVE",
        "REBAL_SELL_STATUS": "REBALANCE",
        "REBALANCE_COMMIT_OK": "REBALANCE",
        "REBAL_COMMIT_DEFERRED": "REBALANCE",
        "REBAL_SELL_FAILED": "REBALANCE",
        "PENDING_BUY": "PENDING_BUY",
        "TRAIL_PRECHECK_NEAR": "TRAIL",
        "EOD_TRAIL_CHECK": "TRAIL",
        "EOD_TRAIL_EXIT": "TRAIL",
        "TRAIL_SKIP": "TRAIL_SKIP",
        "TRAIL_DISABLED_BY_DATA": "TRAIL_SKIP",
        "CASH_DIVERGENCE": "CASH_SYNC",
        "SAFE_MODE": "SAFE_MODE",
        "DD_GUARD": "RISK",
        "Monitor:": "MONITOR",
        "WARNING": "WARNING",
        "ERROR": "ERROR",
        "CRITICAL": "CRITICAL",
    }

    events = []
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                for tag, category in patterns.items():
                    if tag in line:
                        # Extract time
                        time_str = line[:19] if len(line) >= 19 else ""
                        try:
                            ts = time_str.split(" ")[1].split(",")[0][:5]  # HH:MM
                        except (IndexError, ValueError):
                            ts = ""
                        # Extract level
                        level = "INFO"
                        if "[WARNING]" in line:
                            level = "WARNING"
                        elif "[ERROR]" in line:
                            level = "ERROR"
                        elif "[CRITICAL]" in line:
                            level = "CRITICAL"
                        # Extract summary (after the tag)
                        idx = line.find(tag)
                        summary = line[idx:idx+120].strip() if idx >= 0 else line[-120:]
                        events.append({
                            "time": ts,
                            "category": category,
                            "level": level,
                            "summary": summary,
                        })
                        break  # first match per line
    except Exception:
        pass
    return events


def _extract_ops_status(events: List[dict], eq) -> dict:
    """Extract operational status from parsed log events + equity row."""
    status = {
        "trading_mode": "N/A",
        "rebal_status": "미실행",
        "rebal_detail": "",
        "cash_sync": "N/A",
        "cash_delta": "",
        "state_save": "N/A",
        "price_rt": 0,
        "price_fb": 0,
        "price_fail": 0,
    }

    for e in events:
        s = e["summary"]
        if "TRADING_MODE" in s:
            if "paper" in s.lower():
                status["trading_mode"] = "PAPER"
            elif "live" in s.lower():
                status["trading_mode"] = "LIVE"
            elif "mock" in s.lower():
                status["trading_mode"] = "MOCK"
        elif "REBALANCE_COMMIT_OK" in s:
            status["rebal_status"] = "실행 완료"
        elif "REBAL_COMMIT_DEFERRED" in s:
            status["rebal_status"] = "DEFERRED"
            status["rebal_detail"] = "sell 미확정"
        elif "REBAL_SELL_FAILED" in s:
            status["rebal_status"] = "FAILED"
        elif "PENDING_BUY_START" in s or "PENDING_BUY_EXEC" in s:
            status["rebal_status"] = "Pending Buy 실행"
        elif "RECON" in s and "Cash synced" in s:
            status["cash_sync"] = "OK"
            # Extract delta
            try:
                parts = s.split("->")
                if len(parts) == 2:
                    old = float(parts[0].split()[-1].replace(",", ""))
                    new = float(parts[1].strip().replace(",", ""))
                    delta = new - old
                    status["cash_delta"] = _fk(delta)
            except (ValueError, IndexError):
                pass
        elif "STATE_SAVE_OK" in s:
            status["state_save"] = "OK"
        elif "STATE_SAVE_FAIL" in s:
            status["state_save"] = "FAIL"

    # Price feed from monitor logs
    for e in events:
        if e["category"] == "MONITOR" and "rt=" in e["summary"]:
            try:
                s = e["summary"]
                # Extract rt=N fb=N fail=N
                for part in s.split("[")[1].split("]")[0].split():
                    if part.startswith("rt="):
                        status["price_rt"] = int(part.split("=")[1])
                    elif part.startswith("fb="):
                        status["price_fb"] = int(part.split("=")[1])
                    elif part.startswith("fail="):
                        status["price_fail"] = int(part.split("=")[1])
            except (IndexError, ValueError):
                pass

    # Fallback from equity row
    if eq is not None:
        pf = int(eq.get("price_fail_count", 0))
        if pf > 0 and status["price_fail"] == 0:
            status["price_fail"] = pf
        rc = int(eq.get("reconcile_corrections", 0))
        if rc > 0 and status["cash_sync"] == "N/A":
            status["cash_sync"] = f"{rc}건 보정"

    # Event counts for daily stability overview
    status["_event_counts"] = {
        "cash_sync_count": sum(1 for e in events if e["category"] in ("RECON", "CASH_SYNC")),
        "state_save_count": sum(1 for e in events if e["category"] == "STATE_SAVE"),
        "price_fb_count": status["price_fb"],
        "price_fail_count": status["price_fail"],
        "warning_count": sum(1 for e in events if e["level"] == "WARNING"),
        "error_count": sum(1 for e in events if e["level"] in ("ERROR", "CRITICAL")),
    }

    return status


def build_ops_status(data: dict, ops: dict) -> str:
    """Section 1: Operations status card (상단)."""
    mode = ops["trading_mode"]
    mode_color = "#3B82F6" if mode == "PAPER" else "#EF4444" if mode == "LIVE" else "#94A3B8"

    rebal = ops["rebal_status"]
    rebal_color = "#10B981" if "완료" in rebal else "#F59E0B" if rebal in ("DEFERRED", "Pending") else "#94A3B8"

    cash = ops["cash_sync"]
    cash_detail = f" ({ops['cash_delta']})" if ops["cash_delta"] else ""
    cash_color = "#10B981" if cash == "OK" else "#EF4444" if cash == "FAIL" else "#94A3B8"

    save = ops["state_save"]
    save_color = "#10B981" if save == "OK" else "#EF4444" if save == "FAIL" else "#94A3B8"

    rt, fb, fail = ops["price_rt"], ops["price_fb"], ops["price_fail"]
    price_color = "#10B981" if fail == 0 else "#EF4444"
    price_text = f"RT={rt} / FB={fb} / FAIL={fail}"

    def _ops_item(label, value, color):
        return (f'<div style="display:flex;justify-content:space-between;'
                f'padding:5px 0;font-size:13px;border-bottom:1px solid #F1F5F9;">'
                f'<span style="color:#64748B;">{label}</span>'
                f'<span style="color:{color};font-weight:600;">{value}</span></div>')

    items = (
        _ops_item("Mode", mode, mode_color) +
        _ops_item("Rebalance", rebal + (f" ({ops['rebal_detail']})" if ops['rebal_detail'] else ""), rebal_color) +
        _ops_item("Cash Sync", cash + cash_detail, cash_color) +
        _ops_item("State Save", save, save_color) +
        _ops_item("Price Feed", price_text, price_color)
    )

    # 하루 전체 안정성 이벤트 카운트
    counts = ops.get("_event_counts", {})
    if counts:
        count_parts = []
        for label, val in [
            ("Cash보정", counts.get("cash_sync_count", 0)),
            ("State저장", counts.get("state_save_count", 0)),
            ("PriceFB", counts.get("price_fb_count", 0)),
            ("PriceFail", counts.get("price_fail_count", 0)),
            ("Warning", counts.get("warning_count", 0)),
            ("Error", counts.get("error_count", 0)),
        ]:
            c = "#EF4444" if ("Fail" in label or "Error" in label) and val > 0 else "#94A3B8"
            count_parts.append(
                f'<span style="color:{c};font-size:11px;">{label}:{val}</span>')
        counts_html = (
            f'<div style="margin-top:8px;padding-top:8px;border-top:1px solid #E2E8F0;'
            f'font-size:11px;display:flex;gap:10px;flex-wrap:wrap;">'
            + " ".join(count_parts) + '</div>')
    else:
        counts_html = ""

    return f"""<div class="g4-ops" style="background:#FFFFFF;border-radius:12px;padding:18px 22px;
        margin-bottom:20px;border-left:3px solid #00B4D8;
        box-shadow:0 2px 8px rgba(0,0,0,0.06);">
        <div style="font-size:13px;font-weight:700;color:#0A1628;margin-bottom:10px;
            text-transform:uppercase;letter-spacing:0.5px;">
            운영 상태</div>
        {items}
        {counts_html}
    </div>"""


def build_risk_summary(intraday_summary: dict) -> str:
    """Section 2: Risk summary card (리스크 요약)."""
    if not intraday_summary or intraday_summary.get("n_stocks", 0) == 0:
        return ""

    n = intraday_summary["n_stocks"]
    worst_dd = intraday_summary.get("worst_dd_pct", 0)
    avg_dd = intraday_summary.get("avg_max_dd_pct", 0)
    vwap_below = intraday_summary.get("vwap_below_count", 0)
    spikes_3x = intraday_summary.get("total_volume_spikes_3x", 0)
    risk_score = intraday_summary.get("risk_score", 0)

    # Color rules
    dd_color = "#EF4444" if worst_dd <= -4 else "#F59E0B" if worst_dd <= -2 else "#1E293B"
    vwap_color = "#F59E0B" if vwap_below > n * 0.5 else "#1E293B"
    rs_color = "#EF4444" if risk_score >= 70 else "#F59E0B" if risk_score >= 40 else "#10B981"

    cards = (
        _card("Worst DD", f"{worst_dd:.1f}%", dd_color,
              intraday_summary.get("worst_dd_code", "")) +
        _card("Avg DD", f"{avg_dd:.1f}%", "#F59E0B" if avg_dd <= -2 else "#1E293B") +
        _card("VWAP 하회", f"{vwap_below}/{n}", vwap_color,
              f"{vwap_below/n*100:.0f}%" if n > 0 else "") +
        _card("Risk Score", f"{risk_score:.0f}", rs_color)
    )
    return _section("리스크 요약",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_anomaly(data: dict, intraday_summary: dict, events: List[dict]) -> str:
    """Section 3: Anomaly detection (이상 탐지)."""
    alerts = []

    # Intraday-based anomalies
    if intraday_summary and intraday_summary.get("n_stocks", 0) > 0:
        n = intraday_summary["n_stocks"]
        worst_dd = intraday_summary.get("worst_dd_pct", 0)
        vwap_below = intraday_summary.get("vwap_below_count", 0)
        spikes_3x = intraday_summary.get("total_volume_spikes_3x", 0)
        near_trail = intraday_summary.get("near_trail_count", 0)

        if worst_dd <= -4:
            alerts.append(("HIGH",
                f"일부 종목 {worst_dd:.1f}% DD 발생 "
                f"({intraday_summary.get('worst_dd_code', '')})"))
        if vwap_below > n * 0.7:
            alerts.append(("MED",
                f"포트폴리오 {vwap_below}/{n} ({vwap_below/n*100:.0f}%) VWAP 하회"))
        if spikes_3x > 10:
            alerts.append(("MED",
                f"볼륨 스파이크 3x: {spikes_3x}건 (변동성 확대)"))
        if near_trail > 0:
            alerts.append(("HIGH",
                f"Trail Stop 근접 {near_trail}종목 (DD <= -10%)"))

    # Price feed anomalies
    eq = data["equity_row"]
    if eq is not None:
        pf = int(eq.get("price_fail_count", 0))
        if pf > 0:
            alerts.append(("MED", f"가격 조회 실패 {pf}건"))

    # Log-based anomalies
    critical_count = sum(1 for e in events if e["level"] == "CRITICAL")
    error_count = sum(1 for e in events if e["level"] == "ERROR")
    trail_skip_count = sum(1 for e in events if e["category"] == "TRAIL_SKIP")
    safe_mode = any(e["category"] == "SAFE_MODE" for e in events)

    if safe_mode:
        alerts.append(("HIGH", "SAFE MODE 발동 — 신규 매수 차단"))
    if critical_count > 0:
        alerts.append(("HIGH", f"CRITICAL 이벤트 {critical_count}건"))
    if error_count > 2:
        alerts.append(("MED", f"ERROR 이벤트 {error_count}건"))
    if trail_skip_count > 0:
        alerts.append(("MED", f"Trail Stop 데이터 부재 {trail_skip_count}건"))

    if not alerts:
        return _section("이상 탐지",
            '<div style="color:#10B981;padding:8px;font-size:13px;">'
            '이상 없음</div>')

    # 권장 액션 매핑 (경고 키워드 → 의미, 체크포인트, 주의사항)
    _action_map = {
        "DD": ("장중 급락으로 trail stop 근접 위험",
               "내일 해당 종목 시가 및 거래량 확인",
               "trail 발동 시 빈 슬롯 보충 없음에 유의"),
        "VWAP": ("다수 종목이 VWAP 하회하여 매수 체력 약화",
                 "내일 VWAP 회복 여부 및 거래량 추이 확인",
                 "리밸런싱 시 추격매수 주의"),
        "볼륨 스파이크": ("거래량 급증으로 변동성 확대",
                    "내일 스파이크 지속 여부 확인",
                    "급등 후 되돌림 가능성 주의"),
        "Trail Stop 근접": ("trail stop 발동 임박 종목 존재",
                      "내일 해당 종목 시가 갭 확인",
                      "청산 시 자금 유입되나 빈 슬롯 보충 안 됨"),
        "가격 조회 실패": ("일부 종목 실시간 가격 미수신",
                    "내일 해당 종목 장 시작 후 가격 수신 정상화 확인",
                    "fallback 가격 사용 시 trail 정확도 저하"),
        "SAFE MODE": ("DD guard에 의해 전 종목 신규 매수 차단",
                 "DD 회복률 확인, -20% 이상 회복 시 자동 해제",
                 "해제 전까지 리밸런싱 매수 불가"),
        "CRITICAL": ("시스템 크리티컬 오류 발생",
                "로그 파일에서 CRITICAL 이벤트 원인 파악",
                "forensic snapshot 확인, 필요시 수동 재시작"),
        "ERROR": ("시스템 에러 다수 발생",
             "로그 파일에서 ERROR 패턴 확인",
             "연속 발생 시 monitor restart 동작 확인"),
        "Trail Stop 데이터 부재": ("fallback price 사용으로 trail 판단 불가",
                          "내일 가격 수신 정상화 확인",
                          "연속 3일 이상 시 해당 종목 수동 점검"),
    }

    items = ""
    for severity, msg in alerts:
        if severity == "HIGH":
            bg, border, icon = "#FEE2E2", "#EF4444", "!!"
        else:
            bg, border, icon = "#FFFBEB", "#F59E0B", "!"

        # 매칭되는 권장 액션 찾기
        action_html = ""
        for keyword, (meaning, checkpoint, caution) in _action_map.items():
            if keyword in msg:
                action_html = (
                    f'<div style="font-size:11px;color:#64748B;margin-top:4px;'
                    f'padding-left:20px;line-height:1.5;">'
                    f'<span style="color:#94A3B8;">의미:</span> {meaning}<br>'
                    f'<span style="color:#94A3B8;">체크:</span> {checkpoint}<br>'
                    f'<span style="color:#94A3B8;">주의:</span> {caution}'
                    f'</div>')
                break

        items += (f'<div class="g4-alert" style="background:{bg};border-left:4px solid {border};'
                  f'padding:10px 14px;margin-bottom:8px;font-size:13px;'
                  f'border-radius:0 8px 8px 0;">'
                  f'<span style="font-weight:700;color:{border};margin-right:6px;">'
                  f'{icon}</span>{msg}'
                  f'{action_html}</div>')

    return _section("이상 탐지", items)


def build_intraday_flow(intraday_summary: dict, events: List[dict]) -> str:
    """Section 4: Intraday flow summary (장중 흐름 요약)."""
    if not intraday_summary or intraday_summary.get("n_stocks", 0) == 0:
        return ""

    per_stock = intraday_summary.get("per_stock", [])
    if not per_stock:
        return ""

    # Find peak/low/dd across portfolio
    peak_dd = 0.0
    peak_code = ""
    peak_time = ""
    low_dd = 0.0
    low_code = ""
    low_time = ""

    for r in per_stock:
        dd = r.get("max_intraday_dd_pct", 0)
        if dd < low_dd:
            low_dd = dd
            low_code = r.get("code", "")
            low_time = r.get("max_dd_time", "")

    # Extract equity changes from monitor events
    equity_entries = []
    for e in events:
        if e["category"] == "MONITOR" and "equity=" in e["summary"]:
            try:
                s = e["summary"]
                eq_part = s.split("equity=")[1].split(",")[0].replace(",", "")
                eq_val = float(eq_part)
                equity_entries.append((e["time"], eq_val))
            except (IndexError, ValueError):
                pass

    flow_items = []
    if equity_entries:
        peak_entry = max(equity_entries, key=lambda x: x[1])
        low_entry = min(equity_entries, key=lambda x: x[1])
        if len(equity_entries) >= 2:
            range_pct = (peak_entry[1] - low_entry[1]) / low_entry[1] * 100 if low_entry[1] > 0 else 0
            flow_items.append(f"고점: {peak_entry[0]} ({peak_entry[1]:,.0f}원)")
            flow_items.append(f"저점: {low_entry[0]} ({low_entry[1]:,.0f}원)")
            flow_items.append(f"장중 변동폭: {range_pct:.2f}%")

    avg_dd = intraday_summary.get("avg_max_dd_pct", 0)
    worst_dd = intraday_summary.get("worst_dd_pct", 0)
    worst_code = intraday_summary.get("worst_dd_code", "")

    flow_items.append(f"평균 장중DD: {avg_dd:.2f}%")
    flow_items.append(f"최악 DD: {worst_dd:.2f}% ({resolve_stock_name(worst_code)} "
                      f"@ {low_time})")

    # 추가 지표: peak-to-close DD, 저점 회복률, 후반 강도
    if equity_entries and len(equity_entries) >= 2:
        peak_entry = max(equity_entries, key=lambda x: x[1])
        low_entry = min(equity_entries, key=lambda x: x[1])
        last_entry = equity_entries[-1]

        # Peak-to-close drawdown
        if peak_entry[1] > 0:
            ptc_dd = (last_entry[1] - peak_entry[1]) / peak_entry[1] * 100
            flow_items.append(f"고점→종가 DD: {ptc_dd:.2f}%")

        # 저점 이후 회복률
        if low_entry[1] > 0 and last_entry[1] >= low_entry[1]:
            recovery = (last_entry[1] - low_entry[1]) / low_entry[1] * 100
            flow_items.append(f"저점 이후 회복: +{recovery:.2f}%")

        # 후반 강도 (14:00 이후 변화)
        afternoon_entries = [(t, v) for t, v in equity_entries if t >= "14:00"]
        if len(afternoon_entries) >= 2:
            af_first = afternoon_entries[0][1]
            af_last = afternoon_entries[-1][1]
            if af_first > 0:
                af_change = (af_last - af_first) / af_first * 100
                af_label = "강세" if af_change > 0.3 else "약세" if af_change < -0.3 else "보합"
                flow_items.append(
                    f"후반(14:00~) 강도: {af_change:+.2f}% ({af_label})")

    items_html = "".join(
        f'<div style="padding:4px 0;font-size:13px;color:#1E293B;">'
        f'<span style="color:#00B4D8;margin-right:8px;font-weight:700;">-</span>{item}</div>'
        for item in flow_items
    )

    return _section("장중 흐름 요약", f"""
        <div style="background:#F8FAFC;border-radius:10px;padding:16px 20px;">
            {items_html}
        </div>""")


def build_log_events(events: List[dict]) -> str:
    """Section 6: Key log events summary (로그 핵심 이벤트)."""
    if not events:
        return ""

    # Filter: only important categories
    important = [e for e in events
                 if e["category"] in ("LOGIN", "RECON", "REBALANCE", "PENDING_BUY",
                                      "TRAIL", "TRAIL_SKIP", "CASH_SYNC", "SAFE_MODE",
                                      "RISK", "STATE_SAVE")
                 or e["level"] in ("ERROR", "CRITICAL")]

    # Dedup: keep first per (category, level) pair, max 20
    seen = set()
    filtered = []
    for e in important:
        key = (e["category"], e["level"], e["summary"][:40])
        if key not in seen:
            seen.add(key)
            filtered.append(e)
    filtered = filtered[:20]

    if not filtered:
        return ""

    th = ('style="text-align:left;padding:8px 8px;border-bottom:1px solid #E2E8F0;'
          'color:#475569;font-size:11px;font-weight:600;background:#F1F5F9;"')

    rows = ""
    for e in filtered:
        level = e["level"]
        if level == "CRITICAL":
            lc = "#EF4444"
            bg = "#FEE2E2"
        elif level == "ERROR":
            lc = "#EF4444"
            bg = "#FEE2E2"
        elif level == "WARNING":
            lc = "#F59E0B"
            bg = "#FFFBEB"
        else:
            lc = "#1E293B"
            bg = "#FFFFFF"

        # Truncate summary
        summ = e["summary"][:80]
        rows += (f'<tr style="background:{bg};">'
                 f'<td style="padding:5px 8px;font-size:12px;color:#94A3B8;">{e["time"]}</td>'
                 f'<td style="padding:5px 8px;font-size:11px;font-weight:600;color:{lc};">'
                 f'{e["category"]}</td>'
                 f'<td style="padding:5px 8px;font-size:11px;color:#1E293B;">{summ}</td></tr>')

    return _section("로그 핵심 이벤트", f"""
        <div style="overflow-x:auto;border-radius:8px;border:1px solid #E2E8F0;">
        <table style="width:100%;border-collapse:collapse;font-size:12px;">
        <tr><th {th}>시각</th><th {th}>이벤트</th><th {th}>요약</th></tr>
        {rows}
        </table></div>""")


def build_strategy_verdict(data: dict, intraday_summary: dict,
                            verdict: str, ops: dict) -> str:
    """Section 7: Strategy interpretation (전략 해석)."""
    eq = data["equity_row"]
    if eq is None:
        return ""

    daily_pnl = float(eq.get("daily_pnl_pct", 0))
    n_pos = int(eq.get("n_positions", 0))

    lines = []

    # Performance assessment
    if daily_pnl > 0.02:
        lines.append(f"금일 포트폴리오 <b>상승 추세</b> 유지 ({_fp(daily_pnl)})")
    elif daily_pnl > 0:
        lines.append(f"금일 포트폴리오 <b>소폭 상승</b> ({_fp(daily_pnl)})")
    elif daily_pnl > -0.02:
        lines.append(f"금일 포트폴리오 <b>소폭 하락</b> ({_fp(daily_pnl)})")
    else:
        lines.append(f"금일 포트폴리오 <b>하락</b> ({_fp(daily_pnl)}) — 주의 필요")

    # VWAP assessment
    if intraday_summary and intraday_summary.get("n_stocks", 0) > 0:
        n = intraday_summary["n_stocks"]
        vwap_below = intraday_summary.get("vwap_below_count", 0)
        risk_score = intraday_summary.get("risk_score", 0)
        if vwap_below > n * 0.5:
            lines.append(f"VWAP 하회 종목 다수 ({vwap_below}/{n}) — 상승 피로 신호")
        if risk_score >= 70:
            lines.append("Risk Score 고위험 구간 — 변동성 확대 주의")

    # Rebalance assessment
    if "실행" in ops.get("rebal_status", ""):
        lines.append("리밸런싱 실행 완료 — 포트폴리오 재편성됨")
    elif ops.get("rebal_status") == "DEFERRED":
        lines.append("리밸런싱 지연 상태 — sell 미확정, 다음 세션 확인 필요")

    # Position count
    if n_pos < 15:
        lines.append(f"보유 종목 {n_pos}/20 — trail stop 등으로 빈 슬롯 발생")

    # Verdict
    if verdict == "NORMAL":
        lines.append("종합: <b>정상 운영</b> — 모니터링 유지")
    elif verdict == "CAUTION":
        lines.append("종합: <b>주의</b> — 로그 점검 권고")
    else:
        lines.append("종합: <b>위험</b> — 신규 진입 중단 검토")

    items = "".join(f'<div style="padding:3px 0;color:#1E293B;">{l}</div>' for l in lines)
    bg = "#ECFDF5" if verdict == "NORMAL" else "#FFFBEB" if verdict == "CAUTION" else "#FEE2E2"
    border = "#10B981" if verdict == "NORMAL" else "#F59E0B" if verdict == "CAUTION" else "#EF4444"

    return _section("전략 해석", f"""
        <div style="background:{bg};border-left:4px solid {border};
            border-radius:0 10px 10px 0;padding:16px 20px;font-size:13px;line-height:1.8;">
            {items}
        </div>""")


def build_risk(data: dict) -> str:
    eq = data["equity_row"]
    pos = data["positions"]

    monthly_dd = float(eq.get("monthly_dd_pct", 0)) if eq is not None else 0
    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0
    n_pos = int(eq.get("n_positions", 0)) if eq is not None else 0

    # MDD from equity history
    mdd = 0.0
    eq_all = data["equity_all"]
    if not eq_all.empty and "equity" in eq_all.columns:
        eqs = pd.to_numeric(eq_all["equity"], errors="coerce").dropna()
        if len(eqs) > 0:
            peak = eqs.cummax()
            dd = (eqs - peak) / peak
            mdd = float(dd.min())

    # Concentration (top 3 weight)
    concentration = 0.0
    if not pos.empty and "market_value" in pos.columns:
        mv = pd.to_numeric(pos["market_value"], errors="coerce").fillna(0)
        total = mv.sum()
        if total > 0:
            top3 = mv.nlargest(3).sum()
            concentration = top3 / total

    cards = (
        _card("전체 MDD", _fp(mdd), _color(mdd)) +
        _card("월간 DD", _fp(monthly_dd), _color(monthly_dd)) +
        _card("포지션 수", f"{n_pos}종목", "#1E293B",
              "목표: 20종목") +
        _card("집중도 (Top3)", f"{concentration*100:.1f}%",
              "#EF4444" if concentration > 0.30 else "#1E293B",
              "상위 3종목 비중")
    )
    return _section("리스크 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_system(data: dict) -> str:
    eq = data["equity_row"]

    pf = int(eq.get("price_fail_count", 0)) if eq is not None else 0
    rc = int(eq.get("reconcile_corrections", 0)) if eq is not None else 0
    mo_str = str(eq.get("monitor_only", "N")) if eq is not None else "N"
    re_str = str(eq.get("rebalance_executed", "N")) if eq is not None else "N"
    risk_mode = str(eq.get("risk_mode", "NORMAL")) if eq is not None else "NORMAL"

    # Count forensic snapshots for today
    state_dir = Path(data.get("_report_dir", ".")).parent / "state"
    today_compact = data["today_str"].replace("-", "")
    critical_count = len(list(state_dir.glob(f"forensic_{today_compact}*.json"))) if state_dir.exists() else 0

    # Stale target check
    signals_dir = Path(data.get("_report_dir", ".")).parent / "data" / "signals"
    stale_target = "N/A"
    if signals_dir.exists():
        targets = sorted(signals_dir.glob("target_portfolio_*.json"), reverse=True)
        if targets:
            tname = targets[0].stem.replace("target_portfolio_", "")
            stale_target = f"{tname}"
        else:
            stale_target = "없음"

    def _sys_card(title, val, warn):
        c = "#EF4444" if warn else "#10B981"
        return _card(title, str(val), c)

    cards = (
        _sys_card("가격 실패", f"{pf}건", pf > 0) +
        _sys_card("Broker 보정", f"{rc}건", rc > 0) +
        _sys_card("Monitor Only", "예" if mo_str == "Y" else "아니오", mo_str == "Y") +
        _sys_card("리밸런스", "실행" if re_str == "Y" else "미실행", False)
    )

    detail = (f'<div style="font-size:12px;color:#94A3B8;margin-top:8px;">'
              f'Risk Mode: {risk_mode} | '
              f'Target: {stale_target} | '
              f'Critical Errors: {critical_count}건</div>')

    return _section("시스템 상태",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>{detail}')


def build_problems(problems: List[str]) -> str:
    if not problems:
        return _section("문제 탐지",
            '<div style="color:#10B981;padding:8px;">이상 없음</div>')

    items = ""
    for p in problems:
        items += (f'<div style="background:#FFFBEB;border-left:4px solid #F59E0B;'
                  f'padding:10px 14px;margin-bottom:8px;font-size:13px;'
                  f'border-radius:0 8px 8px 0;">{p}</div>')
    return _section("문제 탐지", items)


def build_market_comparison(data: dict) -> str:
    """KOSPI 대비 성과 섹션."""
    eq = data["equity_row"]
    kospi = data.get("_kospi", pd.Series(dtype=float))
    today = data["today_str"]

    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0
    k_ret = get_kospi_return(kospi, today)

    # KOSPI EOD close from equity_log (if available)
    kospi_eod = None
    if eq is not None and "kospi_close" in eq.index:
        kc = eq.get("kospi_close", "")
        if kc and str(kc) not in ("", "nan", "0.00"):
            try:
                kospi_eod = float(kc)
            except (ValueError, TypeError):
                pass

    # Fallback: try to get KOSPI close from kospi_utils
    if kospi_eod is None:
        kospi_eod_val = get_kospi_close_on(kospi, today)
        if kospi_eod_val is not None:
            kospi_eod = kospi_eod_val

    if k_ret is None:
        # Even without return, show close price if available
        if kospi_eod is not None:
            return _section("시장 대비 성과",
                f'<div style="padding:12px;">'
                f'<div style="color:#94A3B8;font-size:13px;">KOSPI 수익률 계산 불가 (전일 종가 부재)</div>'
                f'<div style="color:#94A3B8;font-size:12px;margin-top:4px;">'
                f'KOSPI EOD 종가: {kospi_eod:,.2f}</div>'
                f'<div style="color:#94A3B8;font-size:11px;margin-top:2px;">'
                f'절대수익/내부 리스크 기준으로만 해석 필요</div>'
                f'</div>')
        return _section("시장 대비 성과",
            '<div style="padding:12px;">'
            '<div style="color:#94A3B8;font-size:13px;">시장 비교 데이터 부재 (비거래일 또는 데이터 미수신)</div>'
            '<div style="color:#94A3B8;font-size:12px;margin-top:4px;">'
            '상대성과 평가 보류 — 절대수익/내부 리스크 기준으로만 해석 필요</div>'
            '</div>')

    excess, label = compute_excess_return(daily_pnl, k_ret)

    label_color = {"Outperform": "#10B981", "Underperform": "#EF4444", "In-line": "#94A3B8", "N/A": "#94A3B8"}
    lc = label_color.get(label, "#94A3B8")

    kospi_sub = f"종가 {kospi_eod:,.2f}" if kospi_eod else ""

    cards = (
        _card("포트폴리오", _fp(daily_pnl), _color(daily_pnl)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret), kospi_sub) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>')
    )

    basis = ('<div style="font-size:10px;color:#94A3B8;margin-top:6px;">'
             'KOSPI 수익률 = 당일 종가 / 전일 종가 - 1 | '
             '포트폴리오 수익률 = EOD 총자산 / 전일 종가 총자산 - 1</div>')

    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>{basis}')


def build_cost(data: dict) -> str:
    cost_today = compute_cost(data["trades"])

    # Cumulative cost from all trades
    trades_all = _load_csv(Path(data.get("_report_dir", ".")) / "trades.csv")
    cum_cost = 0.0
    if not trades_all.empty and "cost" in trades_all.columns:
        if "code" in trades_all.columns:
            trades_all = trades_all[trades_all["code"] != "REBALANCE"]
        cum_cost = float(pd.to_numeric(trades_all["cost"], errors="coerce").fillna(0).sum())

    pnl = compute_pnl_breakdown(data["positions"], data["closes"])
    daily_pnl_total = pnl["total"]

    # Cumulative PnL from equity
    eq = data["equity_row"]
    equity = float(eq.get("equity", data["initial_cash"])) if eq is not None else data["initial_cash"]
    cum_pnl = equity - data["initial_cash"]

    # Ratios
    daily_ratio = (cost_today["total"] / abs(daily_pnl_total) * 100) if daily_pnl_total != 0 else 0
    cum_ratio = (cum_cost / abs(cum_pnl) * 100) if cum_pnl != 0 else 0

    if cost_today["total"] == 0 and cum_cost == 0:
        return _section("비용 분석",
            '<div style="color:#94A3B8;padding:12px;">당일 비용 없음</div>')

    cards = (
        _card("당일 비용", _fk(cost_today["total"]) + "원", "#1E293B",
              f"매수 {cost_today['n_buys']}건 / 매도 {cost_today['n_sells']}건") +
        _card("누적 비용", _fk(cum_cost) + "원", "#1E293B") +
        _card("당일 비용/손익", f"{daily_ratio:.1f}%",
              "#EF4444" if daily_ratio > 50 else "#1E293B",
              "비용 잠식률") +
        _card("누적 비용/손익", f"{cum_ratio:.1f}%",
              "#EF4444" if cum_ratio > 30 else "#1E293B",
              "비용 잠식률")
    )
    return _section("비용 분석",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_pnl_attribution(data: dict) -> str:
    """손익 원인 분석: 수익/손실 기여 top3 + 시스템 영향."""
    pos = data["positions"]
    eq = data["equity_row"]

    if pos.empty:
        return _section("손익 원인 분석",
            '<div style="color:#94A3B8;padding:12px;">분석 대상 포지션 없음</div>')

    pos_sorted = pos.copy()
    pos_sorted["pnl_amount"] = pd.to_numeric(pos_sorted["pnl_amount"], errors="coerce").fillna(0)

    # Top 3 winners
    winners = pos_sorted[pos_sorted["pnl_amount"] > 0].nlargest(3, "pnl_amount")
    losers = pos_sorted[pos_sorted["pnl_amount"] < 0].nsmallest(3, "pnl_amount")

    parts = []

    def _stock_list(df, label, icon):
        if df.empty:
            return f'<div style="font-size:13px;color:#94A3B8;margin:4px 0;">{label}: 없음</div>'
        items = []
        for _, r in df.iterrows():
            p = float(r.get("pnl_pct", 0))
            a = float(r["pnl_amount"])
            code = str(r.get("code", ""))
            name = resolve_stock_name(code)
            items.append(f'{name} {_fp(p)} ({_fk(a)}원)')
        return (f'<div style="font-size:13px;margin:4px 0;">'
                f'{icon} {label}: {" / ".join(items)}</div>')

    parts.append(_stock_list(winners, "수익 기여 TOP3", "&#9650;"))
    parts.append(_stock_list(losers, "손실 기여 TOP3", "&#9660;"))

    # System impact
    pf = int(eq.get("price_fail_count", 0)) if eq is not None else 0
    rc = int(eq.get("reconcile_corrections", 0)) if eq is not None else 0
    mo = str(eq.get("monitor_only", "N")) == "Y" if eq is not None else False

    if pf == 0 and rc == 0 and not mo:
        impact = "없음"
        ic = "#10B981"
    elif pf > 3 or rc > 3 or mo:
        impact = "큼"
        ic = "#EF4444"
    else:
        impact = "일부 있음"
        ic = "#F59E0B"

    parts.append(f'<div style="font-size:13px;margin-top:8px;">'
                 f'시스템 영향: <span style="font-weight:600;color:{ic};">{impact}</span>'
                 f' (price_fail={pf}, reconcile={rc}'
                 f'{", monitor_only" if mo else ""})</div>')

    return _section("손익 원인 분석", "\n".join(parts))


def build_auto_verdict(verdict: str, verdict_kr: str, vcolor: str) -> str:
    """자동 판단 + 다음 액션 섹션."""
    actions = {
        "NORMAL": "기존 전략 유지",
        "CAUTION": "로그 점검 및 익일 위험 종목 우선 확인",
        "DANGER": "신규 진입 중단 검토 및 시스템 상태 우선 점검",
    }
    bg_map = {"NORMAL": "#DCFCE7", "CAUTION": "#FEF3C7", "DANGER": "#FEE2E2"}
    color_map = {"NORMAL": "#10B981", "CAUTION": "#F59E0B", "DANGER": "#EF4444"}

    action = actions.get(verdict, actions["NORMAL"])
    bg = bg_map.get(verdict, "#DCFCE7")
    vc = color_map.get(verdict, vcolor)

    badge = (f'<div style="text-align:center;margin:8px 0 16px;">'
             f'<span style="display:inline-block;padding:10px 32px;border-radius:20px;'
             f'font-size:18px;font-weight:700;color:{vc};background:{bg};'
             f'letter-spacing:1px;">'
             f'{verdict_kr}</span></div>')

    action_html = (f'<div style="font-size:14px;text-align:center;padding:10px 16px;'
                   f'background:#F8FAFC;border-radius:8px;color:#1E293B;">'
                   f'다음 액션: <b>{action}</b></div>')

    return _section("자동 판단", badge + action_html)


def build_changes(data: dict) -> str:
    eq = data["equity_row"]
    pos = data["positions"]
    prev_codes = data["prev_position_codes"]

    equity = float(eq.get("equity", data["initial_cash"])) if eq is not None else data["initial_cash"]
    cash = float(eq.get("cash", data["initial_cash"])) if eq is not None else data["initial_cash"]
    cash_ratio = (cash / equity * 100) if equity > 0 else 100

    cur_codes = set(pos["code"].astype(str)) if not pos.empty else set()
    new_codes = sorted(cur_codes - prev_codes)
    removed_codes = sorted(prev_codes - cur_codes)

    parts = [f'<div style="font-size:13px;">현금 비중: <b>{cash_ratio:.1f}%</b> '
             f'({_fk(cash)}원)</div>']

    if new_codes:
        new_labels = [f"{c}({resolve_stock_name(c)})" for c in new_codes]
        parts.append(f'<div style="font-size:13px;margin-top:6px;">'
                     f'<span style="color:#3B82F6;font-weight:600;">신규 편입:</span> '
                     f'{", ".join(new_labels)}</div>')
    if removed_codes:
        rem_labels = [f"{c}({resolve_stock_name(c)})" for c in removed_codes]
        parts.append(f'<div style="font-size:13px;margin-top:4px;">'
                     f'<span style="color:#EF4444;font-weight:600;">제거:</span> '
                     f'{", ".join(rem_labels)}</div>')
    if not new_codes and not removed_codes and prev_codes:
        parts.append('<div style="font-size:13px;margin-top:4px;color:#94A3B8;">'
                     '전일 대비 포지션 변동 없음</div>')
    if not prev_codes:
        parts.append('<div style="font-size:13px;margin-top:4px;color:#94A3B8;">'
                     '전일 데이터 없음 (비교 불가)</div>')

    return _section("현금 비중 / 포지션 변동", "\n".join(parts))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Intraday Chart Sections
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_intraday_chart(data: dict, intraday_dir=None) -> str:
    """Portfolio-level intraday return curve (SVG)."""
    if intraday_dir is None:
        return ""
    intraday_dir = Path(intraday_dir)
    if not intraday_dir.exists():
        return ""

    try:
        from data.intraday_collector import IntradayCollector
        from report.intraday_chart import render_portfolio_intraday_svg

        bars = IntradayCollector.load_all_for_date(
            intraday_dir, data["today_str"])
        if not bars:
            return ""

        # Compute weights from positions
        pos = data["positions"]
        weights = {}
        if not pos.empty and "market_value" in pos.columns:
            mv = pd.to_numeric(pos["market_value"], errors="coerce").fillna(0)
            total = mv.sum()
            if total > 0:
                for _, r in pos.iterrows():
                    code = str(r["code"]).zfill(6)
                    weights[code] = float(r["market_value"]) / total
        if not weights:
            weights = {code: 1.0 / len(bars) for code in bars}

        # Load KOSPI minute bars for overlay (if available)
        kospi_bars = None
        try:
            import json as _json
            today_compact = data["today_str"].replace("-", "")
            report_dir = Path(data.get("_report_dir", "."))
            kb_path = report_dir / f"kospi_minute_{today_compact}.json"
            if kb_path.exists():
                kospi_bars = _json.loads(kb_path.read_text(encoding="utf-8"))
        except Exception:
            pass

        svg = render_portfolio_intraday_svg(bars, weights, kospi_bars=kospi_bars)
        if not svg:
            return ""
        legend = ('<div style="font-size:10px;color:#90a4ae;margin-top:4px;">'
                  '실선: 포트폴리오 | 점선: KOSPI</div>' if kospi_bars else "")
        return _section("장중 수익률 추이", svg + legend)
    except Exception as e:
        logger.warning(f"Intraday chart failed: {e}")
        return ""


def build_risk_minicharts(data: dict, intraday_dir=None) -> str:
    """Mini-charts for positions within 5% of trail stop trigger."""
    if intraday_dir is None:
        return ""
    intraday_dir = Path(intraday_dir)
    if not intraday_dir.exists():
        return ""

    pos = data["positions"]
    if pos.empty:
        return ""

    try:
        from data.intraday_collector import IntradayCollector
        from report.intraday_chart import render_stock_mini_svg

        pos_risk = compute_position_risk(pos)
        at_risk = pos_risk[pos_risk["trail_gap_pct"] <= 5.0]
        if at_risk.empty:
            return ""

        charts = []
        for _, r in at_risk.iterrows():
            code = str(r["code"]).zfill(6)
            bars = IntradayCollector.load_bars_for_date(
                intraday_dir, code, data["today_str"])
            if bars.empty:
                continue
            name = resolve_stock_name(code)
            trail = float(r.get("trail_stop_price", 0))
            hwm = float(r.get("high_watermark", 0))
            avg = float(r.get("avg_price", 0))
            svg = render_stock_mini_svg(
                bars, code, name,
                trail_stop_price=trail,
                high_watermark=hwm,
                avg_price=avg)
            if svg:
                charts.append(svg)

        if not charts:
            return ""

        grid = ('<div style="display:flex;flex-wrap:wrap;gap:8px;">'
                + "".join(charts) + '</div>')
        return _section("위험 종목 장중 차트", grid)
    except Exception as e:
        logger.warning(f"Risk minicharts failed: {e}")
        return ""


def build_intraday_analytics(intraday_summary: dict) -> str:
    """Build intraday analytics section from analyzer output."""
    if not intraday_summary or intraday_summary.get("n_stocks", 0) == 0:
        return ""

    n = intraday_summary["n_stocks"]
    risk_score = intraday_summary.get("risk_score", 0)
    vwap_below = intraday_summary.get("vwap_below_count", 0)
    spikes_2x = intraday_summary.get("total_volume_spikes_2x", 0)
    spikes_3x = intraday_summary.get("total_volume_spikes_3x", 0)
    near_trail = intraday_summary.get("near_trail_count", 0)
    avg_dd = intraday_summary.get("avg_max_dd_pct", 0)
    is_partial = intraday_summary.get("is_partial_session", False)

    # Risk score color
    if risk_score >= 70:
        rs_color = "#EF4444"
        rs_label = "HIGH"
    elif risk_score >= 40:
        rs_color = "#F59E0B"
        rs_label = "MED"
    else:
        rs_color = "#10B981"
        rs_label = "LOW"

    partial_tag = ' <span style="color:#F59E0B;font-size:11px;">(장중)</span>' if is_partial else ""

    # Cards row
    cards = f"""<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px;">
        {_card("Risk Score", f"{risk_score:.0f}", rs_color, f"(참고용 · {rs_label})")}
        {_card("VWAP 하회", f"{vwap_below}/{n}", "#3B82F6" if vwap_below > n//2 else "#94A3B8")}
        {_card("Spike 2x/3x", f"{spikes_2x}/{spikes_3x}", "#F59E0B" if spikes_3x > 5 else "#94A3B8", "개장5분 제외")}
        {_card("Trail 근접", str(near_trail), "#EF4444" if near_trail > 0 else "#10B981", "DD ≤ -10%")}
    </div>"""

    # Top 5 worst drawdown table
    per_stock = intraday_summary.get("per_stock", [])
    analyzed = [r for r in per_stock if r.get("n_bars", 0) >= 5]
    analyzed.sort(key=lambda x: x.get("max_intraday_dd_pct", 0))
    top5 = analyzed[:5]

    rows_html = ""
    for r in top5:
        dd = r.get("max_intraday_dd_pct", 0)
        dd_color = "#EF4444" if dd <= -5 else "#F59E0B" if dd <= -3 else "#1E293B"
        vwap_pct = r.get("close_vs_vwap_pct", 0)
        vwap_color = "#3B82F6" if vwap_pct < 0 else "#FF4757" if vwap_pct > 0 else "#1E293B"
        drop5 = r.get("max_5m_drop_pct", 0)
        spikes = r.get("volume_spike_2x", r.get("volume_spike_count", 0))
        near = r.get("near_trail_stop", False)
        trail_badge = (' <span style="background:#EF4444;color:#fff;'
                       'padding:2px 6px;border-radius:4px;font-size:10px;font-weight:600;">'
                       'TRAIL</span>') if near else ""

        sname = resolve_stock_name(r['code'])
        rows_html += f"""<tr>
            <td style="font-weight:600;">{sname}{trail_badge} <span style="font-size:9px;color:#94A3B8;">{r['code']}</span></td>
            <td style="color:{dd_color};font-weight:700;">{dd:.2f}%</td>
            <td style="color:#64748B;">{r.get('max_dd_time', '')}</td>
            <td style="color:{vwap_color};">{vwap_pct:+.2f}%</td>
            <td>{drop5:.2f}%</td>
            <td style="text-align:center;">{spikes}</td>
        </tr>"""

    table = f"""<div style="border-radius:8px;overflow:hidden;border:1px solid #E2E8F0;">
    <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead>
            <tr style="background:#F1F5F9;">
                <th style="padding:8px;text-align:left;color:#475569;font-weight:600;">종목</th>
                <th style="padding:8px;text-align:right;color:#475569;font-weight:600;">장중DD</th>
                <th style="padding:8px;text-align:center;color:#475569;font-weight:600;">시각</th>
                <th style="padding:8px;text-align:right;color:#475569;font-weight:600;">vs VWAP</th>
                <th style="padding:8px;text-align:right;color:#475569;font-weight:600;">5m Drop</th>
                <th style="padding:8px;text-align:center;color:#475569;font-weight:600;">Spikes</th>
            </tr>
        </thead>
        <tbody>{rows_html}</tbody>
    </table></div>
    <div style="font-size:10px;color:#94A3B8;margin-top:6px;">
        장중DD: 고점 대비 종가 기준 | 5m Drop: 종가-종가 5분 | Spike: 직전5분 평균 대비 2x (개장5분 제외){partial_tag}
    </div>"""

    return _section("장중 분석 (Intraday Analytics)", cards + table)


def _build_eod_source_summary(eod_price_src) -> str:
    """Build EOD price source quality summary section."""
    if not eod_price_src:
        return ""
    src_counts = {}
    for _, src in eod_price_src.values():
        src_counts[src] = src_counts.get(src, 0) + 1
    total = sum(src_counts.values())
    verified = src_counts.get("intraday_last_close", 0) + \
               src_counts.get("eod_master_close", 0)
    quality_pct = (verified / total * 100) if total > 0 else 0
    color = "#10B981" if quality_pct >= 90 else "#F59E0B" if quality_pct >= 70 else "#EF4444"
    rows = ""
    labels = {
        "intraday_last_close": ("실시간 분봉", "#10B981"),
        "eod_master_close": ("장마감 종가", "#3B82F6"),
        "provider_cached": ("캐시 (미검증)", "#F59E0B"),
        "position_fallback": ("포지션 저장가", "#F59E0B"),
        "unavailable": ("조회 실패", "#EF4444"),
    }
    for src_key, (label, scolor) in labels.items():
        cnt = src_counts.get(src_key, 0)
        if cnt > 0:
            rows += (f'<tr><td style="color:{scolor};font-weight:600">'
                     f'{label}</td><td>{cnt}</td></tr>\n')
    return _section("EOD 가격 소스 품질", f"""
<div style="font-size:28px;font-weight:700;color:{color};margin:8px 0;">
    {quality_pct:.0f}% verified ({verified}/{total})
</div>
<div style="border-radius:8px;overflow:hidden;border:1px solid #E2E8F0;">
<table style="width:100%;border-collapse:collapse;font-size:13px;">
<thead><tr style="background:#F1F5F9;">
<th style="padding:8px;text-align:left;color:#475569;font-weight:600;">소스</th>
<th style="padding:8px;text-align:left;color:#475569;font-weight:600;">종목 수</th>
</tr></thead>
<tbody>{rows}</tbody></table></div>""")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Assembly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_daily_html(data: dict, config, today_str: str,
                        intraday_dir=None, intraday_summary=None,
                        eod_price_src=None, broker_summary=None) -> str:
    # Inject broker_summary into data so build_* functions can access it
    if broker_summary:
        data["broker_summary"] = broker_summary

    eq = data["equity_row"]
    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0
    pf = int(eq.get("price_fail_count", 0)) if eq is not None else 0
    rc = int(eq.get("reconcile_corrections", 0)) if eq is not None else 0
    mo = str(eq.get("monitor_only", "N")) == "Y" if eq is not None else False

    verdict, verdict_kr, vcolor = compute_verdict(daily_pnl, pf, rc, mo)
    problems = detect_problems(data)

    # Parse log events for operations + anomaly + log summary
    # _report_dir = report/output, parent.parent = Gen04/
    log_dir = Path(data.get("_report_dir", ".")).parent.parent / "logs"
    log_events = _parse_log_events(log_dir, today_str)
    ops = _extract_ops_status(log_events, eq)
    _is = intraday_summary or {}

    sections = [
        build_ops_status(data, ops),                    # 운영 상태 카드
        build_summary(data, config, verdict, verdict_kr, vcolor),
        build_basis_line(data, today_str),              # 기준 시각 / 계산 기준
        build_today_verdict(data, config, verdict_kr, ops, _is),  # 오늘의 판단 3줄
        build_risk_summary(_is),                        # 리스크 요약 카드
        build_anomaly(data, _is, log_events),           # 이상 탐지
        build_performance(data, config),
        build_intraday_chart(data, intraday_dir),       # 장중 수익률 곡선
        build_intraday_flow(_is, log_events),           # NEW: 장중 흐름 요약
        build_intraday_analytics(_is),                  # 장중 분석 (Phase 1)
        build_market_comparison(data),                  # KOSPI 대비 성과
        build_cost(data),                               # 비용 분석
        build_trades(data),
        build_positions(data, _is),                      # 종목명 + VWAP/DD 포함
        build_risk_minicharts(data, intraday_dir),      # 위험 종목 장중 차트
        build_pnl_attribution(data),                    # 손익 원인 분석
        build_risk(data),
        build_system(data),                             # forensic, stale target
        _build_eod_source_summary(eod_price_src),       # EOD 가격 소스 품질
        build_log_events(log_events),                   # NEW: 로그 핵심 이벤트
        build_problems(problems),
        build_changes(data),
        build_strategy_verdict(data, _is, verdict, ops),  # NEW: 전략 해석
        build_auto_verdict(verdict, verdict_kr, vcolor),
    ]

    body = "\n".join(s for s in sections if s)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
            _css = ""  # fallback: inline CSS below
            _js = ""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Q-TRON Gen4 Daily Report — {today_str}</title>
<style>
{_css if _css else '''
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
    font-family: Pretendard,Malgun Gothic,-apple-system,sans-serif;
    background: #F1F5F9;
    margin: 0; padding: 0; color: var(--text); line-height: 1.6;
}}
'''}
/* ── Legacy compat overrides ── */
.g4-header .g4-brand {{
    font-size: 10px; color: rgba(255,255,255,0.40); letter-spacing: 3px;
    text-transform: uppercase; margin-bottom: 10px; position: relative;
    font-weight: 500;
}}
.g4-header .g4-date {{
    font-size: 15px; color: rgba(255,255,255,0.65); position: relative;
}}
/* Container + section + card + table + alert + timeline + print:
   All handled by premium_style.py — see get_premium_css().
   Only report-specific overrides below. */
.g4-card div:nth-child(2) {{ font-size: 28px; }}
td.g4-profit-cell {{ background: rgba(220,38,38,0.06); }}
td.g4-loss-cell {{ background: rgba(37,99,235,0.06); }}
</style>
<script>
{_js if _js else "/* premium_style.js not loaded — sort disabled */"}

</script>
</head>
<body>
<div class="g4-header">
    <div class="g4-brand">Q-TRON GEN4 DAILY REPORT</div>
    <h1>Daily Performance Report</h1>
    <div class="g4-date">{today_str}</div>
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

def generate_daily_report(report_dir: Path, config,
                           today_str: str = "",
                           intraday_dir=None,
                           intraday_summary=None,
                           eod_price_src=None,
                           broker_summary=None) -> Optional[Path]:
    """Generate daily HTML report. Returns path or None."""
    today_str = today_str or date.today().strftime("%Y-%m-%d")
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    # Auto-detect intraday_dir from config if not provided
    if intraday_dir is None and hasattr(config, "INTRADAY_DIR"):
        intraday_dir = config.INTRADAY_DIR

    try:
        data = load_daily_data(report_dir, today_str, config.INITIAL_CASH)
        # Load KOSPI benchmark
        if hasattr(config, "INDEX_FILE") and config.INDEX_FILE.exists():
            data["_kospi"] = load_kospi_close(config.INDEX_FILE)

        # Auto-load intraday_summary if not provided
        if intraday_summary is None:
            import json as _json
            is_date = today_str.replace("-", "")
            is_path = report_dir / f"intraday_summary_{is_date}.json"
            if is_path.exists():
                try:
                    intraday_summary = _json.loads(is_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

        html = generate_daily_html(data, config, today_str,
                                    intraday_dir=intraday_dir,
                                    intraday_summary=intraday_summary,
                                    eod_price_src=eod_price_src,
                                    broker_summary=broker_summary)

        fname = f"daily_{today_str.replace('-', '')}.html"
        path = report_dir / fname
        path.write_text(html, encoding="utf-8")
        logger.info(f"Daily report generated: {path}")
        return path
    except Exception as e:
        logger.error(f"Daily report generation failed: {e}")
        return None


if __name__ == "__main__":
    import argparse
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import Gen4Config

    parser = argparse.ArgumentParser(description="Gen4 Daily Report")
    parser.add_argument("--date", default=date.today().strftime("%Y-%m-%d"),
                        help="Report date (YYYY-MM-DD)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    config = Gen4Config()
    path = generate_daily_report(config.REPORT_DIR, config, args.date)
    if path:
        print(f"Report: {path}")
    else:
        print("Report generation failed.")
        sys.exit(1)
