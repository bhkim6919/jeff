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
    # Exclude REBALANCE summary rows from trades
    if not trades_today.empty and "code" in trades_today.columns:
        trades_today = trades_today[trades_today["code"] != "REBALANCE"]
    closes_today = filt(closes_df)
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
        return ("DANGER", "위험", "#d32f2f")
    if daily_pnl <= -0.02 or price_fail > 0 or reconcile > 0:
        return ("CAUTION", "주의", "#f57f17")
    return ("NORMAL", "정상", "#2e7d32")


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
        return "#d32f2f"   # red = profit (Korean convention)
    if v < 0:
        return "#1565c0"   # blue = loss (Korean convention)
    return "#78909c"


def _card(title: str, value: str, color: str = "#333", sub: str = "") -> str:
    return f"""<div style="flex:1;min-width:140px;background:#fff;border-radius:8px;
        padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.12);text-align:center;">
        <div style="font-size:12px;color:#78909c;margin-bottom:4px;">{title}</div>
        <div style="font-size:22px;font-weight:700;color:{color};">{value}</div>
        {f'<div style="font-size:11px;color:#aaa;margin-top:2px;">{sub}</div>' if sub else ''}
    </div>"""


def _section(title: str, content: str) -> str:
    return f"""<div style="margin-bottom:24px;">
        <h2 style="font-size:16px;color:#1a237e;border-bottom:2px solid #1565c0;
            padding-bottom:6px;margin-bottom:12px;">{title}</h2>
        {content}
    </div>"""


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Section Builders
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def build_summary(data: dict, config, verdict, verdict_kr, vcolor) -> str:
    eq = data["equity_row"]
    movers = find_top_movers(data["positions"], 3)

    if eq is None:
        equity = config.INITIAL_CASH
        daily_pnl = 0
    else:
        equity = float(eq.get("equity", config.INITIAL_CASH))
        daily_pnl = float(eq.get("daily_pnl_pct", 0))

    cum_pnl = (equity / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0
    n_pos = int(eq.get("n_positions", 0)) if eq is not None else 0

    lines = [f"당일 수익률 <b>{_fp(daily_pnl)}</b>, "
             f"총 자산 <b>{_fk(equity)}</b>원 "
             f"(누적 {_fp(cum_pnl)})"]

    if movers:
        parts = [f"{resolve_stock_name(m['code'])}({_fp(m['pnl_pct'])})" for m in movers]
        lines.append(f"주요 손익: {', '.join(parts)}")

    lines.append(f"포지션 {n_pos}종목 보유")

    badge = (f'<span style="display:inline-block;padding:4px 14px;border-radius:12px;'
             f'font-size:14px;font-weight:700;color:{vcolor};'
             f'background:{"#c8e6c9" if verdict=="NORMAL" else "#fff9c4" if verdict=="CAUTION" else "#ffcdd2"};">'
             f'{verdict_kr}</span>')

    summary_html = "<br>".join(lines)
    return f"""<div style="display:flex;justify-content:space-between;align-items:flex-start;
        background:#f5f5f5;border-radius:8px;padding:16px;margin-bottom:20px;">
        <div style="font-size:14px;line-height:1.7;">{summary_html}</div>
        <div>{badge}</div>
    </div>"""


def build_performance(data: dict, config) -> str:
    eq = data["equity_row"]
    pnl = compute_pnl_breakdown(data["positions"], data["closes"])

    if eq is None:
        daily_pnl = 0.0
        equity = config.INITIAL_CASH
    else:
        daily_pnl = float(eq.get("daily_pnl_pct", 0))
        equity = float(eq.get("equity", config.INITIAL_CASH))

    cum_pnl = (equity / data["initial_cash"] - 1) if data["initial_cash"] > 0 else 0

    cards = (
        _card("당일 수익률", _fp(daily_pnl), _color(daily_pnl)) +
        _card("누적 수익률", _fp(cum_pnl), _color(cum_pnl),
              f"기준 {_fk(data['initial_cash'])}") +
        _card("실현 손익", _fk(pnl["realized"]), _color(pnl["realized"]), "청산 확정") +
        _card("미실현 손익", _fk(pnl["unrealized"]), _color(pnl["unrealized"]), "보유 중")
    )
    return _section("성과 지표",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


def build_trades(data: dict) -> str:
    trades = data["trades"]
    closes = data["closes"]

    if trades.empty and closes.empty:
        return _section("거래 요약",
            '<div style="color:#aaa;padding:12px;">당일 거래 없음</div>')

    cost = compute_cost(trades)
    header = (f'<div style="margin-bottom:8px;">'
              f'<span style="background:#e3f2fd;padding:3px 10px;border-radius:10px;'
              f'font-size:13px;margin-right:8px;">매수 {cost["n_buys"]}건</span>'
              f'<span style="background:#fce4ec;padding:3px 10px;border-radius:10px;'
              f'font-size:13px;">매도 {cost["n_sells"]}건</span></div>')

    # Top 5 trades table
    rows_html = ""
    if not trades.empty:
        top = trades.head(5)
        for _, r in top.iterrows():
            side = str(r.get("side", ""))
            sc = "#d32f2f" if side == "BUY" else "#1565c0"
            price = float(r.get("price", 0))
            qty = int(r.get("quantity", 0))
            code = str(r.get("code", ""))
            name = resolve_stock_name(code)
            rows_html += (f'<tr><td>{code}</td>'
                          f'<td style="font-size:11px;color:#555;">{name}</td>'
                          f'<td style="color:{sc};font-weight:600;">{side}</td>'
                          f'<td style="text-align:right;">{qty:,}</td>'
                          f'<td style="text-align:right;">{price:,.0f}</td></tr>')

    # Closed positions
    close_rows = ""
    if not closes.empty:
        for _, r in closes.iterrows():
            pnl = float(r.get("pnl_pct", 0))
            amt = float(r.get("pnl_amount", 0))
            code = str(r.get("code", ""))
            name = resolve_stock_name(code)
            close_rows += (f'<tr><td>{code}</td>'
                           f'<td style="font-size:11px;color:#555;">{name}</td>'
                           f'<td>{r.get("exit_reason","")}</td>'
                           f'<td style="color:{_color(pnl)};text-align:right;">{_fp(pnl)}</td>'
                           f'<td style="color:{_color(amt)};text-align:right;">{_fk(amt)}</td></tr>')

    table_style = 'style="width:100%;border-collapse:collapse;font-size:13px;"'
    th_style = 'style="text-align:left;padding:6px 8px;border-bottom:1px solid #e0e0e0;color:#78909c;font-size:12px;"'
    td_base = 'style="padding:6px 8px;border-bottom:1px solid #f5f5f5;"'

    content = header
    if rows_html:
        content += f"""<table {table_style}>
            <tr><th {th_style}>종목</th><th {th_style}>종목명</th><th {th_style}>방향</th>
            <th {th_style} style="text-align:right;">수량</th>
            <th {th_style} style="text-align:right;">가격</th></tr>
            {rows_html}</table>"""
    if close_rows:
        content += f"""<div style="margin-top:12px;font-size:13px;color:#78909c;">청산 종목</div>
            <table {table_style}>
            <tr><th {th_style}>종목</th><th {th_style}>종목명</th><th {th_style}>사유</th>
            <th {th_style} style="text-align:right;">손익률</th>
            <th {th_style} style="text-align:right;">손익액</th></tr>
            {close_rows}</table>"""

    return _section("거래 요약", content)


def build_positions(data: dict) -> str:
    pos = data["positions"]
    if pos.empty:
        return _section("미청산 포지션",
            '<div style="color:#aaa;padding:12px;">미보유 (현금 100%)</div>')

    pos = compute_position_risk(pos)

    # Resolve Korean stock names
    codes = pos["code"].astype(str).tolist()
    names = resolve_names_bulk(codes)

    th = 'style="text-align:left;padding:6px 8px;border-bottom:1px solid #e0e0e0;color:#78909c;font-size:11px;"'
    td = 'style="padding:5px 8px;font-size:12px;"'
    tdr = 'style="padding:5px 8px;text-align:right;font-size:12px;"'
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
        rf = str(r.get("risk_flag", "정상"))
        code = str(r.get("code", ""))
        stock_name = names.get(code.zfill(6), code)

        if gap <= 2:
            bg = "#ffebee"
            rf_color = "#d32f2f"
        elif gap <= 5:
            bg = "#fff8e1"
            rf_color = "#f57f17"
        else:
            bg = "#fff"
            rf_color = "#2e7d32"

        rows += (f'<tr style="background:{bg};">'
                 f'<td {td}>{i}</td>'
                 f'<td {td} style="font-weight:600;">{stock_name}'
                 f'<span style="font-size:9px;color:#aaa;margin-left:4px;">{code}</span></td>'
                 f'<td {tdr}>{qty:,}</td>'
                 f'<td {tdr}>{avg:,.0f}</td>'
                 f'<td {tdr}>{cur:,.0f}</td>'
                 f'<td {tdr} style="color:{_color(pnl)};">{_fp(pnl)}</td>'
                 f'<td {tdr} style="color:#90a4ae;font-size:10px;">{_fp(est_cost)}</td>'
                 f'<td {tdr} style="color:{_color(net_pnl)};font-weight:600;">{_fp(net_pnl)}</td>'
                 f'<td {tdr} style="color:{_color(amt)};">{_fk(amt)}</td>'
                 f'<td {tdr}>{hd}일</td>'
                 f'<td {tdr} style="color:{rf_color};">{gap:.1f}%</td>'
                 f'<td {td} style="text-align:center;color:{rf_color};font-weight:600;">{rf}</td>'
                 f'</tr>')

    total_mv = pd.to_numeric(pos["market_value"], errors="coerce").sum()
    total_pnl = pd.to_numeric(pos["pnl_amount"], errors="coerce").sum()

    return _section("미청산 포지션",
        f"""<div style="font-size:12px;color:#78909c;margin-bottom:4px;">
            trail_gap 기준 위험순 정렬 | gap = (현재가/청산가 - 1)%</div>
        <div style="overflow-x:auto;">
        <table style="width:100%;border-collapse:collapse;font-size:13px;min-width:800px;">
        <tr><th {th}>#</th><th {th}>종목명</th>
        <th {th} style="text-align:right;">수량</th>
        <th {th} style="text-align:right;">평균가</th>
        <th {th} style="text-align:right;">현재가</th>
        <th {th} style="text-align:right;">손익률</th>
        <th {th} style="text-align:right;font-size:10px;">비용</th>
        <th {th} style="text-align:right;">순손익률</th>
        <th {th} style="text-align:right;">손익액</th>
        <th {th} style="text-align:right;">보유</th>
        <th {th} style="text-align:right;">Trail Gap</th>
        <th {th} style="text-align:center;">상태</th></tr>
        {rows}
        <tr style="border-top:2px solid #1565c0;font-weight:700;">
        <td colspan="8" style="padding:6px 8px;">합계</td>
        <td style="padding:6px 8px;text-align:right;color:{_color(total_pnl)};">{_fk(total_pnl)}</td>
        <td colspan="3" style="padding:6px 8px;text-align:right;">{total_mv:,.0f}</td></tr>
        </table></div>""")


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
        _card("포지션 수", f"{n_pos}종목", "#333",
              "목표: 20종목") +
        _card("집중도 (Top3)", f"{concentration*100:.1f}%",
              "#d32f2f" if concentration > 0.30 else "#333",
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
        c = "#d32f2f" if warn else "#2e7d32"
        return _card(title, str(val), c)

    cards = (
        _sys_card("가격 실패", f"{pf}건", pf > 0) +
        _sys_card("Broker 보정", f"{rc}건", rc > 0) +
        _sys_card("Monitor Only", "예" if mo_str == "Y" else "아니오", mo_str == "Y") +
        _sys_card("리밸런스", "실행" if re_str == "Y" else "미실행", False)
    )

    detail = (f'<div style="font-size:12px;color:#78909c;margin-top:8px;">'
              f'Risk Mode: {risk_mode} | '
              f'Target: {stale_target} | '
              f'Critical Errors: {critical_count}건</div>')

    return _section("시스템 상태",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>{detail}')


def build_problems(problems: List[str]) -> str:
    if not problems:
        return _section("문제 탐지",
            '<div style="color:#2e7d32;padding:8px;">이상 없음</div>')

    items = ""
    for p in problems:
        items += (f'<div style="background:#fff8e1;border-left:4px solid #ffc107;'
                  f'padding:8px 12px;margin-bottom:6px;font-size:13px;'
                  f'border-radius:0 4px 4px 0;">{p}</div>')
    return _section("문제 탐지", items)


def build_market_comparison(data: dict) -> str:
    """KOSPI 대비 성과 섹션."""
    eq = data["equity_row"]
    kospi = data.get("_kospi", pd.Series(dtype=float))
    today = data["today_str"]

    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0
    k_ret = get_kospi_return(kospi, today)

    if k_ret is None:
        return _section("시장 대비 성과",
            '<div style="color:#aaa;padding:12px;">KOSPI 데이터 없음 (비거래일 또는 데이터 부재)</div>')

    excess, label = compute_excess_return(daily_pnl, k_ret)

    label_color = {"Outperform": "#2e7d32", "Underperform": "#d32f2f", "In-line": "#78909c", "N/A": "#78909c"}
    lc = label_color.get(label, "#78909c")

    cards = (
        _card("포트폴리오", _fp(daily_pnl), _color(daily_pnl)) +
        _card("KOSPI", _fp(k_ret), _color(k_ret)) +
        _card("초과 수익", _fp(excess), _color(excess),
              f'<span style="color:{lc};font-weight:600;">{label}</span>')
    )
    return _section("시장 대비 성과",
        f'<div style="display:flex;gap:12px;flex-wrap:wrap;">{cards}</div>')


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
            '<div style="color:#aaa;padding:12px;">당일 비용 없음</div>')

    cards = (
        _card("당일 비용", _fk(cost_today["total"]) + "원", "#333",
              f"매수 {cost_today['n_buys']}건 / 매도 {cost_today['n_sells']}건") +
        _card("누적 비용", _fk(cum_cost) + "원", "#333") +
        _card("당일 비용/손익", f"{daily_ratio:.1f}%",
              "#d32f2f" if daily_ratio > 50 else "#333",
              "비용 잠식률") +
        _card("누적 비용/손익", f"{cum_ratio:.1f}%",
              "#d32f2f" if cum_ratio > 30 else "#333",
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
            '<div style="color:#aaa;padding:12px;">분석 대상 포지션 없음</div>')

    pos_sorted = pos.copy()
    pos_sorted["pnl_amount"] = pd.to_numeric(pos_sorted["pnl_amount"], errors="coerce").fillna(0)

    # Top 3 winners
    winners = pos_sorted[pos_sorted["pnl_amount"] > 0].nlargest(3, "pnl_amount")
    losers = pos_sorted[pos_sorted["pnl_amount"] < 0].nsmallest(3, "pnl_amount")

    parts = []

    def _stock_list(df, label, icon):
        if df.empty:
            return f'<div style="font-size:13px;color:#aaa;margin:4px 0;">{label}: 없음</div>'
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
        ic = "#2e7d32"
    elif pf > 3 or rc > 3 or mo:
        impact = "큼"
        ic = "#d32f2f"
    else:
        impact = "일부 있음"
        ic = "#f57f17"

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
    bg_map = {"NORMAL": "#c8e6c9", "CAUTION": "#fff9c4", "DANGER": "#ffcdd2"}

    action = actions.get(verdict, actions["NORMAL"])
    bg = bg_map.get(verdict, "#c8e6c9")

    badge = (f'<div style="text-align:center;margin:8px 0 16px;">'
             f'<span style="display:inline-block;padding:8px 24px;border-radius:12px;'
             f'font-size:18px;font-weight:700;color:{vcolor};background:{bg};">'
             f'{verdict_kr}</span></div>')

    action_html = (f'<div style="font-size:14px;text-align:center;padding:8px;'
                   f'background:#f5f5f5;border-radius:6px;">'
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
                     f'<span style="color:#1565c0;">신규 편입:</span> '
                     f'{", ".join(new_labels)}</div>')
    if removed_codes:
        rem_labels = [f"{c}({resolve_stock_name(c)})" for c in removed_codes]
        parts.append(f'<div style="font-size:13px;margin-top:4px;">'
                     f'<span style="color:#d32f2f;">제거:</span> '
                     f'{", ".join(rem_labels)}</div>')
    if not new_codes and not removed_codes and prev_codes:
        parts.append('<div style="font-size:13px;margin-top:4px;color:#aaa;">'
                     '전일 대비 포지션 변동 없음</div>')
    if not prev_codes:
        parts.append('<div style="font-size:13px;margin-top:4px;color:#aaa;">'
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

        svg = render_portfolio_intraday_svg(bars, weights)
        if not svg:
            return ""
        return _section("장중 수익률 추이", svg)
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
        rs_color = "#d32f2f"
        rs_label = "HIGH"
    elif risk_score >= 40:
        rs_color = "#f57c00"
        rs_label = "MED"
    else:
        rs_color = "#388e3c"
        rs_label = "LOW"

    partial_tag = ' <span style="color:#f57c00;font-size:11px;">(장중)</span>' if is_partial else ""

    # Cards row
    cards = f"""<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px;">
        {_card("Risk Score", f"{risk_score:.0f}", rs_color, f"(참고용 · {rs_label})")}
        {_card("VWAP 하회", f"{vwap_below}/{n}", "#1565c0" if vwap_below > n//2 else "#78909c")}
        {_card("Spike 2x/3x", f"{spikes_2x}/{spikes_3x}", "#f57c00" if spikes_3x > 5 else "#78909c", "개장5분 제외")}
        {_card("Trail 근접", str(near_trail), "#d32f2f" if near_trail > 0 else "#388e3c", "DD ≤ -10%")}
    </div>"""

    # Top 5 worst drawdown table
    per_stock = intraday_summary.get("per_stock", [])
    analyzed = [r for r in per_stock if r.get("n_bars", 0) >= 5]
    analyzed.sort(key=lambda x: x.get("max_intraday_dd_pct", 0))
    top5 = analyzed[:5]

    rows_html = ""
    for r in top5:
        dd = r.get("max_intraday_dd_pct", 0)
        dd_color = "#d32f2f" if dd <= -5 else "#f57c00" if dd <= -3 else "#333"
        vwap_pct = r.get("close_vs_vwap_pct", 0)
        vwap_color = "#1565c0" if vwap_pct < 0 else "#d32f2f" if vwap_pct > 0 else "#333"
        drop5 = r.get("max_5m_drop_pct", 0)
        spikes = r.get("volume_spike_2x", r.get("volume_spike_count", 0))
        near = r.get("near_trail_stop", False)
        trail_badge = (' <span style="background:#d32f2f;color:#fff;'
                       'padding:1px 5px;border-radius:3px;font-size:10px;">'
                       'TRAIL</span>') if near else ""

        sname = resolve_stock_name(r['code'])
        rows_html += f"""<tr>
            <td style="font-weight:600;">{sname}{trail_badge} <span style="font-size:9px;color:#aaa;">{r['code']}</span></td>
            <td style="color:{dd_color};font-weight:700;">{dd:.2f}%</td>
            <td>{r.get('max_dd_time', '')}</td>
            <td style="color:{vwap_color};">{vwap_pct:+.2f}%</td>
            <td>{drop5:.2f}%</td>
            <td>{spikes}</td>
        </tr>"""

    table = f"""<table style="width:100%;border-collapse:collapse;font-size:13px;">
        <thead>
            <tr style="background:#e8eaf6;color:#1a237e;">
                <th style="padding:8px;text-align:left;">종목</th>
                <th style="padding:8px;text-align:right;">장중DD</th>
                <th style="padding:8px;text-align:center;">시각</th>
                <th style="padding:8px;text-align:right;">vs VWAP</th>
                <th style="padding:8px;text-align:right;">5m Drop</th>
                <th style="padding:8px;text-align:center;">Spikes</th>
            </tr>
        </thead>
        <tbody>{rows_html}</tbody>
    </table>
    <div style="font-size:10px;color:#aaa;margin-top:6px;">
        장중DD: 고점 대비 종가 기준 | 5m Drop: 종가-종가 5분 | Spike: 직전5분 평균 대비 2x (개장5분 제외){partial_tag}
    </div>"""

    return _section("장중 분석 (Intraday Analytics)", cards + table)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTML Assembly
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def generate_daily_html(data: dict, config, today_str: str,
                        intraday_dir=None, intraday_summary=None) -> str:
    eq = data["equity_row"]
    daily_pnl = float(eq.get("daily_pnl_pct", 0)) if eq is not None else 0
    pf = int(eq.get("price_fail_count", 0)) if eq is not None else 0
    rc = int(eq.get("reconcile_corrections", 0)) if eq is not None else 0
    mo = str(eq.get("monitor_only", "N")) == "Y" if eq is not None else False

    verdict, verdict_kr, vcolor = compute_verdict(daily_pnl, pf, rc, mo)
    problems = detect_problems(data)

    sections = [
        build_summary(data, config, verdict, verdict_kr, vcolor),
        build_performance(data, config),
        build_intraday_chart(data, intraday_dir),      # 장중 수익률 곡선
        build_intraday_analytics(intraday_summary or {}),  # 장중 분석 (Phase 1)
        build_market_comparison(data),                  # KOSPI 대비 성과
        build_cost(data),                               # 비용 분석
        build_trades(data),
        build_positions(data),                          # 종목명 + 빨/파 색상
        build_risk_minicharts(data, intraday_dir),     # 위험 종목 장중 차트
        build_pnl_attribution(data),                    # 손익 원인 분석
        build_risk(data),
        build_system(data),                             # forensic, stale target
        build_problems(problems),
        build_changes(data),
        build_auto_verdict(verdict, verdict_kr, vcolor),
    ]

    body = "\n".join(s for s in sections if s)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Gen4 Daily Report — {today_str}</title>
<style>
body {{ font-family: 'Malgun Gothic','Segoe UI',sans-serif; background:#f0f2f5;
       margin:0; padding:20px; color:#333; }}
.container {{ max-width:800px; margin:0 auto; }}
h1 {{ font-size:20px; color:#1a237e; margin-bottom:16px; }}
</style>
</head>
<body>
<div class="container">
<h1>Gen4 일일 보고서 — {today_str}</h1>
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

def generate_daily_report(report_dir: Path, config,
                           today_str: str = "",
                           intraday_dir=None,
                           intraday_summary=None) -> Optional[Path]:
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
        html = generate_daily_html(data, config, today_str,
                                    intraday_dir=intraday_dir,
                                    intraday_summary=intraday_summary)

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
