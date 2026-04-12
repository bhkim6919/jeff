"""
top20_valuation.py — Gen4 Valuation Top20 Report
==================================================
Generates top20_val_YYYYMMDD.html for value-scored portfolio.

Separate from Gen4 Top20 MA report (existing stays as-is).
Selects top 20 stocks by composite Value Score from the full universe.

Includes: PER, PBR, EPS, BPS, dividend yield, market cap,
          foreign ownership, sector avg PER, MA analysis.

Scoring weights are configurable (default equal-weight rank).
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("gen4.valuation_report")


# ── Value Scoring ────────────────────────────────────────────────────────────

# Default weights (추후 논의 후 조정)
DEFAULT_WEIGHTS = {
    "w_per_rank": 0.20,         # PER 순위 (낮을수록 좋음)
    "w_pbr_rank": 0.15,         # PBR 순위 (낮을수록 좋음)
    "w_div_rank": 0.15,         # 배당수익률 순위 (높을수록 좋음)
    "w_sector_gap_rank": 0.15,  # 업종PER 괴리율 순위 (낮을수록 좋음)
    "w_foreign_rank": 0.15,     # 외국인보유 변화 순위 (높을수록 좋음)
    "w_ma_score": 0.20,         # 이평선배열 점수 (정배열일수록 좋음)
}


def score_valuation(df: pd.DataFrame,
                    weights: dict = None) -> pd.DataFrame:
    """
    Compute composite Value Score for all stocks.

    Input DataFrame must have columns:
        ticker, per, pbr, div_yield, sector_per_gap,
        foreign_ratio, alignment, pct_vs_ma200

    Returns DataFrame with added 'value_score' column, sorted descending.
    """
    w = weights or DEFAULT_WEIGHTS
    scored = df.copy()

    n = len(scored)
    if n == 0:
        return scored

    # ── PER rank (lower is better, exclude negative/zero)
    valid_per = scored["per"].apply(lambda x: x if 0 < x < 200 else np.nan)
    scored["per_rank"] = valid_per.rank(ascending=True, na_option="bottom") / n

    # ── PBR rank (lower is better, exclude negative)
    valid_pbr = scored["pbr"].apply(lambda x: x if x > 0 else np.nan)
    scored["pbr_rank"] = valid_pbr.rank(ascending=True, na_option="bottom") / n

    # ── Dividend yield rank (higher is better)
    scored["div_rank"] = scored["div_yield"].rank(ascending=False, na_option="bottom") / n

    # ── Sector PER gap rank (more negative = cheaper than sector)
    scored["sector_gap_rank"] = scored["sector_per_gap"].rank(
        ascending=True, na_option="bottom") / n

    # ── Foreign ownership rank (higher ratio = better)
    if "foreign_ratio" in scored.columns:
        scored["foreign_rank"] = scored["foreign_ratio"].rank(
            ascending=False, na_option="bottom") / n
    else:
        scored["foreign_rank"] = 0.5

    # ── MA score (alignment-based)
    def _ma_score(row):
        score = 0.0
        align = row.get("alignment", "MIXED")
        if align == "BULLISH":
            score += 0.7
        elif align == "MIXED":
            score += 0.4
        else:
            score += 0.1

        # Above MA200 bonus
        if row.get("above_ma200") is True:
            score += 0.3
        elif row.get("above_ma200") is False:
            score += 0.0
        else:
            score += 0.15

        return min(score, 1.0)

    scored["ma_score_raw"] = scored.apply(_ma_score, axis=1)
    scored["ma_rank"] = scored["ma_score_raw"].rank(
        ascending=False, na_option="bottom") / n

    # ── Composite Score (0~100)
    scored["value_score"] = (
        (1 - scored["per_rank"]) * w.get("w_per_rank", 0.2) +
        (1 - scored["pbr_rank"]) * w.get("w_pbr_rank", 0.15) +
        (1 - scored["div_rank"]) * w.get("w_div_rank", 0.15) +
        (1 - scored["sector_gap_rank"]) * w.get("w_sector_gap_rank", 0.15) +
        (1 - scored["foreign_rank"]) * w.get("w_foreign_rank", 0.15) +
        (1 - scored["ma_rank"]) * w.get("w_ma_score", 0.2)
    ) * 100

    scored = scored.sort_values("value_score", ascending=False).reset_index(drop=True)
    return scored


# ── HTML Helpers ─────────────────────────────────────────────────────────────

def _fmt_num(val, fmt=","):
    """Format number with comma separator."""
    if pd.isna(val) or val == 0:
        return "-"
    try:
        if fmt == ",":
            return f"{int(val):,}"
        elif fmt == ".1f":
            return f"{val:.1f}"
        elif fmt == ".2f":
            return f"{val:.2f}"
        elif fmt == ".1%":
            return f"{val:.1f}%"
        return str(val)
    except (ValueError, TypeError):
        return str(val)


def _val_color(val, reverse=False):
    """Color value: positive=red, negative=blue (Korean market convention)."""
    if pd.isna(val):
        return "-"
    if reverse:
        val = -val
    if val > 0:
        c = "#d32f2f"
    elif val < 0:
        c = "#1565c0"
    else:
        c = "#333"
    return f'<span style="color:{c};font-weight:bold">{val:+.1f}%</span>'


def _per_badge(per, sector_per):
    """Badge showing PER vs sector."""
    if per <= 0 or per > 200:
        return '<span style="background:#ffcdd2;padding:2px 6px;border-radius:3px;font-size:11px">적자/N/A</span>'
    if sector_per and sector_per > 0:
        ratio = per / sector_per
        if ratio < 0.7:
            bg, label = "#c8e6c9", "저평가"
        elif ratio < 1.0:
            bg, label = "#e8f5e9", "적정-"
        elif ratio < 1.3:
            bg, label = "#fff9c4", "적정+"
        else:
            bg, label = "#ffcdd2", "고평가"
        return f'<span style="background:{bg};padding:2px 6px;border-radius:3px;font-size:11px">{label} {per:.1f}</span>'
    return f'{per:.1f}'


def _align_badge(a):
    colors = {"BULLISH": "#c8e6c9", "BEARISH": "#ffcdd2", "MIXED": "#fff9c4"}
    return f'<span style="background:{colors.get(a, "#eee")};padding:2px 8px;border-radius:4px">{a}</span>'


def _score_badge(score):
    """Color-coded score badge."""
    if score >= 70:
        bg, label = "#1b5e20", "STRONG"
    elif score >= 55:
        bg, label = "#2e7d32", "GOOD"
    elif score >= 40:
        bg, label = "#f57f17", "FAIR"
    elif score >= 25:
        bg, label = "#e65100", "WEAK"
    else:
        bg, label = "#b71c1c", "POOR"
    return (f'<span style="background:{bg};color:#fff;padding:3px 10px;'
            f'border-radius:12px;font-size:11px;font-weight:bold">'
            f'{label} {score:.0f}</span>')


# ── Top 5 Value Picks Cards ─────────────────────────────────────────────────

def _build_top5_value_html(top20: pd.DataFrame) -> str:
    """Generate Top 5 Value Picks cards."""
    top5 = top20.head(5)

    html = '<h2>Value Picks (Top 5)</h2>\n'
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px">\n'

    for i, (_, row) in enumerate(top5.iterrows(), 1):
        name = row.get("name", "") or row["ticker"]
        score = row.get("value_score", 0)

        if score >= 70:
            border_color = "#1b5e20"
        elif score >= 55:
            border_color = "#2e7d32"
        elif score >= 40:
            border_color = "#f57f17"
        else:
            border_color = "#e65100"

        per_str = f'{row["per"]:.1f}' if row["per"] > 0 else "N/A"
        pbr_str = f'{row["pbr"]:.2f}' if row["pbr"] > 0 else "N/A"
        div_str = f'{row["div_yield"]:.1f}%' if row["div_yield"] > 0 else "N/A"
        mcap_str = f'{row.get("market_cap", 0) / 1e12:.1f}조' if row.get("market_cap", 0) > 0 else "N/A"
        frgn_str = f'{row.get("foreign_ratio", 0):.1f}%' if row.get("foreign_ratio", 0) > 0 else "N/A"
        sector_gap = row.get("sector_per_gap", 0)
        gap_str = f'{sector_gap:+.1f}%' if sector_gap != 0 else "N/A"

        reasons = []
        if 0 < row["per"] < 10:
            reasons.append(f"PER {per_str} (저PER)")
        elif 0 < row["per"] < 15:
            reasons.append(f"PER {per_str} (적정)")
        if row["pbr"] > 0 and row["pbr"] < 1.0:
            reasons.append(f"PBR {pbr_str} (자산가치 이하)")
        if row["div_yield"] > 2.0:
            reasons.append(f"배당 {div_str} (고배당)")
        if sector_gap < -20:
            reasons.append(f"업종PER 대비 {gap_str} (할인)")
        if row.get("alignment") == "BULLISH":
            reasons.append("MA 정배열")
        if row.get("above_ma200") is True:
            reasons.append("MA200 위")

        reasons_html = "".join(f"<li>{r}</li>" for r in reasons)

        html += f'''
  <div style="background:#fff;border-radius:10px;padding:14px 18px;flex:1;min-width:220px;
              box-shadow:0 2px 6px rgba(0,0,0,0.08);border-top:4px solid {border_color}">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <span style="font-size:18px;font-weight:bold;color:#1a237e">#{i}</span>
      {_score_badge(score)}
    </div>
    <div style="font-size:15px;font-weight:bold;color:#333">{name}</div>
    <div style="font-size:12px;color:#78909c;margin-bottom:6px">{row["ticker"]} | {_fmt_num(row.get("last_close", 0))}</div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;font-size:11px;margin-bottom:6px">
      <span style="background:#e8f5e9;padding:2px 6px;border-radius:4px">PER {per_str}</span>
      <span style="background:#e3f2fd;padding:2px 6px;border-radius:4px">PBR {pbr_str}</span>
      <span style="background:#fff3e0;padding:2px 6px;border-radius:4px">배당 {div_str}</span>
      <span style="background:#f3e5f5;padding:2px 6px;border-radius:4px">시총 {mcap_str}</span>
      <span style="background:#e0f7fa;padding:2px 6px;border-radius:4px">외인 {frgn_str}</span>
      <span style="background:#fce4ec;padding:2px 6px;border-radius:4px">업종 {gap_str}</span>
    </div>
    <ul style="margin:4px 0;padding-left:16px;font-size:12px;color:#37474f">{reasons_html}</ul>
  </div>'''

    html += '\n</div>\n'
    return html


# ── Main HTML Report ─────────────────────────────────────────────────────────

def generate_valuation_html(top20: pd.DataFrame,
                            universe_size: int = 0,
                            report_date: str = "",
                            weights: dict = None) -> str:
    """Generate Valuation Top20 HTML report."""
    if not report_date:
        report_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    w = weights or DEFAULT_WEIGHTS
    total = len(top20)

    # Summary stats
    avg_per = top20[top20["per"] > 0]["per"].mean() if (top20["per"] > 0).any() else 0
    avg_pbr = top20[top20["pbr"] > 0]["pbr"].mean() if (top20["pbr"] > 0).any() else 0
    avg_div = top20["div_yield"].mean()
    avg_score = top20["value_score"].mean()
    n_bull = (top20["alignment"] == "BULLISH").sum()
    n_bear = (top20["alignment"] == "BEARISH").sum()
    n_mix = total - n_bull - n_bear
    above_ma200 = top20["above_ma200"].sum() if "above_ma200" in top20.columns else 0

    # Weight description
    weight_desc = " | ".join([f"{k.replace('w_','').replace('_rank','').replace('_score','')}: {v:.0%}"
                              for k, v in w.items()])

    top5_html = _build_top5_value_html(top20)

    # Table rows
    rows = ""
    for i, (_, row) in enumerate(top20.iterrows(), 1):
        name = row.get("name", "") or row["ticker"]
        per_val = row.get("per", 0)
        sector_per = row.get("sector_per", 0)

        mcap_tril = row.get("market_cap", 0) / 1e12 if row.get("market_cap", 0) > 0 else 0
        mcap_str = f"{mcap_tril:.1f}조" if mcap_tril > 0 else "-"

        rows += f"""<tr>
          <td>{i}</td>
          <td class="ticker">{row['ticker']}</td>
          <td>{name}</td>
          <td class="num">{_fmt_num(row.get('last_close', 0))}</td>
          <td class="num">{_score_badge(row['value_score'])}</td>
          <td class="num">{_per_badge(per_val, sector_per)}</td>
          <td class="num">{row['pbr']:.2f}</td>
          <td class="num">{_fmt_num(row.get('eps', 0))}</td>
          <td class="num">{_fmt_num(row.get('bps', 0))}</td>
          <td class="num">{row['div_yield']:.1f}%</td>
          <td class="num">{mcap_str}</td>
          <td class="num">{row.get('sector', '-')}</td>
          <td class="num">{_fmt_num(sector_per, '.1f')}</td>
          <td class="num">{_val_color(row.get('sector_per_gap', 0), reverse=True)}</td>
          <td class="num">{row.get('foreign_ratio', 0):.1f}%</td>
          <td class="num">{_align_badge(row.get('alignment', 'MIXED'))}</td>
          <td class="num">{_val_color(row.get('pct_vs_ma20', 0))}</td>
          <td class="num">{_val_color(row.get('pct_vs_ma60', 0))}</td>
          <td class="num">{_val_color(row.get('pct_vs_ma120', 0))}</td>
          <td class="num">{_val_color(row.get('pct_vs_ma200', 0))}</td>
        </tr>\n"""

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Gen4 Valuation Top20 Report - {report_date}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', 'Malgun Gothic', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; color: #333; }}
  h1 {{ color: #4a148c; border-bottom: 3px solid #7b1fa2; padding-bottom: 10px; margin-bottom: 20px; }}
  h2 {{ color: #7b1fa2; margin-top: 28px; margin-bottom: 12px; }}
  h3 {{ margin: 0 0 12px 0; color: #37474f; font-size: 15px; }}
  .section {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }}
  .card {{ background: #fff; padding: 16px 20px; border-radius: 10px; flex: 1; min-width: 250px;
          box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
  .metrics {{ display: flex; flex-wrap: wrap; gap: 12px 24px; }}
  .metric {{ display: flex; flex-direction: column; }}
  .metric .label {{ font-size: 11px; color: #78909c; text-transform: uppercase; }}
  .metric .value {{ font-size: 15px; font-weight: 600; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 10px;
          overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-top: 8px; }}
  thead th {{ background: #7b1fa2; color: white; padding: 10px 6px; font-size: 11px;
             white-space: nowrap; position: sticky; top: 0; cursor: help; }}
  thead th.group {{ background: #4a148c; text-align: center; font-size: 12px; padding: 6px; }}
  td {{ padding: 7px 6px; border-bottom: 1px solid #eee; font-size: 12px; white-space: nowrap; }}
  td.num {{ text-align: right; }}
  td.center {{ text-align: center; }}
  td.ticker {{ font-weight: bold; color: #7b1fa2; }}
  tr:hover {{ background: #f3e5f5; }}
  .footer {{ color: #999; margin-top: 16px; font-size: 11px; }}
  .weight-info {{ background: #f3e5f5; padding: 8px 16px; border-radius: 8px;
                 font-size: 11px; color: #4a148c; margin-bottom: 16px; }}
</style>
</head>
<body>
<h1>Gen4 Valuation Top20 Report</h1>

<div class="weight-info">
  <b>Scoring Weights:</b> {weight_desc}
</div>

<h2>Portfolio Summary</h2>
<div class="section">
  <div class="card">
    <h3>Valuation</h3>
    <div class="metrics">
      <div class="metric"><span class="label">Avg PER</span><span class="value">{avg_per:.1f}</span></div>
      <div class="metric"><span class="label">Avg PBR</span><span class="value">{avg_pbr:.2f}</span></div>
      <div class="metric"><span class="label">Avg Dividend</span><span class="value">{avg_div:.1f}%</span></div>
      <div class="metric"><span class="label">Avg Score</span><span class="value">{avg_score:.0f}</span></div>
    </div>
  </div>
  <div class="card">
    <h3>Technical</h3>
    <div class="metrics">
      <div class="metric"><span class="label">Universe</span><span class="value">{universe_size} stocks</span></div>
      <div class="metric"><span class="label">Selected</span><span class="value">{total} stocks</span></div>
      <div class="metric"><span class="label">Alignment</span><span class="value">
        <span style="color:#4caf50">BULL {n_bull}</span> /
        <span style="color:#ff9800">MIX {n_mix}</span> /
        <span style="color:#f44336">BEAR {n_bear}</span>
      </span></div>
      <div class="metric"><span class="label">Above MA200</span><span class="value">{above_ma200}/{total}</span></div>
    </div>
  </div>
</div>

{top5_html}

<h2>Full Ranking</h2>
<table>
<thead>
  <tr>
    <th class="group" colspan="5">Info</th>
    <th class="group" colspan="5">Valuation</th>
    <th class="group" colspan="1">Size</th>
    <th class="group" colspan="3">Sector</th>
    <th class="group" colspan="1">Foreign</th>
    <th class="group" colspan="5">MA Position</th>
  </tr>
  <tr>
    <th title="순번">#</th>
    <th title="종목코드">Ticker</th>
    <th title="종목명">Name</th>
    <th title="전일 종가">Close</th>
    <th title="Value Score (0~100). 높을수록 저평가+기술적 우위">Score</th>
    <th title="주가수익비율. 낮을수록 저평가 (업종 대비 평가)">PER</th>
    <th title="주가순자산비율. 1 미만이면 자산가치 이하">PBR</th>
    <th title="주당순이익 (원)">EPS</th>
    <th title="주당순자산 (원)">BPS</th>
    <th title="배당수익률 (%)">Div%</th>
    <th title="시가총액">시총</th>
    <th title="업종분류">업종</th>
    <th title="동일업종 중간값 PER">업종PER</th>
    <th title="업종PER 대비 괴리율. 음수=저평가, 양수=고평가">Gap</th>
    <th title="외국인 보유비율 (%)">외인%</th>
    <th title="MA 정렬: BULLISH/MIXED/BEARISH">Align</th>
    <th title="종가 vs MA20 이격도">MA20</th>
    <th title="종가 vs MA60 이격도">MA60</th>
    <th title="종가 vs MA120 이격도">MA120</th>
    <th title="종가 vs MA200 이격도">MA200</th>
  </tr>
</thead>
<tbody>{rows}</tbody>
</table>

<p class="footer">
  Gen4 Valuation Report v1.0 | {total} stocks from {universe_size} universe |
  Weights: {weight_desc} |
  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</p>
</body></html>"""
    return html


# ── Stock Name Lookup ────────────────────────────────────────────────────────

def _lookup_names(tickers: List[str]) -> Dict[str, str]:
    """Lookup Korean stock names via pykrx."""
    name_map = {}
    try:
        from pykrx import stock as krx
        for tk in tickers:
            try:
                nm = krx.get_market_ticker_name(tk)
                if nm:
                    name_map[tk] = nm
            except Exception:
                pass
    except ImportError:
        pass
    return name_map


# ── Main Generation ──────────────────────────────────────────────────────────

def generate_top20_valuation_report(ohlcv_dir: Path,
                                     output_dir: Path,
                                     universe: List[str],
                                     sector_map: Dict[str, str] = None,
                                     weights: dict = None,
                                     report_date: str = "") -> Optional[Path]:
    """
    Generate Valuation Top20 report.

    1. Fetch fundamental data (PER/PBR/EPS/BPS/DIV/시총/외국인)
    2. Calculate sector avg PER
    3. Calculate MA analysis for each stock
    4. Score and rank by Value Score
    5. Generate HTML report

    Args:
        ohlcv_dir: Per-stock OHLCV directory
        output_dir: Report output directory
        universe: List of ticker codes to evaluate
        sector_map: {ticker: sector_name} mapping
        weights: Scoring weight overrides
        report_date: YYYYMMDD format

    Returns:
        Path to HTML file, or None on failure.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from data.fundamental_collector import (
        fetch_daily_snapshot, calc_sector_avg_per, calc_ma_analysis
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    if not report_date:
        report_date = datetime.now().strftime("%Y%m%d")

    logger.info(f"[VAL] Generating Valuation Top20 report for {report_date}...")

    # Step 1: Load fundamental data (reuse daily CSV if exists, else fetch)
    fund_csv = Path(__file__).resolve().parent.parent.parent / "backtest" / "data_full" / "fundamental" / f"fundamental_{report_date}.csv"
    if fund_csv.exists():
        logger.info(f"[VAL] Loading cached: {fund_csv}")
        fund_df = pd.read_csv(fund_csv, dtype={"ticker": str})
    else:
        logger.info("[VAL] Fetching fundamental data (no cache)...")
        fund_df = fetch_daily_snapshot()
        # Save for future reuse
        if fund_df is not None and not fund_df.empty:
            fund_csv.parent.mkdir(parents=True, exist_ok=True)
            fund_df.to_csv(fund_csv, index=False)
            logger.info(f"[VAL] Saved cache: {fund_csv}")

    if fund_df is None or fund_df.empty:
        logger.error("[VAL] No fundamental data available")
        return None

    # Filter to universe
    fund_df = fund_df[fund_df["ticker"].isin(universe)].copy()
    logger.info(f"[VAL] Universe filtered: {len(fund_df)} stocks")

    if len(fund_df) == 0:
        logger.error("[VAL] No stocks in universe after filter")
        return None

    # Step 2: Sector avg PER
    if sector_map:
        fund_df = calc_sector_avg_per(fund_df, sector_map)
    else:
        fund_df["sector"] = "N/A"
        fund_df["sector_per"] = 0
        fund_df["sector_per_gap"] = 0

    # Step 3: MA analysis for each stock
    logger.info("[VAL] Computing MA analysis...")
    ma_data = []
    for ticker in fund_df["ticker"].values:
        path = ohlcv_dir / f"{ticker}.csv"
        if not path.exists():
            ma_data.append({
                "ticker": ticker,
                "alignment": "MIXED", "above_ma200": None,
                "last_close": 0,
                **{f"ma{p}": 0 for p in [5, 20, 60, 120, 200]},
                **{f"pct_vs_ma{p}": 0 for p in [5, 20, 60, 120, 200]},
            })
            continue

        try:
            df = pd.read_csv(path, parse_dates=["date"])
            df["close"] = pd.to_numeric(df["close"], errors="coerce").fillna(0)
            df = df.sort_values("date").reset_index(drop=True)
            ma = calc_ma_analysis(df["close"])
            ma["ticker"] = ticker
            ma_data.append(ma)
        except Exception as e:
            logger.debug(f"  MA failed for {ticker}: {e}")
            ma_data.append({"ticker": ticker, "alignment": "MIXED",
                           "above_ma200": None, "last_close": 0})

    ma_df = pd.DataFrame(ma_data)
    fund_df = fund_df.merge(ma_df, on="ticker", how="left")

    # Fill NaN
    for col in ["alignment"]:
        if col in fund_df.columns:
            fund_df[col] = fund_df[col].fillna("MIXED")
    for col in ["foreign_ratio", "sector_per_gap", "pct_vs_ma20",
                "pct_vs_ma60", "pct_vs_ma120", "pct_vs_ma200"]:
        if col in fund_df.columns:
            fund_df[col] = fund_df[col].fillna(0)

    # Step 4: Score
    logger.info("[VAL] Scoring...")
    scored = score_valuation(fund_df, weights)

    # Top 20
    top20 = scored.head(20).copy()

    # Lookup names
    name_map = _lookup_names(top20["ticker"].tolist())
    top20["name"] = top20["ticker"].map(name_map).fillna("")

    logger.info(f"[VAL] Top 20 selected:")
    for i, (_, row) in enumerate(top20.iterrows(), 1):
        logger.info(f"    {i:2d}. {row['ticker']} {row.get('name','')} "
                    f"score={row['value_score']:.0f} "
                    f"PER={row['per']:.1f} PBR={row['pbr']:.2f} "
                    f"DIV={row['div_yield']:.1f}%")

    # Step 5: Generate HTML
    html = generate_valuation_html(
        top20, universe_size=len(universe),
        report_date=report_date, weights=weights)

    html_path = output_dir / f"top20_val_{report_date}.html"
    html_path.write_text(html, encoding="utf-8")

    # JSON export
    json_data = {
        "success": True,
        "report_date": report_date,
        "total_universe": len(universe),
        "total_scored": len(scored),
        "weights": weights or DEFAULT_WEIGHTS,
        "top20": top20.to_dict(orient="records"),
    }
    json_path = output_dir / f"top20_val_{report_date}.json"
    json_path.write_text(json.dumps(json_data, indent=2, ensure_ascii=False,
                                     default=str), encoding="utf-8")

    logger.info(f"[VAL] Report: {html_path}")
    return html_path
