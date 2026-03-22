"""
top20_report.py — Gen4 Top20 MA Analysis Report
=================================================
Generates top20_ma_YYYYMMDD.html/json for the Gen4 selected portfolio.

Shows: Gen4 factor scores + MA alignment + RSI + BB + returns + volume.
Adapted from Gen3 top20_report.py (simplified for Gen4 monthly rebalance).
"""
from __future__ import annotations
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("gen4.top20report")


# ── Technical Indicators ─────────────────────────────────────────────────────

def compute_analysis(close: pd.Series, volume: pd.Series,
                     ticker: str, name: str = "",
                     vol_score: float = 0, mom_score: float = 0) -> Optional[dict]:
    """Compute full technical analysis for one stock."""
    if len(close) < 120:
        return None

    c = close.values.astype(float)
    v = volume.values.astype(float) if volume is not None else np.zeros(len(c))
    last = float(c[-1])
    if last <= 0:
        return None

    # Moving Averages
    ma20 = float(np.mean(c[-20:])) if len(c) >= 20 else last
    ma60 = float(np.mean(c[-60:])) if len(c) >= 60 else last
    ma120 = float(np.mean(c[-120:])) if len(c) >= 120 else last

    pct_ma20 = (last / ma20 - 1) * 100 if ma20 > 0 else 0
    pct_ma60 = (last / ma60 - 1) * 100 if ma60 > 0 else 0
    pct_ma120 = (last / ma120 - 1) * 100 if ma120 > 0 else 0

    # Alignment
    if last > ma20 > ma60 > ma120:
        alignment = "BULLISH"
    elif last < ma20 < ma60 < ma120:
        alignment = "BEARISH"
    else:
        alignment = "MIXED"

    # RSI(14)
    rsi = _calc_rsi(c, 14)

    # Bollinger Bands
    bb_mid = ma20
    bb_std = float(np.std(c[-20:])) if len(c) >= 20 else 0
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std
    bb_pct_b = (last - bb_lower) / (bb_upper - bb_lower) if (bb_upper - bb_lower) > 0 else 0.5

    if bb_pct_b > 1:
        bb_pos = "above"
    elif bb_pct_b > 0.8:
        bb_pos = "upper"
    elif bb_pct_b > 0.2:
        bb_pos = "mid"
    elif bb_pct_b > 0:
        bb_pos = "lower"
    else:
        bb_pos = "below"

    # 52-week high/low
    h252 = float(np.max(c[-252:])) if len(c) >= 252 else float(np.max(c))
    l252 = float(np.min(c[-252:])) if len(c) >= 252 else float(np.min(c))
    pct_52h = (last / h252 - 1) * 100 if h252 > 0 else 0

    # Returns
    ret_1d = (c[-1] / c[-2] - 1) * 100 if len(c) >= 2 else 0
    ret_5d = (c[-1] / c[-6] - 1) * 100 if len(c) >= 6 else 0
    ret_20d = (c[-1] / c[-21] - 1) * 100 if len(c) >= 21 else 0
    ret_60d = (c[-1] / c[-61] - 1) * 100 if len(c) >= 61 else 0

    # Volume ratio
    vol_avg = float(np.mean(v[-20:])) if len(v) >= 20 and np.mean(v[-20:]) > 0 else 1
    vol_ratio = float(v[-1] / vol_avg) if vol_avg > 0 else 0

    # Trading amount (20d avg, in billion won)
    amt_20avg = float(np.mean(c[-20:] * v[-20:])) / 1e8 if len(c) >= 20 else 0

    return {
        "ticker": ticker,
        "name": name,
        "last_close": int(last),
        "ma20": int(ma20), "ma60": int(ma60), "ma120": int(ma120),
        "pct_vs_ma20": round(pct_ma20, 1),
        "pct_vs_ma60": round(pct_ma60, 1),
        "pct_vs_ma120": round(pct_ma120, 1),
        "alignment": alignment,
        "rsi": round(rsi, 1),
        "bb_pct_b": round(bb_pct_b, 2),
        "bb_pos": bb_pos,
        "pct_from_52h": round(pct_52h, 1),
        "ret_1d": round(ret_1d, 1),
        "ret_5d": round(ret_5d, 1),
        "ret_20d": round(ret_20d, 1),
        "ret_60d": round(ret_60d, 1),
        "vol_ratio": round(vol_ratio, 2),
        "amt_20avg_bil": round(amt_20avg, 0),
        # Gen4 factor scores
        "vol_12m": round(vol_score, 6),
        "mom_12_1": round(mom_score, 4),
    }


def _calc_rsi(prices, period=14) -> float:
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices[-(period + 1):])
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains) if len(gains) > 0 else 0
    avg_loss = np.mean(losses) if len(losses) > 0 else 0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - 100 / (1 + rs))


# ── HTML Report ──────────────────────────────────────────────────────────────

def generate_html(analyses: List[dict], target_info: dict,
                  report_date: str = "") -> str:
    """Generate dark-themed HTML report for Gen4 top20 portfolio."""
    if not report_date:
        report_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    n_bull = sum(1 for a in analyses if a["alignment"] == "BULLISH")
    n_bear = sum(1 for a in analyses if a["alignment"] == "BEARISH")
    n_mix = len(analyses) - n_bull - n_bear

    vol_thresh = target_info.get("vol_threshold", 0)
    universe_size = target_info.get("universe_size", 0)

    # Table rows
    rows = ""
    for i, a in enumerate(analyses, 1):
        def _color(v):
            if v > 0: return "color:#d32f2f"
            elif v < 0: return "color:#1565c0"
            return "color:#888"

        align_badge = {
            "BULLISH": '<span style="background:#4caf50;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px">BULL</span>',
            "BEARISH": '<span style="background:#d32f2f;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px">BEAR</span>',
            "MIXED": '<span style="background:#ff9800;color:#fff;padding:2px 6px;border-radius:3px;font-size:11px">MIX</span>',
        }.get(a["alignment"], "")

        rows += f"""<tr>
<td>{i}</td><td>{a['ticker']}</td><td style="text-align:left">{a.get('name','')}</td>
<td>{a['last_close']:,}</td>
<td style="{_color(a['pct_vs_ma20'])}">{a['pct_vs_ma20']:+.1f}%</td>
<td style="{_color(a['pct_vs_ma60'])}">{a['pct_vs_ma60']:+.1f}%</td>
<td style="{_color(a['pct_vs_ma120'])}">{a['pct_vs_ma120']:+.1f}%</td>
<td>{align_badge}</td>
<td>{a['rsi']:.0f}</td>
<td>{a['bb_pos']}</td>
<td style="{_color(a['pct_from_52h'])}">{a['pct_from_52h']:+.1f}%</td>
<td style="{_color(a['ret_1d'])}">{a['ret_1d']:+.1f}%</td>
<td style="{_color(a['ret_5d'])}">{a['ret_5d']:+.1f}%</td>
<td style="{_color(a['ret_20d'])}">{a['ret_20d']:+.1f}%</td>
<td style="{_color(a['ret_60d'])}">{a['ret_60d']:+.1f}%</td>
<td>{a['vol_ratio']:.1f}</td>
<td>{a['amt_20avg_bil']:,.0f}</td>
<td>{a['vol_12m']:.4f}</td>
<td>{a['mom_12_1']:.2%}</td>
</tr>\n"""

    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Gen4 Top20 MA Analysis</title>
<style>
body{{font-family:'Segoe UI',Arial;background:#f5f5f5;color:#333;padding:20px;margin:0}}
h1{{color:#1565c0;margin-bottom:5px}}
.summary{{display:flex;gap:15px;margin:15px 0;flex-wrap:wrap}}
.card{{background:#fff;border-radius:8px;padding:15px;box-shadow:0 2px 6px rgba(0,0,0,0.08);min-width:200px}}
.card h3{{margin:0 0 8px;color:#1565c0;font-size:14px}}
table{{border-collapse:collapse;width:100%;background:#fff;font-size:12px;margin-top:15px}}
th{{background:#1565c0;color:#fff;padding:7px 8px;text-align:center;position:sticky;top:0}}
td{{padding:5px 8px;border-bottom:1px solid #eee;text-align:center}}
tr:hover{{background:#e3f2fd}}
.gen4-badge{{background:#1565c0;color:#fff;padding:3px 8px;border-radius:4px;font-size:11px}}
footer{{margin-top:20px;color:#888;font-size:12px;border-top:1px solid #ddd;padding-top:10px}}
</style></head><body>
<h1>Gen4 Core: Top20 MA Analysis</h1>
<p style="color:#666">{report_date} | LowVol30%ile + Mom12-1 | Universe: {universe_size} stocks | Vol threshold: {vol_thresh:.5f}</p>

<div class="summary">
  <div class="card"><h3>Alignment</h3>
    <span style="color:#4caf50">BULL {n_bull}</span> /
    <span style="color:#ff9800">MIX {n_mix}</span> /
    <span style="color:#d32f2f">BEAR {n_bear}</span>
  </div>
  <div class="card"><h3>Selected</h3>{len(analyses)} / {target_info.get('universe_size',0)} stocks</div>
  <div class="card"><h3>Avg RSI</h3>{np.mean([a['rsi'] for a in analyses]):.0f}</div>
  <div class="card"><h3>Avg Mom12-1</h3>{np.mean([a['mom_12_1'] for a in analyses]):.1%}</div>
</div>

<table>
<tr>
  <th>#</th><th>Ticker</th><th>Name</th><th>Close</th>
  <th>MA20%</th><th>MA60%</th><th>MA120%</th><th>Align</th>
  <th>RSI</th><th>BB</th><th>52wH%</th>
  <th>1D</th><th>5D</th><th>20D</th><th>60D</th>
  <th>VolR</th><th>Amt(bil)</th>
  <th><span class="gen4-badge">Vol12m</span></th>
  <th><span class="gen4-badge">Mom12-1</span></th>
</tr>
{rows}
</table>

<footer>
Gen4 Core v4.0 | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</footer>
</body></html>"""
    return html


# ── Main Generation ──────────────────────────────────────────────────────────

def generate_top20_report(target: dict, ohlcv_dir: Path,
                          output_dir: Path) -> Optional[Path]:
    """
    Generate top20 MA report for Gen4 selected stocks.

    Args:
        target: build_target_portfolio() output
        ohlcv_dir: Per-stock OHLCV directory
        output_dir: Report output directory

    Returns:
        Path to HTML file, or None on failure.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    report_date = target.get("date", date.today().strftime("%Y%m%d"))

    tickers = target.get("target_tickers", [])
    scores = target.get("scores", {})

    if not tickers:
        logger.warning("No tickers in target portfolio")
        return None

    # Load OHLCV and compute analysis
    analyses = []
    for tk in tickers:
        path = ohlcv_dir / f"{tk}.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, parse_dates=["date"])
            for c in ("close", "volume"):
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df = df.sort_values("date").reset_index(drop=True)

            close_s = df["close"]
            vol_s = df["volume"] if "volume" in df.columns else None

            # Get name (from file or empty)
            name = ""

            s = scores.get(tk, {})
            result = compute_analysis(
                close_s, vol_s, tk, name,
                vol_score=s.get("vol_12m", 0),
                mom_score=s.get("mom_12_1", 0))

            if result:
                analyses.append(result)

        except Exception as e:
            logger.warning(f"Failed to analyze {tk}: {e}")

    if not analyses:
        logger.warning("No analyses generated")
        return None

    # Sort by momentum (matching Gen4 selection order)
    analyses.sort(key=lambda a: a["mom_12_1"], reverse=True)

    # Generate HTML
    html = generate_html(analyses, target, report_date)
    html_path = output_dir / f"top20_ma_{report_date}.html"
    html_path.write_text(html, encoding="utf-8")

    # Generate JSON
    json_data = {
        "success": True,
        "report_date": report_date,
        "total_tickers": len(analyses),
        "universe_size": target.get("universe_size", 0),
        "vol_threshold": target.get("vol_threshold", 0),
        "analyses": analyses,
    }
    json_path = output_dir / f"top20_ma_{report_date}.json"
    json_path.write_text(json.dumps(json_data, indent=2, ensure_ascii=False),
                         encoding="utf-8")

    logger.info(f"Top20 report: {html_path}")
    return html_path
