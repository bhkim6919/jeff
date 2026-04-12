"""
top20_report.py — Gen4 Top20 MA Analysis Report
=================================================
Generates top20_ma_YYYYMMDD.html/json for the Gen4 selected portfolio.

Shows: Gen4 factor scores + MA alignment + RSI + BB + returns + volume.
Matches Gen3 v7.9 report quality:
  - Korean stock names (pykrx)
  - Tooltip descriptions on headers
  - Investment Picks Top 5 (scoring cards)
  - Grouped column headers
  - BB/Alignment badges with colors
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

    # Golden/Death Cross (MA20 vs MA60, last 5 days)
    cross = "NONE"
    cross_days_ago = 0
    if len(c) >= 80:
        ma20_arr = np.convolve(c, np.ones(20) / 20, mode='valid')
        ma60_arr = np.convolve(c, np.ones(60) / 60, mode='valid')
        min_len = min(len(ma20_arr), len(ma60_arr))
        if min_len >= 6:
            diff = ma20_arr[-min_len:] - ma60_arr[-min_len:]
            for d in range(1, min(6, len(diff))):
                if diff[-d] > 0 and diff[-(d + 1)] <= 0:
                    cross = "GOLDEN"
                    cross_days_ago = d - 1
                    break
                elif diff[-d] < 0 and diff[-(d + 1)] >= 0:
                    cross = "DEATH"
                    cross_days_ago = d - 1
                    break

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
        "cross": cross, "cross_days_ago": cross_days_ago,
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


def fetch_investor_flow(tickers: List[str]) -> Dict[str, dict]:
    """
    Fetch foreign/institutional net buy data from Naver Finance.
    Returns: {ticker: {foreign_net_1d, inst_net_1d, foreign_net_5d, inst_net_5d}} in 억원.
    """
    result = {}
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError:
        logger.warning("requests/bs4 not available for investor flow")
        return result

    import time as _time
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    logger.info(f"[InvestorFlow] Fetching from Naver Finance ({len(tickers)} stocks)...")

    for ticker in tickers:
        try:
            url = f"https://finance.naver.com/item/frgn.naver?code={ticker}&page=1"
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")

            type2_tables = soup.select("table.type2")
            if len(type2_tables) < 2:
                continue
            table = type2_tables[1]

            rows_data = []
            for row in table.select("tr"):
                tds = [td.get_text(strip=True) for td in row.select("td")]
                if len(tds) >= 7 and "." in tds[0]:
                    rows_data.append(tds)

            if not rows_data:
                continue

            def _pn(s: str) -> int:
                s = s.replace(",", "").replace("+", "").strip()
                if not s or s == "-":
                    return 0
                try:
                    return int(s)
                except ValueError:
                    return 0

            # Columns: date[0], close[1], diff[2], pct[3], volume[4], inst[5], foreign[6]
            inst_1d = _pn(rows_data[0][5])
            frgn_1d = _pn(rows_data[0][6])
            inst_5d = sum(_pn(r[5]) for r in rows_data[:5])
            frgn_5d = sum(_pn(r[6]) for r in rows_data[:5])

            close_price = _pn(rows_data[0][1])
            if close_price > 0:
                scale = close_price / 1e8  # shares -> 억원
                result[ticker] = {
                    "foreign_net_1d": round(frgn_1d * scale),
                    "inst_net_1d": round(inst_1d * scale),
                    "foreign_net_5d": round(frgn_5d * scale),
                    "inst_net_5d": round(inst_5d * scale),
                }

            _time.sleep(0.3)
        except Exception as e:
            logger.debug(f"  {ticker} investor flow error: {e}")
            continue

    logger.info(f"[InvestorFlow] Done: {len(result)}/{len(tickers)} stocks")
    return result


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


# ── Investment Scoring (adapted from Gen3 v7.9) ─────────────────────────────

def _score_investment(a: dict) -> dict:
    """
    Technical investment attractiveness score. 0~100.

    Axes:
      1. Trend health (MA alignment + spacing)     25pt
      2. Overbought/oversold (RSI + BB)             20pt
      3. Momentum quality (pullback + mid-term)     20pt
      4. Gen4 factor bonus (low vol + high mom)     15pt
      5. Liquidity (amt + vol ratio)                10pt
      6. 52w high proximity                         10pt
    """
    score = 0.0
    reasons = []
    warnings = []

    # 1. Trend (25pt)
    trend = 0.0
    if a["alignment"] == "BULLISH":
        trend += 12.0
    elif a["alignment"] == "MIXED":
        trend += 4.0
        if a["pct_vs_ma60"] > 0 and a["pct_vs_ma120"] > 0:
            trend += 3.0

    ma20_gap = abs(a["pct_vs_ma20"])
    if 3 <= ma20_gap <= 15:
        trend += 8.0
    elif ma20_gap < 3:
        trend += 6.0
    elif 15 < ma20_gap <= 30:
        trend += 4.0
    else:
        warnings.append("MA20 이격 과다")

    if a["pct_vs_ma20"] > 0:
        trend += 5.0
    score += min(trend, 25.0)
    if trend >= 20:
        reasons.append("강한 정배열 추세")
    elif trend >= 12:
        reasons.append("상승 추세 유지")

    # 2. RSI + BB (20pt)
    rsi_score = 0.0
    rsi = a["rsi"]
    if 35 <= rsi <= 55:
        rsi_score += 12.0
        reasons.append(f"RSI {rsi:.0f} 중립 (적정)")
    elif 55 < rsi <= 65:
        rsi_score += 8.0
    elif 25 <= rsi < 35:
        rsi_score += 10.0
        reasons.append(f"RSI {rsi:.0f} 과매도 접근")
    elif rsi > 70:
        warnings.append(f"RSI {rsi:.0f} 과열")
    elif rsi < 25:
        rsi_score += 4.0
        warnings.append(f"RSI {rsi:.0f} 극단 과매도")

    bb_b = a["bb_pct_b"]
    if 0.3 <= bb_b <= 0.7:
        rsi_score += 8.0
    elif 0.7 < bb_b <= 0.85:
        rsi_score += 5.0
    elif bb_b > 1.0:
        warnings.append("BB 상단 돌파 (과열)")
    elif bb_b < 0.2:
        rsi_score += 2.0
    score += min(rsi_score, 20.0)

    # 3. Momentum (20pt)
    mom = 0.0
    if a["ret_1d"] < 0 and a["ret_5d"] > 0:
        mom += 8.0
        reasons.append("단기 눌림목 형성")
    elif a["ret_1d"] >= 0 and a["ret_5d"] > 0:
        mom += 6.0
    elif a["ret_1d"] < -5:
        mom += 3.0
    if a["ret_20d"] > 0:
        mom += 4.0
    if a["ret_60d"] > 0:
        mom += 4.0
    if a["ret_60d"] > 200:
        mom -= 5.0
        warnings.append("60일 수익률 과다 (+200%+)")
    elif a["ret_60d"] > 100:
        mom -= 2.0
    score += max(min(mom, 20.0), 0.0)

    # 4a. Gen4 factor bonus (10pt)
    factor = 0.0
    vol_12m = a.get("vol_12m", 0)
    mom_12_1 = a.get("mom_12_1", 0)
    if vol_12m > 0 and vol_12m < 0.03:
        factor += 5.0
        reasons.append(f"저변동성 {vol_12m:.4f}")
    elif vol_12m < 0.05:
        factor += 3.0
    if mom_12_1 > 0.5:
        factor += 5.0
        reasons.append(f"강한 모멘텀 {mom_12_1:.1%}")
    elif mom_12_1 > 0.2:
        factor += 3.0
    elif mom_12_1 > 0:
        factor += 1.0
    score += min(factor, 10.0)

    # 4b. Investor flow (5pt) — foreign/institutional net buy
    flow = 0.0
    f1d = a.get("foreign_net_1d")
    i1d = a.get("inst_net_1d")
    f5d = a.get("foreign_net_5d")
    i5d = a.get("inst_net_5d")
    _has_flow = f1d is not None and i1d is not None
    if _has_flow:
        if f5d and f5d > 100:
            flow += 2.0
            reasons.append(f"외국인 5일 순매수 +{f5d:,.0f}억")
        if i5d and i5d > 100:
            flow += 2.0
            reasons.append(f"기관 5일 순매수 +{i5d:,.0f}억")
        if f1d > 0 and i1d > 0:
            flow += 1.0
            reasons.append("외인+기관 동반 매수")
        elif f1d < -100:
            warnings.append(f"외국인 1일 순매도 {f1d:,.0f}억")
        if f1d < 0 and i1d < 0 and f1d < -50 and i1d < -50:
            warnings.append("외인+기관 동반 매도")
    score += min(flow, 5.0)

    # 5. Liquidity (10pt)
    liq = 0.0
    amt = a["amt_20avg_bil"]
    if amt >= 10000:
        liq += 7.0
    elif amt >= 3000:
        liq += 5.0
    elif amt >= 1000:
        liq += 3.0
    vr = a["vol_ratio"]
    if 0.8 <= vr <= 2.0:
        liq += 3.0
    elif vr > 3.0:
        liq += 1.0
        warnings.append("거래량 급증 (과열 주의)")
    score += min(liq, 10.0)

    # 6. 52w high proximity (10pt)
    from52 = a["pct_from_52h"]
    if -10 <= from52 <= 0:
        score += 8.0
        reasons.append("52주 고점 근접")
    elif -20 <= from52 < -10:
        score += 6.0
    elif -30 <= from52 < -20:
        score += 4.0
    else:
        score += 2.0

    if score >= 70:
        grade = "STRONG_BUY"
    elif score >= 55:
        grade = "BUY"
    elif score >= 40:
        grade = "HOLD"
    elif score >= 25:
        grade = "CAUTION"
    else:
        grade = "AVOID"

    return {"score": round(score, 1), "grade": grade,
            "reasons": reasons, "warnings": warnings}


# ── HTML Helpers ─────────────────────────────────────────────────────────────

def _cpct(val):
    if val > 0:
        c = "#d32f2f"
    elif val < 0:
        c = "#1565c0"
    else:
        c = "#333"
    return '<span style="color:%s;font-weight:bold">%+.1f%%</span>' % (c, val)


def _rsi_color(val):
    if val >= 70:
        return '<span style="color:#d32f2f;font-weight:bold">%.0f</span>' % val
    elif val <= 30:
        return '<span style="color:#1565c0;font-weight:bold">%.0f</span>' % val
    return '%.0f' % val


def _bb_badge(pos, pct_b):
    colors = {"above": "#ffcdd2", "upper": "#ffe0b2", "mid": "#f5f5f5",
              "lower": "#bbdefb", "below": "#90caf9"}
    labels = {"above": "Above", "upper": "Upper", "mid": "Mid",
              "lower": "Lower", "below": "Below"}
    bg = colors.get(pos, "#f5f5f5")
    lb = labels.get(pos, pos)
    return '<span style="background:%s;padding:2px 6px;border-radius:3px;font-size:11px">%s %.0f%%</span>' % (bg, lb, pct_b * 100)


def _align_badge(a):
    colors = {"BULLISH": "#c8e6c9", "BEARISH": "#ffcdd2", "MIXED": "#fff9c4"}
    return '<span style="background:%s;padding:2px 8px;border-radius:4px">%s</span>' % (colors.get(a, "#eee"), a)


def _inv_fmt(val):
    """Format investor flow value (억원). None → '-'."""
    if val is None:
        return "-"
    if val > 0:
        return '<span style="color:#d32f2f">+%s</span>' % format(int(val), ",")
    elif val < 0:
        return '<span style="color:#1565c0">%s</span>' % format(int(val), ",")
    return "0"


def _inv_badge_html(s: dict) -> str:
    """Investor flow badge for Top5 cards."""
    f1d = s.get("foreign_net_1d")
    i1d = s.get("inst_net_1d")
    if f1d is None and i1d is None:
        return ""
    parts = []
    if f1d is not None:
        fc = "#d32f2f" if f1d > 0 else "#1565c0" if f1d < 0 else "#666"
        parts.append(f'<span style="background:#fce4ec;padding:2px 6px;border-radius:4px">'
                     f'F <span style="color:{fc}">{f1d:+,.0f}</span></span>')
    if i1d is not None:
        ic = "#d32f2f" if i1d > 0 else "#1565c0" if i1d < 0 else "#666"
        parts.append(f'<span style="background:#e8eaf6;padding:2px 6px;border-radius:4px">'
                     f'I <span style="color:{ic}">{i1d:+,.0f}</span></span>')
    return "".join(parts)


def _build_top5_html(analyses: list) -> str:
    """Top 5 investment picks with score cards."""
    scored = []
    for a in analyses:
        inv = _score_investment(a)
        scored.append({**a, **inv})
    scored.sort(key=lambda x: -x["score"])
    top5 = scored[:5]

    grade_colors = {
        "STRONG_BUY": "#1b5e20", "BUY": "#2e7d32",
        "HOLD": "#f57f17", "CAUTION": "#e65100", "AVOID": "#b71c1c",
    }
    grade_labels = {
        "STRONG_BUY": "Strong Buy", "BUY": "Buy",
        "HOLD": "Hold", "CAUTION": "Caution", "AVOID": "Avoid",
    }

    html = '<h2>Investment Picks (Top 5)</h2>\n'
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:16px">\n'

    for i, s in enumerate(top5, 1):
        name = s.get("name", "") or s["ticker"]
        gc = grade_colors.get(s["grade"], "#333")
        gl = grade_labels.get(s["grade"], s["grade"])
        reasons_html = "".join(f"<li>{r}</li>" for r in s["reasons"])
        warnings_html = ""
        if s["warnings"]:
            wlist = "".join(f"<li>{w}</li>" for w in s["warnings"])
            warnings_html = (f'<div style="color:#e65100;font-size:11px;margin-top:4px">'
                             f'<b>Warning:</b><ul style="margin:2px 0;padding-left:16px">{wlist}</ul></div>')

        html += f'''
  <div style="background:#fff;border-radius:10px;padding:14px 18px;flex:1;min-width:220px;
              box-shadow:0 2px 6px rgba(0,0,0,0.08);border-top:4px solid {gc}">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <span style="font-size:18px;font-weight:bold;color:#1a237e">#{i}</span>
      <span style="background:{gc};color:#fff;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold">{gl} {s["score"]:.0f}</span>
    </div>
    <div style="font-size:15px;font-weight:bold;color:#333">{name}</div>
    <div style="font-size:12px;color:#78909c;margin-bottom:6px">{s["ticker"]} | {s["last_close"]:,}원</div>
    <div style="display:flex;gap:6px;flex-wrap:wrap;font-size:11px;margin-bottom:6px">
      <span style="background:#e8f5e9;padding:2px 6px;border-radius:4px">RSI {s["rsi"]:.0f}</span>
      <span style="background:#e3f2fd;padding:2px 6px;border-radius:4px">MA20 {s["pct_vs_ma20"]:+.1f}%</span>
      <span style="background:#fff3e0;padding:2px 6px;border-radius:4px">52wH {s["pct_from_52h"]:+.1f}%</span>
      <span style="background:#f3e5f5;padding:2px 6px;border-radius:4px">Vol {s["vol_ratio"]:.1f}x</span>
      <span style="background:#e0f7fa;padding:2px 6px;border-radius:4px">Vol12m {s["vol_12m"]:.4f}</span>
      <span style="background:#fce4ec;padding:2px 6px;border-radius:4px">Mom {s["mom_12_1"]:.1%}</span>
      {_inv_badge_html(s)}
    </div>
    <ul style="margin:4px 0;padding-left:16px;font-size:12px;color:#37474f">{reasons_html}</ul>
    {warnings_html}
  </div>'''

    html += '\n</div>\n'

    # Avoid list
    avoid = [s for s in scored if s["grade"] == "AVOID"]
    if avoid:
        html += '<div style="background:#fff3e0;border-left:4px solid #e65100;padding:10px 16px;border-radius:8px;margin-bottom:16px">\n'
        html += '<b style="color:#e65100">Avoid List:</b> '
        parts = []
        for s in avoid:
            nm = s.get("name", "") or s["ticker"]
            warns = ", ".join(s["warnings"][:2]) if s["warnings"] else "복합 위험"
            parts.append(f'{nm}({s["ticker"]}) — {warns}')
        html += "; ".join(parts)
        html += '\n</div>\n'

    return html


# ── HTML Report ──────────────────────────────────────────────────────────────

def generate_html(analyses: List[dict], target_info: dict,
                  report_date: str = "") -> str:
    """Generate Gen3-quality HTML report for Gen4 top20 portfolio."""
    if not report_date:
        report_date = datetime.now().strftime("%Y-%m-%d %H:%M")

    n_bull = sum(1 for a in analyses if a["alignment"] == "BULLISH")
    n_bear = sum(1 for a in analyses if a["alignment"] == "BEARISH")
    n_mix = len(analyses) - n_bull - n_bear

    vol_thresh = target_info.get("vol_threshold", 0)
    universe_size = target_info.get("universe_size", 0)

    above_ma20 = sum(1 for a in analyses if a["pct_vs_ma20"] > 0)
    above_ma60 = sum(1 for a in analyses if a["pct_vs_ma60"] > 0)
    above_ma120 = sum(1 for a in analyses if a["pct_vs_ma120"] > 0)
    total = len(analyses)

    golden_list = []
    death_list = []
    for a in analyses:
        name = a.get("name", "") or a["ticker"]
        label = f"{name}({a['ticker']})"
        if a.get("cross") == "GOLDEN":
            d = a.get("cross_days_ago", 0)
            golden_list.append(f"{label} {d}d ago" if d > 0 else label)
        elif a.get("cross") == "DEATH":
            d = a.get("cross_days_ago", 0)
            death_list.append(f"{label} {d}d ago" if d > 0 else label)

    # Cross alerts
    alert_html = ""
    if golden_list:
        alert_html += '<div class="alert alert-gold">Golden Cross: %s</div>\n' % ", ".join(golden_list)
    if death_list:
        alert_html += '<div class="alert alert-death">Death Cross: %s</div>\n' % ", ".join(death_list)

    # Table rows
    rows = ""
    for i, a in enumerate(analyses, 1):
        cross_td = ""
        if a.get("cross") == "GOLDEN":
            d = a.get("cross_days_ago", 0)
            label = "GC" if d == 0 else f"GC({d}d)"
            cross_td = f'<span style="color:#d32f2f;font-weight:bold">{label}</span>'
        elif a.get("cross") == "DEATH":
            d = a.get("cross_days_ago", 0)
            label = "DC" if d == 0 else f"DC({d}d)"
            cross_td = f'<span style="color:#1565c0;font-weight:bold">{label}</span>'

        rows += f"""<tr>
          <td>{i}</td><td class="ticker">{a['ticker']}</td><td>{a.get('name','')}</td>
          <td class="num">{a['last_close']:,}</td>
          <td class="num">{_cpct(a['pct_vs_ma20'])}</td><td class="num">{_cpct(a['pct_vs_ma60'])}</td><td class="num">{_cpct(a['pct_vs_ma120'])}</td>
          <td class="num">{_align_badge(a['alignment'])}</td>
          <td class="center">{cross_td}</td><td class="center">{_rsi_color(a['rsi'])}</td>
          <td class="num">{_bb_badge(a['bb_pos'], a['bb_pct_b'])}</td><td class="center">{a['pct_from_52h']:+.1f}%</td>
          <td class="num">{_cpct(a['ret_1d'])}</td><td class="num">{_cpct(a['ret_5d'])}</td>
          <td class="num">{_cpct(a['ret_20d'])}</td><td class="num">{_cpct(a['ret_60d'])}</td>
          <td class="num">{_inv_fmt(a.get('foreign_net_1d'))}</td><td class="num">{_inv_fmt(a.get('inst_net_1d'))}</td>
          <td class="num">{_inv_fmt(a.get('foreign_net_5d'))}</td><td class="num">{_inv_fmt(a.get('inst_net_5d'))}</td>
          <td class="num">{a['vol_ratio']:.1f}x</td>
          <td class="num">{a['amt_20avg_bil']:,.0f}</td>
          <td class="num">{a['vol_12m']:.4f}</td>
          <td class="num">{a['mom_12_1']:.2%}</td>
        </tr>\n"""

    top5_html = _build_top5_html(analyses)

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Gen4 Top20 Report - {report_date}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: 'Segoe UI', 'Malgun Gothic', sans-serif; margin: 0; padding: 20px; background: #f5f5f5; color: #333; }}
  h1 {{ color: #1a237e; border-bottom: 3px solid #1565c0; padding-bottom: 10px; margin-bottom: 20px; }}
  h2 {{ color: #1565c0; margin-top: 28px; margin-bottom: 12px; }}
  h3 {{ margin: 0 0 12px 0; color: #37474f; font-size: 15px; }}
  .section {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }}
  .card {{ background: #fff; padding: 16px 20px; border-radius: 10px; flex: 1; min-width: 250px;
          box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
  .metrics {{ display: flex; flex-wrap: wrap; gap: 12px 24px; }}
  .metric {{ display: flex; flex-direction: column; }}
  .metric .label {{ font-size: 11px; color: #78909c; text-transform: uppercase; }}
  .metric .value {{ font-size: 15px; font-weight: 600; }}
  .alert {{ padding: 10px 16px; border-radius: 8px; margin: 8px 0; font-weight: bold; }}
  .alert-gold {{ background: #fff8e1; border-left: 4px solid #ffc107; color: #e65100; }}
  .alert-death {{ background: #fce4ec; border-left: 4px solid #e53935; color: #b71c1c; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff; border-radius: 10px;
          overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-top: 8px; }}
  thead th {{ background: #1565c0; color: white; padding: 10px 6px; font-size: 11px;
             white-space: nowrap; position: sticky; top: 0; cursor: help; }}
  thead th.group {{ background: #0d47a1; text-align: center; font-size: 12px; padding: 6px; }}
  td {{ padding: 7px 6px; border-bottom: 1px solid #eee; font-size: 12px; white-space: nowrap; }}
  td.num {{ text-align: right; }}
  td.center {{ text-align: center; }}
  td.ticker {{ font-weight: bold; color: #1565c0; }}
  tr:hover {{ background: #e3f2fd; }}
  .footer {{ color: #999; margin-top: 16px; font-size: 11px; }}
</style>
</head>
<body>
<h1>Gen4 Core: Top20 MA Analysis Report</h1>

<h2>Portfolio Summary</h2>
<div class="section">
  <div class="card">
    <h3>Strategy</h3>
    <div class="metrics">
      <div class="metric"><span class="label">Method</span><span class="value">LowVol30%ile + Mom12-1</span></div>
      <div class="metric"><span class="label">Universe</span><span class="value">{universe_size} stocks</span></div>
      <div class="metric"><span class="label">Vol Threshold</span><span class="value">{vol_thresh:.5f}</span></div>
      <div class="metric"><span class="label">Selected</span><span class="value">{total} stocks</span></div>
    </div>
  </div>
  <div class="card">
    <h3>Top20 Breadth</h3>
    <div class="metrics">
      <div class="metric"><span class="label">MA20 &gt;</span><span class="value">{above_ma20}/{total} ({above_ma20*100//max(total,1)}%)</span></div>
      <div class="metric"><span class="label">MA60 &gt;</span><span class="value">{above_ma60}/{total} ({above_ma60*100//max(total,1)}%)</span></div>
      <div class="metric"><span class="label">MA120 &gt;</span><span class="value">{above_ma120}/{total} ({above_ma120*100//max(total,1)}%)</span></div>
      <div class="metric"><span class="label">Avg RSI</span><span class="value">{np.mean([a['rsi'] for a in analyses]):.0f}</span></div>
      <div class="metric"><span class="label">Alignment</span><span class="value">
        <span style="color:#4caf50">BULL {n_bull}</span> /
        <span style="color:#ff9800">MIX {n_mix}</span> /
        <span style="color:#f44336">BEAR {n_bear}</span>
      </span></div>
    </div>
  </div>
  <div class="card">
    <h3>Factor Stats</h3>
    <div class="metrics">
      <div class="metric"><span class="label">Avg Mom12-1</span><span class="value">{np.mean([a['mom_12_1'] for a in analyses]):.1%}</span></div>
      <div class="metric"><span class="label">Avg Vol12m</span><span class="value">{np.mean([a['vol_12m'] for a in analyses]):.4f}</span></div>
      <div class="metric"><span class="label">Avg Ret20d</span><span class="value">{np.mean([a['ret_20d'] for a in analyses]):+.1f}%</span></div>
      <div class="metric"><span class="label">Avg Ret60d</span><span class="value">{np.mean([a['ret_60d'] for a in analyses]):+.1f}%</span></div>
    </div>
  </div>
</div>
{alert_html}

<h2>Stock Analysis</h2>
<table>
<thead>
  <tr>
    <th class="group" colspan="4">Info</th>
    <th class="group" colspan="4">MA Position</th>
    <th class="group" colspan="3">Signal</th>
    <th class="group" colspan="5">Momentum</th>
    <th class="group" colspan="4">Investor Flow</th>
    <th class="group" colspan="2">Liquidity</th>
    <th class="group" colspan="2">Gen4 Factor</th>
  </tr>
  <tr>
    <th title="순번">#</th>
    <th title="종목코드 (6자리)">Ticker</th>
    <th title="종목명">Name</th>
    <th title="전일 종가 (원)">Close</th>
    <th title="종가 vs 20일 이동평균 이격도. 양수=MA 위, 음수=MA 아래">vs MA20</th>
    <th title="종가 vs 60일 이동평균 이격도. 중기 추세 판단용">vs MA60</th>
    <th title="종가 vs 120일 이동평균 이격도. 장기 추세 판단용">vs MA120</th>
    <th title="MA 정렬: BULLISH(종가>MA20>MA60>MA120), BEARISH(역순), MIXED(혼재)">Align</th>
    <th title="골든크로스(GC): MA20이 MA60상향돌파=매수신호. 데드크로스(DC): 하향돌파=매도신호">Cross</th>
    <th title="RSI(14): 상대강도지수. 70이상=과매수, 30이하=과매도, 40~60=중립">RSI</th>
    <th title="볼린저밴드 %B 위치. Upper(80%+)=과열, Mid(20~80%)=중립, Lower(20%-)=과매도">BB</th>
    <th title="52주 신고 대비 현재 위치. 0%=신고가, -20%=신고대비 20% 하락">52wH</th>
    <th title="전일 대비 수익률 (1일)">1d</th>
    <th title="5거래일 수익률 (1주)">5d</th>
    <th title="20거래일 수익률 (1개월)">20d</th>
    <th title="60거래일 수익률 (3개월)">60d</th>
    <th title="외국인 당일 순매수금액(억원). 양수=매수우위, 음수=매도우위">F1d</th>
    <th title="기관 당일 순매수금액(억원). 양수=매수우위, 음수=매도우위">I1d</th>
    <th title="외국인 5일 누적 순매수(억원). 단기 수급 추세">F5d</th>
    <th title="기관 5일 누적 순매수(억원). 단기 수급 추세">I5d</th>
    <th title="거래량 비율: 전일거래량/20일평균. 1.0x=평균, 2.0x=2배 급증, 0.5x=반토막">Vol</th>
    <th title="20일 평균 일거래대금 (억원). 유동성 측정용">Amt</th>
    <th title="Gen4 12개월 변동성 (낮을수록 좋음). LowVol 30%ile 필터 통과 종목">Vol12m</th>
    <th title="Gen4 12-1개월 모멘텀 수익률. 높을수록 추세 강함">Mom12-1</th>
  </tr>
</thead>
<tbody>{rows}</tbody>
</table>

{top5_html}

<p class="footer">
  Gen4 Core v4.0 | LowVol + Mom12-1 Monthly Rebalance | {total} stocks from {universe_size} universe | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
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
        logger.warning("pykrx not available for name lookup")
    return name_map


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

    # Lookup Korean names
    name_map = _lookup_names(tickers)

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

            name = name_map.get(tk, "")

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

    # Fetch investor flow (foreign/institutional net buy from Naver Finance)
    try:
        inv_flow = fetch_investor_flow([a["ticker"] for a in analyses])
        for a in analyses:
            flow = inv_flow.get(a["ticker"], {})
            a["foreign_net_1d"] = flow.get("foreign_net_1d")
            a["inst_net_1d"] = flow.get("inst_net_1d")
            a["foreign_net_5d"] = flow.get("foreign_net_5d")
            a["inst_net_5d"] = flow.get("inst_net_5d")
    except Exception as e:
        logger.warning(f"Investor flow fetch failed: {e}")

    # Sort by momentum (matching Gen4 selection order)
    analyses.sort(key=lambda a: a["mom_12_1"], reverse=True)

    # Generate HTML
    html = generate_html(analyses, target, report_date)
    html_path = output_dir / f"top20_ma_{report_date}.html"
    html_path.write_text(html, encoding="utf-8")

    # Generate JSON (include scores for next-day performance tracking)
    scored_analyses = []
    for a in analyses:
        inv = _score_investment(a)
        scored_analyses.append({**a, **inv})

    json_data = {
        "success": True,
        "report_date": report_date,
        "total_tickers": len(analyses),
        "universe_size": target.get("universe_size", 0),
        "vol_threshold": target.get("vol_threshold", 0),
        "analyses": analyses,
        "analyses_full": sorted(scored_analyses, key=lambda x: -x["score"]),
    }
    json_path = output_dir / f"top20_ma_{report_date}.json"
    json_path.write_text(json.dumps(json_data, indent=2, ensure_ascii=False),
                         encoding="utf-8")

    logger.info(f"Top20 report: {html_path}")
    return html_path
