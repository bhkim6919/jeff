"""
intraday_chart.py — Inline SVG chart renderer for daily report
===============================================================
Pure Python string-building. No external dependencies.
Generates self-contained SVG for offline HTML viewing.

Color convention (Korean):
  Red  (#d32f2f) = profit / up
  Blue (#1565c0) = loss / down
  Gray (#78909c) = neutral
"""
from __future__ import annotations
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ── Colors ───────────────────────────────────────────────────────────────────
C_PROFIT = "#d32f2f"
C_LOSS = "#1565c0"
C_NEUTRAL = "#78909c"
C_GRID = "#e0e0e0"
C_BG = "#fafafa"
C_TRAIL = "#d32f2f"
C_HWM = "#90a4ae"
C_AREA_UP = "rgba(211,47,47,0.15)"
C_AREA_DN = "rgba(21,101,192,0.15)"


def _escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Portfolio Intraday Return Curve ──────────────────────────────────────────

def _compute_index_returns(kospi_bars: List[dict]) -> List[float]:
    """Compute return series from KOSPI minute bars [{time, close, ...}]."""
    if not kospi_bars or len(kospi_bars) < 2:
        return []
    closes = [b.get("close", 0) for b in kospi_bars]
    base = kospi_bars[0].get("open", closes[0])
    if base <= 0:
        base = closes[0]
    if base <= 0:
        return []
    return [(c / base - 1) for c in closes]


def render_portfolio_intraday_svg(
    bars_by_code: Dict[str, pd.DataFrame],
    weights: Dict[str, float],
    width: int = 720,
    height: int = 200,
    kospi_bars: Optional[List[dict]] = None,
) -> str:
    """
    Portfolio-level weighted intraday return curve with optional KOSPI overlay.

    Args:
        bars_by_code: {code: DataFrame with columns [datetime, open, high, low, close, volume]}
        weights: {code: weight} where weight = market_value / total_market_value
        width, height: SVG dimensions
        kospi_bars: optional list of {time, open, high, low, close, volume} from opt20005

    Returns:
        Inline <svg> string, or "" if no data.
    """
    if not bars_by_code:
        return ""

    # Compute per-minute portfolio return
    # 1. For each code, compute return series (close / first_open - 1)
    code_returns = {}
    for code, df in bars_by_code.items():
        if df.empty or "close" not in df.columns:
            continue
        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        if len(closes) < 2:
            continue
        first_open = pd.to_numeric(df["open"], errors="coerce").dropna()
        if first_open.empty:
            continue
        base = float(first_open.iloc[0])
        if base <= 0:
            continue
        rets = (closes / base - 1).tolist()
        code_returns[code] = rets

    if not code_returns:
        return ""

    # 2. Weighted average return at each minute
    max_len = max(len(r) for r in code_returns.values())
    portfolio_returns = []
    for i in range(max_len):
        weighted_sum = 0.0
        weight_sum = 0.0
        for code, rets in code_returns.items():
            w = weights.get(code, 1.0 / len(code_returns))
            if i < len(rets):
                weighted_sum += w * rets[i]
                weight_sum += w
        if weight_sum > 0:
            portfolio_returns.append(weighted_sum / weight_sum)
        else:
            portfolio_returns.append(0.0)

    if not portfolio_returns:
        return ""

    # 3. Build time labels from first code's data
    first_df = next(iter(bars_by_code.values()))
    time_labels = []
    if "datetime" in first_df.columns:
        for dt_str in first_df["datetime"]:
            parts = str(dt_str).split(" ")
            if len(parts) >= 2:
                time_labels.append(parts[1])
            else:
                time_labels.append("")

    # 4. KOSPI overlay returns
    kospi_returns = _compute_index_returns(kospi_bars) if kospi_bars else []

    # 5. Render SVG
    return _render_line_chart(
        values=portfolio_returns,
        time_labels=time_labels,
        width=width,
        height=height,
        y_format="pct",
        title="",
        fill_zero=True,
        overlay_values=kospi_returns,
        overlay_label="KOSPI",
    )


# ── Individual Stock Mini Chart ──────────────────────────────────────────────

def render_stock_mini_svg(
    bars: pd.DataFrame,
    code: str,
    name: str,
    trail_stop_price: float = 0,
    high_watermark: float = 0,
    avg_price: float = 0,
    width: int = 300,
    height: int = 130,
) -> str:
    """
    Individual stock mini-chart with price line + trail stop level.

    Args:
        bars: DataFrame with [datetime, open, high, low, close, volume]
        code: 6-digit stock code
        name: Korean stock name
        trail_stop_price: trail stop level (dashed red line)
        high_watermark: HWM level (dashed gray line)
        avg_price: entry average price (dotted blue line)

    Returns:
        Inline <div> with <svg>, or "" if no data.
    """
    if bars.empty or "close" not in bars.columns:
        return ""

    closes = pd.to_numeric(bars["close"], errors="coerce").dropna().tolist()
    if len(closes) < 2:
        return ""

    time_labels = []
    if "datetime" in bars.columns:
        for dt_str in bars["datetime"]:
            parts = str(dt_str).split(" ")
            time_labels.append(parts[1] if len(parts) >= 2 else "")

    # Price range for Y axis
    all_vals = list(closes)
    if trail_stop_price > 0:
        all_vals.append(trail_stop_price)
    if high_watermark > 0:
        all_vals.append(high_watermark)
    y_min = min(all_vals) * 0.998
    y_max = max(all_vals) * 1.002

    margin = {"top": 28, "right": 55, "bottom": 22, "left": 10}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]

    def x_pos(i):
        return margin["left"] + (i / max(len(closes) - 1, 1)) * plot_w

    def y_pos(v):
        if y_max == y_min:
            return margin["top"] + plot_h / 2
        return margin["top"] + (1 - (v - y_min) / (y_max - y_min)) * plot_h

    # Price line
    points = " ".join(f"{x_pos(i):.1f},{y_pos(v):.1f}"
                      for i, v in enumerate(closes))

    # Color based on overall return
    ret = closes[-1] / closes[0] - 1 if closes[0] > 0 else 0
    line_color = C_PROFIT if ret > 0 else C_LOSS if ret < 0 else C_NEUTRAL
    ret_str = f"{ret*100:+.2f}%"

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'style="background:{C_BG};border-radius:6px;">',

        # Title
        f'<text x="{margin["left"]+4}" y="14" '
        f'font-size="11" font-weight="600" fill="#333">'
        f'{_escape(name)} ({code})</text>',
        f'<text x="{width - margin["right"]}" y="14" '
        f'font-size="11" font-weight="700" fill="{line_color}" '
        f'text-anchor="end">{ret_str}</text>',

        # Grid lines (3 horizontal)
        _svg_hgrid(y_min, y_max, margin, plot_w, plot_h, 3),

        # Price line
        f'<polyline points="{points}" '
        f'fill="none" stroke="{line_color}" stroke-width="1.5"/>',
    ]

    # Trail stop line (dashed red)
    if trail_stop_price > 0 and y_min <= trail_stop_price <= y_max:
        ty = y_pos(trail_stop_price)
        svg_parts.append(
            f'<line x1="{margin["left"]}" y1="{ty:.1f}" '
            f'x2="{margin["left"]+plot_w}" y2="{ty:.1f}" '
            f'stroke="{C_TRAIL}" stroke-width="1" stroke-dasharray="4,3"/>')
        svg_parts.append(
            f'<text x="{width-margin["right"]+3}" y="{ty+3:.1f}" '
            f'font-size="9" fill="{C_TRAIL}">TS {trail_stop_price:,.0f}</text>')

    # HWM line (dashed gray)
    if high_watermark > 0 and y_min <= high_watermark <= y_max:
        hy = y_pos(high_watermark)
        svg_parts.append(
            f'<line x1="{margin["left"]}" y1="{hy:.1f}" '
            f'x2="{margin["left"]+plot_w}" y2="{hy:.1f}" '
            f'stroke="{C_HWM}" stroke-width="1" stroke-dasharray="2,4"/>')
        svg_parts.append(
            f'<text x="{width-margin["right"]+3}" y="{hy+3:.1f}" '
            f'font-size="9" fill="{C_HWM}">HW {high_watermark:,.0f}</text>')

    # Avg price line (dotted blue)
    if avg_price > 0 and y_min <= avg_price <= y_max:
        ay = y_pos(avg_price)
        svg_parts.append(
            f'<line x1="{margin["left"]}" y1="{ay:.1f}" '
            f'x2="{margin["left"]+plot_w}" y2="{ay:.1f}" '
            f'stroke="{C_LOSS}" stroke-width="1" stroke-dasharray="1,3"/>')

    # Time labels (first, mid, last)
    if time_labels:
        label_indices = [0, len(time_labels) // 2, len(time_labels) - 1]
        for idx in label_indices:
            if idx < len(time_labels) and time_labels[idx]:
                svg_parts.append(
                    f'<text x="{x_pos(idx):.1f}" y="{height-4}" '
                    f'font-size="9" fill="{C_NEUTRAL}" text-anchor="middle">'
                    f'{time_labels[idx]}</text>')

    # Current price label
    svg_parts.append(
        f'<text x="{width-margin["right"]+3}" y="{y_pos(closes[-1])+3:.1f}" '
        f'font-size="9" font-weight="600" fill="{line_color}">'
        f'{closes[-1]:,.0f}</text>')

    svg_parts.append("</svg>")

    return (f'<div style="display:inline-block;margin:4px;">'
            f'{"".join(svg_parts)}</div>')


# ── Internal Rendering Helpers ───────────────────────────────────────────────

def _render_line_chart(
    values: List[float],
    time_labels: List[str],
    width: int,
    height: int,
    y_format: str = "pct",
    title: str = "",
    fill_zero: bool = False,
    overlay_values: Optional[List[float]] = None,
    overlay_label: str = "",
) -> str:
    """Render a simple line/area chart as SVG string, with optional overlay line."""
    if not values:
        return ""

    margin = {"top": 20, "right": 50, "bottom": 24, "left": 10}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]

    # Include overlay in y-range calculation
    all_vals = list(values)
    if overlay_values:
        all_vals.extend(overlay_values)
    y_min = min(all_vals)
    y_max = max(all_vals)
    # Add padding
    y_range = y_max - y_min if y_max != y_min else abs(y_max) * 0.1 or 0.001
    y_min -= y_range * 0.1
    y_max += y_range * 0.1

    def x_pos(i):
        return margin["left"] + (i / max(len(values) - 1, 1)) * plot_w

    def y_pos(v):
        return margin["top"] + (1 - (v - y_min) / (y_max - y_min)) * plot_h

    # Build points
    line_points = " ".join(f"{x_pos(i):.1f},{y_pos(v):.1f}"
                           for i, v in enumerate(values))

    last_val = values[-1]
    line_color = C_PROFIT if last_val > 0 else C_LOSS if last_val < 0 else C_NEUTRAL

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'style="background:{C_BG};border-radius:8px;">',
    ]

    if title:
        svg.append(f'<text x="{margin["left"]+4}" y="14" '
                   f'font-size="12" font-weight="600" fill="#333">'
                   f'{_escape(title)}</text>')

    # Grid + zero line
    svg.append(_svg_hgrid(y_min, y_max, margin, plot_w, plot_h, 4))

    # Zero line (if range includes zero)
    if y_min < 0 < y_max:
        zy = y_pos(0)
        svg.append(f'<line x1="{margin["left"]}" y1="{zy:.1f}" '
                   f'x2="{margin["left"]+plot_w}" y2="{zy:.1f}" '
                   f'stroke="#333" stroke-width="0.5"/>')

    # Area fill (above/below zero)
    if fill_zero and y_min < 0 < y_max:
        zy = y_pos(0)
        # Build clip paths for above/below zero fill
        above_pts = []
        below_pts = []
        for i, v in enumerate(values):
            xp = x_pos(i)
            yp = y_pos(v)
            above_pts.append(f"{xp:.1f},{min(yp, zy):.1f}")
            below_pts.append(f"{xp:.1f},{max(yp, zy):.1f}")

        # Above zero area (red tint)
        above_poly = (f"{x_pos(0):.1f},{zy:.1f} "
                      + " ".join(above_pts)
                      + f" {x_pos(len(values)-1):.1f},{zy:.1f}")
        svg.append(f'<polygon points="{above_poly}" '
                   f'fill="{C_AREA_UP}" stroke="none"/>')

        # Below zero area (blue tint)
        below_poly = (f"{x_pos(0):.1f},{zy:.1f} "
                      + " ".join(below_pts)
                      + f" {x_pos(len(values)-1):.1f},{zy:.1f}")
        svg.append(f'<polygon points="{below_poly}" '
                   f'fill="{C_AREA_DN}" stroke="none"/>')

    # Price line
    svg.append(f'<polyline points="{line_points}" '
               f'fill="none" stroke="{line_color}" stroke-width="1.8"/>')

    # Y axis labels
    n_ticks = 4
    for i in range(n_ticks + 1):
        val = y_min + (y_max - y_min) * i / n_ticks
        yp = y_pos(val)
        if y_format == "pct":
            label = f"{val*100:+.2f}%"
        else:
            label = f"{val:,.0f}"
        svg.append(f'<text x="{width-margin["right"]+4}" y="{yp+3:.1f}" '
                   f'font-size="9" fill="{C_NEUTRAL}">{label}</text>')

    # Time labels (09:00, 11:00, 13:00, 15:00)
    if time_labels:
        target_times = ["09:00", "10:00", "11:00", "12:00",
                        "13:00", "14:00", "15:00"]
        for tt in target_times:
            for i, tl in enumerate(time_labels):
                if tl == tt:
                    svg.append(
                        f'<text x="{x_pos(i):.1f}" y="{height-4}" '
                        f'font-size="9" fill="{C_NEUTRAL}" '
                        f'text-anchor="middle">{tt}</text>')
                    break

    # Current value label
    if y_format == "pct":
        val_label = f"{last_val*100:+.2f}%"
    else:
        val_label = f"{last_val:,.0f}"
    svg.append(f'<text x="{width-margin["right"]+4}" y="{y_pos(last_val)-6:.1f}" '
               f'font-size="10" font-weight="700" fill="{line_color}">'
               f'{val_label}</text>')

    # Overlay line (KOSPI etc.)
    if overlay_values and len(overlay_values) >= 2:
        # Resample overlay to match portfolio length
        ov_len = len(overlay_values)
        main_len = len(values)
        ov_points = []
        for i in range(main_len):
            # Map portfolio index to overlay index
            ov_idx = int(i * (ov_len - 1) / max(main_len - 1, 1))
            ov_idx = min(ov_idx, ov_len - 1)
            ov_val = overlay_values[ov_idx]
            ov_points.append(f"{x_pos(i):.1f},{y_pos(ov_val):.1f}")

        ov_line = " ".join(ov_points)
        ov_color = "#90a4ae"  # gray for KOSPI
        svg.append(f'<polyline points="{ov_line}" '
                   f'fill="none" stroke="{ov_color}" '
                   f'stroke-width="1.2" stroke-dasharray="4,3" opacity="0.7"/>')

        # Overlay end label
        ov_last = overlay_values[-1]
        if y_format == "pct":
            ov_label = f"{ov_last*100:+.2f}%"
        else:
            ov_label = f"{ov_last:,.0f}"
        ov_y = y_pos(ov_last)
        # Avoid label collision with main line
        main_y = y_pos(last_val)
        if abs(ov_y - main_y) < 12:
            ov_y = main_y + (12 if ov_y > main_y else -12)
        ol_text = f"{overlay_label} {ov_label}" if overlay_label else ov_label
        svg.append(f'<text x="{width-margin["right"]+4}" y="{ov_y+3:.1f}" '
                   f'font-size="9" fill="{ov_color}">'
                   f'{ol_text}</text>')

    svg.append("</svg>")
    return "".join(svg)


def _svg_hgrid(y_min: float, y_max: float, margin: dict,
               plot_w: float, plot_h: float, n: int) -> str:
    """Generate horizontal grid lines."""
    lines = []
    for i in range(n + 1):
        frac = i / n
        y = margin["top"] + frac * plot_h
        lines.append(
            f'<line x1="{margin["left"]}" y1="{y:.1f}" '
            f'x2="{margin["left"]+plot_w}" y2="{y:.1f}" '
            f'stroke="{C_GRID}" stroke-width="0.5"/>')
    return "".join(lines)
