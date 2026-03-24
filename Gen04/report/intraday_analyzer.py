"""
intraday_analyzer.py — EOD Intraday Analytics (Phase 1)
========================================================
Reads minute bar data from IntradayCollector CSV files and computes:
  1. VWAP (typical price based)
  2. Max intraday drawdown (running high vs close)
  3. Max 5-minute drop (close-close)
  4. Volume spike count (shift(1) baseline, 2x threshold)

Output: per-stock dict + portfolio summary dict.
Used by daily_report.py for HTML section and standalone JSON/CSV export.

Definitions (fixed):
  - VWAP: sum(typical_price * volume) / sum(volume), typical = (H+L+C)/3
  - max_intraday_dd: running cummax(high) vs close, worst ratio
  - max_5m_drop: close[i] vs close[i-4], worst drop
  - volume_spike: volume[i] / mean(volume[i-5..i-1]) > 2.0
  - prev_close: if unavailable, from_prev_close_pct = None (no substitution)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger("gen4.intraday_analyzer")

VOLUME_SPIKE_2X = 2.0
VOLUME_SPIKE_3X = 3.0
VOLUME_SPIKE_WINDOW = 5
OPENING_MINUTES = 5          # 09:00~09:04 = first 5 bars (spike counted separately)
MIN_BARS_FOR_ANALYSIS = 5
NEAR_TRAIL_THRESHOLD = -10.0  # % — DD worse than this → near_trail_stop flag
FULL_SESSION_BARS = 360       # 09:00~14:59 = ~360 bars for a full day


# ── Per-Stock Analysis ────────────────────────────────────────────────────

def analyze_stock(code: str, bars: pd.DataFrame,
                  prev_close: Optional[float] = None) -> dict:
    """Analyze one stock's intraday minute bars.

    Args:
        code: Stock ticker code.
        bars: DataFrame with columns [datetime, open, high, low, close, volume, status].
        prev_close: Previous day's closing price. None if unavailable.

    Returns:
        Dict with analysis results + analysis_warnings list.
    """
    warnings = []

    if bars.empty or len(bars) < MIN_BARS_FOR_ANALYSIS:
        warnings.append("insufficient_bars")
        return {
            "code": code,
            "n_bars": len(bars),
            "analysis_warnings": warnings,
        }

    close_col = bars["close"].astype(float)
    high_col = bars["high"].astype(float)
    low_col = bars["low"].astype(float)
    volume_col = bars["volume"].astype(float)

    last_close = close_col.iloc[-1]
    intraday_high = high_col.max()
    intraday_low = low_col.min()

    # -- Zero volume check --
    zero_vol_count = (volume_col <= 0).sum()
    if zero_vol_count > len(bars) * 0.5:
        warnings.append("zero_volume_bars")

    # -- VWAP --
    typical_price = (high_col + low_col + close_col) / 3
    vol_mask = volume_col > 0
    if vol_mask.sum() > 0:
        tp_vol = (typical_price * volume_col).where(vol_mask, 0).cumsum()
        cum_vol = volume_col.where(vol_mask, 0).cumsum()
        vwap_series = tp_vol / cum_vol.replace(0, float("nan"))
        vwap = vwap_series.iloc[-1]
        if pd.isna(vwap):
            vwap = last_close
            warnings.append("vwap_calc_failed")
    else:
        vwap = last_close
        warnings.append("no_volume_for_vwap")

    close_vs_vwap_pct = (last_close - vwap) / vwap * 100 if vwap > 0 else 0.0

    # -- From prev_close --
    if prev_close and prev_close > 0:
        from_prev_close_pct = (last_close - prev_close) / prev_close * 100
    else:
        from_prev_close_pct = None
        warnings.append("prev_close_missing")

    # -- From intraday high --
    from_intraday_high_pct = ((last_close - intraday_high) / intraday_high * 100
                               if intraday_high > 0 else 0.0)

    # -- Max intraday drawdown (running high vs close, close-based) --
    running_high = high_col.cummax()
    drawdown = (close_col - running_high) / running_high * 100
    drawdown = drawdown.replace([float("inf"), float("-inf")], 0)
    max_dd_idx = drawdown.idxmin()
    max_intraday_dd_pct = drawdown.iloc[max_dd_idx] if not pd.isna(max_dd_idx) else 0.0
    max_dd_time = bars["datetime"].iloc[max_dd_idx] if not pd.isna(max_dd_idx) else ""
    # Extract time only (HH:MM)
    if max_dd_time and " " in str(max_dd_time):
        max_dd_time = str(max_dd_time).split(" ")[-1][:5]

    # -- Max 5m drop (close-close, shift(4) = 5 bars span) --
    if len(bars) >= 5:
        drop_5m = (close_col - close_col.shift(4)) / close_col.shift(4) * 100
        drop_5m = drop_5m.dropna()
        if not drop_5m.empty:
            max_5m_drop_idx = drop_5m.idxmin()
            max_5m_drop_pct = drop_5m.loc[max_5m_drop_idx]
            max_5m_drop_time = bars["datetime"].iloc[max_5m_drop_idx]
            if max_5m_drop_time and " " in str(max_5m_drop_time):
                max_5m_drop_time = str(max_5m_drop_time).split(" ")[-1][:5]
        else:
            max_5m_drop_pct = 0.0
            max_5m_drop_time = ""
    else:
        max_5m_drop_pct = 0.0
        max_5m_drop_time = ""

    # -- Volume spike (shift(1) baseline, exclude self) --
    baseline = volume_col.shift(1).rolling(VOLUME_SPIKE_WINDOW, min_periods=3).mean()
    valid_baseline = baseline.notna() & (baseline > 0)
    ratio = pd.Series(0.0, index=bars.index)
    ratio[valid_baseline] = volume_col[valid_baseline] / baseline[valid_baseline]

    # Parse time for opening filter
    dt_str = bars["datetime"].astype(str)
    time_part = dt_str.str.split(" ").str[-1].str[:5]  # "HH:MM"
    is_opening = time_part < "09:05"

    spike_2x_mask = (ratio > VOLUME_SPIKE_2X) & valid_baseline & ~is_opening
    spike_3x_mask = (ratio > VOLUME_SPIKE_3X) & valid_baseline & ~is_opening
    spike_opening_mask = (ratio > VOLUME_SPIKE_2X) & valid_baseline & is_opening

    volume_spike_2x = int(spike_2x_mask.sum())
    volume_spike_3x = int(spike_3x_mask.sum())
    volume_spike_opening = int(spike_opening_mask.sum())
    max_volume_ratio = float(ratio[valid_baseline & ~is_opening].max()) \
        if (valid_baseline & ~is_opening).any() else 0.0

    # -- Partial day detection --
    last_dt = str(bars["datetime"].iloc[-1])
    analysis_end_time = last_dt.split(" ")[-1][:5] if " " in last_dt else last_dt
    is_partial_day = len(bars) < FULL_SESSION_BARS * 0.8  # <80% of full session

    # -- Near trail stop flag --
    near_trail_stop = float(max_intraday_dd_pct) <= NEAR_TRAIL_THRESHOLD

    return {
        "code": code,
        "n_bars": int(len(bars)),
        # VWAP
        "vwap": round(vwap, 1),
        "close": round(last_close, 1),
        "close_vs_vwap_pct": round(close_vs_vwap_pct, 2),
        # Drawdown
        "intraday_high": round(intraday_high, 1),
        "intraday_low": round(intraday_low, 1),
        "from_intraday_high_pct": round(from_intraday_high_pct, 2),
        "from_prev_close_pct": round(from_prev_close_pct, 2) if from_prev_close_pct is not None else None,
        "max_intraday_dd_pct": round(float(max_intraday_dd_pct), 2),
        "max_dd_time": str(max_dd_time),
        # 5m drop
        "max_5m_drop_pct": round(float(max_5m_drop_pct), 2),
        "max_5m_drop_time": str(max_5m_drop_time),
        # Volume (opening excluded from main count)
        "volume_spike_2x": volume_spike_2x,
        "volume_spike_3x": volume_spike_3x,
        "volume_spike_opening": volume_spike_opening,
        "max_volume_ratio": round(max_volume_ratio, 1),
        # Session info
        "analysis_end_time": analysis_end_time,
        "is_partial_day": is_partial_day,
        # Risk flags
        "near_trail_stop": near_trail_stop,
        # Warnings
        "analysis_warnings": warnings,
    }


# ── Portfolio-Level Analysis ──────────────────────────────────────────────

def extract_prev_closes(intraday_dir: Path, today_str: str,
                        codes: List[str]) -> Dict[str, float]:
    """Extract previous day's last close from intraday CSV files.

    Since intraday files contain multiple days, we read the full file
    and pick the last close before today_str.
    """
    prev_closes = {}
    intraday_dir = Path(intraday_dir)
    for code in codes:
        path = intraday_dir / f"{code}.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            if df.empty or "datetime" not in df.columns:
                continue
            prev_rows = df[~df["datetime"].astype(str).str.startswith(today_str)]
            if not prev_rows.empty:
                prev_closes[code] = float(prev_rows["close"].iloc[-1])
        except Exception:
            continue
    return prev_closes


def analyze_all(bars_by_code: Dict[str, pd.DataFrame],
                prev_closes: Optional[Dict[str, float]] = None) -> List[dict]:
    """Analyze all stocks. Returns list of per-stock results."""
    if prev_closes is None:
        prev_closes = {}
    results = []
    for code, bars in sorted(bars_by_code.items()):
        try:
            r = analyze_stock(code, bars, prev_closes.get(code))
            results.append(r)
        except Exception as e:
            logger.warning(f"[INTRADAY_ANALYZE] {code} failed: {e}")
            results.append({
                "code": code,
                "n_bars": 0,
                "analysis_warnings": [f"error: {e}"],
            })
    return results


def generate_summary(results: List[dict], today_str: str = "") -> dict:
    """Generate portfolio-level summary from per-stock results."""
    if not today_str:
        today_str = date.today().strftime("%Y-%m-%d")

    # Filter to stocks with actual analysis
    analyzed = [r for r in results if r.get("n_bars", 0) >= MIN_BARS_FOR_ANALYSIS]
    n_stocks = len(analyzed)

    if n_stocks == 0:
        return {
            "date": today_str,
            "n_stocks": 0,
            "per_stock": results,
            "analysis_warnings": ["no_stocks_analyzed"],
        }

    # Worst drawdown
    dd_values = [(r["code"], r.get("max_intraday_dd_pct", 0)) for r in analyzed]
    dd_values.sort(key=lambda x: x[1])
    worst_dd_code, worst_dd_pct = dd_values[0]

    avg_max_dd = sum(r.get("max_intraday_dd_pct", 0) for r in analyzed) / n_stocks

    # VWAP below count
    vwap_below = sum(1 for r in analyzed if r.get("close_vs_vwap_pct", 0) < 0)

    # Volume spikes (opening excluded)
    total_spikes_2x = sum(r.get("volume_spike_2x", 0) for r in analyzed)
    total_spikes_3x = sum(r.get("volume_spike_3x", 0) for r in analyzed)
    total_spikes_opening = sum(r.get("volume_spike_opening", 0) for r in analyzed)

    # Near trail stop count
    near_trail_count = sum(1 for r in analyzed if r.get("near_trail_stop", False))

    # Partial day detection (summary level)
    partial_count = sum(1 for r in analyzed if r.get("is_partial_day", False))
    is_partial_session = partial_count > n_stocks * 0.5

    # Risk score (reference only, 0~100)
    risk_score = (
        min(abs(avg_max_dd) * 10, 40)
        + min(vwap_below / max(n_stocks, 1) * 30, 30)
        + min(total_spikes_2x * 2, 30)
    )
    risk_score = round(min(risk_score, 100), 1)

    return {
        "date": today_str,
        "n_stocks": n_stocks,
        "worst_dd_code": worst_dd_code,
        "worst_dd_pct": round(worst_dd_pct, 2),
        "avg_max_dd_pct": round(avg_max_dd, 2),
        "vwap_below_count": vwap_below,
        "total_volume_spikes_2x": total_spikes_2x,
        "total_volume_spikes_3x": total_spikes_3x,
        "total_volume_spikes_opening": total_spikes_opening,
        "near_trail_count": near_trail_count,
        "is_partial_session": is_partial_session,
        "risk_score": risk_score,
        "per_stock": results,
    }


# ── Output Helpers ────────────────────────────────────────────────────────

def save_json(summary: dict, output_dir: Path, today_str: str = "") -> Path:
    """Save summary as JSON."""
    if not today_str:
        today_str = summary.get("date", date.today().strftime("%Y-%m-%d"))
    date_compact = today_str.replace("-", "")
    path = Path(output_dir) / f"intraday_summary_{date_compact}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
    return path


def save_csv(summary: dict, output_dir: Path, today_str: str = "") -> Path:
    """Save per-stock summary as CSV."""
    if not today_str:
        today_str = summary.get("date", date.today().strftime("%Y-%m-%d"))
    date_compact = today_str.replace("-", "")
    path = Path(output_dir) / f"intraday_summary_{date_compact}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)

    per_stock = summary.get("per_stock", [])
    if not per_stock:
        return path

    # Flatten — exclude nested fields
    csv_cols = [
        "code", "n_bars", "vwap", "close", "close_vs_vwap_pct",
        "intraday_high", "intraday_low", "from_intraday_high_pct",
        "from_prev_close_pct", "max_intraday_dd_pct", "max_dd_time",
        "max_5m_drop_pct", "max_5m_drop_time",
        "volume_spike_2x", "volume_spike_3x", "volume_spike_opening",
        "max_volume_ratio", "near_trail_stop",
        "analysis_end_time", "is_partial_day",
    ]
    rows = []
    for r in per_stock:
        row = {col: r.get(col, "") for col in csv_cols}
        rows.append(row)

    df = pd.DataFrame(rows, columns=csv_cols)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


# ── Standalone CLI ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.intraday_collector import IntradayCollector

    intraday_dir = Path(__file__).resolve().parent.parent / "data" / "intraday"
    output_dir = Path(__file__).resolve().parent / "output"

    target_date = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y-%m-%d")
    print(f"Analyzing intraday data for {target_date}...")

    bars = IntradayCollector.load_all_for_date(intraday_dir, target_date)
    print(f"Loaded {len(bars)} stocks")

    prev_closes = extract_prev_closes(intraday_dir, target_date, list(bars.keys()))
    print(f"Prev closes: {len(prev_closes)} stocks")

    results = analyze_all(bars, prev_closes)
    summary = generate_summary(results, target_date)

    json_path = save_json(summary, output_dir, target_date)
    csv_path = save_csv(summary, output_dir, target_date)
    print(f"JSON: {json_path}")
    print(f"CSV:  {csv_path}")

    # Print top 5 worst drawdowns
    analyzed = [r for r in results if r.get("n_bars", 0) >= MIN_BARS_FOR_ANALYSIS]
    analyzed.sort(key=lambda x: x.get("max_intraday_dd_pct", 0))
    print(f"\nRisk Score: {summary.get('risk_score', 'N/A')}")
    print(f"VWAP below: {summary.get('vwap_below_count', 0)}/{summary.get('n_stocks', 0)}")
    print(f"Volume spikes: 2x={summary.get('total_volume_spikes_2x', 0)} "
          f"3x={summary.get('total_volume_spikes_3x', 0)} "
          f"opening={summary.get('total_volume_spikes_opening', 0)}")
    print(f"Near trail stop: {summary.get('near_trail_count', 0)} stocks")
    print(f"Partial session: {summary.get('is_partial_session', False)}")
    print(f"\nTop 5 worst intraday drawdown (close-based):")
    for r in analyzed[:5]:
        trail_flag = " [NEAR TRAIL]" if r.get("near_trail_stop") else ""
        print(f"  {r['code']}: DD={r.get('max_intraday_dd_pct', 0):.2f}% "
              f"@ {r.get('max_dd_time', '?')} | "
              f"VWAP={r.get('close_vs_vwap_pct', 0):+.2f}% | "
              f"5mDrop={r.get('max_5m_drop_pct', 0):.2f}% | "
              f"Spikes={r.get('volume_spike_2x', 0)}"
              f"{trail_flag}")
