"""
trade_quality_analyzer.py -- Post-hoc Trade Quality Analysis
=============================================================
Standalone script reading CSV logs from report/output/.
No dependency on trading engine. Read-only analysis.

Analyses:
  1. Entry rank vs PnL correlation (rank buckets)
  2. Exit type comparison (REBAL vs TRAIL_STOP)
  3. Regime-stratified returns (BULL/SIDE/BEAR)
  4. Exit efficiency (daily OHLCV, gap-filtered)
  5. Timing analysis (next-day return after entry/exit)

Usage:
  cd kr-legacy
  python -m report.trade_quality_analyzer
  python -m report.trade_quality_analyzer --period 2026-03
  python -m report.trade_quality_analyzer --html
  python -m report.trade_quality_analyzer --archive
"""
from __future__ import annotations

import argparse
import logging
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger("gen4.quality")

# ── Defaults ────────────────────────────────────────────────────────────────
_BASE = Path(__file__).resolve().parent.parent
_REPORT_DIR = _BASE / "report" / "output"
_OHLCV_DIR = _BASE.parent / "backtest" / "data_full" / "ohlcv"
_ARCHIVE_DAYS = 30


# ── CSV Loaders ─────────────────────────────────────────────────────────────

def _load(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def load_all(report_dir: Path) -> dict:
    return {
        "closes": _load(report_dir / "close_log.csv"),
        "equity": _load(report_dir / "equity_log.csv"),
        "decisions": _load(report_dir / "decision_log.csv"),
        "positions": _load(report_dir / "daily_positions.csv"),
    }


def _filter_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Filter df to rows matching YYYY-MM period."""
    if df.empty or "date" not in df.columns:
        return df
    return df[df["date"].astype(str).str.startswith(period)]


def _load_ohlcv(code: str, ohlcv_dir: Path) -> pd.DataFrame:
    """Load OHLCV CSV for a stock."""
    path = ohlcv_dir / f"{code}.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date").set_index("date")
        return df
    except Exception:
        return pd.DataFrame()


# ── Analysis Functions ──────────────────────────────────────────────────────

def analyze_entry_rank_vs_pnl(closes: pd.DataFrame) -> dict:
    """Rank bucket analysis: entry_rank vs realized PnL."""
    if closes.empty or "entry_rank" not in closes.columns:
        return {"available": False}

    df = closes.copy()
    df["entry_rank"] = pd.to_numeric(df["entry_rank"], errors="coerce")
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce")
    df = df.dropna(subset=["entry_rank", "pnl_pct"])
    df = df[df["entry_rank"] > 0]  # exclude unrecorded (0)

    if df.empty:
        return {"available": False}

    buckets = {}
    for lo, hi, label in [(1, 5, "1-5"), (6, 10, "6-10"),
                           (11, 15, "11-15"), (16, 20, "16-20")]:
        subset = df[(df["entry_rank"] >= lo) & (df["entry_rank"] <= hi)]
        if subset.empty:
            buckets[label] = {"count": 0, "avg_pnl": 0, "win_rate": 0}
        else:
            wins = (subset["pnl_pct"] > 0).sum()
            buckets[label] = {
                "count": len(subset),
                "avg_pnl": float(subset["pnl_pct"].mean()),
                "win_rate": float(wins / len(subset)),
            }

    corr = float(df[["entry_rank", "pnl_pct"]].corr().iloc[0, 1]) if len(df) > 2 else 0
    return {"available": True, "buckets": buckets, "correlation": corr,
            "total_trades": len(df)}


def analyze_exit_type(closes: pd.DataFrame) -> dict:
    """Compare REBAL vs TRAIL_STOP exits."""
    if closes.empty or "exit_reason" not in closes.columns:
        return {"available": False}

    df = closes.copy()
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce")
    df["hold_days"] = pd.to_numeric(df["hold_days"], errors="coerce")

    result = {}
    for exit_type in ["REBALANCE_EXIT", "TRAIL_STOP_FILLED", "TRAIL_STOP_HIT"]:
        subset = df[df["exit_reason"].str.contains(exit_type, case=False, na=False)]
        if subset.empty:
            continue
        wins = (subset["pnl_pct"] > 0).sum()
        label = "REBAL" if "REBALANCE" in exit_type else "TRAIL"
        if label in result:
            # Merge trail variants
            prev = result[label]
            combined = pd.concat([
                df[df["exit_reason"].str.contains("TRAIL", case=False, na=False)]])
            wins = (combined["pnl_pct"] > 0).sum()
            result[label] = {
                "count": len(combined),
                "avg_pnl": float(combined["pnl_pct"].mean()),
                "avg_hold": float(combined["hold_days"].mean()),
                "win_rate": float(wins / len(combined)),
            }
        else:
            result[label] = {
                "count": len(subset),
                "avg_pnl": float(subset["pnl_pct"].mean()),
                "avg_hold": float(subset["hold_days"].mean()),
                "win_rate": float(wins / len(subset)),
            }

    return {"available": bool(result), **result}


def analyze_regime_returns(equity: pd.DataFrame) -> dict:
    """Stratify daily returns by regime."""
    if equity.empty or "regime" not in equity.columns:
        return {"available": False}

    df = equity.copy()
    df["daily_pnl_pct"] = pd.to_numeric(df["daily_pnl_pct"], errors="coerce")
    df = df.dropna(subset=["daily_pnl_pct"])
    df = df[df["regime"].isin(["BULL", "SIDE", "BEAR"])]

    if df.empty:
        return {"available": False}

    result = {}
    for regime in ["BULL", "SIDE", "BEAR"]:
        subset = df[df["regime"] == regime]
        if subset.empty:
            result[regime] = {"days": 0, "avg_daily": 0, "total": 0}
        else:
            result[regime] = {
                "days": len(subset),
                "avg_daily": float(subset["daily_pnl_pct"].mean()),
                "total": float(subset["daily_pnl_pct"].sum()),
            }

    return {"available": True, **result}


def analyze_exit_efficiency(closes: pd.DataFrame,
                            ohlcv_dir: Path) -> dict:
    """Exit efficiency: (exit - low) / (high - low) during hold period.

    Uses daily OHLCV only. Excludes trades with >10% gap during hold.
    """
    if closes.empty:
        return {"available": False}

    df = closes.copy()
    df["entry_price"] = pd.to_numeric(df["entry_price"], errors="coerce")
    df["exit_price"] = pd.to_numeric(df["exit_price"], errors="coerce")

    efficiencies = []
    skipped = 0

    for _, row in df.iterrows():
        code = str(row.get("code", ""))
        entry_date = str(row.get("entry_date", ""))
        exit_date = str(row.get("date", ""))
        exit_price = float(row.get("exit_price", 0))

        if not code or not entry_date or exit_price <= 0:
            skipped += 1
            continue

        ohlcv = _load_ohlcv(code, ohlcv_dir)
        if ohlcv.empty or "High" not in ohlcv.columns:
            # Try lowercase
            if "high" in ohlcv.columns:
                ohlcv = ohlcv.rename(columns={"high": "High", "low": "Low",
                                               "open": "Open", "close": "Close"})
            else:
                skipped += 1
                continue

        try:
            # Normalize date formats
            ed = pd.Timestamp(entry_date)
            xd = pd.Timestamp(exit_date)
            hold = ohlcv.loc[ed:xd]
            if hold.empty or len(hold) < 2:
                skipped += 1
                continue

            # Gap filter: daily open/close gap > 10%
            if "Open" in hold.columns:
                gaps = hold["Open"].pct_change().abs()
                if (gaps > 0.10).any():
                    skipped += 1
                    continue

            high = float(hold["High"].max())
            low = float(hold["Low"].min())
            range_ = high - low

            if range_ < low * 0.001:
                eff = 1.0
            else:
                eff = (exit_price - low) / range_
                eff = max(0.0, min(1.0, eff))

            efficiencies.append(eff)
        except Exception:
            skipped += 1

    if not efficiencies:
        return {"available": False, "skipped": skipped}

    return {
        "available": True,
        "avg": float(np.mean(efficiencies)),
        "median": float(np.median(efficiencies)),
        "count": len(efficiencies),
        "skipped": skipped,
    }


def analyze_timing(closes: pd.DataFrame, ohlcv_dir: Path) -> dict:
    """Next-day return analysis after entry and exit."""
    if closes.empty:
        return {"available": False}

    entry_next = []
    exit_next = []

    for _, row in closes.iterrows():
        code = str(row.get("code", ""))
        entry_date = str(row.get("entry_date", ""))
        exit_date = str(row.get("date", ""))

        ohlcv = _load_ohlcv(code, ohlcv_dir)
        if ohlcv.empty:
            continue

        close_col = "Close" if "Close" in ohlcv.columns else "close"
        if close_col not in ohlcv.columns:
            continue

        try:
            # Entry next-day return
            ed = pd.Timestamp(entry_date)
            after_entry = ohlcv.loc[ed:]
            if len(after_entry) >= 2:
                ret = (float(after_entry[close_col].iloc[1]) /
                       float(after_entry[close_col].iloc[0]) - 1)
                entry_next.append(ret)

            # Exit next-day return
            xd = pd.Timestamp(exit_date)
            after_exit = ohlcv.loc[xd:]
            if len(after_exit) >= 2:
                ret = (float(after_exit[close_col].iloc[1]) /
                       float(after_exit[close_col].iloc[0]) - 1)
                exit_next.append(ret)
        except Exception:
            continue

    return {
        "available": bool(entry_next or exit_next),
        "entry_next_day_avg": float(np.mean(entry_next)) if entry_next else 0,
        "entry_next_day_count": len(entry_next),
        "exit_next_day_avg": float(np.mean(exit_next)) if exit_next else 0,
        "exit_next_day_count": len(exit_next),
    }


# ── Archive ─────────────────────────────────────────────────────────────────

def archive_old_logs(report_dir: Path, days: int = _ARCHIVE_DAYS):
    """Move daily_positions rows older than N days to archive/YYYY-MM/ files."""
    pos_file = report_dir / "daily_positions.csv"
    if not pos_file.exists():
        print(f"No daily_positions.csv found in {report_dir}")
        return

    df = pd.read_csv(pos_file, encoding="utf-8-sig")
    if df.empty or "date" not in df.columns:
        print("Empty or invalid daily_positions.csv")
        return

    cutoff = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    old = df[df["date"] < cutoff]
    recent = df[df["date"] >= cutoff]

    if old.empty:
        print(f"No rows older than {cutoff} to archive.")
        return

    # Group old rows by month
    old["_month"] = old["date"].str[:7]
    archive_dir = report_dir / "archive"
    archive_dir.mkdir(exist_ok=True)

    for month, group in old.groupby("_month"):
        month_dir = archive_dir / month
        month_dir.mkdir(exist_ok=True)
        out = month_dir / "daily_positions.csv"
        if out.exists():
            existing = pd.read_csv(out, encoding="utf-8-sig")
            group = pd.concat([existing, group.drop(columns=["_month"])]).drop_duplicates()
        else:
            group = group.drop(columns=["_month"])
        group.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"  Archived {len(group)} rows -> {out}")

    # Write back only recent rows
    recent.to_csv(pos_file, index=False, encoding="utf-8-sig")
    print(f"Archived {len(old)} rows (cutoff={cutoff}), {len(recent)} rows remaining.")


# ── Output Formatting ───────────────────────────────────────────────────────

def format_console(results: dict) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("  Gen4 Trade Quality Analysis")
    lines.append("=" * 60)

    # 1. Entry Rank
    r = results.get("entry_rank", {})
    lines.append("\n=== Entry Quality (Rank vs PnL) ===")
    if r.get("available"):
        lines.append(f"Total trades: {r['total_trades']}  "
                     f"Rank-PnL correlation: {r['correlation']:.3f}")
        for label, b in r["buckets"].items():
            if b["count"] > 0:
                lines.append(f"  Rank {label:5s}: "
                             f"avg {b['avg_pnl']:+.2%}  "
                             f"win {b['win_rate']:.0%}  "
                             f"(n={b['count']})")
    else:
        lines.append("  No entry_rank data yet.")

    # 2. Exit Type
    r = results.get("exit_type", {})
    lines.append("\n=== Exit Type Performance ===")
    if r.get("available"):
        for t in ["REBAL", "TRAIL"]:
            if t in r:
                d = r[t]
                lines.append(f"  {t:8s}: "
                             f"avg {d['avg_pnl']:+.2%}  "
                             f"win {d['win_rate']:.0%}  "
                             f"hold {d['avg_hold']:.0f}d  "
                             f"(n={d['count']})")
    else:
        lines.append("  No close data yet.")

    # 3. Regime
    r = results.get("regime", {})
    lines.append("\n=== Regime Returns ===")
    if r.get("available"):
        for regime in ["BULL", "SIDE", "BEAR"]:
            if regime in r:
                d = r[regime]
                lines.append(f"  {regime:4s}: "
                             f"{d['days']:3d} days  "
                             f"avg {d['avg_daily']:+.4f}/day  "
                             f"total {d['total']:+.4f}")
    else:
        lines.append("  No regime data yet (will populate after live sessions).")

    # 4. Exit Efficiency
    r = results.get("efficiency", {})
    lines.append("\n=== Exit Efficiency ===")
    if r.get("available"):
        lines.append(f"  avg={r['avg']:.2f}  median={r['median']:.2f}  "
                     f"(n={r['count']}, skipped={r['skipped']})")
        lines.append("  (0=sold at low, 1=sold at high)")
    else:
        lines.append(f"  Insufficient data (skipped={r.get('skipped', 0)}).")

    # 5. Timing
    r = results.get("timing", {})
    lines.append("\n=== Timing Analysis ===")
    if r.get("available"):
        lines.append(f"  Entry next-day avg: {r['entry_next_day_avg']:+.4f}  "
                     f"(n={r['entry_next_day_count']})")
        lines.append(f"  Exit  next-day avg: {r['exit_next_day_avg']:+.4f}  "
                     f"(n={r['exit_next_day_count']})")
        # Interpretation
        if r["entry_next_day_avg"] > 0:
            lines.append("  -> Entry timing: GOOD (price continued up)")
        else:
            lines.append("  -> Entry timing: POOR (price dropped next day)")
        if r["exit_next_day_avg"] < 0:
            lines.append("  -> Exit timing: GOOD (price dropped after exit)")
        else:
            lines.append("  -> Exit timing: POOR (price rose after exit)")
    else:
        lines.append("  Insufficient OHLCV data for timing analysis.")

    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Gen4 Trade Quality Analyzer (post-hoc)")
    parser.add_argument("--report-dir", type=Path, default=_REPORT_DIR,
                        help="Path to report/output directory")
    parser.add_argument("--ohlcv-dir", type=Path, default=_OHLCV_DIR,
                        help="Path to OHLCV data directory")
    parser.add_argument("--period", type=str, default="",
                        help="Filter to YYYY-MM period")
    parser.add_argument("--archive", action="store_true",
                        help=f"Archive daily_positions older than {_ARCHIVE_DAYS} days")
    args = parser.parse_args()

    if args.archive:
        archive_old_logs(args.report_dir)
        return

    data = load_all(args.report_dir)
    closes = data["closes"]
    equity = data["equity"]

    if args.period:
        closes = _filter_period(closes, args.period)
        equity = _filter_period(equity, args.period)

    results = {
        "entry_rank": analyze_entry_rank_vs_pnl(closes),
        "exit_type": analyze_exit_type(closes),
        "regime": analyze_regime_returns(equity),
        "efficiency": analyze_exit_efficiency(closes, args.ohlcv_dir),
        "timing": analyze_timing(closes, args.ohlcv_dir),
    }

    print(format_console(results))


if __name__ == "__main__":
    main()
