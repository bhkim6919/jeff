"""
Gen4 CLI Monitor — Read-only dashboard for stabilization check.
Reads state files written by the engine (atomic write safe).
No engine dependency — runs as a separate process.

Usage:
    python monitor_cli.py                  # paper mode (default)
    python monitor_cli.py --mode live      # live mode
    python monitor_cli.py --mode paper_test
    python monitor_cli.py --once           # single snapshot, no refresh
"""
from __future__ import annotations

import argparse
import json
import csv
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
REPORT_DIR = BASE_DIR / "report" / "output"
REPORT_DIR_TEST = BASE_DIR / "report" / "output_test"
LOG_DIR = BASE_DIR / "logs"

REFRESH_SEC = 5


# ── Safe file readers ────────────────────────────────────────────────────
def _read_json(path: Path) -> dict | None:
    """Read JSON with fallback to .bak. Returns None on failure."""
    for p in (path, path.with_suffix(".bak")):
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return None


def _read_csv_tail(path: Path, n: int = 5) -> list[dict]:
    """Read last n rows of a CSV. Returns list of dicts."""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        return rows[-n:] if rows else []
    except Exception:
        return []


def _read_log_tail(mode: str, n: int = 20) -> list[str]:
    """Read last n CRITICAL/WARNING lines from today's log."""
    today = datetime.now().strftime("%Y%m%d")
    # Try common log name patterns
    candidates = [
        LOG_DIR / f"gen4_live_{today}.log",
        LOG_DIR / f"gen4_paper_{today}.log",
        LOG_DIR / f"gen4_{mode}_{today}.log",
    ]
    for log_path in candidates:
        if not log_path.exists():
            continue
        try:
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            filtered = [
                ln for ln in lines
                if "[CRITICAL]" in ln or "[WARNING]" in ln or "[ERROR]" in ln
            ]
            return filtered[-n:]
        except Exception:
            continue
    return []


# ── Formatting helpers ───────────────────────────────────────────────────
def _fmt_krw(v: float | int) -> str:
    """Format KRW with commas."""
    return f"{int(v):,}"


def _pct(v: float) -> str:
    """Format as percentage."""
    return f"{v * 100:+.2f}%"


def _age_sec(ts_str: str | None) -> str:
    """Human-readable age from ISO timestamp."""
    if not ts_str:
        return "N/A"
    try:
        ts = datetime.fromisoformat(ts_str)
        delta = datetime.now() - ts
        secs = int(delta.total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        elif secs < 3600:
            return f"{secs // 60}m {secs % 60}s ago"
        else:
            return f"{secs // 3600}h {(secs % 3600) // 60}m ago"
    except Exception:
        return "N/A"


# ── Display ──────────────────────────────────────────────────────────────
def render(mode: str):
    """Render a single snapshot."""
    suffix = f"_{mode}"
    pf_path = STATE_DIR / f"portfolio_state{suffix}.json"
    rt_path = STATE_DIR / f"runtime_state{suffix}.json"
    report_dir = REPORT_DIR_TEST if mode == "paper_test" else REPORT_DIR

    pf = _read_json(pf_path)
    rt = _read_json(rt_path)

    # ── Header ────────────────────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\033[2J\033[H", end="")  # clear screen
    print("=" * 72)
    print(f"  Q-TRON Gen4 Monitor  |  Mode: {mode.upper()}  |  {now}")
    print("=" * 72)

    if pf is None:
        print(f"\n  [NO DATA] {pf_path.name} not found or unreadable")
        print(f"            Waiting for engine to start...\n")
        return

    # ── Status Cards ──────────────────────────────────────────────────
    positions = pf.get("positions", {})
    cash = pf.get("cash", 0)
    peak = pf.get("peak_equity", 0)
    prev_close = pf.get("prev_close_equity", 0)
    ts = pf.get("timestamp", "")
    version = pf.get("version", "?")

    # Calculate equity estimate (cash + sum of invested_total as proxy)
    total_invested = sum(
        p.get("quantity", 0) * p.get("avg_price", 0)
        for p in positions.values()
    )
    equity_est = cash + total_invested

    # Runtime info
    shutdown = rt.get("shutdown_reason", "?") if rt else "?"
    last_rebal = rt.get("last_rebalance_date", "N/A") if rt else "N/A"
    rebal_count = rt.get("rebalance_count", 0) if rt else 0
    pending = rt.get("pending_buys", []) if rt else []
    sell_status = rt.get("rebal_sell_status", "") if rt else ""
    session_start = rt.get("session_start", "") if rt else ""
    recon_unreliable = rt.get("recon_unreliable", False) if rt else False

    # DD from peak
    dd_from_peak = (equity_est / peak - 1) if peak > 0 else 0

    # Stale check (30s=WARN, 90s=STALE)
    age = _age_sec(ts)
    stale_level = 0  # 0=ok, 1=warn, 2=stale
    if ts:
        try:
            delta = (datetime.now() - datetime.fromisoformat(ts)).total_seconds()
            if delta > 90:
                stale_level = 2
            elif delta > 30:
                stale_level = 1
        except Exception:
            stale_level = 2

    stale_badge = (
        " \033[91m[STALE!]\033[0m" if stale_level == 2
        else " \033[93m[WARN]\033[0m" if stale_level == 1
        else ""
    )

    print(f"\n  State: v{version}  |  Updated: {age}{stale_badge}")
    print(f"  Shutdown: {shutdown}  |  Session: {_age_sec(session_start)} started")
    print(f"  Rebal: #{rebal_count} (last: {last_rebal})  |  Pending buys: {len(pending)}")
    if sell_status:
        print(f"  Sell status: {sell_status}")
    if recon_unreliable:
        print(f"  ** RECON_UNRELIABLE: True — new entries may be blocked **")

    print(f"\n  {'Cash':>14s}: {_fmt_krw(cash):>18s} KRW")
    print(f"  {'Equity (est)':>14s}: {_fmt_krw(equity_est):>18s} KRW")
    print(f"  {'Peak':>14s}: {_fmt_krw(peak):>18s} KRW")
    print(f"  {'DD from peak':>14s}: {_pct(dd_from_peak):>18s}")
    print(f"  {'Positions':>14s}: {len(positions):>18d}")

    # ── Equity log (recent) ───────────────────────────────────────────
    eq_rows = _read_csv_tail(report_dir / "equity_log.csv", 3)
    if eq_rows:
        print(f"\n  Recent Equity Log:")
        print(f"  {'Date':<12s} {'Equity':>14s} {'Daily':>8s} {'MonDD':>8s} {'Risk':<18s} {'Rebal':>5s} {'Recon':>5s}")
        print(f"  {'-'*12} {'-'*14} {'-'*8} {'-'*8} {'-'*18} {'-'*5} {'-'*5}")
        for r in eq_rows:
            try:
                date = r.get("date", "").strip()
                eq = _fmt_krw(float(r.get("equity", "0").strip() or "0"))
                daily = f"{float(r.get('daily_pnl_pct', '0').strip() or '0') * 100:+.1f}%"
                mon = f"{float(r.get('monthly_dd_pct', '0').strip() or '0') * 100:+.1f}%"
                risk = r.get("risk_mode", "").strip()
                rebal = r.get("rebalance_executed", "").strip()
                recon = r.get("reconcile_corrections", "").strip()
                print(f"  {date:<12s} {eq:>14s} {daily:>8s} {mon:>8s} {risk:<18s} {rebal:>5s} {recon:>5s}")
            except Exception:
                continue

    # ── Position Table ────────────────────────────────────────────────
    if positions:
        print(f"\n  Positions ({len(positions)}):")
        print(f"  {'Code':<8s} {'Qty':>6s} {'AvgPx':>10s} {'HWM':>10s} "
              f"{'Trail':>10s} {'Entry':>12s} {'Days':>5s} {'DD%':>7s}")
        print(f"  {'-'*8} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*12} {'-'*5} {'-'*7}")

        sorted_pos = sorted(positions.values(), key=lambda p: p.get("code", ""))
        for p in sorted_pos:
            code = p.get("code", "?")
            qty = p.get("quantity", 0)
            avg = p.get("avg_price", 0)
            hwm = p.get("high_watermark", 0)
            trail = p.get("trail_stop_price", 0)
            entry = p.get("entry_date", "")
            skip = p.get("trail_skip_days", 0)

            # Hold days
            try:
                hold = (datetime.now().date() - datetime.strptime(entry, "%Y-%m-%d").date()).days
            except Exception:
                hold = 0

            # DD from HWM
            dd = (trail / hwm - 1) if hwm > 0 else 0

            skip_mark = "*" if skip > 0 else " "
            print(f"  {code:<8s} {qty:>6d} {_fmt_krw(avg):>10s} {_fmt_krw(hwm):>10s} "
                  f"{_fmt_krw(trail):>10s} {entry:>12s} {hold:>5d} {dd*100:>+6.1f}%{skip_mark}")

        if any(p.get("trail_skip_days", 0) > 0 for p in positions.values()):
            print(f"  * = trail_skip_days > 0 (stale price, trail stop skipped)")

    # ── Pending Buys ──────────────────────────────────────────────────
    if pending:
        print(f"\n  Pending Buys ({len(pending)}):")
        for pb in pending:
            if isinstance(pb, dict):
                print(f"    {pb.get('code', '?')} qty={pb.get('qty', '?')} "
                      f"price={pb.get('price', '?')}")
            else:
                print(f"    {pb}")

    # ── Recent Warnings/Errors ────────────────────────────────────────
    log_lines = _read_log_tail(mode, 15)
    if log_lines:
        print(f"\n  Recent Alerts (last 15):")
        for ln in log_lines:
            # Trim timestamp for compact display
            short = ln[20:] if len(ln) > 20 else ln
            # Color: CRITICAL=red, ERROR=yellow, WARNING=dim
            if "[CRITICAL]" in ln:
                print(f"  \033[91m{short[:70]}\033[0m")
            elif "[ERROR]" in ln:
                print(f"  \033[93m{short[:70]}\033[0m")
            else:
                print(f"  \033[90m{short[:70]}\033[0m")

    print(f"\n{'─' * 72}")
    print(f"  Press Ctrl+C to exit  |  Refresh: {REFRESH_SEC}s")


def main():
    parser = argparse.ArgumentParser(description="Gen4 CLI Monitor (read-only)")
    parser.add_argument("--mode", default="paper",
                        choices=["mock", "paper", "paper_test", "live"],
                        help="Trading mode to monitor (default: paper)")
    parser.add_argument("--once", action="store_true",
                        help="Single snapshot, no auto-refresh")
    parser.add_argument("--refresh", type=int, default=REFRESH_SEC,
                        help=f"Refresh interval in seconds (default: {REFRESH_SEC})")
    args = parser.parse_args()

    global REFRESH_SEC
    REFRESH_SEC = args.refresh

    if args.once:
        render(args.mode)
        return

    try:
        while True:
            render(args.mode)
            time.sleep(REFRESH_SEC)
    except KeyboardInterrupt:
        print("\n\n  Monitor stopped.\n")


if __name__ == "__main__":
    main()
