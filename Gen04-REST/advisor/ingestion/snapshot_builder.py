"""Build DailySnapshot from Gen4 engine files (read-only)."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from .schema import DailySnapshot, DataMeta
from .log_parser import find_log_file, parse_log_file, extract_operational_flags
from ..config import (
    STATE_DIR, REPORT_DIR, REPORT_DIR_TEST, SIGNALS_DIR, SIGNALS_DIR_TEST,
    LOG_DIR, OPERATIONAL_TAGS,
)


def _read_json(path: Path) -> dict | None:
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


def _read_csv_rows(path: Path, date_filter: str = "") -> list[dict]:
    """Read CSV, optionally filter by date column."""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if date_filter:
            rows = [r for r in rows
                    if r.get("date", "").strip().startswith(date_filter)]
        return rows
    except Exception:
        return []


def _compute_hash(snapshot_data: dict) -> str:
    serialized = json.dumps(snapshot_data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def build_daily_snapshot(date: str, mode: str = "paper") -> DailySnapshot:
    """Build a DailySnapshot for a given trading day.

    Args:
        date: "20260401" format.
        mode: "paper" | "paper_test" | "live" | "mock"

    Returns:
        DailySnapshot with all available data populated.
    """
    # Date formatting
    date_dash = f"{date[:4]}-{date[4:6]}-{date[6:]}"  # "2026-04-01"
    now = datetime.now().isoformat()

    # Determine paths
    report_dir = REPORT_DIR_TEST if mode == "paper_test" else REPORT_DIR
    signals_dir = SIGNALS_DIR_TEST if mode == "paper_test" else SIGNALS_DIR

    # ── State files ──
    pf_path = STATE_DIR / f"portfolio_state_{mode}.json"
    rt_path = STATE_DIR / f"runtime_state_{mode}.json"
    pf = _read_json(pf_path)
    rt = _read_json(rt_path)

    # ── CSV reports ──
    equity_rows = _read_csv_rows(report_dir / "equity_log.csv", date_dash)
    equity = equity_rows[-1] if equity_rows else {}

    trades = _read_csv_rows(report_dir / "trades.csv", date_dash)
    closes = _read_csv_rows(report_dir / "close_log.csv", date_dash)
    reconcile = _read_csv_rows(report_dir / "reconcile_log.csv", date_dash)

    # ── Signal file ──
    target = None
    sig_path = signals_dir / f"target_portfolio_{date}.json"
    if sig_path.exists():
        target = _read_json(sig_path)

    # ── Config snapshot ──
    config_snapshot = _load_config_snapshot()

    # ── Log events ──
    log_file = find_log_file(LOG_DIR, mode, date)
    log_events = parse_log_file(log_file, date_dash) if log_file else []
    operational_flags = extract_operational_flags(log_events, OPERATIONAL_TAGS)

    # ── Positions ──
    positions = {}
    if pf:
        positions = pf.get("positions", {})

    # ── External (from equity log) ──
    kospi_close = 0.0
    regime = ""
    if equity:
        try:
            kospi_close = float(equity.get("kospi_close", "0").strip() or "0")
        except (ValueError, TypeError):
            pass
        regime = equity.get("regime", "").strip()

    # ── Timestamps per source ──
    timestamps = {}
    if pf:
        timestamps["portfolio_state"] = pf.get("timestamp", "")
    if rt:
        timestamps["runtime_state"] = rt.get("timestamp", "")
    if equity:
        timestamps["equity_log"] = equity.get("date", "")
    if trades:
        timestamps["trades"] = trades[-1].get("date", "")

    # ── Strategy version ──
    strategy_version = config_snapshot.get("STRATEGY_VERSION", "4.0")

    # ── Build meta ──
    meta = DataMeta(
        source="engine",
        mode=mode,
        strategy_version=str(strategy_version),
        is_operational=bool(operational_flags),
        timestamp=now,
    )

    # ── Build snapshot ──
    snapshot = DailySnapshot(
        trading_day=date,
        data_cutoff_time=now,
        reference_point="EOD",
        timestamps=timestamps,
        meta=meta,
        config_snapshot=config_snapshot,
        target=target,
        equity=equity,
        trades=trades,
        closes=closes,
        positions=positions,
        reconcile=reconcile,
        log_events=log_events,
        operational_flags=operational_flags,
        kospi_close=kospi_close,
        regime=regime,
    )

    # ── Hash ──
    snapshot.snapshot_hash = _compute_hash({
        "trading_day": date,
        "equity": equity,
        "trades": trades,
        "closes": closes,
        "config": config_snapshot,
    })

    return snapshot


def _load_config_snapshot() -> dict:
    """Load Gen4 config parameters as a dict (read-only)."""
    try:
        import importlib.util
        config_path = STATE_DIR.parent / "config.py"
        spec = importlib.util.spec_from_file_location("gen4_config", config_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        cls = getattr(mod, "Gen4Config", None)
        if cls is None:
            return {}

        # Extract dataclass fields
        result = {}
        for f in cls.__dataclass_fields__:
            result[f] = getattr(cls, f, None)
        return result
    except Exception:
        return {}
