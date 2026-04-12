"""Snapshot validation — reject invalid data before analysis."""
from __future__ import annotations

from datetime import datetime

from ..ingestion.schema import DailySnapshot


def validate_snapshot(snapshot: DailySnapshot) -> tuple[bool, list[str]]:
    """Validate snapshot integrity. Analysis is blocked on failure.

    Returns:
        (is_valid, list_of_errors)
    """
    errors = []

    # 1. Reference point must be EOD
    if snapshot.reference_point != "EOD":
        errors.append(f"REJECT: reference_point={snapshot.reference_point}, must be EOD")

    # 2. All timestamps must be <= cutoff (no future data)
    try:
        cutoff = datetime.fromisoformat(snapshot.data_cutoff_time)
        for source, ts in snapshot.timestamps.items():
            if not ts:
                continue
            try:
                source_dt = datetime.fromisoformat(ts)
                if source_dt > cutoff:
                    errors.append(
                        f"REJECT: {source} timestamp {ts} > cutoff {cutoff}")
            except (ValueError, TypeError):
                pass  # Non-ISO timestamps (e.g., date-only) — skip
    except (ValueError, TypeError):
        errors.append(f"REJECT: invalid data_cutoff_time={snapshot.data_cutoff_time}")

    # 3. trading_day format check
    if len(snapshot.trading_day) != 8 or not snapshot.trading_day.isdigit():
        errors.append(f"REJECT: invalid trading_day={snapshot.trading_day}")

    # 4. Essential data check
    if not snapshot.equity and not snapshot.positions:
        errors.append("REJECT: both equity and positions are empty")

    # 5. Mode consistency in trades
    if snapshot.trades:
        for t in snapshot.trades:
            trade_mode = t.get("mode", "").strip().lower()
            if trade_mode and trade_mode != snapshot.meta.mode:
                errors.append(
                    f"REJECT: trade mode={trade_mode} != "
                    f"snapshot mode={snapshot.meta.mode}")
                break  # One mismatch is enough

    # 6. Snapshot hash must be set
    if not snapshot.snapshot_hash:
        errors.append("WARN: snapshot_hash is empty (replay verification disabled)")

    return (len([e for e in errors if e.startswith("REJECT")]) == 0, errors)
