"""Data contamination filters — operational exclusion + mode enforcement."""
from __future__ import annotations

from .schema import DailySnapshot, SnapshotWindow
from ..config import MIN_WINDOW_COVERAGE


class OperationalDataError(Exception):
    """Raised when snapshot contains operational flags (exclude from analysis)."""
    pass


class DataMixError(Exception):
    """Raised when data from different modes is mixed."""
    pass


class InsufficientDataError(Exception):
    """Raised when window coverage is below threshold."""
    pass


def filter_for_analysis(snapshot: DailySnapshot) -> DailySnapshot:
    """Reject snapshots with operational flags from strategy analysis.

    Operational data (RECON_UNRELIABLE, DIRTY_EXIT, etc.) is valid for
    alerters but must NOT be used for PnL/Exit/MDD analysis.
    """
    if snapshot.operational_flags:
        raise OperationalDataError(
            f"Snapshot {snapshot.trading_day} has operational flags: "
            f"{snapshot.operational_flags} — excluded from strategy analysis")
    return snapshot


def strict_mode_filter(snapshots: list[DailySnapshot],
                       target_mode: str) -> list[DailySnapshot]:
    """Reject if any snapshot has a different mode."""
    for s in snapshots:
        if s.meta.mode != target_mode:
            raise DataMixError(
                f"Mode mismatch: expected={target_mode}, "
                f"got={s.meta.mode} on {s.trading_day}")
    return snapshots


def build_window(all_snapshots: list[DailySnapshot],
                 end_date: str,
                 window: int = 20) -> SnapshotWindow:
    """Build N-day analysis window with operational gap handling.

    Args:
        all_snapshots: All available snapshots, sorted by trading_day.
        end_date: Last date to include ("20260401").
        window: Requested window size in trading days.

    Returns:
        SnapshotWindow with valid_mask and coverage check.

    Raises:
        InsufficientDataError if coverage < MIN_WINDOW_COVERAGE.
    """
    # Filter to dates <= end_date, take last N
    eligible = [s for s in all_snapshots if s.trading_day <= end_date]
    recent = eligible[-window:] if len(eligible) >= window else eligible

    if not recent:
        raise InsufficientDataError("No snapshots available")

    valid_mask = [not bool(s.operational_flags) for s in recent]
    valid_count = sum(valid_mask)
    actual_window = max(len(recent), 1)
    coverage = valid_count / actual_window

    if coverage < MIN_WINDOW_COVERAGE:
        raise InsufficientDataError(
            f"Window coverage {coverage:.0%} < {MIN_WINDOW_COVERAGE:.0%} "
            f"({valid_count}/{actual_window} valid days)")

    return SnapshotWindow(
        snapshots=recent,
        start_date=recent[0].trading_day,
        end_date=end_date,
        window_size=actual_window,
        valid_mask=valid_mask,
        valid_count=valid_count,
        coverage_ratio=coverage,
    )
