"""Operational Alerter - engine health, data quality, RECON, stale detection."""
from __future__ import annotations

from datetime import datetime

from ..ingestion.schema import DailySnapshot


def _is_trading_day() -> bool:
    """Return True on KST weekdays (Mon–Fri). Does not account for public holidays."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Seoul")).weekday() < 5
    except Exception:
        return datetime.now().weekday() < 5


def check_operational_alerts(snapshot: DailySnapshot) -> list[dict]:
    """Generate operational alerts (8 types).

    These alerts use operational data intentionally (unlike analyzers
    which exclude it). This is the correct place for incident detection.

    Returns list of {priority, message, category, detail}.
    """
    alerts = []

    # ── 1. RECON unreliable ──
    if "RECON_UNRELIABLE" in snapshot.operational_flags:
        alerts.append({
            "priority": "HIGH",
            "category": "ALERT",
            "message": "RECON unreliable - new entries should be blocked",
            "detail": "Broker/state mismatch exceeded safety threshold",
        })

    if "RECON_SAFETY" in snapshot.operational_flags:
        recon_count = len(snapshot.reconcile)
        alerts.append({
            "priority": "HIGH",
            "category": "ALERT",
            "message": f"RECON safety triggered ({recon_count} corrections)",
            "detail": "Large number of reconciliation corrections detected",
        })

    # ── 2. Dirty exit detected ──
    if "DIRTY_EXIT" in snapshot.operational_flags:
        alerts.append({
            "priority": "HIGH",
            "category": "ALERT",
            "message": "Dirty exit detected - engine crashed previously",
            "detail": "Previous session ended abnormally - verify state integrity",
        })

    # ── 3. SAFE_MODE ──
    if "SAFE_MODE" in snapshot.operational_flags:
        alerts.append({
            "priority": "HIGH",
            "category": "ALERT",
            "message": "SAFE_MODE active - trading restricted",
            "detail": "Engine entered safe mode due to DD or operational issues",
        })

    # ── 4. Data staleness (trading days only) ──
    trading_day = _is_trading_day()
    pf_ts = snapshot.timestamps.get("portfolio_state", "")
    if pf_ts and trading_day:
        try:
            age_sec = (datetime.now() - datetime.fromisoformat(pf_ts)).total_seconds()
            if age_sec > 7200:  # 2 hours
                alerts.append({
                    "priority": "HIGH",
                    "category": "ALERT",
                    "message": f"Data latency: portfolio state {age_sec/3600:.1f}h old",
                    "detail": "Engine may not be running or state save is failing",
                })
            elif age_sec > 300:  # 5 minutes (during market hours)
                now = datetime.now()
                if 9 <= now.hour <= 15:
                    alerts.append({
                        "priority": "MEDIUM",
                        "category": "ALERT",
                        "message": f"Data latency: portfolio state {age_sec:.0f}s old",
                        "detail": "State update delayed during market hours",
                    })
        except (ValueError, TypeError):
            pass

    # ── 5. Snapshot incomplete (positions always required; equity_log/config only on trading days) ──
    missing = []
    if not snapshot.equity and trading_day:
        missing.append("equity_log")
    if not snapshot.positions:
        missing.append("positions")
    if not snapshot.config_snapshot and trading_day:
        missing.append("config")

    if missing:
        alerts.append({
            "priority": "HIGH" if "positions" in missing else "MEDIUM",
            "category": "ALERT",
            "message": f"Snapshot incomplete: missing {', '.join(missing)}",
            "detail": "Required data files not found or empty",
        })

    # ── 6. Timestamp gap between sources (trading days only) ──
    if trading_day:
        ts_values = {}
        for source, ts in snapshot.timestamps.items():
            if not ts:
                continue
            try:
                ts_values[source] = datetime.fromisoformat(ts)
            except (ValueError, TypeError):
                pass

        if len(ts_values) >= 2:
            min_ts = min(ts_values.values())
            max_ts = max(ts_values.values())
            gap_sec = (max_ts - min_ts).total_seconds()

            if gap_sec > 1800:  # 30 minutes
                alerts.append({
                    "priority": "MEDIUM",
                    "category": "ALERT",
                    "message": f"Timestamp gap: {gap_sec/60:.0f}min between data sources",
                    "detail": (f"Earliest: {min(ts_values, key=ts_values.get)} "
                               f"Latest: {max(ts_values, key=ts_values.get)}"),
                })

    # ── 7. Ghost fill / pending external ──
    ghost_events = [e for e in snapshot.log_events if "GHOST_FILL" in e.tag]
    pending_ext = [e for e in snapshot.log_events if "PENDING_EXTERNAL" in e.tag]

    if pending_ext:
        alerts.append({
            "priority": "MEDIUM",
            "category": "ALERT",
            "message": f"Pending external orders: {len(pending_ext)} events",
            "detail": "Orders awaiting ghost fill settlement",
        })

    # ── 8. Engine shutdown status ──
    for e in snapshot.log_events:
        if e.tag == "DIRTY_EXIT_DETECTED":
            if not any(a["message"].startswith("Dirty exit") for a in alerts):
                alerts.append({
                    "priority": "HIGH",
                    "category": "ALERT",
                    "message": "Dirty exit detected in log",
                    "detail": e.message[:100],
                })
            break

    return alerts
