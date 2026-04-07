"""Regime Alerter - market regime transitions, volatility expansion, breadth weakness."""
from __future__ import annotations

from ..ingestion.schema import DailySnapshot, SnapshotWindow


def check_regime_alerts(snapshot: DailySnapshot,
                        window: SnapshotWindow | None = None) -> list[dict]:
    """Generate regime-related alerts.

    Returns list of {priority, message, category, detail}.
    """
    alerts = []

    # ── 1. KOSPI below MA200 (from equity_log) ──
    kospi = snapshot.kospi_close
    regime = snapshot.regime

    if regime and "BEAR" in regime.upper():
        alerts.append({
            "priority": "HIGH",
            "category": "ALERT",
            "message": f"Market regime BEAR (KOSPI={kospi:,.0f})",
            "detail": "KOSPI below MA200 - defensive posture recommended",
        })
    elif regime and "SIDE" in regime.upper():
        alerts.append({
            "priority": "MEDIUM",
            "category": "ALERT",
            "message": f"Market regime SIDEWAYS (KOSPI={kospi:,.0f})",
            "detail": "Neutral market - monitor for direction",
        })

    # ── 2. Volatility expansion (from window) ──
    if window and window.valid_count >= 5:
        daily_pnls = []
        for s, v in zip(window.snapshots, window.valid_mask):
            if v and s.equity:
                pnl = _sf(s.equity.get("daily_pnl_pct", "0"))
                daily_pnls.append(pnl)

        if len(daily_pnls) >= 5:
            import statistics
            recent_vol = statistics.stdev(daily_pnls[-5:]) if len(daily_pnls[-5:]) > 1 else 0
            full_vol = statistics.stdev(daily_pnls) if len(daily_pnls) > 1 else 0

            if full_vol > 0 and recent_vol > full_vol * 1.5:
                alerts.append({
                    "priority": "MEDIUM",
                    "category": "ALERT",
                    "message": (f"Volatility expansion: recent 5d vol "
                                f"{recent_vol:.3f} vs avg {full_vol:.3f} "
                                f"({recent_vol/full_vol:.1f}x)"),
                    "detail": "Portfolio volatility expanding - watch DD levels",
                })

    # ── 3. Consecutive loss days ──
    if window and window.valid_count >= 3:
        recent_pnls = []
        for s, v in zip(reversed(window.snapshots), reversed(window.valid_mask)):
            if not v:
                continue
            if s.equity:
                recent_pnls.append(_sf(s.equity.get("daily_pnl_pct", "0")))
            if len(recent_pnls) >= 5:
                break

        consecutive_loss = 0
        for p in recent_pnls:
            if p < 0:
                consecutive_loss += 1
            else:
                break

        if consecutive_loss >= 4:
            alerts.append({
                "priority": "HIGH",
                "category": "ALERT",
                "message": f"Consecutive loss: {consecutive_loss} days",
                "detail": "Extended losing streak - DD guard may activate",
            })
        elif consecutive_loss >= 3:
            alerts.append({
                "priority": "MEDIUM",
                "category": "ALERT",
                "message": f"Consecutive loss: {consecutive_loss} days",
                "detail": "Monitor DD levels closely",
            })

    return alerts


def _sf(v) -> float:
    try:
        return float(str(v).strip() or "0")
    except (ValueError, TypeError):
        return 0.0
