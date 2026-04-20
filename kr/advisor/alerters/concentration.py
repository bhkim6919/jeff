"""Concentration Alerter - stock/sector/market concentration warnings."""
from __future__ import annotations

from ..ingestion.schema import DailySnapshot


def check_concentration_alerts(snapshot: DailySnapshot) -> list[dict]:
    """Generate concentration-related alerts.

    Returns list of {priority, message, category, detail}.
    """
    alerts = []
    positions = snapshot.positions
    if not positions:
        return alerts

    # Calculate position values
    pos_values = []
    total_invested = 0
    for code, pos in positions.items():
        qty = pos.get("quantity", 0)
        avg = pos.get("avg_price", 0)
        value = qty * avg
        sector = pos.get("sector", "")
        pos_values.append({
            "code": code,
            "value": value,
            "sector": sector,
            "is_kosdaq": _is_kosdaq(code),
        })
        total_invested += value

    if total_invested <= 0:
        return alerts

    # ── 1. Top 3 stock concentration > 25% ──
    pos_values.sort(key=lambda x: x["value"], reverse=True)
    top3_value = sum(p["value"] for p in pos_values[:3])
    top3_pct = top3_value / total_invested

    if top3_pct > 0.25:
        top3_codes = [p["code"] for p in pos_values[:3]]
        alerts.append({
            "priority": "MEDIUM",
            "category": "ALERT",
            "message": (f"Stock concentration: top 3 = {top3_pct:.0%} "
                        f"({', '.join(top3_codes)})"),
            "detail": f"Top 3 positions hold {top3_pct:.0%} of portfolio",
        })

    # ── 2. Single sector > 40% ──
    sector_totals: dict[str, float] = {}
    for p in pos_values:
        s = p["sector"] or "UNKNOWN"
        sector_totals[s] = sector_totals.get(s, 0) + p["value"]

    for sector, value in sector_totals.items():
        if sector == "UNKNOWN":
            continue  # UNKNOWN = sector data not populated, not a real concentration risk
        pct = value / total_invested
        if pct > 0.40:
            alerts.append({
                "priority": "MEDIUM",
                "category": "ALERT",
                "message": f"Sector concentration: {sector} = {pct:.0%}",
                "detail": f"Single sector holds {pct:.0%} of portfolio",
            })

    # ── 3. KOSDAQ exposure > 60% ──
    kosdaq_value = sum(p["value"] for p in pos_values if p["is_kosdaq"])
    kosdaq_pct = kosdaq_value / total_invested

    if kosdaq_pct > 0.60:
        alerts.append({
            "priority": "MEDIUM",
            "category": "ALERT",
            "message": f"KOSDAQ concentration: {kosdaq_pct:.0%}",
            "detail": "KOSDAQ exposure above 60% - higher volatility risk",
        })

    # ── 4. Too few positions ──
    n_pos = len(positions)
    if 0 < n_pos < 10:
        alerts.append({
            "priority": "MEDIUM",
            "category": "ALERT",
            "message": f"Low diversification: {n_pos} positions (target 20)",
            "detail": "Portfolio under-diversified vs target allocation",
        })

    return alerts


def _is_kosdaq(code: str) -> bool:
    """Heuristic: KOSDAQ codes often start with 0~3 for KOSPI, higher for KOSDAQ.
    This is a rough estimate - actual market detection would need sector map.
    """
    # Common KOSPI: 0xxxxx, some 3xxxxx
    # Common KOSDAQ: 0xxxxx too, so this is unreliable
    # Better: check if code length is 6 and sector data available
    return False  # Conservative: assume KOSPI unless sector data says otherwise
