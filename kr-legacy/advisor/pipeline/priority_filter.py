"""Priority Filter - output limit, dedup, category diversity."""
from __future__ import annotations

from ..config import MAX_HIGH_RECOMMENDATIONS, MAX_MEDIUM_RECOMMENDATIONS


def filter_and_prioritize(alerts: list[dict]) -> list[dict]:
    """Apply dedup, priority limit, category diversity.

    Input: list of {priority, category, message, detail}
    Output: filtered list, HIGH <= 3, MEDIUM <= 5, LOW excluded.
    """
    # ── 1. Deduplicate ──
    alerts = _deduplicate(alerts)

    # ── 2. Separate by priority ──
    high = [a for a in alerts if a.get("priority") == "HIGH"]
    medium = [a for a in alerts if a.get("priority") == "MEDIUM"]

    # ── 3. Category diversity for HIGH ──
    high = _ensure_diversity(high, medium)

    # ── 4. Apply limits ──
    high = high[:MAX_HIGH_RECOMMENDATIONS]
    medium = medium[:MAX_MEDIUM_RECOMMENDATIONS]

    return high + medium


def _deduplicate(alerts: list[dict]) -> list[dict]:
    """Remove duplicate alerts (same category + similar message)."""
    seen = {}
    result = []
    for a in alerts:
        key = f"{a.get('category', '')}:{a.get('message', '')[:60]}"
        if key not in seen:
            seen[key] = True
            result.append(a)
    return result


def _ensure_diversity(high: list[dict], medium: list[dict]) -> list[dict]:
    """If all HIGH alerts are same category, promote one MEDIUM from different category."""
    if len(high) < 2:
        return high

    categories = set(a.get("category", "") for a in high)
    if len(categories) > 1:
        return high  # Already diverse

    # Find a MEDIUM from different category
    high_cat = next(iter(categories))
    for m in medium:
        if m.get("category", "") != high_cat:
            m["priority"] = "HIGH"
            high.append(m)
            medium.remove(m)
            break

    return high
