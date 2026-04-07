"""Drift Guard - monitor advisor behavior for instability and over-reliance."""
from __future__ import annotations

from collections import defaultdict


class AdvisorDriftGuard:
    """Watch for excessive/oscillating recommendations."""

    def check(self, rec_history: list[dict],
              approval_history: list[dict]) -> list[str]:
        """Check for drift patterns. Returns list of warning strings."""
        warnings = []

        # ── 1. Too many param recs in 20 days ──
        recent_params = [r for r in rec_history[-20:]
                         if r.get("category") == "PARAM"]
        if len(recent_params) > 3:
            warnings.append(
                f"DRIFT: {len(recent_params)} param recommendations in 20 days "
                f"- strategy may be unstable")

        # ── 2. Same param recommended multiple times ──
        param_names = [r.get("parameter", "") for r in recent_params]
        for p in set(param_names):
            count = param_names.count(p)
            if count >= 2:
                warnings.append(
                    f"DRIFT: '{p}' recommended {count}x in 20 days "
                    f"- low confidence signal")

        # ── 3. Rollback frequency ──
        recent_approvals = approval_history[-10:] if approval_history else []
        rollbacks = [a for a in recent_approvals if a.get("rolled_back")]
        if len(rollbacks) >= 2:
            warnings.append(
                f"DRIFT: {len(rollbacks)} rollbacks in recent history "
                f"- param recommendations SUSPENDED")

        # ── 4. Over-approval (>90% approval rate) ──
        if len(recent_approvals) >= 10:
            approved = sum(1 for a in recent_approvals if a.get("approved"))
            rate = approved / len(recent_approvals)
            if rate >= 0.90:
                warnings.append(
                    f"CAUTION: approval rate {rate:.0%} "
                    f"- ensure manual review is meaningful")

        # ── 5. Direction flip ──
        flip_warning = self._check_direction_flip(rec_history)
        if flip_warning:
            warnings.append(flip_warning)

        return warnings

    def _check_direction_flip(self, rec_history: list[dict]) -> str | None:
        """Detect parameter value oscillation (e.g., 0.12->0.10->0.13->0.09)."""
        param_values = defaultdict(list)
        for r in rec_history:
            if r.get("category") != "PARAM":
                continue
            param = r.get("parameter", "")
            value = r.get("suggested_value")
            if param and value is not None:
                param_values[param].append(value)

        for param, values in param_values.items():
            if len(values) < 3:
                continue
            flips = sum(
                1 for i in range(2, len(values))
                if (values[i] - values[i-1]) * (values[i-1] - values[i-2]) < 0
            )
            if flips >= 2:
                return (f"BLOCK: '{param}' direction flipped {flips}x "
                        f"({values[-3:]}) - learning failure detected")

        return None

    def should_suspend_params(self, warnings: list[str]) -> bool:
        """Check if param recommendations should be suspended."""
        return any("SUSPENDED" in w or "BLOCK:" in w for w in warnings)
