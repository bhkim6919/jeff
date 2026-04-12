"""Approval pipeline - recommendation -> validation -> manual approval -> override."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from ..config import ADVISOR_DIR, MAX_ACTIVE_OVERRIDES


APPROVED_DIR = ADVISOR_DIR / "approved"
OVERRIDE_PATH = APPROVED_DIR / "config_override.json"


class OverrideConflictError(Exception):
    pass


class OverrideLimitError(Exception):
    pass


def load_overrides() -> dict:
    """Load current config overrides."""
    if not OVERRIDE_PATH.exists():
        return {"version": 1, "overrides": []}
    try:
        data = json.loads(OVERRIDE_PATH.read_text(encoding="utf-8"))
        return data
    except Exception:
        return {"version": 1, "overrides": []}


def save_overrides(data: dict):
    """Save config overrides (advisor-only file)."""
    APPROVED_DIR.mkdir(parents=True, exist_ok=True)
    OVERRIDE_PATH.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8")


def create_approval_template(recommendation: dict) -> dict:
    """Create an approval template from a recommendation."""
    return {
        "recommendation_id": recommendation.get("id", ""),
        "parameter": recommendation.get("parameter", ""),
        "current_value": recommendation.get("current_value"),
        "suggested_value": recommendation.get("suggested_value"),
        "rationale": recommendation.get("rationale", ""),
        "confidence": recommendation.get("confidence", "LOW"),

        "validation_result": {
            "oos_cagr": None,
            "oos_mdd": None,
            "oos_sharpe": None,
            "exit_reason_shift": None,
            "trade_count_delta": None,
            "note": "Fill after running OOS backtest",
        },

        "approval": {
            "approved": False,
            "reviewer": "",
            "reviewed_at": "",
            "applied_from": "",
            "rollback_condition": {
                "max_dd_increase": 0.03,
                "cagr_drop_threshold": 0.02,
                "review_after_days": 21,
            },
        },
    }


def apply_approved_override(approval: dict) -> dict:
    """Add an approved override to config_override.json.

    Checks for conflicts and limits before applying.
    """
    if not approval.get("approval", {}).get("approved"):
        raise ValueError("Approval not granted - cannot apply")

    param = approval.get("parameter", "")
    value = approval.get("suggested_value")
    if not param or value is None:
        raise ValueError(f"Missing parameter or value: {param}={value}")

    overrides = load_overrides()
    active = overrides.get("overrides", [])

    # Check limit
    if len(active) >= MAX_ACTIVE_OVERRIDES:
        raise OverrideLimitError(
            f"Active overrides ({len(active)}) >= limit ({MAX_ACTIVE_OVERRIDES}) "
            f"- resolve existing before adding new")

    # Check conflict
    for existing in active:
        if existing.get("parameter") == param:
            raise OverrideConflictError(
                f"Parameter '{param}' already has an active override "
                f"(id={existing.get('recommendation_id')})")

    # Add
    active.append({
        "recommendation_id": approval.get("recommendation_id", ""),
        "parameter": param,
        "value": value,
        "approved_at": approval["approval"].get("reviewed_at",
                                                 datetime.now().isoformat()),
        "applied_from": approval["approval"].get("applied_from", ""),
        "rollback_by": "",
    })

    overrides["overrides"] = active
    overrides["version"] = overrides.get("version", 0) + 1
    save_overrides(overrides)

    return overrides


def merge_override(base_config: dict, overrides: list) -> dict:
    """Merge overrides into base config. Conflict -> error."""
    effective = dict(base_config)
    modified = set()

    for ovr in overrides:
        key = ovr.get("parameter", "")
        if not key:
            continue
        if key in modified:
            raise OverrideConflictError(
                f"Parameter '{key}' has multiple overrides")
        effective[key] = ovr.get("value")
        modified.add(key)

    return effective
