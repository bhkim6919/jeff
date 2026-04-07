"""Advisor Metrics - self-evaluation tracking."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from ..config import METRICS_DIR


METRICS_PATH = METRICS_DIR / "advisor_metrics.json"


def load_metrics() -> dict:
    """Load advisor metrics history."""
    if METRICS_PATH.exists():
        try:
            return json.loads(METRICS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "updated_at": "",
        "total_recommendations": 0,
        "approved": 0,
        "kept": 0,
        "reviewed": 0,
        "rolled_back": 0,
        "approval_rate": 0.0,
        "success_rate": 0.0,
        "recommendation_history": [],
    }


def save_metrics(metrics: dict):
    """Save advisor metrics."""
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    metrics["updated_at"] = datetime.now().isoformat()
    METRICS_PATH.write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8")


def record_recommendation(metrics: dict, rec_id: str, param: str) -> dict:
    """Record a new recommendation."""
    metrics["total_recommendations"] += 1
    metrics["recommendation_history"].append({
        "id": rec_id,
        "parameter": param,
        "timestamp": datetime.now().isoformat(),
        "status": "PENDING",
    })
    # Keep last 100
    metrics["recommendation_history"] = metrics["recommendation_history"][-100:]
    _update_rates(metrics)
    return metrics


def record_approval(metrics: dict, rec_id: str, approved: bool) -> dict:
    """Record approval decision."""
    if approved:
        metrics["approved"] += 1
    for h in metrics["recommendation_history"]:
        if h["id"] == rec_id:
            h["status"] = "APPROVED" if approved else "REJECTED"
            break
    _update_rates(metrics)
    return metrics


def record_verdict(metrics: dict, rec_id: str, verdict: str) -> dict:
    """Record post-evaluation verdict (KEEP/REVIEW/ROLLBACK)."""
    if verdict == "KEEP":
        metrics["kept"] += 1
    elif verdict == "REVIEW":
        metrics["reviewed"] += 1
    elif verdict == "ROLLBACK":
        metrics["rolled_back"] += 1

    for h in metrics["recommendation_history"]:
        if h["id"] == rec_id:
            h["status"] = verdict
            break
    _update_rates(metrics)
    return metrics


def _update_rates(metrics: dict):
    total = metrics["total_recommendations"]
    approved = metrics["approved"]
    kept = metrics["kept"]

    metrics["approval_rate"] = approved / total if total > 0 else 0
    metrics["success_rate"] = kept / approved if approved > 0 else 0


def should_suspend_params(metrics: dict) -> bool:
    """Check if param recommendations should be suspended based on track record."""
    if metrics["approved"] < 5:
        return False  # Not enough history
    if metrics["success_rate"] < 0.50:
        return True
    if metrics["approved"] > 0:
        rollback_rate = metrics["rolled_back"] / metrics["approved"]
        if rollback_rate > 0.30:
            return True
    return False
