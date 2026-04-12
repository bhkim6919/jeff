"""Phase 4: Intraday Risk Analyzer - EOD analysis of intraday data.

Reads intraday_summary_{date}.json and generates risk alerts with
debug hints for operational decision-making.

All alerts are advisory only - no engine modification.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


def analyze_intraday_risk(
    summary_path: Path,
    positions: dict | None = None,
) -> list[dict]:
    """Analyze intraday summary and generate risk alerts.

    Args:
        summary_path: Path to intraday_summary_{date}.json
        positions: Optional portfolio positions dict for trail context

    Returns:
        List of alert dicts with priority, category, message, detail, debug_hint
    """
    if not summary_path.exists():
        return []

    try:
        data = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return [_alert("MEDIUM", "Intraday summary parse error",
                       f"Failed to read {summary_path.name}",
                       "Check file encoding or format corruption")]

    alerts = []
    per_stock = data.get("per_stock", [])

    # 1. Flash Drop detection
    alerts.extend(_check_flash_drops(per_stock))

    # 2. Volume Anomaly
    alerts.extend(_check_volume_anomaly(data, per_stock))

    # 3. Cluster DD (systemic risk)
    alerts.extend(_check_cluster_dd(per_stock))

    # 4. Near Trail Intraday
    alerts.extend(_check_near_trail(per_stock, positions))

    # 5. Overall Risk Score
    alerts.extend(_check_risk_score(data))

    # 6. VWAP divergence
    alerts.extend(_check_vwap_divergence(per_stock))

    return alerts


def _check_flash_drops(per_stock: list[dict]) -> list[dict]:
    """Detect stocks with >3% 5-minute drop."""
    alerts = []
    for s in per_stock:
        code = s.get("code", "?")
        drop = s.get("max_5m_drop_pct", 0)
        drop_time = s.get("max_5m_drop_time", "?")
        dd = s.get("max_intraday_dd_pct", 0)
        vol_3x = s.get("volume_spike_3x", 0)

        if drop <= -3.0:
            hint = f"{code} 5min drop {drop:.1f}% at {drop_time}"
            if vol_3x > 10:
                hint += " + volume spike -> institutional selling likely"
            else:
                hint += " -> check for news/disclosure event"

            alerts.append(_alert(
                "HIGH",
                f"Flash drop: {code} {drop:.1f}% in 5min",
                f"at {drop_time}, intraday DD {dd:.1f}%",
                hint,
            ))
    return alerts


def _check_volume_anomaly(data: dict, per_stock: list[dict]) -> list[dict]:
    """Detect abnormal volume patterns."""
    alerts = []
    total_3x = data.get("total_volume_spikes_3x", 0)

    if total_3x > 50:
        alerts.append(_alert(
            "MEDIUM",
            f"Volume anomaly: {total_3x} 3x-spikes across portfolio",
            "Abnormally high volume activity detected",
            "Market-wide event or sector rotation possible. "
            "Check news for policy/earnings surprises",
        ))

    for s in per_stock:
        code = s.get("code", "?")
        max_ratio = s.get("max_volume_ratio", 0)
        vol_3x = s.get("volume_spike_3x", 0)

        if max_ratio > 20 and vol_3x > 15:
            alerts.append(_alert(
                "MEDIUM",
                f"Volume anomaly: {code} max ratio {max_ratio:.0f}x",
                f"{vol_3x} bars with 3x+ volume",
                f"{code}: extreme volume concentration. "
                "Possible block trade or forced liquidation",
            ))
    return alerts


def _check_cluster_dd(per_stock: list[dict]) -> list[dict]:
    """Detect simultaneous drawdown across multiple stocks."""
    alerts = []
    dd_stocks = [s for s in per_stock if s.get("max_intraday_dd_pct", 0) <= -3.0]

    if len(dd_stocks) >= 3:
        codes = [s["code"] for s in dd_stocks[:5]]
        avg_dd = sum(s["max_intraday_dd_pct"] for s in dd_stocks) / len(dd_stocks)

        alerts.append(_alert(
            "HIGH",
            f"Cluster DD: {len(dd_stocks)} stocks with >3% intraday DD",
            f"avg DD {avg_dd:.1f}%, stocks: {', '.join(codes)}",
            "Systemic risk - not individual stock issue. "
            "Check KOSPI index, sector ETFs, macro news. "
            "Trail stop may trigger multiple exits simultaneously",
        ))
    return alerts


def _check_near_trail(per_stock: list[dict],
                      positions: dict | None) -> list[dict]:
    """Detect stocks near trail stop with intraday weakness."""
    alerts = []
    for s in per_stock:
        code = s.get("code", "?")
        near = s.get("near_trail_stop", False)
        dd = s.get("max_intraday_dd_pct", 0)
        close_vs_vwap = s.get("close_vs_vwap_pct", 0)

        if near:
            hint = f"{code} near trail stop"
            if close_vs_vwap < -1.0:
                hint += f", closing below VWAP ({close_vs_vwap:.1f}%)"
                hint += " -> high probability of trail stop trigger tomorrow"
            else:
                hint += f", but recovered above VWAP ({close_vs_vwap:+.1f}%)"
                hint += " -> monitor closely, may hold"

            alerts.append(_alert(
                "HIGH",
                f"Near trail stop: {code}",
                f"intraday DD {dd:.1f}%, close vs VWAP {close_vs_vwap:+.1f}%",
                hint,
            ))

        elif dd <= -4.0 and not near:
            alerts.append(_alert(
                "MEDIUM",
                f"Deep intraday DD: {code} {dd:.1f}%",
                f"close vs VWAP {close_vs_vwap:+.1f}%",
                f"{code} large intraday swing but not near trail. "
                "Check if HWM updated today - may widen gap",
            ))
    return alerts


def _check_risk_score(data: dict) -> list[dict]:
    """Evaluate overall portfolio risk score."""
    alerts = []
    score = data.get("risk_score", 0)

    if score >= 85:
        alerts.append(_alert(
            "HIGH",
            f"Portfolio risk score: {score:.0f}/100",
            "Multiple risk factors elevated simultaneously",
            "Risk score >85 indicates convergence of volume, DD, and trail risks. "
            "Consider whether market conditions justify staying fully invested",
        ))
    elif score >= 70:
        alerts.append(_alert(
            "MEDIUM",
            f"Elevated risk score: {score:.0f}/100",
            "Above normal risk threshold",
            "Monitor closely. Individual stock risks may compound. "
            "Check if DD guard thresholds are near activation",
        ))
    return alerts


def _check_vwap_divergence(per_stock: list[dict]) -> list[dict]:
    """Detect stocks closing significantly below VWAP."""
    alerts = []
    below_vwap = [s for s in per_stock if s.get("close_vs_vwap_pct", 0) < -2.0]

    if len(below_vwap) >= 3:
        codes = [f"{s['code']}({s['close_vs_vwap_pct']:.1f}%)"
                 for s in below_vwap[:4]]
        alerts.append(_alert(
            "MEDIUM",
            f"{len(below_vwap)} stocks closing below VWAP",
            f"Stocks: {', '.join(codes)}",
            "Multiple positions closing below VWAP suggests afternoon weakness. "
            "Tomorrow's open may gap down. Review overnight risk exposure",
        ))
    return alerts


def _alert(priority: str, message: str, detail: str,
           debug_hint: str) -> dict:
    return {
        "priority": priority,
        "category": "INTRADAY",
        "message": message,
        "detail": detail,
        "debug_hint": debug_hint,
    }
