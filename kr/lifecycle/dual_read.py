"""
lifecycle/dual_read.py -- Phase 3 Dual-Read Contract
=====================================================
COM + REST 동시 조회 시 비교 규칙 + 우선순위 + 타이밍 윈도우 정의.

원칙:
  1. Observer-only: 이 모듈은 어떤 state도 수정하지 않는다
  2. Mismatch 시 COM 우선 (Phase 3 동안)
  3. REST source가 PARTIAL/DEGRADED면 REST 값 사용 금지
  4. 결과는 로그만 기록 (DUAL_READ_DIFF / DUAL_READ_MATCH)
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("gen4.dual_read")

# Timing window: |t_com - t_rest| 이내면 비교 eligible
TIMING_WINDOW_S = 2.0

# Price tolerance: 호가 단위 차이는 무시
PRICE_TOLERANCE_PCT = 0.005  # 0.5%


@dataclass
class DualReadResult:
    """Dual-read 비교 결과."""
    eligible: bool = True           # timing window 내 + source 정상
    source_used: str = "com"        # "com" | "rest" (Phase 3에서는 항상 "com")
    diffs: List[Dict] = field(default_factory=list)
    timing_delta_s: float = 0.0
    rest_rejected: bool = False     # REST source가 PARTIAL/DEGRADED
    rejection_reason: str = ""


def compare_holdings(
    com_summary: Dict,
    rest_summary: Dict,
) -> DualReadResult:
    """COM vs REST holdings 비교.

    Args:
        com_summary: COM 측 계좌조회 결과 (또는 state file)
        rest_summary: REST provider.query_account_summary() 결과
    Returns:
        DualReadResult (observer-only, state 변경 없음)
    """
    result = DualReadResult()

    # ── 1. REST source quality gate ──
    rest_status = rest_summary.get("_status", "COMPLETE")
    rest_consistency = rest_summary.get("_consistency", "CLEAN")

    if rest_status in ("PARTIAL", "FAILED"):
        result.rest_rejected = True
        result.rejection_reason = f"REST status={rest_status}"
        result.eligible = False
        logger.warning(f"[DUAL_READ_REJECTED] {result.rejection_reason}")
        return result

    if rest_consistency == "DEGRADED":
        result.rest_rejected = True
        result.rejection_reason = f"REST consistency={rest_consistency}"
        result.eligible = False
        logger.warning(f"[DUAL_READ_REJECTED] {result.rejection_reason}")
        return result

    if rest_summary.get("error"):
        result.rest_rejected = True
        result.rejection_reason = f"REST error={rest_summary['error']}"
        result.eligible = False
        return result

    # ── 2. Timing window ──
    com_ts = _extract_ts(com_summary)
    rest_ts = rest_summary.get("_snapshot_ts", 0)
    if com_ts > 0 and rest_ts > 0:
        result.timing_delta_s = abs(com_ts - rest_ts)
        if result.timing_delta_s > TIMING_WINDOW_S:
            result.eligible = False
            logger.info(
                f"[DUAL_READ_TIMING] delta={result.timing_delta_s:.1f}s "
                f"> window={TIMING_WINDOW_S}s - comparison not eligible"
            )
            return result

    # ── 3. Extract values ──
    com_cash = com_summary.get("cash", com_summary.get("available_cash", 0))
    rest_cash = rest_summary.get("available_cash", 0)

    com_positions = com_summary.get("positions", {})
    rest_holdings = {
        h.get("code", ""): h
        for h in rest_summary.get("holdings", [])
    }

    com_codes = set(com_positions.keys())
    rest_codes = set(rest_holdings.keys())

    # ── 4. Compare: Cash ──
    cash_diff = abs(com_cash - rest_cash)
    if cash_diff > 100:
        result.diffs.append({
            "field": "cash",
            "com": com_cash,
            "rest": rest_cash,
            "diff": cash_diff,
        })

    # ── 5. Compare: Code set ──
    com_only = com_codes - rest_codes
    rest_only = rest_codes - com_codes
    if com_only or rest_only:
        result.diffs.append({
            "field": "codeset",
            "com_only": sorted(com_only),
            "rest_only": sorted(rest_only),
        })

    # ── 6. Compare: Per-code qty ──
    for code in com_codes & rest_codes:
        c_qty = int(com_positions[code].get("quantity",
                    com_positions[code].get("qty", 0)))
        r_qty = int(rest_holdings[code].get("qty", 0))
        if c_qty != r_qty:
            result.diffs.append({
                "field": f"qty_{code}",
                "com": c_qty,
                "rest": r_qty,
            })

    # ── 7. Compare: Last price ──
    for code in com_codes & rest_codes:
        c_price = float(com_positions[code].get("current_price",
                        com_positions[code].get("cur_price", 0)))
        r_price = float(rest_holdings[code].get("cur_price", 0))
        if c_price > 0 and r_price > 0:
            pct = abs(c_price - r_price) / max(c_price, 1)
            if pct > PRICE_TOLERANCE_PCT:
                result.diffs.append({
                    "field": f"price_{code}",
                    "com": c_price,
                    "rest": r_price,
                    "pct": round(pct, 4),
                })

    # ── 8. Log ──
    if result.diffs:
        for d in result.diffs[:5]:
            logger.info(
                f"[DUAL_READ_DIFF] {d['field']}: "
                f"com={d.get('com', 'N/A')} rest={d.get('rest', 'N/A')}"
            )
        if len(result.diffs) > 5:
            logger.info(f"[DUAL_READ_DIFF] ... and {len(result.diffs) - 5} more")
    else:
        logger.info(
            f"[DUAL_READ_MATCH] all fields match "
            f"(delta={result.timing_delta_s:.1f}s)"
        )

    # Phase 3: COM wins on mismatch
    result.source_used = "com"
    return result


def _extract_ts(summary: Dict) -> float:
    """COM summary에서 timestamp 추출."""
    # state file format
    ts_str = summary.get("timestamp", "")
    if ts_str:
        try:
            from datetime import datetime
            return datetime.fromisoformat(ts_str).timestamp()
        except Exception:
            pass
    return summary.get("_snapshot_ts", 0)
