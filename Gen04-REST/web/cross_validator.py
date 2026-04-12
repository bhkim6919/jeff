# -*- coding: utf-8 -*-
"""
cross_validator.py -- COM<->REST 교차검증
========================================
compare_engine_vs_broker: Gen4 state file vs Kiwoom broker API
compare_triple: Gen4 vs REST_DB vs Broker 3자 교차검증

Phase 2 (P2): CrossValidationObserver
  - Observer-only (state write / permission 변경 금지)
  - diff taxonomy (TIMING / CODESET / QTY / CASH / etc.)
  - source quality gate (PARTIAL / DEGRADED 분리)
  - eligible_diff_zero_rate 산출
"""
from __future__ import annotations

import csv
import enum
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.crosscheck")

_GEN04_STATE = Path(__file__).resolve().parent.parent.parent / "Gen04" / "state"


def compare_engine_vs_broker(provider: Any) -> Dict[str, Any]:
    """Compare Gen4 state file vs live Kiwoom broker API."""
    result = {"timestamp": datetime.now().isoformat(), "checks": [], "overall": "MATCH"}

    # 1. Load COM state
    com_state = _load_com_state()
    if not com_state:
        return {"timestamp": result["timestamp"], "overall": "UNAVAILABLE",
                "error": "COM state file not found"}

    com_ts = com_state.get("_ts", 0)

    # 2. Query broker
    try:
        summary = provider.query_account_summary()
        if summary.get("error"):
            return {"timestamp": result["timestamp"], "overall": "UNAVAILABLE",
                    "error": f"Broker query failed: {summary['error']}"}
    except Exception as e:
        return {"timestamp": result["timestamp"], "overall": "UNAVAILABLE", "error": str(e)}

    rest_ts = time.time()

    # 3. Timestamp delta check
    ts_delta = abs(rest_ts - com_ts)
    if ts_delta > 60:
        result["checks"].append({
            "field": "timestamp_delta",
            "severity": "WARN",
            "detail": f"COM {ts_delta:.0f}초 전 상태 (>{60}s)",
        })

    # 4. Cash
    com_cash = com_state.get("cash", 0)
    rest_cash = summary.get("available_cash", 0)
    cash_diff = abs(com_cash - rest_cash)
    result["checks"].append({
        "field": "cash",
        "com": com_cash,
        "rest": rest_cash,
        "diff": cash_diff,
        "severity": "MATCH" if cash_diff <= 100 else "WARN" if cash_diff <= 10000 else "CRITICAL",
    })

    # 5. Holdings count
    com_positions = com_state.get("positions", {})
    rest_holdings = summary.get("holdings", [])
    com_count = len(com_positions)
    rest_count = len(rest_holdings)
    result["checks"].append({
        "field": "holdings_count",
        "com": com_count,
        "rest": rest_count,
        "severity": "MATCH" if com_count == rest_count else "CRITICAL",
    })

    # 6. Ticker set
    com_tickers = set(com_positions.keys())
    rest_tickers = set(h.get("code", "") for h in rest_holdings)
    com_only = com_tickers - rest_tickers
    rest_only = rest_tickers - com_tickers
    if com_only or rest_only:
        result["checks"].append({
            "field": "ticker_set",
            "com_only": list(com_only),
            "rest_only": list(rest_only),
            "severity": "CRITICAL",
        })
    else:
        result["checks"].append({
            "field": "ticker_set",
            "severity": "MATCH",
        })

    # 7. Qty per ticker
    rest_qty_map = {h.get("code", ""): h.get("qty", 0) for h in rest_holdings}
    qty_mismatches = []
    for code, pos in com_positions.items():
        com_qty = pos.get("quantity", 0)
        rest_qty = rest_qty_map.get(code, 0)
        if com_qty != rest_qty:
            qty_mismatches.append({"code": code, "com": com_qty, "rest": rest_qty})
    if qty_mismatches:
        result["checks"].append({
            "field": "qty_mismatch",
            "mismatches": qty_mismatches,
            "severity": "CRITICAL",
        })

    # Overall severity
    severities = [c.get("severity", "MATCH") for c in result["checks"]]
    if "CRITICAL" in severities:
        result["overall"] = "CRITICAL"
    elif "WARN" in severities:
        result["overall"] = "WARN"
    else:
        result["overall"] = "MATCH"

    return result


def _load_com_state() -> Dict:
    """Load Gen4 COM portfolio state."""
    for name in ["portfolio_state_live.json", "portfolio_state_paper.json"]:
        p = _GEN04_STATE / name
        if p.exists():
            try:
                data = json.loads(p.read_text("utf-8"))
                # Parse timestamp
                ts_raw = data.get("timestamp", "")
                if isinstance(ts_raw, str) and ts_raw:
                    try:
                        from datetime import datetime as _dt
                        data["_ts"] = _dt.fromisoformat(ts_raw).timestamp()
                    except Exception:
                        data["_ts"] = 0
                return data
            except Exception:
                continue
    return {}


# ── Phase 1: Triple Crosscheck (Gen4 vs REST_DB vs Broker) ──────

def compare_triple(provider: Any) -> Dict[str, Any]:
    """
    Gen4 state vs REST_DB vs Broker 3자 교차검증.
    동일 snapshot_id 기준으로 비교, 결과를 rest_validation_log에 기록.
    """
    from web.rest_state_db import (
        get_latest_positions, get_latest_equity,
        log_validation, make_snapshot_id,
    )

    snap_id = make_snapshot_id()
    result = {
        "timestamp": datetime.now().isoformat(),
        "snapshot_id": snap_id,
        "checks": [],
        "overall": "MATCH",
    }

    # ── 1. Read Gen4 state ──
    gen4 = _load_com_state()
    if not gen4:
        result["overall"] = "UNAVAILABLE"
        result["error"] = "Gen4 state unavailable"
        return result
    gen4_positions = gen4.get("positions", {})
    gen4_cash = gen4.get("cash", 0)

    # ── 2. Read REST_DB ──
    rest_positions = get_latest_positions()
    rest_equity = get_latest_equity()

    # ── 3. Read Broker ──
    try:
        broker = provider.query_account_summary()
        if broker.get("error"):
            result["overall"] = "UNAVAILABLE"
            result["error"] = f"Broker query failed: {broker['error']}"
            return result
    except Exception as e:
        result["overall"] = "UNAVAILABLE"
        result["error"] = str(e)
        return result

    broker_holdings = {h.get("code", ""): h for h in broker.get("holdings", [])}
    broker_cash = broker.get("available_cash", 0)

    # ── 4. Compare: Positions count ──
    gen4_codes = set(gen4_positions.keys())
    rest_codes = set(rest_positions.keys())
    broker_codes = set(broker_holdings.keys())

    _check(result, snap_id, "POSITION_COUNT",
           len(gen4_codes), len(rest_codes), len(broker_codes),
           threshold=0, exact=True)

    # ── 5. Compare: Position qty per code ──
    all_codes = gen4_codes | rest_codes | broker_codes
    for code in sorted(all_codes):
        g_qty = gen4_positions.get(code, {}).get("quantity", 0)
        r_qty = rest_positions.get(code, {}).get("qty", 0)
        b_qty = int(broker_holdings.get(code, {}).get("qty", 0))

        if g_qty != b_qty or r_qty != b_qty:
            _check(result, snap_id, f"QTY_{code}",
                   g_qty, r_qty, b_qty, threshold=0, exact=True)

    # ── 6. Compare: HWM (Gen4 vs REST_DB) ──
    for code in gen4_codes & rest_codes:
        g_hwm = float(gen4_positions[code].get("high_watermark", 0))
        r_hwm = float(rest_positions[code].get("high_watermark", 0))
        if g_hwm > 0 and r_hwm > 0 and g_hwm != r_hwm:
            diff = abs(g_hwm - r_hwm) / max(g_hwm, 1)
            _check(result, snap_id, f"HWM_{code}",
                   g_hwm, r_hwm, None, threshold=0.001, diff_pct=diff)

    # ── 7. Compare: Trail Stop (Gen4 vs REST_DB) ──
    for code in gen4_codes & rest_codes:
        g_trail = float(gen4_positions[code].get("trail_stop_price", 0))
        r_trail = float(rest_positions[code].get("trail_stop_price", 0))
        if g_trail > 0 and r_trail > 0 and g_trail != r_trail:
            diff = abs(g_trail - r_trail) / max(g_trail, 1)
            _check(result, snap_id, f"TRAIL_{code}",
                   g_trail, r_trail, None, threshold=0.001, diff_pct=diff)

    # ── 8. Compare: Cash ──
    r_cash = float((rest_equity or {}).get("cash", 0))
    cash_diff = abs(gen4_cash - broker_cash)
    _check(result, snap_id, "CASH",
           gen4_cash, r_cash, broker_cash,
           threshold=10000, diff_pct=cash_diff / max(broker_cash, 1) if broker_cash else 0)

    # ── 9. Compare: Equity (Gen4 prev_close vs REST_DB) ──
    g_prev = float(gen4.get("prev_close_equity", 0))
    r_prev = float((rest_equity or {}).get("prev_close_equity", 0))
    if g_prev > 0 and r_prev > 0:
        diff = abs(g_prev - r_prev) / max(g_prev, 1)
        _check(result, snap_id, "PREV_CLOSE_EQUITY",
               g_prev, r_prev, None, threshold=0.005, diff_pct=diff)

    # ── 10. Compare: avg_price (Gen4 vs broker, 참고용) ──
    for code in gen4_codes & broker_codes:
        g_avg = float(gen4_positions[code].get("avg_price", 0))
        b_avg = float(broker_holdings[code].get("pur_pric",
                      broker_holdings[code].get("avg_price", 0)))
        if g_avg > 0 and b_avg > 0:
            diff = abs(g_avg - b_avg) / max(g_avg, 1)
            if diff > 0.01:  # 1% 초과만 기록 (근사값 차이는 무시)
                _check(result, snap_id, f"AVGPRICE_{code}",
                       g_avg, None, b_avg, threshold=0.01, diff_pct=diff)

    # Overall
    severities = [c["status"] for c in result["checks"]]
    if "CRITICAL" in severities:
        result["overall"] = "CRITICAL"
    elif "WARN" in severities:
        result["overall"] = "WARN"

    logger.info(f"[TRIPLE_CHECK] {result['overall']}: "
                f"{len(result['checks'])} checks, "
                f"CRITICAL={severities.count('CRITICAL')}, "
                f"WARN={severities.count('WARN')}")

    return result


def _check(result: dict, snap_id: str, check_type: str,
           gen4_val, rest_val, broker_val,
           threshold: float = 0, exact: bool = False,
           diff_pct: float = 0) -> None:
    """단일 검증 항목 판정 + 로그 기록."""
    from web.rest_state_db import log_validation

    if exact:
        vals = [v for v in (gen4_val, rest_val, broker_val) if v is not None]
        if len(set(vals)) <= 1:
            status = "MATCH"
        else:
            status = "CRITICAL"
    else:
        status = "MATCH" if diff_pct <= threshold else (
            "WARN" if diff_pct <= threshold * 2 else "CRITICAL"
        )

    if status != "MATCH":
        result["checks"].append({
            "field": check_type,
            "gen4": gen4_val,
            "rest": rest_val,
            "broker": broker_val,
            "diff_pct": round(diff_pct, 6) if diff_pct else 0,
            "status": status,
        })

    try:
        log_validation(
            check_type=check_type,
            gen4_value=gen4_val,
            rest_value=rest_val,
            broker_value=broker_val,
            diff_pct=diff_pct,
            status=status,
            snapshot_id=snap_id,
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════
# Phase 2 (P2): CrossValidationObserver
# ══════════════════════════════════════════════════════════════════

class DiffType(enum.Enum):
    """Diff taxonomy -- 원인별 분류."""
    DIFF_NONE = "DIFF_NONE"
    DIFF_TIMING_WINDOW = "DIFF_TIMING_WINDOW"
    DIFF_CODESET = "DIFF_CODESET"
    DIFF_QTY = "DIFF_QTY"
    DIFF_AVG_PRICE = "DIFF_AVG_PRICE"
    DIFF_CASH = "DIFF_CASH"
    DIFF_OPEN_ORDERS = "DIFF_OPEN_ORDERS"
    DIFF_VERSION_MISMATCH = "DIFF_VERSION_MISMATCH"
    DIFF_PARTIAL_SOURCE = "DIFF_PARTIAL_SOURCE"
    DIFF_DEGRADED_SOURCE = "DIFF_DEGRADED_SOURCE"


@dataclass
class SourceMeta:
    """단일 소스의 메타데이터."""
    name: str                           # "broker" | "rest" | "file"
    snapshot_ts: float = 0.0
    batch_id: str = ""
    version_seq: int = 0
    status: str = "COMPLETE"            # COMPLETE | PARTIAL | FAILED
    consistency: str = "CLEAN"          # CLEAN | DEGRADED
    available: bool = True


@dataclass
class XvalSample:
    """단일 교차검증 샘플."""
    sample_id: str = ""
    timestamp: str = ""
    sources: Dict[str, SourceMeta] = field(default_factory=dict)
    diffs: List[Dict] = field(default_factory=list)    # [{type, field, detail}]
    eligible: bool = True               # strict 계산 포함 여부
    exclusion_reason: str = ""


class CrossValidationObserver:
    """Observer-only 3자 교차검증기.

    절대 state write / buy permission 변경을 하지 않는다.
    결과는 로그 + 통계 누적 + CSV만 기록한다.
    """

    # Timing window: 이 시간 내 차이는 DIFF_TIMING_WINDOW로 분류
    TIMING_WINDOW_S = 5.0

    def __init__(self, log_dir: Optional[Path] = None):
        self._samples: List[XvalSample] = []
        self._diff_counts: Dict[str, int] = defaultdict(int)
        self._log_dir = log_dir
        self._consecutive_zero = 0
        self._max_consecutive_zero = 0

    # ── Main Entry Point ──────────────────────────────────────

    def observe(
        self,
        broker_summary: Dict,
        file_state: Dict,
        rest_state: Optional[Dict] = None,
        open_orders_broker: Optional[List] = None,
        open_orders_file: Optional[List] = None,
    ) -> XvalSample:
        """3자 비교 실행. Observer-only: state 변경 없음.

        Args:
            broker_summary: provider.query_account_summary() 결과
            file_state: state_manager.load_portfolio() 결과
            rest_state: REST_DB 상태 (없으면 2자 비교)
            open_orders_broker: provider.query_open_orders() 결과
            open_orders_file: state에서 추출한 pending_external 목록
        """
        now = time.time()
        sample = XvalSample(
            sample_id=f"xval_{int(now)}",
            timestamp=datetime.now().isoformat(),
        )

        # ── 1. Source metadata 수집 ──
        broker_meta = SourceMeta(
            name="broker",
            snapshot_ts=broker_summary.get("_snapshot_ts", now),
            batch_id=broker_summary.get("_batch_id", ""),
            status=broker_summary.get("_status", "COMPLETE"),
            consistency=broker_summary.get("_consistency", "CLEAN"),
            available=broker_summary.get("error") is None,
        )

        file_meta = SourceMeta(
            name="file",
            snapshot_ts=self._parse_ts(file_state.get("timestamp", "")),
            version_seq=file_state.get("_version_seq", 0),
            available=bool(file_state),
        )

        rest_meta = SourceMeta(name="rest", available=False)
        if rest_state:
            rest_meta.available = True
            rest_meta.snapshot_ts = rest_state.get("_snapshot_ts", 0)
            rest_meta.version_seq = rest_state.get("_version_seq", 0)

        sample.sources = {
            "broker": broker_meta,
            "file": file_meta,
            "rest": rest_meta,
        }

        # ── 2. Eligibility gate ──
        if not broker_meta.available:
            sample.eligible = False
            sample.exclusion_reason = "broker unavailable"
            logger.warning(f"[XVAL_EXCLUDED_PARTIAL] {sample.sample_id}: broker unavailable")
            self._record(sample)
            return sample

        if broker_meta.status in ("PARTIAL", "FAILED"):
            sample.eligible = False
            sample.exclusion_reason = f"broker {broker_meta.status}"
            logger.warning(
                f"[XVAL_EXCLUDED_PARTIAL] {sample.sample_id}: "
                f"broker status={broker_meta.status}")
            self._record(sample)
            return sample

        if broker_meta.consistency == "DEGRADED":
            sample.eligible = False
            sample.exclusion_reason = "broker DEGRADED"
            logger.warning(
                f"[XVAL_EXCLUDED_PARTIAL] {sample.sample_id}: broker DEGRADED")
            self._record(sample)
            return sample

        if not file_meta.available:
            sample.eligible = False
            sample.exclusion_reason = "file unavailable"
            self._record(sample)
            return sample

        # ── 3. Timing window check ──
        ts_delta = abs(broker_meta.snapshot_ts - file_meta.snapshot_ts)
        timing_issue = ts_delta > self.TIMING_WINDOW_S

        # ── 4. Extract values ──
        broker_cash = broker_summary.get("available_cash", 0)
        broker_holdings = {
            h.get("code", ""): h
            for h in broker_summary.get("holdings", [])
        }
        broker_codes = set(broker_holdings.keys())

        file_positions = file_state.get("positions", {})
        file_cash = file_state.get("cash", 0)
        file_codes = set(file_positions.keys())

        # ── 5. Compare: Cash ──
        cash_diff = abs(broker_cash - file_cash)
        if cash_diff > 100:  # 100원 tolerance
            if timing_issue:
                self._add_diff(sample, DiffType.DIFF_TIMING_WINDOW,
                               "cash", f"broker={broker_cash} file={file_cash} "
                               f"delta_ts={ts_delta:.1f}s")
            else:
                self._add_diff(sample, DiffType.DIFF_CASH,
                               "cash", f"broker={broker_cash} file={file_cash}")

        # ── 6. Compare: Code set ──
        broker_only = broker_codes - file_codes
        file_only = file_codes - broker_codes
        if broker_only or file_only:
            if timing_issue:
                self._add_diff(sample, DiffType.DIFF_TIMING_WINDOW,
                               "codeset", f"broker_only={broker_only} "
                               f"file_only={file_only}")
            else:
                self._add_diff(sample, DiffType.DIFF_CODESET,
                               "codeset", f"broker_only={broker_only} "
                               f"file_only={file_only}")

        # ── 7. Compare: Per-code qty ──
        for code in broker_codes & file_codes:
            b_qty = int(broker_holdings[code].get("qty", 0))
            f_qty = int(file_positions[code].get("quantity",
                        file_positions[code].get("qty", 0)))
            if b_qty != f_qty:
                if timing_issue:
                    self._add_diff(sample, DiffType.DIFF_TIMING_WINDOW,
                                   f"qty_{code}",
                                   f"broker={b_qty} file={f_qty}")
                else:
                    self._add_diff(sample, DiffType.DIFF_QTY,
                                   f"qty_{code}",
                                   f"broker={b_qty} file={f_qty}")

        # ── 8. Compare: Per-code avg_price ──
        for code in broker_codes & file_codes:
            b_avg = int(broker_holdings[code].get("avg_price", 0))
            f_avg = int(file_positions[code].get("avg_price", 0))
            if b_avg > 0 and f_avg > 0:
                pct = abs(b_avg - f_avg) / max(b_avg, 1)
                if pct > 0.01:  # 1% tolerance
                    self._add_diff(sample, DiffType.DIFF_AVG_PRICE,
                                   f"avg_{code}",
                                   f"broker={b_avg} file={f_avg} pct={pct:.4f}")

        # ── 9. Compare: Open orders ──
        if open_orders_broker is not None and open_orders_file is not None:
            b_count = len(open_orders_broker)
            f_count = len(open_orders_file)
            if b_count != f_count:
                self._add_diff(sample, DiffType.DIFF_OPEN_ORDERS,
                               "open_orders",
                               f"broker={b_count} file={f_count}")

        # ── 10. Version mismatch ──
        if (file_meta.version_seq > 0 and rest_meta.available
                and rest_meta.version_seq > 0
                and file_meta.version_seq != rest_meta.version_seq):
            self._add_diff(sample, DiffType.DIFF_VERSION_MISMATCH,
                           "version_seq",
                           f"file={file_meta.version_seq} "
                           f"rest={rest_meta.version_seq}")

        # ── 11. Result ──
        if not sample.diffs:
            sample.diffs.append({
                "type": DiffType.DIFF_NONE.value,
                "field": "all",
                "detail": "",
            })

        self._record(sample)
        return sample

    # ── Internal ──────────────────────────────────────────────

    def _add_diff(self, sample: XvalSample, diff_type: DiffType,
                  field: str, detail: str) -> None:
        sample.diffs.append({
            "type": diff_type.value,
            "field": field,
            "detail": detail,
        })

    def _record(self, sample: XvalSample) -> None:
        """샘플 기록 + 로그 + 통계 갱신."""
        self._samples.append(sample)

        # Diff 카운트
        for d in sample.diffs:
            self._diff_counts[d["type"]] += 1

        # Consecutive zero tracking
        is_zero = (len(sample.diffs) == 1
                   and sample.diffs[0]["type"] == DiffType.DIFF_NONE.value)
        if is_zero and sample.eligible:
            self._consecutive_zero += 1
            self._max_consecutive_zero = max(
                self._max_consecutive_zero, self._consecutive_zero)
        elif sample.eligible:
            self._consecutive_zero = 0

        # Logging
        if not sample.eligible:
            logger.info(
                f"[XVAL_EXCLUDED_PARTIAL] {sample.sample_id}: "
                f"{sample.exclusion_reason}")
        elif is_zero:
            logger.info(f"[XVAL_DIFF_NONE] {sample.sample_id}: all match")
        else:
            # Classify: timing vs real
            types = {d["type"] for d in sample.diffs}
            if types == {DiffType.DIFF_TIMING_WINDOW.value}:
                logger.info(
                    f"[XVAL_DIFF_TIMING] {sample.sample_id}: "
                    f"{len(sample.diffs)} timing diffs")
            else:
                real_diffs = [
                    d for d in sample.diffs
                    if d["type"] not in (DiffType.DIFF_NONE.value,
                                         DiffType.DIFF_TIMING_WINDOW.value)
                ]
                logger.warning(
                    f"[XVAL_DIFF_REAL] {sample.sample_id}: "
                    f"{len(real_diffs)} real diffs: "
                    + ", ".join(f"{d['type']}:{d['field']}" for d in real_diffs[:5]))

    # ── Statistics ────────────────────────────────────────────

    def get_stats(self) -> Dict:
        """현재 누적 통계 반환."""
        total = len(self._samples)
        eligible = [s for s in self._samples if s.eligible]
        eligible_count = len(eligible)

        zero_eligible = sum(
            1 for s in eligible
            if len(s.diffs) == 1
            and s.diffs[0]["type"] == DiffType.DIFF_NONE.value
        )

        zero_strict = sum(
            1 for s in self._samples
            if len(s.diffs) == 1
            and s.diffs[0]["type"] == DiffType.DIFF_NONE.value
        )

        timing_only = sum(
            1 for s in eligible
            if all(d["type"] in (DiffType.DIFF_TIMING_WINDOW.value,
                                  DiffType.DIFF_NONE.value)
                   for d in s.diffs)
            and any(d["type"] == DiffType.DIFF_TIMING_WINDOW.value
                    for d in s.diffs)
        )

        partial_excluded = total - eligible_count
        degraded = sum(
            1 for s in self._samples
            if not s.eligible and "DEGRADED" in s.exclusion_reason
        )

        # Critical diff counts (CODESET, QTY, CASH only)
        critical_types = {DiffType.DIFF_CODESET.value,
                          DiffType.DIFF_QTY.value,
                          DiffType.DIFF_CASH.value}
        critical_count = sum(
            self._diff_counts.get(t, 0) for t in critical_types
        )

        stats = {
            "total_samples": total,
            "eligible_samples": eligible_count,
            "strict_diff_zero_rate": (
                zero_strict / total if total > 0 else 0),
            "eligible_diff_zero_rate": (
                zero_eligible / eligible_count if eligible_count > 0 else 0),
            "timing_window_diff_rate": (
                timing_only / eligible_count if eligible_count > 0 else 0),
            "partial_source_rate": (
                partial_excluded / total if total > 0 else 0),
            "degraded_source_rate": (
                degraded / total if total > 0 else 0),
            "diff_by_type": dict(self._diff_counts),
            "consecutive_zero": self._consecutive_zero,
            "max_consecutive_zero": self._max_consecutive_zero,
            "critical_diff_count": critical_count,
        }
        return stats

    def log_summary(self) -> Dict:
        """통계 요약 로그 출력 + 반환."""
        stats = self.get_stats()
        logger.info(
            f"[XVAL_SUMMARY] "
            f"total={stats['total_samples']} "
            f"eligible={stats['eligible_samples']} "
            f"strict_zero={stats['strict_diff_zero_rate']:.2%} "
            f"eligible_zero={stats['eligible_diff_zero_rate']:.2%} "
            f"timing={stats['timing_window_diff_rate']:.2%} "
            f"partial={stats['partial_source_rate']:.2%} "
            f"critical_diffs={stats['critical_diff_count']} "
            f"consec_zero={stats['consecutive_zero']}"
        )
        return stats

    def check_phase3_ready(self, min_samples: int = 200,
                           min_zero_rate: float = 0.99) -> Dict:
        """Phase 3 진입 조건 판정."""
        stats = self.get_stats()
        eligible = stats["eligible_samples"]
        zero_rate = stats["eligible_diff_zero_rate"]
        critical = stats["critical_diff_count"]

        ready = (
            eligible >= min_samples
            and zero_rate >= min_zero_rate
            and critical == 0
        )

        result = {
            "ready": ready,
            "eligible_samples": eligible,
            "eligible_diff_zero_rate": zero_rate,
            "critical_diff_count": critical,
            "min_samples": min_samples,
            "min_zero_rate": min_zero_rate,
        }

        if ready:
            logger.info(f"[XVAL_PHASE3_READY] {result}")
        else:
            reasons = []
            if eligible < min_samples:
                reasons.append(f"samples {eligible}/{min_samples}")
            if zero_rate < min_zero_rate:
                reasons.append(f"zero_rate {zero_rate:.2%}/{min_zero_rate:.0%}")
            if critical > 0:
                reasons.append(f"critical_diffs={critical}")
            result["blocking_reasons"] = reasons
            logger.info(f"[XVAL_PHASE3_NOT_READY] {', '.join(reasons)}")

        return result

    def save_daily_summary(self, output_dir: Path) -> Optional[Path]:
        """일별 summary를 JSON + CSV로 저장."""
        if not self._samples:
            return None

        output_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        stats = self.get_stats()

        # JSON
        json_path = output_dir / f"xval_summary_{date_str}.json"
        json_path.write_text(
            json.dumps(stats, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        # CSV (append)
        csv_path = output_dir / "xval_daily.csv"
        write_header = not csv_path.exists()
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if write_header:
                w.writerow([
                    "date", "total", "eligible", "strict_zero_rate",
                    "eligible_zero_rate", "timing_rate", "partial_rate",
                    "critical_diffs", "consec_zero",
                ])
            w.writerow([
                date_str,
                stats["total_samples"],
                stats["eligible_samples"],
                f"{stats['strict_diff_zero_rate']:.4f}",
                f"{stats['eligible_diff_zero_rate']:.4f}",
                f"{stats['timing_window_diff_rate']:.4f}",
                f"{stats['partial_source_rate']:.4f}",
                stats["critical_diff_count"],
                stats["consecutive_zero"],
            ])

        logger.info(f"[XVAL_SAVED] {json_path.name}")
        return json_path

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse_ts(ts_str: str) -> float:
        if not ts_str:
            return 0.0
        try:
            return datetime.fromisoformat(ts_str).timestamp()
        except Exception:
            return 0.0
