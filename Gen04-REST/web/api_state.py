# -*- coding: utf-8 -*-
"""
api_state.py -- ApiStateTracker Singleton
==========================================
REST API 모니터링 시스템의 중앙 상태 관리.
모든 요청 추적, 건강 상태, freshness, COM/REST 동기 비교를 담당.

Usage:
    from web.api_state import tracker
    tracker.record_request(...)
    state = tracker.snapshot()
"""
from __future__ import annotations

import re
import statistics
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple


# ── Enums ─────────────────────────────────────────────────────

class HealthStatus(str, Enum):
    GREEN = "GREEN"    # All systems nominal
    YELLOW = "YELLOW"  # Degraded (stale data, high latency, retries)
    RED = "RED"        # Failures (auth fail, repeated errors)
    BLACK = "BLACK"    # Dead (no connection, provider down)


class LogTag(str, Enum):
    # Auth
    AUTH_START = "AUTH_START"
    AUTH_OK = "AUTH_OK"
    AUTH_FAIL = "AUTH_FAIL"
    # API
    API_REQ = "API_REQ"
    API_RESP = "API_RESP"
    API_TIMEOUT = "API_TIMEOUT"
    API_RETRY = "API_RETRY"
    # WebSocket
    WS_CONNECT = "WS_CONNECT"
    WS_DROP = "WS_DROP"
    WS_RECV = "WS_RECV"
    # State
    SNAPSHOT_UPDATE = "SNAPSHOT_UPDATE"
    STALE_DETECTED = "STALE_DETECTED"
    SYNC_MISMATCH = "SYNC_MISMATCH"
    # Orders
    ORDER_SUBMIT = "ORDER_SUBMIT"
    ORDER_ACK = "ORDER_ACK"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_FAIL = "ORDER_FAIL"
    # UI
    UI_STATE_CHANGE = "UI_STATE_CHANGE"


# ── Data Classes ──────────────────────────────────────────────

@dataclass
class RequestTrace:
    """Single API request/response trace record."""
    request_id: str
    endpoint: str
    api_id: str
    request_ts: float          # Unix timestamp
    response_ts: float = 0.0
    latency_ms: float = 0.0
    status: str = "pending"    # pending | ok | error | timeout | retry
    http_status: int = 0
    retry_count: int = 0
    error: str = ""
    related_account: str = ""
    related_code: str = ""
    related_order_id: str = ""
    tag: str = ""
    request_body_summary: str = ""
    response_body_summary: str = ""

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "endpoint": self.endpoint,
            "api_id": self.api_id,
            "request_ts": self.request_ts,
            "request_time": datetime.fromtimestamp(self.request_ts).strftime("%H:%M:%S.%f")[:-3] if self.request_ts else "",
            "response_ts": self.response_ts,
            "response_time": datetime.fromtimestamp(self.response_ts).strftime("%H:%M:%S.%f")[:-3] if self.response_ts else "",
            "latency_ms": round(self.latency_ms, 1),
            "status": self.status,
            "http_status": self.http_status,
            "retry_count": self.retry_count,
            "error": self.error,
            "related_account": self.related_account,
            "related_code": self.related_code,
            "related_order_id": self.related_order_id,
            "tag": self.tag,
            "request_body": self.request_body_summary,
            "response_body": self.response_body_summary,
        }


@dataclass
class FreshnessPoint:
    """Timestamp + staleness tracking for a data source."""
    source: str
    last_update: float = 0.0
    update_count: int = 0
    stale_threshold_sec: float = 60.0  # WARN after this
    critical_threshold_sec: float = 300.0  # STALE after this

    @property
    def age_sec(self) -> float:
        if self.last_update == 0:
            return float("inf")
        return time.time() - self.last_update

    @property
    def status(self) -> str:
        age = self.age_sec
        if age == float("inf"):
            return "NEVER"
        if age > self.critical_threshold_sec:
            return "STALE"
        if age > self.stale_threshold_sec:
            return "WARN"
        return "FRESH"

    def to_dict(self) -> dict:
        age = self.age_sec
        return {
            "source": self.source,
            "last_update": self.last_update,
            "last_update_str": datetime.fromtimestamp(self.last_update).strftime("%H:%M:%S") if self.last_update else "never",
            "age_sec": round(age, 1) if age != float("inf") else None,
            "status": self.status,
            "update_count": self.update_count,
        }


@dataclass
class SyncComparison:
    """REST vs COM side-by-side comparison record."""
    field_name: str
    rest_value: Any = None
    com_value: Any = None
    rest_ts: float = 0.0
    com_ts: float = 0.0
    match: bool = True
    diff: str = ""
    last_match_ts: float = 0.0

    def to_dict(self) -> dict:
        return {
            "field": self.field_name,
            "rest": self.rest_value,
            "com": self.com_value,
            "rest_ts": datetime.fromtimestamp(self.rest_ts).strftime("%H:%M:%S") if self.rest_ts else "",
            "com_ts": datetime.fromtimestamp(self.com_ts).strftime("%H:%M:%S") if self.com_ts else "",
            "match": self.match,
            "diff": self.diff,
            "last_match": datetime.fromtimestamp(self.last_match_ts).strftime("%H:%M:%S") if self.last_match_ts else "",
        }


# ── Constants ─────────────────────────────────────────────────

MAX_TRACES = 500         # Keep last N request traces
MAX_LOG_LINES = 1000     # Max log lines to parse
LATENCY_WINDOW = 100     # Last N requests for latency stats
HEALTH_CHECK_INTERVAL = 5  # Seconds between health recalculations

# Thresholds for health
LATENCY_WARN_MS = 1000
LATENCY_CRITICAL_MS = 3000
FAILURE_RATE_WARN = 0.05   # 5%
FAILURE_RATE_CRITICAL = 0.15  # 15%


# ── ApiStateTracker ───────────────────────────────────────────

class ApiStateTracker:
    """
    Singleton state tracker for REST API monitoring.
    Thread-safe. All mutations go through lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # Request traces (ring buffer)
        self._traces: Deque[RequestTrace] = deque(maxlen=MAX_TRACES)

        # Latency history for stats
        self._latencies: Deque[float] = deque(maxlen=LATENCY_WINDOW)

        # Counters
        self._total_requests: int = 0
        self._total_failures: int = 0
        self._total_timeouts: int = 0
        self._total_retries: int = 0

        # Health
        self._health: HealthStatus = HealthStatus.BLACK
        self._health_reason: str = "Not initialized"
        self._last_health_check: float = 0.0

        # Token state
        self._token_valid: bool = False
        self._token_expires_at: float = 0.0
        self._token_last_refresh: float = 0.0

        # Server info
        self._server_type: str = "UNKNOWN"
        self._base_url: str = ""

        # Connection timestamps
        self._first_request_ts: float = 0.0
        self._last_request_ts: float = 0.0
        self._last_success_ts: float = 0.0
        self._last_failure_ts: float = 0.0

        # WebSocket state
        self._ws_connected: bool = False
        self._ws_last_msg_ts: float = 0.0
        self._ws_reconnect_count: int = 0
        self._ws_msg_count: int = 0

        # Freshness tracking
        self._freshness: Dict[str, FreshnessPoint] = {
            "account_summary": FreshnessPoint("account_summary", stale_threshold_sec=60, critical_threshold_sec=300),
            "holdings": FreshnessPoint("holdings", stale_threshold_sec=60, critical_threshold_sec=300),
            "open_orders": FreshnessPoint("open_orders", stale_threshold_sec=30, critical_threshold_sec=120),
            "price_tick": FreshnessPoint("price_tick", stale_threshold_sec=10, critical_threshold_sec=60),
            "ws_message": FreshnessPoint("ws_message", stale_threshold_sec=30, critical_threshold_sec=120),
        }

        # Sync comparison (REST vs COM)
        self._sync: Dict[str, SyncComparison] = {}

        # Log file path
        self._log_dir: Path = Path(__file__).resolve().parent.parent / "data" / "logs"

    # ── Record Methods ────────────────────────────────────────

    def record_request_start(
        self,
        endpoint: str,
        api_id: str,
        *,
        related_code: str = "",
        related_account: str = "",
        related_order_id: str = "",
        body_summary: str = "",
        tag: str = "",
    ) -> str:
        """Record start of an API request. Returns request_id."""
        req_id = f"req_{uuid.uuid4().hex[:8]}"
        now = time.time()

        trace = RequestTrace(
            request_id=req_id,
            endpoint=endpoint,
            api_id=api_id,
            request_ts=now,
            related_code=related_code,
            related_account=related_account,
            related_order_id=related_order_id,
            request_body_summary=body_summary[:200],
            tag=tag or LogTag.API_REQ.value,
        )

        with self._lock:
            self._traces.append(trace)
            self._total_requests += 1
            if self._first_request_ts == 0:
                self._first_request_ts = now
            self._last_request_ts = now

        return req_id

    def record_request_end(
        self,
        request_id: str,
        *,
        status: str = "ok",
        http_status: int = 200,
        latency_ms: float = 0.0,
        error: str = "",
        retry_count: int = 0,
        response_summary: str = "",
        tag: str = "",
    ) -> None:
        """Record completion of an API request."""
        now = time.time()

        with self._lock:
            for trace in reversed(self._traces):
                if trace.request_id == request_id:
                    trace.response_ts = now
                    trace.latency_ms = latency_ms or (now - trace.request_ts) * 1000
                    trace.status = status
                    trace.http_status = http_status
                    trace.error = error[:500]
                    trace.retry_count = retry_count
                    trace.response_body_summary = response_summary[:200]
                    if tag:
                        trace.tag = tag

                    self._latencies.append(trace.latency_ms)

                    if status in ("error", "timeout"):
                        self._total_failures += 1
                        self._last_failure_ts = now
                    else:
                        self._last_success_ts = now

                    if status == "timeout":
                        self._total_timeouts += 1
                    if retry_count > 0:
                        self._total_retries += retry_count
                    break

    def record_request(
        self,
        endpoint: str,
        api_id: str,
        *,
        latency_ms: float,
        status: str = "ok",
        http_status: int = 200,
        error: str = "",
        retry_count: int = 0,
        related_code: str = "",
        related_account: str = "",
        related_order_id: str = "",
        body_summary: str = "",
        response_summary: str = "",
        tag: str = "",
    ) -> str:
        """Convenience: record complete request in one call."""
        req_id = self.record_request_start(
            endpoint, api_id,
            related_code=related_code,
            related_account=related_account,
            related_order_id=related_order_id,
            body_summary=body_summary,
            tag=tag,
        )
        self.record_request_end(
            req_id,
            status=status,
            http_status=http_status,
            latency_ms=latency_ms,
            error=error,
            retry_count=retry_count,
            response_summary=response_summary,
        )
        return req_id

    # ── Token State ───────────────────────────────────────────

    def update_token_state(
        self,
        valid: bool,
        expires_at: float = 0.0,
    ) -> None:
        with self._lock:
            self._token_valid = valid
            if expires_at:
                self._token_expires_at = expires_at
            if valid:
                self._token_last_refresh = time.time()

    # ── Server Info ───────────────────────────────────────────

    def set_server_info(self, server_type: str, base_url: str = "") -> None:
        with self._lock:
            self._server_type = server_type
            if base_url:
                self._base_url = base_url

    # ── WebSocket State ───────────────────────────────────────

    def update_ws_state(
        self,
        connected: bool,
        reconnect_count: int = 0,
    ) -> None:
        with self._lock:
            self._ws_connected = connected
            self._ws_reconnect_count = reconnect_count

    def record_ws_message(self) -> None:
        with self._lock:
            self._ws_last_msg_ts = time.time()
            self._ws_msg_count += 1
            fp = self._freshness.get("ws_message")
            if fp:
                fp.last_update = self._ws_last_msg_ts
                fp.update_count += 1

    # ── Freshness ─────────────────────────────────────────────

    def update_freshness(self, source: str) -> None:
        """Mark a data source as freshly updated."""
        with self._lock:
            fp = self._freshness.get(source)
            if fp:
                fp.last_update = time.time()
                fp.update_count += 1
            else:
                self._freshness[source] = FreshnessPoint(
                    source=source,
                    last_update=time.time(),
                    update_count=1,
                )

    # ── Sync Comparison ───────────────────────────────────────

    def update_sync(
        self,
        field_name: str,
        rest_value: Any,
        com_value: Any,
        rest_ts: float = 0.0,
        com_ts: float = 0.0,
    ) -> None:
        """Update REST vs COM comparison for a field."""
        now = time.time()
        with self._lock:
            existing = self._sync.get(field_name)
            match = str(rest_value) == str(com_value)

            if existing and match and not existing.match:
                # Mismatch resolved
                last_match = now
            elif existing and existing.match:
                last_match = existing.last_match_ts or now
            else:
                last_match = now if match else (existing.last_match_ts if existing else 0.0)

            diff = ""
            if not match:
                diff = f"REST={rest_value} vs COM={com_value}"

            self._sync[field_name] = SyncComparison(
                field_name=field_name,
                rest_value=rest_value,
                com_value=com_value,
                rest_ts=rest_ts or now,
                com_ts=com_ts or now,
                match=match,
                diff=diff,
                last_match_ts=last_match,
            )

    # ── Health Calculation ────────────────────────────────────

    def _recalculate_health(self) -> Tuple[HealthStatus, str]:
        """Compute health status from current metrics. Call under lock."""
        now = time.time()

        # BLACK: no requests ever, or provider dead
        if self._total_requests == 0:
            return HealthStatus.BLACK, "No requests recorded"

        # BLACK: no successful request in 5 minutes
        if self._last_success_ts > 0 and (now - self._last_success_ts) > 300:
            return HealthStatus.BLACK, f"No success for {int(now - self._last_success_ts)}s"

        # BLACK: token not valid and no recent success
        if not self._token_valid and self._last_success_ts == 0:
            return HealthStatus.BLACK, "Token invalid, no successful requests"

        # Failure rate
        rate = self._total_failures / max(self._total_requests, 1)
        if rate > FAILURE_RATE_CRITICAL:
            return HealthStatus.RED, f"Failure rate {rate:.1%}"

        # Recent failures (last 5 min)
        recent_fails = sum(
            1 for t in self._traces
            if t.status in ("error", "timeout") and (now - t.request_ts) < 300
        )
        recent_total = sum(
            1 for t in self._traces
            if (now - t.request_ts) < 300
        )
        if recent_total > 0:
            recent_rate = recent_fails / recent_total
            if recent_rate > FAILURE_RATE_CRITICAL:
                return HealthStatus.RED, f"Recent failure rate {recent_rate:.1%} ({recent_fails}/{recent_total})"

        # Auth failure
        if not self._token_valid:
            return HealthStatus.RED, "Token expired/invalid"

        # Latency check
        if self._latencies:
            p95 = self._calc_p95()
            if p95 > LATENCY_CRITICAL_MS:
                return HealthStatus.RED, f"P95 latency {p95:.0f}ms"
            if p95 > LATENCY_WARN_MS:
                return HealthStatus.YELLOW, f"P95 latency {p95:.0f}ms"

        # Stale data
        stale_sources = [
            fp.source for fp in self._freshness.values()
            if fp.status == "STALE"
        ]
        if stale_sources:
            return HealthStatus.YELLOW, f"Stale: {', '.join(stale_sources)}"

        # Failure rate warning
        if rate > FAILURE_RATE_WARN:
            return HealthStatus.YELLOW, f"Failure rate {rate:.1%}"

        # Sync mismatches
        mismatches = [s.field_name for s in self._sync.values() if not s.match]
        if mismatches:
            return HealthStatus.YELLOW, f"Sync mismatch: {', '.join(mismatches)}"

        return HealthStatus.GREEN, "All systems nominal"

    def _calc_p95(self) -> float:
        """Calculate P95 latency from recent samples."""
        if not self._latencies:
            return 0.0
        sorted_lat = sorted(self._latencies)
        idx = int(len(sorted_lat) * 0.95)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]

    # ── Snapshot (main output) ────────────────────────────────

    def snapshot(self) -> dict:
        """
        Full state snapshot for SSE/API consumers.
        This is the single source of truth for the web dashboard.
        """
        with self._lock:
            now = time.time()

            # Recalculate health
            health, reason = self._recalculate_health()
            self._health = health
            self._health_reason = reason

            # Latency stats
            lat_last = self._latencies[-1] if self._latencies else 0.0
            lat_avg = statistics.mean(self._latencies) if self._latencies else 0.0
            lat_p95 = self._calc_p95()

            # Token remaining
            token_remaining_sec = max(0, self._token_expires_at - now) if self._token_expires_at else 0

            # Failure rate
            fail_rate = self._total_failures / max(self._total_requests, 1)

            return {
                "timestamp": now,
                "timestamp_str": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

                # Control panel
                "health": {
                    "status": health.value,
                    "reason": reason,
                },
                "token": {
                    "valid": self._token_valid,
                    "expires_at": self._token_expires_at,
                    "remaining_sec": round(token_remaining_sec),
                    "remaining_str": self._format_duration(token_remaining_sec),
                    "last_refresh": datetime.fromtimestamp(self._token_last_refresh).strftime("%H:%M:%S") if self._token_last_refresh else "never",
                },
                "server": {
                    "type": self._server_type,
                    "url": self._base_url,
                },
                "latency": {
                    "last_ms": round(lat_last, 1),
                    "avg_ms": round(lat_avg, 1),
                    "p95_ms": round(lat_p95, 1),
                },
                "counters": {
                    "total_requests": self._total_requests,
                    "total_failures": self._total_failures,
                    "total_timeouts": self._total_timeouts,
                    "total_retries": self._total_retries,
                    "failure_rate": round(fail_rate, 4),
                    "failure_rate_pct": f"{fail_rate:.1%}",
                },
                "timestamps": {
                    "first_request": datetime.fromtimestamp(self._first_request_ts).strftime("%H:%M:%S") if self._first_request_ts else "",
                    "last_request": datetime.fromtimestamp(self._last_request_ts).strftime("%H:%M:%S") if self._last_request_ts else "",
                    "last_success": datetime.fromtimestamp(self._last_success_ts).strftime("%H:%M:%S") if self._last_success_ts else "",
                    "last_failure": datetime.fromtimestamp(self._last_failure_ts).strftime("%H:%M:%S") if self._last_failure_ts else "",
                },
                "websocket": {
                    "connected": self._ws_connected,
                    "last_msg": datetime.fromtimestamp(self._ws_last_msg_ts).strftime("%H:%M:%S") if self._ws_last_msg_ts else "",
                    "reconnect_count": self._ws_reconnect_count,
                    "msg_count": self._ws_msg_count,
                },

                # Freshness
                "freshness": {
                    k: v.to_dict() for k, v in self._freshness.items()
                },

                # Sync comparison
                "sync": [s.to_dict() for s in self._sync.values()],

                # Recent traces (newest first, last 50)
                "traces": [t.to_dict() for t in reversed(list(self._traces))][:50],
            }

    def get_traces(
        self,
        *,
        limit: int = 100,
        status_filter: str = "",
        tag_filter: str = "",
    ) -> List[dict]:
        """Get filtered traces for the debug panel."""
        with self._lock:
            result = []
            for trace in reversed(self._traces):
                if status_filter and trace.status != status_filter:
                    continue
                if tag_filter and tag_filter not in trace.tag:
                    continue
                result.append(trace.to_dict())
                if len(result) >= limit:
                    break
            return result

    def get_latency_histogram(self, buckets: int = 20) -> List[dict]:
        """Generate latency distribution for debug view."""
        with self._lock:
            if not self._latencies:
                return []
            lat_list = list(self._latencies)

        min_lat = min(lat_list)
        max_lat = max(lat_list)
        if max_lat == min_lat:
            return [{"range": f"{min_lat:.0f}ms", "count": len(lat_list)}]

        step = (max_lat - min_lat) / buckets
        histogram = []
        for i in range(buckets):
            lo = min_lat + i * step
            hi = lo + step
            count = sum(1 for l in lat_list if lo <= l < hi or (i == buckets - 1 and l == hi))
            histogram.append({
                "range_lo": round(lo),
                "range_hi": round(hi),
                "label": f"{lo:.0f}-{hi:.0f}ms",
                "count": count,
            })
        return histogram

    # ── Log File Parser ───────────────────────────────────────

    def parse_log_file(self, max_lines: int = 200) -> List[dict]:
        """
        Parse today's rest_api_YYYYMMDD.log file into structured entries.
        Returns newest-first list of parsed log entries.
        """
        today = date.today().strftime("%Y%m%d")
        log_file = self._log_dir / f"rest_api_{today}.log"

        if not log_file.exists():
            return []

        entries = []
        # Read last N lines
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            for line in lines[-max_lines:]:
                entry = self._parse_log_line(line.strip())
                if entry:
                    entries.append(entry)
        except Exception:
            pass

        entries.reverse()  # newest first
        return entries

    def _parse_log_line(self, line: str) -> Optional[dict]:
        """Parse a single log line into structured dict."""
        if not line:
            return None

        # Format: 2026-04-07 09:15:32 [INFO] gen4.rest: [TAG] message
        match = re.match(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+\[(\w+)\]\s+([\w.]+):\s*(.*)",
            line,
        )
        if not match:
            return {"time": "", "level": "INFO", "logger": "", "message": line, "tag": ""}

        ts_str, level, logger_name, message = match.groups()

        # Extract tag from message: [TAG_NAME] ...
        tag = ""
        tag_match = re.match(r"\[(\w+)\]\s*(.*)", message)
        if tag_match:
            tag = tag_match.group(1)
            message = tag_match.group(2)

        return {
            "time": ts_str,
            "level": level,
            "logger": logger_name,
            "tag": tag,
            "message": message[:300],
        }

    # ── Utilities ─────────────────────────────────────────────

    @staticmethod
    def _format_duration(seconds: float) -> str:
        if seconds <= 0:
            return "expired"
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"


# ── Module-level singleton ────────────────────────────────────

tracker = ApiStateTracker()
