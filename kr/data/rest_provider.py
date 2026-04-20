"""
rest_provider.py — Kiwoom REST API Provider
============================================
BrokerProvider 구현체. COM(QAxWidget) 대신 HTTP REST + WebSocket 사용.

Phase 0: HTTP 조회/주문.
Phase 1: WebSocket 실시간 (0B 가격, 00 주문체결, 04 잔고).
"""
from __future__ import annotations

import enum
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests
from dotenv import load_dotenv

from data.provider_base import BrokerProvider
from data.rest_logger import setup_rest_logging
from data.rest_token_manager import TokenManager
from data.rest_websocket import KiwoomWebSocket
from web.api_state import tracker as api_tracker

# 로깅 자동 초기화 (import 시 1회)
setup_rest_logging()

logger = logging.getLogger("gen4.rest")

# Rate limit: minimum interval between REST calls
_MIN_REQUEST_INTERVAL = 0.2  # 200ms


def _decode_name(raw: str) -> str:
    """Decode Kiwoom EUC-KR encoded stock name."""
    if raw and any(ord(c) > 0x7F for c in raw):
        try:
            return raw.encode("latin-1").decode("euc-kr", errors="replace")
        except Exception:
            pass
    return raw


@dataclass
class _FillSlot:
    """단일 주문의 WebSocket fill 매칭 슬롯."""
    order_no: str
    code: str
    side: str
    requested_qty: int
    exec_price: int = 0
    exec_qty: int = 0          # 누적 체결 수량
    filled: threading.Event = field(default_factory=threading.Event)
    created_at: float = field(default_factory=time.time)


class KiwoomRestProvider(BrokerProvider):
    """Kiwoom REST API provider for Q-TRON Gen4."""

    def __init__(
        self,
        *,
        server_type: str = "REAL",
        sector_map_path: Optional[str] = None,
        env_path: Optional[str] = None,
    ) -> None:
        import os

        env_file = env_path or str(Path(__file__).resolve().parent.parent / ".env")
        load_dotenv(env_file)

        self._server_type_val = server_type
        self._alive = True

        # Credentials
        app_key = os.getenv("KIWOOM_APP_KEY", "")
        app_secret = os.getenv("KIWOOM_APP_SECRET", "")
        self._account_no = os.getenv("KIWOOM_ACCOUNT", "").replace("-", "")
        self._base_url = os.getenv("KIWOOM_API_URL", "https://api.kiwoom.com")

        if not app_key or not app_secret:
            raise RuntimeError("KIWOOM_APP_KEY / KIWOOM_APP_SECRET not set in .env")

        self._token_mgr = TokenManager(app_key, app_secret, self._base_url)

        # Tracker: server info + token
        api_tracker.set_server_info(server_type, self._base_url)

        # Rate limiter
        self._last_request_time = 0.0

        # Callbacks
        self._ghost_fill_callback: Optional[Callable] = None
        self._real_data_callback: Optional[Callable] = None
        self._micro_callback: Optional[Callable] = None
        self._ghost_orders_list: List[Dict] = []

        # WebSocket client (lazy init on first register_real)
        self._ws: Optional[KiwoomWebSocket] = None
        self._ws_started = False

        # Fill queue: order_no → FillSlot (다중 주문 동시 매칭 지원)
        self._fill_lock = threading.Lock()
        self._fill_slots: Dict[str, "_FillSlot"] = {}  # order_no → slot
        # Dedup: 동일 order_no + exec event 중복 방지
        self._fill_dedup: set = set()  # (order_no, cumulative_qty) tuples

        # WS 04(잔고변동) 이벤트 버퍼 — 상위에서 consume (portfolio 직접 수정 안 함)
        self._balance_lock = threading.Lock()
        self._balance_events: List[Dict] = []
        self._balance_seq = 0

        # Batch snapshot consistency tracking
        # batch 진행 중 WS 00(주문체결)/04(잔고변동) 이벤트 카운터
        self._batch_lock = threading.Lock()
        self._batch_active = False
        self._batch_ws_event_count = 0

        # 8005 Circuit Breaker (P5 추가, 2026-04-17)
        # 연속 8005 N회 → provider 를 일정 시간 halt. 300ms/req 스팸 방지 + 명시적 알림
        self._cb_lock = threading.Lock()
        self._cb_consecutive_8005 = 0
        self._cb_halt_until: float = 0.0   # epoch sec — 이 시각까지 모든 _request 차단
        self._cb_threshold = 5              # 연속 N회
        self._cb_halt_sec = 180              # 차단 시간
        self._cb_last_8005_event_sent: float = 0.0

        # Sector map
        self._sector_map: Dict[str, str] = {}
        if sector_map_path:
            self._load_sector_map(sector_map_path)

        logger.info(
            f"[REST_PROVIDER] init: server={server_type}, "
            f"account={self._account_no[:4]}****, base={self._base_url}"
        )

    def _load_sector_map(self, path: str) -> None:
        import json

        try:
            with open(path, "r", encoding="utf-8") as f:
                self._sector_map = json.load(f)
        except Exception as e:
            logger.warning(f"[REST_PROVIDER] sector_map load failed: {e}")

    # ── Central HTTP Request ──────────────────────────────────

    def _is_8005(self, data: dict) -> bool:
        """
        8005 Token 무효 에러 감지 — 여러 형태 커버.
        Kiwoom 응답 포맷 변동 가능성 고려 (return_code=1, return_code=8005, 메시지에 [8005:...] 등).
        """
        rc = data.get("return_code", 0)
        msg = str(data.get("return_msg", ""))
        if rc == 8005:
            return True
        if "[8005:" in msg or "[8005]" in msg:
            return True
        if rc == 1 and ("Token" in msg or "token" in msg or "토큰" in msg):
            return True
        return False

    def _cb_note_success(self) -> None:
        """정상 응답 받으면 circuit breaker 리셋."""
        with self._cb_lock:
            if self._cb_consecutive_8005 > 0 or self._cb_halt_until > 0:
                # Recovery signal — CRITICAL 이었으면 emit INFO
                try:
                    from web.data_events import emit_event, Level
                    emit_event(
                        source="KIWOOM.token",
                        level=Level.INFO,
                        code="consecutive_8005",
                        message=f"Kiwoom 인증 복구됨 (연속 실패 리셋)",
                        telegram=False,   # Recovery Telegram 은 emit_event 내부에서 처리
                    )
                except Exception:
                    pass
            self._cb_consecutive_8005 = 0
            self._cb_halt_until = 0.0

    def _cb_note_8005(self) -> None:
        """8005 받을 때마다 호출. threshold 도달 시 halt + emit."""
        with self._cb_lock:
            self._cb_consecutive_8005 += 1
            if self._cb_consecutive_8005 >= self._cb_threshold and self._cb_halt_until == 0.0:
                self._cb_halt_until = time.time() + self._cb_halt_sec
                try:
                    from web.data_events import emit_event, Level
                    emit_event(
                        source="KIWOOM.token",
                        level=Level.CRITICAL,
                        code="consecutive_8005",
                        message=(
                            f"Kiwoom 8005 연속 {self._cb_consecutive_8005}회 — "
                            f"{self._cb_halt_sec}s halt"
                        ),
                        details={
                            "consecutive": self._cb_consecutive_8005,
                            "halt_sec": self._cb_halt_sec,
                        },
                        telegram=True,
                    )
                except Exception:
                    pass

    def _request(
        self,
        api_id: str,
        path: str,
        body: dict,
        retry_on_401: bool = True,
        related_code: str = "",
    ) -> dict:
        """Central REST API caller with rate limit, token refresh, and tracker."""
        if not self._alive:
            return {"return_code": -1, "return_msg": "Provider shut down"}

        # Circuit breaker: halt 중이면 즉시 에러 반환 (300ms spam 방지)
        if self._cb_halt_until > 0 and time.time() < self._cb_halt_until:
            remain = int(self._cb_halt_until - time.time())
            return {
                "return_code": -1,
                "return_msg": f"Circuit breaker halted ({remain}s remaining) — 8005 consecutive",
            }

        # Rate limit
        elapsed = time.time() - self._last_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)

        headers = self._token_mgr.auth_headers()
        headers["api-id"] = api_id

        url = f"{self._base_url}{path}"
        self._last_request_time = time.time()

        # Tracker: start
        req_id = api_tracker.record_request_start(path, api_id, related_code=related_code)
        t0 = time.time()

        try:
            resp = requests.post(url, json=body, headers=headers, timeout=15)
            latency = (time.time() - t0) * 1000

            if resp.status_code >= 500:
                logger.error(f"[REST] {api_id} HTTP {resp.status_code}")
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=f"HTTP {resp.status_code}")
                return {"return_code": -1, "return_msg": f"HTTP {resp.status_code}"}

            data = resp.json()

            # Raw response 로깅 (P5 — 포맷 변경 진단용, DEBUG 레벨)
            logger.debug(
                f"[REST_RAW] {api_id} rc={data.get('return_code')} "
                f"msg={str(data.get('return_msg', ''))[:120]}"
            )

            # 8005 직접 감지 (return_code=1 외 케이스 커버)
            if self._is_8005(data):
                msg = str(data.get("return_msg", ""))
                self._cb_note_8005()
                if retry_on_401:
                    logger.warning(f"[REST] 8005 detected, refreshing token...")
                    self._token_mgr.invalidate()
                    api_tracker.record_request_end(
                        req_id, status="retry", latency_ms=latency,
                        error="8005_token_invalid", retry_count=1)
                    return self._request(
                        api_id, path, body, retry_on_401=False, related_code=related_code
                    )

            rc = data.get("return_code", -1)
            if rc not in (0, None):
                logger.warning(
                    f"[REST] {api_id} rc={rc} "
                    f"msg={data.get('return_msg', '')[:80]}"
                )
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=data.get("return_msg", "")[:200])
            else:
                api_tracker.record_request_end(
                    req_id, status="ok", http_status=resp.status_code, latency_ms=latency)
                # 성공 응답 → circuit breaker 리셋
                self._cb_note_success()

            return data

        except requests.Timeout:
            latency = (time.time() - t0) * 1000
            logger.error(f"[REST_TIMEOUT] {api_id} {path}")
            api_tracker.record_request_end(
                req_id, status="timeout", latency_ms=latency, error="HTTP timeout")
            return {"return_code": -1, "return_msg": "HTTP timeout"}
        except Exception as e:
            latency = (time.time() - t0) * 1000
            logger.error(f"[REST_ERROR] {api_id}: {e}")
            api_tracker.record_request_end(
                req_id, status="error", latency_ms=latency, error=str(e)[:200])
            return {"return_code": -1, "return_msg": str(e)}

    # ── Paginated Request (연속조회) ────────────────────────────

    class SnapshotStatus(enum.Enum):
        """Paginated batch 결과 상태."""
        COMPLETE = "COMPLETE"    # 모든 페이지 성공
        PARTIAL = "PARTIAL"      # 일부 페이지 실패 (부분 데이터)
        FAILED = "FAILED"        # 첫 페이지 실패 (데이터 없음)

    class SnapshotConsistency(enum.Enum):
        """Snapshot 기준시점 일관성."""
        CLEAN = "CLEAN"          # batch 중 WS 이벤트 개입 없음
        DEGRADED = "DEGRADED"    # batch 중 WS 00/04 이벤트 개입 감지

    @dataclass
    class PaginatedResult:
        """_request_all() 반환 객체. snapshot 메타데이터 포함."""
        data: Dict = field(default_factory=dict)
        snapshot_ts: float = 0.0
        batch_end_ts: float = 0.0
        request_batch_id: str = ""
        pages_fetched: int = 0
        total_rows: int = 0
        elapsed_ms: float = 0.0
        ok: bool = False
        status: str = "FAILED"              # COMPLETE | PARTIAL | FAILED
        consistency: str = "CLEAN"           # CLEAN | DEGRADED
        ws_events_during_batch: int = 0      # batch 중 수신된 WS 00/04 이벤트 수

    def _request_all(
        self,
        api_id: str,
        path: str,
        body: dict,
        *,
        list_key: str,
        max_pages: int = 100,
        related_code: str = "",
    ) -> "KiwoomRestProvider.PaginatedResult":
        """연속조회(cont-yn/next-key) 자동 처리 wrapper.

        Args:
            list_key: JSON 응답에서 리스트 데이터가 담긴 키 (예: "acnt_evlt_remn_indv_tot", "oso")
            max_pages: 무한 루프 방지 최대 페이지 수
        Returns:
            PaginatedResult with merged list data + snapshot metadata
        """
        batch_id = uuid.uuid4().hex[:12]
        snapshot_ts = time.time()
        t0 = snapshot_ts

        # Batch consistency: WS 이벤트 카운터 초기화 + batch 활성화
        with self._batch_lock:
            self._batch_active = True
            self._batch_ws_event_count = 0

        merged_list: List[Dict] = []
        first_page_data: Dict = {}
        summary_fields: Dict[str, Any] = {}  # list_key 외 scalar 필드 (first page 기준)
        next_key: Optional[str] = None
        page = 0
        expected_pages_complete = True  # 모든 예상 페이지를 받았는지

        while page < max_pages:
            if not self._alive:
                expected_pages_complete = False
                break

            # Rate limit
            elapsed = time.time() - self._last_request_time
            if elapsed < _MIN_REQUEST_INTERVAL:
                time.sleep(_MIN_REQUEST_INTERVAL - elapsed)

            headers = self._token_mgr.auth_headers()
            headers["api-id"] = api_id
            if next_key:
                headers["cont-yn"] = "Y"
                headers["next-key"] = next_key

            url = f"{self._base_url}{path}"
            self._last_request_time = time.time()

            req_id = api_tracker.record_request_start(
                path, api_id, related_code=related_code,
            )
            pt0 = time.time()

            try:
                resp = requests.post(url, json=body, headers=headers, timeout=15)
                latency = (time.time() - pt0) * 1000
            except requests.Timeout:
                latency = (time.time() - pt0) * 1000
                logger.error(f"[REST_PAGE] {api_id} page={page} TIMEOUT")
                api_tracker.record_request_end(
                    req_id, status="timeout", latency_ms=latency, error="HTTP timeout")
                expected_pages_complete = False
                break
            except Exception as e:
                latency = (time.time() - pt0) * 1000
                logger.error(f"[REST_PAGE] {api_id} page={page} ERROR: {e}")
                api_tracker.record_request_end(
                    req_id, status="error", latency_ms=latency, error=str(e)[:200])
                expected_pages_complete = False
                break

            if resp.status_code >= 500:
                logger.error(f"[REST_PAGE] {api_id} page={page} HTTP {resp.status_code}")
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=f"HTTP {resp.status_code}")
                expected_pages_complete = False
                break

            data = resp.json()

            # Token expired → refresh once, restart this page
            if data.get("return_code") == 1 and page == 0:
                msg = data.get("return_msg", "")
                if "토큰" in msg or "token" in msg.lower() or "401" in msg:
                    logger.warning(f"[REST_PAGE] Token expired on page 0, refreshing")
                    self._token_mgr.invalidate()
                    api_tracker.record_request_end(
                        req_id, status="retry", latency_ms=latency, error="token_expired")
                    continue  # retry same page with new token

            rc = data.get("return_code", -1)
            if rc not in (0, None):
                logger.warning(f"[REST_PAGE] {api_id} page={page} rc={rc} msg={data.get('return_msg', '')[:80]}")
                api_tracker.record_request_end(
                    req_id, status="error", http_status=resp.status_code, latency_ms=latency,
                    error=data.get("return_msg", "")[:200])
                if page == 0:
                    # 첫 페이지 실패 → 전체 실패 (batch 해제 필수)
                    with self._batch_lock:
                        self._batch_active = False
                    return self.PaginatedResult(
                        data=data, snapshot_ts=snapshot_ts, request_batch_id=batch_id,
                        pages_fetched=0, total_rows=0,
                        elapsed_ms=(time.time() - t0) * 1000, ok=False,
                        status="FAILED",
                    )
                expected_pages_complete = False
                break  # 후속 페이지 실패 → 지금까지 수집한 것으로 진행

            api_tracker.record_request_end(
                req_id, status="ok", http_status=resp.status_code, latency_ms=latency)

            # 첫 페이지: summary scalar 필드 보존
            if page == 0:
                first_page_data = data
                for k, v in data.items():
                    if k != list_key and k not in ("return_code", "return_msg"):
                        summary_fields[k] = v

            # 후속 페이지: summary 필드 변화 감지
            if page > 0:
                for k, v in data.items():
                    if k != list_key and k not in ("return_code", "return_msg"):
                        if k in summary_fields and summary_fields[k] != v:
                            logger.warning(
                                f"[REST_PAGE_WARNING] {api_id} summary field '{k}' changed: "
                                f"page0={summary_fields[k]} → page{page}={v}"
                            )

            # list 데이터 merge (append)
            page_rows = data.get(list_key, [])
            merged_list.extend(page_rows)

            row_count = len(page_rows)
            logger.info(
                f"[REST_PAGE] {api_id} batch={batch_id} page={page} "
                f"rows={row_count} total={len(merged_list)} "
                f"next_key={resp.headers.get('next-key', 'N/A')!r} "
                f"elapsed={latency:.0f}ms"
            )

            page += 1

            # 연속조회 판단: Response Header의 cont-yn
            resp_cont = resp.headers.get("cont-yn", "N")
            resp_next_key = resp.headers.get("next-key", "")

            if resp_cont == "Y" and resp_next_key:
                next_key = resp_next_key
            else:
                break

        # Batch 종료: WS 이벤트 카운터 수확 + batch 비활성화
        batch_end_ts = time.time()
        total_elapsed = (batch_end_ts - t0) * 1000

        with self._batch_lock:
            ws_events = self._batch_ws_event_count
            self._batch_active = False

        # ── Status 판정 ──
        has_data = page > 0
        if not has_data:
            status = self.SnapshotStatus.FAILED.value
        elif not expected_pages_complete:
            status = self.SnapshotStatus.PARTIAL.value
        else:
            status = self.SnapshotStatus.COMPLETE.value

        # ── Consistency 판정 ──
        if ws_events > 0:
            consistency = self.SnapshotConsistency.DEGRADED.value
            logger.warning(
                f"[SNAPSHOT_DEGRADED] {api_id} batch={batch_id} "
                f"ws_events={ws_events} during {total_elapsed:.0f}ms batch window"
            )
        else:
            consistency = self.SnapshotConsistency.CLEAN.value

        # 최종 결과 구성: first page summary + merged list
        result_data = dict(first_page_data) if first_page_data else {}
        result_data[list_key] = merged_list
        if has_data:
            result_data["return_code"] = 0

        # Status-specific 로그
        if status == self.SnapshotStatus.PARTIAL.value:
            logger.warning(
                f"[REST_BATCH_PARTIAL] {api_id} batch={batch_id} "
                f"pages={page} total_rows={len(merged_list)} "
                f"(some pages failed - data incomplete)"
            )

        logger.info(
            f"[REST_BATCH_DONE] {api_id} batch={batch_id} "
            f"pages={page} total_rows={len(merged_list)} "
            f"status={status} consistency={consistency} "
            f"ws_events={ws_events} elapsed={total_elapsed:.0f}ms"
        )

        return self.PaginatedResult(
            data=result_data,
            snapshot_ts=snapshot_ts,
            batch_end_ts=batch_end_ts,
            request_batch_id=batch_id,
            pages_fetched=page,
            total_rows=len(merged_list),
            elapsed_ms=total_elapsed,
            ok=has_data,
            status=status,
            consistency=consistency,
            ws_events_during_batch=ws_events,
        )

    # ── Lifecycle ─────────────────────────────────────────────

    def shutdown(self) -> None:
        self._alive = False
        self._real_data_callback = None
        self._micro_callback = None
        if self._ws:
            self._ws.stop()
            self._ws = None
            self._ws_started = False
        logger.info("[REST_PROVIDER] shutdown")

    @property
    def alive(self) -> bool:
        return self._alive

    def is_connected(self) -> bool:
        try:
            tok = self._token_mgr.token
            return bool(tok)
        except Exception:
            return False

    @property
    def ws_down(self) -> bool:
        """True if WebSocket permanently failed (MAX_RECONNECT exceeded)."""
        return self._ws is not None and self._ws.permanently_down

    def ensure_connected(self) -> bool:
        if not self._alive:
            return False
        try:
            self._token_mgr.invalidate()
            tok = self._token_mgr.token
            return bool(tok)
        except Exception:
            return False

    # ── Identity ──────────────────────────────────────────────

    @property
    def server_type(self) -> str:
        return self._server_type_val

    # ── Stock Information ─────────────────────────────────────

    def get_stock_info(self, code: str) -> dict:
        data = self._request("ka10001", "/api/dostk/stkinfo", {"stk_cd": code})
        if data.get("return_code") != 0:
            return {"name": "", "sector": "", "market": "", "market_cap": 0, "listed_shares": 0}

        name = _decode_name(data.get("stk_nm", ""))

        return {
            "name": name,
            "sector": self._sector_map.get(code, ""),
            "market": data.get("mrkt_tp", ""),
            "market_cap": int(data.get("mac", "0") or "0"),
            "listed_shares": int(data.get("flo_stk", "0") or "0"),
        }

    def get_current_price(self, code: str) -> float:
        data = self._request("ka10004", "/api/dostk/mrkcond", {"stk_cd": code})
        if data.get("return_code") != 0:
            return 0.0
        # Use best bid/ask midpoint or first available price
        buy1 = data.get("buy_fpr_bid", "0")
        sell1 = data.get("sel_fpr_bid", "0")
        buy_p = abs(int(buy1 or "0"))
        sell_p = abs(int(sell1 or "0"))
        if buy_p and sell_p:
            return float((buy_p + sell_p) // 2)
        return float(buy_p or sell_p)

    # ── Account Queries ───────────────────────────────────────

    def query_account_holdings(self) -> List[Dict]:
        result = self._request_all(
            "kt00018",
            "/api/dostk/acnt",
            {"qry_tp": "2", "dmst_stex_tp": "NXT"},  # 2=개별
            list_key="acnt_evlt_remn_indv_tot",
        )
        if not result.ok:
            return []

        holdings = []
        for item in result.data.get("acnt_evlt_remn_indv_tot", []):
            code = item.get("stk_cd", "").replace("A", "")
            name = _decode_name(item.get("stk_nm", ""))

            holdings.append({
                "code": code,
                "name": name,
                "qty": int(item.get("rmnd_qty", "0")),
                "quantity": int(item.get("rmnd_qty", "0")),
                "avg_price": int(item.get("pur_pric", "0")),
                "cur_price": int(item.get("cur_prc", "0")),
                "pnl": int(item.get("evltv_prft", "0")),
                "_snapshot_ts": result.snapshot_ts,
                "_batch_id": result.request_batch_id,
            })

        logger.info(
            f"[REST_PAGE_MERGE] holdings: {len(holdings)} items, "
            f"pages={result.pages_fetched}, batch={result.request_batch_id}"
        )
        return holdings

    def query_account_summary(self) -> Dict:
        result = self._request_all(
            "kt00018",
            "/api/dostk/acnt",
            {"qry_tp": "1", "dmst_stex_tp": "NXT"},  # 1=합산
            list_key="acnt_evlt_remn_indv_tot",
        )
        if not result.ok:
            msg = result.data.get("return_msg", "query failed") if result.data else "query failed"
            return {"error": msg, "holdings_reliable": False}

        data = result.data

        # Summary 필드는 first page 기준 (summary_fields 보존됨)
        tot_eval = int(data.get("tot_evlt_amt", "0"))
        prsm_asset = int(data.get("prsm_dpst_aset_amt", "0"))
        available_cash = prsm_asset - tot_eval

        holdings = []
        prev_eval_amt_total = 0
        for item in data.get("acnt_evlt_remn_indv_tot", []):
            code = item.get("stk_cd", "").replace("A", "")
            name = _decode_name(item.get("stk_nm", ""))
            qty = int(item.get("rmnd_qty", "0"))
            pred_close = abs(int(item.get("pred_close_pric", "0") or "0"))
            prev_eval_amt_total += pred_close * qty
            holdings.append({
                "code": code,
                "name": name,
                "qty": qty,
                "avg_price": int(item.get("pur_pric", "0")),
                "cur_price": int(item.get("cur_prc", "0")),
                "prev_close_price": pred_close,
                "eval_amt": int(item.get("evlt_amt", "0")),
                "pnl": int(item.get("evltv_prft", "0")),
                "pnl_rate": item.get("prft_rt", "0"),
            })

        # PARTIAL → holdings_reliable=False (RECON이 truth로 승격 금지)
        is_partial = result.status == self.SnapshotStatus.PARTIAL.value
        is_degraded = result.consistency == self.SnapshotConsistency.DEGRADED.value
        holdings_reliable = not is_partial

        if is_partial:
            logger.warning(
                f"[REST_BATCH_UNSAFE] account_summary PARTIAL - "
                f"holdings_reliable=False, RECON truth promotion blocked"
            )

        logger.info(
            f"[REST_PAGE_MERGE] account_summary: {len(holdings)} holdings, "
            f"pages={result.pages_fetched}, status={result.status}, "
            f"consistency={result.consistency}, batch={result.request_batch_id}"
        )

        api_tracker.update_freshness("account_summary")
        api_tracker.update_freshness("holdings")
        return {
            "추정예탁자산": prsm_asset,
            "총매입금액": int(data.get("tot_pur_amt", "0")),
            "총평가금액": tot_eval,
            "총평가손익금액": int(data.get("tot_evlt_pl", "0")),
            "전일평가금액": prev_eval_amt_total,
            "holdings": holdings,
            "available_cash": available_cash,
            "error": None,
            "holdings_reliable": holdings_reliable,
            "_snapshot_ts": result.snapshot_ts,
            "_batch_end_ts": result.batch_end_ts,
            "_batch_id": result.request_batch_id,
            "_pages_fetched": result.pages_fetched,
            "_status": result.status,
            "_consistency": result.consistency,
            "_ws_events_during_batch": result.ws_events_during_batch,
        }

    def query_minute_chart(self, code: str, tic_scope: str = "1",
                           base_dt: str = "", max_bars: int = 90) -> Dict:
        """주식분봉차트조회요청 (ka10080) — 단일 종목 분봉 데이터.

        Args:
            code: 종목코드 (6자리)
            tic_scope: 틱범위 "1"/"3"/"5"/"10"/"15"/"30"/"45"/"60" (분)
            base_dt: 기준일자 YYYYMMDD (빈 값이면 당일)
            max_bars: 최근 N개 bar만 반환 (default 90개 = 1분봉 1.5시간)

        Returns:
            {
              "code": "005930",
              "tic_scope": "1",
              "bars": [
                {"time": "093000", "open":..., "high":..., "low":..., "close":..., "volume":...},
                ...
              ],
              "error": None or str,
            }
        """
        body = {
            "stk_cd": code,
            "tic_scope": str(tic_scope),
            "upd_stkpc_tp": "1",  # 수정주가 적용
        }
        if base_dt:
            body["base_dt"] = base_dt

        try:
            data = self._request("ka10080", "/api/dostk/chart", body)
        except Exception as e:
            return {"code": code, "tic_scope": tic_scope, "bars": [], "error": str(e)}

        if data.get("return_code") != 0:
            return {
                "code": code, "tic_scope": tic_scope, "bars": [],
                "error": data.get("return_msg", "unknown"),
            }

        raw_bars = data.get("stk_min_pole_chart_qry", []) or []
        # Kiwoom 응답 순서가 일관되지 않을 수 있어 cntr_tm 기준으로 강제 오름차순 정렬
        # (x축: 왼쪽=오래된, 오른쪽=최신)
        try:
            raw_bars = sorted(raw_bars, key=lambda b: str(b.get("cntr_tm", "")))
        except Exception:
            pass
        # max_bars 적용 (최근 N개 — 정렬 후 끝에서 자름)
        if len(raw_bars) > max_bars:
            raw_bars = raw_bars[-max_bars:]

        bars = []
        for b in raw_bars:
            # Kiwoom은 가격 앞에 +/-/공백 부호가 붙기도 함 → abs(int)
            def _ab(v):
                try: return abs(int(str(v).replace("+", "").replace("-", "").strip() or "0"))
                except Exception: return 0
            cntr_tm = str(b.get("cntr_tm", ""))  # YYYYMMDDHHMMSS
            # time 부분만 추출 (HHMMSS)
            t = cntr_tm[8:] if len(cntr_tm) >= 14 else cntr_tm
            bars.append({
                "time": t,
                "datetime": cntr_tm,
                "open": _ab(b.get("open_pric")),
                "high": _ab(b.get("high_pric")),
                "low": _ab(b.get("low_pric")),
                "close": _ab(b.get("cur_prc")),
                "volume": _ab(b.get("trde_qty")),
            })

        return {
            "code": code,
            "tic_scope": tic_scope,
            "bars": bars,
            "error": None,
        }

    def query_sellable_qty(self, code: str) -> Dict:
        holdings = self.query_account_holdings()
        for h in holdings:
            if h["code"] == code:
                return {
                    "code": code,
                    "hold_qty": h["qty"],
                    "sellable_qty": h["qty"],  # REST에서는 trde_able_qty 사용 가능
                    "source": "rest_kt00018",
                    "error": None,
                }
        return {
            "code": code,
            "hold_qty": 0,
            "sellable_qty": 0,
            "source": "rest_kt00018",
            "error": "not found in holdings",
        }

    # ── Order Execution ───────────────────────────────────────

    def send_order(
        self,
        code: str,
        side: str,
        quantity: int,
        price: int = 0,
        hoga_type: str = "03",
    ) -> Dict:
        api_id = "kt10000" if side.upper() == "BUY" else "kt10001"

        # Map hoga_type: "03"=시장가 → trde_tp "3"
        trde_tp_map = {"01": "0", "03": "3", "00": "0"}
        trde_tp = trde_tp_map.get(hoga_type, "3")

        body: dict = {
            "dmst_stex_tp": "SOR",  # 주문은 SOR (최적 집행)
            "stk_cd": code,
            "ord_qty": str(quantity),
            "trde_tp": trde_tp,
        }
        if price > 0 and trde_tp == "0":
            body["ord_uv"] = str(price)

        data = self._request(api_id, "/api/dostk/ordr", body)

        if data.get("return_code") == 0:
            order_no = data.get("ord_no", "")
            logger.info(
                f"[REST_ORDER] {side} {code} qty={quantity} order_no={order_no}"
            )

            # Fill slot 등록 (WS 00 이벤트에서 매칭)
            slot = _FillSlot(
                order_no=order_no, code=code, side=side.upper(),
                requested_qty=quantity,
            )
            with self._fill_lock:
                self._fill_slots[order_no] = slot

            # Wait for WebSocket fill (if WS connected, max 30s)
            exec_price = 0
            exec_qty = 0
            if self._ws_started and self._ws and self._ws.connected:
                filled = slot.filled.wait(timeout=30)
                exec_price = slot.exec_price
                exec_qty = slot.exec_qty
                if filled and exec_qty > 0:
                    logger.info(
                        f"[REST_ORDER_FILLED] {order_no}: {exec_qty}@{exec_price}"
                    )
                else:
                    logger.warning(
                        f"[REST_ORDER_TIMEOUT] {order_no}: no fill in 30s "
                        f"(partial={exec_qty}/{quantity})"
                    )

            # Slot 정리 (ghost callback이 나중에 쓸 수 있으므로 즉시 삭제하지 않음)
            # 60초 이상 된 slot은 _cleanup_stale_slots()에서 제거
            return {
                "order_no": order_no,
                "exec_price": exec_price,
                "exec_qty": exec_qty,
                "error": None,
                "status": "FILLED" if exec_qty >= quantity else (
                    "PARTIAL" if exec_qty > 0 else "SUBMITTED"),
            }
        else:
            error_msg = data.get("return_msg", "order failed")
            logger.error(f"[REST_ORDER_FAIL] {side} {code}: {error_msg}")
            return {
                "order_no": "",
                "exec_price": 0,
                "exec_qty": 0,
                "error": error_msg,
            }

    def query_open_orders(self) -> Optional[List[Dict]]:
        result = self._request_all(
            "ka10075",
            "/api/dostk/acnt",
            {
                "qry_tp": "0",
                "all_stk_tp": "0",
                "sell_tp": "0",
                "sort_tp": "1",
                "trde_tp": "0",
                "stex_tp": "SOR",
                "dmst_stex_tp": "NXT",
            },
            list_key="oso",
        )
        if not result.ok:
            return None

        # PARTIAL → 미체결 누락 위험 → None 반환으로 안전장치 유지
        # (상위: None이면 opt10075_fail_streak 증가 → BLOCKED 전환)
        if result.status == self.SnapshotStatus.PARTIAL.value:
            logger.warning(
                f"[REST_BATCH_UNSAFE] open_orders PARTIAL - "
                f"returning None to trigger safety guard. "
                f"pages={result.pages_fetched}, batch={result.request_batch_id}"
            )
            return None

        orders = []
        _field_check_logged = False
        for item in result.data.get("oso", []):
            ord_qty = int(item.get("ord_qty", "0"))
            cntr_qty = int(item.get("cntr_qty", "0"))
            # noncntr_qty: 문서 필드 목록에 있으나 JSON 예시에 없음
            # fallback: oso_qty 또는 ord_qty - cntr_qty
            raw_noncntr = item.get("noncntr_qty")
            has_noncntr = raw_noncntr is not None and raw_noncntr != ""
            if has_noncntr:
                remaining = int(raw_noncntr)
            else:
                oso_qty_raw = item.get("oso_qty")
                if oso_qty_raw is not None and oso_qty_raw != "":
                    remaining = int(oso_qty_raw)
                else:
                    remaining = max(0, ord_qty - cntr_qty)

            if not _field_check_logged:
                logger.info(
                    f"[OPEN_ORDERS_FIELD_CHECK] has_noncntr_qty={has_noncntr} "
                    f"oso_qty={item.get('oso_qty', 'N/A')} "
                    f"cntr_qty={cntr_qty} computed_open_qty={remaining}"
                )
                _field_check_logged = True

            orders.append({
                "order_no": item.get("ord_no", ""),
                "code": item.get("stk_cd", "").replace("A", ""),
                "side": "SELL" if item.get("sell_tp", "") == "1" else "BUY",
                "qty": ord_qty,
                "filled_qty": cntr_qty,
                "remaining": remaining,
                "order_time": item.get("ord_tm", ""),
                "status_raw": item.get("ord_stt", ""),
                "_snapshot_ts": result.snapshot_ts,
                "_batch_id": result.request_batch_id,
            })

        logger.info(
            f"[REST_PAGE_MERGE] open_orders: {len(orders)} items, "
            f"pages={result.pages_fetched}, status={result.status}, "
            f"consistency={result.consistency}, batch={result.request_batch_id}"
        )
        api_tracker.update_freshness("open_orders")
        return orders

    def cancel_order(
        self, code: str, order_no: str, qty: int, side: str = "BUY"
    ) -> Dict:
        body = {
            "dmst_stex_tp": "NXT",
            "stk_cd": code,
            "orig_ord_no": order_no,
            "cncl_qty": str(qty) if qty > 0 else "0",
        }
        data = self._request("kt10003", "/api/dostk/ordr", body)
        if data.get("return_code") == 0:
            logger.info(f"[REST_CANCEL] {code} order_no={order_no} → OK")
            return {"ok": True, "error": None}
        else:
            return {"ok": False, "error": data.get("return_msg", "cancel failed")}

    def cancel_all_open_orders(self) -> Optional[int]:
        orders = self.query_open_orders()
        if orders is None:
            return None
        count = 0
        for o in orders:
            result = self.cancel_order(
                o["code"], o["order_no"], o["remaining"], o["side"]
            )
            if result["ok"]:
                count += 1
        return count

    # ── Ghost Order Management ────────────────────────────────

    def set_ghost_fill_callback(self, callback: Optional[Callable]) -> None:
        self._ghost_fill_callback = callback

    def get_ghost_orders(self) -> List[Dict]:
        return list(self._ghost_orders_list)

    def cleanup_stale_fill_slots(self, max_age_s: float = 120.0) -> int:
        """120초 이상 된 fill slot 정리. 반환: 제거 건수."""
        now = time.time()
        removed = 0
        with self._fill_lock:
            stale_keys = [
                k for k, s in self._fill_slots.items()
                if (now - s.created_at) > max_age_s
            ]
            for k in stale_keys:
                del self._fill_slots[k]
                removed += 1
            # dedup set도 주기적 정리 (1000건 초과 시 전체 리셋)
            if len(self._fill_dedup) > 1000:
                self._fill_dedup.clear()
        if removed:
            logger.info(f"[FILL_CLEANUP] Removed {removed} stale fill slots")
        return removed

    def clear_ghost_orders(self) -> None:
        self._ghost_orders_list.clear()

    # ── WebSocket Helpers ────────────────────────────────────

    def _ensure_ws(self) -> KiwoomWebSocket:
        """Lazy-init and start WebSocket client."""
        if not self._ws:
            self._ws = KiwoomWebSocket(
                token=self._token_mgr.token,
                server_type=self._server_type_val,
                token_refresher=lambda: self._token_mgr.token,
            )
            self._ws.set_on_price_tick(self._on_ws_price)
            self._ws.set_on_order_exec(self._on_ws_order)
            self._ws.set_on_balance_update(self._on_ws_balance)
        if not self._ws_started:
            self._ws.start()
            self._ws_started = True
            # Wait for connection + auth settle (2s delay inside WS)
            import time as _t
            for _ in range(16):  # up to 4 seconds
                if self._ws.connected and self._ws.authenticated:
                    break
                _t.sleep(0.25)
            if self._ws.authenticated:
                logger.info("[WS] Connected and ready")
            elif self._ws.connected:
                logger.warning("[WS] Connected, auth settling...")
            else:
                logger.warning("[WS] Connection pending")
        return self._ws

    def _on_ws_price(self, code: str, values: dict) -> None:
        """Handle Type 0B (주식체결) WebSocket message."""
        try:
            price_raw = values.get("10", "0")
            price = abs(int(price_raw.replace("+", "").replace("-", "") or "0"))
            volume_raw = values.get("13", "0")
            volume = abs(int(volume_raw.replace("+", "").replace("-", "") or "0"))

            if self._real_data_callback and price > 0:
                self._real_data_callback(code, float(price), volume)

            if self._micro_callback and price > 0:
                fid_data = {
                    "timestamp": values.get("20", ""),
                    "price": price,
                    "best_ask": abs(int(values.get("27", "0").replace("+", "").replace("-", "") or "0")),
                    "best_bid": abs(int(values.get("28", "0").replace("+", "").replace("-", "") or "0")),
                    "ask_qty_1": abs(int(values.get("1030", "0") or "0")),
                    "bid_qty_1": abs(int(values.get("1031", "0") or "0")),
                    "total_ask": 0,  # not in 0B, available in 0D
                    "total_bid": 0,
                    "net_bid": 0,
                    "volume": volume,
                }
                self._micro_callback(code, fid_data)

        except Exception as e:
            logger.error(f"[WS_PRICE_ERR] {code}: {e}")

    def _on_ws_order(self, values: dict) -> None:
        """Handle Type 00 (주문체결) WebSocket message."""
        try:
            # Batch consistency: batch 진행 중 WS 00 이벤트 카운트
            with self._batch_lock:
                if self._batch_active:
                    self._batch_ws_event_count += 1

            order_no = values.get("9203", "")
            code = values.get("9001", "").replace("A", "")
            exec_qty = abs(int(values.get("911", "0") or "0"))
            exec_price = abs(int(values.get("910", "0") or "0"))
            order_status = values.get("913", "")
            side_raw = values.get("907", "")
            side = "BUY" if side_raw == "1" else "SELL"

            logger.info(
                f"[WS_ORDER] {side} {code} order={order_no} "
                f"status={order_status} exec={exec_qty}@{exec_price}"
            )

            # Fill queue 매칭 (다중 주문 동시 지원)
            matched_slot = False
            if order_no and exec_qty > 0 and exec_price > 0:
                with self._fill_lock:
                    slot = self._fill_slots.get(order_no)
                    if slot:
                        # Dedup: (order_no, cumulative_qty) 기준
                        new_cumulative = slot.exec_qty + exec_qty
                        dedup_key = (order_no, new_cumulative)
                        if dedup_key in self._fill_dedup:
                            logger.warning(
                                f"[WS_FILL_DEDUP] {order_no} cumulative={new_cumulative} "
                                f"already processed, skipping"
                            )
                        else:
                            self._fill_dedup.add(dedup_key)
                            slot.exec_qty = new_cumulative
                            slot.exec_price = exec_price  # 최신 체결가
                            # Clamp: requested 초과 방지
                            if slot.exec_qty > slot.requested_qty:
                                logger.warning(
                                    f"[WS_FILL_CLAMP] {order_no}: "
                                    f"cumulative={slot.exec_qty} > requested={slot.requested_qty}, "
                                    f"clamping to {slot.requested_qty}"
                                )
                                slot.exec_qty = slot.requested_qty
                            # 전부 체결 or 서버에서 체결 확인 시 signal
                            if (slot.exec_qty >= slot.requested_qty
                                    or order_status in ("체결", "확인")):
                                slot.filled.set()
                            matched_slot = True
                            logger.info(
                                f"[WS_FILL_MATCH] {order_no}: "
                                f"+{exec_qty} -> {slot.exec_qty}/{slot.requested_qty}"
                            )

            # Ghost fill callback (slot 유무 관계없이 항상 전달)
            if self._ghost_fill_callback and order_no:
                self._ghost_fill_callback({
                    "order_no": order_no,
                    "code": code,
                    "side": side,
                    "exec_qty": exec_qty,
                    "exec_price": exec_price,
                    "status": order_status,
                    "_matched_slot": matched_slot,
                })

        except Exception as e:
            logger.error(f"[WS_ORDER_ERR] {e}")

    def _on_ws_balance(self, values: dict) -> None:
        """Handle Type 04 (잔고변동) WebSocket message.

        Portfolio를 직접 수정하지 않고 이벤트 버퍼에 적재.
        상위에서 drain_balance_events()로 소비.
        """
        # Batch consistency: batch 진행 중 WS 04 이벤트 카운트
        with self._batch_lock:
            if self._batch_active:
                self._batch_ws_event_count += 1

        try:
            code = values.get("9001", "").replace("A", "")
            qty = abs(int(values.get("930", "0") or "0"))
            avg_price = abs(int(values.get("931", "0") or "0"))
            total_cost = abs(int(values.get("932", "0") or "0"))
            orderable_qty = abs(int(values.get("933", "0") or "0"))
            cur_price_raw = values.get("10", "0")
            cur_price = abs(int(cur_price_raw.replace("+", "").replace("-", "") or "0"))

            event = {
                "code": code,
                "qty": qty,
                "avg_price": avg_price,
                "total_cost": total_cost,
                "orderable_qty": orderable_qty,
                "cur_price": cur_price,
                "event_ts": time.time(),
            }

            with self._balance_lock:
                self._balance_seq += 1
                event["_seq"] = self._balance_seq
                self._balance_events.append(event)

            logger.info(
                f"[WS_BALANCE] {code} qty={qty} avg={avg_price} "
                f"cur={cur_price} seq={event['_seq']}"
            )
        except Exception as e:
            logger.error(f"[WS_BALANCE_ERR] {e}")

    def drain_balance_events(self) -> List[Dict]:
        """WS 04 이벤트 버퍼에서 꺼내기. 호출 후 버퍼 비워짐."""
        with self._balance_lock:
            events = list(self._balance_events)
            self._balance_events.clear()
        return events

    @property
    def balance_event_count(self) -> int:
        """현재 버퍼에 쌓인 잔고 이벤트 수."""
        with self._balance_lock:
            return len(self._balance_events)

    # ── Real-time Data ────────────────────────────────────────

    def register_real(self, codes: List[str], fids: str = "10;27") -> None:
        ws = self._ensure_ws()
        ws.subscribe(codes, "0B", owner_key="provider")
        # Also subscribe to order execution for this session
        ws.subscribe([""], "00", owner_key="provider")
        logger.info(f"[REST] register_real: {len(codes)} codes via WebSocket")

    def unregister_real(self) -> None:
        if self._ws:
            self._ws.unsubscribe_all()
        logger.info("[REST] unregister_real: WebSocket unsubscribed")

    def register_real_append(
        self, codes: List[str], fids: str = "10;27", screen: Optional[str] = None
    ) -> int:
        ws = self._ensure_ws()
        ws.subscribe(codes, "0B", owner_key="provider")
        return len(codes)

    def unregister_real_screen(self, screen: str) -> None:
        # WebSocket has no screen concept — log only
        logger.info(f"[REST] unregister_real_screen: {screen} (no-op in REST)")

    def set_real_data_callback(self, callback: Optional[Callable]) -> None:
        self._real_data_callback = callback

    def set_micro_callback(self, callback: Optional[Callable]) -> None:
        self._micro_callback = callback

    # ── Index Data ────────────────────────────────────────────

    def get_kospi_close(self, trade_date: str = "") -> float:
        data = self._request(
            "ka20001",
            "/api/dostk/sect",
            {"mrkt_tp": "0", "inds_cd": "001"},
        )
        if data.get("return_code") != 0:
            return 0.0
        raw = data.get("cur_prc", "0")
        val = float(raw.replace("+", "").replace("-", ""))
        return val

    def get_kosdaq_close(self, trade_date: str = "") -> float:
        data = self._request(
            "ka20001",
            "/api/dostk/sect",
            {"mrkt_tp": "1", "inds_cd": "101"},
        )
        if data.get("return_code") != 0:
            return 0.0
        raw = data.get("cur_prc", "0")
        val = float(raw.replace("+", "").replace("-", ""))
        return val

    # ── Theme APIs (ka90001, ka90002) ──────────────────────────────────────

    def get_theme_groups(self, date_range: int = 1) -> List[dict]:
        """ka90001 테마그룹조회 — 전체 테마 목록 + 등락률.

        Args:
            date_range: 기간 (1=당일, 5=5일, 20=20일 등)

        Returns:
            [{"code": "000001", "name": "2차전지", "count": 15,
              "change_pct": 2.3}, ...]
        """
        data = self._request(
            "ka90001", "/api/dostk/thme",
            {
                "qry_tp": "0",       # 전체 조회
                "date_tp": str(date_range),
                "flu_pl_amt_tp": "3", # 전체 (상승+하락)
                "stex_tp": "1",       # KRX
            },
            related_code="THEME",
        )
        if data.get("return_code") != 0:
            logger.warning(f"[THEME] ka90001 failed: {data.get('return_msg', '?')}")
            return []

        raw_list = data.get("thema_grp", [])
        results = []
        for t in raw_list:
            try:
                code = t.get("thema_grp_cd", "")
                name = t.get("thema_nm", "")
                count = int(t.get("stk_num", 0))
                flu_rt = float(str(t.get("flu_rt", "0")).replace("+", ""))
                results.append({
                    "code": code, "name": name, "count": count,
                    "change_pct": flu_rt,
                })
            except (ValueError, TypeError):
                continue
        return results

    def get_theme_stocks(self, theme_code: str, date_range: int = 1) -> dict:
        """ka90002 테마종목조회 — 특정 테마의 종목 상세.

        Returns:
            {"change_pct": 2.3, "period_pct": 5.1,
             "stocks": [{"code": "005930", "name": "삼성전자",
                         "price": 75000, "change_pct": 1.5}, ...]}
        """
        data = self._request(
            "ka90002", "/api/dostk/thme",
            {
                "thema_grp_cd": theme_code,
                "date_tp": str(date_range),
                "stex_tp": "1",
            },
            related_code="THEME",
        )
        if data.get("return_code") != 0:
            logger.warning(f"[THEME] ka90002 failed: {data.get('return_msg', '?')}")
            return {}

        flu_rt = float(str(data.get("flu_rt", "0")).replace("+", ""))
        dt_prft = float(str(data.get("dt_prft_rt", "0")).replace("+", ""))

        stocks = []
        for s in data.get("thema_comp_stk", []):
            try:
                stocks.append({
                    "code": s.get("stk_cd", ""),
                    "name": s.get("stk_nm", ""),
                    "price": abs(float(str(s.get("cur_prc", "0")).replace("+", "").replace(",", ""))),
                    "change_pct": float(str(s.get("flu_rt", "0")).replace("+", "")),
                })
            except (ValueError, TypeError):
                continue

        return {"change_pct": flu_rt, "period_pct": dt_prft, "stocks": stocks}

    def get_index_minute_bars(
        self,
        index_code: str = "001",
        trade_date: str = "",
        tick_range: int = 1,
    ) -> List[dict]:
        # Phase 0: stub — REST 업종분봉(ka20005) 구현은 Phase 1
        logger.info("[REST_STUB] get_index_minute_bars (Phase 1)")
        return []
