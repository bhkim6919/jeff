"""
test_pagination.py — _request_all() 연속조회 단위+통합 테스트
=============================================================
cont-yn / next-key loop의 정확성 + 방어 로직 검증.

Usage:
    cd kr
    ../.venv64/Scripts/python.exe -m pytest tests/test_pagination.py -v
    # or standalone:
    ../.venv64/Scripts/python.exe tests/test_pagination.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Mock Response Builder ─────────────────────────────────────

class MockResponse:
    """requests.post() 대체 응답 객체."""

    def __init__(
        self,
        body: dict,
        status_code: int = 200,
        cont_yn: str = "N",
        next_key: str = "",
    ) -> None:
        self._body = body
        self.status_code = status_code
        self.headers: Dict[str, str] = {}
        if cont_yn:
            self.headers["cont-yn"] = cont_yn
        if next_key:
            self.headers["next-key"] = next_key

    def json(self) -> dict:
        return self._body


def _make_holdings_page(
    codes: List[str],
    cont_yn: str = "N",
    next_key: str = "",
    summary_fields: Optional[dict] = None,
) -> MockResponse:
    """kt00018 응답 페이지 생성."""
    items = []
    for code in codes:
        items.append({
            "stk_cd": f"A{code}",
            "stk_nm": f"종목{code}",
            "rmnd_qty": "100",
            "pur_pric": "50000",
            "cur_prc": "55000",
            "evlt_amt": "5500000",
            "evltv_prft": "500000",
            "prft_rt": "+10.00",
        })

    body: dict = {
        "return_code": 0,
        "return_msg": "정상처리 되었습니다.",
        "acnt_evlt_remn_indv_tot": items,
    }
    if summary_fields:
        body.update(summary_fields)

    return MockResponse(body, cont_yn=cont_yn, next_key=next_key)


def _make_orders_page(
    count: int,
    start_no: int = 1,
    cont_yn: str = "N",
    next_key: str = "",
) -> MockResponse:
    """ka10075 미체결 응답 페이지 생성."""
    items = []
    for i in range(count):
        no = start_no + i
        items.append({
            "ord_no": f"{no:07d}",
            "stk_cd": f"A{100000 + no}",
            "sell_tp": "0",
            "ord_qty": "50",
            "cntr_qty": "0",
            "noncntr_qty": "50",
            "ord_tm": "093012",
            "ord_stt": "접수",
        })

    body = {
        "return_code": 0,
        "return_msg": "정상처리 되었습니다.",
        "oso": items,
    }
    return MockResponse(body, cont_yn=cont_yn, next_key=next_key)


# ── Test Helpers ──────────────────────────────────────────────

def _create_provider_no_connect():
    """실제 연결 없이 provider 인스턴스 생성."""
    with patch.dict("os.environ", {
        "KIWOOM_APP_KEY": "test_key",
        "KIWOOM_APP_SECRET": "test_secret",
        "KIWOOM_ACCOUNT": "1234-5678",
        "KIWOOM_API_URL": "https://mockapi.kiwoom.com",
    }):
        with patch("data.rest_token_manager.TokenManager") as MockTM:
            mock_tm = MockTM.return_value
            mock_tm.token = "fake_token"
            mock_tm.auth_headers.return_value = {
                "authorization": "Bearer fake_token",
                "content-type": "application/json;charset=UTF-8",
            }

            with patch("data.rest_provider.api_tracker") as mock_tracker:
                mock_tracker.record_request_start.return_value = "req_001"
                mock_tracker.record_request_end.return_value = None
                mock_tracker.set_server_info.return_value = None
                mock_tracker.update_freshness.return_value = None

                from data.rest_provider import KiwoomRestProvider
                provider = KiwoomRestProvider(server_type="MOCK")
                # Override rate limit for test speed
                provider._last_request_time = 0
                return provider


# ══════════════════════════════════════════════════════════════
# 단위 테스트: _request_all()
# ══════════════════════════════════════════════════════════════

class TestRequestAllUnit:
    """_request_all() 핵심 동작 검증."""

    def test_single_page_no_continuation(self):
        """cont-yn=N 단일 페이지 → 정상 종료, pages=1."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930", "000660"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is True
        assert result.pages_fetched == 1
        assert result.total_rows == 2
        assert len(result.data["acnt_evlt_remn_indv_tot"]) == 2
        assert result.request_batch_id  # non-empty
        assert result.snapshot_ts > 0

    def test_multi_page_continuation(self):
        """cont-yn=Y 다중 페이지 → 3페이지 merge 후 정상 종료."""
        provider = _create_provider_no_connect()

        page1_codes = [f"{10000 + i}" for i in range(10)]
        page2_codes = [f"{20000 + i}" for i in range(10)]
        page3_codes = [f"{30000 + i}" for i in range(5)]

        responses = [
            _make_holdings_page(page1_codes, cont_yn="Y", next_key="KEY_PAGE2"),
            _make_holdings_page(page2_codes, cont_yn="Y", next_key="KEY_PAGE3"),
            _make_holdings_page(page3_codes, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is True
        assert result.pages_fetched == 3
        assert result.total_rows == 25
        assert len(result.data["acnt_evlt_remn_indv_tot"]) == 25

    def test_max_pages_safety(self):
        """max_pages 초과 시 중단 (무한루프 방지)."""
        provider = _create_provider_no_connect()

        # 무한히 cont-yn=Y 반환하는 서버 시뮬레이션
        def infinite_pages(*args, **kwargs):
            return _make_holdings_page(["999999"], cont_yn="Y", next_key="NEXT")

        with patch("requests.post", side_effect=infinite_pages):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
                max_pages=5,
            )

        assert result.ok is True
        assert result.pages_fetched == 5  # 5에서 중단
        assert result.total_rows == 5

    def test_next_key_missing_stops(self):
        """cont-yn=Y인데 next-key가 빈 문자열이면 중단."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="Y", next_key=""),  # next_key 없음
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is True
        assert result.pages_fetched == 1  # 1페이지에서 중단

    def test_first_page_error(self):
        """첫 페이지 return_code != 0 → ok=False."""
        provider = _create_provider_no_connect()

        responses = [
            MockResponse({"return_code": -1, "return_msg": "서버 오류"}),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is False
        assert result.pages_fetched == 0

    def test_second_page_error_keeps_first(self):
        """2번째 페이지 실패 → 1페이지 데이터는 유지."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930", "000660"], cont_yn="Y", next_key="KEY2"),
            MockResponse({"return_code": -1, "return_msg": "timeout"}, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        # 1페이지 성공 후 2페이지 실패 → 부분 결과
        assert result.ok is True  # 첫 페이지는 성공했으므로
        assert result.total_rows == 2

    def test_summary_field_first_page_preserved(self):
        """Summary scalar는 first page 기준, 후속 페이지 값과 다르면 warning."""
        provider = _create_provider_no_connect()

        summary1 = {"tot_evlt_amt": "1000000", "prsm_dpst_aset_amt": "2000000"}
        summary2 = {"tot_evlt_amt": "1000500", "prsm_dpst_aset_amt": "2000000"}

        responses = [
            _make_holdings_page(["005930"], cont_yn="Y", next_key="KEY2", summary_fields=summary1),
            _make_holdings_page(["000660"], cont_yn="N", summary_fields=summary2),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "1"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        # first page summary preserved
        assert result.data["tot_evlt_amt"] == "1000000"  # NOT 1000500
        assert result.total_rows == 2

    def test_http_500_stops(self):
        """HTTP 500 → 즉시 중단."""
        provider = _create_provider_no_connect()

        responses = [
            MockResponse({}, status_code=500),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is False
        assert result.pages_fetched == 0

    def test_timeout_stops(self):
        """requests.Timeout → 중단."""
        import requests as req_lib
        provider = _create_provider_no_connect()

        with patch("requests.post", side_effect=req_lib.Timeout("timeout")):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.ok is False
        assert result.pages_fetched == 0

    def test_batch_id_consistent(self):
        """모든 페이지에서 동일 batch_id 사용."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="Y", next_key="K2"),
            _make_holdings_page(["000660"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert len(result.request_batch_id) == 12
        assert result.snapshot_ts > 0
        assert result.elapsed_ms > 0


# ══════════════════════════════════════════════════════════════
# 통합 테스트: Holdings / OpenOrders end-to-end
# ══════════════════════════════════════════════════════════════

class TestHoldingsPagination:
    """query_account_holdings() 연속조회 통합."""

    def test_25_stocks_across_3_pages(self):
        """25종목 = 10+10+5 페이지 → 누락 0건."""
        provider = _create_provider_no_connect()

        all_codes = [f"{100000 + i}" for i in range(25)]
        responses = [
            _make_holdings_page(all_codes[0:10], cont_yn="Y", next_key="P2"),
            _make_holdings_page(all_codes[10:20], cont_yn="Y", next_key="P3"),
            _make_holdings_page(all_codes[20:25], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            holdings = provider.query_account_holdings()

        assert len(holdings) == 25
        codes_received = {h["code"] for h in holdings}
        codes_expected = set(all_codes)
        assert codes_received == codes_expected, f"Missing: {codes_expected - codes_received}"

    def test_40_stocks_across_4_pages(self):
        """40종목 = 10+10+10+10 페이지 → 누락 0건."""
        provider = _create_provider_no_connect()

        all_codes = [f"{200000 + i}" for i in range(40)]
        responses = [
            _make_holdings_page(all_codes[0:10], cont_yn="Y", next_key="P2"),
            _make_holdings_page(all_codes[10:20], cont_yn="Y", next_key="P3"),
            _make_holdings_page(all_codes[20:30], cont_yn="Y", next_key="P4"),
            _make_holdings_page(all_codes[30:40], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            holdings = provider.query_account_holdings()

        assert len(holdings) == 40

    def test_single_page_20_stocks(self):
        """20종목 단일 페이지 → 기존 동작과 동일."""
        provider = _create_provider_no_connect()

        codes = [f"{300000 + i}" for i in range(20)]
        responses = [
            _make_holdings_page(codes, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            holdings = provider.query_account_holdings()

        assert len(holdings) == 20

    def test_snapshot_metadata_in_items(self):
        """각 holding에 _snapshot_ts, _batch_id 존재."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            holdings = provider.query_account_holdings()

        assert len(holdings) == 1
        assert "_snapshot_ts" in holdings[0]
        assert "_batch_id" in holdings[0]
        assert holdings[0]["_snapshot_ts"] > 0


class TestOpenOrdersPagination:
    """query_open_orders() 연속조회 통합."""

    def test_30_orders_across_3_pages(self):
        """미체결 30건 = 10+10+10 → 누락 0건."""
        provider = _create_provider_no_connect()

        responses = [
            _make_orders_page(10, start_no=1, cont_yn="Y", next_key="P2"),
            _make_orders_page(10, start_no=11, cont_yn="Y", next_key="P3"),
            _make_orders_page(10, start_no=21, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            orders = provider.query_open_orders()

        assert orders is not None
        assert len(orders) == 30
        order_nos = {o["order_no"] for o in orders}
        assert len(order_nos) == 30  # 중복 없음

    def test_empty_orders(self):
        """미체결 0건 → 빈 리스트."""
        provider = _create_provider_no_connect()

        responses = [
            _make_orders_page(0, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            orders = provider.query_open_orders()

        assert orders is not None
        assert len(orders) == 0

    def test_snapshot_metadata_in_orders(self):
        """각 order에 _snapshot_ts, _batch_id 존재."""
        provider = _create_provider_no_connect()

        responses = [
            _make_orders_page(3, start_no=1, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            orders = provider.query_open_orders()

        assert orders is not None
        assert len(orders) == 3
        assert "_snapshot_ts" in orders[0]
        assert "_batch_id" in orders[0]


class TestAccountSummaryPagination:
    """query_account_summary() summary+list 혼합 통합."""

    def test_summary_with_paginated_holdings(self):
        """Summary scalar는 first page, holdings는 전체 merge."""
        provider = _create_provider_no_connect()

        summary = {
            "tot_evlt_amt": "10000000",
            "prsm_dpst_aset_amt": "15000000",
            "tot_pur_amt": "8000000",
            "tot_evlt_pl": "2000000",
        }

        codes1 = [f"{400000 + i}" for i in range(10)]
        codes2 = [f"{400010 + i}" for i in range(10)]
        codes3 = [f"{400020 + i}" for i in range(5)]

        responses = [
            _make_holdings_page(codes1, cont_yn="Y", next_key="P2", summary_fields=summary),
            _make_holdings_page(codes2, cont_yn="Y", next_key="P3", summary_fields=summary),
            _make_holdings_page(codes3, cont_yn="N", summary_fields=summary),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider.query_account_summary()

        assert result["error"] is None
        assert result["holdings_reliable"] is True
        assert len(result["holdings"]) == 25
        assert result["추정예탁자산"] == 15000000
        assert result["총평가금액"] == 10000000
        assert result["available_cash"] == 5000000
        assert result["_pages_fetched"] == 3


# ══════════════════════════════════════════════════════════════
# Status / Consistency 테스트
# ══════════════════════════════════════════════════════════════

class TestSnapshotStatus:
    """COMPLETE / PARTIAL / FAILED 상태 판정 검증."""

    def test_complete_status(self):
        """모든 페이지 성공 → COMPLETE."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="Y", next_key="K2"),
            _make_holdings_page(["000660"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.status == "COMPLETE"
        assert result.consistency == "CLEAN"
        assert result.ok is True

    def test_partial_status_on_second_page_fail(self):
        """2페이지 실패 → PARTIAL, 1페이지 데이터 유지."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="Y", next_key="K2"),
            MockResponse({"return_code": -1, "return_msg": "server error"}, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.status == "PARTIAL"
        assert result.ok is True  # 1페이지 데이터는 있음
        assert result.total_rows == 1

    def test_failed_status_on_first_page(self):
        """첫 페이지 실패 → FAILED."""
        provider = _create_provider_no_connect()

        responses = [
            MockResponse({"return_code": -1, "return_msg": "error"}),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.status == "FAILED"
        assert result.ok is False

    def test_partial_timeout_mid_batch(self):
        """3페이지 중 마지막 timeout → PARTIAL."""
        import requests as req_lib
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"] * 5, cont_yn="Y", next_key="K2"),
            _make_holdings_page(["000660"] * 5, cont_yn="Y", next_key="K3"),
            req_lib.Timeout("timeout"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.status == "PARTIAL"
        assert result.pages_fetched == 2
        assert result.total_rows == 10

    def test_batch_end_ts_recorded(self):
        """batch_end_ts가 snapshot_ts 이후."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.batch_end_ts >= result.snapshot_ts


class TestSnapshotConsistencyIntegration:
    """WS 이벤트 개입 시 DEGRADED 판정."""

    def test_ws_event_during_batch_marks_degraded(self):
        """batch 진행 중 WS 00/04 이벤트 → DEGRADED."""
        provider = _create_provider_no_connect()

        # Simulate WS event during batch by directly setting counter
        def post_with_ws_event(*args, **kwargs):
            # Simulate a WS 00 event arriving during REST call
            with provider._batch_lock:
                if provider._batch_active:
                    provider._batch_ws_event_count += 1
            return _make_holdings_page(["005930"], cont_yn="N")

        with patch("requests.post", side_effect=post_with_ws_event):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.consistency == "DEGRADED"
        assert result.ws_events_during_batch >= 1
        assert result.status == "COMPLETE"  # 데이터 자체는 완전

    def test_no_ws_event_stays_clean(self):
        """batch 중 WS 이벤트 없음 → CLEAN."""
        provider = _create_provider_no_connect()

        responses = [
            _make_holdings_page(["005930"], cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider._request_all(
                "kt00018", "/api/dostk/acnt",
                {"qry_tp": "2"},
                list_key="acnt_evlt_remn_indv_tot",
            )

        assert result.consistency == "CLEAN"
        assert result.ws_events_during_batch == 0


class TestPartialGateIntegration:
    """PARTIAL → 상위 소비자 차단 검증."""

    def test_partial_summary_marks_unreliable(self):
        """summary PARTIAL → holdings_reliable=False."""
        provider = _create_provider_no_connect()

        summary = {"tot_evlt_amt": "1000000", "prsm_dpst_aset_amt": "2000000",
                    "tot_pur_amt": "800000", "tot_evlt_pl": "200000"}

        responses = [
            _make_holdings_page(["005930"] * 10, cont_yn="Y", next_key="K2",
                                summary_fields=summary),
            MockResponse({"return_code": -1, "return_msg": "fail"}, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider.query_account_summary()

        assert result["holdings_reliable"] is False
        assert result["_status"] == "PARTIAL"
        assert result["error"] is None  # 에러는 아님, partial일 뿐

    def test_complete_summary_reliable(self):
        """summary COMPLETE → holdings_reliable=True."""
        provider = _create_provider_no_connect()

        summary = {"tot_evlt_amt": "1000000", "prsm_dpst_aset_amt": "2000000",
                    "tot_pur_amt": "800000", "tot_evlt_pl": "200000"}

        responses = [
            _make_holdings_page(["005930"], cont_yn="N", summary_fields=summary),
        ]

        with patch("requests.post", side_effect=responses):
            result = provider.query_account_summary()

        assert result["holdings_reliable"] is True
        assert result["_status"] == "COMPLETE"

    def test_partial_open_orders_returns_none(self):
        """open_orders PARTIAL → None 반환 (opt10075 fail 트리거)."""
        provider = _create_provider_no_connect()

        responses = [
            _make_orders_page(10, start_no=1, cont_yn="Y", next_key="K2"),
            MockResponse({"return_code": -1, "return_msg": "fail"}, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            orders = provider.query_open_orders()

        assert orders is None  # PARTIAL → safety: return None

    def test_complete_open_orders_returns_list(self):
        """open_orders COMPLETE → 정상 리스트."""
        provider = _create_provider_no_connect()

        responses = [
            _make_orders_page(5, start_no=1, cont_yn="N"),
        ]

        with patch("requests.post", side_effect=responses):
            orders = provider.query_open_orders()

        assert orders is not None
        assert len(orders) == 5


# ══════════════════════════════════════════════════════════════
# Standalone runner
# ══════════════════════════════════════════════════════════════

def _run_standalone() -> int:
    """pytest 없이 실행 가능한 standalone runner."""
    import traceback

    test_classes = [
        TestRequestAllUnit,
        TestHoldingsPagination,
        TestOpenOrdersPagination,
        TestAccountSummaryPagination,
        TestSnapshotStatus,
        TestSnapshotConsistencyIntegration,
        TestPartialGateIntegration,
    ]

    passed = 0
    failed = 0
    errors: List[str] = []

    for cls in test_classes:
        print(f"\n{'=' * 50}")
        print(f"  {cls.__name__}")
        print(f"{'=' * 50}")

        instance = cls()
        for name in sorted(dir(instance)):
            if not name.startswith("test_"):
                continue
            method = getattr(instance, name)
            try:
                method()
                print(f"  [PASS] {name}")
                passed += 1
            except Exception as e:
                print(f"  [FAIL] {name}: {e}")
                traceback.print_exc()
                failed += 1
                errors.append(f"{cls.__name__}.{name}: {e}")

    print(f"\n{'=' * 50}")
    print(f"  Results: {passed} passed, {failed} failed")
    if errors:
        print(f"\n  Failures:")
        for e in errors:
            print(f"    - {e}")
    print(f"{'=' * 50}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(_run_standalone())
