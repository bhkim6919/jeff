"""
test_cross_validator.py -- CrossValidationObserver P2 테스트
=============================================================
Usage:
    cd Gen04-REST
    ../.venv/Scripts/python.exe tests/test_cross_validator.py
"""
from __future__ import annotations

import sys
import time
import tempfile
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from web.cross_validator import CrossValidationObserver, DiffType


# ── Test Helpers ──────────────────────────────────────────────

def _broker_summary(
    cash: int = 1000000,
    holdings: list = None,
    snapshot_ts: float = None,
    status: str = "COMPLETE",
    consistency: str = "CLEAN",
    error: str = None,
) -> dict:
    ts = snapshot_ts or time.time()
    return {
        "available_cash": cash,
        "holdings": holdings or [],
        "error": error,
        "holdings_reliable": True,
        "_snapshot_ts": ts,
        "_batch_id": "test_batch",
        "_status": status,
        "_consistency": consistency,
    }


def _file_state(
    cash: int = 1000000,
    positions: dict = None,
    version_seq: int = 1,
    timestamp: str = None,
) -> dict:
    from datetime import datetime
    return {
        "cash": cash,
        "positions": positions or {},
        "timestamp": timestamp or datetime.now().isoformat(),
        "_version_seq": version_seq,
    }


def _holding(code: str, qty: int = 100, avg_price: int = 50000) -> dict:
    return {"code": code, "qty": qty, "avg_price": avg_price}


def _position(qty: int = 100, avg_price: int = 50000) -> dict:
    return {"quantity": qty, "avg_price": avg_price}


# ══════════════════════════════════════════════════════════════
# Test Cases
# ══════════════════════════════════════════════════════════════

class TestDiffNone:
    """정상 케이스: 3자 동일."""

    def test_exact_match(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            cash=1000000,
            holdings=[_holding("005930", 100)],
            snapshot_ts=now,
        )
        file = _file_state(
            cash=1000000,
            positions={"005930": _position(100)},
            timestamp=_ts_iso(now),
        )
        sample = obs.observe(broker, file)
        assert sample.eligible is True
        assert len(sample.diffs) == 1
        assert sample.diffs[0]["type"] == "DIFF_NONE"

    def test_multiple_stocks_match(self):
        obs = CrossValidationObserver()
        now = time.time()
        codes = [f"{100000 + i}" for i in range(20)]
        holdings = [_holding(c, 50) for c in codes]
        positions = {c: _position(50) for c in codes}

        broker = _broker_summary(cash=500000, holdings=holdings, snapshot_ts=now)
        file = _file_state(cash=500000, positions=positions, timestamp=_ts_iso(now))

        sample = obs.observe(broker, file)
        assert sample.diffs[0]["type"] == "DIFF_NONE"


class TestTimingMismatch:
    """Timing window 내 차이 -> DIFF_TIMING_WINDOW."""

    def test_cash_diff_within_timing(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(cash=1000000, snapshot_ts=now)
        # file은 10초 전 (> 5s timing window)
        file = _file_state(cash=999000, timestamp=_ts_iso(now - 10))

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_TIMING_WINDOW" in types
        assert "DIFF_CASH" not in types  # timing으로 분류, real 아님

    def test_qty_diff_within_timing(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            holdings=[_holding("005930", 100)], snapshot_ts=now)
        file = _file_state(
            positions={"005930": _position(90)},
            timestamp=_ts_iso(now - 8))  # 8s gap

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_TIMING_WINDOW" in types
        assert "DIFF_QTY" not in types

    def test_within_window_is_real(self):
        """timing window 내 (< 5s)이면 DIFF_QTY (real)."""
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            holdings=[_holding("005930", 100)], snapshot_ts=now)
        file = _file_state(
            positions={"005930": _position(90)},
            timestamp=_ts_iso(now - 2))  # 2s gap -> real

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_QTY" in types
        assert "DIFF_TIMING_WINDOW" not in types


class TestPartialExclusion:
    """PARTIAL/DEGRADED source -> strict 제외."""

    def test_broker_partial_excluded(self):
        obs = CrossValidationObserver()
        broker = _broker_summary(status="PARTIAL")
        file = _file_state()

        sample = obs.observe(broker, file)
        assert sample.eligible is False
        assert "PARTIAL" in sample.exclusion_reason

    def test_broker_degraded_excluded(self):
        obs = CrossValidationObserver()
        broker = _broker_summary(consistency="DEGRADED")
        file = _file_state()

        sample = obs.observe(broker, file)
        assert sample.eligible is False
        assert "DEGRADED" in sample.exclusion_reason

    def test_broker_error_excluded(self):
        obs = CrossValidationObserver()
        broker = _broker_summary(error="connection failed")
        file = _file_state()

        sample = obs.observe(broker, file)
        assert sample.eligible is False

    def test_partial_not_counted_in_eligible_rate(self):
        obs = CrossValidationObserver()
        now = time.time()

        # 3 clean matches + 2 partial excluded
        for _ in range(3):
            broker = _broker_summary(cash=100, snapshot_ts=now)
            file = _file_state(cash=100, timestamp=_ts_iso(now))
            obs.observe(broker, file)

        for _ in range(2):
            broker = _broker_summary(cash=100, status="PARTIAL")
            file = _file_state(cash=100)
            obs.observe(broker, file)

        stats = obs.get_stats()
        assert stats["total_samples"] == 5
        assert stats["eligible_samples"] == 3
        assert stats["eligible_diff_zero_rate"] == 1.0  # 3/3
        assert stats["strict_diff_zero_rate"] == 3 / 5  # 3/5


class TestRealMismatch:
    """Real diff 분류 검증."""

    def test_codeset_diff(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            holdings=[_holding("005930", 100), _holding("000660", 50)],
            snapshot_ts=now)
        file = _file_state(
            positions={"005930": _position(100)},
            timestamp=_ts_iso(now))

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_CODESET" in types

    def test_qty_diff(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            holdings=[_holding("005930", 100)], snapshot_ts=now)
        file = _file_state(
            positions={"005930": _position(80)},
            timestamp=_ts_iso(now))

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_QTY" in types

    def test_cash_diff(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(cash=1000000, snapshot_ts=now)
        file = _file_state(cash=900000, timestamp=_ts_iso(now))

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_CASH" in types

    def test_avg_price_diff(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(
            holdings=[_holding("005930", 100, avg_price=50000)],
            snapshot_ts=now)
        file = _file_state(
            positions={"005930": _position(100, avg_price=40000)},
            timestamp=_ts_iso(now))  # 20% diff

        sample = obs.observe(broker, file)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_AVG_PRICE" in types

    def test_open_orders_diff(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(snapshot_ts=now)
        file = _file_state(timestamp=_ts_iso(now))

        sample = obs.observe(
            broker, file,
            open_orders_broker=[{"order_no": "001"}],
            open_orders_file=[],
        )
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_OPEN_ORDERS" in types


class TestVersionMismatch:
    """version_seq 불일치."""

    def test_version_mismatch_detected(self):
        obs = CrossValidationObserver()
        now = time.time()
        broker = _broker_summary(snapshot_ts=now)
        file = _file_state(version_seq=10, timestamp=_ts_iso(now))
        rest = {"_snapshot_ts": now, "_version_seq": 8}

        sample = obs.observe(broker, file, rest_state=rest)
        types = {d["type"] for d in sample.diffs}
        assert "DIFF_VERSION_MISMATCH" in types


class TestStatistics:
    """통계 누적 검증."""

    def test_consecutive_zero_tracking(self):
        obs = CrossValidationObserver()
        now = time.time()

        # 5 consecutive matches
        for i in range(5):
            broker = _broker_summary(cash=100, snapshot_ts=now)
            file = _file_state(cash=100, timestamp=_ts_iso(now))
            obs.observe(broker, file)

        stats = obs.get_stats()
        assert stats["consecutive_zero"] == 5
        assert stats["max_consecutive_zero"] == 5

        # 1 mismatch breaks streak
        broker = _broker_summary(cash=999, snapshot_ts=now)
        file = _file_state(cash=100, timestamp=_ts_iso(now))
        obs.observe(broker, file)

        stats = obs.get_stats()
        assert stats["consecutive_zero"] == 0
        assert stats["max_consecutive_zero"] == 5  # preserved

    def test_diff_by_type_counts(self):
        obs = CrossValidationObserver()
        now = time.time()

        # 1 DIFF_NONE
        obs.observe(
            _broker_summary(cash=100, snapshot_ts=now),
            _file_state(cash=100, timestamp=_ts_iso(now)))

        # 1 DIFF_CASH
        obs.observe(
            _broker_summary(cash=999999, snapshot_ts=now),
            _file_state(cash=100, timestamp=_ts_iso(now)))

        stats = obs.get_stats()
        assert stats["diff_by_type"]["DIFF_NONE"] == 1
        assert stats["diff_by_type"]["DIFF_CASH"] == 1


class TestPhase3Gate:
    """Phase 3 진입 조건 판정."""

    def test_not_ready_insufficient_samples(self):
        obs = CrossValidationObserver()
        result = obs.check_phase3_ready(min_samples=200)
        assert result["ready"] is False
        assert any("samples" in r for r in result["blocking_reasons"])

    def test_ready_when_enough_clean_samples(self):
        obs = CrossValidationObserver()
        now = time.time()

        for _ in range(200):
            broker = _broker_summary(cash=100, snapshot_ts=now)
            file = _file_state(cash=100, timestamp=_ts_iso(now))
            obs.observe(broker, file)

        result = obs.check_phase3_ready(min_samples=200, min_zero_rate=0.99)
        assert result["ready"] is True

    def test_not_ready_critical_diffs(self):
        obs = CrossValidationObserver()
        now = time.time()

        for _ in range(199):
            obs.observe(
                _broker_summary(cash=100, snapshot_ts=now),
                _file_state(cash=100, timestamp=_ts_iso(now)))

        # 1 critical diff
        obs.observe(
            _broker_summary(cash=999999, snapshot_ts=now),
            _file_state(cash=100, timestamp=_ts_iso(now)))

        result = obs.check_phase3_ready(min_samples=200, min_zero_rate=0.99)
        assert result["ready"] is False


class TestObserverOnly:
    """Observer-only: state 변경 없음 확인."""

    def test_no_side_effects(self):
        """observe()가 입력 dict를 변경하지 않음."""
        obs = CrossValidationObserver()
        now = time.time()

        broker = _broker_summary(cash=100, snapshot_ts=now)
        file = _file_state(cash=200, timestamp=_ts_iso(now))

        broker_copy = dict(broker)
        file_copy = dict(file)

        obs.observe(broker, file)

        # 원본 dict 변경 없음
        assert broker == broker_copy
        assert file == file_copy


class TestDailySummary:
    """일별 summary 저장."""

    def test_save_creates_files(self):
        obs = CrossValidationObserver()
        now = time.time()
        obs.observe(
            _broker_summary(cash=100, snapshot_ts=now),
            _file_state(cash=100, timestamp=_ts_iso(now)))

        with tempfile.TemporaryDirectory() as td:
            path = obs.save_daily_summary(Path(td))
            assert path is not None
            assert path.exists()
            # CSV also created
            csv_path = Path(td) / "xval_daily.csv"
            assert csv_path.exists()


# ── Helpers ──────────────────────────────────────────────────

def _ts_iso(ts: float) -> str:
    from datetime import datetime
    return datetime.fromtimestamp(ts).isoformat()


# ── Standalone Runner ────────────────────────────────────────

def _run_standalone() -> int:
    import traceback

    test_classes = [
        TestDiffNone,
        TestTimingMismatch,
        TestPartialExclusion,
        TestRealMismatch,
        TestVersionMismatch,
        TestStatistics,
        TestPhase3Gate,
        TestObserverOnly,
        TestDailySummary,
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
