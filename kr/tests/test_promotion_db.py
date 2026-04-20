"""
test_promotion_db.py — Phase C DB-primary 검증
================================================
Promotion artifacts가 PG에 저장/조회되는지 확인.

전제: PG 연결 가능 + v013 migration 적용됨. 연결 안 되면 skip.

Run:
  .venv/Scripts/python.exe -m pytest kr/tests/test_promotion_db.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

_KR = Path(__file__).resolve().parent.parent
if str(_KR) not in sys.path:
    sys.path.insert(0, str(_KR))
_ROOT = _KR.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pytest


@pytest.fixture(scope="module", autouse=True)
def _require_db():
    """DB 연결 불가 시 전체 모듈 skip."""
    try:
        from lab.promotion import db as _pdb
        if not _pdb.is_db_available():
            pytest.skip("PG unavailable — skipping DB-primary tests")
    except Exception as e:
        pytest.skip(f"promotion.db import failed: {e}")


@pytest.fixture(autouse=True)
def _cleanup_test_strategy():
    """각 테스트 전후로 test_db_strategy_* 데이터 정리."""
    from shared.db.pg_base import connection
    patterns = ["test_db_%"]
    for _ in (0, 1):  # before + after
        try:
            with connection() as conn:
                cur = conn.cursor()
                for p in patterns:
                    cur.execute(
                        "DELETE FROM promotion_regime_history WHERE strategy_name LIKE %s",
                        (p,),
                    )
                    cur.execute(
                        "DELETE FROM promotion_transition_log WHERE strategy LIKE %s",
                        (p,),
                    )
                conn.commit()
                cur.close()
        except Exception:
            pass
        yield
        break


# ─── Regime History (DB) ─────────────────────────────────────────────

def test_db_regime_insert_and_load():
    from lab.promotion.regime_history import record_regime, load_history

    ok = record_regime(
        trade_date="2026-01-10",
        strategy_name="test_db_regime_A",
        regime_label="BULL",
        confidence=0.8,
        snapshot_version="sv1",
    )
    assert ok is True

    rows = load_history("test_db_regime_A")
    assert len(rows) == 1
    assert rows[0]["regime_label"] == "BULL"
    assert rows[0]["snapshot_version"] == "sv1"
    assert rows[0]["confidence"] == 0.8


def test_db_regime_idempotent_same_sv():
    from lab.promotion.regime_history import record_regime, load_history

    args = dict(
        trade_date="2026-01-11",
        strategy_name="test_db_regime_B",
        regime_label="BEAR",
        confidence=0.9,
        snapshot_version="sv1",
    )
    assert record_regime(**args) is True
    # 동일 snapshot_version → skip
    assert record_regime(**args) is False

    rows = load_history("test_db_regime_B")
    assert len(rows) == 1

    # 다른 snapshot_version → 새 row
    args2 = dict(args, snapshot_version="sv2")
    assert record_regime(**args2) is True
    rows = load_history("test_db_regime_B")
    assert len(rows) == 2


def test_db_regime_coverage():
    from lab.promotion.regime_history import record_regime, coverage_from_history

    name = "test_db_regime_C"
    for i, (d, lbl) in enumerate([
        ("2026-02-01", "BULL"),
        ("2026-02-02", "BULL"),
        ("2026-02-03", "BEAR"),
        ("2026-02-04", "SIDEWAYS"),
    ]):
        record_regime(
            trade_date=d, strategy_name=name, regime_label=lbl,
            confidence=0.7, snapshot_version=f"sv_{i}",
        )
    cov = coverage_from_history(name)
    assert cov["regime_coverage"] == 3  # BULL/BEAR/SIDEWAYS
    assert cov["regime_flip_observed"] == 2
    assert cov["total_days"] == 4


# ─── Transition Log (DB) ─────────────────────────────────────────────

def test_db_transition_dedup():
    from lab.promotion.transition_log import record_transition, load_transitions

    name = "test_db_trans_A"
    assert record_transition(
        strategy=name, new_status="CANDIDATE",
        score=55, reason="initial",
    ) is True
    # 동일 status → skip
    assert record_transition(
        strategy=name, new_status="CANDIDATE",
        score=55, reason="again",
    ) is False
    # 다른 status → append
    assert record_transition(
        strategy=name, new_status="PAPER_READY",
        score=70, reason="upgraded",
    ) is True

    rows = load_transitions(name)
    assert len(rows) == 2
    assert rows[-1]["old_status"] == "CANDIDATE"
    assert rows[-1]["new_status"] == "PAPER_READY"


def test_db_transition_force():
    from lab.promotion.transition_log import record_transition, load_transitions

    name = "test_db_trans_B"
    record_transition(strategy=name, new_status="CANDIDATE", score=50)
    # force=True → 동일 status 여도 기록
    assert record_transition(
        strategy=name, new_status="CANDIDATE", score=55, force=True,
    ) is True

    rows = load_transitions(name)
    assert len(rows) == 2


# ─── Ops Snapshot (DB) ───────────────────────────────────────────────

def test_db_ops_snapshot_upsert_and_load():
    from runtime.ops_metrics import (
        FieldEvidence, save_ops_snapshot, load_ops_snapshot,
    )
    from shared.db.pg_base import connection

    # 테스트 전용 필드 사용 후 정리
    _test_fields = [
        "test_db_ops_field_critical",
        "test_db_ops_field_high",
    ]
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM promotion_ops_snapshot WHERE field_name = ANY(%s)",
                (_test_fields,),
            )
            conn.commit()
            cur.close()
    except Exception:
        pass

    # UPSERT
    save_ops_snapshot({
        "test_db_ops_field_critical": FieldEvidence(
            value=0, source="test", window="session",
        ),
        "test_db_ops_field_high": FieldEvidence(
            value=None, source="test", window="24h",  # UNKNOWN
        ),
    }, origin="pytest")

    # Load
    snap = load_ops_snapshot()
    assert "test_db_ops_field_critical" in snap
    assert snap["test_db_ops_field_critical"]["value"] == 0
    assert "test_db_ops_field_high" in snap
    assert snap["test_db_ops_field_high"]["value"] is None  # UNKNOWN preserved

    # UPSERT 업데이트 (동일 field_name 에 새 값)
    save_ops_snapshot({
        "test_db_ops_field_critical": FieldEvidence(
            value=3, source="test2", window="session",
        ),
    }, origin="pytest_update")
    snap = load_ops_snapshot()
    assert snap["test_db_ops_field_critical"]["value"] == 3

    # Cleanup
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM promotion_ops_snapshot WHERE field_name = ANY(%s)",
                (_test_fields,),
            )
            conn.commit()
            cur.close()
    except Exception:
        pass


if __name__ == "__main__":
    import traceback
    pytest.main([__file__, "-v"])
