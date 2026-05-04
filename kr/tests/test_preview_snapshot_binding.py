"""Preview Snapshot Binding (Option A — drift detection, no execute change).

Goal (Jeff/JUG): preview 에서 본 주문과 execute 주문이 달라지는 문제 차단.
mismatch 면 reject — 저장된 preview order 로 직접 execute 하지 않음,
_execute_rebalance_live 의 핵심 흐름은 그대로 유지.

5 fields stored on preview, re-verified at execute:
  preview_id        — uuid per preview
  snapshot_version  — target.snapshot_version (data version)
  target_hash       — canonical hash of (target_tickers, scores)
  order_hash        — canonical hash of computed (sells, buys)
  created_at        — ISO datetime

Required scenarios (per Jeff):
  - preview 없음 → execute reject
  - target_hash mismatch → reject
  - order_hash mismatch → reject
  - snapshot_version mismatch → reject
  - created_at TTL 초과 → reject
  - hash match → 기존 execute 진행
  - broker_sync fail → 기존처럼 reject
  - monitor_only → 기존처럼 reject
  - AUD-N1/N2 fail-closed 테스트 유지

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_preview_snapshot_binding.py -v
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

import web.rebalance_api as ra  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_state():
    """Reset module-level state between tests so previews don't leak."""
    ra._state = ra.RebalCycleState()
    ra._initialized = True
    yield
    ra._state = ra.RebalCycleState()
    ra._initialized = False


def _state_mgr(runtime: dict | None = None,
               portfolio: dict | None = None):
    sm = MagicMock()
    sm.load_runtime.return_value = runtime if runtime is not None else {}
    sm.load_portfolio.return_value = portfolio if portfolio is not None else {
        "cash": 1_000_000, "positions": {}
    }
    sm.load_pending_external.return_value = []
    sm.save_runtime.return_value = True
    return sm


def _provider_clean():
    p = MagicMock()
    p.query_open_orders.return_value = []
    p.query_account_summary.return_value = {
        "available_cash": 1_000_000,
        "holdings_reliable": True,
        "holdings": [],
        "error": None,
    }
    p.get_current_price = MagicMock(return_value=70_000)
    return p


def _config():
    return SimpleNamespace(
        SIGNALS_DIR="/tmp/nope",
        INITIAL_CASH=100_000_000,
        DAILY_DD_LIMIT=-0.04,
        MONTHLY_DD_LIMIT=-0.07,
        N_STOCKS=20,
        BUY_COST=0.00115,
        SELL_COST=0.00295,
        CASH_BUFFER_RATIO=0.01,
    )


def _make_preview_state(preview_id="abc123", snapshot_version="20260504:DB:...:42:hash",
                       target_hash="t" * 64, order_hash="o" * 64,
                       created_at=None, phase="PREVIEW_READY"):
    """Populate _state as if a preview was just created."""
    if created_at is None:
        created_at = datetime.now().isoformat(timespec="seconds")
    ra._state.preview_id = preview_id
    ra._state.snapshot_version = snapshot_version
    ra._state.target_hash = target_hash
    ra._state.order_hash = order_hash
    ra._state.created_at = created_at
    ra._state.phase = phase
    ra._state.preview_sells = []
    ra._state.preview_buys = []


# ── Hash function determinism ────────────────────────────────────────


def test_target_hash_stable_for_same_target():
    """동일 target → 동일 hash."""
    target = {
        "target_tickers": ["005930", "000660"],
        "scores": {"005930": {"vol_12m": 0.2}, "000660": {"vol_12m": 0.3}},
    }
    h1 = ra._compute_target_hash(target)
    h2 = ra._compute_target_hash(target)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_target_hash_invariant_under_dict_reorder():
    """딕셔너리 키 순서가 달라도 hash 동일 (canonical)."""
    t1 = {
        "target_tickers": ["005930", "000660"],
        "scores": {"005930": {"vol_12m": 0.2}, "000660": {"vol_12m": 0.3}},
    }
    t2 = {
        "scores": {"000660": {"vol_12m": 0.3}, "005930": {"vol_12m": 0.2}},
        "target_tickers": ["000660", "005930"],  # reordered
    }
    assert ra._compute_target_hash(t1) == ra._compute_target_hash(t2)


def test_target_hash_changes_when_tickers_change():
    """target_tickers 가 다르면 hash 다름."""
    t1 = {"target_tickers": ["005930"], "scores": {}}
    t2 = {"target_tickers": ["005930", "000660"], "scores": {}}
    assert ra._compute_target_hash(t1) != ra._compute_target_hash(t2)


def test_order_hash_stable_for_same_orders():
    sells = [{"code": "005930", "qty": 100, "price": 70000}]
    buys = [{"code": "000660", "target_amount": 5_000_000}]
    h1 = ra._compute_order_hash(sells, buys)
    h2 = ra._compute_order_hash(sells, buys)
    assert h1 == h2


def test_order_hash_ignores_display_fields():
    """name, rank, mom 등 표시 전용 필드 변화는 hash 에 영향 없음."""
    sells_a = [{"code": "005930", "qty": 100, "price": 70000, "name": "삼성전자", "amount": 7_000_000}]
    sells_b = [{"code": "005930", "qty": 100, "price": 70000, "name": "Different name"}]
    buys = [{"code": "000660", "target_amount": 5_000_000, "rank": 1, "mom": 0.5}]
    buys_alt = [{"code": "000660", "target_amount": 5_000_000, "rank": 99, "mom": -0.1}]
    assert ra._compute_order_hash(sells_a, buys) == ra._compute_order_hash(sells_b, buys_alt)


def test_order_hash_changes_on_qty_change():
    sells_a = [{"code": "005930", "qty": 100, "price": 70000}]
    sells_b = [{"code": "005930", "qty": 50, "price": 70000}]  # different qty
    buys = []
    assert ra._compute_order_hash(sells_a, buys) != ra._compute_order_hash(sells_b, buys)


# ── _check_preview_drift: each reject condition ──────────────────────


def test_drift_check_no_preview_id_rejects(monkeypatch):
    """preview_id 미설정 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    # Don't populate _state — preview_id stays empty
    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "no preview_id" in out


def test_drift_check_ttl_exceeded_rejects(monkeypatch):
    """created_at 이 TTL (48h) 초과 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    old = (datetime.now() - timedelta(hours=49)).isoformat(timespec="seconds")
    _make_preview_state(created_at=old)

    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "TTL" in out


def test_drift_check_missing_created_at_rejects(monkeypatch):
    """legacy preview (created_at 없음) → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    _make_preview_state(created_at="")

    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "created_at missing" in out


def test_drift_check_target_missing_rejects(monkeypatch):
    """target_portfolio 파일 없음 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    _make_preview_state()

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: None,
    )
    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "target portfolio missing" in out


def test_drift_check_snapshot_version_mismatch_rejects(monkeypatch):
    """snapshot_version 변경 (새 batch 적재) → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    _make_preview_state(snapshot_version="20260504:DB:...:42:OLD")

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {
            "target_tickers": ["005930"],
            "scores": {},
            "snapshot_version": "20260504:DB:...:42:NEW",
        },
    )
    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "snapshot_version changed" in out


def test_drift_check_target_hash_mismatch_rejects(monkeypatch):
    """target_hash 변경 (target portfolio 수정) → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    # Stored target_hash matches a previous target — current target differs
    fake_target = {
        "target_tickers": ["005930", "000660"],
        "scores": {},
        "snapshot_version": "sv1",
    }
    stored_old_hash = ra._compute_target_hash({
        "target_tickers": ["005930"],  # different
        "scores": {},
    })
    _make_preview_state(snapshot_version="sv1", target_hash=stored_old_hash)

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: fake_target,
    )
    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "target_hash changed" in out


def test_drift_check_order_hash_mismatch_rejects(monkeypatch):
    """order_hash 변경 (가격/포지션 변동) → reject.

    target_hash + snapshot_version 은 일치하지만 portfolio.positions 또는
    가격 변화로 compute_orders 결과가 달라지는 시나리오.
    """
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    target = {
        "target_tickers": ["005930"],
        "scores": {},
        "snapshot_version": "sv1",
    }
    stored_target_hash = ra._compute_target_hash(target)
    # Stored order_hash refers to a DIFFERENT order set than what
    # _recompute_orders_for_drift will produce.
    _make_preview_state(
        snapshot_version="sv1",
        target_hash=stored_target_hash,
        order_hash="z" * 64,  # mismatch — recomputed will differ
    )

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: target,
    )
    # Mock compute_orders to return some plausible orders
    fake_sell_order = SimpleNamespace(ticker="005930", quantity=10)
    fake_buy_order = SimpleNamespace(ticker="000660", target_amount=1_000_000)
    monkeypatch.setattr(
        "strategy.rebalancer.compute_orders",
        lambda **kw: ([fake_sell_order], [fake_buy_order]),
    )

    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFTED" in out
    assert "order_hash changed" in out


def test_drift_check_all_match_passes(monkeypatch):
    """모든 hash + version + TTL match → drift 없음 (None 반환)."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    target = {
        "target_tickers": ["005930"],
        "scores": {"005930": {"vol_12m": 0.2}},
        "snapshot_version": "sv1",
    }
    correct_target_hash = ra._compute_target_hash(target)
    fake_sell = SimpleNamespace(ticker="005930", quantity=10)
    fake_buy = SimpleNamespace(ticker="000660", target_amount=1_000_000)
    correct_order_hash = ra._compute_order_hash(
        [{"code": "005930", "qty": 10, "price": 70_000}],
        [{"code": "000660", "target_amount": 1_000_000}],
    )
    _make_preview_state(
        snapshot_version="sv1",
        target_hash=correct_target_hash,
        order_hash=correct_order_hash,
    )
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: target,
    )
    monkeypatch.setattr(
        "strategy.rebalancer.compute_orders",
        lambda **kw: ([fake_sell], [fake_buy]),
    )
    out = ra._check_preview_drift(sm, cfg, p)
    assert out is None, f"Expected no drift, got: {out}"


def test_drift_check_handles_recompute_exception(monkeypatch):
    """compute_orders 예외 시 PREVIEW_DRIFT_CHECK_FAILED (drift 가 아닌 별개 분류)."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    target = {
        "target_tickers": ["005930"],
        "scores": {},
        "snapshot_version": "sv1",
    }
    correct_target_hash = ra._compute_target_hash(target)
    _make_preview_state(
        snapshot_version="sv1",
        target_hash=correct_target_hash,
    )
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: target,
    )
    def _raise(**kw): raise RuntimeError("rebalancer broken")
    monkeypatch.setattr("strategy.rebalancer.compute_orders", _raise)

    out = ra._check_preview_drift(sm, cfg, p)
    assert out is not None
    assert "PREVIEW_DRIFT_CHECK_FAILED" in out
    assert "rebalancer broken" in out


# ── execute_sell / execute_buy integration with drift check ──────────


def test_execute_sell_rejects_when_no_preview_id(monkeypatch):
    """execute_sell: preview 없음 (preview_id 빈 문자열) → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    ra._state.phase = "PREVIEW_READY"
    # Don't populate preview_id — drift check should catch
    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                         trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "PREVIEW_DRIFTED" in out["error"]


def test_execute_sell_rejects_on_target_hash_mismatch(monkeypatch):
    """execute_sell: target_hash 변경 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    _make_preview_state(target_hash="x" * 64)  # mismatch
    ra._state.phase = "PREVIEW_READY"
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"target_tickers": ["005930"], "scores": {}, "snapshot_version": "sv1"},
    )
    ra._state.snapshot_version = "sv1"
    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                         trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "PREVIEW_DRIFTED" in out["error"]


def test_execute_buy_rejects_when_no_preview_id(monkeypatch):
    """execute_buy: preview 없음 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    ra._state.phase = "BUY_READY"
    ra._state.sell_status = "COMPLETE"
    out = ra.execute_buy(sm, cfg, p, executor=MagicMock(),
                        trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "PREVIEW_DRIFTED" in out["error"]


def test_execute_buy_rejects_on_ttl_expired(monkeypatch):
    """execute_buy: created_at TTL 초과 → reject."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    old = (datetime.now() - timedelta(hours=49)).isoformat(timespec="seconds")
    _make_preview_state(created_at=old, phase="BUY_READY")
    ra._state.sell_status = "COMPLETE"
    out = ra.execute_buy(sm, cfg, p, executor=MagicMock(),
                        trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    assert "PREVIEW_DRIFTED" in out["error"]
    assert "TTL" in out["error"]


# ── Existing protections still active (PR 1, AUD-N1/N2 regression) ──


def test_monitor_only_still_rejects_after_drift_check_passes(monkeypatch):
    """drift check 통과해도 monitor_only 가 set 되어 있으면 reject (PR 1 회귀 방지)."""
    sm = _state_mgr(runtime={"monitor_only_reason": "holdings_unreliable"})
    p = _provider_clean()
    cfg = _config()
    target = {
        "target_tickers": ["005930"],
        "scores": {},
        "snapshot_version": "sv1",
    }
    correct_h = ra._compute_target_hash(target)
    correct_oh = ra._compute_order_hash(
        [{"code": "005930", "qty": 10, "price": 70_000}],
        [{"code": "000660", "target_amount": 1_000_000}],
    )
    _make_preview_state(snapshot_version="sv1", target_hash=correct_h, order_hash=correct_oh)
    ra._state.phase = "PREVIEW_READY"
    fake_sell = SimpleNamespace(ticker="005930", quantity=10)
    fake_buy = SimpleNamespace(ticker="000660", target_amount=1_000_000)
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio", lambda _d: target,
    )
    monkeypatch.setattr(
        "strategy.rebalancer.compute_orders",
        lambda **kw: ([fake_sell], [fake_buy]),
    )

    out = ra.execute_sell(sm, cfg, p, executor=MagicMock(),
                         trade_logger=MagicMock(), tracker=MagicMock())
    assert out["ok"] is False
    # The drift check passes (all hashes match), but the gate's
    # _check_monitor_only blocks it.
    assert "MONITOR_ONLY" in out["error"]


def test_aud_n1_fail_closed_still_active(monkeypatch):
    """AUD-N1: BuyPermission 예외 시 BLOCK 거동 유지 (회귀 방지)."""
    from risk.exposure_guard import BuyPermission  # noqa: F401

    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    guard.get_buy_permission.side_effect = RuntimeError("guard broken")
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": [], "scores": {}, "snapshot_version": ""},
    )

    out = ra._check_gates(sm, cfg, p, guard=guard)
    assert out is not None
    assert "GATE_GUARD_FAILED" in out


# ── create_preview populates 5 binding fields ────────────────────────


def test_create_preview_populates_binding_fields(monkeypatch):
    """create_preview 가 5 필드 모두 채우고 응답에도 포함되는지 검증."""
    sm = _state_mgr()
    p = _provider_clean()
    cfg = _config()
    ra._state.phase = "WINDOW_OPEN"

    target = {
        "target_tickers": ["005930"],
        "scores": {"005930": {"vol_12m": 0.2, "mom_12_1": 0.1}},
        "snapshot_version": "sv-test",
        "date": "20260504",
    }
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio", lambda _d: target,
    )
    fake_sell = SimpleNamespace(ticker="005930", quantity=10)
    fake_buy = SimpleNamespace(ticker="000660", target_amount=1_000_000)
    monkeypatch.setattr(
        "strategy.rebalancer.compute_orders",
        lambda **kw: ([fake_sell], [fake_buy]),
    )

    result = ra.create_preview(sm, cfg, p)

    # Fields populated on _state
    assert ra._state.preview_id != ""
    assert ra._state.snapshot_version == "sv-test"
    assert len(ra._state.target_hash) == 64  # SHA-256 hex
    assert len(ra._state.order_hash) == 64
    assert ra._state.created_at != ""

    # Fields exposed in response
    assert "preview_id" in result
    assert "snapshot_version" in result
    assert "target_hash" in result
    assert "order_hash" in result
    assert "created_at" in result
    assert result["snapshot_version"] == "sv-test"
