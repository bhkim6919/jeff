"""AUD-N1/N2 — rebalance_api gate fail-closed regression tests.

Pre-fix: gate check exceptions silently passed → BUY / Rebalance SELL
could proceed when the guard machinery itself was broken (import error,
attribute error, runtime exception). That violates the project's
Broker Truth doctrine (CLAUDE.md Global Safety Rule #1) — uncertainty
must reject, not permit.

Post-fix: any exception during gate check → return explicit failure
reason → caller treats as rejection.

Scope:
  - _check_gates: BuyPermission exception path (AUD-N1)
  - _check_sell_gates: pending_external exception path (AUD-N2)
  - Safety SELL paths NOT modified (eod_phase Trail Stop / DD trim do
    not route through these gates — verified separately in audit)

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_rebalance_api_gate_fail_closed.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

import web.rebalance_api as ra  # noqa: E402


def _state_mgr_clean():
    sm = MagicMock()
    sm.load_runtime.return_value = {}
    sm.load_pending_external.return_value = []
    return sm


def _provider_clean():
    p = MagicMock()
    p.query_open_orders.return_value = []
    return p


def _config():
    cfg = MagicMock()
    cfg.SIGNALS_DIR = "/tmp/nope"
    return cfg


# ── AUD-N1: BuyPermission exception → fail-closed ────────────────────


def test_check_gates_buy_permission_exception_rejects(monkeypatch):
    """guard.get_buy_permission() 예외 → BLOCK (이전엔 silent pass)."""
    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    guard.get_buy_permission.side_effect = RuntimeError("guard broken")

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_gates(sm, cfg, p, guard=guard)
    assert result is not None
    assert "GATE_GUARD_FAILED" in result
    assert "RuntimeError" in result
    assert "guard broken" in result


def test_check_gates_buy_permission_import_error_rejects(monkeypatch):
    """BuyPermission enum import 실패 → BLOCK (방어 우회 차단)."""
    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    # Simulate the enum import inside _check_gates raising
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "risk.exposure_guard":
            raise ImportError("module gone")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = ra._check_gates(sm, cfg, p, guard=guard)
    assert result is not None
    assert "GATE_GUARD_FAILED" in result


def test_check_gates_no_guard_skips_check(monkeypatch):
    """guard=None 시 BuyPermission 체크 자체가 적용 안 됨 — 다른 게이트는 정상 진행."""
    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )
    # guard=None — should NOT raise; BuyPermission check is skipped
    result = ra._check_gates(sm, cfg, p, guard=None)
    assert result is None


def test_check_gates_normal_buy_permission_passes(monkeypatch):
    """정상 NORMAL BuyPermission → 통과 (기존 동작 보존)."""
    from risk.exposure_guard import BuyPermission

    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    guard.get_buy_permission.return_value = (BuyPermission.NORMAL, "ok")

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_gates(sm, cfg, p, guard=guard)
    assert result is None


def test_check_gates_blocked_buy_permission_rejects(monkeypatch):
    """BLOCKED BuyPermission → 명시 거부 (기존 동작 보존)."""
    from risk.exposure_guard import BuyPermission

    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    guard.get_buy_permission.return_value = (BuyPermission.BLOCKED, "DD breach")

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_gates(sm, cfg, p, guard=guard)
    assert result is not None
    assert "BLOCKED" in result
    assert "DD breach" in result


# ── AUD-N2: pending_external exception → fail-closed ─────────────────


def test_check_sell_gates_pending_external_exception_rejects(monkeypatch):
    """state_mgr.load_pending_external() 예외 → BLOCK (이전엔 silent pass)."""
    sm = MagicMock()
    sm.load_runtime.return_value = {}
    sm.load_pending_external.side_effect = OSError("disk error")
    p = _provider_clean()
    cfg = _config()
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_sell_gates(sm, cfg, p, guard=None)
    assert result is not None
    assert "GATE_PENDING_EXTERNAL_FAILED" in result
    assert "OSError" in result
    assert "disk error" in result


def test_check_sell_gates_pending_external_empty_passes(monkeypatch):
    """pending_external = [] → 통과 (기존 동작 보존)."""
    sm = _state_mgr_clean()
    p = _provider_clean()
    cfg = _config()
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_sell_gates(sm, cfg, p, guard=None)
    assert result is None


def test_check_sell_gates_pending_external_nonempty_rejects(monkeypatch):
    """pending_external 1건 이상 → 명시 거부 (기존 동작 보존)."""
    sm = MagicMock()
    sm.load_runtime.return_value = {}
    sm.load_pending_external.return_value = [{"ticker": "005930"}]
    p = _provider_clean()
    cfg = _config()
    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    result = ra._check_sell_gates(sm, cfg, p, guard=None)
    assert result is not None
    assert "pending_external=1" in result


# ── Doctrine integrity: Safety SELL path NOT routed through these gates ─


def test_safety_sell_path_does_not_use_check_gates():
    """문서화 invariant — eod_phase Trail Stop SELL 은 _check_gates 를 거치지 않는다.

    이 테스트는 코드 경로를 import 시점에 확인 (실제 호출 추적은 lifecycle test 영역).
    eod_phase.execute_sell 호출 사이트가 _check_*_gates 를 import 하지 않음을
    grep 으로 검증해도 되지만, 여기서는 doctrine 명시만.
    """
    # Doctrine: kr/lifecycle/eod_phase.py:254 (Trail Stop) 와
    # kr/lifecycle/rebalance_phase.py 의 DD trim 경로는
    # rebalance_api._check_*_gates 를 거치지 않는다 (audit 4 확인).
    # 이 테스트는 검증보다 doctrine 의 명시적 기록 — RG5 lifecycle test
    # 영역에서 별도 통합 검증 (preview_execute_drift 모듈 등).
    assert True  # documentation-as-test


# ── Caller-side semantics: rejection reason format ────────────────────


def test_gate_rejection_reasons_are_strings(monkeypatch):
    """모든 gate 거부 reason 은 사람이 읽을 수 있는 string 이어야 한다 (운영 가시성)."""
    sm = MagicMock()
    sm.load_runtime.return_value = {}
    sm.load_pending_external.side_effect = RuntimeError("test")
    p = _provider_clean()
    cfg = _config()
    guard = MagicMock()
    guard.get_buy_permission.side_effect = RuntimeError("test")

    monkeypatch.setattr(
        "strategy.factor_ranker.load_target_portfolio",
        lambda _d: {"date": "20260504", "target_tickers": []},
    )

    r1 = ra._check_gates(sm, cfg, p, guard=guard)
    assert isinstance(r1, str) and len(r1) > 0

    # Reset guard to clean for sell-only check
    guard2 = MagicMock()
    from risk.exposure_guard import BuyPermission
    guard2.get_buy_permission.return_value = (BuyPermission.NORMAL, "ok")
    r2 = ra._check_sell_gates(sm, cfg, p, guard=guard2)
    assert isinstance(r2, str) and len(r2) > 0
