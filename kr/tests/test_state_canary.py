"""Tests for kr/lifecycle/state_canary.py.

Pins behaviour for the 04-30 multi-file deletion pattern:

    * portfolio_state_live.json    missing / empty / unparseable
    * runtime_state_live.json      missing / empty / unparseable
    * backtest/data_full/ohlcv/    directory missing / under threshold
    * backtest/data_full/index/KOSPI.csv   missing / truncated

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_state_canary.py -v
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

from lifecycle import state_canary  # noqa: E402


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_canary_state():
    state_canary.reset_for_test()
    yield
    state_canary.reset_for_test()


@pytest.fixture
def fake_repo(tmp_path: Path) -> Path:
    """Build a minimal Q-TRON-shaped tree under tmp_path."""
    (tmp_path / "kr" / "state").mkdir(parents=True)
    (tmp_path / "backtest" / "data_full" / "ohlcv").mkdir(parents=True)
    (tmp_path / "backtest" / "data_full" / "index").mkdir(parents=True)
    (tmp_path / "backup" / "reports" / "incidents").mkdir(parents=True)

    # Healthy defaults — tests overwrite specifically.
    (tmp_path / "kr" / "state" / "portfolio_state_live.json").write_text(
        json.dumps({"cash": 1_000_000, "positions": {}}), encoding="utf-8")
    (tmp_path / "kr" / "state" / "runtime_state_live.json").write_text(
        json.dumps({"timestamp": "2026-04-30T09:45:00",
                    "shutdown_reason": "running"}), encoding="utf-8")
    (tmp_path / "backtest" / "data_full" / "index" / "KOSPI.csv").write_text(
        "x" * 200_000, encoding="utf-8")  # ~200 KB, well above truncated threshold
    # Drop ≥ 2,500 dummy CSVs so the OHLCV count check passes.
    ohlcv = tmp_path / "backtest" / "data_full" / "ohlcv"
    for i in range(2600):
        (ohlcv / f"{i:06d}.csv").write_text("date,close\n", encoding="utf-8")

    return tmp_path


def _make_config(repo: Path) -> SimpleNamespace:
    """Mimic the Gen4Config attribute the canary actually reads."""
    return SimpleNamespace(BASE_DIR=repo / "kr")


def _spy_telegram(monkeypatch) -> list[tuple[str, str]]:
    """Patch the telegram path to record calls instead of sending."""
    calls: list[tuple[str, str]] = []

    def _fake_send(text, severity="INFO"):
        calls.append((severity, text))
        return True

    fake_module = SimpleNamespace(send=_fake_send)
    monkeypatch.setitem(sys.modules, "notify", SimpleNamespace(telegram_bot=fake_module))
    monkeypatch.setitem(sys.modules, "notify.telegram_bot", fake_module)
    return calls


# ── Healthy path ─────────────────────────────────────────────────────


def test_canary_passes_on_healthy_tree(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is True
    assert calls == []


# ── Per-file failures ────────────────────────────────────────────────


def test_canary_fires_on_missing_portfolio_state(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").unlink()
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert len(calls) == 1
    sev, text = calls[0]
    assert sev == "CRITICAL"
    assert "STATE_CANARY_TRIGGERED" in text
    assert "portfolio_state_live.json" in text
    assert "missing" in text


def test_canary_fires_on_empty_state_file(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "runtime_state_live.json").write_text("", encoding="utf-8")
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "runtime_state_live.json" in calls[0][1]
    assert "empty" in calls[0][1]


def test_canary_fires_on_unparseable_state_file(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").write_text(
        "{not: valid", encoding="utf-8")
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "portfolio_state_live.json" in calls[0][1]
    assert "unreadable" in calls[0][1]


def test_canary_fires_on_missing_ohlcv_dir(fake_repo, monkeypatch):
    """The exact 04-30 KR_BATCH preflight failure scenario."""
    calls = _spy_telegram(monkeypatch)
    import shutil
    shutil.rmtree(fake_repo / "backtest" / "data_full" / "ohlcv")
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "backtest/data_full/ohlcv" in calls[0][1]
    assert "directory missing" in calls[0][1]


def test_canary_fires_on_ohlcv_below_threshold(fake_repo, monkeypatch):
    """e.g. partial deletion or truncation that drops count below
    the preflight `MIN_CSV_COUNT` (2500)."""
    calls = _spy_telegram(monkeypatch)
    ohlcv = fake_repo / "backtest" / "data_full" / "ohlcv"
    for csv in list(ohlcv.glob("*.csv"))[:200]:  # leave 2400 (below 2500 threshold)
        csv.unlink()
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "csv_count=2400" in calls[0][1]


def test_canary_fires_on_missing_kospi_csv(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "backtest" / "data_full" / "index" / "KOSPI.csv").unlink()
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "KOSPI.csv" in calls[0][1]


def test_canary_fires_on_truncated_kospi_csv(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "backtest" / "data_full" / "index" / "KOSPI.csv").write_text(
        "tiny", encoding="utf-8")
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert "KOSPI.csv" in calls[0][1]
    assert "truncated" in calls[0][1]


# ── Multi-failure (the 04-30 scenario) ───────────────────────────────


def test_canary_collects_multiple_failures_in_one_alert(fake_repo, monkeypatch):
    """Reproduces 04-30: state files vanish AND OHLCV dir vanishes
    in a single sweep. Canary must report both in a single Telegram
    message, not fire twice."""
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").unlink()
    (fake_repo / "kr" / "state" / "runtime_state_live.json").unlink()
    import shutil
    shutil.rmtree(fake_repo / "backtest" / "data_full" / "ohlcv")
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    assert len(calls) == 1
    text = calls[0][1]
    assert "portfolio_state_live.json" in text
    assert "runtime_state_live.json" in text
    assert "backtest/data_full/ohlcv" in text


# ── Dedup / idempotency ──────────────────────────────────────────────


def test_canary_does_not_double_fire_in_same_process(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").unlink()
    state_canary.run_state_canary(_make_config(fake_repo))
    state_canary.run_state_canary(_make_config(fake_repo))
    state_canary.run_state_canary(_make_config(fake_repo))
    assert len(calls) == 1


def test_reset_for_test_re_arms_canary(fake_repo, monkeypatch):
    calls = _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").unlink()
    state_canary.run_state_canary(_make_config(fake_repo))
    assert len(calls) == 1
    state_canary.reset_for_test()
    state_canary.run_state_canary(_make_config(fake_repo))
    assert len(calls) == 2


# ── Forensic snapshot ────────────────────────────────────────────────


def test_canary_writes_forensic_snapshot_on_fire(fake_repo, monkeypatch):
    _spy_telegram(monkeypatch)
    (fake_repo / "kr" / "state" / "portfolio_state_live.json").unlink()
    incidents = fake_repo / "backup" / "reports" / "incidents"
    before = list(incidents.glob("*_state_canary.md"))
    state_canary.run_state_canary(_make_config(fake_repo))
    after = list(incidents.glob("*_state_canary.md"))
    assert len(after) == len(before) + 1
    body = after[-1].read_text(encoding="utf-8")
    # Failure detail is in the markdown.
    assert "portfolio_state_live.json" in body
    assert "missing" in body
    # Listings of the relevant directories are included for forensics.
    assert "kr/state" in body
    assert "backtest/data_full" in body


def test_canary_does_not_write_snapshot_on_pass(fake_repo, monkeypatch):
    _spy_telegram(monkeypatch)
    incidents = fake_repo / "backup" / "reports" / "incidents"
    before = list(incidents.glob("*_state_canary.md"))
    state_canary.run_state_canary(_make_config(fake_repo))
    after = list(incidents.glob("*_state_canary.md"))
    assert len(after) == len(before)


# ── Robustness ───────────────────────────────────────────────────────


def test_canary_returns_false_on_internal_exception(fake_repo, monkeypatch):
    """Unexpected exception path — canary must NOT propagate, must NOT
    block startup, but should still return False so the caller knows
    the check did not succeed."""
    calls = _spy_telegram(monkeypatch)

    def _explode(*_a, **_kw):
        raise RuntimeError("simulated internal failure")

    monkeypatch.setattr(state_canary, "_check_state_file", _explode)
    ok = state_canary.run_state_canary(_make_config(fake_repo))
    assert ok is False
    # Telegram still fires with the internal_error label.
    assert any("internal_error" in t for _, t in calls)


def test_canary_handles_missing_backup_dir(tmp_path, monkeypatch):
    """If the incident dir doesn't exist the canary creates it; if
    creation fails the canary still completes (returns False) without
    raising."""
    calls = _spy_telegram(monkeypatch)
    # Build a partial tree — no backup/ directory at all.
    (tmp_path / "kr" / "state").mkdir(parents=True)
    (tmp_path / "backtest" / "data_full" / "ohlcv").mkdir(parents=True)
    (tmp_path / "backtest" / "data_full" / "index").mkdir(parents=True)
    cfg = SimpleNamespace(BASE_DIR=tmp_path / "kr")
    ok = state_canary.run_state_canary(cfg)
    # Multiple failures expected (state files missing, KOSPI missing,
    # OHLCV under threshold). Telegram still fires once.
    assert ok is False
    assert len(calls) == 1
