"""Tests for PR 3 (AUD-P1-D): batch inter-process lock + atomic checkpoint.

Verifies:
  - _acquire_batch_lock succeeds on clean state
  - Second acquire while held by alive PID is rejected
  - Stale lock (dead PID) recovers
  - Stale lock (> 30 min old) recovers
  - Unparseable lock recovers
  - _release_batch_lock removes only owner's lock
  - _atomic_write_json is crash-safe (tmp pattern)
  - _save_checkpoint atomic write end-to-end

Run from repo root::

    .venv64/Scripts/python.exe -m pytest kr/tests/test_batch_lock.py -v
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

KR_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KR_ROOT))

from lifecycle import batch as batch_mod  # noqa: E402


def _config(tmp_path):
    """Minimal config with OHLCV_DIR pointed at tmp_path."""
    ohlcv = tmp_path / "ohlcv"
    ohlcv.mkdir()
    return SimpleNamespace(OHLCV_DIR=ohlcv)


@pytest.fixture
def cfg(tmp_path):
    return _config(tmp_path)


@pytest.fixture
def logger():
    lg = logging.getLogger("test.batch_lock")
    lg.setLevel(logging.DEBUG)
    return lg


# ── _atomic_write_json ────────────────────────────────────────────────


def test_atomic_write_json_basic(tmp_path):
    p = tmp_path / "x.json"
    batch_mod._atomic_write_json(p, {"a": 1, "b": [1, 2]})
    assert json.loads(p.read_text(encoding="utf-8")) == {"a": 1, "b": [1, 2]}


def test_atomic_write_json_uses_tmp_then_rename(tmp_path):
    p = tmp_path / "x.json"
    batch_mod._atomic_write_json(p, {"a": 1})
    # tmp should be cleaned up after rename
    assert not (tmp_path / "x.json.tmp").exists()
    assert p.exists()


def test_atomic_write_json_overwrites(tmp_path):
    p = tmp_path / "x.json"
    p.write_text('{"old": true}')
    batch_mod._atomic_write_json(p, {"new": True})
    assert json.loads(p.read_text(encoding="utf-8")) == {"new": True}


# ── _acquire_batch_lock ───────────────────────────────────────────────


def test_acquire_lock_clean(cfg, logger):
    assert batch_mod._acquire_batch_lock(cfg, logger) is True
    lock_p = batch_mod._batch_lock_path(cfg)
    assert lock_p.exists()
    data = json.loads(lock_p.read_text(encoding="utf-8"))
    assert data["pid"] == os.getpid()


def test_acquire_lock_rejects_when_held_by_alive_pid(cfg, logger):
    # Pre-write a lock owned by current PID with fresh timestamp
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    batch_mod._atomic_write_json(lock_p, {
        "pid": os.getpid(),  # alive
        "started_at": datetime.now().isoformat(timespec="seconds"),
    })
    # Second acquire should be rejected
    assert batch_mod._acquire_batch_lock(cfg, logger) is False


def test_acquire_lock_recovers_stale_dead_pid(cfg, logger):
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    # PID 1 typically not the test process; combined with low pid OR
    # explicit dead PID via mock.
    fake_dead_pid = 999_999_999
    batch_mod._atomic_write_json(lock_p, {
        "pid": fake_dead_pid,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    })
    with patch.object(batch_mod, "_pid_alive", return_value=False):
        assert batch_mod._acquire_batch_lock(cfg, logger) is True
    # Now lock should be ours
    data = json.loads(lock_p.read_text(encoding="utf-8"))
    assert data["pid"] == os.getpid()


def test_acquire_lock_recovers_stale_old_timestamp(cfg, logger):
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    # Even with alive PID (current process), if started_at is > 30 min old
    # we treat as stale (process likely orphaned the lock)
    old_ts = (datetime.now() - timedelta(hours=2)).isoformat(timespec="seconds")
    batch_mod._atomic_write_json(lock_p, {
        "pid": os.getpid(),
        "started_at": old_ts,
    })
    # Force pid_alive=True so the only stale signal is timestamp
    with patch.object(batch_mod, "_pid_alive", return_value=True):
        assert batch_mod._acquire_batch_lock(cfg, logger) is True


def test_acquire_lock_recovers_unparseable(cfg, logger):
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    lock_p.write_text("not json garbage", encoding="utf-8")
    assert batch_mod._acquire_batch_lock(cfg, logger) is True


def test_acquire_lock_recovers_missing_started_at(cfg, logger):
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    batch_mod._atomic_write_json(lock_p, {"pid": os.getpid()})
    # Missing started_at — treat as stale_time=True
    with patch.object(batch_mod, "_pid_alive", return_value=True):
        assert batch_mod._acquire_batch_lock(cfg, logger) is True


# ── _release_batch_lock ───────────────────────────────────────────────


def test_release_owner_lock_removes_file(cfg, logger):
    assert batch_mod._acquire_batch_lock(cfg, logger) is True
    lock_p = batch_mod._batch_lock_path(cfg)
    assert lock_p.exists()
    batch_mod._release_batch_lock(cfg, logger)
    assert not lock_p.exists()


def test_release_non_owner_leaves_lock(cfg, logger):
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    other_pid = os.getpid() + 999_999  # unlikely to match
    batch_mod._atomic_write_json(lock_p, {
        "pid": other_pid,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    })
    batch_mod._release_batch_lock(cfg, logger)
    # Lock should still be there (we don't own it)
    assert lock_p.exists()


def test_release_no_lock_is_noop(cfg, logger):
    # Should not raise
    batch_mod._release_batch_lock(cfg, logger)
    assert not batch_mod._batch_lock_path(cfg).exists()


# ── _save_checkpoint atomic ───────────────────────────────────────────


def test_save_checkpoint_atomic_no_partial_file(cfg):
    cp = {"date": "2026-05-04", "completed_steps": ["step1"]}
    batch_mod._save_checkpoint(cfg, "step2", cp)
    cp_path = Path(cfg.OHLCV_DIR).parent / "batch_checkpoint.json"
    assert cp_path.exists()
    data = json.loads(cp_path.read_text(encoding="utf-8"))
    assert "step1" in data["completed_steps"]
    assert "step2" in data["completed_steps"]
    assert data["last_step"] == "step2"
    # No tmp file remaining
    assert not (cp_path.parent / "batch_checkpoint.json.tmp").exists()


def test_save_checkpoint_overwrites_existing(cfg):
    cp_path = Path(cfg.OHLCV_DIR).parent / "batch_checkpoint.json"
    cp_path.write_text('{"old": true}', encoding="utf-8")
    cp = {"date": "2026-05-04", "completed_steps": []}
    batch_mod._save_checkpoint(cfg, "step1", cp)
    data = json.loads(cp_path.read_text(encoding="utf-8"))
    assert data["completed_steps"] == ["step1"]


# ── End-to-end concurrent simulation ──────────────────────────────────


def test_two_concurrent_acquires_only_one_succeeds(cfg, logger):
    # Process A acquires
    assert batch_mod._acquire_batch_lock(cfg, logger) is True
    # Process B (simulated by NOT patching _pid_alive so it sees us as alive)
    # tries — should fail
    assert batch_mod._acquire_batch_lock(cfg, logger) is False
    # Cleanup
    batch_mod._release_batch_lock(cfg, logger)


def test_after_release_next_acquire_succeeds(cfg, logger):
    assert batch_mod._acquire_batch_lock(cfg, logger) is True
    batch_mod._release_batch_lock(cfg, logger)
    assert batch_mod._acquire_batch_lock(cfg, logger) is True
    batch_mod._release_batch_lock(cfg, logger)


def test_run_batch_returns_none_on_lock_conflict(cfg, monkeypatch):
    """run_batch wrapper returns None when lock cannot be acquired —
    callers (tray_server / pipeline) treat falsy as 'no result'."""
    # Hold lock with a PID we'll mock as alive
    lock_p = batch_mod._batch_lock_path(cfg)
    lock_p.parent.mkdir(parents=True, exist_ok=True)
    batch_mod._atomic_write_json(lock_p, {
        "pid": os.getpid(),
        "started_at": datetime.now().isoformat(timespec="seconds"),
    })

    def _should_not_be_called(*a, **kw):
        raise RuntimeError("inner body must not run when lock held")

    monkeypatch.setattr(batch_mod, "_run_batch_inner", _should_not_be_called)
    result = batch_mod.run_batch(cfg)
    assert result is None
    # Lock should still be held by "the other process" — release manually
    lock_p.unlink()
