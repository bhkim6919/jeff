# -*- coding: utf-8 -*-
"""Unit tests for shared.util.truth_guard.

Covers v4 §필수 1 mandate:
 - Consumer module → forbidden path → ForbiddenTruthSource
 - Producer/non-consumer → forbidden path → passthrough (no false positive)
 - Consumer → non-forbidden path → passthrough (pandas/logging oversight)
 - Stack-walk: consumer calling library that opens file → still blocked
   if library attempts a forbidden pattern, else passthrough
 - Idempotent activation
 - Deactivation restores builtins.open (test hygiene)
"""
from __future__ import annotations

import builtins
import importlib
import sys
from pathlib import Path

import pytest

# Path setup so `kr.pipeline.completion_schema` resolves for truth_guard
# when it lazy-imports FORBIDDEN_TRUTH_PATTERNS.
_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "kr"))

from shared.util import truth_guard


@pytest.fixture(autouse=True)
def _clean_guard_state():
    """Ensure no guard state leaks across tests."""
    truth_guard.deactivate_for_tests()
    yield
    truth_guard.deactivate_for_tests()


# ---------- Installation / lifecycle ----------

def test_not_installed_by_default():
    assert truth_guard.is_installed() is False
    # builtins.open still the original
    assert builtins.open is truth_guard._ORIGINAL_OPEN  # type: ignore[attr-defined]


def test_activation_installs_patch():
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    assert truth_guard.is_installed() is True
    assert "kr.pipeline.notify" in truth_guard.active_consumers()
    assert builtins.open is not truth_guard._ORIGINAL_OPEN  # type: ignore[attr-defined]


def test_activation_idempotent():
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    truth_guard.activate_consumer_mode("kr.pipeline.watchdog")
    truth_guard.activate_consumer_mode("kr.pipeline.notify")  # same twice
    consumers = truth_guard.active_consumers()
    assert "kr.pipeline.notify" in consumers
    assert "kr.pipeline.watchdog" in consumers
    assert truth_guard.is_installed()


def test_deactivate_restores_original_open():
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    truth_guard.deactivate_for_tests()
    assert truth_guard.is_installed() is False
    assert builtins.open is truth_guard._ORIGINAL_OPEN  # type: ignore[attr-defined]


# ---------- Path matching: forbidden patterns ----------

def test_path_matcher_state_file():
    truth_guard._FORBIDDEN_REGEX, _ = truth_guard._load_patterns()
    assert truth_guard._path_matches_forbidden("state_20260422.json")
    assert truth_guard._path_matches_forbidden("kr/data/pipeline/state_20260422.json")


def test_path_matcher_head_json():
    truth_guard._FORBIDDEN_REGEX, _ = truth_guard._load_patterns()
    assert truth_guard._path_matches_forbidden("kr/data/lab_live/head.json")
    assert truth_guard._path_matches_forbidden("C:\\path\\to\\head.json")


def test_path_matcher_log_files():
    truth_guard._FORBIDDEN_REGEX, _ = truth_guard._load_patterns()
    assert truth_guard._path_matches_forbidden("kr/logs/gen4_batch_20260422.log")
    assert truth_guard._path_matches_forbidden("kr/logs/gen4_live_20260422.log")


def test_path_matcher_allows_marker_and_heartbeat():
    """Marker + heartbeat are NOT forbidden — those are the canonical truth."""
    truth_guard._FORBIDDEN_REGEX, _ = truth_guard._load_patterns()
    assert not truth_guard._path_matches_forbidden("run_completion_20260422.json")
    assert not truth_guard._path_matches_forbidden("heartbeat.json")
    assert not truth_guard._path_matches_forbidden("heartbeat.bak.json")


def test_path_matcher_allows_unrelated_files():
    """pandas/logging/data files must pass through."""
    truth_guard._FORBIDDEN_REGEX, _ = truth_guard._load_patterns()
    assert not truth_guard._path_matches_forbidden("data.csv")
    assert not truth_guard._path_matches_forbidden("KOSPI.csv")
    assert not truth_guard._path_matches_forbidden("equity.json")
    assert not truth_guard._path_matches_forbidden("trades.json")
    assert not truth_guard._path_matches_forbidden("/var/log/app.log")


# ---------- Consumer-stack detection ----------

def test_consumer_from_registered_prefix_gets_blocked(tmp_path: Path):
    """Simulate: module 'kr.pipeline.notify' opens state_*.json → block."""
    truth_guard.activate_consumer_mode("kr.pipeline.notify")

    state_file = tmp_path / "state_20260422.json"
    state_file.write_text("{}", encoding="utf-8")

    # Inject a fake frame by importing from a module named after the consumer
    # prefix — the simplest way is to exec code in that module's namespace.
    code = compile(
        "from builtins import open as _o\n"
        "_o(str(p), 'r').close()\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.notify", "p": state_file}
    with pytest.raises(truth_guard.ForbiddenTruthSource):
        exec(code, fake_globals)


def test_non_consumer_caller_passes_through(tmp_path: Path):
    """Producer (e.g. kr.pipeline.orchestrator) opening state_*.json is fine."""
    truth_guard.activate_consumer_mode("kr.pipeline.notify")

    state_file = tmp_path / "state_20260422.json"
    state_file.write_text("{}", encoding="utf-8")

    # Caller is this test module (kr.tests.util.test_truth_guard) which is
    # NOT in TRUTH_GUARD_CONSUMER_PREFIXES → should pass.
    with open(str(state_file), "r") as f:
        assert f.read() == "{}"


def test_consumer_opens_non_forbidden_path_passes(tmp_path: Path):
    """pandas/logging case: consumer opens data.csv → must pass through."""
    truth_guard.activate_consumer_mode("kr.pipeline.notify")

    data_file = tmp_path / "data.csv"
    data_file.write_text("a,b\n1,2\n", encoding="utf-8")

    code = compile(
        "from builtins import open as _o\n"
        "with _o(str(p), 'r') as f:\n"
        "    txt = f.read()\n"
        "assert 'a,b' in txt\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.notify", "p": data_file}
    exec(code, fake_globals)  # must not raise


def test_consumer_reading_head_json_blocked(tmp_path: Path):
    truth_guard.activate_consumer_mode("kr.pipeline.watchdog")

    head_file = tmp_path / "head.json"
    head_file.write_text("{}", encoding="utf-8")

    code = compile(
        "from builtins import open as _o\n"
        "_o(str(p), 'r').close()\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.watchdog", "p": head_file}
    with pytest.raises(truth_guard.ForbiddenTruthSource):
        exec(code, fake_globals)


def test_consumer_reading_log_file_blocked(tmp_path: Path):
    """gen4_batch_*.log / gen4_live_*.log are log-as-truth anti-pattern."""
    truth_guard.activate_consumer_mode("kr.pipeline.watchdog")

    log_file = tmp_path / "gen4_batch_20260422.log"
    log_file.write_text("", encoding="utf-8")

    code = compile(
        "from builtins import open as _o\n"
        "_o(str(p), 'r').close()\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.watchdog", "p": log_file}
    with pytest.raises(truth_guard.ForbiddenTruthSource):
        exec(code, fake_globals)


# ---------- Stack-walk: indirect calls ----------

def test_library_function_called_from_consumer_is_blocked_on_forbidden(tmp_path: Path):
    """Consumer → library helper → open(state_*.json) → block.

    Even though the direct caller of open() is a library frame, stack walk
    finds the consumer frame higher up → raises. This is desired: consumer
    responsibility propagates through helpers.
    """
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    state_file = tmp_path / "state_20260422.json"
    state_file.write_text("{}", encoding="utf-8")

    # Library-like helper in a non-consumer module
    def _lib_helper(path):
        with open(str(path), "r") as f:
            return f.read()

    # Inject consumer frame via exec with notify-module globals, which calls
    # the helper defined in THIS test module.
    code = compile(
        "_lib_helper(p)",
        "<test>",
        "exec",
    )
    fake_globals = {
        "__name__": "kr.pipeline.notify",
        "p": state_file,
        "_lib_helper": _lib_helper,
    }
    with pytest.raises(truth_guard.ForbiddenTruthSource):
        exec(code, fake_globals)


def test_library_from_consumer_opens_non_forbidden_passes(tmp_path: Path):
    """Consumer → library → open(regular_file) → passthrough.

    This is the pandas/logging oversight prevention test: library calls
    from consumer contexts should NOT be falsely blocked for normal files.
    """
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    data_file = tmp_path / "equity.json"
    data_file.write_text('{"value": 100}', encoding="utf-8")

    def _lib_helper(path):
        with open(str(path), "r") as f:
            return f.read()

    code = compile(
        "result = _lib_helper(p)",
        "<test>",
        "exec",
    )
    fake_globals = {
        "__name__": "kr.pipeline.notify",
        "p": data_file,
        "_lib_helper": _lib_helper,
        "result": None,
    }
    exec(code, fake_globals)
    assert fake_globals["result"] == '{"value": 100}'


# ---------- Sub-module prefix matching ----------

def test_submodule_of_consumer_prefix_also_blocked(tmp_path: Path):
    """'kr.pipeline.notify.telegram' should be treated as consumer."""
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    state_file = tmp_path / "state_20260422.json"
    state_file.write_text("{}", encoding="utf-8")

    code = compile(
        "from builtins import open as _o\n_o(str(p), 'r').close()\n",
        "<test>",
        "exec",
    )
    # Submodule of notify — prefix match via '.' rule
    fake_globals = {"__name__": "kr.pipeline.notify.telegram", "p": state_file}
    with pytest.raises(truth_guard.ForbiddenTruthSource):
        exec(code, fake_globals)


def test_similarly_named_non_consumer_not_blocked(tmp_path: Path):
    """'kr.pipeline.notify_helper' (no '.') must NOT be treated as consumer —
    prefix match requires exact name or name + '.' separator.
    """
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    state_file = tmp_path / "state_20260422.json"
    state_file.write_text("{}", encoding="utf-8")

    code = compile(
        "from builtins import open as _o\nf = _o(str(p), 'r')\nf.close()\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.notify_helper", "p": state_file}
    exec(code, fake_globals)  # must NOT raise — not a consumer


# ---------- Jeff gate: producer write not blocked ----------

def test_producer_write_to_state_file_not_blocked(tmp_path: Path):
    """Jeff gate A1-3: producer write (orchestrator path) must pass.

    Orchestrator/state.py writes state_*.json — that module is NOT in
    TRUTH_GUARD_CONSUMER_PREFIXES, so write through builtins.open is fine.
    """
    truth_guard.activate_consumer_mode("kr.pipeline.notify")
    state_file = tmp_path / "state_20260422.json"

    # Simulate a producer module writing
    code = compile(
        "from builtins import open as _o\n"
        "with _o(str(p), 'w') as f:\n"
        "    f.write('{\"schema_version\": 1}')\n",
        "<test>",
        "exec",
    )
    fake_globals = {"__name__": "kr.pipeline.state", "p": state_file}
    exec(code, fake_globals)  # must not raise
    assert state_file.read_text(encoding="utf-8") == '{"schema_version": 1}'
