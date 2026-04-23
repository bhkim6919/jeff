# -*- coding: utf-8 -*-
"""Unit tests for shared.util.pyc_guard.

Jeff v4 gate:
    - detect_pyc_only_modules() finds orphaned .pyc in allowlist
    - Runtime hook: CRITICAL module missing .py → PycOnlyImportBlocked
    - Runtime hook: non-CRITICAL missing .py → DummyModule + CRITICAL log
    - DummyModule access is traced (no silent pass-through)
    - Modules with .py present → passthrough (finder returns None)
    - Module NOT in allowlist → finder returns None (no interference)
    - Events recorded for marker integration
"""
from __future__ import annotations

import logging
import sys
import textwrap
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "kr"))

from shared.util import pyc_guard
from shared.util.pyc_guard import (
    DummyModule,
    ImportEvent,
    PycGuardFinder,
    PycOnlyImportBlocked,
    detect_pyc_only_modules,
    get_import_events,
    clear_events_for_tests,
    install_import_hook,
    uninstall_import_hook,
)


@pytest.fixture(autouse=True)
def _clean_hook_state():
    uninstall_import_hook()
    clear_events_for_tests()
    yield
    uninstall_import_hook()
    clear_events_for_tests()


def _touch(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


# ---------- Static detection ----------

def test_detect_finds_orphaned_pyc(tmp_path: Path):
    """Repo layout:
        repo/kr/web/data_events.py       ← MISSING
        repo/kr/web/__pycache__/data_events.cpython-312.pyc ← orphaned
    """
    (tmp_path / "kr" / "web" / "__pycache__").mkdir(parents=True)
    _touch(tmp_path / "kr" / "web" / "__init__.py")
    _touch(tmp_path / "kr" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__pycache__" / "data_events.cpython-312.pyc",
           content="fake bytecode")

    events = detect_pyc_only_modules(tmp_path, ["kr.web.data_events"])
    assert len(events) == 1
    assert events[0].module == "kr.web.data_events"
    assert events[0].source_missing is True
    assert events[0].kind == "DETECT"


def test_detect_skips_when_source_present(tmp_path: Path):
    (tmp_path / "kr" / "web" / "__pycache__").mkdir(parents=True)
    _touch(tmp_path / "kr" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "data_events.py", "x = 1")  # source present
    _touch(tmp_path / "kr" / "web" / "__pycache__" / "data_events.cpython-312.pyc")

    events = detect_pyc_only_modules(tmp_path, ["kr.web.data_events"])
    assert events == []


def test_detect_skips_modules_not_in_allowlist(tmp_path: Path):
    (tmp_path / "kr" / "web" / "__pycache__").mkdir(parents=True)
    _touch(tmp_path / "kr" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__pycache__" / "random_mod.cpython-312.pyc")

    events = detect_pyc_only_modules(tmp_path, ["kr.web.data_events"])
    assert events == []  # random_mod not in allowlist


def test_detect_records_events_in_module_log(tmp_path: Path):
    (tmp_path / "kr" / "web" / "__pycache__").mkdir(parents=True)
    _touch(tmp_path / "kr" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__init__.py")
    _touch(tmp_path / "kr" / "web" / "__pycache__" / "data_events.cpython-312.pyc")

    detect_pyc_only_modules(tmp_path, ["kr.web.data_events"])
    evs = get_import_events()
    assert len(evs) == 1
    assert evs[0].module == "kr.web.data_events"


# ---------- Runtime hook: CRITICAL module ----------

def test_critical_module_missing_source_raises(tmp_path: Path, monkeypatch):
    """Jeff v4 필수 2: CRITICAL_MODULES → ImportError (loud crash).

    Synthetic module name to avoid collision with real 'shared.db.csv_loader'
    that exists in this repo.
    """
    # Build a fake repo with NO source for the critical module.
    (tmp_path / "pycg_crit" / "db").mkdir(parents=True)
    _touch(tmp_path / "pycg_crit" / "__init__.py")
    _touch(tmp_path / "pycg_crit" / "db" / "__init__.py")
    # NB: intentionally NOT creating csv_loader.py

    sys.path.insert(0, str(tmp_path))
    try:
        install_import_hook(
            allowlist={"pycg_crit.db.csv_loader"},
            critical={"pycg_crit.db.csv_loader"},
            repo_root=tmp_path,
        )

        for k in list(sys.modules.keys()):
            if k.startswith("pycg_crit"):
                sys.modules.pop(k, None)
        with pytest.raises(PycOnlyImportBlocked, match="pycg_crit.db.csv_loader"):
            __import__("pycg_crit.db.csv_loader")
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        for k in list(sys.modules.keys()):
            if k.startswith("pycg_crit"):
                sys.modules.pop(k, None)

    # Event recorded
    evs = [e for e in get_import_events() if e.kind == "INTERCEPT"]
    assert len(evs) == 1
    assert evs[0].action == "RAISE"
    assert evs[0].module == "pycg_crit.db.csv_loader"


# ---------- Runtime hook: non-CRITICAL DummyModule ----------

def test_non_critical_missing_source_returns_dummy(tmp_path: Path, caplog):
    """Jeff v4 권장: 비핵심 → DummyModule + CRITICAL log (no silent pass).

    Synthetic module name to avoid collision with real kr.web.data_events.
    """
    (tmp_path / "pycg_fake2" / "web").mkdir(parents=True)
    _touch(tmp_path / "pycg_fake2" / "__init__.py")
    _touch(tmp_path / "pycg_fake2" / "web" / "__init__.py")
    # NB: intentionally NOT creating data_events.py

    sys.path.insert(0, str(tmp_path))
    try:
        install_import_hook(
            allowlist={"pycg_fake2.web.data_events"},
            critical=frozenset(),  # explicitly non-critical
            repo_root=tmp_path,
        )

        for k in list(sys.modules.keys()):
            if k.startswith("pycg_fake2"):
                sys.modules.pop(k, None)
        with caplog.at_level(logging.CRITICAL, logger="gen4.pyc_guard"):
            mod = __import__("pycg_fake2.web.data_events", fromlist=["data_events"])

        assert isinstance(mod, DummyModule)
        # CRITICAL log must be emitted (not silent)
        assert any("PYC_GUARD_DUMMY_SUBSTITUTE" in r.message
                   for r in caplog.records)

        # Event recorded
        evs = [e for e in get_import_events() if e.action == "DUMMY"]
        assert len(evs) == 1
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        for k in list(sys.modules.keys()):
            if k.startswith("pycg_fake2"):
                sys.modules.pop(k, None)


def test_dummy_module_attribute_access_logs_critical(caplog):
    """DummyModule attribute access must CRITICAL log — no silent no-op."""
    mod = DummyModule("kr.web.data_events")

    with caplog.at_level(logging.CRITICAL, logger="gen4.pyc_guard"):
        _ = mod.some_function  # trigger __getattr__

    assert any("PYC_GUARD_DUMMY_ACCESS" in r.message for r in caplog.records)
    assert any("some_function" in r.message for r in caplog.records)


def test_dummy_attr_call_logs_critical(caplog):
    """DummyAttr call also logs — ensures call-chain doesn't slip through."""
    mod = DummyModule("kr.web.data_events")

    with caplog.at_level(logging.CRITICAL, logger="gen4.pyc_guard"):
        attr = mod.some_function
        result = attr(1, 2, key="x")

    assert result is None
    assert any("PYC_GUARD_DUMMY_CALL" in r.message for r in caplog.records)


# ---------- Runtime hook: negative cases ----------

def test_source_present_passes_through(tmp_path: Path):
    """If .py source exists, finder returns None → default import works.

    Uses a synthetic top-level pkg name to avoid collision with the already-
    imported 'kr' package in this repo.
    """
    (tmp_path / "pycg_fake" / "sub").mkdir(parents=True)
    _touch(tmp_path / "pycg_fake" / "__init__.py")
    _touch(tmp_path / "pycg_fake" / "sub" / "__init__.py")
    _touch(tmp_path / "pycg_fake" / "sub" / "healthy_mod.py", "VALUE = 42\n")

    sys.path.insert(0, str(tmp_path))
    try:
        install_import_hook(
            allowlist={"pycg_fake.sub.healthy_mod"},
            critical={"pycg_fake.sub.healthy_mod"},  # even if "critical"
            repo_root=tmp_path,
        )
        for k in list(sys.modules.keys()):
            if k.startswith("pycg_fake"):
                sys.modules.pop(k, None)
        mod = __import__("pycg_fake.sub.healthy_mod", fromlist=["healthy_mod"])
        assert mod.VALUE == 42  # real module loaded, not DummyModule
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        for k in list(sys.modules.keys()):
            if k.startswith("pycg_fake"):
                sys.modules.pop(k, None)


def test_not_in_allowlist_passes_through(tmp_path: Path):
    """Modules outside allowlist are completely ignored by the finder."""
    install_import_hook(
        allowlist={"kr.web.data_events"},
        critical=frozenset(),
        repo_root=tmp_path,
    )
    # 'json' stdlib — not in allowlist, must import normally
    sys.modules.pop("json", None)
    import json
    assert hasattr(json, "loads")


# ---------- Jeff gate: "조용히 지나가면 안 된다" verification ----------

def test_dummy_module_cannot_silently_substitute_real_behavior():
    """Critical invariant: operating on DummyModule must leave audit trail.

    Scenario: legacy code does `from foo import some_func; some_func(x)`.
    With Dummy substitute, some_func returns None AND logs 2x CRITICAL.
    """
    clear_events_for_tests()
    logger = logging.getLogger("gen4.pyc_guard")
    handler = _CollectingHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.CRITICAL)

    try:
        mod = DummyModule("kr.web.data_events")
        some_func = mod.some_func     # 1 CRITICAL (access)
        result = some_func("x")        # 2 CRITICAL (call)

        messages = [r.getMessage() for r in handler.records]
        assert any("PYC_GUARD_DUMMY_ACCESS" in m for m in messages)
        assert any("PYC_GUARD_DUMMY_CALL" in m for m in messages)
        assert result is None  # no silent real-seeming return value
    finally:
        logger.removeHandler(handler)


class _CollectingHandler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.CRITICAL)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
