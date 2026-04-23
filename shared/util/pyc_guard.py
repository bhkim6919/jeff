# -*- coding: utf-8 -*-
"""shared/util/pyc_guard.py — Guard against .pyc-only (source-missing) modules.

Two layers, both mandatory per v3 §Hardening-4 + v4 §필수 2:

    1. Preflight detection (static):
       `detect_pyc_only_modules()` walks __pycache__ directories under
       the repo root and reports modules whose .pyc exists without a
       .py sibling. Called by preflight (Phase B) to block EOD until
       source is restored. Also used by orchestrator.tick() at startup
       to register known_bombs in the completion marker.

    2. Runtime import hook (dynamic):
       `install_import_hook()` inserts a PycGuardFinder into sys.meta_path.
       When code attempts to import an allowlisted module whose .py is
       missing:
         - if module ∈ PYC_CRITICAL_MODULES → raise PycOnlyImportBlocked
           (ImportError subclass — legacy callers that wrap imports in
           try/except still catch it; unwrapped callers crash loud)
         - else → log CRITICAL, record event, return a DummyModule that
           logs CRITICAL on every attribute access (no silent pass-through)

The DummyModule is intentionally noisy: Jeff v4 마지막 주문 "조용히 지나가면
안 된다" — attribute access is traced, not merely a no-op object.

Event log:
    All detections and import interceptions are appended to a
    process-local list `get_import_events()` so orchestrator can
    mirror them into the completion marker.checks.imports_ok flag.
"""
from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import logging
import os
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

_log = logging.getLogger("gen4.pyc_guard")
_INSTALL_LOCK = threading.Lock()
_EVENT_LOCK = threading.Lock()
_INSTALLED = False
_EVENTS: list["ImportEvent"] = []


class PycOnlyImportBlocked(ImportError):
    """Raised when a CRITICAL module's .py is missing and only .pyc exists.

    Subclasses ImportError so legacy try/except ImportError still catches;
    loud crash for callers that expected the module to be present.
    """


@dataclass
class ImportEvent:
    """Records one pyc-only detection or import interception."""
    module: str
    kind: str               # 'DETECT' (static) | 'INTERCEPT' (dynamic)
    action: str             # 'RAISE' | 'DUMMY'
    source_missing: bool    # True iff .py source not found
    pyc_path: Optional[str]
    ts: float = field(default_factory=lambda: __import__("time").time())


def get_import_events() -> list[ImportEvent]:
    """Return snapshot of events recorded in this process."""
    with _EVENT_LOCK:
        return list(_EVENTS)


def clear_events_for_tests() -> None:
    """Test hygiene: reset event log."""
    with _EVENT_LOCK:
        _EVENTS.clear()


def _record(event: ImportEvent) -> None:
    with _EVENT_LOCK:
        _EVENTS.append(event)


# ---------- Static detection (preflight) ----------

def _module_name_from_path(py_path: Path, repo_root: Path) -> Optional[str]:
    """Convert a .py filesystem path to a dotted module name.

    Returns None if path is outside repo_root.
    """
    try:
        rel = py_path.relative_to(repo_root)
    except ValueError:
        return None
    parts = list(rel.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]  # strip '.py'
    return ".".join(parts) if parts else None


def _pyc_stem_to_module_part(pyc_name: str) -> Optional[str]:
    """__pycache__/foo.cpython-312.pyc → 'foo' (or None for __init__ variants)."""
    # Pyc filename format: <name>.<tag>.pyc  (e.g. 'foo.cpython-312.pyc')
    if not pyc_name.endswith(".pyc"):
        return None
    stem = pyc_name[:-4]  # remove '.pyc'
    # Drop the last '.' segment (the tag like 'cpython-312' or 'opt-1.cpython-312')
    if "." not in stem:
        return None
    return stem.rsplit(".", 1)[0]


def detect_pyc_only_modules(
    repo_root: Path,
    allowlist: Iterable[str],
) -> list[ImportEvent]:
    """Walk __pycache__ dirs under repo_root; report pyc-only modules in allowlist.

    A module is considered pyc-only when:
        - its .pyc exists under <pkg_dir>/__pycache__/<name>.<tag>.pyc
        - AND its .py does NOT exist at <pkg_dir>/<name>.py

    Returns a list of ImportEvent(kind='DETECT', action='DETECT_ONLY').
    Side effect: records events into the module-level log.

    Note: does NOT import anything. Pure filesystem scan.
    """
    allowset = frozenset(allowlist)
    events: list[ImportEvent] = []

    for pyc_path in repo_root.rglob("__pycache__/*.pyc"):
        pkg_dir = pyc_path.parent.parent  # __pycache__ sits inside pkg dir
        module_part = _pyc_stem_to_module_part(pyc_path.name)
        if module_part is None:
            continue

        # Candidate .py files: either <name>.py or (for package) <name>/__init__.py
        py_candidate = pkg_dir / f"{module_part}.py"
        init_candidate = pkg_dir / module_part / "__init__.py"
        if py_candidate.exists() or init_candidate.exists():
            continue  # source present → not orphaned

        # Orphaned .pyc. Compute module's full dotted name.
        if py_candidate.parent != pkg_dir:
            continue
        # Resolve the module name by walking up from pkg_dir to repo_root.
        try:
            rel_pkg = pkg_dir.relative_to(repo_root)
        except ValueError:
            continue
        pkg_parts = [p for p in rel_pkg.parts if p]  # drop ''
        full_name = ".".join(pkg_parts + [module_part]) if pkg_parts else module_part

        if full_name not in allowset:
            continue  # not in our allowlist — don't report

        ev = ImportEvent(
            module=full_name,
            kind="DETECT",
            action="DETECT_ONLY",
            source_missing=True,
            pyc_path=str(pyc_path),
        )
        _record(ev)
        events.append(ev)
        _log.critical(
            "[PYC_ONLY_DETECT] module=%s pyc=%s source_missing=True",
            full_name, pyc_path,
        )

    return events


# ---------- Runtime import hook ----------

class DummyModule:
    """Stand-in for a non-critical pyc-only module.

    Every attribute access logs CRITICAL — we refuse to silently pass
    through. Assigning to attributes is allowed (some callers may attempt
    introspection or cleanup) but also logged.
    """

    __slots__ = ("_pyc_guard_module_name",)

    def __init__(self, module_name: str):
        object.__setattr__(self, "_pyc_guard_module_name", module_name)

    def __getattr__(self, name: str) -> Any:
        # Dunder passthrough for common introspection (avoid recursion storms)
        if name.startswith("_pyc_guard_"):
            return object.__getattribute__(self, name)
        _log.critical(
            "[PYC_GUARD_DUMMY_ACCESS] module=%s attr=%r — "
            "stub returned; source recovery required",
            object.__getattribute__(self, "_pyc_guard_module_name"),
            name,
        )
        # Return a callable/attr placeholder that also logs on use
        return _DummyAttr(
            object.__getattribute__(self, "_pyc_guard_module_name"),
            name,
        )

    def __setattr__(self, name: str, value: Any) -> None:
        _log.critical(
            "[PYC_GUARD_DUMMY_SETATTR] module=%s attr=%r",
            object.__getattribute__(self, "_pyc_guard_module_name"),
            name,
        )
        object.__setattr__(self, name, value)

    def __repr__(self) -> str:
        return f"<DummyModule {object.__getattribute__(self, '_pyc_guard_module_name')!r} — source missing>"


class _DummyAttr:
    """Wraps unresolved attribute access on a DummyModule."""
    def __init__(self, module: str, attr: str):
        self._m = module
        self._a = attr

    def __call__(self, *args, **kwargs):
        _log.critical(
            "[PYC_GUARD_DUMMY_CALL] module=%s attr=%r args=%d kwargs=%d",
            self._m, self._a, len(args), len(kwargs),
        )
        return None

    def __repr__(self) -> str:
        return f"<DummyAttr {self._m}.{self._a} — source missing>"


class PycGuardFinder(importlib.abc.MetaPathFinder):
    """sys.meta_path finder that intercepts pyc-only allowlisted imports."""

    def __init__(
        self,
        *,
        allowlist: frozenset[str],
        critical: frozenset[str],
        repo_root: Path,
    ):
        self._allowlist = allowlist
        self._critical = critical
        self._repo_root = repo_root

    def find_spec(self, name, path=None, target=None):  # noqa: D401
        if name not in self._allowlist:
            return None

        # Determine where the .py *would* be by dotted → path conversion.
        # For top-level: <repo>/<name>.py
        # For nested:   <repo>/a/b/c.py
        dotted = name.split(".")
        py_candidate = self._repo_root.joinpath(*dotted).with_suffix(".py")
        init_candidate = self._repo_root.joinpath(*dotted, "__init__.py")

        if py_candidate.exists() or init_candidate.exists():
            return None  # source present → default finder takes over

        # Source missing. Find any orphaned .pyc for logging.
        pyc_hint = None
        pkg_dir = self._repo_root.joinpath(*dotted[:-1])
        cache_dir = pkg_dir / "__pycache__"
        if cache_dir.is_dir():
            for p in cache_dir.glob(f"{dotted[-1]}.*.pyc"):
                pyc_hint = str(p)
                break

        # Route: CRITICAL → raise; else DummyModule.
        if name in self._critical:
            _record(ImportEvent(
                module=name, kind="INTERCEPT", action="RAISE",
                source_missing=True, pyc_path=pyc_hint,
            ))
            _log.critical(
                "[PYC_GUARD_BLOCK_CRITICAL] module=%s — source missing, "
                "raising PycOnlyImportBlocked",
                name,
            )
            raise PycOnlyImportBlocked(
                f"module '{name}' is in PYC_CRITICAL_MODULES and has no .py "
                f"source file. Restore from git reflog / backup before use."
            )

        # Non-critical → return a loader that produces DummyModule
        _record(ImportEvent(
            module=name, kind="INTERCEPT", action="DUMMY",
            source_missing=True, pyc_path=pyc_hint,
        ))
        _log.critical(
            "[PYC_GUARD_DUMMY_SUBSTITUTE] module=%s — source missing, "
            "returning DummyModule",
            name,
        )
        return importlib.util.spec_from_loader(name, _DummyLoader(name))


class _DummyLoader(importlib.abc.Loader):
    def __init__(self, name: str):
        self._name = name

    def create_module(self, spec):
        return DummyModule(self._name)

    def exec_module(self, module):
        # Nothing to execute — the module is a stub.
        return None


def install_import_hook(
    *,
    allowlist: Optional[Iterable[str]] = None,
    critical: Optional[Iterable[str]] = None,
    repo_root: Optional[Path] = None,
) -> None:
    """Install PycGuardFinder on sys.meta_path. Idempotent."""
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED:
            return
        # Lazy import to avoid cycles
        from kr.pipeline.completion_schema import (
            PYC_ALLOWLIST_MODULES,
            PYC_CRITICAL_MODULES,
        )
        aset = frozenset(allowlist if allowlist is not None else PYC_ALLOWLIST_MODULES)
        cset = frozenset(critical if critical is not None else PYC_CRITICAL_MODULES)
        rr = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent.parent
        finder = PycGuardFinder(allowlist=aset, critical=cset, repo_root=rr)
        sys.meta_path.insert(0, finder)
        _INSTALLED = True
        _log.info(
            "[PYC_GUARD_INSTALLED] allowlist=%d critical=%d root=%s",
            len(aset), len(cset), rr,
        )


def uninstall_import_hook() -> None:
    """Test-only: remove PycGuardFinder from sys.meta_path."""
    global _INSTALLED
    with _INSTALL_LOCK:
        sys.meta_path[:] = [f for f in sys.meta_path
                            if not isinstance(f, PycGuardFinder)]
        _INSTALLED = False


def is_installed() -> bool:
    return _INSTALLED
