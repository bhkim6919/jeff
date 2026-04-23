# -*- coding: utf-8 -*-
"""shared/util/truth_guard.py — Runtime guard for marker-only truth reads.

Blocks reads of legacy truth sources (state_YYYYMMDD.json, head.json,
gen4_*_YYYYMMDD.log) from consumer-side code paths. Producers — the
orchestrator and step modules that write these files — pass through
normally; the guard only activates when the caller frame is in a
registered consumer module prefix.

Design references:
    v4 §필수 1 — scope limited to consumer stack (avoid pandas/logging oversight)

Usage:
    # In a consumer entrypoint (notifier / watchdog / dashboard):
    from shared.util.truth_guard import activate_consumer_mode
    activate_consumer_mode(__name__)   # installs guarded open() once

    # Any subsequent open() call FROM a registered consumer prefix
    # targeting a FORBIDDEN_TRUTH_PATTERN path raises ForbiddenTruthSource.
    # Calls from other modules (producer / library) are unaffected.

Implementation:
    - Monkey-patches builtins.open once per process.
    - guarded_open() inspects the call stack; if any frame's __name__
      matches TRUTH_GUARD_CONSUMER_PREFIXES AND the path matches
      FORBIDDEN_TRUTH_PATTERNS → raise.
    - Otherwise calls through to original open().

Performance note: stack walk is O(stack_depth). For typical consumer
paths (5–15 frames) this is <5μs per open() call. Fine for hot paths
since consumers don't open() in tight loops.
"""
from __future__ import annotations

import builtins
import logging
import os
import re
import sys
import threading
from pathlib import Path
from typing import Any, Optional

# Import patterns from pipeline module. Lazy import to avoid cycle if
# pipeline package imports this guard indirectly.
def _load_patterns() -> tuple[list[re.Pattern], tuple[str, ...]]:
    from kr.pipeline.completion_schema import (
        FORBIDDEN_TRUTH_PATTERNS,
        TRUTH_GUARD_CONSUMER_PREFIXES,
    )
    return (
        [re.compile(p) for p in FORBIDDEN_TRUTH_PATTERNS],
        tuple(TRUTH_GUARD_CONSUMER_PREFIXES),
    )


_log = logging.getLogger("gen4.truth_guard")

# State flags
_INSTALL_LOCK = threading.Lock()
_INSTALLED = False
_ORIGINAL_OPEN = builtins.open  # capture before any patch
_FORBIDDEN_REGEX: Optional[list[re.Pattern]] = None
_CONSUMER_PREFIXES: tuple[str, ...] = ()
_ACTIVE_CONSUMERS: set[str] = set()


class ForbiddenTruthSource(RuntimeError):
    """Raised when consumer code attempts to read a legacy truth source.

    The correct replacement is MarkerReader (pipeline.completion_marker).
    """


def activate_consumer_mode(module_name: str) -> None:
    """Register `module_name` as a consumer. Installs guarded open() once.

    Idempotent — calling multiple times (from different consumer modules)
    is safe. The guard expands its active-consumer set and keeps the
    single patched open() installed.
    """
    global _INSTALLED, _FORBIDDEN_REGEX, _CONSUMER_PREFIXES

    with _INSTALL_LOCK:
        _ACTIVE_CONSUMERS.add(module_name)
        if _INSTALLED:
            return

        _FORBIDDEN_REGEX, _CONSUMER_PREFIXES = _load_patterns()
        builtins.open = _guarded_open  # type: ignore[assignment]
        _INSTALLED = True
        _log.info(
            "[TRUTH_GUARD_INSTALLED] consumer=%s prefixes=%s patterns=%d",
            module_name, _CONSUMER_PREFIXES, len(_FORBIDDEN_REGEX),
        )


def deactivate_for_tests() -> None:
    """Test-only: restore original open() and reset state.

    Production code should never call this. Tests need it because pytest
    runs many cases in one process and state would bleed across tests.
    """
    global _INSTALLED, _FORBIDDEN_REGEX, _CONSUMER_PREFIXES
    with _INSTALL_LOCK:
        builtins.open = _ORIGINAL_OPEN  # type: ignore[assignment]
        _INSTALLED = False
        _FORBIDDEN_REGEX = None
        _CONSUMER_PREFIXES = ()
        _ACTIVE_CONSUMERS.clear()


def is_installed() -> bool:
    return _INSTALLED


def active_consumers() -> frozenset[str]:
    return frozenset(_ACTIVE_CONSUMERS)


def _path_matches_forbidden(path_like: Any) -> bool:
    """Check if `path_like` (str/Path/PathLike) matches any forbidden pattern."""
    if _FORBIDDEN_REGEX is None:
        return False
    try:
        s = os.fspath(path_like) if hasattr(os, "fspath") else str(path_like)
    except TypeError:
        return False
    # Normalize for cross-platform regex (state_20260422.json match should
    # work regardless of Windows \ vs POSIX /).
    return any(rx.search(s) for rx in _FORBIDDEN_REGEX)


def _caller_in_consumer_stack() -> Optional[str]:
    """Walk the call stack (skipping this module) and return the first
    frame's module name that matches a consumer prefix, else None.

    We skip frames belonging to this guard module and to builtins so the
    check reflects the true caller.
    """
    # Start at the frame that called guarded_open() — sys._getframe(2):
    # frame 0 = this function, frame 1 = guarded_open, frame 2 = caller.
    try:
        frame = sys._getframe(2)
    except ValueError:
        return None

    guard_module = __name__
    while frame is not None:
        mod_name = frame.f_globals.get("__name__", "")
        if mod_name and mod_name != guard_module:
            for prefix in _CONSUMER_PREFIXES:
                if mod_name == prefix or mod_name.startswith(prefix + "."):
                    return mod_name
        frame = frame.f_back
    return None


def _guarded_open(*args: Any, **kwargs: Any):
    """Replacement for builtins.open() — raises on consumer-scope forbidden reads."""
    # Extract path (positional-or-keyword 'file' per Python 3.x signature)
    path = None
    if args:
        path = args[0]
    elif "file" in kwargs:
        path = kwargs["file"]

    if path is not None and _path_matches_forbidden(path):
        caller = _caller_in_consumer_stack()
        if caller is not None:
            _log.critical(
                "[TRUTH_GUARD_BLOCKED] consumer=%s path=%s — "
                "use MarkerReader (pipeline.completion_marker) instead",
                caller, path,
            )
            raise ForbiddenTruthSource(
                f"consumer '{caller}' attempted to read legacy truth source "
                f"'{path}'. Use MarkerReader from "
                f"kr.pipeline.completion_marker instead."
            )

    return _ORIGINAL_OPEN(*args, **kwargs)
