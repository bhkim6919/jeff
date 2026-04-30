"""Pipeline completion-marker schema constants.

Single source of truth for run-type identifiers, status labels,
expected daily windows, status-transition rules, and the consumer
allowlists used by ``shared/util/truth_guard.py`` and the pyc-only
import guard.

The module is consumed by:

    * ``kr/pipeline/completion_marker.py`` — writes / reads the
      per-day ``run_completion_{yyyymmdd}.json`` markers.
    * ``kr/pipeline/marker_integration.py`` — bridges legacy callers
      to the marker writer.
    * ``kr/pipeline/preflight.py`` — uses ``PYC_CRITICAL_MODULES`` /
      ``PYC_ALLOWLIST_MODULES`` to decide whether stale ``__pycache__``
      entries are tolerated.
    * ``kr/web/app.py`` — the deadman dashboard's R13 panel reads
      ``EXPECTED_WINDOWS_KST`` to compute "expected vs actual" run
      status per window.
    * ``shared/util/truth_guard.py`` — uses
      ``FORBIDDEN_TRUTH_PATTERNS`` + ``TRUTH_GUARD_CONSUMER_PREFIXES``
      to refuse legacy "truth" file paths.

History note (2026-04-30):
    The source file was missing from the repo (only the cached
    ``.pyc`` survived in ``__pycache__``), so the deadman R13 panel
    showed ``ModuleNotFoundError("No module named
    'pipeline.completion_schema'")`` even though every other
    consumer was loading the cached bytecode just fine. This module
    is reconstructed from the ``.pyc`` constants so R13 — and the
    explicit ``import``s in ``kr/web/app.py:3903`` and the test
    suite — succeed against an actual source file.
"""
from __future__ import annotations


# ── Run-type identifiers ──────────────────────────────────────────────
# Used as ``run_type`` keys in the per-day marker JSON.

RUN_KR_BATCH: str = "KR_BATCH"
RUN_KR_EOD:   str = "KR_EOD"
RUN_US_BATCH: str = "US_BATCH"
RUN_US_EOD:   str = "US_EOD"

ALL_RUN_TYPES: frozenset[str] = frozenset({
    RUN_KR_BATCH,
    RUN_KR_EOD,
    RUN_US_BATCH,
    RUN_US_EOD,
})


# ── Status labels ─────────────────────────────────────────────────────
# Stored in the marker JSON as ``status``.

STATUS_MISSING:                 str = "MISSING"
STATUS_RUNNING:                 str = "RUNNING"
STATUS_SUCCESS:                 str = "SUCCESS"
STATUS_FAILED:                  str = "FAILED"
STATUS_PARTIAL:                 str = "PARTIAL"
STATUS_PRE_FLIGHT_FAIL:         str = "PRE_FLIGHT_FAIL"
STATUS_PRE_FLIGHT_STALE_INPUT:  str = "PRE_FLIGHT_STALE_INPUT"

ALL_STATUSES: frozenset[str] = frozenset({
    STATUS_MISSING,
    STATUS_RUNNING,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
})

TERMINAL_STATUSES: frozenset[str] = frozenset({
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
})

# Display severity ranking — used by the dashboard / Telegram alert
# formatter to colour or order incidents (lower = better).
SEVERITY: dict[str, int] = {
    STATUS_SUCCESS:                 0,
    STATUS_MISSING:                 1,
    STATUS_RUNNING:                 2,
    STATUS_PRE_FLIGHT_STALE_INPUT:  3,
    STATUS_PRE_FLIGHT_FAIL:         4,
    STATUS_PARTIAL:                 5,
    STATUS_FAILED:                  6,
}


# ── Expected daily windows (KST, minutes since midnight) ─────────────
# Pairs of ``(earliest, deadline)`` per run type. ``deadline > 1440``
# means the window crosses midnight (handled by the R13 reader in
# ``kr/web/app.py``: ``if deadline > 1440: deadline - 1440 wraps to
# next day``).

def _hm(hours: int, minutes: int) -> int:
    """Convert HH:MM to minutes since midnight."""
    return hours * 60 + minutes


EXPECTED_WINDOWS_KST: dict[str, tuple[int, int]] = {
    # KR batch: 16:05 KST start, must finish by 18:00.
    RUN_KR_BATCH: (_hm(16, 5),  _hm(18, 0)),
    # KR EOD: 15:35 KST start, must finish by 18:30.
    RUN_KR_EOD:   (_hm(15, 35), _hm(18, 30)),
    # US batch (post-close ET): 23:40 KST → 00:40 next day (1480).
    RUN_US_BATCH: (_hm(23, 40), _hm(24, 40)),
    # US EOD: 05:05 KST → 07:00.
    RUN_US_EOD:   (_hm(5, 5),   _hm(7, 0)),
}


# ── Status-transition rules ──────────────────────────────────────────
# Maps ``(prev_status, new_status)`` → label describing the transition.
# Used by ``kr/pipeline/completion_marker.py`` to validate updates and
# by R13 to distinguish "same attempt continued" vs "fresh run started".

_T_FRESH: str = "fresh"
_T_SAME:  str = "same_attempt"
_T_NEW:   str = "new_attempt"

ALLOWED_TRANSITIONS: dict[tuple[str, str], str] = {
    # First-ever observation of a run on this day → fresh.
    (STATUS_MISSING, STATUS_RUNNING):                  _T_FRESH,

    # In-flight: same attempt continues.
    (STATUS_RUNNING, STATUS_SUCCESS):                  _T_SAME,
    (STATUS_RUNNING, STATUS_PARTIAL):                  _T_SAME,
    (STATUS_RUNNING, STATUS_FAILED):                   _T_SAME,
    (STATUS_RUNNING, STATUS_PRE_FLIGHT_FAIL):          _T_SAME,
    (STATUS_RUNNING, STATUS_PRE_FLIGHT_STALE_INPUT):   _T_SAME,

    # Retry of the *same attempt* after a non-success terminal state.
    (STATUS_FAILED,                  STATUS_RUNNING):  _T_SAME,
    (STATUS_PARTIAL,                 STATUS_RUNNING):  _T_SAME,
    (STATUS_PRE_FLIGHT_FAIL,         STATUS_RUNNING):  _T_SAME,
    (STATUS_PRE_FLIGHT_STALE_INPUT,  STATUS_RUNNING):  _T_SAME,

    # SUCCESS → RUNNING means a *new* attempt was triggered (manual
    # re-run, force flag, etc.). Marker writer treats this as a new
    # attempt counter.
    (STATUS_SUCCESS, STATUS_RUNNING):                  _T_NEW,
}


# ── Marker / heartbeat file naming ───────────────────────────────────

MARKER_SCHEMA_VERSION:   int = 1
MARKER_FILENAME_FMT:     str = "run_completion_{yyyymmdd}.json"
HEARTBEAT_FILENAME:      str = "heartbeat.json"
HEARTBEAT_BAK_FILENAME:  str = "heartbeat.bak.json"


# ── Truth-guard config (consumed by shared/util/truth_guard.py) ──────
# Files matching FORBIDDEN_TRUTH_PATTERNS must not be read by modules
# whose dotted name starts with one of TRUTH_GUARD_CONSUMER_PREFIXES.

FORBIDDEN_TRUTH_PATTERNS: tuple[str, ...] = (
    r"state_\d{8}\.json$",
    r"(^|[\\/])head\.json$",
    r"gen4_(batch|live)_\d{8}\.log$",
)

TRUTH_GUARD_CONSUMER_PREFIXES: tuple[str, ...] = (
    "kr.pipeline.notify",
    "kr.pipeline.watchdog",
    "kr.web.dashboard",
    "scripts.watchdog_external",
)


# ── pyc-only import guard (consumed by kr/pipeline/preflight.py) ─────
# Modules whose ``__pycache__`` entry is permitted to load without a
# matching source file. CRITICAL is the smallest set — anything in
# CRITICAL must always be importable. ALLOWLIST is the broader set
# that includes CRITICAL plus modules where pyc-only loading is
# tolerated (slow recovery path) but flagged as a warning.

PYC_CRITICAL_MODULES: frozenset[str] = frozenset({
    "kr.pipeline.orchestrator",
    "kr.web.lab_live.engine",
    "kr.web.lab_live.market_context",
    "shared.db.csv_loader",
})

PYC_ALLOWLIST_MODULES: frozenset[str] = frozenset({
    # Critical (mirror PYC_CRITICAL_MODULES so allowlist check alone
    # is sufficient for the broad case).
    "kr.pipeline.orchestrator",
    "kr.web.lab_live.engine",
    "kr.web.lab_live.market_context",
    "shared.db.csv_loader",
    # Other production modules where pyc-only is tolerated.
    "kr.risk.p2_gates",
    "kr.tools.health_check",
    "kr.web.lab_live.daily_drivers",
    "kr.web.lab_live.state_store",
    "shared.data_events",
    "shared.db.pg_base",
    "tools.gate_observer",
})
