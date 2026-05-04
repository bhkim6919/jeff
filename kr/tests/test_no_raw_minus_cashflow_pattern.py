"""Regression: forbid the `raw_equity - cumulative_cashflow` anti-pattern.

Background (Jeff doctrine 2026-05-04, Accounting Correction Sprint CF0)
-----------------------------------------------------------------------
The previous module `kr/finance/capital_events.py` (introduced 2026-04-20)
defined `adjust_equity()` which computed:

    adjusted = raw_equity - cumulative_net_deposits_in_window

This pattern was rejected because it produces:
  - replay divergence (recompute drifts from stored adj)
  - dual truth (raw and adj coexisting -> cache mismatch)
  - DD peak continuity broken (deposit moves peak)
  - intraday cashflow boundary undefined
  - PG/CSV drift on rebuild

Instead the doctrine is:
  - raw equity = immutable broker truth
  - returns/DD = computed cashflow-aware (Modified Dietz first)
  - never store equity_adj as a separate time series

CF0 quarantined the original module to `kr/finance/_deprecated_capital_events.py`
and removed `adjust_equity()` entirely. This test prevents either name from
sneaking back into active runtime code.

Scope (Jeff specified)
----------------------
  Searches: kr/, shared/, us/  (active runtime)
  Excludes: any path containing _deprecated, tests, docs, __pycache__,
            .claude (worktrees), kr-legacy
"""
from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SCOPE_DIRS = ("kr", "shared", "us")

EXCLUDE_PATH_SEGMENTS = (
    "_deprecated",
    "tests",
    "docs",
    "__pycache__",
    ".claude",
    "kr-legacy",
)

FORBIDDEN = [
    # Match call / definition / import-of-name (require open-paren or import context),
    # NOT bare textual mentions in comments / docstrings / HTTP detail strings.
    (re.compile(r"\badjust_equity\s*\("),
     "adjust_equity(...) - quarantined anti-pattern call/def (raw - cashflow)"),
    (re.compile(r"\bfrom\s+\S+\s+import\s+[^\n#]*\badjust_equity\b"),
     "import adjust_equity - quarantined anti-pattern import"),
]


def _iter_active_python_files():
    for top in SCOPE_DIRS:
        base = REPO_ROOT / top
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            rel_parts = path.relative_to(REPO_ROOT).parts
            if any(any(seg in part for seg in EXCLUDE_PATH_SEGMENTS) for part in rel_parts):
                continue
            yield path


def test_no_adjust_equity_reference_in_active_runtime():
    offenders: list[str] = []
    for path in _iter_active_python_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8", errors="ignore")
        for pattern, label in FORBIDDEN:
            for m in pattern.finditer(text):
                line_no = text[: m.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(REPO_ROOT)}:{line_no}  ({label})")
    assert not offenders, (
        "Anti-pattern regression: forbidden symbol(s) re-entered active runtime.\n"
        "See kr/finance/_deprecated_capital_events.py header for rationale.\n"
        "Offenders:\n  " + "\n  ".join(offenders)
    )


def test_deprecated_module_still_quarantined():
    """The quarantined file must remain at its quarantine path so the
    test_no_adjust_equity_reference_in_active_runtime exclusion is meaningful."""
    deprecated = REPO_ROOT / "kr" / "finance" / "_deprecated_capital_events.py"
    assert deprecated.exists(), (
        "Quarantine guard: kr/finance/_deprecated_capital_events.py is missing. "
        "If you intend to remove the quarantined module, also remove the corresponding "
        "exclusion in this test and the README/audit trail."
    )


def test_no_active_capital_events_module():
    """The original active path must not be re-created - that would re-expose
    the import surface that previously hosted adjust_equity()."""
    active = REPO_ROOT / "kr" / "finance" / "capital_events.py"
    assert not active.exists(), (
        "Quarantine breach: kr/finance/capital_events.py exists in the active path. "
        "It was quarantined to _deprecated_capital_events.py 2026-05-04 (CF0). "
        "Do not restore it - new accounting code must live under kr/accounting/."
    )
