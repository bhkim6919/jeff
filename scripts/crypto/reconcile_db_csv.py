"""Thin entry point for D3-2 DB↔CSV reconcile job.

This wrapper sets PYTHONPATH so Windows Task Scheduler (and CLI) can invoke
the job without environment fiddling. Implementation lives in
``crypto.jobs.reconcile_listings``.

Examples::

    # Production run
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/reconcile_db_csv.py

    # Skip Telegram even on drift (offline diagnostics)
    python scripts/crypto/reconcile_db_csv.py --no-telegram

Per Jeff D3-2 scope (2026-04-28):
    - Compare DB ↔ CSV row counts
    - Detect pair / source / delisted_at drift
    - Logger alert + JSON evidence
    - Telegram best-effort
    - Auto-fix forbidden — report-only
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.jobs.reconcile_listings import run  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(run())
