"""Thin entry point for D3-1 incremental listings job.

This wrapper exists so Windows Task Scheduler (and ad-hoc CLI runs) can invoke
the job without setting PYTHONPATH manually. The actual work lives in
``crypto.jobs.incremental_listings``.

Examples::

    # Production (what Task Scheduler runs)
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/run_incremental_listings.py

    # Dry-run (crawl + diff, no writes)
    python scripts/crypto/run_incremental_listings.py --dry-run

    # Verify stricter scope
    python scripts/crypto/run_incremental_listings.py --max-pages 1
"""

from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
if str(WORKTREE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.jobs.incremental_listings import run  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(run())
