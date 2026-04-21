# -*- coding: utf-8 -*-
"""us/_bootstrap_path.py — sys.path bootstrap for the US market package.

Imported for side-effect from entry points (main.py, web/app.py) that
need 'shared.*' imports (e.g. shared.db.pg_base) to resolve. The us/
directory is expected to already be on sys.path — the caller inserts
it before importing this module. Bootstrap's only job is adding the
project root so the shared/ package is reachable, plus a single audit
line so startup failures are visible in logs.

Minimum viable reconstruction (2026-04-21): the original source file
was missing from the repo (only the cached .pyc remained in
us/__pycache__/). Behavior matches what the running server was using
at last successful startup (tray_stderr 2026-04-20 20:29:39):

    [BOOTSTRAP_AUDIT] us sys.path[:3]=['...\\us', '...', ''] shared_ok=True

No additional checks are performed here — keep this file small so the
reason for any import failure is obvious from the traceback alone.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent

if str(_ROOT) not in sys.path:
    sys.path.insert(1, str(_ROOT))

try:
    from shared.db.pg_base import get_db_config  # noqa: F401
    print(f"[BOOTSTRAP_AUDIT] us sys.path[:3]={sys.path[:3]} shared_ok=True")
except Exception as _e:
    print(
        f"[BOOTSTRAP_FAIL] us: shared.db.pg_base unreachable: {_e}\n"
        f"  sys.path[:5]={sys.path[:5]}"
    )
