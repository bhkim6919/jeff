# -*- coding: utf-8 -*-
"""kr/_bootstrap_path.py — sys.path bootstrap for the KR market package.

Imported for side-effect from entry points (tray_server.py, main.py)
that need 'shared.*' imports to resolve. The kr/ directory is expected
to already be on sys.path — the caller inserts it before importing this
module. Bootstrap's only job is adding the project root so the shared/
package is reachable, plus a single audit line so startup failures are
visible in logs.

Minimum viable reconstruction (2026-04-21): the original source file
was missing from the repo (only the cached .pyc remained in
kr/__pycache__/). This caused tray_server.py to die silently when
launched via .venv64/Scripts/pythonw.exe — no tray icon, no stderr.
Symmetric with the us/_bootstrap_path.py reconstruction committed in
9c987ac1 earlier today.

Kept deliberately small so that if an import fails, the traceback
points at the obvious place and the bootstrap itself is not a suspect.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent

if str(_ROOT) not in sys.path:
    sys.path.insert(1, str(_ROOT))

try:
    from shared.db.pg_base import get_db_config  # noqa: F401
    print(f"[BOOTSTRAP_AUDIT] kr sys.path[:3]={sys.path[:3]} shared_ok=True")
except Exception as _e:
    print(
        f"[BOOTSTRAP_FAIL] kr: shared.db.pg_base unreachable: {_e}\n"
        f"  sys.path[:5]={sys.path[:5]}"
    )
