"""Main-project .env loader for the Crypto Lab worktree.

Background:
    The worktree (``C:/Q-TRON-32_ARCHIVE-crypto-d1``) uses sparse-checkout to
    hide kr/, us/, etc. from the working tree. This means
    ``shared/db/pg_base.py``'s default behavior — looking for ``kr/.env`` /
    ``us/.env`` *inside the worktree* — fails: the .env files exist only in
    the main project directory.

    Instead of modifying the shared module (off-limits during D1), this helper
    explicitly loads env vars from the main project's kr/.env. The worktree
    does not store credentials of its own.

Usage:
    from crypto.db.env import ensure_main_project_env_loaded
    ensure_main_project_env_loaded()
    # now shared/db/pg_base can resolve DB_PASSWORD etc.

Side effects:
    Calling this populates os.environ with DB_NAME / DB_USER / DB_PASSWORD /
    DB_HOST / DB_PORT (and any other variables present in the main env
    file). It does NOT overwrite values already set in os.environ.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Main project location is hard-coded — same machine, same user. This worktree
# was set up specifically for D1 development next to the active KR/US runtime.
MAIN_PROJECT_ROOT = Path("C:/Q-TRON-32_ARCHIVE")
CANDIDATE_ENV_PATHS = (
    MAIN_PROJECT_ROOT / "kr" / ".env",
    MAIN_PROJECT_ROOT / "us" / ".env",
    MAIN_PROJECT_ROOT / ".env",
)

REQUIRED_KEYS = ("DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT")

_loaded = False


def ensure_main_project_env_loaded() -> Path:
    """Idempotently load .env from the main project. Returns the path used.

    Raises:
        FileNotFoundError: none of the candidate paths exist.
        RuntimeError: dotenv is not installed in the active venv.
    """
    global _loaded
    if _loaded:
        # Re-detect path for logging clarity, but do not reload.
        for p in CANDIDATE_ENV_PATHS:
            if p.exists():
                return p
        raise FileNotFoundError("env already loaded but no candidate path found")

    try:
        from dotenv import load_dotenv  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "python-dotenv is required for crypto/db/env.py. Install via: "
            "pip install python-dotenv (already present in .venv64)."
        ) from exc

    chosen: Path | None = None
    for path in CANDIDATE_ENV_PATHS:
        if path.exists():
            load_dotenv(path, override=False)
            chosen = path
            break

    if chosen is None:
        raise FileNotFoundError(
            "No main-project .env file found. Searched: "
            + ", ".join(str(p) for p in CANDIDATE_ENV_PATHS)
        )

    missing = [k for k in REQUIRED_KEYS if not os.environ.get(k)]
    if missing:
        raise RuntimeError(
            f"main-project .env loaded from {chosen}, but missing keys: {missing}"
        )

    _loaded = True
    logger.debug("Main-project env loaded from %s", chosen)
    return chosen
