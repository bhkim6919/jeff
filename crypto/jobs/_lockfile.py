"""File-based mutex for crypto jobs (Jeff D3 #2).

Why exclusive O_CREAT instead of fcntl/msvcrt advisory locks:
    - Cross-platform (Windows Task Scheduler is the chosen runner)
    - Survives unclean process death (stale detection via pid + age)
    - Inspectable from disk during incidents

Contract:
    - ``acquire()`` raises ``LockHeld`` if another live process owns the lock
    - Stale locks (PID gone OR age > ``stale_after_sec``) are auto-reclaimed
    - The acquired path stores ``{"pid": int, "created_at_utc": str, "owner": str}``
    - ``release()`` only deletes locks the current process owns
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_STALE_AFTER_SEC = 2 * 60 * 60  # 2 hours


class LockHeld(RuntimeError):
    """Raised when another live process owns the lock."""


def _pid_alive(pid: int) -> bool:
    """Return True if a process with ``pid`` is currently running.

    On Windows, ``os.kill(pid, 0)`` raises OSError with winerror 87 for invalid
    handles and PermissionError (winerror 5) for processes we can see but can't
    signal — both indicate the PID is taken. Returns False only on
    ProcessLookupError or winerror 87 (no such process).
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        # winerror 87 = "parameter is incorrect" → invalid PID on Windows
        if getattr(exc, "winerror", None) == 87:
            return False
        return True


class FileLock:
    """Acquire an exclusive lockfile for the lifetime of the context.

    Usage::

        with FileLock(Path("crypto/data/_locks/incremental_listings.lock"),
                      owner="incremental_listings") as lock:
            ...  # job body

    Re-entrancy is NOT supported — nested ``with`` on the same path will raise.
    """

    def __init__(
        self,
        path: Path,
        *,
        owner: str,
        stale_after_sec: int = DEFAULT_STALE_AFTER_SEC,
    ) -> None:
        self._path = path
        self._owner = owner
        self._stale_after_sec = stale_after_sec
        self._fd: Optional[int] = None
        self._acquired = False

    @property
    def path(self) -> Path:
        return self._path

    def _read_existing(self) -> Optional[dict]:
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return None

    def _is_stale(self, payload: dict) -> tuple[bool, str]:
        pid = int(payload.get("pid", -1))
        if not _pid_alive(pid):
            return True, f"owner pid {pid} not alive"
        try:
            created = datetime.fromisoformat(payload.get("created_at_utc", ""))
        except ValueError:
            return True, "created_at_utc unparseable"
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age > self._stale_after_sec:
            return True, f"age {age:.0f}s exceeds stale_after {self._stale_after_sec}s"
        return False, ""

    def acquire(self) -> "FileLock":
        if self._acquired:
            raise RuntimeError(f"FileLock {self._path} already acquired")

        self._path.parent.mkdir(parents=True, exist_ok=True)

        # 1) Pre-check: if the file exists, decide whether to reclaim.
        existing = self._read_existing()
        if existing is not None:
            stale, reason = self._is_stale(existing)
            if not stale:
                raise LockHeld(
                    f"lock held by pid={existing.get('pid')} "
                    f"owner={existing.get('owner')} "
                    f"created_at_utc={existing.get('created_at_utc')}"
                )
            logger.warning(
                "[lock] reclaiming stale lock at %s — %s", self._path, reason
            )
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass

        # 2) Atomic create-exclusive. If another process beat us between the
        #    pre-check and now, O_EXCL fails and we surface that as LockHeld.
        try:
            fd = os.open(
                str(self._path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
        except FileExistsError as exc:
            raise LockHeld(f"race-lost on {self._path}") from exc

        payload = {
            "pid": os.getpid(),
            "created_at_utc": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
            "owner": self._owner,
        }
        try:
            os.write(fd, json.dumps(payload).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)

        self._fd = None
        self._acquired = True
        return self

    def release(self) -> None:
        if not self._acquired:
            return
        # Only delete if we still own it (defensive — prevents a stale-cleanup
        # racing job from deleting our newly-acquired lock).
        existing = self._read_existing()
        if existing and int(existing.get("pid", -1)) == os.getpid():
            try:
                self._path.unlink()
            except FileNotFoundError:
                pass
        self._acquired = False

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
