# -*- coding: utf-8 -*-
"""kr/pipeline/bootstrap.py — Environment fail-fast validation (R-6).

Addresses R-6 from the design doc: on 2026-04-20 22:00, tray-side
`ZoneInfo("Asia/Seoul")` lookups were silently failing because tzdata
was missing from the Python env. Try/except swallowed the errors,
`_is_after_kr_close()` returned False, and the Lab EOD auto-trigger
became silently unreachable.

`bootstrap_env()` MUST be called once at process start (tray_server.py,
orchestrator entry) before any timezone or pipeline-state operation.
strict=True (default) turns silent failure into loud failure.
"""
from __future__ import annotations

import importlib
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

_log = logging.getLogger("gen4.pipeline.bootstrap")


class BootstrapError(RuntimeError):
    """Raised when required runtime dependencies are missing."""


def bootstrap_env(
    *,
    data_dir: Optional[Path] = None,
    strict: bool = True,
) -> dict:
    """Validate runtime preconditions. Returns a checks dict.

    Checks:
      - 'tzdata'            : `import tzdata` succeeds
      - 'zoneinfo_seoul'    : ZoneInfo('Asia/Seoul') + now() succeeds
      - 'data_dir_writable' : data_dir (if given) is writable

    strict=True → raise `BootstrapError` on ANY failure.
    strict=False → return dict with False entries + log warnings.
    """
    checks: dict[str, bool] = {}
    errors: list[str] = []

    checks["tzdata"] = _check_tzdata(errors)
    checks["zoneinfo_seoul"] = _check_zoneinfo_seoul(errors)
    if data_dir is not None:
        checks["data_dir_writable"] = _check_data_dir(Path(data_dir), errors)

    if all(checks.values()):
        _log.info("[PIPELINE_BOOTSTRAP_OK] checks=%s", checks)
        return checks

    summary = (
        f"pipeline bootstrap failed: checks={checks} errors={errors}"
    )
    if strict:
        _log.error("[PIPELINE_BOOTSTRAP_FAIL] %s", summary)
        raise BootstrapError(summary)
    _log.warning("[PIPELINE_BOOTSTRAP_WARN] %s", summary)
    return checks


def _check_tzdata(errors: list[str]) -> bool:
    try:
        importlib.import_module("tzdata")
        return True
    except ImportError as e:
        errors.append(f"tzdata missing: {e}")
        return False


def _check_zoneinfo_seoul(errors: list[str]) -> bool:
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        try:
            tz = ZoneInfo("Asia/Seoul")
        except ZoneInfoNotFoundError as e:
            errors.append(f"ZoneInfo('Asia/Seoul') not found: {e}")
            return False

        # Actually use it — some env corruptions only surface on call
        _ = datetime.now(tz)
        return True
    except Exception as e:
        errors.append(f"zoneinfo verification raised: {e}")
        return False


def _check_data_dir(data_dir: Path, errors: list[str]) -> bool:
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=".bootstrap_probe_",
            suffix=".tmp",
            dir=str(data_dir),
            delete=False,
        ) as f:
            probe = Path(f.name)
            f.write(b"ok")
        try:
            probe.unlink()
        except OSError:
            pass
        return True
    except Exception as e:
        errors.append(f"data_dir not writable ({data_dir}): {e}")
        return False
