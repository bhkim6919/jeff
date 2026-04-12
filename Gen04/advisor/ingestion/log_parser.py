"""Parse Gen4 log files into structured LogEvent objects."""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from .schema import LogEvent

# Pattern: "2026-04-01 09:00:03,547 [INFO] gen4.live: [TAG] message"
_LOG_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ "
    r"\[(\w+)\] "
    r"([\w.]+): "
    r"(.*)$"
)

_TAG_RE = re.compile(r"\[([A-Z_]+(?:\[\d+\])?)\]")


def parse_log_file(log_path: Path, date_filter: str = "") -> list[LogEvent]:
    """Parse a log file into LogEvent list.

    Args:
        log_path: Path to log file.
        date_filter: If set, only include lines starting with this date prefix
                     (e.g., "2026-04-01").
    """
    if not log_path.exists():
        return []

    events = []
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []

    for line in lines:
        if date_filter and not line.startswith(date_filter):
            continue

        m = _LOG_RE.match(line)
        if not m:
            continue

        ts, level, logger, message = m.groups()

        # Extract tag
        tag_match = _TAG_RE.search(message)
        tag = tag_match.group(1) if tag_match else ""

        events.append(LogEvent(
            timestamp=ts,
            level=level,
            logger=logger,
            tag=tag,
            message=message,
            raw=line,
        ))

    return events


def find_log_file(log_dir: Path, mode: str, date: str) -> Path | None:
    """Find the log file for a given mode and date.

    Args:
        date: "20260401" format.
    """
    candidates = [
        log_dir / f"gen4_{mode}_{date}.log",
        log_dir / f"gen4_live_{date}.log",
        log_dir / f"gen4_paper_{date}.log",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def extract_operational_flags(events: list[LogEvent],
                              operational_tags: frozenset) -> list[str]:
    """Extract operational flags from log events."""
    flags = set()
    for e in events:
        if e.level in ("CRITICAL", "ERROR"):
            for tag in operational_tags:
                if tag in e.message:
                    flags.add(tag)
        if e.tag in operational_tags:
            flags.add(e.tag)
    return sorted(flags)
