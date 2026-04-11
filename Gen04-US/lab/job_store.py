# -*- coding: utf-8 -*-
"""
job_store.py — Lab Job Persistence (Atomic JSON)
==================================================
Thread-safe, atomic write, duplicate detection.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("qtron.us.lab.jobs")

JOBS_DIR = Path(__file__).resolve().parent / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class LabJob:
    job_id: str
    config_hash: str
    group: str
    strategies: List[str]
    universe_snapshot_id: str
    data_snapshot_id: str
    start_date: str
    end_date: str
    status: str = "PENDING"      # PENDING | RUNNING | DONE | FAILED
    started_at: str = ""
    finished_at: str = ""
    error: str = ""
    results: dict = field(default_factory=dict)
    result_meta: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> LabJob:
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


def compute_config_hash(strategies: List[str], start_date: str, end_date: str,
                        universe_snapshot_id: str, data_snapshot_id: str) -> str:
    """SHA256 of run configuration — identical config = identical hash."""
    h = hashlib.sha256()
    h.update(",".join(sorted(strategies)).encode())
    h.update(f"{start_date}_{end_date}".encode())
    h.update(universe_snapshot_id.encode())
    h.update(data_snapshot_id.encode())
    return h.hexdigest()[:16]


def compute_data_snapshot_id(close_dict: dict, date_range: tuple) -> str:
    """Data content hash — ticker + price fingerprint."""
    h = hashlib.sha256()
    for ticker in sorted(close_dict.keys()):
        series = close_dict[ticker]
        h.update(ticker.encode())
        h.update(str(len(series)).encode())
        if len(series) > 0:
            h.update(f"{series.iloc[0]:.4f}".encode())
            h.update(f"{series.iloc[-1]:.4f}".encode())
            h.update(f"{series.index[0]}".encode())
            h.update(f"{series.index[-1]}".encode())
            # P1 강화: std + mean
            h.update(f"{series.std():.4f}".encode())
            h.update(f"{series.mean():.4f}".encode())
    h.update(f"{date_range[0]}_{date_range[1]}".encode())
    return h.hexdigest()[:16]


class JobStore:
    """Thread-safe Lab Job persistence."""

    def __init__(self, jobs_dir: Path = JOBS_DIR):
        self._dir = jobs_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._job_locks: Dict[str, threading.Lock] = {}

    def _job_path(self, job_id: str) -> Path:
        return self._dir / f"{job_id}.json"

    def _atomic_write(self, path: Path, data: dict) -> bool:
        """Atomic tmp → rename."""
        tmp = path.with_suffix(".tmp")
        try:
            content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(str(tmp), str(path))
            return True
        except Exception as e:
            logger.error(f"[JOB] Write failed {path.name}: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    def create_job(self, group: str, strategies: List[str],
                   universe_snapshot_id: str, data_snapshot_id: str,
                   start_date: str, end_date: str,
                   force: bool = False) -> LabJob:
        """Create a new job. Rejects if duplicate RUNNING or returns cached DONE."""
        config_hash = compute_config_hash(
            strategies, start_date, end_date, universe_snapshot_id, data_snapshot_id
        )

        with self._lock:
            # Check for duplicate
            existing = self._find_by_hash(config_hash)
            if existing:
                if existing.status == "RUNNING" and not force:
                    raise ValueError(f"Job already RUNNING: {existing.job_id}")
                if existing.status == "DONE" and not force:
                    logger.info(f"[JOB] Cache hit: {existing.job_id}")
                    return existing

            job = LabJob(
                job_id=str(uuid.uuid4())[:8],
                config_hash=config_hash,
                group=group,
                strategies=strategies,
                universe_snapshot_id=universe_snapshot_id,
                data_snapshot_id=data_snapshot_id,
                start_date=start_date,
                end_date=end_date,
                status="PENDING",
            )

            self._atomic_write(self._job_path(job.job_id), job.to_dict())
            return job

    def update_job(self, job: LabJob) -> bool:
        """Update job state (thread-safe per job_id)."""
        lock = self._job_locks.setdefault(job.job_id, threading.Lock())
        with lock:
            return self._atomic_write(self._job_path(job.job_id), job.to_dict())

    def get_job(self, job_id: str) -> Optional[LabJob]:
        path = self._job_path(job_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return LabJob.from_dict(data)
        except Exception:
            return None

    def list_jobs(self) -> List[LabJob]:
        jobs = []
        for p in sorted(self._dir.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                jobs.append(LabJob.from_dict(data))
            except Exception:
                pass
        return jobs

    def _find_by_hash(self, config_hash: str) -> Optional[LabJob]:
        for p in self._dir.glob("*.json"):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if data.get("config_hash") == config_hash:
                    return LabJob.from_dict(data)
            except Exception:
                pass
        return None
