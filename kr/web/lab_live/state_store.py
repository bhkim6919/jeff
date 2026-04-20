"""
state_store.py -- Persistent state for Lab Live
==================================================
v1: Monolithic state.json (legacy, 함수 유지)
v2: Per-strategy files + committed HEAD pointer
    - state/trades/equity 전부 동일 committed version
    - all-or-nothing recovery (부분 복구 금지)
    - file-level lock (cross-process safety)
"""
from __future__ import annotations
import json
import logging
import os
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("lab_live.state")

# ── Thread-level lock (same process) ────────────────────────────
_thread_lock = threading.RLock()

SCHEMA_VERSION = 1


# ═══════════════════════════════════════════════════════════════════
#  File-Level Lock (cross-process)
# ═══════════════════════════════════════════════════════════════════

class FileLock:
    """Cross-process file lock. Windows: msvcrt, Unix: fcntl."""

    def __init__(self, lock_path: Path, timeout: float = 30.0):
        self._path = lock_path
        self._timeout = timeout
        self._fd = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Stale lock detection: if lock file > timeout age, remove
        if self._path.exists():
            try:
                age = time.time() - self._path.stat().st_mtime
                if age > self._timeout:
                    logger.warning(f"[STATE_LOCK] Stale lock removed (age={age:.0f}s)")
                    self._path.unlink(missing_ok=True)
            except OSError:
                pass

        self._fd = open(self._path, "w")
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                self._lock_fd()
                break
            except (OSError, BlockingIOError):
                if time.monotonic() > deadline:
                    self._fd.close()
                    self._fd = None
                    raise TimeoutError(f"FileLock timeout: {self._path}")
                time.sleep(0.1)

        # Write PID for diagnostics
        self._fd.write(f"{os.getpid()}\n")
        self._fd.flush()

    def release(self) -> None:
        if self._fd:
            try:
                self._unlock_fd()
                self._fd.close()
            except OSError:
                pass
            self._fd = None

    def _lock_fd(self):
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_fd(self):
        if os.name == "nt":
            import msvcrt
            try:
                self._fd.seek(0)
                msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        else:
            import fcntl
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


# ═══════════════════════════════════════════════════════════════════
#  Atomic I/O primitives
# ═══════════════════════════════════════════════════════════════════

def atomic_write_json(path: Path, payload: dict) -> None:
    """Atomic JSON write: tmp -> fsync -> replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    # backup
    if path.exists():
        bak = path.with_suffix(path.suffix + ".bak")
        try:
            if bak.exists():
                bak.unlink()
            path.rename(bak)
        except OSError:
            pass
    os.replace(str(tmp), str(path))


def safe_read_json(path: Path) -> Optional[dict]:
    """JSON read (no .bak fallback — v2 handles recovery explicitly)."""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return None


def _read_bak(path: Path) -> Optional[dict]:
    """Read .bak file only."""
    bak = path.with_suffix(path.suffix + ".bak")
    if bak.exists():
        try:
            return json.loads(bak.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return None


# ═══════════════════════════════════════════════════════════════════
#  v1 Legacy (유지, 호출만 v2로 전환)
# ═══════════════════════════════════════════════════════════════════

def save_state(lanes: dict, state_file: Path) -> None:
    """[v1 legacy] 전체 Lab Live 상태 저장."""
    state = {
        "last_run_date": datetime.now().strftime("%Y-%m-%d"),
        "last_run_ts": datetime.now().isoformat(),
        "lanes": {},
    }
    for name, lane in lanes.items():
        state["lanes"][name] = {
            "cash": lane["cash"],
            "positions": lane["positions"],
            "pending_buys": lane.get("pending_buys", []),
            "last_rebal_idx": lane.get("last_rebal_idx", -999),
            "equity_history": lane.get("equity_history", []),
        }
    atomic_write_json(state_file, state)
    logger.info(f"[LAB_LIVE] State saved: {len(lanes)} lanes")


def load_state(state_file: Path) -> Optional[dict]:
    """[v1 legacy] 저장된 상태 복원."""
    data = safe_read_json(state_file)
    if not data:
        bak = state_file.with_suffix(state_file.suffix + ".bak")
        data = safe_read_json(bak) if bak.exists() else None
    if data:
        logger.info(f"[LAB_LIVE] State loaded: last_run={data.get('last_run_date')}")
    return data


def save_trades(trades: list, trades_file: Path) -> None:
    """거래 이력 저장 (append-friendly)."""
    existing = []
    if trades_file.exists():
        try:
            raw = json.loads(trades_file.read_text(encoding="utf-8"))
            # v2 format: {"version_seq": N, "trades": [...]}
            if isinstance(raw, dict) and "trades" in raw:
                existing = raw["trades"]
            elif isinstance(raw, list):
                existing = raw
        except (json.JSONDecodeError, OSError):
            pass
    existing.extend(trades)
    atomic_write_json(trades_file, existing)


def load_trades(trades_file: Path) -> list:
    """거래 이력 로드."""
    if not trades_file.exists():
        return []
    try:
        raw = json.loads(trades_file.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and "trades" in raw:
            return raw["trades"]
        if isinstance(raw, list):
            return raw
        return []
    except (json.JSONDecodeError, OSError):
        return []


def append_equity(equity_row: dict, equity_file: Path) -> None:
    """[v1 legacy] Equity history CSV에 한 줄 추가."""
    import csv
    equity_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not equity_file.exists()
    with open(equity_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["date"] + sorted(
            [k for k in equity_row.keys() if k != "date"]))
        if write_header:
            writer.writeheader()
        writer.writerow(equity_row)


# ═══════════════════════════════════════════════════════════════════
#  v2: Per-Strategy + Committed HEAD
# ═══════════════════════════════════════════════════════════════════

def _read_head(head_file: Path) -> Optional[dict]:
    """Read head.json with .bak fallback."""
    data = safe_read_json(head_file)
    if data:
        return data
    # HEAD corrupted → try .bak
    bak_data = _read_bak(head_file)
    if bak_data:
        logger.warning("[STATE_V2] HEAD corrupted, recovered from head.json.bak")
    return bak_data


def _validate_all_versions(
    strategy_data: Dict[str, dict],
    trades_data: Optional[dict],
    equity_data: Optional[dict],
    expected_ver: int,
) -> bool:
    """Check all artifacts have expected version_seq."""
    for sname, sdata in strategy_data.items():
        if sdata.get("version_seq") != expected_ver:
            logger.warning(f"[STATE_V2] Version mismatch: {sname} "
                           f"has {sdata.get('version_seq')}, expected {expected_ver}")
            return False
    if trades_data and trades_data.get("version_seq") != expected_ver:
        logger.warning(f"[STATE_V2] trades version mismatch: "
                       f"{trades_data.get('version_seq')} != {expected_ver}")
        return False
    if equity_data and equity_data.get("version_seq") != expected_ver:
        logger.warning(f"[STATE_V2] equity version mismatch: "
                       f"{equity_data.get('version_seq')} != {expected_ver}")
        return False
    return True


def _load_all_primary(
    states_dir: Path,
    trades_file: Path,
    equity_json_file: Path,
    strategies: List[str],
) -> Tuple[Dict[str, dict], Optional[dict], Optional[dict]]:
    """Load all primary files (not .bak)."""
    strat_data = {}
    for sname in strategies:
        fp = states_dir / f"{sname}.json"
        data = safe_read_json(fp)
        if data:
            strat_data[sname] = data
    trades = safe_read_json(trades_file)
    equity = safe_read_json(equity_json_file)
    return strat_data, trades, equity


def _load_all_bak(
    states_dir: Path,
    trades_file: Path,
    equity_json_file: Path,
    strategies: List[str],
) -> Tuple[Dict[str, dict], Optional[dict], Optional[dict]]:
    """Load all .bak files."""
    strat_data = {}
    for sname in strategies:
        fp = states_dir / f"{sname}.json"
        data = _read_bak(fp)
        if data:
            strat_data[sname] = data
    trades = _read_bak(trades_file)
    equity = _read_bak(equity_json_file)
    return strat_data, trades, equity


def _find_consistent_version(
    strat_data: Dict[str, dict],
    trades: Optional[dict],
    equity: Optional[dict],
) -> Optional[int]:
    """Find common version_seq across all artifacts. None if inconsistent."""
    versions = set()
    for sd in strat_data.values():
        versions.add(sd.get("version_seq"))
    if trades:
        versions.add(trades.get("version_seq"))
    if equity:
        versions.add(equity.get("version_seq"))
    if len(versions) == 1:
        return versions.pop()
    return None


def save_state_v2(
    lanes: Dict[str, dict],
    trades: List[dict],
    equity_rows: List[dict],
    config,
    snapshot_version: str = "",
    run_meta: Optional[dict] = None,
    trade_date: str = "",
) -> int:
    """
    Committed version write protocol.
    Returns the new committed_version_seq.
    """
    with _thread_lock:
        flock = FileLock(config.state_io_lock_file)
        with flock:
            # Read current HEAD
            head = _read_head(config.head_file)
            current_ver = head.get("committed_version_seq", 0) if head else 0
            next_ver = current_ver + 1
            now = datetime.now()

            # 1. Write all strategy files
            config.states_dir.mkdir(parents=True, exist_ok=True)
            strategy_names = []
            for sname, lane in lanes.items():
                strategy_names.append(sname)
                payload = {
                    "version_seq": next_ver,
                    "strategy": sname,
                    "cash": lane["cash"],
                    "positions": lane["positions"],
                    "pending_buys": lane.get("pending_buys", []),
                    "last_rebal_idx": lane.get("last_rebal_idx", -999),
                    "equity_history": lane.get("equity_history", []),
                }
                atomic_write_json(config.states_dir / f"{sname}.json", payload)

            # 2. Write trades.json (versioned)
            trades_payload = {
                "version_seq": next_ver,
                "trades": trades,
            }
            atomic_write_json(config.trades_file, trades_payload)

            # 3. Write equity.json (versioned, replaces CSV append)
            equity_payload = {
                "version_seq": next_ver,
                "rows": equity_rows,
            }
            atomic_write_json(config.equity_json_file, equity_payload)

            # 4. LAST: Write HEAD = COMMIT
            head_payload = {
                "schema_version": SCHEMA_VERSION,
                "committed_version_seq": next_ver,
                "strategies": sorted(strategy_names),
                "last_run_date": trade_date or now.strftime("%Y-%m-%d"),
                "last_run_ts": now.isoformat(),
                "snapshot_version": snapshot_version,
            }
            # run_meta: source, data dates for reproducibility
            if run_meta:
                head_payload["run_meta"] = run_meta
            atomic_write_json(config.head_file, head_payload)

            logger.info(f"[STATE_V2] Committed version {next_ver}: "
                        f"{len(strategy_names)} strategies")
            return next_ver


def load_state_v2(config) -> Optional[dict]:
    """
    All-or-nothing load.
    Returns {"lanes": {...}, "trades": [...], "equity_rows": [...],
             "version_seq": N} or None.
    """
    with _thread_lock:
        # Migration check
        if not config.head_file.exists() and config.state_file.exists():
            _migrate_monolithic(config)

        head = _read_head(config.head_file)
        if not head:
            logger.info("[STATE_V2] No head.json — fresh start")
            return None

        committed_ver = head.get("committed_version_seq", 0)
        strategies = head.get("strategies", [])

        if not strategies:
            logger.warning("[STATE_V2] HEAD has no strategies — fresh start")
            return None

        # Load primary files
        strat_data, trades_data, equity_data = _load_all_primary(
            config.states_dir, config.trades_file, config.equity_json_file,
            strategies,
        )

        # Check all strategies loaded
        if len(strat_data) != len(strategies):
            missing = set(strategies) - set(strat_data.keys())
            logger.warning(f"[STATE_V2] Missing strategy files: {missing}")
        else:
            # Validate versions
            if _validate_all_versions(strat_data, trades_data, equity_data, committed_ver):
                return _assemble_result(strat_data, trades_data, equity_data, committed_ver, head)

        # PRIMARY FAILED → try .bak rollback (all-or-nothing)
        logger.warning("[STATE_V2] Primary version mismatch -> attempting .bak rollback")
        bak_strat, bak_trades, bak_equity = _load_all_bak(
            config.states_dir, config.trades_file, config.equity_json_file,
            strategies,
        )

        if len(bak_strat) == len(strategies):
            bak_ver = _find_consistent_version(bak_strat, bak_trades, bak_equity)
            if bak_ver is not None:
                logger.warning(f"[STATE_V2] Rolled back to .bak version {bak_ver}")
                return _assemble_result(bak_strat, bak_trades, bak_equity, bak_ver, head)

        # BAK FAILED → try archive fallback (last resort)
        logger.warning("[STATE_V2] .bak recovery failed -> attempting archive fallback")
        archive_result = _recover_from_archive(config)
        if archive_result is not None:
            return archive_result

        logger.error("[STATE_V2] LOAD FAIL: all recovery paths exhausted")
        return {
            "status": "CORRUPTED",
            "recovered": False,
            "message": "Version mismatch — primary, .bak, and archive recovery all failed",
            "lanes": {},
            "trades": [],
            "equity_rows": [],
            "version_seq": 0,
        }


def _assemble_result(
    strat_data: Dict[str, dict],
    trades_data: Optional[dict],
    equity_data: Optional[dict],
    version_seq: int,
    head: Optional[dict] = None,
) -> dict:
    """Assemble load result in engine-compatible format."""
    lanes = {}
    for sname, sd in strat_data.items():
        lanes[sname] = {
            "cash": sd["cash"],
            "positions": sd.get("positions", {}),
            "pending_buys": sd.get("pending_buys", []),
            "last_rebal_idx": sd.get("last_rebal_idx", -999),
            "equity_history": sd.get("equity_history", []),
        }
    return {
        "lanes": lanes,
        "trades": trades_data.get("trades", []) if trades_data else [],
        "equity_rows": equity_data.get("rows", []) if equity_data else [],
        "version_seq": version_seq,
        "last_run_date": head.get("last_run_date", "") if head else "",
        "snapshot_version": head.get("snapshot_version", "") if head else "",
    }


# ═══════════════════════════════════════════════════════════════════
#  Archive Recovery + Rotation
# ═══════════════════════════════════════════════════════════════════

def _find_latest_archive(config) -> Optional[Path]:
    """Find the most recent valid archive directory."""
    archive_root = config.state_dir / "archive"
    if not archive_root.exists():
        return None
    # Archive dirs are named YYYYMMDD_HHMMSS → lexicographic sort = chronological
    dirs = sorted(
        [d for d in archive_root.iterdir() if d.is_dir() and (d / "head.json").exists()],
        key=lambda d: d.name,
        reverse=True,
    )
    return dirs[0] if dirs else None


def _recover_from_archive(config) -> Optional[dict]:
    """
    Last-resort recovery: load from latest archive.
    Returns assembled result or None.
    """
    archive_dir = _find_latest_archive(config)
    if not archive_dir:
        logger.warning("[STATE_V2] No archive available for recovery")
        return None

    head = safe_read_json(archive_dir / "head.json")
    if not head:
        logger.warning(f"[STATE_V2] Archive {archive_dir.name} has unreadable head.json")
        return None

    ver = head.get("committed_version_seq", 0)
    strategies = head.get("strategies", [])
    arch_states_dir = archive_dir / "states"

    strat_data = {}
    for sname in strategies:
        fp = arch_states_dir / f"{sname}.json"
        data = safe_read_json(fp)
        if data:
            strat_data[sname] = data

    trades = safe_read_json(archive_dir / "trades.json")
    equity = safe_read_json(archive_dir / "equity.json")

    if len(strat_data) != len(strategies):
        missing = set(strategies) - set(strat_data.keys())
        logger.warning(f"[STATE_V2] Archive {archive_dir.name} missing strategies: {missing}")
        return None

    if _validate_all_versions(strat_data, trades, equity, ver):
        logger.warning(f"[STATE_V2] Recovered from archive {archive_dir.name} (version {ver})")
        result = _assemble_result(strat_data, trades, equity, ver)  # no head for archive
        result["recovered_from"] = f"archive/{archive_dir.name}"
        return result

    logger.warning(f"[STATE_V2] Archive {archive_dir.name} has version inconsistency")
    return None


ARCHIVE_KEEP_COUNT = 10


def _rotate_archives(config) -> int:
    """Delete oldest archives beyond ARCHIVE_KEEP_COUNT. Returns deleted count."""
    archive_root = config.state_dir / "archive"
    if not archive_root.exists():
        return 0
    dirs = sorted(
        [d for d in archive_root.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )
    to_delete = dirs[:-ARCHIVE_KEEP_COUNT] if len(dirs) > ARCHIVE_KEEP_COUNT else []
    deleted = 0
    for d in to_delete:
        try:
            shutil.rmtree(d)
            deleted += 1
        except OSError as e:
            logger.warning(f"[STATE_V2] Archive rotation failed for {d.name}: {e}")
    if deleted:
        logger.info(f"[STATE_V2] Archive rotation: deleted {deleted} old archives, "
                     f"kept {len(dirs) - deleted}")
    return deleted


# ═══════════════════════════════════════════════════════════════════
#  Migration: monolithic state.json → per-strategy + HEAD
# ═══════════════════════════════════════════════════════════════════

def _migrate_monolithic(config) -> None:
    """
    Migrate legacy state.json to per-strategy files.
    Uses temp directory for atomicity.
    """
    logger.info("[STATE_V2] Migrating monolithic state.json → per-strategy files")

    migrating_dir = config.state_dir / "states_migrating"

    # Cleanup incomplete previous migration
    if migrating_dir.exists():
        logger.warning("[STATE_V2] Found incomplete migration — cleaning up")
        shutil.rmtree(migrating_dir)

    legacy = load_state(config.state_file)
    if not legacy:
        logger.warning("[STATE_V2] Legacy state.json unreadable — fresh start")
        return

    lanes = legacy.get("lanes", {})
    if not lanes:
        logger.warning("[STATE_V2] Legacy state has no lanes — fresh start")
        return

    ver = 1

    # 1. Write strategy files to temp dir
    migrating_dir.mkdir(parents=True, exist_ok=True)
    strategy_names = []
    for sname, lane in lanes.items():
        strategy_names.append(sname)
        payload = {
            "version_seq": ver,
            "strategy": sname,
            "cash": lane.get("cash", 0),
            "positions": lane.get("positions", {}),
            "pending_buys": lane.get("pending_buys", []),
            "last_rebal_idx": lane.get("last_rebal_idx", -999),
            "equity_history": lane.get("equity_history", []),
        }
        fp = migrating_dir / f"{sname}.json"
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())

    # 2. Wrap trades.json with version
    existing_trades = load_trades(config.trades_file)
    trades_payload = {"version_seq": ver, "trades": existing_trades}
    atomic_write_json(config.trades_file, trades_payload)

    # 3. Convert equity CSV → equity.json
    equity_rows = _convert_equity_csv(config.equity_file)
    equity_payload = {"version_seq": ver, "rows": equity_rows}
    atomic_write_json(config.equity_json_file, equity_payload)

    # 4. Validate migration completeness
    files_in_migrating = list(migrating_dir.glob("*.json"))
    if len(files_in_migrating) != len(strategy_names):
        logger.error(f"[STATE_V2] Migration validation failed: "
                     f"expected {len(strategy_names)} files, got {len(files_in_migrating)}")
        shutil.rmtree(migrating_dir)
        return

    for sname in strategy_names:
        fp = migrating_dir / f"{sname}.json"
        data = safe_read_json(fp)
        if not data or data.get("version_seq") != ver:
            logger.error(f"[STATE_V2] Migration validation failed: {sname}")
            shutil.rmtree(migrating_dir)
            return

    # trades + equity version check
    t_check = safe_read_json(config.trades_file)
    e_check = safe_read_json(config.equity_json_file)
    if (not t_check or t_check.get("version_seq") != ver or
            not e_check or e_check.get("version_seq") != ver):
        logger.error("[STATE_V2] Migration validation failed: trades/equity version")
        shutil.rmtree(migrating_dir)
        return

    # 5. Atomic rename: states_migrating → states
    target = config.states_dir
    if target.exists():
        shutil.rmtree(target)
    os.rename(str(migrating_dir), str(target))

    # 6. Write HEAD (committed)
    now = datetime.now()
    head_payload = {
        "schema_version": SCHEMA_VERSION,
        "committed_version_seq": ver,
        "strategies": sorted(strategy_names),
        "last_run_date": legacy.get("last_run_date", now.strftime("%Y-%m-%d")),
        "last_run_ts": legacy.get("last_run_ts", now.isoformat()),
    }
    atomic_write_json(config.head_file, head_payload)

    # 7. LAST: Rename legacy file
    migrated = config.state_file.with_suffix(".json.migrated")
    try:
        os.rename(str(config.state_file), str(migrated))
        logger.info(f"[STATE_V2] Migration complete: {len(strategy_names)} strategies, "
                    f"legacy → {migrated.name}")
    except OSError as e:
        logger.warning(f"[STATE_V2] Legacy rename failed (non-critical): {e}")


def _convert_equity_csv(csv_path: Path) -> List[dict]:
    """Convert legacy equity_history.csv to list of dicts."""
    import csv as csv_mod
    rows = []
    if not csv_path.exists():
        return rows
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                rows.append(dict(row))
    except (OSError, csv_mod.Error) as e:
        logger.warning(f"[STATE_V2] Equity CSV conversion error: {e}")
    return rows


# ═══════════════════════════════════════════════════════════════════
#  Archive: HEAD + states + trades + equity
# ═══════════════════════════════════════════════════════════════════

def archive_state_v2(config) -> Optional[Path]:
    """
    Archive current committed state under archive/{timestamp}/.
    Returns archive directory path, or None on failure.
    """
    with _thread_lock:
        flock = FileLock(config.state_io_lock_file)
        with flock:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_dir = config.state_dir / "archive" / ts
            archive_dir.mkdir(parents=True, exist_ok=True)

            try:
                # head.json
                if config.head_file.exists():
                    shutil.copy2(config.head_file, archive_dir / "head.json")

                # states/
                if config.states_dir.exists():
                    shutil.copytree(config.states_dir,
                                    archive_dir / "states",
                                    dirs_exist_ok=True)

                # trades.json
                if config.trades_file.exists():
                    shutil.copy2(config.trades_file, archive_dir / "trades.json")

                # equity.json
                if config.equity_json_file.exists():
                    shutil.copy2(config.equity_json_file, archive_dir / "equity.json")

                logger.info(f"[STATE_V2] Archived -> {archive_dir}")

                # Rotation: keep only last N archives
                _rotate_archives(config)

                return archive_dir

            except Exception as e:
                logger.error(f"[STATE_V2] Archive failed: {e}")
                # Cleanup partial archive
                try:
                    shutil.rmtree(archive_dir)
                except OSError:
                    pass
                return None
