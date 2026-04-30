# -*- coding: utf-8 -*-
"""kr/pipeline/preflight.py — Pre-execution blocker for mapped runs.

Phase B goal (Jeff): switch from "detect failure after it happens" to
"block before it starts". Five checks, each returning True / False / None
(unknown). If ANY check is False, the run does not enter _execute().

Jeff B constraints enforced:
    §1 최소 범위  — only 5 checks initially
    §2 정책 명확  — fail → marker PRE_FLIGHT_FAIL, incident written,
                    watchdog reads marker; step does NOT enter _execute
    §3 fingerprint  — captured + saved in marker; re-verified at step start
                       (drift → PRE_FLIGHT_STALE_INPUT)
    §4 독립성  — each run_type's preflight is isolated; KR failure does
                NOT block US, and vice versa
    과차단 방지 — None is not False (SKIPPED checks don't downgrade pass/fail)
    기준 일치  — uses the same ChecksBlock schema A2 marker uses; the
                "all non-false" rule mirrors derive_status()

Integration point:
    StepBase.run() calls run_and_record(run_type, state) between
    mark_started/record_start and _execute. Returns (ok, summary).
    Step exits early (skipped_result) when ok=False.
"""
from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from .completion_marker import (
    ChecksBlock,
    CompletionMarker,
    ErrorBlock,
    FingerprintBlock,
    MetricsBlock,
)
from .completion_schema import (
    RUN_KR_BATCH,
    RUN_KR_EOD,
    RUN_US_EOD,
    STATUS_PRE_FLIGHT_FAIL,
    STATUS_PRE_FLIGHT_STALE_INPUT,
    STATUS_RUNNING,
)

_log = logging.getLogger("gen4.pipeline.preflight")


# ---------- Check primitives ----------

@dataclass
class CheckResult:
    """Outcome of a single named check.

    ok:
        True  — the specific property is confirmed
        False — the specific property is broken (blocking)
        None  — the check did not run / is inconclusive (not blocking)
    """
    ok: Optional[bool]
    error: Optional[str] = None
    detail: dict = field(default_factory=dict)


CheckFn = Callable[[str, Any], CheckResult]  # (run_type, state) -> CheckResult


# ---------- Overridable check registry ----------
# Tests/ops can monkeypatch these module-level callables to substitute
# lightweight stubs (e.g. during CI) or stricter versions.

def check_imports(run_type: str, state: Any) -> CheckResult:
    """Verify critical modules import cleanly + no pyc-only bombs in allowlist.

    Uses pyc_guard.detect_pyc_only_modules (filesystem scan, no side effects)
    PLUS a real __import__ of each PYC_CRITICAL_MODULES to catch import-time
    errors (syntax, missing deps, etc.) that the filesystem scan cannot see.
    """
    from shared.util.pyc_guard import detect_pyc_only_modules
    from .completion_schema import PYC_ALLOWLIST_MODULES, PYC_CRITICAL_MODULES

    repo_root = Path(__file__).resolve().parents[2]
    orphans = detect_pyc_only_modules(repo_root, PYC_ALLOWLIST_MODULES)
    # Critical orphans always block
    critical_orphans = [e for e in orphans if e.module in PYC_CRITICAL_MODULES]
    if critical_orphans:
        return CheckResult(
            ok=False,
            error=f"pyc-only critical modules: "
                  f"{sorted(e.module for e in critical_orphans)}",
            detail={"critical_orphans": [e.module for e in critical_orphans],
                    "all_orphans": [e.module for e in orphans]},
        )

    # Try real imports of critical modules
    failed_imports: list[tuple[str, str]] = []
    for mod in sorted(PYC_CRITICAL_MODULES):
        try:
            __import__(mod)
        except Exception as e:  # noqa: BLE001 — any failure blocks
            failed_imports.append((mod, repr(e)))
    if failed_imports:
        return CheckResult(
            ok=False,
            error=f"import failures: {failed_imports}",
            detail={"failed_imports": failed_imports},
        )

    return CheckResult(
        ok=True,
        detail={"non_critical_orphans": [e.module for e in orphans]},
    )


def check_db_upsert(run_type: str, state: Any) -> CheckResult:
    """Connect to PG, run the exact intraday upsert in a transaction, ROLLBACK.

    If no DB config is present (e.g. CI), return None (not blocking).
    Any connection/SQL failure blocks with the captured error text — this
    is how we would have caught the 2026-04-22 `ON CONFLICT` regression
    before it hit EOD.
    """
    try:
        from shared.db.pg_base import get_connection
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=None, error=f"pg_base import failed: {e!r}",
            detail={"skipped": "pg_base import"},
        )

    # DB config check — skip if no env
    if not os.environ.get("PGHOST") and not os.environ.get("QTRON_PG_HOST"):
        return CheckResult(
            ok=None, error="no PG env configured",
            detail={"skipped": "no PG env"},
        )

    try:
        conn = get_connection()
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=False, error=f"PG connect failed: {e!r}",
            detail={"stage": "connect"},
        )

    # The dry-run: start transaction, attempt an intraday upsert that
    # exercises the ON CONFLICT clause, ALWAYS rollback.
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                try:
                    cur.execute(
                        "INSERT INTO intraday "
                        "(symbol, ts, open, high, low, close, volume) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (symbol, ts) DO UPDATE "
                        "SET close=EXCLUDED.close",
                        ("__PREFLIGHT__", datetime(1970, 1, 1),
                         0.0, 0.0, 0.0, 0.0, 0),
                    )
                finally:
                    cur.execute("ROLLBACK")
        return CheckResult(ok=True, detail={"verified": "intraday ON CONFLICT"})
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=False, error=f"DB upsert dry-run failed: {e!r}",
            detail={"stage": "upsert_dry_run"},
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


def check_kospi_parse(run_type: str, state: Any) -> CheckResult:
    """Read KOSPI.csv last rows; parse date column robustly.

    Catches the 2026-04-22 mixed-format bug (ISO `2026-04-22` vs `%Y%m%d`)
    BEFORE live run ingests it.
    """
    repo_root = Path(__file__).resolve().parents[2]
    csv_path = repo_root / "backtest" / "data_full" / "index" / "KOSPI.csv"
    if not csv_path.exists():
        return CheckResult(
            ok=None, error=f"KOSPI.csv absent at {csv_path}",
            detail={"skipped": "file missing", "path": str(csv_path)},
        )

    try:
        import pandas as pd
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=None, error=f"pandas import failed: {e!r}",
            detail={"skipped": "pandas missing"},
        )

    try:
        df = pd.read_csv(csv_path).tail(10)
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=False, error=f"CSV read failed: {e!r}",
            detail={"path": str(csv_path)},
        )

    # Find a date-like column
    date_col = None
    for candidate in ("date", "Date", "DATE", "tdate", "tradedate"):
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        return CheckResult(
            ok=None, error=f"no date-like column in {list(df.columns)}",
            detail={"skipped": "no date column"},
        )

    # Attempt parse. Use format='mixed' for robustness — this is exactly
    # the fix the 2026-04-22 bug called for.
    try:
        parsed = pd.to_datetime(df[date_col], format="mixed", errors="raise")
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=False,
            error=f"date parse failed on last 10 rows of KOSPI.csv: {e!r}",
            detail={"path": str(csv_path), "col": date_col,
                    "sample": list(df[date_col].astype(str).head(5))},
        )

    return CheckResult(
        ok=True,
        detail={"rows_checked": len(df), "col": date_col,
                "last_parsed": str(parsed.iloc[-1])},
    )


def check_report_builder(run_type: str, state: Any) -> CheckResult:
    """Verify the daily-report builder module imports + has expected entry.

    MVP: just check the module can be imported and exposes the function.
    A fuller sample-build check is a follow-up.
    """
    # Map run_type → (module path, required function name). Keep minimal.
    report_map = {
        RUN_KR_EOD: ("kr.web.lab_live.engine", None),  # engine is the umbrella
        RUN_KR_BATCH: ("kr.web.lab_live.engine", None),
        RUN_US_EOD: ("us.lab.forward", None),
    }
    entry = report_map.get(run_type)
    if entry is None:
        return CheckResult(
            ok=None, error=f"no report builder mapped for {run_type}",
            detail={"skipped": "unmapped run_type"},
        )
    module_name, fn_name = entry
    try:
        mod = __import__(module_name, fromlist=["__name__"])
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=False, error=f"report module {module_name} import failed: {e!r}",
            detail={"module": module_name},
        )
    if fn_name is not None and not hasattr(mod, fn_name):
        return CheckResult(
            ok=False, error=f"{module_name}.{fn_name} missing",
            detail={"module": module_name, "fn": fn_name},
        )
    return CheckResult(ok=True, detail={"module": module_name})


def check_write_perm(run_type: str, state: Any) -> CheckResult:
    """Confirm we can atomically write to the marker/state directory."""
    data_dir = Path(state.data_dir)
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".preflight_wperm_", dir=str(data_dir))
        os.close(fd)
        os.unlink(tmp)
    except OSError as e:
        return CheckResult(
            ok=False, error=f"write perm failed in {data_dir}: {e!r}",
            detail={"path": str(data_dir)},
        )
    return CheckResult(ok=True, detail={"path": str(data_dir)})


def check_universe_healthy(run_type: str, state: Any) -> CheckResult:
    """R1 (2026-04-23) — Ensure OHLCV cache produces a non-empty universe.

    Motivation (RCA 20260423): CSV cache truncation → universe=0 → batch
    returns None → lifecycle break. This check catches the condition
    BEFORE batch enters step2.

    Two-stage design (Jeff-mandated):
      1st (lightweight, ~1s): CSV file count + row-count sampling.
      2nd (strict, ~10s): actual build_universe_from_ohlcv() call.

    Only applies to KR_BATCH. Other run_types return None (skipped).

    Note: intentionally does NOT cache — freshness matters more than speed
    given this is batch-critical.
    """
    from .completion_schema import RUN_KR_BATCH  # noqa: WPS433
    if run_type != RUN_KR_BATCH:
        return CheckResult(ok=None, detail={"skipped": "non-KR-batch"})

    # Lazy imports to keep preflight module importable without kr/ deps
    try:
        sys_path_added = False
        repo_root = Path(__file__).resolve().parents[2]
        kr_dir = repo_root / "kr"
        import sys
        if str(kr_dir) not in sys.path:
            sys.path.insert(0, str(kr_dir))
            sys_path_added = True
        from config import Gen4Config  # noqa: WPS433
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=None,
            error=f"Gen4Config import failed: {e!r}",
            detail={"skipped": "config import"},
        )

    try:
        cfg = Gen4Config()
        ohlcv_dir = Path(cfg.OHLCV_DIR)
        min_history = int(cfg.UNIV_MIN_HISTORY)
        min_universe = int(cfg.UNIV_MIN_COUNT)
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=None,
            error=f"Gen4Config init failed: {e!r}",
            detail={"skipped": "config init"},
        )

    # ---------- 1st stage: lightweight (file count + sampling) ----------
    if not ohlcv_dir.exists():
        return CheckResult(
            ok=False,
            error=f"[reason:dir_missing] OHLCV dir missing: {ohlcv_dir}",
            detail={"stage": "1st", "issue": "dir_missing",
                    "reason_code": "dir_missing", "ohlcv_dir": str(ohlcv_dir)},
        )

    csvs = list(ohlcv_dir.glob("*.csv"))
    csv_count = len(csvs)
    MIN_CSV_COUNT = 2500
    if csv_count < MIN_CSV_COUNT:
        return CheckResult(
            ok=False,
            error=f"[reason:csv_count_low] CSV count {csv_count} < {MIN_CSV_COUNT}",
            detail={"stage": "1st", "csv_count": csv_count,
                    "reason_code": "csv_count_low", "ohlcv_dir": str(ohlcv_dir)},
        )

    # Sample 50 files — check history length
    import random  # noqa: WPS433
    sample_size = min(50, csv_count)
    sample = random.sample(csvs, sample_size)

    try:
        import pandas as pd  # noqa: WPS433
    except Exception as e:  # noqa: BLE001
        return CheckResult(
            ok=None,
            error=f"pandas import failed: {e!r}",
            detail={"stage": "1st", "issue": "pandas import"},
        )

    history_pass = 0
    read_failures = 0
    latest_date = None
    for f in sample:
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            if len(df) >= min_history:
                history_pass += 1
            if not df.empty:
                d = df["date"].max()
                if latest_date is None or d > latest_date:
                    latest_date = d
        except Exception:  # noqa: BLE001
            read_failures += 1

    history_ratio = history_pass / sample_size if sample_size else 0.0
    MIN_HISTORY_PASS_RATIO = 0.80
    if history_ratio < MIN_HISTORY_PASS_RATIO:
        return CheckResult(
            ok=False,
            error=(f"[reason:history_sample_low] history sample: "
                   f"{history_pass}/{sample_size} "
                   f"({history_ratio:.1%}) < {MIN_HISTORY_PASS_RATIO:.0%}"),
            detail={
                "stage": "1st",
                "csv_count": csv_count,
                "sample_size": sample_size,
                "history_pass": history_pass,
                "history_ratio": round(history_ratio, 3),
                "read_failures": read_failures,
                "reason_code": "history_sample_low",
                "ohlcv_dir": str(ohlcv_dir),
            },
        )

    # ---------- 2nd stage: strict (real universe build) ----------
    try:
        from data.universe_builder import build_universe_from_ohlcv  # noqa: WPS433
    except Exception as e:  # noqa: BLE001
        # Code/env error — NOT recoverable by data restore.
        return CheckResult(
            ok=None,
            error=f"universe_builder import failed: {e!r}",
            detail={"stage": "2nd", "issue": "import",
                    "reason_code": "import_failed"},
        )

    try:
        universe = build_universe_from_ohlcv(
            ohlcv_dir,
            min_close=cfg.UNIV_MIN_CLOSE,
            min_amount=cfg.UNIV_MIN_AMOUNT,
            min_history=min_history,
            min_count=min_universe,
            allowed_markets=cfg.MARKETS,
            sector_map={},
        )
    except Exception as e:  # noqa: BLE001
        # Logic crash — NOT recoverable by data restore.
        return CheckResult(
            ok=False,
            error=f"universe_builder crash: {e!r}",
            detail={"stage": "2nd", "issue": "build_crash",
                    "reason_code": "build_crash"},
        )

    if len(universe) < min_universe:
        return CheckResult(
            ok=False,
            error=(f"[reason:universe_size_low] universe too small: "
                   f"{len(universe)} < {min_universe}"),
            detail={
                "stage": "2nd",
                "universe_count": len(universe),
                "min_universe": min_universe,
                "csv_count": csv_count,
                "history_ratio": round(history_ratio, 3),
                "reason_code": "universe_size_low",
                "ohlcv_dir": str(ohlcv_dir),
            },
        )

    return CheckResult(
        ok=True,
        detail={
            "csv_count": csv_count,
            "sample_size": sample_size,
            "history_ratio": round(history_ratio, 3),
            "latest_sampled_date": str(latest_date) if latest_date else None,
            "universe_count": len(universe),
        },
    )


# Per-run_type check list — run order matters (imports first, cheap → expensive)
# R1 (2026-04-23): universe_healthy added to KR_BATCH — placed last because
# it's the most expensive check (~10s for real universe build).
CHECKS_FOR_RUN: dict[str, list[tuple[str, CheckFn]]] = {
    RUN_KR_BATCH: [
        ("imports_ok", check_imports),
        ("write_perm_ok", check_write_perm),
        ("db_upsert_ok", check_db_upsert),
        ("kospi_parse_ok", check_kospi_parse),
        ("report_ok", check_report_builder),
        ("universe_healthy", check_universe_healthy),  # R1
    ],
    RUN_KR_EOD: [
        ("imports_ok", check_imports),
        ("write_perm_ok", check_write_perm),
        ("db_upsert_ok", check_db_upsert),
        ("kospi_parse_ok", check_kospi_parse),
        ("report_ok", check_report_builder),
    ],
    RUN_US_EOD: [
        ("imports_ok", check_imports),
        ("write_perm_ok", check_write_perm),
        ("db_upsert_ok", check_db_upsert),
        # NB: no KOSPI for US
        ("report_ok", check_report_builder),
    ],
}


# ---------- Fingerprint ----------

def _file_fingerprint(path: Path, tail_bytes: int = 256) -> Optional[dict]:
    try:
        st = path.stat()
    except OSError:
        return None
    try:
        with path.open("rb") as f:
            f.seek(max(0, st.st_size - tail_bytes))
            tail = f.read()
        tail_sha = hashlib.sha256(tail).hexdigest()[:32]
    except OSError:
        tail_sha = None
    return {
        "path": str(path),
        "mtime": st.st_mtime,
        "size": st.st_size,
        "tail_sha256": tail_sha,
    }


def _git_head_sha() -> Optional[str]:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=2,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        if out.returncode == 0:
            return out.stdout.strip()[:12]
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def _input_files_for(run_type: str) -> list[Path]:
    repo_root = Path(__file__).resolve().parents[2]
    base = [
        repo_root / "kr" / "data" / "lab_live" / "head.json",
    ]
    if run_type in (RUN_KR_BATCH, RUN_KR_EOD):
        base.append(repo_root / "backtest" / "data_full" / "index" / "KOSPI.csv")
    return base


def compute_fingerprint(run_type: str, *, clock: Any = None) -> FingerprintBlock:
    """Snapshot inputs + code state. Pure read, <2s budget."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    repo_root = Path(__file__).resolve().parents[2]

    inputs = []
    for p in _input_files_for(run_type):
        fp = _file_fingerprint(p)
        if fp is not None:
            inputs.append(fp)

    code_modules: dict[str, str] = {}
    # Hash the source files of critical modules. Lookup by dotted name.
    from .completion_schema import PYC_CRITICAL_MODULES
    for mod in sorted(PYC_CRITICAL_MODULES):
        py_path = repo_root.joinpath(*mod.split(".")).with_suffix(".py")
        if py_path.exists():
            try:
                code_modules[mod] = hashlib.sha256(py_path.read_bytes()).hexdigest()[:16]
            except OSError:
                code_modules[mod] = "READ_FAILED"

    return FingerprintBlock(
        captured_at=clock(),
        git_head_sha=_git_head_sha(),
        db_schema_version=None,  # TODO: query schema_migrations if present
        db_target=os.environ.get("PGHOST") or os.environ.get("QTRON_PG_HOST"),
        inputs=inputs,
        code_modules=code_modules,
    )


def validate_fingerprint(
    current: FingerprintBlock, stored: FingerprintBlock,
) -> tuple[bool, str, str]:
    """Jeff v3 §Hardening-2 strict compare. Returns (ok, category, detail).

    Categories:
        OK            — identical (or no meaningful drift)
        CODE_CHANGED   — any code_modules sha differs
        GIT_CHANGED    — git_head_sha differs
        DB_CHANGED     — db_target or db_schema_version differs
        INPUT_CHANGED  — inputs list differs (can retry preflight once)
        UNKNOWN_DRIFT  — some other mismatch
    """
    if current.code_modules != stored.code_modules:
        diff = {
            k: (stored.code_modules.get(k), current.code_modules.get(k))
            for k in set(current.code_modules) | set(stored.code_modules)
            if current.code_modules.get(k) != stored.code_modules.get(k)
        }
        return False, "CODE_CHANGED", f"modules differ: {diff}"
    if current.git_head_sha != stored.git_head_sha:
        return False, "GIT_CHANGED", \
            f"git head {stored.git_head_sha} → {current.git_head_sha}"
    if current.db_target != stored.db_target:
        return False, "DB_CHANGED", \
            f"db_target {stored.db_target} → {current.db_target}"
    if current.db_schema_version != stored.db_schema_version:
        return False, "DB_CHANGED", \
            f"db_schema_version {stored.db_schema_version} → {current.db_schema_version}"
    if current.inputs != stored.inputs:
        return False, "INPUT_CHANGED", \
            f"inputs diff: {_inputs_diff(current.inputs, stored.inputs)}"
    return True, "OK", "identical"


def _inputs_diff(cur: list[dict], sto: list[dict]) -> list[dict]:
    """Minimal diff summary for logging."""
    by_path_cur = {i.get("path"): i for i in cur}
    by_path_sto = {i.get("path"): i for i in sto}
    diffs = []
    for path in set(by_path_cur) | set(by_path_sto):
        c, s = by_path_cur.get(path), by_path_sto.get(path)
        if c != s:
            diffs.append({"path": path, "cur": c, "sto": s})
    return diffs


# ---------- Preflight orchestration ----------

@dataclass
class PreflightOutcome:
    run_type: str
    ok: bool                            # True iff no check returned False
    checks: ChecksBlock
    errors: dict[str, str]              # per-check error text
    fingerprint: FingerprintBlock
    summary: str                        # one-line human summary
    # R6 (2026-04-24): numeric metrics harvested from check details so
    # consumers (marker, dashboard) can track trends (universe_count drift).
    metrics: "MetricsBlock" = field(default_factory=lambda: MetricsBlock())

    def blocking_checks(self) -> list[str]:
        return [k for k, v in self.checks.to_dict().items() if v is False]


def _run_checks(run_type: str, state: Any) -> PreflightOutcome:
    check_list = CHECKS_FOR_RUN.get(run_type, [])
    results: dict[str, CheckResult] = {}
    errors: dict[str, str] = {}

    for check_name, check_fn in check_list:
        try:
            r = check_fn(run_type, state)
        except Exception as e:  # noqa: BLE001 — check must never crash preflight
            _log.exception("[PREFLIGHT_CHECK_CRASH] %s", check_name)
            r = CheckResult(ok=False, error=f"check_crash: {e!r}",
                            detail={"crash": True})
        results[check_name] = r
        if r.error:
            errors[check_name] = r.error

    checks = ChecksBlock(**{
        name: results[name].ok for name in (
            "imports_ok", "db_upsert_ok", "kospi_parse_ok",
            "report_ok", "head_updated", "write_perm_ok",
            "universe_healthy",  # R1 (2026-04-23)
        ) if name in results
    })

    # R6: harvest numeric metrics from check details (check_universe_healthy
    # already puts `universe_count` in detail; more metrics added here as
    # producers expose them).
    uh = results.get("universe_healthy")
    uni_count = None
    if uh is not None and isinstance(uh.detail, dict):
        try:
            raw = uh.detail.get("universe_count")
            if raw is not None:
                uni_count = int(raw)
        except (TypeError, ValueError):
            uni_count = None
    metrics = MetricsBlock(universe_count=uni_count)

    blocking = [k for k, r in results.items() if r.ok is False]
    ok = len(blocking) == 0
    summary = (
        f"all {len(results)} checks pass" if ok
        else f"blocked by: {blocking}"
    )
    fp = compute_fingerprint(run_type)

    return PreflightOutcome(
        run_type=run_type, ok=ok, checks=checks, errors=errors,
        fingerprint=fp, summary=summary, metrics=metrics,
    )


def _maybe_auto_recover(
    run_type: str, state: Any, outcome: PreflightOutcome,
) -> PreflightOutcome:
    """Item 2 (2026-04-30 RCA) — one-shot OHLCV recovery between checks.

    If any failed check carries a recoverable reason_code (currently:
    only ``universe_healthy``), invoke the restore script via
    ``preflight_recovery.try_auto_recover`` and re-run preflight ONCE.
    The single-shot behavior is enforced by ``preflight_recovery``'s
    persisted marker, so this function may be called many times across
    orchestrator ticks without triggering a recovery storm.

    Returns the (possibly new) outcome. Original outcome on no-op.
    Never raises.
    """
    if outcome.ok:
        return outcome
    if "universe_healthy" not in outcome.errors:
        return outcome  # other failures — recovery would not help

    try:
        from . import preflight_recovery  # local import (lazy)
    except Exception as e:  # noqa: BLE001
        _log.warning("[PREFLIGHT_AUTO_RECOVERY_IMPORT_FAIL] %r", e)
        return outcome

    # Re-run check_universe_healthy to capture the live CheckResult
    # (with `detail.reason_code` needed for recovery classification).
    try:
        cr = check_universe_healthy(run_type, state)
    except Exception as e:  # noqa: BLE001
        _log.warning("[PREFLIGHT_AUTO_RECOVERY_PROBE_FAIL] %r", e)
        return outcome

    if not preflight_recovery.is_recoverable(cr):
        return outcome

    recovered, evidence = preflight_recovery.try_auto_recover(
        run_type=run_type, check_name="universe_healthy",
        check_result=cr, state=state, logger=_log,
    )
    if not recovered:
        return outcome  # original failure stands; recovery already logged

    # Re-run ALL checks once after successful recovery.
    new_outcome = _run_checks(run_type, state)
    _log.warning(
        "[PREFLIGHT_AUTO_RECOVERY_RECHECK] run_type=%s prev_ok=%s "
        "new_ok=%s elapsed_recovery=%s",
        run_type, outcome.ok, new_outcome.ok,
        evidence.get("elapsed_sec"),
    )
    return new_outcome


def run_and_record(run_type: str, state: Any) -> PreflightOutcome:
    """Public entry point called from StepBase.run() pre-execute hook.

    Responsibilities:
      1. Run all checks (never raises — any exception becomes a False check).
      2. (Item 2) Auto-recover OHLCV via preflight_recovery if any failed
         check is classified recoverable, then re-run checks ONCE.
      3. Compute fingerprint.
      4. Re-verify prior fingerprint if marker already holds one (drift).
      5. Write outcome into marker:
           - all pass + no drift → attach fingerprint+checks to RUNNING run
           - any fail → transition RUNNING → PRE_FLIGHT_FAIL + incident
           - drift → transition RUNNING → PRE_FLIGHT_STALE_INPUT + incident
      6. Return PreflightOutcome so caller can decide to skip _execute.

    Never propagates exceptions. Marker/incident failures are logged and
    swallowed (Jeff B §4 독립성).
    """
    outcome = _run_checks(run_type, state)
    outcome = _maybe_auto_recover(run_type, state, outcome)

    # Load marker and possibly check drift
    try:
        marker = CompletionMarker.load_or_create_today(
            data_dir=state.data_dir, trade_date=state.trade_date)
    except Exception as e:  # noqa: BLE001
        _log.critical("[PREFLIGHT_MARKER_LOAD_FAIL] %r", e)
        return outcome  # can't record; return whatever we computed

    # Drift check: if a prior fingerprint exists for this run_type, compare.
    stored_fp = marker.run(run_type).preflight_fingerprint
    drift_code: Optional[str] = None
    drift_detail: Optional[str] = None
    if stored_fp is not None:
        fp_ok, category, detail = validate_fingerprint(outcome.fingerprint, stored_fp)
        if not fp_ok:
            drift_code = category
            drift_detail = detail

    try:
        if drift_code and drift_code != "INPUT_CHANGED":
            # CODE/GIT/DB drift is hard-block: no rerun allowed
            # Ensure RUNNING before terminal transition (if called before
            # marker_integration.record_start has run)
            if marker.run(run_type).status not in (STATUS_RUNNING,):
                marker.transition(run_type, STATUS_RUNNING)
            marker.transition(
                    run_type, STATUS_PRE_FLIGHT_STALE_INPUT,
                    checks=outcome.checks,
                    fingerprint=outcome.fingerprint,
                    error=ErrorBlock(
                        stage="preflight_drift",
                        message=f"{drift_code}: {drift_detail}",
                    ),
                )
            marker.save()
            _log.critical(
                "[PREFLIGHT_STALE_INPUT] run_type=%s %s: %s",
                run_type, drift_code, drift_detail,
            )
            from . import incident_writer
            incident_writer.write_if_new(marker, run_type, data_dir=state.data_dir)
            outcome = PreflightOutcome(
                run_type=outcome.run_type, ok=False, checks=outcome.checks,
                errors={**outcome.errors, "_drift": f"{drift_code}: {drift_detail}"},
                fingerprint=outcome.fingerprint,
                summary=f"STALE_INPUT: {drift_code}",
            )
            return outcome

        if not outcome.ok:
            # Preflight fail — transition to PRE_FLIGHT_FAIL
            blocking = outcome.blocking_checks()
            err_msg = "; ".join(
                f"{k}={outcome.errors.get(k, '?')}" for k in blocking
            )
            # Ensure RUNNING before terminal transition (if called before
            # marker_integration.record_start has run)
            if marker.run(run_type).status not in (STATUS_RUNNING,):
                marker.transition(run_type, STATUS_RUNNING)
            marker.transition(
                    run_type, STATUS_PRE_FLIGHT_FAIL,
                    checks=outcome.checks,
                    fingerprint=outcome.fingerprint,
                    error=ErrorBlock(
                        stage="preflight",
                        message=err_msg[:2000],
                    ),
                )
            marker.save()
            _log.critical(
                "[PREFLIGHT_FAIL] run_type=%s blocking=%s",
                run_type, blocking,
            )
            from . import incident_writer
            incident_writer.write_if_new(marker, run_type, data_dir=state.data_dir)
            return outcome

        # All pass — attach fingerprint + checks + metrics to running entry
        marker.set_attrs(
            run_type,
            checks=outcome.checks,
            metrics=outcome.metrics,
            fingerprint=outcome.fingerprint,
        )
        marker.save()
        _log.info(
            "[PREFLIGHT_PASS] run_type=%s %s",
            run_type, outcome.summary,
        )
    except Exception as e:  # noqa: BLE001
        _log.critical(
            "[PREFLIGHT_MARKER_WRITE_FAIL] run_type=%s err=%r",
            run_type, e,
        )
        # Non-fatal: step decides independently based on outcome.ok

    return outcome
