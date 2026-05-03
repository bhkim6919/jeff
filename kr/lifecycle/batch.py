"""
Batch mode entry point extracted from main.py.
"""
from __future__ import annotations
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from lifecycle.utils import is_weekday

try:
    from notify.helpers import alert_data_failure as _alert_data
except Exception:
    def _alert_data(*a, **kw): pass  # notify 미초기화 시 no-op


# ── Inter-process Lock (PR 3 / AUD-P1-D) ─────────────────────────────
#
# Tray scheduler + web manual button + CLI invocation all converge on
# run_batch(). Without a lock, two concurrent invocations can interleave
# checkpoint reads/writes and corrupt OHLCV CSV append. Mirror pattern
# from us/lab/forward.py:_acquire_lock — PID liveness + 30-min stale
# recovery + atomic JSON write.

LOCK_STALE_SECONDS = 1800  # 30 min — match us/lab/forward.py


def _atomic_write_json(path: Path, data: dict) -> None:
    """Atomic JSON write: tmp → fsync → os.replace. Survives crash mid-write."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        try:
            os.fsync(f.fileno())  # durability — ignored on platforms that
        except Exception:         # don't support fsync on regular files
            pass
    os.replace(str(tmp), str(path))


def _batch_lock_path(config) -> Path:
    return Path(config.OHLCV_DIR).parent / "batch.lock"


def _pid_alive(pid: int) -> bool:
    """Best-effort PID liveness probe. Tolerant on Windows + POSIX."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _process_start_ts(pid: int) -> Optional[float]:
    """Best-effort process creation time (epoch seconds) for a PID.

    Used to detect PID reuse across long uptime: if the lock metadata
    stored a process_start_ts and the current PID's process_start_ts
    differs, the PID was recycled (lock owner crashed, OS reused PID).

    Returns None on any failure — caller falls back to PID liveness +
    timestamp staleness only (existing behavior). Never raises.
    """
    if pid <= 0:
        return None
    try:
        # Windows: GetProcessTimes via ctypes
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

            kernel32.OpenProcess.restype = wintypes.HANDLE
            kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.GetProcessTimes.restype = wintypes.BOOL

            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not handle:
                return None
            try:
                class _FILETIME(ctypes.Structure):
                    _fields_ = [
                        ("dwLowDateTime", wintypes.DWORD),
                        ("dwHighDateTime", wintypes.DWORD),
                    ]
                create = _FILETIME()
                exit_ = _FILETIME()
                kern = _FILETIME()
                user = _FILETIME()
                ok = kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(create), ctypes.byref(exit_),
                    ctypes.byref(kern), ctypes.byref(user))
                if not ok:
                    return None
                # FILETIME = 100-ns intervals since 1601-01-01 UTC.
                t = (create.dwHighDateTime << 32) | create.dwLowDateTime
                # Convert to epoch seconds (subtract 1601→1970 offset).
                return (t - 116444736000000000) / 10_000_000.0
            finally:
                kernel32.CloseHandle(handle)
        # POSIX: /proc/<pid>/stat field 22 = starttime in clock ticks
        # since boot. Combine with /proc/uptime + boot time.
        else:
            stat_path = Path(f"/proc/{pid}/stat")
            if not stat_path.exists():
                return None
            raw = stat_path.read_text(encoding="ascii", errors="replace")
            # field layout: pid (comm) state ppid ... starttime is field 22
            # comm may contain spaces, so split on closing paren first.
            close_paren = raw.rfind(")")
            if close_paren < 0:
                return None
            after = raw[close_paren + 1 :].split()
            # field 22 = index 19 in `after` (state is index 0)
            if len(after) < 20:
                return None
            ticks = int(after[19])
            clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
            uptime_path = Path("/proc/uptime")
            if not uptime_path.exists():
                return None
            uptime_sec = float(uptime_path.read_text().split()[0])
            boot_time = time.time() - uptime_sec
            return boot_time + (ticks / clk_tck)
    except Exception:
        return None


def _acquire_batch_lock(config, logger) -> bool:
    """Try to acquire batch lock. Returns True if acquired, False if held by
    another active process.

    Stale recovery rules (any one triggers clear):
      - PID dead (os.kill(pid, 0) fails)
      - started_at > 30 min old
      - PID reused: stored process_start_ts differs from current
        process_start_ts of that PID (best-effort; gracefully degrades
        when start-time probe fails — falls back to existing checks).
    """
    lock_p = _batch_lock_path(config)
    try:
        lock_p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    if lock_p.exists():
        existing = None
        try:
            existing = json.loads(lock_p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"[BATCH_LOCK_PARSE_FAIL] {e} — treating as stale")

        if isinstance(existing, dict):
            pid = int(existing.get("pid", 0) or 0)
            started = existing.get("started_at", "") or ""
            stored_proc_ts = existing.get("process_start_ts")

            alive = _pid_alive(pid)
            stale_time = True
            if started:
                try:
                    dt = datetime.fromisoformat(started)
                    age = (datetime.now() - dt.replace(tzinfo=None)).total_seconds()
                    stale_time = age > LOCK_STALE_SECONDS
                except Exception:
                    stale_time = True

            # PID-reuse detection (JUG follow-up): if lock stored a
            # process_start_ts and we can read the current one for that
            # PID, a mismatch (>1s tolerance) means the original process
            # died and the OS recycled the PID for a different process.
            pid_reused = False
            if alive and isinstance(stored_proc_ts, (int, float)):
                current_proc_ts = _process_start_ts(pid)
                if current_proc_ts is not None:
                    if abs(current_proc_ts - float(stored_proc_ts)) > 1.0:
                        pid_reused = True

            if alive and not stale_time and not pid_reused:
                logger.critical(
                    f"[BATCH_LOCK_HELD] another batch is active "
                    f"(pid={pid} since={started}) — refusing to start"
                )
                return False

            logger.warning(
                f"[BATCH_LOCK_STALE] clearing stale lock "
                f"(pid={pid} alive={alive} stale_time={stale_time} "
                f"pid_reused={pid_reused})"
            )
        else:
            logger.warning("[BATCH_LOCK_INVALID] unparseable lock — clearing")

        try:
            lock_p.unlink()
        except Exception as e:
            logger.warning(f"[BATCH_LOCK_UNLINK_FAIL] {e}")
            return False

    try:
        own_proc_ts = _process_start_ts(os.getpid())
        _atomic_write_json(lock_p, {
            "pid": os.getpid(),
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "host": os.environ.get("COMPUTERNAME", "") or os.environ.get("HOSTNAME", ""),
            # process_start_ts: epoch seconds when the lock-owner process
            # was created. None if probe failed (degrades gracefully —
            # PID-reuse detection becomes a no-op, existing checks remain).
            "process_start_ts": own_proc_ts,
        })
        return True
    except Exception as e:
        logger.error(f"[BATCH_LOCK_ACQUIRE_FAIL] {e}")
        return False


def _release_batch_lock(config, logger) -> None:
    """Release batch lock if owned by current PID. No-op if not owner
    (avoid stomping a stale-recovered concurrent run)."""
    lock_p = _batch_lock_path(config)
    if not lock_p.exists():
        return
    try:
        data = json.loads(lock_p.read_text(encoding="utf-8"))
        if int(data.get("pid", 0) or 0) != os.getpid():
            logger.warning(
                f"[BATCH_LOCK_RELEASE_SKIP] lock_pid={data.get('pid')} "
                f"self={os.getpid()} — not owner, leaving lock"
            )
            return
    except Exception:
        # Unreadable lock — proceed with unlink (we're cleaning up, not
        # claiming ownership).
        pass
    try:
        lock_p.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"[BATCH_LOCK_RELEASE_FAIL] {e}")


# R16 (2026-04-23): Fundamental snapshot hard timeout
#
# Background: 2026-04-23 batch hung in Step 5 Fundamental snapshot for 60+
# minutes. Root cause: fetch_daily_fundamental_naver loops 2770 stocks with
# requests.get(timeout=10) and sleep(0.35). No internal timeout → unbounded
# runtime when Naver rate-limits.
#
# Sizing (measured 2026-04-23 via CLI on production repo):
#   - 74s for 50 stocks → 1.48s/stock (stable rate, 100-stock test confirmed)
#   - 2770 stocks × 1.48s = 4099s ≈ 68 min normal case
#   - Jeff spec: normal + 30 min buffer = 98 min → round to 100 min
#
# Fix: wrap call with hard thread-level timeout. On timeout return None
# (existing handling: logger.warning + _alert_data + continue). Lab
# strategies use latest-available fundamental file on timeout.
FUNDAMENTAL_SNAPSHOT_TIMEOUT_SEC = 6000  # 100 minutes (normal ~68min + 30min buffer)


def _fetch_daily_snapshot_with_timeout(logger, timeout_sec: int = FUNDAMENTAL_SNAPSHOT_TIMEOUT_SEC):
    """Call fetch_daily_snapshot with a hard thread timeout. None on timeout.

    The underlying function may be slow (Naver Finance 2770-stock crawl)
    or hang on rate limits. This wrapper guarantees the caller returns
    within `timeout_sec`, matching existing "returned None" handling
    downstream (batch step 5/6 tolerate None gracefully).

    Implementation note: we deliberately do NOT use `with ... as executor`
    because that calls shutdown(wait=True) on exit — which blocks the
    caller until the daemon thread finishes, defeating the purpose of the
    timeout. Instead we call shutdown(wait=False) so the orphaned thread
    continues in the background while we return immediately.
    """
    import concurrent.futures as _cf
    from data.fundamental_collector import fetch_daily_snapshot

    executor = _cf.ThreadPoolExecutor(
        max_workers=1, thread_name_prefix="fund-snapshot",
    )
    future = executor.submit(fetch_daily_snapshot)
    try:
        result = future.result(timeout=timeout_sec)
        executor.shutdown(wait=False)
        return result
    except _cf.TimeoutError:
        logger.warning(
            f"[FUND_TIMEOUT] fetch_daily_snapshot exceeded "
            f"{timeout_sec}s hard limit — returning None. "
            f"Lab uses latest-available file. "
            f"Daemon thread continues orphaned in background."
        )
        try:
            _alert_data(
                "fundamental_timeout",
                f"fetch_daily_snapshot > {timeout_sec}s",
                {"timeout_sec": timeout_sec},
            )
        except Exception:
            pass
        # Abandon the still-running task — daemon=False on the underlying
        # thread would block process exit, but ThreadPoolExecutor workers
        # are daemon=True in CPython so process can still terminate.
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            # Python < 3.9 fallback — cancel_futures unsupported
            executor.shutdown(wait=False)
        return None
    except Exception:
        executor.shutdown(wait=False)
        raise


def _ensure_fundamental_csv(fund_path: Path, fund_date: str, logger) -> "Optional[pandas.DataFrame]":  # noqa: F821
    """R26 (2026-04-24): CSV 존재하면 재사용, 없으면 fetch + write.

    Returns DataFrame (CSV 또는 fetch 결과) 또는 None (fetch 실패/timeout).
    DB upsert 판정과 분리되어, DB 갱신을 보장하지 않던 구 skip 패턴을 대체.
    """
    import pandas as _pd
    if fund_path.exists():
        try:
            df = _pd.read_csv(fund_path)
            logger.info(f"  Fundamental CSV reused: {fund_path} ({len(df)} stocks)")
            return df
        except Exception as e:
            logger.warning(f"  Fundamental CSV read failed: {e} → refetching")

    # CSV 없음 또는 read 실패 → fetch
    fund_df = _fetch_daily_snapshot_with_timeout(logger)
    if fund_df is None:
        logger.warning("  Fundamental: fetch_daily_snapshot returned None")
        return None
    try:
        fund_df.to_csv(fund_path, index=False)
        logger.info(f"  Fundamental fetched + saved: {fund_path} ({len(fund_df)} stocks)")
    except Exception as e:
        logger.warning(f"  Fundamental CSV write failed: {e} (DB upsert still proceeds)")
    return fund_df


def _ensure_fundamental_db(fund_date: str, fund_df, logger) -> None:
    """R26 (2026-04-24): DB 독립 판정 — 해당 fund_date 의 fundamental 행이 DB 에
    없으면 upsert. 있으면 skip. CSV/DB 가 다른 단계에서 disjoint 하게 갱신되므로
    이 경계를 명확히 분리한다.
    """
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        if db.has_fundamental_for(fund_date):
            logger.info(f"  Fundamental DB already fresh for {fund_date} — skip upsert")
            return
        n = db.upsert_fundamental(fund_date, fund_df)
        logger.info(f"  Fundamental DB upsert: {n} rows (date={fund_date})")
    except Exception as e2:
        logger.warning(f"  Fundamental DB save failed: {e2}")
        try:
            _alert_data("fundamental_db", str(e2), {"fund_date": fund_date})
        except Exception:
            pass


def _load_checkpoint(config) -> dict:
    """배치 진행 체크포인트 로드. 중단 후 재시작 시 완료 단계 skip."""
    cp_path = Path(config.OHLCV_DIR).parent / "batch_checkpoint.json"
    if cp_path.exists():
        try:
            cp = json.loads(cp_path.read_text(encoding="utf-8"))
            if cp.get("date") == datetime.now().strftime("%Y-%m-%d"):
                return cp
        except Exception:
            pass
    return {"date": datetime.now().strftime("%Y-%m-%d"), "completed_steps": []}


def _save_checkpoint(config, step: str, cp: dict) -> None:
    """완료 단계를 체크포인트에 기록. PR 3: atomic write (tmp → os.replace)
    to survive crash / concurrent reader without partial-JSON corruption.
    """
    cp_path = Path(config.OHLCV_DIR).parent / "batch_checkpoint.json"
    if step not in cp.get("completed_steps", []):
        cp.setdefault("completed_steps", []).append(step)
    cp["last_step"] = step
    cp["last_ts"] = datetime.now().isoformat(timespec="seconds")
    try:
        _atomic_write_json(cp_path, cp)
    except Exception:
        pass


def run_batch(config, fast: bool = False):
    """Batch: pykrx update → universe → scoring → target portfolio.

    중단 후 재시작 시 완료 단계는 skip (batch_checkpoint.json 기반).

    PR 3 (AUD-P1-D): inter-process lock prevents tray + web manual
    + CLI invocations from interleaving. Returns None and logs CRITICAL
    on lock conflict (existing callers treat falsy return as "no result").
    """
    logger = logging.getLogger("gen4.batch")

    # Acquire inter-process lock — fail fast if another batch is active.
    # Stale locks (dead PID or > 30 min old) are cleared automatically.
    if not _acquire_batch_lock(config, logger):
        logger.critical(
            "[BATCH_LOCK_REJECTED] batch declined: another batch is "
            "running. See [BATCH_LOCK_HELD] log entry for owner PID."
        )
        return None

    try:
        return _run_batch_inner(config, fast=fast, logger=logger)
    finally:
        _release_batch_lock(config, logger)


def _run_batch_inner(config, fast: bool, logger):
    """Original run_batch body — extracted so the outer wrapper can manage
    the inter-process lock without indenting the entire body."""
    from data.pykrx_provider import update_ohlcv_incremental, get_stock_list
    from data.universe_builder import build_universe_from_ohlcv
    from strategy.factor_ranker import build_target_portfolio, save_target_portfolio
    import pandas as pd

    logger.info("=" * 60)
    logger.info("  Gen4 Batch Mode")
    logger.info("=" * 60)

    # 체크포인트 로드 (동일 날짜 재실행 시 완료 단계 skip)
    _cp = _load_checkpoint(config)
    _done = set(_cp.get("completed_steps", []))
    if _done:
        logger.info(f"  [RESUME] Skipping completed steps: {sorted(_done)}")

    ohlcv_dir = config.OHLCV_DIR

    # Step 1: pykrx OHLCV update (existing + new listings)
    # R26 (2026-04-24): CSV update 와 DB sync 를 독립 checkpoint 로 분리.
    # 레거시 "step1_ohlcv" 토큰은 두 단계 모두 완료로 해석 (backward-compat).
    _legacy_done = "step1_ohlcv" in _done
    _csv_done = _legacy_done or "step1_ohlcv_csv" in _done
    _db_done = _legacy_done or "step1_ohlcv_db" in _done

    # 1a. CSV update (pykrx incremental)
    if _csv_done:
        logger.info("[1/5] OHLCV CSV update — SKIP (checkpoint)")
    elif not is_weekday():
        logger.info("  Skipping pykrx update — weekend, using existing data")
        _csv_done = True
    else:
        try:
            existing = set(f.stem for f in ohlcv_dir.glob("*.csv"))
            live_list = set()
            for market in config.MARKETS:
                try:
                    market_list = set(get_stock_list(market, ohlcv_dir=ohlcv_dir))
                    live_list |= market_list
                    logger.info(f"  {market}: {len(market_list)} tickers")
                except Exception as e:
                    logger.warning(f"  {market} ticker list failed: {e}")
            codes = sorted(existing | live_list)
            new_count = len(live_list - existing)
            if new_count > 0:
                logger.info(f"  New listings detected: {new_count} stocks")
            if codes:
                updated = update_ohlcv_incremental(ohlcv_dir, codes, days=30)
                logger.info(f"  Updated {updated}/{len(codes)} stocks")
            _save_checkpoint(config, "step1_ohlcv_csv", _cp)
            _csv_done = True
        except Exception as e:
            logger.warning(f"  pykrx update failed: {e}. Using existing data.")

        # KOSPI index 업데이트 (DB + CSV) — CSV block 소속
        _update_kospi_index(config, logger)

    # 1b. DB sync (CSV → DB upsert, CSV 성공 여부 무관 독립 실행)
    # 기존 문제: CSV update 완료 후 DB upsert 실패해도 step1_ohlcv 로 묶여 저장됐음 →
    # 재시작 시 DB stale 방치. 지금은 DB 전용 체크포인트로 재시도 가능.
    if _db_done:
        logger.info("[1/5] OHLCV DB sync — SKIP (checkpoint)")
    else:
        try:
            from data.db_provider import DbProvider
            import pandas as _pd
            db = DbProvider()
            db_synced = 0
            # CSV dir 전체 스캔: CSV update 가 skip/실패한 경우에도 DB 보수
            for csv_path in sorted(ohlcv_dir.glob("*.csv")):
                try:
                    _df = _pd.read_csv(csv_path, parse_dates=["date"])
                    _df = _df.tail(5)
                    if not _df.empty:
                        db.upsert_ohlcv(csv_path.stem, _df)
                        db_synced += 1
                except Exception:
                    continue
            logger.info(f"  DB synced: {db_synced} stocks")
            _save_checkpoint(config, "step1_ohlcv_db", _cp)
        except Exception as e2:
            logger.warning(f"  DB sync failed: {e2} (non-critical)")

    # Step 2: Build universe
    logger.info("[2/5] Building universe...")
    # Load sector map for market filter
    _sector_map_path = config.BASE_DIR / "data" / "sector_map.json"
    _sector_map_batch = {}
    if _sector_map_path.exists():
        try:
            _sector_map_batch = json.load(open(_sector_map_path, encoding="utf-8"))
        except Exception:
            pass
    _markets = getattr(config, "MARKETS", None)
    logger.info(f"  Market filter: {_markets or 'ALL'}")

    # ── R4 Stage 3 (2026-04-28): DB primary + CSV fallback ──
    #
    # Stage 1 (2026-04-23 ~ 2026-04-28) ran CSV as primary and DB as
    # shadow. Three consecutive business-day shadow comparisons
    # (04-24 / 04-27 / 04-28) all logged ``diff_pct=0.0%`` with sample
    # sizes 908 / 917 / 932, meeting the R4 protocol's transition
    # criterion. JUG approval (Jeff 2026-04-28) authorizes the swap to
    # DB primary as the default path.
    #
    # Design (J0=A / J1=D / J2=C / J3=B / J4=A / J5=C):
    #   * env QTRON_UNIVERSE_PRIMARY = DB | CSV (default DB).
    #     Operators flip via PowerShell to roll back without code change;
    #     batch reads env at every invocation (no tray restart needed).
    #   * Primary builder runs first. If empty result OR exception OR
    #     count below ``UNIV_MIN_COUNT // 2`` (clearly degraded), fall
    #     back to the other source. Both-source failure → empty-universe
    #     guard fires the existing batch-error path.
    #   * Shadow comparison still emits ``[UNIVERSE_SHADOW]`` for the
    #     non-primary source so the monitoring stream stays alive after
    #     the swap.
    #   * ``[UNIVERSE_FALLBACK_TRIGGERED]`` log + Telegram WARN on every
    #     fallback so post-hoc sparse-day analysis is feasible (Jeff
    #     2026-04-28 보완).
    #   * universe is ``sorted()`` at the end as a defensive guard so
    #     downstream G6-style idempotency does not depend on the
    #     builder's internal ordering (Jeff 2026-04-28 보완).
    import os as _os
    _PRIMARY = _os.environ.get("QTRON_UNIVERSE_PRIMARY", "DB").upper()
    if _PRIMARY not in ("DB", "CSV"):
        logger.warning(
            f"[UNIVERSE_PRIMARY_INVALID] {_PRIMARY!r} → falling back to DB default"
        )
        _PRIMARY = "DB"
    _SHADOW = "CSV" if _PRIMARY == "DB" else "DB"
    _FALLBACK_THRESHOLD = config.UNIV_MIN_COUNT // 2

    from data.universe_builder import build_universe_from_db, compare_universes
    from data.db_provider import DbProvider

    def _build_db():
        _db = DbProvider()
        return build_universe_from_db(
            _db,
            min_close=config.UNIV_MIN_CLOSE,
            min_amount=config.UNIV_MIN_AMOUNT,
            min_history=config.UNIV_MIN_HISTORY,
            min_count=config.UNIV_MIN_COUNT,
            allowed_markets=_markets,
            sector_map=_sector_map_batch,
        )

    def _build_csv():
        return build_universe_from_ohlcv(
            ohlcv_dir,
            min_close=config.UNIV_MIN_CLOSE,
            min_amount=config.UNIV_MIN_AMOUNT,
            min_history=config.UNIV_MIN_HISTORY,
            min_count=config.UNIV_MIN_COUNT,
            allowed_markets=_markets,
            sector_map=_sector_map_batch,
        )

    _builder = {"DB": _build_db, "CSV": _build_csv}

    universe = []
    used_source = _PRIMARY
    fallback_reason = None
    try:
        universe = _builder[_PRIMARY]()
    except Exception as _primary_err:
        fallback_reason = f"primary_exception: {type(_primary_err).__name__}: {_primary_err}"
        logger.warning(
            f"[UNIVERSE_PRIMARY_FAIL] primary={_PRIMARY} {_primary_err!r}"
        )
        universe = []

    if (not universe) or len(universe) < _FALLBACK_THRESHOLD:
        if fallback_reason is None:
            fallback_reason = (
                f"primary_empty_or_degraded: count={len(universe)} "
                f"< threshold={_FALLBACK_THRESHOLD}"
            )
        logger.warning(
            f"[UNIVERSE_FALLBACK_TRIGGERED] primary={_PRIMARY} "
            f"reason={fallback_reason} threshold={_FALLBACK_THRESHOLD}"
        )
        try:
            universe = _builder[_SHADOW]()
            used_source = _SHADOW
        except Exception as _fb_err:
            logger.error(
                f"[UNIVERSE_FALLBACK_FAIL] shadow={_SHADOW} {_fb_err!r}"
            )
            universe = []
        # Telegram WARN per K4=A: every fallback occurrence.
        try:
            _alert_data(
                f"⚠️ [KR] Universe fallback: {_PRIMARY}→{used_source} "
                f"reason={fallback_reason}"
            )
        except Exception:
            pass

    # Defensive sort for G6-style idempotency. Both builders already
    # return code-ascending lists today (build_universe_from_db: SQL
    # ``ORDER BY code``; build_universe_from_ohlcv: ``sorted(glob)``),
    # but a future refactor of either source must not silently break
    # determinism downstream.
    universe = sorted(universe)

    logger.info(
        f"[UNIVERSE_SOURCE] primary={_PRIMARY} used={used_source} "
        f"fallback={fallback_reason or 'none'} count={len(universe)}"
    )
    logger.info(f"  Universe: {len(universe)} stocks")
    if not universe:
        logger.error("Empty universe!")
        _notify_batch_error("Empty universe — batch 중단", logger)
        # R27 (gen3-v7 carry-over): persist last_batch_failed_at so the
        # next-day state-staleness guard fires correctly. Both DB and
        # CSV exhausted at this point, so the marker reflects an
        # honest both-source failure (not just a stage-1 csv miss).
        try:
            from core.state_manager import StateManager
            _sm = StateManager(config.STATE_DIR,
                               trading_mode=getattr(config, "TRADING_MODE", "live"))
            _rt = _sm._atomic_read(_sm._runtime_file) or {}
            _rt["last_batch_failed_at"] = __import__("datetime").datetime.utcnow().isoformat()
            _rt["last_batch_fail_reason"] = "empty_universe"
            _sm._atomic_write(_sm._runtime_file,
                              {"timestamp": __import__("datetime").datetime.now().isoformat(),
                               **_rt})
        except Exception as _e:
            logger.warning(f"[BATCH_FAIL_MARKER] {_e}")
        return None

    # Shadow comparison: emit [UNIVERSE_SHADOW] for the non-primary
    # source so the monitoring stream survives the Stage 3 swap.
    try:
        _shadow_universe = sorted(_builder[_SHADOW]())
        # compare_universes expects (csv_universe, db_universe) — pass
        # them in the canonical (csv, db) order regardless of which is
        # currently primary so the diff fields keep their historical
        # meaning.
        _csv_side = universe if used_source == "CSV" else _shadow_universe
        _db_side = universe if used_source == "DB" else _shadow_universe
        _diff = compare_universes(_csv_side, _db_side)
        logger.info(
            f"[UNIVERSE_SHADOW] primary={used_source} shadow={_SHADOW} "
            f"csv={_diff['csv_count']} db={_diff['db_count']} "
            f"only_csv={_diff['only_csv_count']} only_db={_diff['only_db_count']} "
            f"diff_pct={_diff['diff_pct']}%"
        )
        if _diff["only_csv_count"] > 0 or _diff["only_db_count"] > 0:
            logger.info(f"  only_csv_sample: {_diff['only_csv_sample']}")
            logger.info(f"  only_db_sample:  {_diff['only_db_sample']}")
    except Exception as _r4_err:
        # Shadow must never break batch
        logger.warning(f"[UNIVERSE_SHADOW_FAIL] {_r4_err!r} (non-critical)")

    # Step 3: Load OHLCV for scoring (DB only — CSV fallback 금지)
    logger.info("[3/5] Loading OHLCV...")
    close_dict = {}
    selected_source = "DB"
    from data.db_provider import DbProvider
    db = DbProvider()
    close_dict = db.load_close_dict(min_history=config.VOL_LOOKBACK)
    # Filter to universe
    close_dict = {k: v for k, v in close_dict.items() if k in universe}
    logger.info(f"  Loaded {len(close_dict)} stocks [DB]")
    # PG 실패 시 pg_base retry 3회 후 raise → batch 중단 (올바른 동작)

    # Step 4: Score and select
    logger.info("[4/5] Scoring and selecting...")
    target = build_target_portfolio(close_dict, config)

    # KR-P0-004: persist snapshot_version so downstream (lab_live, rebalance API)
    # can detect stale/duplicated batches by comparing the same key format.
    #   {trade_date}:{source}:{data_last_date}:{universe_count}:{matrix_hash}
    try:
        import hashlib as _hl
        _data_last_dates = [s.index.max() for s in close_dict.values()
                            if hasattr(s, 'index') and len(s) > 0]
        if _data_last_dates:
            _dl = max(_data_last_dates)
            _dl_str = _dl.strftime("%Y-%m-%d") if hasattr(_dl, 'strftime') else str(_dl)[:10]
        else:
            _dl_str = "?"
        # matrix_hash: deterministic fingerprint of loaded close-series (code → last10 values)
        _h = _hl.sha1()
        for _k in sorted(close_dict.keys()):
            _s = close_dict[_k]
            try:
                _tail = list(_s.tail(10).values)
                _h.update(f"{_k}:{_tail}".encode("utf-8"))
            except Exception:
                _h.update(f"{_k}:?".encode("utf-8"))
        _matrix_hash = _h.hexdigest()[:12]
        _snap_ver = (
            f"{target.get('date', '')}:{selected_source}:{_dl_str}"
            f":{len(close_dict)}:{_matrix_hash}"
        )
        target["snapshot_version"] = _snap_ver
        target["selected_source"] = selected_source
        target["data_last_date"] = _dl_str
        target["universe_count"] = len(close_dict)
        target["matrix_hash"] = _matrix_hash
        logger.info(f"[BATCH_SNAPSHOT_VERSION] {_snap_ver}")
        # P1-5: data freshness gate — warn if data_last_date lags trade_date
        try:
            from datetime import date as _date, timedelta as _td
            _tdate = target.get("date", "")
            if _tdate and _dl_str and _dl_str != "?":
                _td_dt = _date.fromisoformat(_tdate[:10])
                _dl_dt = _date.fromisoformat(_dl_str[:10])
                _lag_days = (_td_dt - _dl_dt).days
                if _lag_days > 4:
                    logger.critical(
                        f"[BATCH_DATA_STALE] market=KR data_last={_dl_dt} "
                        f"trade_date={_td_dt} lag={_lag_days}d > 4d — "
                        f"review OHLCV sync before next rebalance")
        except Exception as _e:
            logger.warning(f"[BATCH_DATA_STALE_CHECK_FAIL] {_e}")
    except Exception as _e:
        logger.warning(f"[BATCH_SNAPSHOT_VERSION_FAIL] {_e} — target saved without snapshot_version")

    path = save_target_portfolio(target, config.SIGNALS_DIR)
    logger.info(f"  Target: {len(target['target_tickers'])} stocks -> {path}")

    # DB 저장 (PostgreSQL) — AUDIT ONLY: rebalance는 JSON만 읽음
    # canonical = signals/target_portfolio_{date}.json
    # PG target_portfolio 테이블은 이력 조회/감사용으로만 사용
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        db.save_target_portfolio(target)
        logger.info(f"  Target saved to DB (audit)")
    except Exception as e:
        logger.warning(f"  DB audit save failed: {e} (non-critical)")
    for i, tk in enumerate(target["target_tickers"], 1):
        s = target["scores"].get(tk, {})
        logger.info(f"    {i:2d}. {tk}  vol={s.get('vol_12m',0):.4f}  mom={s.get('mom_12_1',0):.4f}")

    # Step 5 (fast): Fundamental snapshot (lightweight, Lab 9전략 필수)
    if fast:
        logger.info("[5/5] Fundamental snapshot (fast, for Lab strategies)...")
        try:
            fund_dir = config.OHLCV_DIR.parent / "fundamental"
            fund_dir.mkdir(parents=True, exist_ok=True)
            fund_date = target.get("date", datetime.now().strftime("%Y%m%d"))
            fund_path = fund_dir / f"fundamental_{fund_date}.csv"

            # R26 (2026-04-24): CSV 존재 ≠ DB 갱신. CSV/DB 판정 분리.
            # 기존: CSV exists → 전체 skip (DB 도 upsert 되지 않음, 오늘 3일 stale 원인)
            # 수정: CSV/DB 독립 판정 — CSV 없으면 fetch, DB stale 이면 CSV 에서 읽어 upsert.
            fund_df = _ensure_fundamental_csv(fund_path, fund_date, logger)
            if fund_df is not None:
                _ensure_fundamental_db(fund_date, fund_df, logger)
            else:
                logger.warning("  Fundamental: no DataFrame available (fetch/CSV both missing)")
                _alert_data("fundamental", "CSV absent and fetch returned None",
                            {"expected_date": fund_date})
        except Exception as e:
            logger.warning(f"  Fundamental failed: {e} (Lab uses latest available)")
            _alert_data("fundamental", f"fetch exception: {e}",
                        {"expected_date": fund_date})

        # Lab Live daily run (9전략 forward paper trading)
        try:
            _run_lab_live_daily(config, logger)
        except Exception as e:
            logger.warning(f"  Lab Live failed: {e} (non-critical)")

        # Advisor daily analysis + Telegram
        try:
            _run_advisor(config, logger)
        except Exception as e:
            logger.warning(f"  Advisor failed: {e} (non-critical)")

        # Gate freshness signal: runtime에 batch 완료 timestamp 기록 (BATCH_MISSING 해소)
        try:
            from core.state_manager import StateManager
            sm = StateManager(config.STATE_DIR, trading_mode=getattr(config, "TRADING_MODE", "live"))
            sm.save_batch_completion(
                business_date=target.get("date", ""),
                snapshot_version=target.get("snapshot_version", ""),
            )
            logger.info(f"[BATCH_RUNTIME_UPDATED] business_date={target.get('date','')} mode=fast")
        except Exception as _e:
            logger.warning(f"[BATCH_RUNTIME_SAVE_FAIL] {_e} (non-critical, gate may show BATCH_MISSING)")

        logger.info("Batch complete (fast).")
        _notify_batch_result(target, logger, mode="fast")
        return target
    logger.info("[5/7] Generating Top20 MA report...")
    try:
        from report.top20_report import generate_top20_report
        html_path = generate_top20_report(target, ohlcv_dir, config.REPORT_DIR)
        if html_path:
            logger.info(f"  Report: {html_path}")
    except Exception as e:
        logger.warning(f"  Report generation failed: {e} (non-critical)")

    # Step 6: Collect daily fundamental snapshot (for backtest DB + Valuation report)
    # R26 (2026-04-24): CSV/DB 독립 판정 — CSV 없으면 fetch, DB stale 이면 CSV 재활용 upsert.
    # 기존: CSV exists → 전체 skip (DB upsert 아예 없음, step 7 Valuation 에만 사용)
    logger.info("[6/7] Collecting fundamental snapshot...")
    try:
        fund_dir = config.OHLCV_DIR.parent / "fundamental"
        fund_dir.mkdir(parents=True, exist_ok=True)
        fund_date = target.get("date", datetime.now().strftime("%Y%m%d"))
        fund_path = fund_dir / f"fundamental_{fund_date}.csv"

        fund_df = _ensure_fundamental_csv(fund_path, fund_date, logger)
        if fund_df is not None:
            _ensure_fundamental_db(fund_date, fund_df, logger)
    except Exception as e:
        logger.warning(f"  Fundamental collection failed: {e} (non-critical)")

    # Step 7: Generate Valuation Top20 report (reuses Step 6 CSV)
    logger.info("[7/7] Generating Valuation Top20 report...")
    try:
        from report.top20_valuation import generate_top20_valuation_report
        # Load sector map for sector PER comparison
        sector_map_dict = {}
        if config.SECTOR_MAP.exists():
            import json as _json
            sector_map_dict = _json.loads(config.SECTOR_MAP.read_text(encoding="utf-8"))

        val_date = target.get("date", datetime.now().strftime("%Y%m%d"))
        val_path = generate_top20_valuation_report(
            ohlcv_dir=ohlcv_dir,
            output_dir=config.REPORT_DIR,
            universe=list(close_dict.keys()),  # full universe, not just top20
            sector_map=sector_map_dict,
            report_date=val_date,
        )
        if val_path:
            logger.info(f"  Valuation Report: {val_path}")
    except Exception as e:
        logger.warning(f"  Valuation report failed: {e} (non-critical)")

    # Step 8: Lab Live daily run (9전략 forward paper trading)
    logger.info("[8/9] Lab Live daily run...")
    try:
        _run_lab_live_daily(config, logger)
    except Exception as e:
        logger.warning(f"  Lab Live failed: {e} (non-critical)")

    # Step 9: Advisor daily analysis + Telegram
    logger.info("[9/9] Advisor daily analysis...")
    try:
        _run_advisor(config, logger)
    except Exception as e:
        logger.warning(f"  Advisor failed: {e} (non-critical)")

    # Note: AUTO GATE advisory observation (gate_observer.run_today) is triggered
    # by kr/tray_server.py post-EOD to keep a single-producer contract.
    # Do not run it here — the tray_server is the sole producer.

    # Gate freshness signal: runtime에 batch 완료 timestamp 기록 (BATCH_MISSING 해소)
    try:
        from core.state_manager import StateManager
        sm = StateManager(config.STATE_DIR, trading_mode=getattr(config, "TRADING_MODE", "live"))
        sm.save_batch_completion(
            business_date=target.get("date", ""),
            snapshot_version=target.get("snapshot_version", ""),
        )
        logger.info(f"[BATCH_RUNTIME_UPDATED] business_date={target.get('date','')} mode=full")
    except Exception as _e:
        logger.warning(f"[BATCH_RUNTIME_SAVE_FAIL] {_e} (non-critical, gate may show BATCH_MISSING)")

    logger.info("Batch complete.")
    _notify_batch_result(target, logger, mode="full")
    return target


def _fetch_kospi_pykrx(from_date: str, to_date: str):
    """
    pykrx 로 KOSPI 지수(1001) fetch.

    Returns:
        (DataFrame|None, error_str|None)
        - DataFrame: 표준 스키마 (date/open/high/low/close/volume)
        - error_str: 예외 발생 시 메시지, 정상이면 None
    """
    import pandas as pd
    try:
        from pykrx import stock as _pykrx_stock
        _from = from_date.replace("-", "")
        _to = to_date.replace("-", "")
        raw = _pykrx_stock.get_index_ohlcv_by_date(_from, _to, "1001")
        if raw is None or raw.empty:
            return (None, None)  # empty, not error
        raw = raw.reset_index()
        _col_map = {"날짜": "date", "시가": "open", "고가": "high",
                    "저가": "low", "종가": "close", "거래량": "volume"}
        raw = raw.rename(columns={k: v for k, v in _col_map.items() if k in raw.columns})
        if "date" not in raw.columns and raw.columns[0].lower() in ("date", "index"):
            raw = raw.rename(columns={raw.columns[0]: "date"})
        raw["date"] = pd.to_datetime(raw["date"])
        for c in ["open", "high", "low", "close", "volume"]:
            if c not in raw.columns:
                raw[c] = 0
        return (raw[["date", "open", "high", "low", "close", "volume"]].copy(), None)
    except Exception as e:
        return (None, str(e))


def _fetch_kospi_yfinance(from_date: str, to_date: str, max_attempts: int = 3):
    """
    yfinance 로 ^KS11 fetch (재시도 포함).

    Returns:
        (DataFrame|None, error_str|None, attempts)
    """
    import pandas as pd
    import time as _t
    last_err = None
    for attempt in range(max_attempts):
        try:
            import yfinance as yf
            raw = yf.download("^KS11", start=from_date, end=to_date,
                              auto_adjust=True, progress=False)
            if raw.empty:
                return (None, None, attempt + 1)
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            raw = raw.reset_index()
            raw = raw.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                      "Low": "low", "Close": "close", "Volume": "volume"})
            raw["date"] = pd.to_datetime(raw["date"])
            return (raw[["date", "open", "high", "low", "close", "volume"]].copy(),
                    None, attempt + 1)
        except Exception as e:
            last_err = str(e)
            if attempt < max_attempts - 1:
                _t.sleep(5)
    return (None, last_err, max_attempts)


def _fetch_kospi_db(last_known_date):
    """
    DB 에서 이미 저장된 KOSPI 확인 (다른 프로세스가 업데이트했을 수 있음).

    Returns:
        (DataFrame|None, error_str|None)
    """
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        db_idx = db.get_kospi_index()
        if len(db_idx) == 0:
            return (None, None)
        if last_known_date is None:
            return (db_idx[["date", "open", "high", "low", "close", "volume"]].copy(), None)
        fresh = db_idx[db_idx["date"] > last_known_date].copy()
        if fresh.empty:
            return (None, None)
        return (fresh[["date", "open", "high", "low", "close", "volume"]], None)
    except Exception as e:
        return (None, str(e))


def _classify_kospi_supply(df, expected_today: str, error: str = None) -> str:
    """
    소스 결과를 SupplyStatus 로 분류.

    Returns:
        "SUCCESS_TODAY" | "SUCCESS_STALE" | "EMPTY_NOT_READY" | "ERROR" | "TIMEOUT"
    """
    if error:
        return "TIMEOUT" if "timeout" in error.lower() else "ERROR"
    if df is None or len(df) == 0:
        return "EMPTY_NOT_READY"
    try:
        import pandas as pd
        last = str(pd.to_datetime(df["date"].max()).date())
        return "SUCCESS_TODAY" if last >= expected_today else "SUCCESS_STALE"
    except Exception:
        return "ERROR"


# SupplyStatus 우선순위 (높을수록 선호)
_SUPPLY_STATUS_RANK = {
    "SUCCESS_TODAY":    5,
    "SUCCESS_STALE":    3,
    "EMPTY_NOT_READY":  2,
    "TIMEOUT":          1,
    "ERROR":            0,
}


def _update_kospi_index(config, logger):
    """KOSPI index 파일 + DB 업데이트 (SupplyStatus 기반 best-available selection).

    Step 2 재작성 (2026-04-17):
    - 각 소스(pykrx/yfinance/db)를 **독립적으로 시도**하고 결과를 SupplyStatus 로 분류
    - "누가 1순위"가 아니라 "현재 시점에 누가 SUCCESS_TODAY 인가" 기준 선택
    - pykrx 는 당일 KOSPI 지수를 자주 empty 로 주므로 primary 취급 금지 (2026-04-17 확인)
    - yfinance 가 오늘 기준 primary — 종목 OHLCV 와 KOSPI 지수는 다른 소스 특성
    - 결과는 DataEvent + market_context.supply_status 로 propagate

    Flow:
        1. 각 소스 시도 → (df, status)
        2. best-available 선택 (SUCCESS_TODAY > SUCCESS_STALE > ...)
        3. 선택된 df 병합 (SUCCESS_TODAY 와 SUCCESS_STALE 은 union 이 나을 수도 — 아래 로직)
        4. CSV/DB 업데이트
        5. emit_event 로 각 소스 + pipeline 결과 기록
    """
    import pandas as pd
    from datetime import datetime, timedelta

    # ── 1. 기준 날짜 결정 ──
    index_file = config.INDEX_FILE
    try:
        existing = pd.read_csv(index_file, parse_dates=["index"])
        existing = existing.rename(columns={"index": "date"})
        existing["date"] = pd.to_datetime(existing["date"])
        last_date = existing["date"].max()
    except Exception:
        last_date = None

    today = datetime.now()
    if today.hour < 16:
        today -= timedelta(days=1)
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")

    # DB is the canonical truth. CSV-alone "up-to-date" check is NOT sufficient
    # (incident 2026-04-24: inject_kospi_close appended a degraded row to CSV
    # first, batch then saw CSV last_date=today and skipped DB upsert entirely,
    # leaving DB stale and chart broken). Cross-check DB last_date below.
    #
    # TODO (P1): migrate to `write_kospi_index_dual_sink(date, row)` single
    # writer so CSV and DB cannot diverge by construction.
    if last_date is not None and str(last_date.date()) >= today_str:
        _db_last = None
        _db_today_close = None
        _csv_today_close = None
        try:
            from data.db_provider import DbProvider
            _db = DbProvider()
            _db_idx = _db.get_kospi_index()
            if _db_idx is not None and len(_db_idx) > 0:
                _db_last = str(pd.to_datetime(_db_idx["date"].max()).date())
                _today_db_rows = _db_idx[
                    _db_idx["date"].astype(str).str.startswith(today_str)
                ]
                if not _today_db_rows.empty:
                    _db_today_close = float(_today_db_rows.iloc[-1]["close"])
        except Exception as _e:
            logger.warning(f"  KOSPI DB last_date lookup failed: {_e}")

        try:
            _today_csv_rows = existing[existing["date"].astype(str).str.startswith(today_str)]
            if not _today_csv_rows.empty:
                # CSV column case could be 'Close' or 'close'
                _row = _today_csv_rows.iloc[-1]
                _csv_today_close = float(_row.get("Close", _row.get("close", 0)))
        except Exception:
            pass

        if _db_last is None or _db_last < today_str:
            # CSV has today but DB is missing/stale → force upsert CSV today row
            logger.warning(
                f"  [KOSPI_SYNC_DIVERGE] CSV has {today_str} but DB last={_db_last} — "
                f"forcing DB upsert from CSV (DB=truth principle)"
            )
            try:
                _today_rows = existing[existing["date"].astype(str).str.startswith(today_str)]
                # Normalize columns to lower-case schema expected by upsert_kospi_index
                _rename = {c: c.lower() for c in _today_rows.columns}
                _push = _today_rows.rename(columns=_rename).copy()
                for _c in ("open", "high", "low", "close", "volume"):
                    if _c not in _push.columns:
                        _push[_c] = _push.get("close", 0)
                from data.db_provider import DbProvider
                _n = DbProvider().upsert_kospi_index(
                    _push[["date", "open", "high", "low", "close", "volume"]]
                )
                logger.info(f"  [KOSPI_SYNC_RECOVER] DB upserted from CSV: {_n} row(s)")
            except Exception as _e:
                logger.warning(f"  [KOSPI_SYNC_RECOVER_FAIL] {_e}")
                _alert_data("kospi_index_db",
                            f"forced CSV→DB upsert failed: {_e}",
                            {"today": today_str, "csv_last": str(last_date.date())})
            return

        if (
            _db_today_close is not None
            and _csv_today_close is not None
            and abs(_db_today_close - _csv_today_close) > 0.01
        ):
            logger.warning(
                f"  [KOSPI_SYNC_DIVERGE] CSV close={_csv_today_close} vs "
                f"DB close={_db_today_close} for {today_str} — values differ "
                f"(DB=truth; CSV may be stale injection). Leaving both as-is; "
                f"investigate via incident log."
            )
            _alert_data("kospi_index_value_diverge",
                        f"CSV={_csv_today_close} DB={_db_today_close}",
                        {"today": today_str})

        logger.info(f"  KOSPI index up-to-date ({today_str}, CSV+DB)")
        return

    from_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d") if last_date else "2019-01-01"
    to_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    # ── 2. 모든 소스 독립 시도 + SupplyStatus 분류 ──
    # (소스 호출 비용 낮은 순서: pykrx → DB → yfinance. 단 선호도는 status 로 결정)
    sources: dict = {}   # {source_name: {"df": ..., "status": ..., "error": ...}}

    # pykrx
    pykrx_df, pykrx_err = _fetch_kospi_pykrx(from_date, to_date)
    pykrx_status = _classify_kospi_supply(pykrx_df, today_str, pykrx_err)
    sources["pykrx"] = {"df": pykrx_df, "status": pykrx_status, "error": pykrx_err}
    logger.info(f"  [KOSPI.pykrx] status={pykrx_status}, rows={0 if pykrx_df is None else len(pykrx_df)}")
    try:
        from web.data_events import emit_event, Level
        level = Level.INFO if pykrx_status == "SUCCESS_TODAY" else (
            Level.WARN if pykrx_status in ("ERROR", "EMPTY_NOT_READY", "TIMEOUT") else Level.INFO)
        emit_event(
            source="KOSPI.pykrx",
            level=level,
            code=pykrx_status.lower(),
            message=f"pykrx status={pykrx_status}" + (f" err={pykrx_err}" if pykrx_err else ""),
            details={"status": pykrx_status, "rows": 0 if pykrx_df is None else len(pykrx_df),
                     "error": pykrx_err},
            telegram=False,
        )
    except Exception:
        pass

    # DB (기존 저장분 — 다른 프로세스 업데이트 가능성 + 백업)
    db_df, db_err = _fetch_kospi_db(last_date)
    db_status = _classify_kospi_supply(db_df, today_str, db_err)
    sources["db"] = {"df": db_df, "status": db_status, "error": db_err}
    logger.info(f"  [KOSPI.db] status={db_status}, rows={0 if db_df is None else len(db_df)}")

    # yfinance — pykrx 가 이미 SUCCESS_TODAY 면 skip 가능 (네트워크 절약)
    # 단 오늘 (2026-04-17) 경험상 pykrx 는 지수 1001 을 자주 empty 로 주므로 기본적으로 yfinance 도 시도
    yf_skip = (pykrx_status == "SUCCESS_TODAY" and db_status != "ERROR")
    if yf_skip:
        logger.info("  [KOSPI.yfinance] skip (pykrx SUCCESS_TODAY)")
        yf_df, yf_err, yf_attempts = None, None, 0
        yf_status = "EMPTY_NOT_READY"  # 시도 안 했으므로
    else:
        yf_df, yf_err, yf_attempts = _fetch_kospi_yfinance(from_date, to_date)
        yf_status = _classify_kospi_supply(yf_df, today_str, yf_err)
        logger.info(
            f"  [KOSPI.yfinance] status={yf_status}, "
            f"rows={0 if yf_df is None else len(yf_df)}, attempts={yf_attempts}"
        )
        try:
            from web.data_events import emit_event, Level
            level = Level.INFO if yf_status == "SUCCESS_TODAY" else (
                Level.WARN if yf_status in ("ERROR", "EMPTY_NOT_READY", "TIMEOUT") else Level.INFO)
            emit_event(
                source="KOSPI.yfinance",
                level=level,
                code=yf_status.lower(),
                message=f"yfinance status={yf_status}" + (f" err={yf_err}" if yf_err else ""),
                details={"status": yf_status, "rows": 0 if yf_df is None else len(yf_df),
                         "attempts": yf_attempts, "error": yf_err},
                telegram=False,
            )
        except Exception:
            pass
    sources["yfinance"] = {"df": yf_df, "status": yf_status, "error": yf_err}

    # ── 3. Best-available selection ──
    # 우선순위: SUCCESS_TODAY > SUCCESS_STALE > EMPTY_NOT_READY > TIMEOUT > ERROR
    # 여러 소스가 SUCCESS_TODAY 면 모두 union (서로 다른 날짜 데이터 보완)
    ranked = sorted(
        sources.items(),
        key=lambda kv: _SUPPLY_STATUS_RANK.get(kv[1]["status"], 0),
        reverse=True,
    )
    best_name, best_info = ranked[0]
    best_status = best_info["status"]
    logger.info(f"  [KOSPI.pipeline] best={best_name} status={best_status}")

    if best_status in ("ERROR", "TIMEOUT") or best_info["df"] is None:
        # 모든 소스 실패 — CRITICAL
        errs = {n: s.get("error") for n, s in sources.items()}
        logger.warning(f"  KOSPI index: all sources failed. errors={errs}")
        _alert_data("KOSPI_index", "all sources failed",
                    {"last_date": str(last_date), "today": today_str, "errors": errs})
        try:
            from web.data_events import emit_event, Level
            emit_event(
                source="KOSPI.pipeline",
                level=Level.CRITICAL,
                code="all_sources_failed",
                message=f"KOSPI index 확보 실패. last={last_date}, today={today_str}",
                details={"last_date": str(last_date), "today": today_str,
                         "sources": {n: s["status"] for n, s in sources.items()}},
                telegram=True,  # CRITICAL 은 Telegram + DEBUG 힌트 자동 삽입
            )
        except Exception:
            pass
        return

    # 성공 — union (best 기준 + SUCCESS_TODAY/STALE 인 다른 소스 보충)
    new_df = best_info["df"].copy()
    for other_name, other_info in ranked[1:]:
        if other_info["status"] in ("SUCCESS_TODAY", "SUCCESS_STALE") and other_info["df"] is not None:
            missing = other_info["df"][~other_info["df"]["date"].isin(new_df["date"])]
            if not missing.empty:
                new_df = pd.concat([new_df, missing]).sort_values("date").reset_index(drop=True)
                logger.info(f"  [KOSPI.pipeline] supplement +{len(missing)} rows from {other_name}")

    # ── 4. CSV 업데이트 ──
    try:
        existing_df = pd.read_csv(index_file)
        date_col = "index" if "index" in existing_df.columns else "date"
        existing_df = existing_df.rename(columns={date_col: "date"})
        existing_df["date"] = pd.to_datetime(existing_df["date"])
        combined = pd.concat([existing_df, new_df]).drop_duplicates("date").sort_values("date")
        combined = combined.rename(columns={"date": "index", "open": "Open", "high": "High",
                                            "low": "Low", "close": "Close", "volume": "Volume"})
        combined.to_csv(index_file, index=False)
        logger.info(f"  KOSPI.csv updated: {len(combined)} rows total")
    except Exception as e:
        logger.warning(f"  KOSPI.csv update failed: {e}")

    # ── 5. DB 업데이트 ──
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        upserted = db.upsert_kospi_index(new_df)
        logger.info(f"  kospi_index DB upserted: {upserted} rows")
    except Exception as e:
        logger.warning(f"  kospi_index DB upsert failed: {e} (non-critical)")
        _alert_data("kospi_index_db", str(e))

    # ── 6. 파이프라인 성공 이벤트 (Recovery signal — 이전 WARN state reset) ──
    try:
        from web.data_events import emit_event, Level
        emit_event(
            source="KOSPI.pipeline",
            level=Level.INFO,
            code="all_sources_failed",   # 같은 code 로 recovery → NORMAL 복귀
            message=f"KOSPI pipeline OK ({best_name}/{best_status}, {len(new_df)} new rows)",
            details={"best_source": best_name, "best_status": best_status,
                     "new_rows": len(new_df),
                     "sources": {n: s["status"] for n, s in sources.items()}},
            telegram=False,
        )
    except Exception:
        pass


def _run_lab_live_daily(config, logger):
    """Lab Live 9전략 forward paper trading daily run."""
    try:
        from web.lab_live.engine import LabLiveSimulator
        sim = LabLiveSimulator()
        sim.initialize()
        result = sim.run_daily()
        if result.get("ok"):
            logger.info(f"  Lab Live: {result['date']}, {result['trades']} trades, "
                        f"{result['elapsed']:.1f}s")
        elif result.get("skipped"):
            logger.info(f"  Lab Live: already ran for {result['date']}")
        else:
            logger.warning(f"  Lab Live: {result}")
    except Exception as e:
        logger.warning(f"  Lab Live error: {e}")


def _run_advisor(config, logger):
    """Advisor 일일 분석 + 텔레그램 알림."""
    try:
        from advisor.runner import run_analysis
        from notify.helpers import _notify_advisor
        from datetime import datetime

        today = datetime.now().strftime("%Y%m%d")
        mode = getattr(config, "TRADING_MODE", "live")

        result = run_analysis(today, mode)
        status = result.get("status", "UNKNOWN")

        # Summary log
        alerts = result.get("alerts", [])
        recs = result.get("recommendations", [])
        n_high = sum(1 for a in alerts if a.get("priority") == "HIGH")
        logger.info(f"  Advisor: {status}, {len(alerts)} alerts ({n_high} HIGH), "
                     f"{len(recs)} recommendations, {result.get('elapsed_sec', 0):.1f}s")

        # Telegram
        _notify_advisor(alerts, recs)

    except Exception as e:
        logger.warning(f"  Advisor error: {e}")


def _notify_batch_result(target: dict, logger, mode: str = "full") -> None:
    """Batch 완료 텔레그램 알림."""
    try:
        from notify.telegram_bot import send
        tickers = target.get("target_tickers", [])
        date = target.get("date", "?")
        send(
            f"✅ <b>KR Batch Complete</b> ({mode})\n"
            f"Date: {date}\n"
            f"Target: {len(tickers)}종목",
            severity="INFO",
        )
    except Exception:
        pass


def _notify_batch_error(reason: str, logger) -> None:
    """Batch 에러 텔레그램 알림."""
    try:
        from notify.telegram_bot import send
        from datetime import datetime as _dt
        send(
            f"🚨 <b>KR Batch Error</b>\n"
            f"시간: {_dt.now().strftime('%H:%M:%S')}\n"
            f"사유: {reason}",
            severity="CRITICAL",
        )
    except Exception:
        pass
