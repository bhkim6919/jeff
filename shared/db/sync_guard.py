# -*- coding: utf-8 -*-
"""shared/db/sync_guard.py — R26 (C) CSV/DB independence helper (2026-04-24).

Background
----------
2026-04-23 오후: KR batch Step 6 fundamental 이 CSV 존재 → DB upsert skip 하는
구조로 인해 DB 에 3일 stale 이 방치됐다. 같은 패턴이 Step 5 / Step 1 에도
있었고 (R26 A 에서 수정), 향후 새 writer 가 추가될 때 동일 사고가
재발할 가능성이 크다.

Solution
--------
쓰는 쪽에서 CSV/DB 를 독립 판정하도록 강제하는 얇은 helper. 호출자는
"CSV 를 어떻게 얻고" "DB 가 신선한지" "DB 를 어떻게 upsert 하는지" 세
가지 람다만 제공하면, guard 는 다음 순서로 실행한다:

    1) CSV 가 이미 있으면 재사용, 없으면 fetcher() 호출해 새로 만든다.
    2) DB 가 이미 신선하면 upsert 를 skip.
    3) 둘 중 하나라도 부족하면 부족한 것만 메꾸고, 그 외는 건드리지 않는다.

이 모듈은 Q-TRON 의 특정 테이블이나 스키마를 몰라도 된다. 각 writer 가
도메인 지식을 자기 람다에 가둬서 전달한다.

Usage (kr/lifecycle/batch.py Step 5 예시)
------------------------------------------
    from shared.db.sync_guard import db_sync_guard, SyncReport
    import pandas as pd
    from data.db_provider import DbProvider

    def _fetch_fund():
        return _fetch_daily_snapshot_with_timeout(logger)

    db = DbProvider()
    report = db_sync_guard(
        resource="fundamental",
        identity=fund_date,
        csv_path=fund_path,
        fetcher=_fetch_fund,
        csv_writer=lambda df, p: df.to_csv(p, index=False),
        csv_reader=lambda p: pd.read_csv(p),
        db_fresh=lambda: db.has_fundamental_for(fund_date),
        db_writer=lambda df: db.upsert_fundamental(fund_date, df),
        logger=logger,
    )
    if report.failed:
        _alert_data("fundamental", report.reason, {"fund_date": fund_date})

Notes
-----
* 이 모듈은 Q-TRON 내부 다른 모듈을 import 하지 않는다 (standalone).
* fetcher 가 None 을 반환하면 DB upsert 도 시도하지 않는다 (데이터 없음).
* 모든 예외는 잡아 SyncReport.failed=True 로 보고 — 호출자가 알림 결정.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

_log = logging.getLogger("qtron.db_sync_guard")

# Type aliases for clarity
Fetcher = Callable[[], Any]                 # returns DataFrame | None
CsvWriter = Callable[[Any, Path], None]
CsvReader = Callable[[Path], Any]
DbFreshCheck = Callable[[], bool]
DbWriter = Callable[[Any], int]             # returns rows affected


@dataclass
class SyncReport:
    resource: str
    identity: str
    csv_source: str   # "reused" | "fetched" | "missing"
    db_action: str    # "fresh_skip" | "upserted" | "skipped_no_data" | "error"
    csv_path: Optional[Path] = None
    rows: int = 0
    failed: bool = False
    reason: Optional[str] = None


def db_sync_guard(
    *,
    resource: str,
    identity: str,
    csv_path: Path,
    fetcher: Fetcher,
    csv_writer: CsvWriter,
    csv_reader: CsvReader,
    db_fresh: DbFreshCheck,
    db_writer: DbWriter,
    logger: Optional[logging.Logger] = None,
) -> SyncReport:
    """Execute the CSV/DB independence pattern for one resource.

    Parameters
    ----------
    resource : str
        Human-readable label used for logging, e.g. "fundamental".
    identity : str
        The identifying key for the batch — usually a YYYYMMDD date string.
        Used in logs and the returned report.
    csv_path : Path
        Where the CSV cache lives. Reused if present, otherwise fetched.
    fetcher : callable → DataFrame | None
        Called only when `csv_path` is missing OR unreadable. Return None
        to signal "fetch failed / timed out"; guard will then *skip the
        DB write* (never uploads an empty / fabricated DataFrame).
    csv_writer, csv_reader : callables
        CSV serialization hooks. Typically `df.to_csv` and `pd.read_csv`.
    db_fresh : callable → bool
        Returns True if DB already contains data for `identity`. When
        True, guard does not call `db_writer`.
    db_writer : callable(df) → int
        Performs the upsert. Must be idempotent; the guard only calls it
        when `db_fresh()` is False.
    logger : logging.Logger, optional
        Caller's logger — inherits format. Defaults to module logger.

    Returns
    -------
    SyncReport describing what happened. Never raises.
    """
    lg = logger or _log
    report = SyncReport(
        resource=resource, identity=identity, csv_path=csv_path,
        csv_source="missing", db_action="skipped_no_data",
    )

    # ---- Step 1: ensure we have a DataFrame (CSV-first, fetch fallback) ----
    df = None
    if csv_path.exists():
        try:
            df = csv_reader(csv_path)
            report.csv_source = "reused"
            lg.info(f"  [{resource}] CSV reused: {csv_path}")
        except Exception as e:
            lg.warning(f"  [{resource}] CSV read failed: {e} → refetching")

    if df is None:
        try:
            df = fetcher()
        except Exception as e:
            lg.warning(f"  [{resource}] fetcher crashed: {e}")
            report.failed = True
            report.reason = f"fetcher_crash: {e}"
            report.db_action = "error"
            return report

        if df is None:
            lg.warning(f"  [{resource}] fetcher returned None (missing data)")
            report.reason = "fetcher_returned_none"
            # Not a guard failure — might be an expected "no data today"
            # situation. Caller decides via report.csv_source == "missing".
            return report

        try:
            csv_writer(df, csv_path)
            report.csv_source = "fetched"
            lg.info(f"  [{resource}] CSV fetched + saved: {csv_path}")
        except Exception as e:
            lg.warning(
                f"  [{resource}] CSV write failed: {e} (continuing to DB step)"
            )
            report.csv_source = "fetched"
            report.reason = f"csv_write_soft_fail: {e}"

    # ---- Step 2: DB freshness check (independent of CSV outcome) ----
    try:
        fresh = bool(db_fresh())
    except Exception as e:
        lg.warning(f"  [{resource}] db_fresh check crashed: {e}")
        report.failed = True
        report.reason = f"db_fresh_crash: {e}"
        report.db_action = "error"
        return report

    if fresh:
        lg.info(f"  [{resource}] DB already fresh for {identity} — skip upsert")
        report.db_action = "fresh_skip"
        return report

    # ---- Step 3: DB upsert ----
    try:
        n = int(db_writer(df) or 0)
        report.rows = n
        report.db_action = "upserted"
        lg.info(f"  [{resource}] DB upsert: {n} rows (identity={identity})")
    except Exception as e:
        lg.warning(f"  [{resource}] DB upsert failed: {e}")
        report.failed = True
        report.reason = f"db_writer_error: {e}"
        report.db_action = "error"

    return report
