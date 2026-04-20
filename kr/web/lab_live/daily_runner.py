"""
daily_runner.py -- EOD Auto-Run + OHLCV Update
================================================
장 마감 후 pykrx로 데이터 업데이트 → 9전략 가상 체결.
"""
from __future__ import annotations
import logging
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger("lab_live.daily")


def update_ohlcv(ohlcv_dir: Path, days_back: int = 3) -> str:
    """pykrx로 최근 OHLCV 데이터 업데이트. 최신 날짜 반환."""
    try:
        from pykrx import stock
    except ImportError:
        logger.warning("[LAB_LIVE] pykrx not available, skipping OHLCV update")
        return ""

    # Determine date range
    today = datetime.now().strftime("%Y%m%d")
    from_date = (datetime.now() - pd.Timedelta(days=days_back + 2)).strftime("%Y%m%d")

    files = sorted(ohlcv_dir.glob("*.csv"))
    updated = 0
    latest_date = ""

    for f in files:
        code = f.stem
        if len(code) != 6 or not code.isdigit():
            continue
        try:
            df = pd.read_csv(f, parse_dates=["date"])
            last_date = df["date"].max()

            # Fetch new data
            new = stock.get_market_ohlcv(from_date, today, code)
            if new.empty:
                continue

            new_rows = []
            for dt, row in new.iterrows():
                if dt > last_date:
                    new_rows.append({
                        "date": dt,
                        "open": int(row.iloc[0]),
                        "high": int(row.iloc[1]),
                        "low": int(row.iloc[2]),
                        "close": int(row.iloc[3]),
                        "volume": int(row.iloc[4]),
                    })

            if new_rows:
                new_df = pd.DataFrame(new_rows)
                new_df["date"] = pd.to_datetime(new_df["date"])
                combined = pd.concat([df, new_df]).drop_duplicates(
                    subset="date").sort_values("date").reset_index(drop=True)
                combined.to_csv(f, index=False)
                updated += 1

                # DB upsert: CSV와 동일 데이터를 DB에도 저장
                # raw ingestion = CSV, serving = DB, 양쪽 sync 유지
                try:
                    _db_upsert_batch.append((code, combined.tail(10)))
                except NameError:
                    pass  # _db_upsert_batch not initialized (shouldn't happen)

                ld = str(combined["date"].max().date())
                if ld > latest_date:
                    latest_date = ld

            if updated % 100 == 0 and updated > 0:
                time.sleep(0.3)

        except Exception:
            continue

    # ── DB Sync: CSV → DB (idempotent upsert) ──
    # 역할: incremental EOD sync (batch.py = bulk sync, 여기 = EOD 직후)
    # 중복 upsert는 ON CONFLICT DO UPDATE로 idempotent
    db_synced = 0
    db_failed = 0
    csv_total = 0
    try:
        from data.db_provider import DbProvider
        db = DbProvider()
        for f in files:
            code = f.stem
            if len(code) != 6 or not code.isdigit():
                continue
            csv_total += 1
            try:
                _df = pd.read_csv(f, parse_dates=["date"])
                _tail = _df.tail(10)
                if not _tail.empty:
                    db.upsert_ohlcv(code, _tail)
                    db_synced += 1
            except Exception:
                db_failed += 1
                continue
        csv_last = latest_date
        # DB last date 확인
        db_last = "?"
        try:
            conn = db._conn()
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM ohlcv")
            row = cur.fetchone()
            cur.close()
            conn.close()
            db_last = str(row[0]) if row and row[0] else "?"
        except Exception:
            pass

        # 상태 판정: OK=full sync, PARTIAL=일부 실패 또는 날짜 불일치, FAIL=전체 실패
        completeness = round(db_synced / csv_total, 4) if csv_total > 0 else 0
        if db_failed == 0 and csv_last and db_last >= csv_last:
            sync_status = "OK"
        elif db_synced > 0:
            sync_status = "PARTIAL"
        else:
            sync_status = "FAIL"

        # failed_ratio > 10% → PARTIAL을 실질 FAIL로 승격
        if sync_status == "PARTIAL" and csv_total > 0 and (db_failed / csv_total) > 0.10:
            sync_status = "FAIL"
            logger.warning(
                f"[OHLCV_SYNC] PARTIAL→FAIL: failed_ratio={db_failed/csv_total:.1%} > 10%"
            )

        logger.info(
            f"[OHLCV_SYNC] synced={db_synced}/{csv_total}, failed={db_failed}, "
            f"csv_last={csv_last}, db_last={db_last}, "
            f"completeness={completeness:.1%}, status={sync_status}"
        )

        # Sync 결과를 shared state 파일에 저장 → engine이 소비
        _sync_state = {
            "sync_status": sync_status,
            "synced_count": db_synced,
            "failed_count": db_failed,
            "expected_count": csv_total,
            "completeness_ratio": completeness,
            "csv_last_date": csv_last,
            "db_last_date": db_last,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        try:
            import json
            sync_file = ohlcv_dir.parent.parent / "data" / "lab_live" / "ohlcv_sync.json"
            sync_file.parent.mkdir(parents=True, exist_ok=True)
            sync_file.write_text(json.dumps(_sync_state, indent=2), encoding="utf-8")
        except Exception:
            pass  # non-critical

    except Exception as e:
        logger.warning(f"[OHLCV_SYNC] DB sync failed: {e}, status=FAIL (non-critical)")

    # KOSPI index 업데이트 (CSV + DB)
    # 수동 EOD 경로에서도 KOSPI가 최신이어야 engine의 dates=idx_df["date"]가 당일로 확장됨
    # batch._update_kospi_index 재사용 (yfinance → DB fallback)
    try:
        from lifecycle.batch import _update_kospi_index
        from config import Gen4Config
        _update_kospi_index(Gen4Config(), logger)
    except Exception as e:
        logger.warning(f"[LAB_LIVE] KOSPI index update failed: {e}")

    logger.info(f"[LAB_LIVE] OHLCV update: {updated} stocks, latest={latest_date}")
    return latest_date
