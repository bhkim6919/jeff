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

                ld = str(combined["date"].max().date())
                if ld > latest_date:
                    latest_date = ld

            if updated % 100 == 0 and updated > 0:
                time.sleep(0.3)

        except Exception:
            continue

    logger.info(f"[LAB_LIVE] OHLCV update: {updated} stocks, latest={latest_date}")
    return latest_date
