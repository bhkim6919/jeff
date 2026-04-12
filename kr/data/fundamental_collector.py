"""
fundamental_collector.py — Fundamental Data Collector
======================================================
Collects PER, PBR, EPS, BPS, DIV, market cap, foreign ownership
for backtest and Valuation Top20 report.

Data source: Naver Finance (pykrx fundamental API broken since 2025)

Usage:
  # Daily snapshot (for batch report)
  python -m data.fundamental_collector --mode daily

  # Historical bulk collection (for backtest DB) — uses pykrx OHLCV dates
  python -m data.fundamental_collector --mode historical --start 20190101 --end 20260325
"""
from __future__ import annotations
import logging
import time
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    requests = None
    BeautifulSoup = None

try:
    from pykrx import stock as krx
except ImportError:
    krx = None

logger = logging.getLogger("gen4.fundamental")

_API_DELAY = 0.35
_FUND_DIR = Path(__file__).resolve().parent.parent.parent / "backtest" / "data_full" / "fundamental"


def _suppress_pykrx(func, *args, **kwargs):
    """Call pykrx with root logger noise suppressed."""
    root = logging.getLogger()
    prev = root.level
    root.setLevel(logging.WARNING)
    try:
        return func(*args, **kwargs)
    finally:
        root.setLevel(prev)


def _last_business_day() -> str:
    d = datetime.today()
    if d.hour < 16:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def _parse_number(s: str) -> float:
    """Parse Korean number string: '1,234', '+5.67', '-', '' → float."""
    if not s or s == "-" or s == "N/A":
        return 0.0
    s = s.replace(",", "").replace("+", "").replace("배", "").replace("원", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


# ── Naver Finance Scraper ───────────────────────────────────────────────────

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _fetch_naver_fundamental(ticker: str) -> Optional[dict]:
    """
    Fetch PER/PBR/EPS/BPS/DIV/market_cap from Naver Finance for one stock.

    Returns dict with keys:
        per, pbr, eps, bps, div_yield, market_cap, foreign_ratio
    """
    if requests is None:
        return None

    result = {
        "ticker": ticker,
        "per": 0.0, "pbr": 0.0, "eps": 0, "bps": 0,
        "div_yield": 0.0, "market_cap": 0, "foreign_ratio": 0.0,
    }

    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # ── per_table: PER, PBR, EPS, BPS, dividend
        per_table = soup.select_one("table.per_table")
        if per_table:
            rows = per_table.select("tr")
            for row in rows:
                text = row.get_text(strip=True)
                tds = row.select("td")
                ems = row.select("em")

                if "PER" in text and "EPS" in text and "추정" not in text:
                    # PER|EPS row — get the em values
                    vals = [em.get_text(strip=True) for em in ems]
                    if len(vals) >= 2:
                        result["per"] = _parse_number(vals[-2])
                        result["eps"] = int(_parse_number(vals[-1]))

                elif "PBR" in text and "BPS" in text:
                    vals = [em.get_text(strip=True) for em in ems]
                    if len(vals) >= 2:
                        result["pbr"] = _parse_number(vals[-2])
                        result["bps"] = int(_parse_number(vals[-1]))

                elif "배당수익률" in text:
                    vals = [em.get_text(strip=True) for em in ems]
                    if vals:
                        result["div_yield"] = _parse_number(vals[-1])

        # ── Market cap from em#_market_sum
        # Format: "1,118조\n8,116" (조 + 억) or just "8,116" (억 only)
        market_sum = soup.select_one("em#_market_sum")
        if market_sum:
            raw = market_sum.get_text(strip=True)
            # Parse "X조Y" or "X조\nY" format
            jo_match = re.search(r'([\d,]+)\s*조', raw)
            eok_parts = re.findall(r'([\d,]+)', raw)
            if jo_match and len(eok_parts) >= 2:
                # "1,118조 8,116억"
                jo = int(jo_match.group(1).replace(",", ""))
                eok = int(eok_parts[-1].replace(",", ""))
                result["market_cap"] = (jo * 10000 + eok) * 100_000_000
            elif eok_parts:
                # Just 억
                eok = int(eok_parts[0].replace(",", ""))
                result["market_cap"] = eok * 100_000_000

        # ── Foreign ratio from frgn page
        # Table row format: date, close, diff, pct, volume, net_buy, 소진율(%)
        # The 소진율 is the LAST column in data rows (e.g. 49.21%)
        try:
            frgn_url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
            frgn_resp = requests.get(frgn_url, headers=_HEADERS, timeout=10)
            if frgn_resp.status_code == 200:
                frgn_soup = BeautifulSoup(frgn_resp.text, "html.parser")
                # Look for the data table with type2 class
                for table in frgn_soup.select("table.type2"):
                    for row in table.select("tr"):
                        tds = [td.get_text(strip=True) for td in row.select("td")]
                        # Data row has 7+ columns, first looks like date (YYYY.MM.DD)
                        if len(tds) >= 7 and "." in tds[0] and len(tds[0]) == 10:
                            # Last column is foreign ratio %
                            ratio_str = tds[-1].replace("%", "").replace(",", "").strip()
                            try:
                                ratio = float(ratio_str)
                                if 0 < ratio <= 100:
                                    result["foreign_ratio"] = ratio
                            except ValueError:
                                pass
                            break  # Only need first (most recent) row
                    if result["foreign_ratio"] > 0:
                        break
            time.sleep(0.2)
        except Exception:
            pass

        return result

    except Exception as e:
        logger.debug(f"Naver fetch failed for {ticker}: {e}")
        return None


def fetch_daily_fundamental_naver(tickers: List[str]) -> Optional[pd.DataFrame]:
    """
    Fetch fundamentals for multiple stocks via Naver Finance.

    Returns DataFrame with columns:
        ticker, per, pbr, eps, bps, div_yield, market_cap, foreign_ratio
    """
    if requests is None:
        logger.error("requests library not installed")
        return None

    logger.info(f"[FUND] Fetching from Naver Finance ({len(tickers)} stocks)...")
    results = []
    success = 0

    for i, ticker in enumerate(tickers):
        data = _fetch_naver_fundamental(ticker)
        if data:
            results.append(data)
            success += 1

        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i+1}/{len(tickers)} ({success} success)")

        time.sleep(_API_DELAY)

    if not results:
        logger.warning("[FUND] No data fetched")
        return None

    df = pd.DataFrame(results)
    df["date"] = _last_business_day()
    logger.info(f"[FUND] Fetched: {len(df)} stocks from Naver Finance")
    return df


# ── Daily Snapshot (for Valuation Top20 Report) ─────────────────────────────

def fetch_daily_snapshot(date_str: str = "",
                         tickers: List[str] = None) -> Optional[pd.DataFrame]:
    """
    Fetch all fundamental data for current date.

    If tickers is None, uses universe from OHLCV directory.

    Returns DataFrame with:
        ticker, date, per, pbr, eps, bps, div_yield,
        market_cap, foreign_ratio
    """
    if not date_str:
        date_str = _last_business_day()

    # Get ticker list from OHLCV if not provided
    if tickers is None:
        ohlcv_dir = _FUND_DIR.parent / "ohlcv"
        if ohlcv_dir.exists():
            tickers = sorted(f.stem for f in ohlcv_dir.glob("*.csv"))
        else:
            logger.error("No OHLCV directory found for ticker list")
            return None

    df = fetch_daily_fundamental_naver(tickers)
    if df is not None:
        df["date"] = date_str

    return df


# ── Sector Average PER ──────────────────────────────────────────────────────

def calc_sector_avg_per(fund_df: pd.DataFrame,
                        sector_map: Dict[str, str]) -> pd.DataFrame:
    """
    Calculate sector average PER and add sector_per, sector_per_gap columns.

    sector_per_gap = (stock PER - sector avg PER) / sector avg PER * 100
    Negative = stock is cheaper than sector average.
    """
    df = fund_df.copy()
    df["sector"] = df["ticker"].map(sector_map).fillna("기타")

    # Exclude invalid PER for sector average (PER <= 0 or > 200)
    valid = df[(df["per"] > 0) & (df["per"] < 200)]
    sector_avg = valid.groupby("sector")["per"].median().rename("sector_per")

    df = df.merge(sector_avg, on="sector", how="left")
    df["sector_per_gap"] = np.where(
        df["sector_per"] > 0,
        (df["per"] - df["sector_per"]) / df["sector_per"] * 100,
        0
    )
    return df


# ── Historical Bulk Collection (for Backtest DB) ────────────────────────────

def collect_historical(start: str = "20190101", end: str = "",
                       market: str = "KOSPI",
                       interval_days: int = 21,
                       output_dir: Path = _FUND_DIR) -> Path:
    """
    Collect fundamental snapshots at regular intervals for backtest.

    NOTE: Historical data uses Naver Finance which only returns CURRENT values.
    For true historical fundamentals, use pykrx get_market_fundamental_by_date
    per-stock (slow but works for individual tickers).

    This collector saves daily snapshots that accumulate over time into a
    historical database. Run daily via batch to build up the DB.

    Args:
        start: Start date (YYYYMMDD) — for per-stock pykrx historical
        end: End date (YYYYMMDD)
        market: "KOSPI"
        interval_days: Calendar days between snapshots
        output_dir: Output directory

    Returns:
        Path to saved CSV
    """
    if not end:
        end = _last_business_day()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "fundamental_history.csv"

    # For historical, try pykrx per-stock method (slow but has history)
    # Get list of top tickers from OHLCV
    ohlcv_dir = output_dir.parent / "ohlcv"
    if not ohlcv_dir.exists():
        logger.error("No OHLCV directory")
        return output_path

    tickers = sorted(f.stem for f in ohlcv_dir.glob("*.csv"))
    if not tickers:
        logger.error("No tickers in OHLCV directory")
        return output_path

    logger.info(f"[HIST] Collecting historical fundamentals for {len(tickers)} stocks "
                f"from {start} to {end}")

    # Load existing data to check what we have
    existing_tickers = set()
    if output_path.exists():
        try:
            existing = pd.read_csv(output_path, dtype={"ticker": str, "date": str})
            existing_tickers = set(existing["ticker"].unique())
            logger.info(f"[HIST] Existing: {len(existing_tickers)} tickers already collected")
        except Exception:
            pass

    all_dfs = []
    collected = 0

    for i, ticker in enumerate(tickers):
        if ticker in existing_tickers:
            continue

        try:
            # pykrx per-stock fundamental by date (this API still works)
            df = _suppress_pykrx(
                krx.get_market_fundamental_by_date, start, end, ticker)
            time.sleep(_API_DELAY)

            if df is None or df.empty:
                continue

            df = df.reset_index()
            # Column mapping (pykrx returns: 날짜/date, BPS, PER, PBR, EPS, DIV, DPS)
            col_map = {
                "날짜": "date", "BPS": "bps", "PER": "per",
                "PBR": "pbr", "EPS": "eps", "DIV": "div_yield", "DPS": "dps",
            }
            df = df.rename(columns=col_map)

            if "date" not in df.columns and df.index.name in ("날짜", "date"):
                df = df.reset_index()
                df = df.rename(columns=col_map)

            df["ticker"] = ticker

            # Sample at intervals (not every day)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date")
                df = df.iloc[::interval_days]  # every N days
                df["date"] = df["date"].dt.strftime("%Y%m%d")

            cols = ["ticker", "date"] + [c for c in ["per", "pbr", "eps", "bps", "div_yield"]
                                          if c in df.columns]
            all_dfs.append(df[cols])
            collected += 1

            if collected % 50 == 0:
                logger.info(f"  Progress: {collected} tickers collected ({i+1}/{len(tickers)})")

        except Exception as e:
            logger.debug(f"  {ticker}: {e}")
            continue

    if not all_dfs:
        logger.info(f"[HIST] No new data to add")
        return output_path

    new_df = pd.concat(all_dfs, ignore_index=True)

    # Merge with existing
    if output_path.exists():
        try:
            existing = pd.read_csv(output_path, dtype={"ticker": str, "date": str})
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")
            combined = combined.sort_values(["date", "ticker"]).reset_index(drop=True)
        except Exception:
            combined = new_df
    else:
        combined = new_df

    combined.to_csv(output_path, index=False)

    n_dates = combined["date"].nunique()
    n_tickers = combined["ticker"].nunique()
    logger.info(f"[HIST] Saved: {output_path}")
    logger.info(f"[HIST] Total: {n_dates} dates x {n_tickers} tickers = {len(combined)} rows")

    return output_path


# ── Moving Average Helper (for Valuation Report) ────────────────────────────

def calc_ma_analysis(close_series: pd.Series) -> dict:
    """
    Calculate moving average analysis for valuation report.

    Returns dict with MA values, alignment, golden/death cross, disparity.
    """
    c = close_series.values.astype(float)
    last = float(c[-1]) if len(c) > 0 else 0

    result = {"last_close": int(last)}

    for period in [5, 20, 60, 120, 200]:
        if len(c) >= period:
            ma = float(np.mean(c[-period:]))
            result[f"ma{period}"] = int(ma)
            result[f"pct_vs_ma{period}"] = round((last / ma - 1) * 100, 1) if ma > 0 else 0
        else:
            result[f"ma{period}"] = 0
            result[f"pct_vs_ma{period}"] = 0

    # MA alignment (using MA20/60/120)
    ma20 = result.get("ma20", 0)
    ma60 = result.get("ma60", 0)
    ma120 = result.get("ma120", 0)

    if last > ma20 > ma60 > ma120 and ma120 > 0:
        result["alignment"] = "BULLISH"
    elif last < ma20 < ma60 < ma120 and last > 0:
        result["alignment"] = "BEARISH"
    else:
        result["alignment"] = "MIXED"

    # MA200 position
    ma200 = result.get("ma200", 0)
    if ma200 > 0:
        result["above_ma200"] = last > ma200
    else:
        result["above_ma200"] = None

    return result


# ── CLI Entry Point ─────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Gen4 Fundamental Data Collector")
    parser.add_argument("--mode", choices=["daily", "historical"],
                        default="daily", help="Collection mode")
    parser.add_argument("--start", default="20190101",
                        help="Historical start date (YYYYMMDD)")
    parser.add_argument("--end", default="",
                        help="Historical end date (YYYYMMDD)")
    parser.add_argument("--interval", type=int, default=21,
                        help="Days between historical snapshots")
    parser.add_argument("--output", default="",
                        help="Output directory override")
    args = parser.parse_args()

    out_dir = Path(args.output) if args.output else _FUND_DIR

    if args.mode == "daily":
        df = fetch_daily_snapshot()
        if df is not None:
            out_dir.mkdir(parents=True, exist_ok=True)
            date_str = _last_business_day()
            path = out_dir / f"fundamental_{date_str}.csv"
            df.to_csv(path, index=False)
            print(f"Saved: {path} ({len(df)} stocks)")
        else:
            print("Failed to fetch daily snapshot")

    elif args.mode == "historical":
        path = collect_historical(
            start=args.start, end=args.end,
            interval_days=args.interval, output_dir=out_dir)
        print(f"Historical data: {path}")


if __name__ == "__main__":
    main()
