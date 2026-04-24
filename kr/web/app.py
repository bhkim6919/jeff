# -*- coding: utf-8 -*-
"""
app.py -- FastAPI Web Monitoring Server
========================================
kr 모니터링 대시보드 백엔드.
SSE(Server-Sent Events)로 실시간 상태를 브라우저에 스트리밍.

Usage:
    cd kr
    python -m uvicorn web.app:app --host 0.0.0.0 --port 8080 --reload

    Or programmatically:
    from web.app import create_app
    app = create_app()
"""
from __future__ import annotations

# ── Path bootstrap MUST run before any kr.* / shared.* import ────────────────
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))  # audit:allow-syspath: bootstrap-locator (kr/)
import _bootstrap_path  # noqa: F401  -- side-effect: sys.path setup (kr + project root)

import asyncio
import json
import logging
import threading
import time
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import hashlib

from web.api_state import tracker

logger = logging.getLogger("gen4.rest.web")

# ── Paths ─────────────────────────────────────────────────────

WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"
# State dir — canonical location is kr/state/ (config.STATE_DIR).
# kr-legacy/state/ is a frozen mirror and stops updating once the live/batch
# process moves to kr/state, which silently desynced the gate (BATCH_MISSING
# even after batch wrote the canonical file). Pin to kr/state/.
_GEN04_STATE_DIR = Path(__file__).resolve().parent.parent / "state"
_GEN04_REPORT_DIR = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "report" / "output"
_GEN04_SECTOR_MAP_PATH = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "data" / "sector_map.json"

# Sector map cache (loaded once)
_sector_map_cache: dict = {"data": None, "ts": 0}

# ── DD Guard Thresholds ───────────────────────────────────────
# Source: kr-legacy/risk/exposure_guard.py lines 159-165
# config_version tracks drift — update when engine config changes
_DD_CONFIG_VERSION = "gen4_v4.1_trail12_rebal21_dd4m7"
_DD_LEVELS = (
    (-0.25, 0.00, "DD_SAFE_MODE"),
    (-0.20, 0.00, "DD_SEVERE"),
    (-0.15, 0.00, "DD_CRITICAL"),
    (-0.10, 0.50, "DD_WARNING"),
    (-0.05, 0.70, "DD_CAUTION"),
)

# ── Index name mapping (provider returns no name) ─────────────
_INDEX_NAMES = {"0_001": "코스피", "1_101": "코스닥"}

# ── Safe Read Helpers ─────────────────────────────────────────

_LAST_GOOD: dict = {}  # key=source_key → {"data":..., "source_ts":...}
_LAST_GOOD_TTL = 300   # seconds


def _safe_read_json(path: str) -> dict:
    """Read JSON file safely. No mutable defaults. Returns source_ts + read_ts."""
    try:
        raw = Path(path).read_text("utf-8")
        data = json.loads(raw)
        # Extract source timestamp from data if present
        source_ts_raw = data.get("timestamp", "")
        if isinstance(source_ts_raw, (int, float)):
            source_ts = float(source_ts_raw)
        elif isinstance(source_ts_raw, str) and source_ts_raw:
            from datetime import datetime as _dt
            try:
                source_ts = _dt.fromisoformat(source_ts_raw).timestamp()
            except Exception:
                source_ts = time.time()
        else:
            source_ts = time.time()
        return {"ok": True, "data": data, "source_ts": source_ts, "read_ts": time.time(), "error": None}
    except Exception as e:
        return {"ok": False, "data": None, "source_ts": 0, "read_ts": time.time(), "error": str(e)}


def _safe_read_csv_tail(path: str, n: int = 10) -> dict:
    """Read last N lines of CSV. Returns source_ts from file mtime."""
    try:
        p = Path(path)
        mtime = p.stat().st_mtime
        import csv as _csv
        with open(p, "r", encoding="utf-8-sig") as f:
            reader = _csv.DictReader(f)
            rows = list(reader)
        tail = rows[-n:] if len(rows) > n else rows
        return {"ok": True, "rows": tail, "source_ts": mtime, "read_ts": time.time(), "error": None}
    except Exception as e:
        return {"ok": False, "rows": [], "source_ts": 0, "read_ts": time.time(), "error": str(e)}


def _get_or_fallback(source_key: str, fresh: dict) -> dict:
    """Use fresh data if ok, else cached with TTL. Expired = TTL exceeded."""
    now = time.time()
    if fresh["ok"]:
        # Store rows for CSV sources, data for JSON sources
        store_val = fresh.get("rows") if "rows" in fresh else fresh["data"]
        _LAST_GOOD[source_key] = {"data": store_val, "source_ts": fresh["source_ts"]}
        return {**fresh, "from_cache": False, "expired": False}
    cached = _LAST_GOOD.get(source_key)
    if cached:
        age = now - cached["source_ts"]
        expired = age > _LAST_GOOD_TTL
        return {
            "ok": False, "data": cached["data"], "source_ts": cached["source_ts"],
            "read_ts": now, "error": fresh["error"], "from_cache": True, "expired": expired,
        }
    return {**fresh, "from_cache": False, "expired": True}


# ── Dashboard Data Helpers ────────────────────────────────────

def _get_sector_map() -> dict:
    """Load sector_map.json once, cache forever."""
    if _sector_map_cache["data"] is None:
        raw = _safe_read_json(str(_GEN04_SECTOR_MAP_PATH))
        if raw["ok"] and raw["data"]:
            _sector_map_cache["data"] = raw["data"]
            _sector_map_cache["ts"] = raw["source_ts"]
        else:
            _sector_map_cache["data"] = {}
    return _sector_map_cache["data"]


# 29개 섹터 → 10개 테마 그룹핑
_THEME_MAP = {
    "전기·전자": "반도체/IT", "IT 하드웨어": "반도체/IT",
    "금융": "금융", "보험": "금융", "기타금융": "금융",
    "화학": "화학/소재", "금속": "화학/소재", "비금속": "화학/소재",
    "의약품": "바이오/의료", "의료·정밀기기": "바이오/의료",
    "음식료·담배": "소비재", "유통": "소비재", "섬유·의복·가죽": "소비재",
    "일반서비스": "소비재",
    "기계": "산업재", "건설": "산업재", "운수·창고": "산업재",
    "기기·장비": "산업재",
    "전기·가스": "에너지", "광업": "에너지",
    "통신": "통신/미디어", "디지털컨텐츠화": "통신/미디어",
    "레저·엔터테인먼트": "통신/미디어",
    "운송장비·부품": "자동차/운송",
    "부동산": "부동산",
}

def _map_theme(sector: str) -> str:
    return _THEME_MAP.get(sector, "기타")


def _compute_rebal_schedule(last_rebal_str: str, cycle_days: int = 21) -> dict:
    """R24 (2026-04-23): precise rebal schedule from KOSPI trading calendar.

    Replaces dashboard.js calendar-day approx (`cycle * 1.4`) with real
    business day counting. Future dates via pd.bdate_range (excludes
    weekends; Korean holidays not perfect but close enough for display).

    Returns dict with: last, cycle, next_date ("YYYY.MM.DD"), d_day (int).
    Fields next_date/d_day omitted silently on any failure — caller may
    fall back to client-side approximation.
    """
    result = {"last": last_rebal_str, "cycle": cycle_days}
    if not last_rebal_str:
        return result
    try:
        import pandas as _pd
        # Past: count trading days elapsed from KOSPI.csv
        from config import Gen4Config as _G4Cfg
        _kdf = _pd.read_csv(_G4Cfg().INDEX_FILE)
        _dcol = "date" if "date" in _kdf.columns else "index"
        _kdf[_dcol] = _pd.to_datetime(_kdf[_dcol])
        _last_dt = _pd.to_datetime(last_rebal_str, format="%Y%m%d")
        _today_dt = _pd.Timestamp.now().normalize()
        _elapsed = _kdf[
            (_kdf[_dcol] > _last_dt) & (_kdf[_dcol] <= _today_dt)
        ]
        _d_day = max(0, cycle_days - len(_elapsed))
        result["d_day"] = _d_day
        # Future: use pandas business-day offset (weekdays only)
        if _d_day > 0:
            _next_dt = _pd.bdate_range(
                _today_dt + _pd.Timedelta(days=1),
                periods=_d_day,
            )[-1]
        else:
            _next_dt = _today_dt
        result["next_date"] = _next_dt.strftime("%Y.%m.%d")
    except Exception:
        pass
    return result


def _inject_sectors(account_data: dict) -> None:
    """Add sector/theme field to each holding in account data."""
    if not account_data:
        return
    sm = _get_sector_map()
    holdings = account_data.get("holdings", [])
    sector_summary = {}
    for h in holdings:
        code = h.get("code", "")
        sector = sm.get(code, "기타")
        theme = _map_theme(sector)
        h["sector"] = theme
        s = sector_summary.setdefault(theme, {"count": 0, "eval_amt": 0, "pnl": 0})
        s["count"] += 1
        s["eval_amt"] += h.get("eval_amt", 0) or 0
        pnl_val = h.get("pnl", 0)
        s["pnl"] += int(pnl_val) if pnl_val else 0
    # Sector regime: PnL 기반 간이 판정
    sector_list = []
    for k, v in sorted(sector_summary.items(), key=lambda x: -x[1]["eval_amt"]):
        eval_amt = v["eval_amt"]
        pnl = v["pnl"]
        pnl_pct = (pnl / (eval_amt - pnl) * 100) if eval_amt > pnl and (eval_amt - pnl) > 0 else 0
        # 섹터 레짐: 수익률 기반 3단계
        if pnl_pct > 3.0:
            s_regime = "BULL"
        elif pnl_pct < -3.0:
            s_regime = "BEAR"
        else:
            s_regime = "SIDEWAYS"
        sector_list.append({
            "sector": k, "count": v["count"], "eval_amt": eval_amt,
            "pnl": pnl, "pnl_pct": round(pnl_pct, 2), "regime": s_regime,
        })
    account_data["sector_summary"] = sector_list


def _compute_dd_guard(total_asset: float) -> dict:
    """Wrapper for backward compat."""
    return _compute_dd_guard_from(total_asset, None)


def _compute_dd_guard_from(total_asset: float, account_data: dict = None) -> dict:
    """Compute DD guard from rest_equity_snapshots + broker fallback.

    Priority:
      1. rest_equity_snapshots (peak/trough: all-time MAX/MIN of EOD close_equity)
      2. broker pred_close_pric × qty + cash (전일대비 fallback)
    """
    from datetime import date as _date_cls
    today_iso = _date_cls.today().isoformat()

    result = {
        "daily_dd": None, "daily_dd_available": False,
        "monthly_dd": None, "monthly_dd_available": False,
        "level": "UNKNOWN", "buy_permission": "UNKNOWN",
        "config_version": _DD_CONFIG_VERSION,
        "source_ts": int(time.time()), "stale": False,
        "source_total_asset": total_asset,
        "source_prev_close": None, "source_peak": None, "source_trough": None,
        "prev_close_date": "", "peak_date": "", "trough_date": "",
        "prev_close_source": "none",
        "snapshot_count": 0,
        "from_cache": False, "expired": False, "error": None,
    }

    if not total_asset or total_asset <= 0:
        return result

    # 1) Prev close — rest_equity_snapshots → dashboard_snapshots → broker
    prev_close = None
    try:
        from web.rest_state_db import get_prev_eod_equity
        prev_row = get_prev_eod_equity(today_iso)
        if prev_row and (prev_row.get("close_equity") or 0) > 0:
            prev_close = float(prev_row["close_equity"])
            result["prev_close_date"] = prev_row.get("market_date", "")
            result["prev_close_source"] = "rest_eod"
    except Exception as e:
        logging.getLogger("web").debug(f"[DD_GUARD] rest_eod prev: {e}")

    if prev_close is None:
        try:
            from web.dashboard_db import get_prev_day_last_equity
            prev_row = get_prev_day_last_equity(today_iso)
            if prev_row and prev_row.get("equity", 0) > 0:
                prev_close = float(prev_row["equity"])
                result["prev_close_date"] = prev_row.get("market_date", "")
                result["prev_close_source"] = "dashboard_history"
        except Exception as e:
            logging.getLogger("web").debug(f"[DD_GUARD] dash_history prev: {e}")

    if prev_close is None and account_data:
        prev_eval = account_data.get("prev_eval_amt") or 0
        cash = account_data.get("cash") or 0
        if prev_eval > 0:
            prev_close = float(prev_eval + cash)
            result["prev_close_source"] = "broker"
    result["source_prev_close"] = prev_close

    # 2) UI peak/trough — 오늘 tick (intraday drawdown/recovery 시각화)
    try:
        from web.dashboard_db import get_today_equity_peak_trough
        pt = get_today_equity_peak_trough(today_iso)
        result["snapshot_count"] = pt.get("count", 0)
        if pt.get("count", 0) > 0:
            peak_val = pt["peak"]
            if total_asset > peak_val:
                result["source_peak"] = total_asset
                result["peak_date"] = ""
            else:
                result["source_peak"] = peak_val
                result["peak_date"] = pt.get("peak_ts", "")

            trough_val = pt["trough"]
            if total_asset < trough_val:
                result["source_trough"] = total_asset
                result["trough_date"] = ""
            else:
                result["source_trough"] = trough_val
                result["trough_date"] = pt.get("trough_ts", "")
        elif total_asset > 0:
            result["source_peak"] = total_asset
            result["source_trough"] = total_asset
    except Exception as e:
        logging.getLogger("web").debug(f"[DD_GUARD] today peak_trough: {e}")

    # 3) monthly_dd — all-time history peak 기준 (alert_engine 호환)
    alltime_peak = None
    try:
        from web.dashboard_db import get_alltime_equity_peak
        ap = get_alltime_equity_peak()
        if ap.get("count", 0) > 0 and ap["peak"] > 0:
            alltime_peak = max(ap["peak"], total_asset)
    except Exception as e:
        logging.getLogger("web").debug(f"[DD_GUARD] alltime peak: {e}")

    # Daily DD (vs yesterday EOD)
    if prev_close and prev_close > 0:
        result["daily_dd"] = round((total_asset - prev_close) / prev_close, 6)
        result["daily_dd_available"] = True

    # Monthly DD (vs all-time peak — alert_engine 임계치 판정용)
    if alltime_peak and alltime_peak > 0:
        result["monthly_dd"] = round((total_asset - alltime_peak) / alltime_peak, 6)
        result["monthly_dd_available"] = True

    # DD level
    dd_val = result["monthly_dd"] if result["monthly_dd_available"] else result["daily_dd"]
    if dd_val is not None:
        level = "NORMAL"
        buy_scale = 1.0
        for threshold, scale, label in _DD_LEVELS:
            if dd_val <= threshold:
                level = label
                buy_scale = scale
                break
        result["level"] = level
        if buy_scale >= 1.0:
            result["buy_permission"] = "NORMAL"
        elif buy_scale > 0:
            result["buy_permission"] = "REDUCED"
        else:
            result["buy_permission"] = "BLOCKED"

    return result


def _read_trail_stops() -> dict:
    """Wrapper for backward compat — reads file internally."""
    raw = _safe_read_json(str(_GEN04_STATE_DIR / "portfolio_state_live.json"))
    fb = _get_or_fallback("portfolio_state", raw)
    return _read_trail_stops_from(fb)


def _read_trail_stops_from(fb: dict) -> dict:
    """Read trail stop data from pre-read fallback. No file I/O."""
    result = {"stops": [], "source_ts": fb.get("source_ts", 0), "error": fb.get("error"),
              "from_cache": fb.get("from_cache", False), "expired": fb.get("expired", False)}
    data = fb.get("data")
    if not data:
        return result
    for pos in (data.get("positions") or {}).values():
        hwm = pos.get("high_watermark", 0) or 0
        trail = pos.get("trail_stop_price", 0) or 0
        cur = pos.get("current_price", 0) or 0
        if not hwm or not cur:
            result["stops"].append({
                "code": pos.get("code", ""), "hwm": hwm, "trail_price": trail,
                "current_price": cur, "drop_from_hwm_pct": None,
                "distance_to_trail_pct": None, "risk_zone": "N/A",
                "triggered": None, "price_source": "state_file",
                "source_ts": fb.get("source_ts", 0),
            })
            continue
        drop = round((cur - hwm) / hwm * 100, 2)
        dist = round((cur - trail) / cur * 100, 2) if cur > 0 and trail > 0 else None
        triggered = cur <= trail if trail > 0 else None
        if drop > -5:
            zone = "SAFE"
        elif drop > -8:
            zone = "CAUTION"
        elif drop > -10:
            zone = "WARNING"
        else:
            zone = "DANGER"
        result["stops"].append({
            "code": pos.get("code", ""), "hwm": hwm, "trail_price": trail,
            "current_price": cur, "drop_from_hwm_pct": drop,
            "distance_to_trail_pct": dist, "risk_zone": zone,
            "triggered": triggered, "price_source": "state_file",
            "source_ts": fb.get("source_ts", 0),
        })
    return result


def _detect_engine_status() -> dict:
    """Detect whether the KR live engine is currently running.

    The UI must distinguish three separate situations that all look like
    "stale RECON" on the surface:

      1. Engine running, RECON ran recently, all good.
      2. Engine running, RECON temporarily stale (retry pending).
      3. Engine NOT running at all — nobody is writing runtime_state.

    (3) is the dangerous one: REST polling keeps broker values fresh so
    the dashboard looks alive, but trail stop / DD guard / rebalance /
    EOD are all dead. Operators can mistake it for (2).

    This helper identifies (3) by three independent checks:
      - kr/state/ directory exists
      - runtime_state_live.json exists and is parseable
      - the state was written recently (<10min) by the engine itself
        (``_write_origin == "engine"``)

    Any one of those failing forces ENGINE_OFFLINE in the returned dict.
    Returned dict is additive — existing callers ignore it; new call
    sites (state snapshot, health endpoint) override RED/disable
    auto-trading accordingly.
    """
    state_dir = _GEN04_STATE_DIR
    runtime_file = state_dir / "runtime_state_live.json"
    portfolio_file = state_dir / "portfolio_state_live.json"

    checks = {
        "state_dir_exists": state_dir.exists(),
        "runtime_file_exists": runtime_file.exists(),
        "portfolio_file_exists": portfolio_file.exists(),
    }

    last_write_ts = 0.0
    last_write_origin = None
    if checks["runtime_file_exists"]:
        try:
            from datetime import datetime as _dt
            data = json.loads(runtime_file.read_text(encoding="utf-8"))
            last_write_origin = data.get("_write_origin")
            ts_str = data.get("_write_ts") or data.get("timestamp")
            if ts_str:
                try:
                    last_write_ts = _dt.fromisoformat(ts_str).timestamp()
                except Exception:
                    pass
        except Exception:
            pass

    last_write_age_sec = (time.time() - last_write_ts) if last_write_ts else None

    fresh = (last_write_age_sec is not None and last_write_age_sec < 600)
    engine_origin = (last_write_origin == "engine")

    if not checks["state_dir_exists"]:
        status, reason = "OFFLINE", "kr/state/ directory missing"
    elif not checks["runtime_file_exists"]:
        status, reason = "OFFLINE", "runtime_state_live.json missing"
    elif last_write_age_sec is None:
        status, reason = "OFFLINE", "engine state has no write timestamp"
    elif not fresh:
        _h = last_write_age_sec / 3600.0
        status, reason = "OFFLINE", f"engine state stale ({_h:.1f}h since last write)"
    elif not engine_origin:
        status, reason = "OFFLINE", f"state not written by live engine (origin={last_write_origin!r})"
    else:
        status, reason = "ONLINE", "live engine writing state"

    return {
        "status": status,
        "reason": reason,
        "last_write_ts": last_write_ts,
        "last_write_age_sec": round(last_write_age_sec, 1) if last_write_age_sec is not None else None,
        "last_write_origin": last_write_origin,
        "checks": checks,
    }


def _apply_engine_offline_override(snap: dict) -> dict:
    """Apply ENGINE_OFFLINE safety gate to a state snapshot in-place.

    Shared between /api/state (get_state_snapshot) and /sse/state
    (_sse_generator) so both paths produce the same safety-gated view.
    First version only patched the one-shot endpoint, which meant the
    dashboard (which uses SSE) still showed YELLOW + "Sync mismatch"
    while /api/state correctly showed RED + ENGINE_OFFLINE — exactly
    the misleading UI we were trying to eliminate.

    When the engine is OFFLINE:
      - health.status = RED, health.reason = "ENGINE_OFFLINE: ..."
      - auto_trading.enabled = False, blocker ← "ENGINE_OFFLINE" (top)
      - recon.status = UNAVAILABLE, engine_offline = True
      - sync rows annotated com_disabled=True, com_reason=ENGINE_OFFLINE
      - dd_guard.buy_permission = BLOCKED (so the BUY pill stops saying NORMAL)
      - system_risk.primary = ENGINE_OFFLINE with matching reason_codes
        (so the RISK pill stops saying READ_FAIL or the stale value)

    Returns the same dict it was given (convenience for chaining).
    """
    engine = _detect_engine_status()
    snap["engine_status"] = engine
    if engine["status"] != "OFFLINE":
        return snap

    snap["health"] = {
        "status": "RED",
        "reason": f"ENGINE_OFFLINE: {engine['reason']}",
    }
    _auto_dict = snap.get("auto_trading") or {}
    _blockers = list(_auto_dict.get("blockers") or [])
    if "ENGINE_OFFLINE" not in _blockers:
        _blockers.insert(0, "ENGINE_OFFLINE")
    _auto_dict["enabled"] = False
    _auto_dict["blockers"] = _blockers
    _auto_dict["reason_summary"] = "engine_offline"
    _auto_dict["highest_priority_blocker"] = "ENGINE_OFFLINE"
    snap["auto_trading"] = _auto_dict
    _recon = snap.get("recon") or {}
    _recon["status"] = "UNAVAILABLE"
    _recon["engine_offline"] = True
    _recon["engine_reason"] = engine["reason"]
    snap["recon"] = _recon
    for _row in (snap.get("sync") or []):
        if isinstance(_row, dict):
            _row["com_disabled"] = True
            _row["com_reason"] = "ENGINE_OFFLINE"

    # Hero badges on dashboard (BUY / RISK / Emergency) read these two
    # fields directly; without overriding them the operator sees a green
    # "BUY: NORMAL" next to the red ENGINE_OFFLINE banner — the exact
    # mixed signal we must not allow.
    _dd = snap.get("dd_guard") or {}
    _dd["buy_permission"] = "BLOCKED"
    _dd["engine_offline"] = True
    snap["dd_guard"] = _dd
    _sr = snap.get("system_risk") or {}
    _sr["primary"] = "ENGINE_OFFLINE"
    _rc = list(_sr.get("reason_codes") or [])
    if "ENGINE_OFFLINE" not in _rc:
        _rc.insert(0, "ENGINE_OFFLINE")
    _sr["reason_codes"] = _rc
    _sr["engine_offline"] = True
    _sr["reason"] = engine["reason"]
    snap["system_risk"] = _sr
    return snap


def _compute_recon_status() -> dict:
    """Read RECON status from runtime state.

    When the live engine is OFFLINE (see _detect_engine_status()), RECON
    is reported with an extra ``status="UNAVAILABLE"`` + ``engine_offline=True``
    to distinguish it from the "engine running, RECON just stale" case.
    Existing fields (``stale``, ``expired``, ``age_sec``, ...) stay so older
    consumers keep working.
    """
    raw = _safe_read_json(str(_GEN04_STATE_DIR / "runtime_state_live.json"))
    fb = _get_or_fallback("runtime_state", raw)
    data = fb.get("data") or {}
    source_ts = fb.get("source_ts", 0)
    age = time.time() - source_ts if source_ts else 99999

    engine = _detect_engine_status()

    out = {
        "unreliable": data.get("recon_unreliable", False),
        "last_run": data.get("timestamp", ""),
        "age_sec": round(age, 1),
        "stale": age > 7200,
        "source": "runtime_state_live.json",
        "from_cache": fb.get("from_cache", False),
        "expired": fb.get("expired", False),
        "error": fb.get("error"),
    }
    if engine["status"] == "OFFLINE":
        out["status"] = "UNAVAILABLE"
        out["engine_offline"] = True
        out["engine_reason"] = engine["reason"]
    else:
        out["status"] = "AVAILABLE"
        out["engine_offline"] = False
    return out


def _read_recent_trades(limit: int = 10) -> dict:
    """Read recent trades from COM trades.csv + REST test_orders DB (merged)."""
    # 1. COM trades.csv
    raw = _safe_read_csv_tail(str(_GEN04_REPORT_DIR / "trades.csv"), n=limit * 2)
    fb = _get_or_fallback("trades_csv", raw)
    if fb.get("from_cache"):
        rows = fb.get("data") or []
    else:
        rows = fb.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    trades = []
    for i, row in enumerate(reversed(rows)):
        if len(trades) >= limit:
            break
        eid = row.get("event_id", "")
        if not eid:
            key_str = f"{row.get('date','')}{row.get('code','')}{row.get('side','')}{row.get('quantity','')}{row.get('price','')}{i}"
            eid = hashlib.md5(key_str.encode()).hexdigest()[:12]
        trades.append({
            "date": row.get("date", ""),
            "code": row.get("code", ""),
            "side": row.get("side", ""),
            "quantity": int(row.get("quantity", 0) or 0),
            "price": float(row.get("price", 0) or 0),
            "mode": row.get("mode", "COM"),
            "event_id": eid,
        })

    # 2. REST test_orders (from dashboard.db)
    try:
        from web.dashboard_db import _conn as _dash_conn
        c = _dash_conn()
        db_rows = c.execute(
            "SELECT * FROM test_orders WHERE exec_price > 0 ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        c.close()
        for r in db_rows:
            trades.append({
                "date": str(r["ts"])[:10] if r["ts"] else "",
                "code": r["code"] or "",
                "side": r["side"] or "",
                "quantity": r["qty"] or 0,
                "price": r["exec_price"] or 0,
                "mode": "REST",
                "event_id": f"rest_{r['order_no']}",
            })
    except Exception:
        pass

    # Sort by date desc
    trades.sort(key=lambda t: t.get("date", ""), reverse=True)
    return {
        "trades": trades, "source_ts": fb.get("source_ts", 0),
        "from_cache": fb.get("from_cache", False), "expired": fb.get("expired", False),
        "error": fb.get("error"),
    }


def _compute_system_risk(dd_guard: dict, recon: dict, data_sources: dict) -> dict:
    """Compute SYSTEM RISK with priority ordering."""
    reasons = []
    # 1. READ_FAIL (highest)
    for src, info in data_sources.items():
        if not info.get("ok", True) and info.get("expired", False):
            reasons.append("READ_FAIL")
            break
    # 2. SAFE_MODE
    if dd_guard.get("level") == "DD_SAFE_MODE":
        reasons.append("SAFE_MODE")
    # 3. RECON_WARN
    if recon.get("unreliable"):
        reasons.append("RECON_WARN")
    # 4. STALE
    max_age = max((info.get("age_sec", 0) for info in data_sources.values()), default=0)
    if max_age > 120:
        reasons.append("STALE")
    # Primary = highest priority
    priority = ["READ_FAIL", "SAFE_MODE", "RECON_WARN", "STALE"]
    primary = "OK"
    for p in priority:
        if p in reasons:
            primary = p
            break
    return {"primary": primary, "reason_codes": reasons}


# ── App Factory ───────────────────────────────────────────────

def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    application = FastAPI(
        title="Q-TRON REST Monitor",
        description="kr Trading System Monitoring Dashboard",
        version="1.0.0",
        docs_url="/docs",
    )

    # IP monitor — check every 10 min in background
    import threading
    def _ip_monitor_loop():
        # sys.path already prepared by _bootstrap_path at top of module
        from data.ip_monitor import check_ip
        import time as _time
        while True:
            try:
                check_ip()
            except Exception:
                pass
            _time.sleep(600)
    _ip_thread = threading.Thread(target=_ip_monitor_loop, daemon=True)
    _ip_thread.start()

    # CORS (allow local development)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Static files
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    application.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Templates
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # ── Routes ────────────────────────────────────────────

    @application.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        """Main dashboard page."""
        return templates.TemplateResponse(request, "index.html")

    # NOTE: /api/state는 line ~1271의 get_state_snapshot()이 진짜 핸들러 (portfolio cache + auto_trading 포함).
    # 과거 이 자리에 있던 tracker-only 중복 라우트는 FastAPI 라우팅 우선순위(먼저 등록된 게 이김) 때문에
    # portfolio/auto_trading 필드를 가려서 "AUTO GATE: UNKNOWN" 이슈 유발 → 제거 (2026-04-18).

    # ── Portfolio (live REST API data) ─────────────────
    _provider_cache = {"instance": None}

    def _get_provider():
        if _provider_cache["instance"] is None:
            # sys.path already prepared by _bootstrap_path at top of module
            from data.rest_provider import KiwoomRestProvider
            _provider_cache["instance"] = KiwoomRestProvider(server_type="REAL")
        return _provider_cache["instance"]

    @application.get("/api/rebalance")
    async def get_rebalance():
        """Rebalance schedule from state file."""
        try:
            state_dir = Path(__file__).resolve().parent.parent / "state"
            # Try REST state first, then COM state
            for name in ["portfolio_state_live.json", "portfolio_state_paper.json"]:
                sf = state_dir / name
                if sf.exists():
                    import json as _json
                    with open(sf, "r", encoding="utf-8") as f:
                        state = _json.load(f)
                    last_rebal = state.get("last_rebalance_date", "")
                    # Also check kr-legacy COM state
                    # Runtime state has rebalance date
                    com_rt = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "state" / "runtime_state_live.json"
                    if com_rt.exists():
                        with open(com_rt, "r", encoding="utf-8") as f:
                            rt = _json.load(f)
                        last_rebal = rt.get("last_rebalance_date", last_rebal)
                    return {"last_rebalance": last_rebal, "cycle_days": 21}
            return {"last_rebalance": "", "cycle_days": 21}
        except Exception as e:
            return {"last_rebalance": "", "cycle_days": 21, "error": str(e)}

    # ── Rebalance Command API (state machine) ──────────────────────

    def _rebal_deps():
        """Shared dependency factory for rebalance endpoints."""
        from core.state_manager import StateManager
        from config import Gen4Config
        from data.rest_provider import KiwoomRestProvider
        from runtime.order_executor import OrderExecutor
        from runtime.order_tracker import OrderTracker
        from report.reporter import TradeLogger
        cfg = Gen4Config()
        sm = StateManager(cfg.STATE_DIR, trading_mode="live")
        prov = KiwoomRestProvider(server_type="REAL")
        tracker = OrderTracker()
        tl = TradeLogger(cfg.TRADE_LOG, cfg.CLOSE_LOG)
        executor = OrderExecutor(prov, tracker, tl, simulate=False,
                                 trading_mode="live")
        return cfg, sm, prov, executor, tl, tracker

    @application.get("/api/rebalance/status")
    async def get_rebalance_status_api():
        """Rebalance state machine status."""
        try:
            from web.rebalance_api import get_rebalance_status
            from core.state_manager import StateManager
            from config import Gen4Config
            cfg = Gen4Config()
            sm = StateManager(cfg.STATE_DIR, trading_mode="live")
            return get_rebalance_status(sm, cfg)
        except Exception as e:
            return {"error": str(e), "phase": "ERROR"}

    @application.get("/api/rebalance/preview")
    async def get_rebalance_preview_api():
        """Create preview snapshot. Locks preview_hash for SELL/BUY."""
        try:
            from web.rebalance_api import create_preview
            from core.state_manager import StateManager
            from config import Gen4Config
            from data.rest_provider import KiwoomRestProvider
            cfg = Gen4Config()
            sm = StateManager(cfg.STATE_DIR, trading_mode="live")
            prov = KiwoomRestProvider(server_type="REAL")
            return create_preview(sm, cfg, prov)
        except Exception as e:
            return {"error": str(e), "sells": [], "buys": []}

    @application.post("/api/rebalance/sell")
    async def execute_rebalance_sell_api(request: Request):
        """Execute SELL. Requires: preview_hash, request_id."""
        try:
            from web.rebalance_api import execute_sell
            body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
            cfg, sm, prov, executor, tl, tracker = _rebal_deps()
            return execute_sell(
                sm, cfg, prov, executor, tl, tracker,
                request_id=body.get("request_id", ""),
                preview_hash=body.get("preview_hash", ""),
            )
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.post("/api/rebalance/buy")
    async def execute_rebalance_buy_api(request: Request):
        """Execute BUY. Requires: request_id."""
        try:
            from web.rebalance_api import execute_buy
            body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
            cfg, sm, prov, executor, tl, tracker = _rebal_deps()
            return execute_buy(
                sm, cfg, prov, executor, tl, tracker,
                request_id=body.get("request_id", ""),
                preview_hash=body.get("preview_hash", ""),
            )
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.post("/api/rebalance/skip")
    async def skip_rebalance_api():
        """Skip cycle: reset to IDLE."""
        try:
            from web.rebalance_api import skip_rebalance
            from core.state_manager import StateManager
            from config import Gen4Config
            cfg = Gen4Config()
            sm = StateManager(cfg.STATE_DIR, trading_mode="live")
            return skip_rebalance(sm)
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.post("/api/rebalance/mode")
    async def set_rebalance_mode_api(request: Request):
        """Toggle auto/manual mode."""
        try:
            from web.rebalance_api import set_rebalance_mode
            from core.state_manager import StateManager
            from config import Gen4Config
            cfg = Gen4Config()
            sm = StateManager(cfg.STATE_DIR, trading_mode="live")
            body = await request.json()
            mode = body.get("mode", "manual")
            new_mode = set_rebalance_mode(mode, state_mgr=sm)
            return {"ok": True, "mode": new_mode}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.get("/api/rebalance/preview-compare")
    async def rebalance_preview_compare():
        """배치 Target vs 현재 포트폴리오 비교 + 시가 대비 등락률."""
        try:
            from pathlib import Path as _Path
            signals_dir = _Path(__file__).resolve().parent.parent / "data" / "signals"
            state_dir = _Path(__file__).resolve().parent.parent / "state"
            ohlcv_dir = _Path(__file__).resolve().parent.parent.parent / "backtest" / "data_full" / "ohlcv"
            sector_map_path = _Path(__file__).resolve().parent.parent / "data" / "sector_map.json"
            if not sector_map_path.exists():
                sector_map_path = _Path(__file__).resolve().parent.parent.parent / "backtest" / "data_full" / "sector_map.json"

            # 1. Load latest target — unified via factor_ranker.load_target_portfolio
            # which has PG fallback (hot-patch 2026-04-22, see factor_ranker.py).
            try:
                from strategy.factor_ranker import load_target_portfolio
                target = load_target_portfolio(signals_dir)
            except Exception:
                target = None
            if not target:
                return {"error": "No target portfolio found"}
            target_tickers = set(target.get("target_tickers", []))
            target_date = target.get("date", "")

            # 2. Load current holdings from state
            current_holdings = set()
            state_file = state_dir / "portfolio_state_live.json"
            if not state_file.exists():
                state_file = state_dir / "portfolio_state_paper.json"
            if state_file.exists():
                state_data = json.loads(state_file.read_text(encoding="utf-8"))
                positions = state_data.get("positions", {})
                current_holdings = set(positions.keys())

            # 3. Compare
            new_entries_codes = sorted(target_tickers - current_holdings)
            exit_codes = sorted(current_holdings - target_tickers)
            unchanged_codes = sorted(target_tickers & current_holdings)

            # 4. Load sector map for names
            sector_map = {}
            if sector_map_path.exists():
                sector_map = json.loads(sector_map_path.read_text(encoding="utf-8"))

            # 5. Get today's prices from DB (CSV fallback)
            import pandas as pd
            _db_provider = None
            try:
                from data.db_provider import DbProvider
                _db_provider = DbProvider()
            except Exception:
                pass

            def get_price_info(code):
                try:
                    if _db_provider:
                        df = _db_provider.get_ohlcv(code)
                        if not df.empty:
                            last = df.iloc[-1]
                            opn = float(last["open"])
                            cls = float(last["close"])
                            chg = (cls / opn - 1) * 100 if opn > 0 else 0
                            return {"open": int(opn), "close": int(cls), "change_pct": round(chg, 2)}
                except Exception:
                    pass
                # CSV fallback
                f = ohlcv_dir / f"{code}.csv"
                if not f.exists():
                    return {"open": 0, "close": 0, "change_pct": 0}
                try:
                    df = pd.read_csv(f, parse_dates=["date"]).sort_values("date")
                    if len(df) < 2:
                        return {"open": 0, "close": 0, "change_pct": 0}
                    last = df.iloc[-1]
                    opn = float(last.get("open", 0))
                    cls = float(last.get("close", 0))
                    chg = (cls / opn - 1) * 100 if opn > 0 else 0
                    return {"open": int(opn), "close": int(cls), "change_pct": round(chg, 2)}
                except Exception:
                    return {"open": 0, "close": 0, "change_pct": 0}

            def build_item(code):
                name = sector_map.get(code, {}).get("name", code) if isinstance(sector_map.get(code), dict) else code
                price = get_price_info(code)
                score = target.get("scores", {}).get(code, {})
                return {
                    "code": code, "name": name,
                    "open": price["open"], "close": price["close"],
                    "change_pct": price["change_pct"],
                    "mom": round(score.get("mom_12_1", 0), 4),
                    "vol": round(score.get("vol_12m", 0), 4),
                }

            new_entries = [build_item(c) for c in new_entries_codes]
            exits = [build_item(c) for c in exit_codes]
            unchanged = [build_item(c) for c in unchanged_codes]

            # 6. Rebalance timing
            rebal_state = {}
            rs_file = state_dir / "runtime_state_live.json"
            if not rs_file.exists():
                rs_file = state_dir / "runtime_state_paper.json"
            if rs_file.exists():
                try:
                    rebal_state = json.loads(rs_file.read_text(encoding="utf-8"))
                except Exception:
                    pass
            last_rebal = rebal_state.get("last_rebalance_date", "")
            # R24 (2026-04-23): compute days_since_rebal on the fly from KOSPI
            # trading calendar. Previously `rebal_state.get("days_since_rebal", 0)`
            # returned 0 (writer never populates this field) → dashboard showed
            # "D-21" permanently regardless of elapsed trading days.
            days_since = rebal_state.get("days_since_rebal")
            if days_since is None and last_rebal:
                try:
                    import pandas as _pd
                    from config import Gen4Config as _G4Cfg
                    _kdf = _pd.read_csv(_G4Cfg().INDEX_FILE)
                    # KOSPI.csv may have `date` or `index` column name
                    # depending on who last wrote it (batch vs yfinance).
                    _dcol = "date" if "date" in _kdf.columns else "index"
                    _kdf[_dcol] = _pd.to_datetime(_kdf[_dcol])
                    _last_dt = _pd.to_datetime(last_rebal, format="%Y%m%d")
                    _today_dt = _pd.Timestamp.now().normalize()
                    _trading = _kdf[
                        (_kdf[_dcol] >= _last_dt)
                        & (_kdf[_dcol] <= _today_dt)
                    ]
                    days_since = max(0, len(_trading) - 1)
                except Exception:
                    days_since = 0
            days_since = days_since or 0
            days_remaining = max(0, 21 - days_since)

            # 7. REBALANCE SCORE 계산
            n_total = max(len(current_holdings), 1)
            n_target = max(len(target_tickers), 1)

            # (1) Drift Score: 제외 종목 / 보유 종목
            drift_ratio = len(exit_codes) / n_total if current_holdings else 0
            drift_score = min(drift_ratio * 100, 100)

            # (2) Replacement Pressure: 신규 편입 / 포지션 수
            replace_ratio = len(new_entries_codes) / n_target
            replace_score = min(replace_ratio * 100, 100)

            # (3) Quality Score: 유지 종목 중 target에 남은 비율 → 이탈률
            # 유지 = target ∩ current, 전체 current 중 유지 비율
            retention = len(unchanged_codes) / n_total if current_holdings else 1.0
            quality_score = (1 - retention) * 100

            # (4) Market Stress (DD guard 기반)
            risk_mode = rebal_state.get("risk_mode", "NORMAL")
            market_scores = {"NORMAL": 0, "CAUTION": 20, "WARNING": 40,
                             "CRITICAL": 70, "SEVERE": 100}
            market_score = market_scores.get(risk_mode, 0)

            # 과매매 방지: 교체 비율 < 20% → 강제 HOLD
            force_hold = replace_ratio < 0.2 and drift_ratio < 0.2

            # 최종 점수
            rebal_score = round(
                drift_score * 0.30 +
                replace_score * 0.25 +
                quality_score * 0.30 +
                market_score * 0.15, 1)

            if force_hold:
                rebal_score = min(rebal_score, 24)  # HOLD 강제

            # 의사결정
            if rebal_score < 25:
                decision = "HOLD"
            elif rebal_score < 45:
                decision = "WATCH"
            elif rebal_score < 65:
                decision = "SOFT_REBALANCE"
            else:
                decision = "FULL_REBALANCE"

            # 사유
            reasons = []
            if drift_score >= 30:
                reasons.append("drift")
            if quality_score >= 40:
                reasons.append("score_decay")
            if market_score >= 40:
                reasons.append("market_stress")
            if replace_score >= 30:
                reasons.append("new_candidates")

            return {
                "target_date": target_date,
                "target_count": len(target_tickers),
                "current_count": len(current_holdings),
                "new_entries": new_entries,
                "exits": exits,
                "unchanged": unchanged,
                "days_remaining": days_remaining,
                "last_rebal_date": last_rebal,
                "rebal_score": {
                    "total": rebal_score,
                    "decision": decision,
                    "drift": round(drift_score, 1),
                    "replacement": round(replace_score, 1),
                    "quality": round(quality_score, 1),
                    "market": market_score,
                    "reasons": reasons,
                    "force_hold": force_hold,
                },
            }
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/profit")
    async def get_profit():
        """Profit analysis from trade logs."""
        try:
            import csv
            from datetime import datetime, timedelta
            log_dir = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "data" / "logs"
            trades_file = log_dir / "trades.csv"
            result = {"day": 0, "week": 0, "month": 0, "year": 0, "fees": 0}
            if not trades_file.exists():
                return result
            now = datetime.now()
            with open(trades_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        dt_str = row.get("datetime", row.get("date", ""))
                        pnl = float(row.get("pnl", row.get("realized_pnl", 0)) or 0)
                        fee = float(row.get("commission", row.get("fee", 0)) or 0)
                        tax = float(row.get("tax", 0) or 0)
                        dt = datetime.strptime(dt_str[:10], "%Y-%m-%d") if dt_str else now
                        delta = (now - dt).days
                        if delta <= 1:
                            result["day"] += pnl
                        if delta <= 7:
                            result["week"] += pnl
                        if delta <= 30:
                            result["month"] += pnl
                        if delta <= 365:
                            result["year"] += pnl
                        result["fees"] += fee + tax
                    except Exception:
                        continue
            return {k: round(v) for k, v in result.items()}
        except Exception as e:
            return {"day": 0, "week": 0, "month": 0, "year": 0, "fees": 0, "error": str(e)}

    # 전일종가 캐시: {code: prev_close}, 하루 1회 갱신
    _prev_close_cache: dict = {}
    _prev_close_cache_date: str = ""

    def _enrich_day_change(holdings: list, ref_date: str = None) -> None:
        """보유종목에 전일대비 등락률 추가. Observer-only, Engine 간섭 없음.

        ref_date: Lab 시뮬레이션용 — cur_price가 ref_date 종가일 때
                  ref_date 하루 전 거래일의 종가와 비교. None이면 시스템 today 사용.

        데이터 소스 우선순위 (Live 대시보드 기준):
        0. Kiwoom 응답의 prev_close_price (pred_close_pric) — authoritative source
        1. DB OHLCV (PostgreSQL) — fallback
        2. pykrx fallback (KRX 직접 조회, 하루 1회 캐싱)
        """
        codes = [h["code"] for h in holdings if h.get("code")]
        if not codes:
            return

        prev_closes = {}  # {code: float}

        # Source 0: Kiwoom 응답에 이미 포함된 prev_close_price 우선 사용
        # (pred_close_pric 필드, rest_provider에서 이미 채워줌)
        for h in holdings:
            code = h.get("code", "")
            pcp = h.get("prev_close_price", 0) or 0
            if code and pcp > 0:
                prev_closes[code] = float(pcp)

        # DB/pykrx 조회는 Kiwoom에서 빠진 종목만 (ref_date 주어진 Lab 경로는 항상 DB로)
        codes_missing = [c for c in codes if c not in prev_closes] if ref_date is None else codes

        # Source 1: DB
        try:
            from data.db_provider import DbProvider
            db = DbProvider()
            prev_data = db.get_prev_closes(codes_missing, max_stale_bdays=3, ref_date=ref_date)
            for code, info in prev_data.items():
                if info and not info.get("stale") and info.get("prev_close", 0) > 0:
                    prev_closes[code] = info["prev_close"]
        except Exception as e:
            logger.debug(f"[DAY_CHG] DB source failed: {e}")

        # Source 2: pykrx fallback (DB에 없는 종목만)
        missing = [c for c in codes if c not in prev_closes]
        if missing:
            try:
                from datetime import datetime as _dt
                today_str = _dt.now().strftime("%Y-%m-%d")

                # 하루 1회 캐싱
                if _prev_close_cache_date != today_str:
                    _enrich_day_change._cache = {}
                    _enrich_day_change._cache_date = today_str

                cache = getattr(_enrich_day_change, '_cache', {})
                uncached = [c for c in missing if c not in cache]

                if uncached:
                    from pykrx import stock
                    from datetime import timedelta
                    # 최근 5영업일 조회해서 전일 close 추출
                    end = _dt.now().strftime("%Y%m%d")
                    start = (_dt.now() - timedelta(days=10)).strftime("%Y%m%d")
                    for code in uncached:
                        try:
                            df = stock.get_market_ohlcv(start, end, code)
                            if len(df) >= 2:
                                # 마지막 행 = 오늘(장중), 그 전 행 = 전일
                                cache[code] = int(df.iloc[-2]["종가"])
                            elif len(df) == 1:
                                cache[code] = int(df.iloc[0]["종가"])
                        except Exception:
                            continue
                    _enrich_day_change._cache = cache
                    _enrich_day_change._cache_date = today_str
                    logger.info(f"[DAY_CHG] pykrx fetched {len(uncached)} codes, "
                               f"found {len(cache)} prev_closes")

                for code in missing:
                    if code in cache and cache[code] > 0:
                        prev_closes[code] = cache[code]
            except Exception as e:
                logger.warning(f"[DAY_CHG] pykrx fallback failed: {e}")

        # Enrich holdings
        for h in holdings:
            code = h.get("code", "")
            pc = prev_closes.get(code, 0)
            cp = h.get("cur_price", 0)
            if pc > 0 and cp > 0:
                h["prev_close"] = pc
                h["day_change_pct"] = round((cp - pc) / pc * 100, 2)
                h["day_change_reason"] = None
            else:
                h["prev_close"] = None
                h["day_change_pct"] = None
                h["day_change_reason"] = "no_prev_close"

    @application.get("/api/portfolio")
    async def get_portfolio():
        """Fetch live portfolio from Kiwoom REST API (kt00018)."""
        try:
            provider = _get_provider()
            summary = provider.query_account_summary()
            _enrich_day_change(summary.get("holdings", []))
            return summary
        except Exception as e:
            return {"error": str(e), "holdings_reliable": False}

    # Minute-bar chart cache (hover 요청 부하 방지)
    # {code: {"ts": epoch, "tic_scope": "1", "data": {...}}}
    _minute_chart_cache: dict = {}
    _MINUTE_CHART_TTL = 60  # seconds — 장중 1분봉은 60초 캐싱

    @application.get("/api/chart/minute/{code}")
    async def get_minute_chart(code: str, tic_scope: str = Query("1"),
                               bars: int = Query(60, ge=10, le=300)):
        """분봉 차트 조회 (ka10080). 카드 hover 시 mini chart 렌더용.

        Params:
          tic_scope: 1/3/5/10/15/30/45/60 (분)
          bars: 최근 N개 (10~300)
        """
        import time as _time
        key = f"{code}:{tic_scope}:{bars}"
        cached = _minute_chart_cache.get(key)
        if cached and (_time.time() - cached["ts"]) < _MINUTE_CHART_TTL:
            return cached["data"]
        try:
            provider = _get_provider()
            result = provider.query_minute_chart(code, tic_scope=tic_scope, max_bars=bars)
            _minute_chart_cache[key] = {"ts": _time.time(), "data": result}
            return result
        except Exception as e:
            return {"code": code, "tic_scope": tic_scope, "bars": [], "error": str(e)}

    @application.get("/api/traces")
    async def get_traces(
        limit: int = Query(100, ge=1, le=500),
        status: str = Query("", description="Filter: ok|error|timeout|retry"),
        tag: str = Query("", description="Filter by log tag substring"),
    ):
        """Filtered request traces."""
        return tracker.get_traces(limit=limit, status_filter=status, tag_filter=tag)

    @application.get("/api/latency-histogram")
    async def get_latency_histogram(
        buckets: int = Query(20, ge=5, le=50),
    ):
        """Latency distribution histogram data."""
        return tracker.get_latency_histogram(buckets=buckets)

    @application.get("/api/logs")
    async def get_logs(
        max_lines: int = Query(200, ge=10, le=1000),
    ):
        """Parsed log file entries (today)."""
        return tracker.parse_log_file(max_lines=max_lines)

    @application.get("/api/chart/today")
    async def get_chart_today():
        """KOSPI vs Portfolio intraday chart data from DB."""
        try:
            from web.dashboard_db import load_today_snapshots, get_snapshot_count_today
            snapshots = load_today_snapshots()
            return {"snapshots": snapshots, "count": len(snapshots)}
        except Exception as e:
            return {"snapshots": [], "count": 0, "error": str(e)}

    # ── Theme Detail API (마우스 오버 시 종목 상세) ────────
    _theme_detail_cache: dict = {}  # {theme_code: {"data": ..., "ts": 0}}

    @application.get("/api/theme/{theme_code}")
    async def get_theme_detail(theme_code: str):
        """ka90002 테마종목조회 — 5분 캐시."""
        now = time.time()
        cached = _theme_detail_cache.get(theme_code)
        if cached and (now - cached["ts"]) < 300:
            return cached["data"]
        try:
            provider = _get_global_provider()
            result = provider.get_theme_stocks(theme_code, date_range=1)
            if not result:
                return {"error": "no data", "stocks": []}
            # 보유종목 매칭
            pf = _portfolio_cache.get("data") or {}
            held_codes = {h.get("code", "") for h in pf.get("holdings", [])}
            for s in result.get("stocks", []):
                s["held"] = s.get("code", "") in held_codes
            _theme_detail_cache[theme_code] = {"data": result, "ts": now}
            return result
        except Exception as e:
            return {"error": str(e), "stocks": []}

    # ── Test Order (REST+COM 교차 검증용) ────────────────
    # 안전장치: 최대 3주, 테스트 전용, DB 기록, 체결 확인(ka10076)

    def _ensure_test_orders_table():
        from web.dashboard_db import _conn as _dash_conn
        c = _dash_conn()
        c.execute("""
            CREATE TABLE IF NOT EXISTS test_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT, code TEXT, side TEXT, qty INTEGER,
                order_no TEXT, exec_price REAL, exec_qty INTEGER,
                status TEXT, error TEXT, fee REAL DEFAULT 0, tax REAL DEFAULT 0
            )
        """)
        c.commit(); c.close()

    def _confirm_fill(provider, code: str, order_no: str) -> dict:
        """ka10076으로 체결 확인. Ghost fill 방지."""
        try:
            resp = provider._request("ka10076", "/api/dostk/acnt", {
                "qry_tp": "0", "sell_tp": "0", "stex_tp": "SOR",
                "ord_dt": time.strftime("%Y%m%d"),
                "cont_yn": "N", "cont_key": "",
            }, related_code="FILL_CHECK")

            if resp.get("return_code") != 0:
                return {"confirmed": False, "error": f"ka10076 rc={resp.get('return_code')}"}

            for item in resp.get("cntr", []):
                if item.get("ord_no") == order_no and code in str(item.get("stk_cd", "")):
                    return {
                        "confirmed": True,
                        "exec_price": float(item.get("cntr_pric", 0)),
                        "exec_qty": int(item.get("cntr_qty", 0)),
                        "fee": float(item.get("tdy_trde_cmsn", 0)),
                        "tax": float(item.get("tdy_trde_tax", 0)),
                        "status": item.get("ord_stt", ""),
                    }
            return {"confirmed": False, "error": "order_no not found in ka10076"}
        except Exception as e:
            return {"confirmed": False, "error": str(e)}

    def _save_test_order(code, side, qty, order_no, exec_price, exec_qty, status, error, fee=0, tax=0):
        try:
            _ensure_test_orders_table()
            from web.dashboard_db import _conn as _dash_conn
            c = _dash_conn()
            c.execute(
                "INSERT INTO test_orders (ts,code,side,qty,order_no,exec_price,exec_qty,status,error,fee,tax) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (time.strftime("%Y-%m-%dT%H:%M:%S"), code, side, qty,
                 order_no, exec_price, exec_qty, status, error, fee, tax)
            )
            c.commit(); c.close()
        except Exception:
            pass

    @application.post("/api/test/buy")
    async def test_buy(request: Request):
        """Test BUY via REST + 체결 확인(ka10076)."""
        try:
            body = await request.json()
        except Exception:
            return {"error": "Invalid JSON"}

        code = str(body.get("code", "")).strip().zfill(6)
        if not code or code == "000000":
            return {"error": "code 필수"}
        qty = int(body.get("qty", 1))
        if qty > 3:
            return {"error": "테스트 주문은 최대 3주까지만 허용"}

        try:
            provider = _get_provider()
            result = await asyncio.to_thread(provider.send_order, code, "BUY", qty, 0, "03")

            if result.get("error"):
                _save_test_order(code, "BUY", qty, "", 0, 0, "ERROR", result["error"])
                return result

            order_no = result.get("order_no", "")

            # WS 체결 안 됐으면 ka10076으로 확인
            if result.get("exec_qty", 0) == 0 and order_no:
                await asyncio.sleep(2)  # 체결 대기
                fill = await asyncio.to_thread(_confirm_fill, provider, code, order_no)
                if fill.get("confirmed"):
                    result["exec_price"] = fill["exec_price"]
                    result["exec_qty"] = fill["exec_qty"]
                    result["fee"] = fill.get("fee", 0)
                    result["tax"] = fill.get("tax", 0)
                    result["status"] = "FILLED"
                    result["confirmed_by"] = "ka10076"

            _save_test_order(code, "BUY", qty, order_no,
                           result.get("exec_price", 0), result.get("exec_qty", 0),
                           result.get("status", "SUBMITTED"), result.get("error", ""),
                           result.get("fee", 0), result.get("tax", 0))
            return result
        except Exception as e:
            return {"error": str(e)}

    @application.post("/api/test/sell")
    async def test_sell(request: Request):
        """Test SELL via REST + 체결 확인(ka10076)."""
        try:
            body = await request.json()
        except Exception:
            return {"error": "Invalid JSON"}

        code = str(body.get("code", "")).strip().zfill(6)
        if not code or code == "000000":
            return {"error": "code 필수"}
        qty = int(body.get("qty", 1))
        if qty > 3:
            return {"error": "테스트 주문은 최대 3주까지만 허용"}

        try:
            provider = _get_provider()
            result = await asyncio.to_thread(provider.send_order, code, "SELL", qty, 0, "03")

            if result.get("error"):
                _save_test_order(code, "SELL", qty, "", 0, 0, "ERROR", result["error"])
                return result

            order_no = result.get("order_no", "")

            if result.get("exec_qty", 0) == 0 and order_no:
                await asyncio.sleep(2)
                fill = await asyncio.to_thread(_confirm_fill, provider, code, order_no)
                if fill.get("confirmed"):
                    result["exec_price"] = fill["exec_price"]
                    result["exec_qty"] = fill["exec_qty"]
                    result["fee"] = fill.get("fee", 0)
                    result["tax"] = fill.get("tax", 0)
                    result["status"] = "FILLED"
                    result["confirmed_by"] = "ka10076"

            _save_test_order(code, "SELL", qty, order_no,
                           result.get("exec_price", 0), result.get("exec_qty", 0),
                           result.get("status", "SUBMITTED"), result.get("error", ""),
                           result.get("fee", 0), result.get("tax", 0))
            return result
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/test/orders")
    async def test_orders():
        """List test order history."""
        try:
            _ensure_test_orders_table()
            from web.dashboard_db import _conn as _dash_conn
            c = _dash_conn()
            rows = c.execute("SELECT * FROM test_orders ORDER BY id DESC LIMIT 20").fetchall()
            c.close()
            return [dict(r) for r in rows]
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/report/today")
    async def get_report_today():
        """Today's REST EOD report."""
        from datetime import date as _d
        today = _d.today().strftime("%Y%m%d")
        report_path = Path(__file__).resolve().parent.parent / "report" / "output" / f"rest_daily_{today}.html"
        if report_path.exists():
            from fastapi.responses import HTMLResponse
            return HTMLResponse(report_path.read_text("utf-8"))
        return {"error": "리포트 미생성. 15:40 이후 자동 생성됩니다."}

    @application.get("/api/crosscheck/today")
    async def get_crosscheck():
        """COM vs REST cross-validation result."""
        try:
            from web.cross_validator import compare_engine_vs_broker
            provider = _get_provider()
            result = await asyncio.to_thread(compare_engine_vs_broker, provider)
            return result
        except Exception as e:
            return {"error": str(e)}

    @application.post("/api/alert/test")
    async def test_alert():
        """Send test Telegram message."""
        try:
            from notify.telegram_bot import send
            ok = send("Q-TRON REST 텔레그램 테스트 알림", "INFO")
            return {"ok": ok}
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/batch/status")
    async def batch_status():
        """오늘 KST 기준 KR batch 완료 여부 (target_portfolio 파일 존재 확인)."""
        try:
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo
            kst_today = _dt.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
            signals_dir = Path(__file__).resolve().parent.parent / "data" / "signals"
            target_file = signals_dir / f"target_portfolio_{kst_today}.json"
            done = target_file.exists()
            return {"kr_done": done, "kr_date": kst_today if done else None}
        except Exception as e:
            return {"kr_done": False, "kr_date": None, "error": str(e)}

    @application.get("/api/state")
    async def get_state_snapshot():
        """초기 렌더링용 캐시 스냅샷. SSE 연결 전 빈 화면 제거용."""
        if not _portfolio_cache.get("data"):
            return {
                "snapshot_id": _payload_seq["n"],
                "loading": True,
                "_data_source": "UI_CACHE",
            }
        cache = dict(_portfolio_cache)
        snap = tracker.snapshot()
        snap["account"]  = cache["data"]
        snap["dd_guard"] = cache.get("dd_guard")
        snap["recon"]    = cache.get("recon")
        _ts = cache.get("ts")
        _age = (time.time() - _ts) if _ts else None
        snap["cache_age_sec"] = round(max(0.0, _age), 1) if _age is not None else None
        snap["snapshot_id"]   = _payload_seq["n"]
        snap["_data_source"]  = "UI_CACHE"
        # P2: auto trading state (advisory read-only)
        try:
            from kr.risk.auto_trading_gate import compute_auto_trading_state
            from kr.risk.strategy_health import compute_strategy_health
            _guard = getattr(application.state, "guard", None)
            _runtime = (cache.get("runtime") or {})
            _equity_dd = float((cache.get("dd_guard") or {}).get("equity_dd_pct", 0.0) or 0.0)
            _health = compute_strategy_health(equity_dd_pct=_equity_dd)
            _auto = compute_auto_trading_state(
                guard=_guard, runtime=_runtime,
                strategy_health=_health,
            )
            snap["auto_trading"] = _auto.to_dict()
            snap["strategy_health"] = _health
        except Exception as _e:
            snap["auto_trading"] = {"enabled": False, "blockers": [f"EVAL_ERROR:{type(_e).__name__}"],
                                    "reason_summary": "eval_error"}

        # theme_regime fallback — SSE 10min cycle 에 의존하지 않고 DB 에서 직접 로드.
        # cache 우선, 비었으면 PG regime_theme_daily 오늘자 조회.
        try:
            if not snap.get("theme_regime"):
                _tr_cached = _portfolio_cache.get("theme_regime")
                if _tr_cached:
                    snap["theme_regime"] = _tr_cached
                else:
                    snap["theme_regime"] = _load_theme_regime_from_db() or []
        except Exception as _tr_err:
            logging.getLogger("web").debug(f"theme_regime fallback: {_tr_err}")

        # ENGINE_OFFLINE safety gate — shared with /sse/state via helper.
        _apply_engine_offline_override(snap)
        return snap

    @application.get("/api/health")
    async def get_health():
        """Quick health check endpoint.

        ENGINE_OFFLINE overrides tracker health: the REST layer can be
        perfectly healthy while the live engine is not running, and that
        case must surface as RED — not YELLOW or GREEN — so external
        monitors (Telegram alerts, uptime checks, tray icon) treat it
        as a real outage.
        """
        snap = tracker.snapshot()
        status = snap["health"]["status"]
        reason = snap["health"]["reason"]
        engine = _detect_engine_status()
        if engine["status"] == "OFFLINE":
            status = "RED"
            reason = f"ENGINE_OFFLINE: {engine['reason']}"
        return {
            "status": status,
            "reason": reason,
            "timestamp": snap["timestamp_str"],
            "engine_status": engine["status"],
        }

    @application.get("/api/advisor/today")
    async def advisor_today():
        """Today's advisor analysis results.

        Priority: ENGINE_OFFLINE > STALE > NORMAL.

        Stale handling: the prior implementation silently fell back to
        the "latest available" directory when today/yesterday had no
        output. That surfaced 20-day-old alerts on the dashboard as if
        they were current — SAFE_MODE / RECON corrections / pending
        orders from 2026-04-01 on a 2026-04-21 screen — the exact
        operator-misjudgment class we are trying to eliminate. When
        last_run_date is more than 3 days old we now return status=STALE
        with alerts=[] and a single message pointing at the date, so
        nothing looks like a live warning.

        Engine gate: when the live engine is OFFLINE (see
        _detect_engine_status) the entire advisor is paused regardless
        of file freshness. Any advisor run from before the outage is
        operating on stale assumptions — treat as DISABLED.
        """
        try:
            from datetime import datetime, timedelta

            # 1) Engine gate takes priority — advisor output is meaningless
            #    while the engine is not running. Return DISABLED even if
            #    today's file exists (might have been produced by a cron
            #    that does not know engine is down).
            engine = _detect_engine_status()
            if engine.get("status") == "OFFLINE":
                return {
                    "status": "DISABLED",
                    "alerts": [],
                    "recommendations": [],
                    "message": "AI ADVISOR paused (engine offline)",
                    "engine_offline": True,
                    "engine_reason": engine.get("reason"),
                }

            today = datetime.now().strftime("%Y%m%d")
            advisor_dir = _Path(__file__).resolve().parent.parent / "advisor" / "output" / today
            if not advisor_dir.exists():
                # Try yesterday
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
                advisor_dir = _Path(__file__).resolve().parent.parent / "advisor" / "output" / yesterday
            if not advisor_dir.exists():
                # Find latest
                out_dir = _Path(__file__).resolve().parent.parent / "advisor" / "output"
                if out_dir.exists():
                    dirs = sorted([d for d in out_dir.iterdir() if d.is_dir()], reverse=True)
                    advisor_dir = dirs[0] if dirs else None
            if not advisor_dir or not advisor_dir.exists():
                return {"status": "NO_DATA", "alerts": [], "recommendations": []}

            # 2) Compute staleness from directory name (YYYYMMDD)
            stale_days = None
            last_run_iso = advisor_dir.name  # fallback
            try:
                last_run_date = datetime.strptime(advisor_dir.name, "%Y%m%d").date()
                stale_days = (datetime.now().date() - last_run_date).days
                last_run_iso = last_run_date.isoformat()
            except ValueError:
                pass

            # 3) Older than 3 days → STALE. Empty alerts/recs so the
            #    dashboard cannot render them as current warnings.
            if stale_days is not None and stale_days > 3:
                return {
                    "status": "STALE",
                    "alerts": [],
                    "recommendations": [],
                    "date": advisor_dir.name,
                    "last_run_date": last_run_iso,
                    "stale_days": stale_days,
                    "message": f"AI ADVISOR unavailable — last run: {last_run_iso}",
                }

            # 4) Fresh enough — return live data.
            result = {
                "status": "OK",
                "date": advisor_dir.name,
                "last_run_date": last_run_iso,
                "stale_days": stale_days,
            }
            for fname in ["alerts.json", "recommendations.json", "daily_analysis.json"]:
                fpath = advisor_dir / fname
                if fpath.exists():
                    result[fname.replace(".json", "")] = json.loads(
                        fpath.read_text(encoding="utf-8"))
            return result
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}

    @application.get("/api/db/health")
    async def db_health():
        """PostgreSQL DB health check."""
        try:
            from data.db_provider import get_conn
            conn = get_conn()
            cur = conn.cursor()
            tables = []
            for t in ['ohlcv', 'fundamental', 'target_portfolio', 'sector_map',
                       'kospi_index', 'trades', 'equity_history', 'portfolio_snapshot']:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    cnt = cur.fetchone()[0]
                    if t != 'sector_map':
                        cur.execute(f"SELECT MAX(date) FROM {t}")
                        latest = str(cur.fetchone()[0] or "")
                    else:
                        latest = f"{cnt} stocks"
                    tables.append({
                        "table": t, "rows": cnt, "latest": latest,
                        "status": "OK" if cnt > 0 else "EMPTY",
                    })
                except Exception as e:
                    tables.append({"table": t, "rows": 0, "latest": "", "status": f"ERROR: {e}"})
            cur.execute("SELECT pg_size_pretty(pg_database_size('qtron'))")
            db_size = cur.fetchone()[0]
            cur.close()
            conn.close()
            return {"status": "OK", "db_size": db_size, "tables": tables}
        except Exception as e:
            return {"status": "ERROR", "error": str(e), "tables": []}

    @application.get("/api/trades/recent")
    async def get_recent_trades(limit: int = Query(10, ge=1, le=50)):
        """Recent trades from trades.csv (legacy)."""
        return _read_recent_trades(limit=limit)

    # ── Trades / Positions / Export / Charts (PG read-only) ──

    @application.get("/api/trades")
    async def get_trades_pg(
        start: str = Query("", description="YYYY-MM-DD"),
        end: str = Query("", description="YYYY-MM-DD"),
        code: str = Query("", description="종목코드"),
        side: str = Query("", description="BUY|SELL"),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        """거래 내역 조회 (LIVE → Lab fallback)."""
        # LIVE trades
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                where, params = ["mode='LIVE'"], []
                if start:
                    where.append("date >= %s"); params.append(start)
                if end:
                    where.append("date <= %s"); params.append(end)
                if code:
                    where.append("code = %s"); params.append(code)
                if side:
                    where.append("side = %s"); params.append(side.upper())
                w = " AND ".join(where)
                cur.execute(f"SELECT COUNT(*) FROM report_trades WHERE {w}", params)
                total = cur.fetchone()[0]
                if total > 0:
                    cur.execute(
                        f"SELECT id,date,code,side,quantity,price,cost,slippage_pct,created_at "
                        f"FROM report_trades WHERE {w} ORDER BY date DESC, id DESC "
                        f"LIMIT %s OFFSET %s", params + [limit, offset])
                    cols = [d[0] for d in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                    cur.close()
                    return {"total": total, "limit": limit, "offset": offset, "trades": rows}
                cur.close()
        except Exception:
            pass
        # Lab fallback
        try:
            import json as _json
            from pathlib import Path as _P
            t_path = _P(__file__).resolve().parent.parent / "data" / "lab_live" / "trades.json"
            t_data = _json.loads(t_path.read_text(encoding="utf-8"))
            t_list = t_data.get("trades", [])
            # Filter
            if code:
                t_list = [t for t in t_list if t.get("ticker") == code]
            if side:
                s = side.upper()
                if s == "SELL":
                    t_list = [t for t in t_list if t.get("exit_date")]
                elif s == "BUY":
                    t_list = [t for t in t_list if t.get("entry_date")]
            if start:
                t_list = [t for t in t_list if t.get("exit_date", t.get("entry_date", "")) >= start]
            if end:
                t_list = [t for t in t_list if t.get("exit_date", t.get("entry_date", "")) <= end]
            total = len(t_list)
            rows = []
            for t in t_list[offset:offset+limit]:
                rows.append({
                    "date": t.get("exit_date", t.get("entry_date", "")),
                    "code": t.get("ticker", ""),
                    "side": "SELL" if t.get("exit_reason") else "BUY",
                    "quantity": t.get("qty", 0),
                    "price": t.get("exit_price", t.get("entry_price", 0)),
                    "cost": abs(t.get("pnl_amount", 0)),
                    "strategy": t.get("strategy", ""),
                    "exit_reason": t.get("exit_reason", ""),
                    "pnl_pct": t.get("pnl_pct", 0),
                })
            return {"total": total, "limit": limit, "offset": offset, "trades": rows, "source": "lab"}
        except Exception as e:
            return {"error": str(e), "trades": []}

    @application.get("/api/trades/summary")
    async def get_trades_summary(
        start: str = Query("", description="YYYY-MM-DD"),
        end: str = Query("", description="YYYY-MM-DD"),
    ):
        """거래 통계 요약 (LIVE → Lab fallback)."""
        # Lab fallback first check
        try:
            import json as _json
            from pathlib import Path as _P
            t_path = _P(__file__).resolve().parent.parent / "data" / "lab_live" / "trades.json"
            t_data = _json.loads(t_path.read_text(encoding="utf-8"))
            t_list = t_data.get("trades", [])
            if t_list:
                closed = [t for t in t_list if t.get("exit_date") and t.get("pnl_pct") is not None]
                wins = sum(1 for t in closed if t["pnl_pct"] > 0)
                avg_pnl = sum(t["pnl_pct"] for t in closed) / len(closed) if closed else 0
                avg_hold = 0
                for t in closed:
                    try:
                        from datetime import datetime as _dt
                        d1 = _dt.strptime(t["entry_date"], "%Y-%m-%d")
                        d2 = _dt.strptime(t["exit_date"], "%Y-%m-%d")
                        avg_hold += (d2 - d1).days
                    except Exception:
                        pass
                avg_hold = avg_hold / len(closed) if closed else 0
                return {
                    "buy_count": len(t_list),
                    "sell_count": len(closed),
                    "closed_trades": len(closed),
                    "win_rate": round(wins / len(closed) * 100, 1) if closed else 0,
                    "avg_pnl_pct": round(avg_pnl, 2),
                    "avg_hold_days": round(avg_hold, 1),
                    "source": "lab",
                }
        except Exception:
            pass
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                w, p = "mode='LIVE'", []
                if start:
                    w += " AND date >= %s"; p.append(start)
                if end:
                    w += " AND date <= %s"; p.append(end)
                cur.execute(f"SELECT COUNT(*) FILTER (WHERE side='BUY'), "
                            f"COUNT(*) FILTER (WHERE side='SELL'), "
                            f"COALESCE(SUM(cost),0) FROM report_trades WHERE {w}", p)
                buy_cnt, sell_cnt, total_cost = cur.fetchone()
                # close_log 승률
                cw = "mode='LIVE'"
                cp = []
                if start:
                    cw += " AND date >= %s"; cp.append(start)
                if end:
                    cw += " AND date <= %s"; cp.append(end)
                cur.execute(f"SELECT COUNT(*), "
                            f"COUNT(*) FILTER (WHERE pnl_pct > 0), "
                            f"COALESCE(AVG(pnl_pct),0), "
                            f"COALESCE(AVG(hold_days),0) "
                            f"FROM report_close_log WHERE {cw}", cp)
                closes, wins, avg_pnl, avg_hold = cur.fetchone()
                cur.close()
            win_rate = round(wins / closes * 100, 1) if closes > 0 else 0
            return {
                "buy_count": buy_cnt or 0, "sell_count": sell_cnt or 0,
                "total_cost": round(total_cost or 0, 0),
                "closed_trades": closes or 0, "win_rate": win_rate,
                "avg_pnl_pct": round(avg_pnl or 0, 2),
                "avg_hold_days": round(avg_hold or 0, 1),
            }
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/positions/{code}/history")
    async def get_position_history(code: str, days: int = Query(30, ge=1, le=365)):
        """종목별 일별 포지션 이력 (PG report_daily_positions)."""
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT date,quantity,avg_price,current_price,market_value,"
                    "pnl_pct,high_watermark,trail_stop_price,hold_days "
                    "FROM report_daily_positions "
                    "WHERE code=%s AND mode='LIVE' ORDER BY date DESC LIMIT %s",
                    (code, days))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                cur.close()
            return {"code": code, "history": rows}
        except Exception as e:
            return {"code": code, "error": str(e), "history": []}

    @application.get("/api/positions/{code}/closes")
    async def get_position_closes(code: str):
        """종목별 청산 이력 (PG report_close_log)."""
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT date,exit_reason,quantity,entry_price,exit_price,"
                    "entry_date,hold_days,pnl_pct,pnl_amount,max_hwm_pct "
                    "FROM report_close_log "
                    "WHERE code=%s AND mode='LIVE' ORDER BY date DESC",
                    (code,))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                cur.close()
            return {"code": code, "closes": rows}
        except Exception as e:
            return {"code": code, "error": str(e), "closes": []}

    # ── Capital Events API (2026-04-20 추가) ─────────────────────
    # 실계좌 입금/출금/배당 등 외부 자본 이동 기록.
    # 누적 수익률 계산 시 입금을 "수익" 으로 오인하지 않도록 보정에 사용.
    # See: kr/finance/capital_events.py

    @application.post("/api/capital/events")
    async def capital_events_create(request: Request):
        """Record a capital event. Body:
          {"mode":"live","market":"KR","event_date":"2026-05-04",
           "event_type":"deposit","amount":30000000,"currency":"KRW",
           "note":"5월 리밸 전 추가 입금"}
        """
        try:
            from finance.capital_events import record_event
            body = await request.json()
            rid = record_event(
                mode=body.get("mode", "live"),
                market=body.get("market", "KR"),
                event_date=body["event_date"],
                event_type=body["event_type"],
                amount=float(body["amount"]),
                currency=body.get("currency", "KRW"),
                note=body.get("note", ""),
                recorded_by=body.get("recorded_by", "jeff"),
                source=body.get("source", "manual"),
                external_ref=body.get("external_ref"),
            )
            return {"ok": True, "id": rid}
        except (KeyError, ValueError) as e:
            return {"ok": False, "error": f"invalid input: {e}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.get("/api/capital/events")
    async def capital_events_list(
        mode: str | None = Query(None),
        market: str | None = Query(None),
        date_from: str | None = Query(None),
        date_to: str | None = Query(None),
        limit: int = Query(500, ge=1, le=5000),
    ):
        """List capital events with optional filters."""
        try:
            from finance.capital_events import list_events
            rows = list_events(
                mode=mode, market=market,
                date_from=date_from, date_to=date_to, limit=limit,
            )
            total_signed = sum(r["signed_amount"] for r in rows)
            return {"ok": True, "count": len(rows), "total_signed": round(total_signed, 2), "rows": rows}
        except Exception as e:
            return {"ok": False, "error": str(e), "rows": []}

    @application.get("/api/capital/summary")
    async def capital_summary(
        mode: str = Query("live"),
        market: str = Query("KR"),
        baseline_date: str | None = Query(None,
            description="시작일 (없으면 전체). e.g. 2026-04-01"),
        as_of_date: str | None = Query(None,
            description="기준일 (없으면 today)"),
    ):
        """
        Cumulative capital events summary.
        Returns: {cumulative_by_date, net_total, deposits, withdraws, etc.}
        Used for equity adjustment.
        """
        try:
            from datetime import date as _d
            from finance.capital_events import cumulative_by_date, list_events
            df = baseline_date or "1970-01-01"
            dt = as_of_date or _d.today().strftime("%Y-%m-%d")
            cum = cumulative_by_date(
                mode=mode, market=market, date_from=df, date_to=dt,
            )
            rows = list_events(
                mode=mode, market=market, date_from=df, date_to=dt,
                limit=5000,
            )
            # per type totals
            by_type: dict[str, float] = {}
            for r in rows:
                by_type[r["event_type"]] = by_type.get(r["event_type"], 0.0) + r["signed_amount"]
            return {
                "ok": True,
                "mode": mode, "market": market,
                "baseline_date": df, "as_of_date": dt,
                "event_count": len(rows),
                "cumulative_by_date": cum,
                "net_total": cum[max(cum.keys())] if cum else 0.0,
                "by_type": {k: round(v, 2) for k, v in by_type.items()},
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.get("/api/charts/equity-unified")
    async def get_equity_unified(days: int = Query(90, ge=7, le=730)):
        """Unified equity curve: Gen4 LIVE + 9 strategies + KOSPI, same baseline.

        Baseline = first Gen4 LIVE date (real-money start). All series are
        normalized as %-change from that date so 11 lines share one axis.
        Frontend picks Top-3 by latest cumul_return as default-visible along
        with LIVE + KOSPI; remaining strategies are toggleable via checkbox.
        """
        from shared.db.pg_base import connection
        import json as _json
        from pathlib import Path as _P

        try:
            # 1. LIVE equity (primary baseline source)
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT r.date, r.equity, "
                    "COALESCE(NULLIF(r.kospi_close, 0), k.close_price) AS kospi_close "
                    "FROM report_equity_log r "
                    "LEFT JOIN kospi_index k ON r.date::date = k.date "
                    "WHERE r.mode='LIVE' ORDER BY r.date ASC"
                )
                live_rows = cur.fetchall()
                cur.close()
            live_map = {str(r[0]): float(r[1]) for r in live_rows}
            live_kospi_map = {
                str(r[0]): float(r[2]) for r in live_rows if r[2] is not None
            }

            if not live_rows:
                return {"error": "no_live_data", "series": {}, "dates": []}

            baseline_date = str(live_rows[0][0])

            # 2. Date universe: from baseline through latest LIVE date, clipped to `days`
            dates_sorted = sorted(live_map.keys())
            if days and len(dates_sorted) > days:
                dates_sorted = dates_sorted[-days:]
            baseline_date = dates_sorted[0]

            # 3. KOSPI close map over the same range
            kospi_map: dict = {}
            try:
                with connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT date, close_price FROM kospi_index "
                        "WHERE date >= %s ORDER BY date",
                        (baseline_date,),
                    )
                    for r in cur.fetchall():
                        kospi_map[str(r[0])] = float(r[1])
                    cur.close()
            except Exception:
                pass
            # Merge live_kospi_map fills (R21 already joins, but keep both as safety)
            for d, v in live_kospi_map.items():
                kospi_map.setdefault(d, v)

            # 4. Lab 9-strategy equity from lab_live/equity.json
            strategies = [
                "breakout_trend", "hybrid_qscore", "liquidity_signal",
                "lowvol_momentum", "mean_reversion", "momentum_base",
                "quality_factor", "sector_rotation", "vol_regime",
            ]
            lab_map: dict = {s: {} for s in strategies}
            try:
                eq_path = (
                    _P(__file__).resolve().parent.parent
                    / "data" / "lab_live" / "equity.json"
                )
                eq_data = _json.loads(eq_path.read_text(encoding="utf-8"))
                # Keep only the last row per (date, strategy) — equity.json contains
                # historical seeds (100_000_000 resets) plus post-run commits.
                for row in eq_data.get("rows", []):
                    dt = row.get("date", "")
                    if not dt or dt < baseline_date:
                        continue
                    for s in strategies:
                        if s in row:
                            try:
                                v = float(row[s])
                            except (TypeError, ValueError):
                                continue
                            if v > 0:
                                lab_map[s][dt] = v
            except Exception:
                pass

            # 5. Normalize all series to % from baseline_date
            def _pct_series(values: dict, dates: list) -> list:
                base = values.get(baseline_date)
                if base is None:
                    # fallback: earliest available value within range
                    for d in dates:
                        if values.get(d) is not None:
                            base = values[d]
                            break
                out = []
                for d in dates:
                    v = values.get(d)
                    if v is None or base is None or base == 0:
                        out.append(None)
                    else:
                        out.append(round((v / base - 1) * 100, 3))
                return out

            series: dict = {}
            series["live"] = {
                "label": "Gen4 LIVE",
                "kind": "live",
                "pct": _pct_series(live_map, dates_sorted),
            }
            series["kospi"] = {
                "label": "KOSPI",
                "kind": "benchmark",
                "pct": _pct_series(kospi_map, dates_sorted),
            }
            for s in strategies:
                series[s] = {
                    "label": s,
                    "kind": "strategy",
                    "pct": _pct_series(lab_map[s], dates_sorted),
                }

            return {
                "days": days,
                "baseline_date": baseline_date,
                "dates": dates_sorted,
                "series": series,
            }
        except Exception as e:
            return {"error": str(e), "series": {}, "dates": []}

    @application.get("/api/charts/equity")
    async def get_equity_chart(days: int = Query(90, ge=7, le=730)):
        """Equity curve 데이터. LIVE 없으면 Lab Forward Trading 합산.

        R21 (2026-04-23): LIVE 경로 `report_equity_log.kospi_close` 가 0 으로
        저장되는 알려진 이슈 — writer 미구현. `kospi_index` 테이블에서 LEFT
        JOIN 으로 실제 값 채워넣어 dashboard KOSPI 비교선 정상 표시.
        """
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                # R21: LEFT JOIN kospi_index to replace zero/null kospi_close.
                # COALESCE prefers the joined value over the log's possibly-0 value.
                cur.execute(
                    "SELECT r.date, r.equity, r.cash, r.n_positions, "
                    "r.daily_pnl_pct, "
                    "COALESCE(NULLIF(r.kospi_close, 0), k.close_price) "
                    "  AS kospi_close, "
                    "r.regime "
                    "FROM report_equity_log r "
                    "LEFT JOIN kospi_index k ON r.date::date = k.date "
                    "WHERE r.mode='LIVE' ORDER BY r.date DESC LIMIT %s",
                    (days,),
                )
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                cur.close()
            # R22 (2026-04-23): single LIVE row collapses chart to 1 point,
            # so require >=2. Prior threshold of 7 was too aggressive — it
            # kept the "Portfolio" chart silently fed by Lab Forward 9전략
            # aggregate for days after LIVE began (incident 2026-04-24: Jeff
            # saw Portfolio=+1.5% while real Gen4 LIVE was -0.43%). The
            # "Equity Curve" card is intended to show real Gen4 LIVE capital
            # vs KOSPI, so any >=2-row LIVE series wins over the paper fallback.
            if rows and len(rows) >= 2:
                rows.reverse()
                return {"days": days, "data": rows, "source": "live"}
        except Exception:
            pass

        # Fallback: Lab Forward Trading equity (A군 합산)
        try:
            import json as _json
            from pathlib import Path as _P
            eq_path = _P(__file__).resolve().parent.parent / "data" / "lab_live" / "equity.json"
            eq_data = _json.loads(eq_path.read_text(encoding="utf-8"))
            eq_rows = eq_data.get("rows", [])
            # 날짜별 최신 row만 (중복 제거) + A군만 합산
            a_strats = ["breakout_trend", "hybrid_qscore", "liquidity_signal",
                        "lowvol_momentum", "mean_reversion", "momentum_base",
                        "quality_factor", "sector_rotation", "vol_regime"]
            seen = {}
            for row in eq_rows:
                dt = row.get("date", "")
                if not dt:
                    continue
                total = sum(float(row.get(s, 0)) for s in a_strats if s in row)
                if total > 0:
                    seen[dt] = total
            # KOSPI 종가 로드
            kospi_map = {}
            try:
                from shared.db.pg_base import connection as _conn
                with _conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT date, close_price FROM kospi_index ORDER BY date")
                    for r in cur.fetchall():
                        kospi_map[str(r[0])] = float(r[1])
                    cur.close()
            except Exception:
                pass
            result = []
            dates_sorted = sorted(seen.keys())[-days:]
            initial = seen.get(dates_sorted[0], 9e8) if dates_sorted else 9e8
            for dt in dates_sorted:
                eq = seen[dt]
                pnl = (eq / initial - 1) * 100 if initial > 0 else 0
                result.append({
                    "date": dt, "equity": round(eq),
                    "daily_pnl_pct": round(pnl, 2),
                    "n_positions": 0, "cash": 0,
                    "kospi_close": kospi_map.get(dt),
                    "regime": None,
                })
            return {"days": days, "data": result, "source": "lab_forward"}
        except Exception as e:
            return {"error": str(e), "data": []}

    @application.get("/api/charts/lab-comparison")
    async def get_lab_comparison(days: int = Query(30, ge=7, le=365)):
        """Lab 9전략 비교 데이터 (PG meta_strategy_daily + meta_strategy_risk)."""
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                # 전략별 cumul_return 시계열
                cur.execute(
                    "SELECT trade_date, strategy, cumul_return, daily_return "
                    "FROM meta_strategy_daily "
                    "ORDER BY trade_date DESC LIMIT %s",
                    (days * 9,))
                cols = [d[0] for d in cur.description]
                daily_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                # 전략별 최신 리스크
                cur.execute(
                    "SELECT DISTINCT ON (strategy) strategy, daily_mdd, "
                    "realized_vol_20d, hit_rate_20d "
                    "FROM meta_strategy_risk "
                    "ORDER BY strategy, trade_date DESC")
                rcols = [d[0] for d in cur.description]
                risk_rows = [dict(zip(rcols, r)) for r in cur.fetchall()]
                cur.close()
            # 전략별 그룹핑
            strategies = {}
            for r in daily_rows:
                s = r["strategy"]
                if s not in strategies:
                    strategies[s] = []
                strategies[s].append({
                    "date": r["trade_date"],
                    "cumul": r["cumul_return"],
                    "daily": r["daily_return"],
                })
            for s in strategies:
                strategies[s].reverse()
            risk_map = {r["strategy"]: r for r in risk_rows}
            return {"strategies": strategies, "risk": risk_map}
        except Exception as e:
            return {"error": str(e), "strategies": {}, "risk": {}}

    @application.get("/api/export/trades")
    async def export_trades_csv(
        start: str = Query("", description="YYYY-MM-DD"),
        end: str = Query("", description="YYYY-MM-DD"),
    ):
        """거래 내역 CSV 다운로드."""
        import io, csv as _csv
        from starlette.responses import StreamingResponse
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                w, p = "mode='LIVE'", []
                if start:
                    w += " AND date >= %s"; p.append(start)
                if end:
                    w += " AND date <= %s"; p.append(end)
                cur.execute(
                    f"SELECT date,code,side,quantity,price,cost,slippage_pct,created_at "
                    f"FROM report_trades WHERE {w} ORDER BY date DESC, id DESC", p)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                cur.close()
            buf = io.StringIO()
            writer = _csv.writer(buf)
            writer.writerow(cols)
            for r in rows:
                writer.writerow([str(v) if v is not None else "" for v in r])
            buf.seek(0)
            fn = f"trades_{start or 'all'}_{end or 'all'}.csv"
            return StreamingResponse(
                iter([buf.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={fn}"},
            )
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/export/equity")
    async def export_equity_csv():
        """Equity 이력 CSV 다운로드."""
        import io, csv as _csv
        from starlette.responses import StreamingResponse
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT date,equity,cash,n_positions,daily_pnl_pct,"
                    "kospi_close,regime,mode,created_at "
                    "FROM report_equity_log WHERE mode='LIVE' ORDER BY date")
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                cur.close()
            buf = io.StringIO()
            writer = _csv.writer(buf)
            writer.writerow(cols)
            for r in rows:
                writer.writerow([str(v) if v is not None else "" for v in r])
            buf.seek(0)
            return StreamingResponse(
                iter([buf.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=equity_history.csv"},
            )
        except Exception as e:
            return {"error": str(e)}

    # ── Risk Metrics / Rebalance History / Alert History ──

    @application.get("/api/risk/metrics")
    async def get_risk_metrics(days: int = Query(60, ge=7, le=730)):
        """Sharpe, MDD, Sortino 등 리스크 지표 (LIVE → Lab fallback)."""
        rows = []
        try:
            from shared.db.pg_base import connection
            import math
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT date, equity, daily_pnl_pct, kospi_close "
                    "FROM report_equity_log "
                    "WHERE mode='LIVE' ORDER BY date DESC LIMIT %s", (days,))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                cur.close()
        except Exception:
            pass

        # Lab fallback: equity.json → 합산 equity → daily return 계산
        if len(rows) < 2:
            try:
                import json as _json, math
                from pathlib import Path as _P
                eq_path = _P(__file__).resolve().parent.parent / "data" / "lab_live" / "equity.json"
                eq_data = _json.loads(eq_path.read_text(encoding="utf-8"))
                a_strats = ["breakout_trend", "hybrid_qscore", "liquidity_signal",
                            "lowvol_momentum", "mean_reversion", "momentum_base",
                            "quality_factor", "sector_rotation", "vol_regime"]
                seen = {}
                for row in eq_data.get("rows", []):
                    dt = row.get("date", "")
                    if not dt:
                        continue
                    total = sum(float(row.get(s, 0)) for s in a_strats if s in row)
                    if total > 0:
                        seen[dt] = total
                # KOSPI
                kospi_map = {}
                try:
                    from shared.db.pg_base import connection as _conn
                    with _conn() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT date, close_price FROM kospi_index ORDER BY date")
                        for r in cur.fetchall():
                            kospi_map[str(r[0])] = float(r[1])
                        cur.close()
                except Exception:
                    pass
                dates_sorted = sorted(seen.keys())[-days:]
                rows = []
                prev_eq = None
                for dt in dates_sorted:
                    eq = seen[dt]
                    pnl = ((eq / prev_eq) - 1) if prev_eq and prev_eq > 0 else 0
                    rows.append({"date": dt, "equity": eq, "daily_pnl_pct": pnl,
                                 "kospi_close": kospi_map.get(dt)})
                    prev_eq = eq
            except Exception:
                pass

        if len(rows) < 2:
            return {"error": "insufficient data", "count": len(rows)}

        try:
            import math
            returns = [r["daily_pnl_pct"] for r in rows if r["daily_pnl_pct"] is not None]
            equities = [r["equity"] for r in rows if r["equity"] is not None]
            kospi = [r["kospi_close"] for r in rows if r["kospi_close"] is not None]

            if len(returns) < 2:
                return {"error": "insufficient return data"}

            mean_r = sum(returns) / len(returns)
            std_r = (sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)) ** 0.5
            sharpe = (mean_r / std_r * (252 ** 0.5)) if std_r > 0 else 0

            neg_returns = [r for r in returns if r < 0]
            if neg_returns:
                down_dev = (sum(r ** 2 for r in neg_returns) / len(neg_returns)) ** 0.5
                sortino = (mean_r / down_dev * (252 ** 0.5)) if down_dev > 0 else 0
            else:
                sortino = 0

            peak = equities[0]
            max_dd = 0
            for eq in equities:
                if eq > peak:
                    peak = eq
                dd = (eq - peak) / peak if peak > 0 else 0
                if dd < max_dd:
                    max_dd = dd

            if len(equities) >= 2 and equities[0] > 0:
                total_return = equities[-1] / equities[0]
                years = len(equities) / 252
                cagr = (total_return ** (1 / years) - 1) if years > 0 else 0
            else:
                cagr = 0

            wins = sum(1 for r in returns if r > 0)
            win_rate = wins / len(returns) * 100 if returns else 0
            best_day = max(returns)
            worst_day = min(returns)
            cum_return = equities[-1] / equities[0] - 1 if equities[0] > 0 else 0

            kospi_return = None
            if len(kospi) >= 2 and kospi[0] and kospi[0] > 0:
                kospi_return = round((kospi[-1] / kospi[0] - 1) * 100, 2)

            return {
                "days": len(returns),
                "period": f"{rows[0]['date']} ~ {rows[-1]['date']}",
                "sharpe": round(sharpe, 2),
                "sortino": round(sortino, 2),
                "mdd": round(max_dd * 100, 2),
                "cagr": round(cagr * 100, 2),
                "cum_return": round(cum_return * 100, 2),
                "win_rate": round(win_rate, 1),
                "best_day": round(best_day * 100, 2),
                "worst_day": round(worst_day * 100, 2),
                "avg_daily": round(mean_r * 100, 4),
                "volatility": round(std_r * (252 ** 0.5) * 100, 2),
                "kospi_return": kospi_return,
            }
        except Exception as e:
            return {"error": str(e)}

    @application.get("/api/rebalance/history")
    async def get_rebalance_history(limit: int = Query(10, ge=1, le=50)):
        """리밸런스 이력 (LIVE → Lab fallback)."""
        # Lab fallback
        try:
            import json as _json
            from pathlib import Path as _P
            from collections import defaultdict
            t_path = _P(__file__).resolve().parent.parent / "data" / "lab_live" / "trades.json"
            t_data = _json.loads(t_path.read_text(encoding="utf-8"))
            t_list = t_data.get("trades", [])
            if t_list:
                by_date = defaultdict(lambda: {"buys": 0, "sells": 0, "total": 0,
                                               "pnl_sum": 0, "pnl_count": 0})
                for t in t_list:
                    dt = t.get("exit_date", t.get("entry_date", ""))
                    if not dt:
                        continue
                    by_date[dt]["sells"] += 1
                    by_date[dt]["total"] += 1
                    if t.get("pnl_pct") is not None:
                        by_date[dt]["pnl_sum"] += t["pnl_pct"]
                        by_date[dt]["pnl_count"] += 1
                result = []
                for dt in sorted(by_date.keys(), reverse=True)[:limit]:
                    d = by_date[dt]
                    avg_pnl = d["pnl_sum"] / d["pnl_count"] if d["pnl_count"] > 0 else 0
                    wins = sum(1 for t in t_list
                               if t.get("exit_date") == dt and (t.get("pnl_pct") or 0) > 0)
                    result.append({
                        "date": dt, "buys": 0, "sells": d["sells"],
                        "total": d["total"], "total_cost": 0,
                        "closed": d["pnl_count"], "close_wins": wins,
                        "avg_pnl": round(avg_pnl, 2), "avg_hold": 0,
                    })
                return {"rebalances": result, "source": "lab"}
        except Exception:
            pass
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT date,
                           COUNT(*) FILTER (WHERE side='BUY') AS buys,
                           COUNT(*) FILTER (WHERE side='SELL') AS sells,
                           COUNT(*) AS total,
                           COALESCE(SUM(cost), 0) AS total_cost
                    FROM report_trades
                    WHERE mode='LIVE'
                    GROUP BY date
                    HAVING COUNT(*) FILTER (WHERE side='BUY') > 0
                       AND COUNT(*) FILTER (WHERE side='SELL') > 0
                    ORDER BY date DESC
                    LIMIT %s
                """, (limit,))
                cols = [d[0] for d in cur.description]
                rebal_dates = [dict(zip(cols, r)) for r in cur.fetchall()]
                result = []
                for rd in rebal_dates:
                    cur.execute("""
                        SELECT COUNT(*) AS closed,
                               COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins,
                               COALESCE(AVG(pnl_pct), 0) AS avg_pnl,
                               COALESCE(AVG(hold_days), 0) AS avg_hold
                        FROM report_close_log
                        WHERE mode='LIVE' AND date = %s
                    """, (rd["date"],))
                    close_row = cur.fetchone()
                    rd["closed"] = close_row[0] if close_row else 0
                    rd["close_wins"] = close_row[1] if close_row else 0
                    rd["avg_pnl"] = round(close_row[2], 2) if close_row else 0
                    rd["avg_hold"] = round(close_row[3], 1) if close_row else 0
                    rd["total_cost"] = round(rd["total_cost"], 0)
                    result.append(rd)
                cur.close()
            return {"rebalances": result}
        except Exception as e:
            return {"error": str(e), "rebalances": []}

    @application.get("/api/alerts/history")
    async def get_alert_history(limit: int = Query(50, ge=1, le=200)):
        """알림 발송 이력 (PG dashboard_alert_state)."""
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT alert_key, last_sent, send_count, suppressed, updated_at "
                    "FROM dashboard_alert_state "
                    "ORDER BY updated_at DESC NULLS LAST "
                    "LIMIT %s", (limit,))
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                cur.close()
            # Stringify timestamps
            for r in rows:
                for k in ["last_sent", "updated_at"]:
                    if r.get(k):
                        r[k] = str(r[k])
            return {"alerts": rows, "total": len(rows)}
        except Exception as e:
            return {"error": str(e), "alerts": []}

    # ── Lab Simulator ─────────────────────────────────────

    @application.get("/lab", response_class=HTMLResponse)
    async def lab_page(request: Request):
        """Lab simulator page."""
        return templates.TemplateResponse(request, "lab.html")

    @application.get("/api/lab/params")
    async def lab_params():
        """Return default Lab parameters and their ranges."""
        from web.lab_simulator import DEFAULT_PARAMS, PARAM_RANGES
        return {"defaults": DEFAULT_PARAMS, "ranges": PARAM_RANGES}

    @application.get("/api/lab/ranking")
    async def lab_ranking(
        source: str = Query("실시간순위", description="실시간순위|등락률|거래량|거래대금|과거CSV"),
        top_n: int = Query(20, ge=5, le=50),
        date: str = Query("", description="CSV date (YYYYMMDD) for 과거CSV source"),
    ):
        """Fetch ranking stocks (live API or historical CSV)."""
        if source == "과거CSV":
            from web.lab_simulator import load_csv_ranking
            ranking = load_csv_ranking(date_str=date, top_n=top_n)
            return {"ranking": ranking, "source": source, "date": date}

        from web.lab_simulator import fetch_ranking
        try:
            provider = _get_provider()
            ranking = fetch_ranking(provider, source=source, top_n=top_n)
            return {"ranking": ranking, "source": source}
        except Exception as e:
            from web.lab_simulator import _fallback_ranking
            return {"ranking": _fallback_ranking(top_n), "source": source, "fallback": True}

    @application.get("/api/lab/dates")
    async def lab_dates():
        """Available CSV dates for historical backtesting."""
        from web.lab_simulator import available_csv_dates
        return {"dates": available_csv_dates()}

    @application.get("/api/lab/history")
    async def lab_history(limit: int = Query(20, ge=1, le=100)):
        """Saved simulation results (summary)."""
        from web.lab_simulator import get_saved_results
        return {"results": get_saved_results(limit)}

    @application.post("/api/lab/simulate")
    async def lab_simulate(request: Request):
        """Run simulation with given params, return 3-strategy results."""
        from web.lab_simulator import fetch_ranking, run_simulation
        body = await request.json()
        params = body.get("params", {})
        ranking_data = body.get("ranking")

        if not ranking_data:
            try:
                provider = _get_provider()
                source = params.get("ranking_source", "등락률")
                top_n = params.get("top_n", 20)
                ranking_data = fetch_ranking(provider, source=source, top_n=top_n)
            except Exception:
                from web.lab_simulator import _fallback_ranking
                ranking_data = _fallback_ranking(params.get("top_n", 20))

        result = run_simulation(ranking_data, params)
        return result

    # ── Lab Realtime Simulator ────────────────────────────

    _sim_instance: dict = {"sim": None}

    @application.post("/api/lab/realtime/start")
    async def lab_realtime_start(request: Request):
        """Start real-time simulation with WebSocket price tracking."""
        from web.lab_realtime import RealtimeSimulator

        # Lab + Surge can now run simultaneously (event bus WS)
        # Stop existing lab sim if running (not surge)
        if _sim_instance["sim"] and _sim_instance["sim"].running:
            _sim_instance["sim"].stop()

        body = await request.json()
        params = body.get("params", {})
        ranking = body.get("ranking", [])

        if not ranking:
            return {"error": "No ranking data provided"}

        provider = _get_provider()
        sim = RealtimeSimulator(provider, params)
        result = sim.start(ranking)

        if result.get("error"):
            return result

        _sim_instance["sim"] = sim
        _global_sim_ref["sim"] = sim
        return {"ok": True, "codes": result.get("codes", [])}

    @application.get("/api/lab/realtime/state")
    async def lab_realtime_state():
        """Current real-time simulation state."""
        sim = _sim_instance.get("sim") or _global_sim_ref.get("sim")
        if not sim:
            return {"running": False, "strategies": [], "events": []}
        return sim.get_state()

    @application.post("/api/lab/realtime/stop")
    async def lab_realtime_stop():
        """Stop real-time simulation, close all virtual positions."""
        sim = _sim_instance.get("sim")
        if not sim or not sim.running:
            return {"error": "No simulation running"}
        result = sim.stop()
        return result

    # ── Lab Live (9-Strategy Forward Paper Trading) ──────
    _lab_live_sim: dict = {"sim": None}

    def _ensure_lab_live():
        """Lab Live sim 자동 복원.

        R8/R9 (2026-04-23): v2 (head.json + states/) 및 v1 (state.json)
        양쪽을 모두 체크해 auto-restore. 기존에는 v1 path 만 확인해서
        v2-migrated 시스템에서 tray 재시작 후 dashboard = empty 였음.
        (배치 step 8은 직접 sim.initialize() 호출해서 복원 되지만,
        dashboard 첫 접근 경로가 이 gate 에 걸려 skip → "초기화된 듯" 증상)
        """
        if _lab_live_sim.get("sim") and _lab_live_sim["sim"]._initialized:
            return _lab_live_sim["sim"]
        from web.lab_live.config import LabLiveConfig
        cfg = LabLiveConfig()
        # v2 우선: head.json 존재 → 복원 가능. v1 fallback: state.json.
        if cfg.head_file.exists() or cfg.state_file.exists():
            from web.lab_live.engine import LabLiveSimulator
            sim = LabLiveSimulator()
            sim.initialize()
            _lab_live_sim["sim"] = sim
            return sim
        return None

    @application.post("/api/lab/live/start")
    async def lab_live_start(request: Request):
        """Initialize Lab Live simulator. Restores previous state."""
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        reset = body.get("reset", False)

        from web.lab_live.engine import LabLiveSimulator
        sim = _lab_live_sim.get("sim")
        if sim and sim._initialized and not reset:
            return {"ok": True, "already_initialized": True,
                    "last_run_date": sim._last_run_date}

        sim = LabLiveSimulator()
        result = sim.initialize(reset=reset)
        _lab_live_sim["sim"] = sim
        return result

    @application.post("/api/lab/live/run-daily")
    async def lab_live_run_daily(request: Request):
        """Run daily EOD update. Updates OHLCV + generates signals + virtual execution."""
        sim = _lab_live_sim.get("sim")
        if not sim or not sim._initialized:
            # Auto-init
            from web.lab_live.engine import LabLiveSimulator
            sim = LabLiveSimulator()
            sim.initialize()
            _lab_live_sim["sim"] = sim

        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        update_data = body.get("update_ohlcv", True)

        # 항상 동기 실행 (OHLCV 업데이트는 평일에만)
        import threading
        def _bg_run():
            try:
                if update_data:
                    from datetime import datetime as _dt
                    if _dt.now().weekday() < 5:  # 평일만
                        from web.lab_live.daily_runner import update_ohlcv
                        update_ohlcv(sim.config.ohlcv_dir, days_back=3)
                result = sim.run_daily()
                logger.info(f"[LAB_LIVE] Run result: {result}")
                # EOD 완료/에러 알림
                try:
                    from notify.telegram_bot import send
                    if result.get("ok"):
                        send(
                            f"✅ <b>KR Lab EOD Complete</b>\n"
                            f"Date: {result.get('date')}\n"
                            f"Trades: {result.get('trades', 0)}\n"
                            f"Source: {result.get('selected_source', '?')}\n"
                            f"Elapsed: {result.get('elapsed', 0)}s",
                            severity="INFO",
                        )
                    elif result.get("skipped"):
                        pass  # skip은 알림 불필요
                    elif result.get("error"):
                        send(
                            f"⚠️ <b>KR Lab EOD Error</b>\n"
                            f"{result.get('error')}",
                            severity="WARN",
                        )
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"[LAB_LIVE] Run error: {e}")
                try:
                    from notify.telegram_bot import send
                    send(f"🚨 <b>KR Lab EOD Failed</b>\n{e}", severity="CRITICAL")
                except Exception:
                    pass
        t = threading.Thread(target=_bg_run, daemon=True)
        t.start()
        t.join(timeout=120)  # 최대 2분 대기
        return sim.get_state()

    @application.get("/api/lab/live/state")
    async def lab_live_state():
        """Current Lab Live state (all lanes, positions, P&L)."""
        sim = _lab_live_sim.get("sim")
        if not sim or not sim._initialized:
            # 자동 복원 시도
            sim = _ensure_lab_live()
            if not sim:
                return {"initialized": False, "lanes": []}
        state = sim.get_state()
        # 전일대비 등락률 enrichment (observer-only)
        # Lab의 cur_price는 last_run_date 종가 → ref_date=last_run_date로 조회해
        # 그 "직전 거래일" 종가와 비교해야 함 (시스템 today 쓰면 동일 날짜 반환 → 0%)
        all_pos = [p for lane in state.get("lanes", []) for p in lane.get("positions", [])]
        if all_pos:
            for p in all_pos:
                p["cur_price"] = p.get("current_price", 0)
            _ref_date = getattr(sim, "_last_run_date", None)
            _enrich_day_change(all_pos, ref_date=_ref_date)
            for p in all_pos:
                p.pop("cur_price", None)
        return state

    @application.get("/api/lab/live/meta")
    async def lab_live_meta():
        """Meta Layer summary for UI (observer-only)."""
        try:
            from web.lab_live.meta_summary import build_daily_summary
            from web.lab_live.daily_drivers import (
                build_drivers_for_lane, build_kospi_series,
            )
            from data.db_provider import DbProvider
            sim = _lab_live_sim.get("sim")
            # sim 이 init 상태면 그 _last_run_date 사용.
            # tray 재시작 직후 sim 이 None 이어도 PG meta_strategy_daily 에서 최신
            # trade_date 를 fallback 조회하여 view 는 항상 복원되도록 한다.
            # (메타분석/카드 상세가 매 재시작마다 사라지던 문제 — 2026-04-20 fix)
            trade_date = None
            if sim and sim._initialized:
                trade_date = sim._last_run_date
            if not trade_date:
                try:
                    from shared.db.pg_base import connection as _mc
                    with _mc() as _conn:
                        _cur = _conn.cursor()
                        _cur.execute(
                            "SELECT MAX(trade_date) FROM meta_strategy_daily"
                        )
                        _r = _cur.fetchone()
                        _cur.close()
                    if _r and _r[0]:
                        trade_date = str(_r[0])
                except Exception:
                    pass
            if not trade_date:
                return {"ok": False}
            summary = build_daily_summary(trade_date)
            if not summary:
                return {"ok": False}

            # ── Daily drivers 주입 (카드 확장 섹션용) ──
            # 안전: 실패 시 drivers만 비우고 summary는 유지.
            # sim 이 없으면 drivers 는 skip (summary 만 반환) — UI 는 기본 메타분석
            # 박스까지는 보이고 카드 expand detail 만 비는 상태.
            try:
                if sim is None:
                    # sim 미초기화 상태 — drivers 건너뛰고 summary 만 제공
                    raise RuntimeError("sim not initialized — drivers skipped (fallback mode)")
                db = DbProvider()
                kospi_series = build_kospi_series(db, trade_date, window=30)
                sector_map = getattr(sim, "_sector_map", {}) or {}
                init_cash = getattr(sim.config, "initial_cash", 100_000_000)
                sfit = summary.get("strategy_fit", {}) or {}
                for sname, lane in getattr(sim, "_lanes", {}).items():
                    drv = build_drivers_for_lane(
                        lane=lane,
                        sname=sname,
                        trade_date=trade_date,
                        sector_map=sector_map,
                        initial_cash=init_cash,
                        kospi_series=kospi_series,
                        db_provider=db,
                        window=30,
                    )
                    if sname in sfit:
                        sfit[sname]["drivers"] = drv
                    else:
                        sfit[sname] = {"drivers": drv}
                summary["strategy_fit"] = sfit
            except Exception as _drv_err:
                # drivers 실패는 non-fatal
                summary["drivers_error"] = str(_drv_err)

            # ── Live Promotion 판정 주입 ──────────────────────
            # Per LIVE_PROMOTION_CRITERIA.md: hard gates + readiness + composition
            try:
                from lab.promotion.engine import evaluate_promotion_batch
                from lab.promotion.adapters import (
                    lane_to_metrics, runtime_to_ops, build_data_quality, resolve_factor,
                )
                import json as _json
                # Ops evidence (structured state + fallback). UNKNOWN 유지 — 0 대체 금지.
                ops = runtime_to_ops()

                # ohlcv_sync state
                _sync_path = _Path(__file__).resolve().parent.parent / "data" / "lab_live" / "ohlcv_sync.json"
                _sync = {}
                if _sync_path.exists():
                    try:
                        _sync = _json.loads(_sync_path.read_text(encoding="utf-8"))
                    except Exception:
                        _sync = {}
                run_meta = getattr(sim, "_run_meta", {}) or {}

                # per-strategy inputs
                # regime_history 는 build_data_quality 내부에서 strategy_name 기반으로
                # 조회한다 (hardcoded fallback 제거). history 없으면 UNKNOWN.
                init_cash = getattr(sim.config, "initial_cash", 100_000_000)
                strategies_data = []
                dq_map: dict = {}
                for sname, lane in getattr(sim, "_lanes", {}).items():
                    dq_strat = build_data_quality(
                        run_meta, _sync, summary.get("strategy_fit", {}),
                        strategy_name=sname,
                    )
                    dq_map[sname] = dq_strat

                    metrics = lane_to_metrics(
                        lane, strategy=sname, initial_cash=init_cash, market="KR",
                    )
                    # positions snapshot for composition check
                    positions_snap = {}
                    for tk, pos in (getattr(lane, "positions", {}) or {}).items():
                        sec_info = sector_map.get(tk, {}) if isinstance(sector_map.get(tk), dict) else {}
                        positions_snap[tk] = {
                            "sector": sec_info.get("sector", "Other"),
                            "weight": float(
                                (getattr(pos, "qty", 0) * getattr(pos, "current_price", 0))
                                / max(1.0, (lane.cash + sum(
                                    p.qty * p.current_price for p in lane.positions.values()
                                )))
                            ),
                            "qty": getattr(pos, "qty", 0),
                            "current_price": getattr(pos, "current_price", 0),
                        }
                    strategies_data.append({
                        "metrics": metrics,
                        "equity_history": getattr(lane, "equity_history", []) or [],
                        "positions": positions_snap,
                        "factor_tag": resolve_factor(sname),
                    })

                promo = evaluate_promotion_batch(
                    strategies_data,
                    ops_map={s["metrics"].strategy: ops for s in strategies_data},
                    data_quality_map=dq_map,
                )
                summary["promotion"] = promo

                # 각 strategy_fit에 promotion 요약 주입 (UI 표시용)
                sfit = summary.get("strategy_fit", {}) or {}
                for sname, result in promo.get("per_strategy", {}).items():
                    if sname not in sfit:
                        sfit[sname] = {}
                    sfit[sname]["promotion"] = {
                        "status": result.get("status"),
                        "total_score": result.get("total_score"),
                        "hard_pass": result.get("hard_pass"),
                        "critical_fail": result.get("critical_fail"),
                        "blockers": result.get("blockers", []),
                        "evidence_missing": result.get("evidence_missing", []),
                        "failures_by_category": result.get("failures_by_category", {}),
                        "evidence_coverage": result.get("evidence_coverage"),
                        "unknown_fields": result.get("unknown_fields", []),
                        "subscores": result.get("subscores", {}),
                        "group": result.get("group"),
                        "versions": result.get("versions", {}),
                    }
                summary["strategy_fit"] = sfit
            except Exception as _pro_err:
                summary["promotion_error"] = str(_pro_err)

            return {"ok": True, **summary}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── Debug: Data Events + Market Context ──────────────────
    # 두 엔드포인트 분리 (Jeff 제약 #5): market_context 는 "현재 상태",
    # data_events 는 "이벤트 로그". 같은 패널에 섞지 않음.

    @application.get("/api/debug/data_events")
    async def debug_data_events(
        limit: int = Query(50, ge=1, le=200),
        min_level: str = Query("", description="DEBUG|INFO|WARN|ERROR|CRITICAL"),
        sources: str = Query("", description="comma-separated substring match"),
    ):
        """순환 buffer 에서 최근 데이터/환경 이벤트 조회 (UI DEBUG 패널용)."""
        try:
            from web.data_events import get_events, get_escalation_states
            src_list = [s.strip() for s in sources.split(",") if s.strip()] if sources else None
            events = get_events(
                limit=limit,
                min_level=min_level or None,
                sources=src_list,
            )
            return {
                "ok": True,
                "events": events,
                "active_escalations": get_escalation_states(),
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "events": []}

    @application.get("/api/debug/market_context")
    async def debug_market_context():
        """
        현재 시장 컨텍스트 (effective_trade_date, index_ready, run_mode).
        engine._run_meta.market_context 에서 추출. 아직 run 안 됐으면 null.
        """
        sim = _lab_live_sim.get("sim")
        if not sim or not sim._initialized:
            return {"ok": False, "reason": "lab_live not initialized"}

        run_meta = getattr(sim, "_run_meta", None) or {}
        mctx = run_meta.get("market_context")
        if not mctx:
            return {
                "ok": False,
                "reason": "no run yet",
                "last_run_date": getattr(sim, "_last_run_date", None),
            }

        return {
            "ok": True,
            "market_context": mctx,
            "last_run_date": getattr(sim, "_last_run_date", None),
            "selected_source": run_meta.get("selected_source"),
            "data_last_date": run_meta.get("data_last_date"),
        }

    @application.get("/api/debug/batch_log")
    async def debug_batch_log(lines: int = Query(200, ge=10, le=2000)):
        """R14 (2026-04-23): real-time batch log tail for Debug UI.

        Reads today's batch-related log. Priority order:
          1. kr/logs/gen4_batch_{today}.log   (CLI batch direct log)
          2. kr/data/logs/rest_api_{today}.log (orchestrator + batch mixed)

        Returns list of log lines with source file. Caller refreshes every
        ~5s for near-real-time tailing during 1-2 hour batch runs.
        """
        import os as _os
        from pathlib import Path as _Path
        from datetime import date as _date

        today = _date.today().strftime("%Y%m%d")
        repo_root = _Path(__file__).resolve().parents[2]
        candidates = [
            repo_root / "kr" / "logs" / f"gen4_batch_{today}.log",
            repo_root / "kr" / "data" / "logs" / f"rest_api_{today}.log",
        ]

        for path in candidates:
            if not path.exists():
                continue
            try:
                content = path.read_text(encoding="utf-8", errors="replace")
                all_lines = content.splitlines()
                tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
                return {
                    "ok": True,
                    "source": str(path.relative_to(repo_root)),
                    "total_lines": len(all_lines),
                    "shown": len(tail),
                    "mtime": path.stat().st_mtime,
                    "lines": tail,
                }
            except OSError as e:
                return {"ok": False, "error": f"read failed: {e!r}", "lines": []}

        return {"ok": False, "error": "no today log found", "lines": []}

    @application.get("/api/debug/qobs")
    async def debug_qobs():
        """R14 (2026-04-23): observe_today.ps1 equivalent for Debug UI.

        Returns pipeline snapshot: heartbeat, marker, incidents, DEADMAN,
        watchdog status. Replicates scripts/observe_today.ps1 logic.
        """
        import os as _os
        import json as _json
        from pathlib import Path as _Path
        from datetime import date as _date, datetime as _dt, timezone as _tz

        repo_root = _Path(__file__).resolve().parents[2]
        pipeline_dir = repo_root / "kr" / "data" / "pipeline"
        incident_dir = repo_root / "backup" / "reports" / "incidents"

        result = {
            "ok": True,
            "trade_date": _date.today().isoformat(),
            "heartbeat": None,
            "marker": None,
            "incidents": [],
            "deadman_configured": False,
        }

        # Heartbeat
        hb_path = pipeline_dir / "heartbeat.json"
        if hb_path.exists():
            try:
                hb = _json.loads(hb_path.read_text(encoding="utf-8"))
                ts_raw = hb.get("ts", "")
                try:
                    ts = _dt.fromisoformat(ts_raw)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=_tz.utc)
                    age_sec = int(
                        (_dt.now(_tz.utc) - ts).total_seconds()
                    )
                except Exception:
                    age_sec = None
                result["heartbeat"] = {
                    "age_sec": age_sec,
                    "tick_seq": hb.get("tick_seq"),
                    "pid": hb.get("pid"),
                    "tray_session": hb.get("tray_session", "")[:16],
                }
            except Exception as e:
                result["heartbeat"] = {"error": str(e)}

        # Marker
        today = _date.today().strftime("%Y%m%d")
        marker_path = pipeline_dir / f"run_completion_{today}.json"
        if marker_path.exists():
            try:
                m = _json.loads(marker_path.read_text(encoding="utf-8"))
                result["marker"] = {
                    "last_update": m.get("last_update"),
                    "runs": {
                        rt: {
                            "status": r.get("status"),
                            "attempt_no": r.get("attempt_no"),
                            "worst_status_today": r.get("worst_status_today"),
                            "started_at": r.get("started_at"),
                            "finished_at": r.get("finished_at"),
                            "error": r.get("error"),
                            "checks": r.get("checks"),
                        }
                        for rt, r in (m.get("runs") or {}).items()
                    },
                    "known_bombs": m.get("known_bombs") or [],
                }
            except Exception as e:
                result["marker"] = {"error": str(e)}

        # Incidents today
        if incident_dir.exists():
            try:
                today_prefix = today
                incidents = sorted(
                    [
                        {"name": f.name, "size": f.stat().st_size}
                        for f in incident_dir.glob(f"{today_prefix}_*.md")
                    ],
                    key=lambda x: x["name"],
                )
                # Also include non-dated watchdog_external incidents from today
                for f in incident_dir.glob(f"{today_prefix}*watchdog*.md"):
                    if not any(i["name"] == f.name for i in incidents):
                        incidents.append(
                            {"name": f.name, "size": f.stat().st_size}
                        )
                result["incidents"] = incidents
            except OSError:
                result["incidents"] = []

        # DEADMAN configured
        result["deadman_configured"] = bool(
            _os.environ.get("QTRON_TELEGRAM_TOKEN_DEADMAN")
            and _os.environ.get("QTRON_TELEGRAM_CHAT_ID_DEADMAN")
        )

        # R13 (2026-04-23): Expected vs actual run status per EXPECTED_WINDOWS_KST.
        # For each expected run_type, shows window + current phase + marker status
        # + alert flag (past_deadline without SUCCESS/SKIPPED).
        try:
            from pipeline.completion_schema import EXPECTED_WINDOWS_KST
            from zoneinfo import ZoneInfo
            _kst_now = _dt.now(ZoneInfo("Asia/Seoul"))
            _now_min = _kst_now.hour * 60 + _kst_now.minute
            marker_runs = (result.get("marker") or {}).get("runs") or {}
            expected: dict = {}
            for run_type, (earliest, deadline) in EXPECTED_WINDOWS_KST.items():
                run_entry = marker_runs.get(run_type) or {}
                actual_status = run_entry.get("status")
                # Phase vs window (minutes — handle deadline past midnight: US_BATCH
                # deadline 1480 = 00:40 next day. Treat now_min + 1440 as "today
                # afternoon + next day carry-over" for deadline > 1440.)
                if deadline > 1440:
                    # Window spans midnight: in_window if now >= earliest OR
                    # now <= (deadline - 1440)
                    if _now_min >= earliest or _now_min <= (deadline - 1440):
                        phase = "in_window"
                    elif _now_min < earliest:
                        phase = "before_window"
                    else:
                        phase = "past_deadline"
                else:
                    if _now_min < earliest:
                        phase = "before_window"
                    elif _now_min <= deadline:
                        phase = "in_window"
                    else:
                        phase = "past_deadline"
                alert = (
                    phase == "past_deadline"
                    and actual_status not in ("SUCCESS", "SKIPPED")
                )
                expected[run_type] = {
                    "earliest_kst": f"{earliest//60:02d}:{earliest%60:02d}",
                    "deadline_kst": (
                        f"{(deadline % 1440)//60:02d}:{(deadline % 1440)%60:02d}"
                        + ("+1d" if deadline > 1440 else "")
                    ),
                    "phase": phase,
                    "actual_status": actual_status,
                    "alert": alert,
                }
            result["expected_runs"] = expected
            result["now_kst"] = _kst_now.strftime("%H:%M:%S")
        except Exception as _r13e:
            result["expected_runs_error"] = repr(_r13e)

        return result

    @application.get("/api/lab/live/trades")
    async def lab_live_trades(limit: int = Query(50, ge=1, le=500)):
        """Recent trades from Lab Live."""
        sim = _lab_live_sim.get("sim")
        if not sim:
            return {"trades": []}
        return {"trades": sim.get_trades(limit)}

    @application.get("/api/lab/live/equity")
    async def lab_live_equity():
        """Equity history for all strategies."""
        sim = _lab_live_sim.get("sim")
        if not sim:
            return {"equity": {}}
        return {"equity": sim.get_equity_history()}

    @application.post("/api/lab/live/reset")
    async def lab_live_reset():
        """Reset Lab Live to initial state (100M fresh start)."""
        from web.lab_live.engine import LabLiveSimulator
        sim = LabLiveSimulator()
        result = sim.initialize(reset=True)
        _lab_live_sim["sim"] = sim
        return result

    @application.get("/sse/lab-live")
    async def sse_lab_live_stream(request: Request):
        """SSE stream for Lab Live state."""
        async def _gen():
            while True:
                if await request.is_disconnected():
                    break
                sim = _lab_live_sim.get("sim")
                if sim and sim._initialized:
                    try:
                        state = sim.get_state()
                        payload = json.dumps(state, default=str)
                        yield f"event: lab_live\ndata: {payload}\n\n"
                    except Exception as e:
                        yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                else:
                    yield f"event: heartbeat\ndata: {{}}\n\n"
                await asyncio.sleep(2.0)

        return StreamingResponse(
            _gen(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Strategy Lab (9-Strategy Comparison / Backtest) ────
    _strategy_lab_lock = {"running": False}

    @application.get("/api/lab/strategy/runs")
    async def strategy_lab_runs():
        """List completed Strategy Lab runs."""
        # sys.path already prepared by _bootstrap_path at top of module
        lab_dir = Path(__file__).resolve().parent.parent / "report" / "output" / "lab"
        runs = []
        if lab_dir.exists():
            for d in sorted(lab_dir.iterdir(), reverse=True):
                if d.is_dir():
                    summary = d / "summary.json"
                    status = d / "status.json"
                    if summary.exists():
                        try:
                            data = json.loads(summary.read_text(encoding="utf-8"))
                            runs.append({
                                "run_id": d.name,
                                "period": data.get("period", ""),
                                "mode": data.get("mode", ""),
                                "groups": data.get("groups", {}),
                                "n_strategies": len(data.get("strategies", {})),
                            })
                        except Exception:
                            pass
                    elif status.exists():
                        try:
                            data = json.loads(status.read_text(encoding="utf-8"))
                            runs.append({
                                "run_id": d.name,
                                "state": data.get("state", "unknown"),
                                "progress_pct": data.get("progress_pct", 0),
                            })
                        except Exception:
                            pass
        return {"runs": runs[:20]}

    @application.get("/api/lab/strategy/results")
    async def strategy_lab_results(run_id: str = Query("latest")):
        """Get Strategy Lab results (summary + equity + trades)."""
        lab_dir = Path(__file__).resolve().parent.parent / "report" / "output" / "lab"
        if not lab_dir.exists():
            return {"error": "No lab runs found"}

        if run_id == "latest":
            # Find most recent run with summary.json (completed full run)
            dirs = sorted([d for d in lab_dir.iterdir() if d.is_dir()], reverse=True)
            run_dir = None
            for d in dirs:
                if (d / "summary.json").exists():
                    run_dir = d
                    break
            if not run_dir:
                return {"error": "No completed lab runs found"}
        else:
            run_dir = lab_dir / run_id
            if not run_dir.exists():
                return {"error": f"Run {run_id} not found"}

        result = {"run_id": run_dir.name}

        # Summary
        summary_path = run_dir / "summary.json"
        if summary_path.exists():
            result["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))

        # Summary CSV (table-friendly)
        csv_path = run_dir / "summary.csv"
        if csv_path.exists():
            import csv
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                result["table"] = list(reader)

        # Equity curves
        eq_path = run_dir / "equity_curves.csv"
        if eq_path.exists():
            import pandas as pd
            eq = pd.read_csv(eq_path, index_col=0)
            result["equity"] = {
                "dates": [str(d)[:10] for d in eq.index.tolist()],
                "strategies": {col: eq[col].tolist() for col in eq.columns},
            }

        # Per-strategy trades count
        detail_dir = run_dir / "detail"
        if detail_dir.exists():
            strat_details = {}
            for sd in detail_dir.iterdir():
                if sd.is_dir():
                    metrics_path = sd / "metrics.json"
                    if metrics_path.exists():
                        try:
                            strat_details[sd.name] = json.loads(
                                metrics_path.read_text(encoding="utf-8"))
                        except Exception:
                            pass
            result["details"] = strat_details

        # Charts
        chart_dir = run_dir / "charts"
        if chart_dir.exists():
            result["charts"] = [f.name for f in chart_dir.glob("*.png")]

        # Status
        status_path = run_dir / "status.json"
        if status_path.exists():
            result["status"] = json.loads(status_path.read_text(encoding="utf-8"))

        return result

    @application.post("/api/lab/strategy/run")
    async def strategy_lab_run(request: Request):
        """Start a Strategy Lab run (background subprocess)."""
        if _strategy_lab_lock["running"]:
            return {"ok": False, "reason": "LAB_ALREADY_RUNNING"}, 409

        body = await request.json()
        group = body.get("group", "")
        start = body.get("start", "2026-03-01")
        end = body.get("end", "2026-04-08")
        strategies = body.get("strategies", "")

        import subprocess
        python_exe = str(Path(__file__).resolve().parent.parent.parent / ".venv" / "Scripts" / "python.exe")
        gen04_dir = str(Path(__file__).resolve().parent.parent)

        cmd = [python_exe, "-m", "lab.run_lab",
               "--start", start, "--end", end, "--no-charts"]
        if group:
            cmd.extend(["--group", group])
        if strategies:
            cmd.extend(["--strategies", strategies])

        _strategy_lab_lock["running"] = True
        try:
            proc = subprocess.Popen(cmd, cwd=gen04_dir,
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            # Don't wait - background
            import threading
            def _wait():
                proc.wait()
                _strategy_lab_lock["running"] = False
            threading.Thread(target=_wait, daemon=True).start()
            return {"ok": True, "pid": proc.pid}
        except Exception as e:
            _strategy_lab_lock["running"] = False
            return {"ok": False, "error": str(e)}

    @application.get("/api/lab/strategy/status")
    async def strategy_lab_status():
        """Check if Strategy Lab is currently running."""
        lab_dir = Path(__file__).resolve().parent.parent / "report" / "output" / "lab"
        # Find latest status
        latest_status = None
        if lab_dir.exists():
            dirs = sorted([d for d in lab_dir.iterdir() if d.is_dir()], reverse=True)
            for d in dirs[:1]:
                sp = d / "status.json"
                if sp.exists():
                    try:
                        latest_status = json.loads(sp.read_text(encoding="utf-8"))
                        latest_status["run_id"] = d.name
                    except Exception:
                        pass
        return {
            "running": _strategy_lab_lock["running"],
            "latest": latest_status,
        }

    @application.get("/sse/lab")
    async def sse_lab_stream(request: Request):
        """SSE stream for real-time lab simulation state (1s interval)."""
        return StreamingResponse(
            _sse_lab_generator(request, interval=1.0),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Surge Simulator ──────────────────────────────────

    _surge_instance: dict = {"sim": None}

    @application.get("/surge")
    async def surge_page():
        """Surge redirect to Lab (Surge Sim tab)."""
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/lab", status_code=302)

    @application.get("/api/surge/params")
    async def surge_params():
        """Default surge simulator params and ranges."""
        from web.surge.config import DEFAULT_SURGE_CONFIG, SURGE_PARAM_RANGES
        return {"defaults": DEFAULT_SURGE_CONFIG.to_dict(), "ranges": SURGE_PARAM_RANGES}

    @application.post("/api/surge/start")
    async def surge_start(request: Request):
        """Start surge simulator. Mutually exclusive with lab realtime."""
        from web.surge.engine import SurgeSimulator
        from web.surge.config import config_from_dict

        # Mutual exclusion: lab realtime
        # Surge + Lab can now run simultaneously (event bus WS)
        # Stop existing surge sim only if needed
        # if _surge_instance["sim"] and _surge_instance["sim"].running:
        #     _surge_instance["sim"].stop()

        try:
            body = await request.json()
        except Exception:
            return {"error": "Invalid JSON body"}
        config = config_from_dict(body.get("params", {}))

        provider = _get_provider()
        sim = SurgeSimulator(provider, config)
        result = sim.start()

        if result.get("error"):
            return result

        _surge_instance["sim"] = sim
        _surge_sim_ref["sim"] = sim
        return result

    @application.post("/api/surge/stop")
    async def surge_stop():
        """Stop surge simulator."""
        sim = _surge_instance.get("sim")
        if not sim or not sim.running:
            return {"error": "No surge simulation running"}
        return sim.stop()

    @application.get("/api/surge/state")
    async def surge_state():
        """Current surge simulation state."""
        sim = _surge_instance.get("sim") or _surge_sim_ref.get("sim")
        if not sim:
            return {"running": False, "positions": [], "trades": [], "events": []}
        return sim.get_state()

    @application.get("/api/surge/trades")
    async def surge_trades():
        """Surge trade history."""
        sim = _surge_instance.get("sim") or _surge_sim_ref.get("sim")
        if not sim:
            return {"trades": []}
        return {"trades": sim.get_trades()}

    @application.get("/api/surge/summary")
    async def surge_summary():
        """Daily summary metrics."""
        sim = _surge_instance.get("sim") or _surge_sim_ref.get("sim")
        if not sim:
            return {"summary": {}}
        return {"summary": sim.get_summary()}

    @application.get("/sse/surge")
    async def sse_surge_stream(request: Request):
        """SSE stream for surge simulation state (1s interval)."""
        return StreamingResponse(
            _sse_surge_generator(request, interval=1.0),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── SSE Stream ────────────────────────────────────────

    @application.get("/sse/state")
    async def sse_state_stream(request: Request):
        """
        Server-Sent Events stream for real-time state updates.
        Pushes full snapshot every 2 seconds.
        Client receives: event: state\ndata: {json}\n\n
        """
        return StreamingResponse(
            _sse_generator(request, interval=2.0, event_type="state"),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # nginx proxy support
            },
        )

    @application.get("/sse/traces")
    async def sse_traces_stream(request: Request):
        """
        SSE stream for trace updates (last 20, every 3 seconds).
        Lighter than full state for trace-focused views.
        """
        return StreamingResponse(
            _sse_traces_generator(request, interval=3.0),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @application.get("/sse/health")
    async def sse_health_stream(request: Request):
        """
        SSE stream for health status only (lightweight, every 5 seconds).
        For Basic mode.
        """
        return StreamingResponse(
            _sse_health_generator(request, interval=5.0),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # ── Regime Prediction Router ────────────────────────
    try:
        from regime.api import router as regime_router
        application.include_router(regime_router)
    except ImportError as e:
        logging.getLogger("web").warning(f"Regime module not loaded: {e}")

    # ── Pipeline Orchestrator Router (Phase 4) ──────────
    # Endpoints are always mounted so dashboards can query state even
    # when QTRON_PIPELINE is unset (status returns enabled=false).
    try:
        from pipeline.api import router as pipeline_router
        application.include_router(pipeline_router)
    except ImportError as e:
        logging.getLogger("web").warning(f"Pipeline module not loaded: {e}")

    # ── Unified Dashboard (Observer-Only) ────────────────

    def _fetch_us(path: str, timeout: float = 3.0):
        """Fetch from US server. Returns None on failure."""
        try:
            import requests as _req
            resp = _req.get(f"http://localhost:8081{path}", timeout=timeout)
            return resp.json() if resp.ok else None
        except Exception:
            return None

    @application.get("/unified", response_class=HTMLResponse)
    async def unified_page(request: Request):
        return templates.TemplateResponse(request, "unified.html")

    @application.get("/api/us/health")
    async def us_health_proxy():
        data = _fetch_us("/api/health")
        return data or {"market": "US", "error": "US server not available"}

    @application.get("/api/us/portfolio")
    async def us_portfolio_proxy():
        data = _fetch_us("/api/portfolio")
        return data or {"market": "US", "error": "US server not available"}

    @application.get("/api/us/target")
    async def us_target_proxy():
        data = _fetch_us("/api/target")
        return data or {"market": "US", "error": "US server not available"}

    @application.get("/api/us/orders")
    async def us_orders_proxy():
        data = _fetch_us("/api/orders/open")
        return data or {"orders": [], "error": "US server not available"}

    @application.get("/api/unified/state")
    async def unified_state():
        """Combined KR + US state. Partial success allowed."""
        from datetime import datetime

        # KR (use portfolio cache from SSE, same source as KR dashboard)
        kr_data, kr_available = None, False
        kr_ts = ""
        try:
            cached = _portfolio_cache.get("data")
            if cached:
                kr_data = dict(cached)
                kr_available = True
                cache_ts = _portfolio_cache.get("ts", 0)
                if cache_ts:
                    from datetime import datetime as _dt
                    kr_ts = _dt.fromtimestamp(cache_ts).strftime("%H:%M:%S KST")
                else:
                    kr_ts = datetime.now().strftime("%H:%M:%S KST")
            else:
                # Cache empty — try direct query
                try:
                    provider = _get_global_provider()
                    summary = provider.query_account_summary()
                    if summary and summary.get("error") is None:
                        kr_data = {
                            "holdings_count": len(summary.get("holdings", [])),
                            "cash": summary.get("available_cash", 0),
                            "total_asset": summary.get("\ucd94\uc815\uc608\ud0c1\uc790\uc0b0", 0),
                            "total_buy": summary.get("\ucd1d\ub9e4\uc785\uae08\uc561", 0),
                            "total_eval": summary.get("\ucd1d\ud3c9\uac00\uae08\uc561", 0),
                            "total_pnl": summary.get("\ucd1d\ud3c9\uac00\uc190\uc775\uae08\uc561", 0),
                            "pnl_pct": round(summary.get("\ucd1d\ud3c9\uac00\uc190\uc775\uae08\uc561", 0) / max(summary.get("\ucd1d\ub9e4\uc785\uae08\uc561", 1), 1) * 100, 2),
                            "holdings": summary.get("holdings", []),
                        }
                        kr_available = True
                        kr_ts = datetime.now().strftime("%H:%M:%S KST")
                except Exception:
                    pass
        except Exception:
            pass

        # US (HTTP, timeout 3s)
        us_data, us_available = None, False
        us_health, us_ts = None, ""
        try:
            us_data = _fetch_us("/api/portfolio")
            us_health = _fetch_us("/api/health")
            if us_data and "error" not in us_data:
                us_available = True
                us_ts = datetime.now().strftime("%H:%M:%S ET")
        except Exception:
            pass

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "kr": {
                "data": kr_data,
                "snapshot_at": kr_ts,
                "available": kr_available,
            },
            "us": {
                "data": us_data,
                "health": us_health,
                "snapshot_at": us_ts,
                "available": us_available,
            },
        }

    # ── API: Telegram (Dashboard → Mobile) ──────────────

    @application.post("/api/notify/telegram")
    async def send_telegram_text(request: Request):
        """Send text message to Telegram from dashboard."""
        try:
            body = await request.json()
            text = body.get("text", "").strip()
            if not text or len(text) > 2000:
                return {"ok": False, "error": "Message empty or too long"}

            from notify.telegram_bot import send
            ok = send(f"[Dashboard] {text}", "INFO")
            return {"ok": ok}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @application.post("/api/notify/telegram/photo")
    async def send_telegram_photo(request: Request):
        """Send photo + caption to Telegram from dashboard."""
        try:
            form = await request.form()
            caption = form.get("caption", "")
            file = form.get("photo")
            if not file:
                return {"ok": False, "error": "No photo"}

            photo_bytes = await file.read()
            if len(photo_bytes) > 10 * 1024 * 1024:
                return {"ok": False, "error": "File too large (max 10MB)"}

            from notify.telegram_bot import send_photo
            ok = send_photo(photo_bytes, caption=f"[Dashboard] {caption}", filename=file.filename or "image.png")
            return {"ok": ok}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    return application


# ── SSE Generators ────────────────────────────────────────────

_global_provider_cache = {"instance": None}
_portfolio_cache = {"data": None, "ts": 0}
_index_cache = {"data": None, "ts": 0}
_trades_cache = {"data": None, "ts": 0}
_payload_seq = {"n": 0}
_regime_running = {"active": False}
_regime_history: list = []  # [{ts, label, score, kospi_change, breadth_ratio}, ...]
_regime_lock = threading.Lock()  # P0-1: protect _regime_history concurrent access


def _get_global_provider():
    if _global_provider_cache["instance"] is None:
        # sys.path already prepared by _bootstrap_path at top of module
        from data.rest_provider import KiwoomRestProvider
        _global_provider_cache["instance"] = KiwoomRestProvider(server_type="REAL")
    return _global_provider_cache["instance"]


def _load_theme_regime_from_db(limit: int = 15) -> list:
    """Fallback: load theme_regime from PG regime_theme_daily (no Kiwoom call).

    Prefers today's row. Falls back to the most recent market_date present.
    Returns list compatible with dashboard.js consumer:
        [{code, name, count, change_pct, regime, streak, held_count}]
    """
    try:
        from shared.db.pg_base import connection as _conn
        from datetime import date as _date
        today_str = _date.today().strftime("%Y-%m-%d")
        with _conn() as _c:
            cur = _c.cursor()
            cur.execute(
                "SELECT theme_code, theme_name, stock_count, change_pct, regime, "
                "streak_days, market_date FROM regime_theme_daily "
                "WHERE market_date=%s ORDER BY change_pct DESC LIMIT %s",
                (today_str, limit),
            )
            rows = cur.fetchall()
            if not rows:
                cur.execute(
                    "SELECT theme_code, theme_name, stock_count, change_pct, regime, "
                    "streak_days, market_date FROM regime_theme_daily "
                    "WHERE market_date=(SELECT MAX(market_date) FROM regime_theme_daily) "
                    "ORDER BY change_pct DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
            cur.close()
        out = []
        for r in rows:
            out.append({
                "code": r[0], "name": r[1], "count": r[2],
                "change_pct": float(r[3]) if r[3] is not None else 0.0,
                "regime": r[4], "streak": r[5], "held_count": 0,
                "as_of": str(r[6]) if r[6] else "",
            })
        return out
    except Exception as _e:
        logging.getLogger("web").debug(f"_load_theme_regime_from_db: {_e}")
        return []


def _enrich_day_change_fast(holdings: list) -> None:
    """경량 전일대비 enrich — Kiwoom prev_close_price 우선, DB fallback.
    모듈 레벨 (create_app 클로저 우회, SSE generator에서 호출 가능).

    소스 순서:
      1. h["prev_close_price"] from Kiwoom (kt00018 응답의 pred_close_pric)
      2. DB ohlcv 테이블 get_prev_closes (Kiwoom에서 빠진 종목만)
    """
    prev_closes = {}  # code → float
    missing = []
    for h in holdings:
        code = h.get("code", "")
        pcp = h.get("prev_close_price", 0) or 0
        try:
            pcp = float(pcp)
        except Exception:
            pcp = 0
        if code and pcp > 0:
            prev_closes[code] = pcp
        elif code:
            missing.append(code)

    # DB fallback (prev_close_price 0인 종목만)
    if missing:
        try:
            from data.db_provider import DbProvider
            db = DbProvider()
            data = db.get_prev_closes(missing, max_stale_bdays=5)
            for code, info in data.items():
                pc = info.get("prev_close", 0) if info else 0
                if pc > 0:
                    prev_closes[code] = float(pc)
        except Exception:
            pass

    for h in holdings:
        try:
            code = h.get("code", "")
            cp = float(h.get("cur_price", 0) or 0)
            pc = prev_closes.get(code, 0)
            if cp > 0 and pc > 0:
                h["prev_close"] = pc
                h["day_change_pct"] = round((cp - pc) / pc * 100, 2)
                h["day_change_reason"] = None
            else:
                h["day_change_pct"] = None
                h["day_change_reason"] = "no_prev_close"
        except Exception:
            h["day_change_pct"] = None
            h["day_change_reason"] = "enrich_error"


async def _sse_generator(
    request: Request,
    interval: float = 2.0,
    event_type: str = "state",
) -> AsyncGenerator[str, None]:
    """Generate SSE events with full state snapshot + dashboard data.

    ENGINE SAFETY: _portfolio_cache → UI only (UI_CACHE)
    ❌ 주문 판단 / engine state 덮어쓰기 금지
    방향: broker → RECON → engine → _portfolio_cache → UI  (역방향 금지)
    """
    from datetime import datetime as _dt
    while True:
        if await request.is_disconnected():
            break

        try:
            data = tracker.snapshot()
            now = time.time()

            if not hasattr(_sse_generator, '_cycles'):
                _sse_generator._cycles = {}
            client_id = id(request)
            _sse_generator._cycles.setdefault(client_id, 0)
            _sse_generator._cycles[client_id] += 1
            cycle = _sse_generator._cycles[client_id]

            # ── WebSocket state sync (every cycle) ──
            try:
                _p = _get_global_provider()
                _ws_obj = getattr(_p, '_ws', None)
                if _ws_obj is not None:
                    tracker.update_ws_state(
                        connected=getattr(_ws_obj, '_connected', False),
                        reconnect_count=getattr(_ws_obj, '_reconnect_count', 0),
                    )
            except Exception:
                pass

            # ── Portfolio (every 10th cycle ~20s) ──
            if cycle % 10 == 1:
                try:
                    provider = _get_global_provider()
                    summary = await asyncio.to_thread(provider.query_account_summary)
                    if summary.get("error") is None:
                        # 전일 등락률 enrich — Kiwoom pred_close_pric 직접 사용
                        # (create_app 클로저의 _enrich_day_change 대신 모듈 레벨 helper)
                        try:
                            _enrich_day_change_fast(summary.get("holdings", []))
                        except Exception as _e:
                            logging.getLogger("web").debug(f"[DAY_CHG] enrich failed: {_e}")
                        _portfolio_cache["data"] = {
                            "holdings_count": len(summary.get("holdings", [])),
                            "cash": summary.get("available_cash", 0),
                            "total_asset": summary.get("추정예탁자산", 0),
                            "total_buy": summary.get("총매입금액", 0),
                            "total_eval": summary.get("총평가금액", 0),
                            "total_pnl": summary.get("총평가손익금액", 0),
                            "prev_eval_amt": summary.get("전일평가금액", 0),
                            "pnl_pct": round(summary.get("총평가손익금액", 0) / max(summary.get("총매입금액", 1), 1) * 100, 2),
                            "holdings": summary.get("holdings", []),
                        }
                        _portfolio_cache["ts"] = now
                except Exception as e:
                    logging.getLogger("web").warning(f"Portfolio fetch: {e}")

            if _portfolio_cache["data"]:
                _inject_sectors(_portfolio_cache["data"])
                data["account"] = _portfolio_cache["data"]

            # ── Rebalance (every 10th cycle, offset 2) ──
            # Also caches the FULL runtime dict so the auto-trading gate
            # (compute_auto_trading_state via cache.get("runtime")) can read
            # last_batch_completed_at / business_date / snapshot_version.
            # Without this wiring the gate sees an empty dict and emits a
            # permanent BATCH_MISSING even after the batch wrote the file.
            if cycle % 10 == 2:
                try:
                    rt_file = _GEN04_STATE_DIR / "runtime_state_live.json"
                    if rt_file.exists():
                        with open(rt_file, "r", encoding="utf-8") as _f:
                            _rt = json.load(_f)
                        _portfolio_cache["rebal"] = _rt.get("last_rebalance_date", "")
                        _portfolio_cache["runtime"] = _rt
                except Exception:
                    pass
            if _portfolio_cache.get("rebal"):
                data["rebalance"] = _compute_rebal_schedule(
                    _portfolio_cache["rebal"], 21
                )

            # ── DD Guard + Trail Stops + RECON (every 10th cycle, offset 3) ──
            # Single file read for dd_guard + trail_stops (same source)
            if cycle % 10 == 3:
                _acct = _portfolio_cache.get("data") or {}
                total_asset = _acct.get("total_asset", 0)
                _pstate_raw = _safe_read_json(str(_GEN04_STATE_DIR / "portfolio_state_live.json"))
                _pstate_fb = _get_or_fallback("portfolio_state", _pstate_raw)
                _portfolio_cache["dd_guard"] = _compute_dd_guard_from(total_asset, _acct)
                _portfolio_cache["trail_stops"] = _read_trail_stops_from(_pstate_fb)
                _portfolio_cache["recon"] = _compute_recon_status()

                # Phase 1: REST_DB 병행 미러링 (observer only, 격리)
                try:
                    from web.rest_state_db import sync_positions_from_gen4, make_snapshot_id
                    _fb_data = _pstate_fb.get("data")
                    if _fb_data and _fb_data.get("positions"):
                        _snap_id = make_snapshot_id()
                        _portfolio_cache["_last_snapshot_id"] = _snap_id
                        sync_positions_from_gen4(
                            _fb_data["positions"], _snap_id,
                            asof_ts=_fb_data.get("timestamp", ""),
                        )
                except Exception as _db_err:
                    logging.getLogger("web").debug(f"[REST_DB] position sync: {_db_err}")

            if _portfolio_cache.get("dd_guard"):
                data["dd_guard"] = _portfolio_cache["dd_guard"]
            if _portfolio_cache.get("trail_stops"):
                data["trail_stops"] = _portfolio_cache["trail_stops"]
            if _portfolio_cache.get("recon"):
                data["recon"] = _portfolio_cache["recon"]

            # ── REST vs COM sync comparison (every 10th cycle, offset 4) ──
            if cycle % 10 == 4:
                try:
                    _acct = _portfolio_cache.get("data") or {}
                    _com_raw = _safe_read_json(str(_GEN04_STATE_DIR / "portfolio_state_live.json"))
                    _com_data = (_com_raw.get("data") or _com_raw) if _com_raw else {}
                    _com_positions = _com_data.get("positions", {})
                    _com_cash = _com_data.get("cash", 0)
                    _com_ts_str = _com_data.get("timestamp", "")
                    _com_ts = 0.0
                    if _com_ts_str:
                        try:
                            from datetime import datetime as _sync_dt
                            _com_ts = _sync_dt.fromisoformat(_com_ts_str).timestamp()
                        except Exception:
                            pass
                    _rest_ts = _portfolio_cache.get("ts", 0.0)

                    # 1) Holdings count
                    _rest_holdings = _acct.get("holdings_count", 0)
                    _com_holdings = len(_com_positions)
                    tracker.update_sync("보유종목수", _rest_holdings, _com_holdings,
                                        rest_ts=_rest_ts, com_ts=_com_ts)

                    # 2) Cash
                    _rest_cash = _acct.get("cash", 0)
                    tracker.update_sync("현금", f"{_rest_cash:,.0f}", f"{_com_cash:,.0f}",
                                        rest_ts=_rest_ts, com_ts=_com_ts)

                    # 3) Total eval
                    _rest_eval = _acct.get("total_eval", 0)
                    _com_eval = sum(
                        p.get("current_price", 0) * p.get("qty", 0)
                        for p in _com_positions.values()
                    ) if _com_positions else 0
                    tracker.update_sync("평가금액", f"{_rest_eval:,.0f}", f"{_com_eval:,.0f}",
                                        rest_ts=_rest_ts, com_ts=_com_ts)

                    # 4) Total equity (deposit)
                    _rest_equity = _rest_cash + _rest_eval
                    _com_equity = _com_cash + _com_eval
                    tracker.update_sync("총자산", f"{_rest_equity:,.0f}", f"{_com_equity:,.0f}",
                                        rest_ts=_rest_ts, com_ts=_com_ts)
                except Exception as _sync_err:
                    logging.getLogger("web").debug(f"[SYNC] comparison error: {_sync_err}")

            # ── Index (every 30th cycle ~60s) ──
            if cycle % 30 == 5:
                try:
                    provider = _get_global_provider()
                    idx_data = provider._request("ka20001", "/api/dostk/sect",
                                                 {"mrkt_tp": "0", "inds_cd": "001"})
                    if idx_data.get("return_code") == 0:
                        cur_raw = str(idx_data.get("cur_prc", "0"))
                        cur_val = abs(float(cur_raw.replace("+", "").replace(",", "")))
                        chg_pct = float(str(idx_data.get("flu_rt", "0")).replace("+", ""))
                        name = _INDEX_NAMES.get("0_001", "지수")
                        _index_cache["data"] = {
                            "name": name, "price": cur_val,
                            "change_pct": chg_pct,
                            "stale": False, "error": None,
                        }
                        _index_cache["ts"] = now
                except Exception as e:
                    _index_cache["data"] = {"name": "", "price": 0, "stale": True, "error": str(e)}

            if _index_cache.get("data"):
                idx = _index_cache["data"].copy()
                idx["stale"] = (now - _index_cache.get("ts", 0)) > 120
                data["index"] = idx

            # ── Save market snapshot to DB (every 30th cycle ~60s) ──
            # Portfolio daily change: 오늘 첫 equity 대비 현재 변동률 (KOSPI와 동일 기준)
            if cycle % 30 == 10:
                try:
                    from web.dashboard_db import save_snapshot
                    _idx = _index_cache.get("data") or {}
                    _acct = _portfolio_cache.get("data") or {}
                    cur_equity = _acct.get("total_asset", 0)

                    # 전일 종가 equity 기준 (GUI와 동일 기준)
                    if not hasattr(_sse_generator, '_base_equity'):
                        _sse_generator._base_equity = 0
                    if _sse_generator._base_equity <= 0:
                        # prev_close_equity from kr-legacy LIVE state file
                        _dd = _portfolio_cache.get("dd_guard") or {}
                        _prev = _dd.get("source_prev_close", 0)
                        if _prev and _prev > 0:
                            _sse_generator._base_equity = _prev
                        elif cur_equity > 0:
                            _sse_generator._base_equity = cur_equity  # fallback

                    # 일간 변동률 계산 (전일 종가 대비)
                    base_eq = _sse_generator._base_equity
                    daily_pnl_pct = round(
                        (cur_equity / base_eq - 1) * 100, 2
                    ) if base_eq > 0 and cur_equity > 0 else 0

                    save_snapshot(
                        kospi_price=_idx.get("price", 0),
                        kospi_change_pct=_idx.get("change_pct", 0),
                        portfolio_equity=cur_equity,
                        portfolio_pnl_pct=daily_pnl_pct,
                        portfolio_cash=_acct.get("cash", 0),
                        holdings_count=_acct.get("holdings_count", 0),
                    )

                    # REST_DB EOD snapshot (broker truth, 하루 1회)
                    try:
                        from web.rest_state_db import sync_equity_snapshot, get_eod_equity
                        from datetime import date as _date_cls
                        _today = _date_cls.today().isoformat()
                        _is_eod = (_hour >= 15 and _min >= 30)
                        if _is_eod and cur_equity > 0 and get_eod_equity(_today) is None:
                            sync_equity_snapshot(
                                market_date=_today,
                                equity=cur_equity,
                                cash=_acct.get("cash", 0),
                                holdings_count=_acct.get("holdings_count", 0),
                                is_eod=True,
                                snapshot_id=_portfolio_cache.get("_last_snapshot_id", ""),
                            )
                            logging.getLogger("web").info(
                                f"[REST_DB] EOD snapshot saved: date={_today} "
                                f"equity={cur_equity:,.0f}"
                            )
                    except Exception as _db_err:
                        logging.getLogger("web").debug(f"[REST_DB] equity sync: {_db_err}")
                except Exception:
                    pass

            # ── Recent trades preview (every 30th cycle, offset 15) ──
            if cycle % 30 == 15:
                _trades_cache["data"] = _read_recent_trades(limit=5)
                _trades_cache["ts"] = now

            if _trades_cache.get("data"):
                data["recent_trades_preview"] = _trades_cache["data"]

            # ── Regime auto-predict + read (every 150 cycles ~5min) ──
            # Non-blocking: heavy work via asyncio.to_thread
            if cycle % 150 == 7:
                try:
                    from regime.storage import load_latest_json
                    from regime.scorer import compute_rolling_stats
                    regime_data = load_latest_json()
                    if regime_data:
                        regime_data["rolling_stats"] = compute_rolling_stats(20)
                    _portfolio_cache["regime"] = regime_data
                except Exception:
                    pass

            # Regime predict/score in background (non-blocking)
            if cycle % 150 == 8:
                asyncio.ensure_future(_regime_background_task())
            if _portfolio_cache.get("regime"):
                data["regime_prediction"] = _portfolio_cache["regime"]

            # Load today's actual regime (from DB)
            if cycle % 150 == 9:
                try:
                    from regime.storage import load_actual
                    from datetime import date as _date
                    _today_actual = load_actual(_date.today().isoformat())
                    _portfolio_cache["regime_actual"] = _today_actual
                except Exception:
                    pass
            if _portfolio_cache.get("regime_actual"):
                data["regime_actual"] = _portfolio_cache["regime_actual"]

            # ── Theme Regime (every 150th cycle ~5min) ──
            if cycle % 150 == 12:
                try:
                    from regime.theme_regime import ThemeRegimeTracker
                    provider = _get_global_provider()
                    _theme_tracker = ThemeRegimeTracker(provider)
                    themes = _theme_tracker.collect_and_classify(top_n=15)
                    # 보유종목 매칭: 각 테마에 ka90002로 종목 확인은 비용 큼
                    # → 보유종목의 sector_map 기반 근사 매칭
                    _pf = _portfolio_cache.get("data") or {}
                    _held = {h.get("code", "") for h in _pf.get("holdings", [])}
                    _sm = _get_sector_map()
                    for t in themes:
                        # 테마명이 보유종목의 섹터에 포함되면 held로 근사
                        t["held_count"] = 0  # 정확한 매칭은 ka90002 on-demand
                    _portfolio_cache["theme_regime"] = themes
                except Exception as _te:
                    logging.getLogger("web").warning(f"Theme regime: {_te}")
            if _portfolio_cache.get("theme_regime"):
                data["theme_regime"] = _portfolio_cache["theme_regime"]
            else:
                # DB fallback — avoid 10min cold-start gap where UI shows "loading..."
                _tr_db = _load_theme_regime_from_db()
                if _tr_db:
                    data["theme_regime"] = _tr_db
                    # Warm cache so subsequent ticks skip DB roundtrip
                    _portfolio_cache["theme_regime"] = _tr_db

            # ── System Risk ──
            ds = {}
            _source_map = {
                "portfolio_state": _portfolio_cache.get("dd_guard") or {},
                "runtime_state": _portfolio_cache.get("recon") or {},
                "trades": _trades_cache.get("data") or {},
                "index": _index_cache.get("data") or {},
            }
            for key, src_data in _source_map.items():
                src_ts = src_data.get("source_ts", 0) if isinstance(src_data, dict) else 0
                ds[key] = {
                    "age_sec": round(now - src_ts, 1) if src_ts else 99999,
                    "ok": src_data.get("error") is None if isinstance(src_data, dict) else True,
                    "from_cache": src_data.get("from_cache", False) if isinstance(src_data, dict) else False,
                    "expired": src_data.get("expired", False) if isinstance(src_data, dict) else False,
                }
            data["system_risk"] = _compute_system_risk(
                data.get("dd_guard", {}), data.get("recon", {}), ds
            )

            # ── Payload metadata (snapshot_id는 data 완성 후 마지막에 증가) ──
            ts_str = _dt.now().strftime("%Y%m%d-%H%M%S")
            data["server_ts"] = now
            max_age = max((v.get("age_sec", 0) for v in ds.values()), default=0)
            data["data_age_max_sec"] = round(max_age, 1)
            data["data_sources"] = ds
            # cache 단일 시점 메타 (UI_CACHE: UI 전용, 판단 금지)
            _pc_ts = _portfolio_cache.get("ts")
            _pc_age = (now - _pc_ts) if _pc_ts else None
            data["cache_age_sec"] = round(max(0.0, _pc_age), 1) if _pc_age is not None else None
            data["_data_source"] = "UI_CACHE"
            # P2: auto trading state (advisory read-only) — parity with /api/state one-shot path
            try:
                from kr.risk.auto_trading_gate import compute_auto_trading_state
                from kr.risk.strategy_health import compute_strategy_health
                _guard = getattr(application.state, "guard", None)
                _runtime = (_portfolio_cache.get("runtime") or {})
                _equity_dd = float((_portfolio_cache.get("dd_guard") or {}).get("equity_dd_pct", 0.0) or 0.0)
                _health = compute_strategy_health(equity_dd_pct=_equity_dd)
                _auto = compute_auto_trading_state(
                    guard=_guard, runtime=_runtime,
                    strategy_health=_health,
                )
                data["auto_trading"] = _auto.to_dict()
                data["strategy_health"] = _health
            except Exception as _e:
                data["auto_trading"] = {"enabled": False, "blockers": [f"EVAL_ERROR:{type(_e).__name__}"],
                                        "reason_summary": "eval_error"}
            # ENGINE_OFFLINE safety gate — identical to /api/state path so
            # SSE-consuming dashboards see the same RED banner / disabled
            # auto-trading / UNAVAILABLE recon whenever the live engine is
            # missing. Without this, the browser (SSE) would still show
            # YELLOW "Sync mismatch" while /api/state correctly showed RED.
            _apply_engine_offline_override(data)
            # snapshot_id / payload_id — 모든 필드 확정 후 증가
            _payload_seq["n"] += 1
            data["snapshot_id"] = _payload_seq["n"]
            data["payload_id"]  = f"{ts_str}-{_payload_seq['n']:04d}"

            payload = json.dumps(data, ensure_ascii=False, default=str)
            yield f"event: {event_type}\ndata: {payload}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


async def _regime_background_task() -> None:
    """Run regime predict/score in background thread. Non-blocking."""
    if _regime_running["active"]:
        return
    _regime_running["active"] = True
    try:
        from datetime import date as _date
        _today_str = _date.today().isoformat()
        _hour = int(time.strftime("%H"))
        _min = int(time.strftime("%M"))

        # ── Intraday regime estimate (09:00~15:30, every cycle) ──
        if 9 <= _hour < 16:
            try:
                from regime.actual import calculate_actual as _calc_actual
                provider = _get_global_provider()
                _live_actual = await asyncio.to_thread(_calc_actual, provider, True)
                if not _live_actual.get("unavailable"):
                    _live_actual["intraday"] = True
                    _portfolio_cache["regime_actual"] = _live_actual
                    # Save to history ring buffer (lock: P0-1)
                    with _regime_lock:
                        _regime_history.append({
                            "ts": time.time(),
                            "label": _live_actual.get("actual_label", ""),
                            "score": _live_actual.get("scores", {}).get("total", 0),
                            "kospi_change": _live_actual.get("kospi_change", 0),
                            "breadth_ratio": _live_actual.get("breadth_ratio", 0),
                        })
                        if len(_regime_history) > 72:
                            _regime_history.pop(0)
            except Exception as _e:
                logging.getLogger("web").debug(f"[Regime] Intraday estimate: {_e}")

        # Auto-predict (08:00~15:30, once per day)
        if _portfolio_cache.get("_regime_predict_date") != _today_str and 8 <= _hour < 16:
            try:
                from regime.calendar import next_trading_day
                from regime.storage import load_latest_prediction
                _target = str(next_trading_day(_date.today()))
                existing = load_latest_prediction(target_date=_target)
                if not existing:
                    provider = _get_global_provider()
                    from regime.predictor import predict_regime
                    # v2: EMA + persistence (latest.json has ema_score)
                    from regime.storage import load_latest_json as _load_lj
                    _prev = _load_lj()
                    _prev_ema = _prev.get("ema_score") if _prev else None
                    _prev_reg = _prev.get("predicted_regime") if _prev else None
                    _result = await asyncio.to_thread(
                        predict_regime, provider,
                        prev_ema_score=_prev_ema, prev_regime=_prev_reg,
                    )
                    if not _result.get("unavailable"):
                        _portfolio_cache["_regime_predict_date"] = _today_str
                        logging.getLogger("web").info(
                            f"[Regime] Auto-predict: {_result.get('predicted_label')} "
                            f"(score={_result.get('composite_score', 0):.3f})")
                else:
                    _portfolio_cache["_regime_predict_date"] = _today_str
            except Exception as _e:
                logging.getLogger("web").warning(f"[Regime] Auto-predict failed: {_e}")

        # Auto-score (after 15:35, once per day)
        if _portfolio_cache.get("_regime_score_date") != _today_str and _hour >= 15 and _min >= 35:
            try:
                from regime.actual import calculate_actual
                from regime.scorer import score_prediction as _score_fn
                from regime.storage import load_latest_prediction
                provider = _get_global_provider()
                actual_r = await asyncio.to_thread(calculate_actual, provider, True)
                if not actual_r.get("unavailable"):
                    pred = load_latest_prediction(target_date=_today_str)
                    if pred:
                        _score_fn(
                            predicted=pred["predicted_regime"],
                            actual=actual_r["actual_regime"],
                            available_weight=pred.get("available_weight", 1.0),
                            global_available=bool(pred.get("global_avail", 0)),
                        )
                    _portfolio_cache["_regime_score_date"] = _today_str
                    logging.getLogger("web").info(
                        f"[Regime] Auto-score: actual={actual_r.get('actual_label')}")
            except Exception as _e:
                logging.getLogger("web").warning(f"[Regime] Auto-score failed: {_e}")

        # Daily DB cleanup (once per day)
        if _portfolio_cache.get("_db_cleanup_date") != _today_str:
            try:
                from web.dashboard_db import cleanup_old_snapshots
                await asyncio.to_thread(cleanup_old_snapshots, 30)
                from notify.alert_state import daily_rollover
                daily_rollover()
                _portfolio_cache["_db_cleanup_date"] = _today_str
            except Exception:
                pass

        # ── EOD Report + Crosscheck (15:40 이후, 1일 1회) ──
        if _portfolio_cache.get("_eod_report_date") != _today_str and _hour >= 15 and _min >= 40:
            try:
                from report.rest_daily_report import generate_eod_report
                from regime.storage import load_actual, load_latest_json
                _actual = load_actual(_today_str)
                _predict = load_latest_json()
                await asyncio.to_thread(
                    generate_eod_report,
                    portfolio=_portfolio_cache.get("data"),
                    dd_guard=_portfolio_cache.get("dd_guard"),
                    trail_stops=_portfolio_cache.get("trail_stops"),
                    recon=_portfolio_cache.get("recon"),
                    regime_actual=_actual,
                    regime_predict=_predict,
                    rebalance=_compute_rebal_schedule(
                        _portfolio_cache.get("rebal", ""), 21
                    ),
                )
                _portfolio_cache["_eod_report_date"] = _today_str
                logging.getLogger("web").info("[EOD] REST daily report generated")
            except Exception as _e:
                logging.getLogger("web").warning(f"[EOD] Report failed: {_e}")

            # Crosscheck (legacy + Phase 1 triple)
            try:
                from web.cross_validator import compare_engine_vs_broker, compare_triple
                provider = _get_global_provider()
                xcheck = await asyncio.to_thread(compare_engine_vs_broker, provider)
                if xcheck.get("overall") == "CRITICAL":
                    from notify.telegram_bot import notify_crosscheck_critical
                    diffs = [f"{c['field']}: {c.get('severity','')}" for c in xcheck.get("checks", []) if c.get("severity") == "CRITICAL"]
                    notify_crosscheck_critical(diffs)
                logging.getLogger("web").info(f"[Crosscheck] {xcheck.get('overall')}")
            except Exception as _e:
                logging.getLogger("web").warning(f"[Crosscheck] Failed: {_e}")

            # Phase 1: Triple crosscheck (Gen4 vs REST_DB vs Broker)
            try:
                from web.cross_validator import compare_triple
                provider = _get_global_provider()
                triple = await asyncio.to_thread(compare_triple, provider)
                logging.getLogger("web").info(
                    f"[TripleCheck] {triple.get('overall')} "
                    f"({len(triple.get('checks', []))} issues)")
            except Exception as _e:
                logging.getLogger("web").warning(f"[TripleCheck] Failed: {_e}")

        # ── Alert Engine (evaluate snapshot → send) ──
        try:
            from notify.alert_engine import evaluate, send_alerts
            # Build snapshot from cached data
            _snap = {
                "regime_actual": _portfolio_cache.get("regime_actual"),
                "trail_stops": _portfolio_cache.get("trail_stops"),
                "dd_guard": _portfolio_cache.get("dd_guard"),
                "recon": _portfolio_cache.get("recon"),
                "data_age_max_sec": 0,
                "rebalance": _compute_rebal_schedule(
                    _portfolio_cache.get("rebal", ""), 21
                ),
            }
            events = evaluate(_snap)
            if events:
                sent = await asyncio.to_thread(send_alerts, events)
                if sent:
                    logging.getLogger("web").info(f"[Alert] Sent {sent} alerts")
        except Exception as _e:
            logging.getLogger("web").debug(f"[Alert] Engine error: {_e}")

    finally:
        _regime_running["active"] = False


async def _sse_traces_generator(
    request: Request,
    interval: float = 3.0,
) -> AsyncGenerator[str, None]:
    """Generate SSE events with recent traces only."""
    while True:
        if await request.is_disconnected():
            break

        try:
            traces = tracker.get_traces(limit=20)
            payload = json.dumps(traces, ensure_ascii=False, default=str)
            yield f"event: traces\ndata: {payload}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


async def _sse_health_generator(
    request: Request,
    interval: float = 5.0,
) -> AsyncGenerator[str, None]:
    """Generate SSE events with health status only."""
    while True:
        if await request.is_disconnected():
            break

        try:
            snap = tracker.snapshot()
            health_data = {
                "health": snap["health"],
                "token": snap["token"],
                "latency": snap["latency"],
                "counters": snap["counters"],
                "websocket": snap["websocket"],
                "timestamp_str": snap["timestamp_str"],
            }
            payload = json.dumps(health_data, ensure_ascii=False, default=str)
            yield f"event: health\ndata: {payload}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


async def _sse_lab_generator(
    request: Request,
    interval: float = 1.0,
) -> AsyncGenerator[str, None]:
    """Generate SSE events with real-time lab simulation state."""
    while True:
        if await request.is_disconnected():
            break

        try:
            # Access the sim instance from the app closure
            sim = _global_sim_ref.get("sim")
            if sim:
                data = sim.get_state()
            else:
                data = {"running": False, "strategies": [], "events": []}
            payload = json.dumps(data, ensure_ascii=False, default=str)
            yield f"event: lab\ndata: {payload}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


# Shared ref for SSE generator to access sim instance
_global_sim_ref: dict = {"sim": None}
_surge_sim_ref: dict = {"sim": None}

async def _sse_surge_generator(
    request: Request,
    interval: float = 1.0,
) -> AsyncGenerator[str, None]:
    """Generate SSE events with surge simulation state."""
    while True:
        if await request.is_disconnected():
            break

        try:
            sim = _surge_sim_ref.get("sim")
            if sim and sim.running:
                data = sim.get_state()
                payload = json.dumps(data, ensure_ascii=False, default=str)
                yield f"event: surge\ndata: {payload}\n\n"
            elif sim:
                data = sim.get_state()
                payload = json.dumps(data, ensure_ascii=False, default=str)
                yield f"event: surge\ndata: {payload}\n\n"
            else:
                yield f"event: heartbeat\ndata: {{}}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


# ── Module-level app instance ─────────────────────────────────

app = create_app()


if __name__ == "__main__":
    import uvicorn
    import subprocess

    _port = 8080

    # ── Port conflict auto-resolve ──
    # CREATE_NO_WINDOW 로 netstat/taskkill 의 console window 억제 (2026-04-24).
    _no_window = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, timeout=5,
            creationflags=_no_window,
        )
        for line in result.stdout.splitlines():
            if f":{_port}" in line and "LISTENING" in line:
                parts = line.split()
                pid = int(parts[-1])
                print(f"[PORT] {_port} occupied by PID {pid} — killing...")
                subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                               capture_output=True, timeout=5,
                               creationflags=_no_window)
                import time as _t
                _t.sleep(1)
                print(f"[PORT] PID {pid} killed, proceeding.")
                break
        else:
            print(f"[PORT] {_port} is free.")
    except Exception as e:
        print(f"[PORT] Check failed ({e}), attempting startup anyway.")

    uvicorn.run(
        "web.app:app",
        host="0.0.0.0",
        port=_port,
        reload=True,
        log_level="info",
    )
