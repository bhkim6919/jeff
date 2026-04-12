# -*- coding: utf-8 -*-
"""
app.py -- FastAPI Web Monitoring Server
========================================
Gen04-REST 모니터링 대시보드 백엔드.
SSE(Server-Sent Events)로 실시간 상태를 브라우저에 스트리밍.

Usage:
    cd Gen04-REST
    python -m uvicorn web.app:app --host 0.0.0.0 --port 8080 --reload

    Or programmatically:
    from web.app import create_app
    app = create_app()
"""
from __future__ import annotations

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
_GEN04_STATE_DIR = Path(__file__).resolve().parent.parent.parent / "Gen04" / "state"
_GEN04_REPORT_DIR = Path(__file__).resolve().parent.parent.parent / "Gen04" / "report" / "output"
_GEN04_SECTOR_MAP_PATH = Path(__file__).resolve().parent.parent.parent / "Gen04" / "data" / "sector_map.json"

# Sector map cache (loaded once)
_sector_map_cache: dict = {"data": None, "ts": 0}

# ── DD Guard Thresholds ───────────────────────────────────────
# Source: Gen04/risk/exposure_guard.py lines 159-165
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
    """Wrapper for backward compat — reads file internally."""
    raw = _safe_read_json(str(_GEN04_STATE_DIR / "portfolio_state_live.json"))
    fb = _get_or_fallback("portfolio_state", raw)
    return _compute_dd_guard_from(total_asset, fb)


def _compute_dd_guard_from(total_asset: float, fb: dict) -> dict:
    """Compute DD guard from pre-read fallback data. No file I/O."""
    result = {
        "daily_dd": None, "daily_dd_available": False,
        "monthly_dd": None, "monthly_dd_available": False,
        "level": "UNKNOWN", "buy_permission": "UNKNOWN",
        "config_version": _DD_CONFIG_VERSION,
        "source_ts": 0, "stale": True,
        "source_total_asset": total_asset,
        "source_prev_close": None, "source_peak": None,
        "from_cache": False, "expired": False, "error": None,
    }
    result["from_cache"] = fb.get("from_cache", False)
    result["expired"] = fb.get("expired", False)
    result["error"] = fb.get("error")
    result["source_ts"] = fb.get("source_ts", 0)
    result["stale"] = (time.time() - result["source_ts"]) > 60 if result["source_ts"] else True

    data = fb.get("data")
    if not data or not total_asset or total_asset <= 0:
        return result

    prev_close = data.get("prev_close_equity")
    peak = data.get("peak_equity")
    result["source_prev_close"] = prev_close
    result["source_peak"] = peak

    # P1-3: Equity basis divergence detection
    if prev_close and prev_close > 0 and total_asset > 0:
        equity_diff = abs(total_asset - prev_close) / prev_close
        if equity_diff > 0.01:
            logging.getLogger("web").warning(
                f"[EQUITY_MISMATCH] broker={total_asset:,.0f} engine_prev={prev_close:,.0f} "
                f"diff={equity_diff:.4%} — valuation basis may differ"
            )

    # Partial: daily
    if prev_close and prev_close > 0:
        result["daily_dd"] = round((total_asset - prev_close) / prev_close, 6)
        result["daily_dd_available"] = True

    # Partial: monthly
    if peak and peak > 0:
        result["monthly_dd"] = round((total_asset - peak) / peak, 6)
        result["monthly_dd_available"] = True

    # Determine level from monthly_dd (primary) or daily_dd (fallback)
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


def _compute_recon_status() -> dict:
    """Read RECON status from runtime state."""
    raw = _safe_read_json(str(_GEN04_STATE_DIR / "runtime_state_live.json"))
    fb = _get_or_fallback("runtime_state", raw)
    data = fb.get("data") or {}
    source_ts = fb.get("source_ts", 0)
    age = time.time() - source_ts if source_ts else 99999
    return {
        "unreliable": data.get("recon_unreliable", False),
        "last_run": data.get("timestamp", ""),
        "age_sec": round(age, 1),
        "stale": age > 7200,
        "source": "runtime_state_live.json",
        "from_cache": fb.get("from_cache", False),
        "expired": fb.get("expired", False),
        "error": fb.get("error"),
    }


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
        description="Gen04-REST Trading System Monitoring Dashboard",
        version="1.0.0",
        docs_url="/docs",
    )

    # IP monitor — check every 10 min in background
    import threading
    def _ip_monitor_loop():
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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

    @application.get("/api/state")
    async def get_state():
        """Full state snapshot (JSON). For polling or initial load."""
        return tracker.snapshot()

    # ── Portfolio (live REST API data) ─────────────────
    _provider_cache = {"instance": None}

    def _get_provider():
        if _provider_cache["instance"] is None:
            import sys, os
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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
                    # Also check Gen04 COM state
                    # Runtime state has rebalance date
                    com_rt = Path(__file__).resolve().parent.parent.parent / "Gen04" / "state" / "runtime_state_live.json"
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

            # 1. Load latest target
            target_files = sorted(signals_dir.glob("target_portfolio_*.json"), reverse=True)
            if not target_files:
                return {"error": "No target portfolio found"}
            target = json.loads(target_files[0].read_text(encoding="utf-8"))
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
            days_since = rebal_state.get("days_since_rebal", 0)
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
            log_dir = Path(__file__).resolve().parent.parent.parent / "Gen04" / "data" / "logs"
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

    @application.get("/api/portfolio")
    async def get_portfolio():
        """Fetch live portfolio from Kiwoom REST API (kt00018)."""
        try:
            provider = _get_provider()
            summary = provider.query_account_summary()
            return summary
        except Exception as e:
            return {"error": str(e), "holdings_reliable": False}

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

    @application.get("/api/health")
    async def get_health():
        """Quick health check endpoint."""
        snap = tracker.snapshot()
        return {
            "status": snap["health"]["status"],
            "reason": snap["health"]["reason"],
            "timestamp": snap["timestamp_str"],
        }

    @application.get("/api/advisor/today")
    async def advisor_today():
        """Today's advisor analysis results."""
        try:
            from datetime import datetime
            today = datetime.now().strftime("%Y%m%d")
            advisor_dir = _Path(__file__).resolve().parent.parent / "advisor" / "output" / today
            if not advisor_dir.exists():
                # Try yesterday
                from datetime import timedelta
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

            result = {"date": advisor_dir.name, "status": "OK"}
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
        """Recent trades from trades.csv."""
        return _read_recent_trades(limit=limit)

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
        """state.json이 있으면 자동 복원."""
        if _lab_live_sim.get("sim") and _lab_live_sim["sim"]._initialized:
            return _lab_live_sim["sim"]
        from web.lab_live.config import LabLiveConfig
        cfg = LabLiveConfig()
        if cfg.state_file.exists():
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
            except Exception as e:
                logger.error(f"[LAB_LIVE] Run error: {e}")
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
        return sim.get_state()

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
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
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
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from data.rest_provider import KiwoomRestProvider
        _global_provider_cache["instance"] = KiwoomRestProvider(server_type="REAL")
    return _global_provider_cache["instance"]


async def _sse_generator(
    request: Request,
    interval: float = 2.0,
    event_type: str = "state",
) -> AsyncGenerator[str, None]:
    """Generate SSE events with full state snapshot + dashboard data."""
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
                    summary = provider.query_account_summary()
                    if summary.get("error") is None:
                        _portfolio_cache["data"] = {
                            "holdings_count": len(summary.get("holdings", [])),
                            "cash": summary.get("available_cash", 0),
                            "total_asset": summary.get("추정예탁자산", 0),
                            "total_buy": summary.get("총매입금액", 0),
                            "total_eval": summary.get("총평가금액", 0),
                            "total_pnl": summary.get("총평가손익금액", 0),
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
            if cycle % 10 == 2:
                try:
                    rt_file = _GEN04_STATE_DIR / "runtime_state_live.json"
                    if rt_file.exists():
                        with open(rt_file, "r", encoding="utf-8") as _f:
                            _rt = json.load(_f)
                        _portfolio_cache["rebal"] = _rt.get("last_rebalance_date", "")
                except Exception:
                    pass
            if _portfolio_cache.get("rebal"):
                data["rebalance"] = {"last": _portfolio_cache["rebal"], "cycle": 21}

            # ── DD Guard + Trail Stops + RECON (every 10th cycle, offset 3) ──
            # Single file read for dd_guard + trail_stops (same source)
            if cycle % 10 == 3:
                total_asset = (_portfolio_cache.get("data") or {}).get("total_asset", 0)
                _pstate_raw = _safe_read_json(str(_GEN04_STATE_DIR / "portfolio_state_live.json"))
                _pstate_fb = _get_or_fallback("portfolio_state", _pstate_raw)
                _portfolio_cache["dd_guard"] = _compute_dd_guard_from(total_asset, _pstate_fb)
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
                        # prev_close_equity from Gen04 LIVE state file
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

                    # Phase 1: REST_DB equity 병행 기록 (observer, 격리)
                    try:
                        from web.rest_state_db import sync_equity_snapshot
                        from datetime import date as _date_cls
                        _today = _date_cls.today().isoformat()
                        _dd = _portfolio_cache.get("dd_guard") or {}
                        _is_eod = (_hour >= 15 and _min >= 30)
                        sync_equity_snapshot(
                            market_date=_today,
                            equity=cur_equity,
                            cash=_acct.get("cash", 0),
                            holdings_count=_acct.get("holdings_count", 0),
                            peak_equity=_dd.get("source_peak", 0) or 0,
                            prev_close_equity=_dd.get("source_prev_close", 0) or 0,
                            is_eod=_is_eod,
                            snapshot_id=_portfolio_cache.get("_last_snapshot_id", ""),
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

            # ── Payload metadata ──
            _payload_seq["n"] += 1
            ts_str = _dt.now().strftime("%Y%m%d-%H%M%S")
            data["server_ts"] = now
            data["payload_id"] = f"{ts_str}-{_payload_seq['n']:04d}"
            max_age = max((v.get("age_sec", 0) for v in ds.values()), default=0)
            data["data_age_max_sec"] = round(max_age, 1)
            data["data_sources"] = ds

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
                    rebalance={"last": _portfolio_cache.get("rebal", ""), "cycle": 21},
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
                "rebalance": {"last": _portfolio_cache.get("rebal", ""), "cycle": 21},
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
    try:
        result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            if f":{_port}" in line and "LISTENING" in line:
                parts = line.split()
                pid = int(parts[-1])
                print(f"[PORT] {_port} occupied by PID {pid} — killing...")
                subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                               capture_output=True, timeout=5)
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
