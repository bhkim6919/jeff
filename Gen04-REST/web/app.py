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
import time
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web.api_state import tracker

logger = logging.getLogger("gen4.rest.web")

# ── Paths ─────────────────────────────────────────────────────

WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


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

    @application.get("/api/health")
    async def get_health():
        """Quick health check endpoint."""
        snap = tracker.snapshot()
        return {
            "status": snap["health"]["status"],
            "reason": snap["health"]["reason"],
            "timestamp": snap["timestamp_str"],
        }

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

        # Mutual exclusion: surge simulator
        if _surge_instance["sim"] and _surge_instance["sim"].running:
            return {"error": "Surge sim is running. Stop it first."}

        # Stop existing sim if running
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

    @application.get("/surge", response_class=HTMLResponse)
    async def surge_page(request: Request):
        """Surge trader simulator page."""
        return templates.TemplateResponse(request, "surge.html")

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
        if _sim_instance["sim"] and _sim_instance["sim"].running:
            return {"error": "Lab realtime sim is running. Stop it first."}

        # Stop existing surge sim
        if _surge_instance["sim"] and _surge_instance["sim"].running:
            _surge_instance["sim"].stop()

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

    return application


# ── SSE Generators ────────────────────────────────────────────

_global_provider_cache = {"instance": None}
_portfolio_cache = {"data": None, "ts": 0}

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
    """Generate SSE events with full state snapshot."""
    while True:
        if await request.is_disconnected():
            break

        try:
            data = tracker.snapshot()
            # Inject live portfolio data (every 10th cycle = ~20s)
            if not hasattr(_sse_generator, '_cycle'):
                _sse_generator._cycle = 0
            _sse_generator._cycle += 1
            if _sse_generator._cycle % 10 == 1:  # Refresh every ~20s
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
                        _portfolio_cache["ts"] = time.time()
                except Exception as e:
                    logging.getLogger("web").warning(f"Portfolio fetch: {e}")
            # Always include cached portfolio
            if _portfolio_cache["data"]:
                data["account"] = _portfolio_cache["data"]
            # Rebalance schedule (lightweight, always include)
            if _sse_generator._cycle % 10 == 2:
                try:
                    rt_file = Path(__file__).resolve().parent.parent.parent / "Gen04" / "state" / "runtime_state_live.json"
                    if rt_file.exists():
                        with open(rt_file, "r", encoding="utf-8") as _f:
                            _rt = json.load(_f)
                        _portfolio_cache["rebal"] = _rt.get("last_rebalance_date", "")
                except Exception:
                    pass
            if _portfolio_cache.get("rebal"):
                data["rebalance"] = {"last": _portfolio_cache["rebal"], "cycle": 21}
            payload = json.dumps(data, ensure_ascii=False, default=str)
            yield f"event: {event_type}\ndata: {payload}\n\n"
        except Exception as e:
            error_data = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_data}\n\n"

        await asyncio.sleep(interval)


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
            if sim and sim.has_state_changed():
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
    uvicorn.run(
        "web.app:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        log_level="info",
    )
