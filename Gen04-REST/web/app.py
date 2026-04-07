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

    # CORS (allow local development)
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET"],
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
