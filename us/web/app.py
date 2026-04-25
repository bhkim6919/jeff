# -*- coding: utf-8 -*-
"""
app.py -- FastAPI Dashboard for Q-TRON US
==========================================
US market dashboard with real-time Alpaca data.
Supports KR/US market toggle for cross-market overview.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import time as _time_module
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# sys.path bootstrap — us/ + project root (single source: us/_bootstrap_path.py)
# `-m uvicorn web.app:app` 로 기동해도 main.py를 거치지 않으므로 직접 보장.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # us/  # audit:allow-syspath: bootstrap-locator
import _bootstrap_path  # noqa: F401

from config import USConfig
from data.alpaca_provider import AlpacaProvider
from data.db_provider import DbProviderUS

logger = logging.getLogger("qtron.us.web")

WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

# ── Singletons ───────────────────────────────────────────────

_config = USConfig()
_provider: AlpacaProvider | None = None
_db: DbProviderUS | None = None


def _get_provider() -> AlpacaProvider:
    global _provider
    if _provider is None:
        _provider = AlpacaProvider(_config)
    return _provider


def _get_db() -> DbProviderUS:
    global _db
    if _db is None:
        _db = DbProviderUS()
    return _db


def _get_runtime_data() -> dict:
    """Load latest runtime state for BUY gate evaluation."""
    try:
        from core.state_manager import StateManagerUS
        sm = StateManagerUS(_config.STATE_DIR, _config.TRADING_MODE)
        return sm.load_runtime() or {}
    except Exception:
        return {}


def is_order_success(result: dict) -> bool:
    """Alpaca send_order 결과 성공 여부 판정.

    send_order() 정상 응답: {"order_no": <id>, "exec_qty": 0, "status": "SUBMITTED"}
    send_order() 실패 응답: {"error": "...", "status": "REJECTED"}

    - order_no 존재 + error 없음 → 성공
    - error 존재 OR order_no 없음 → 실패
    - None 또는 비-dict → 실패 (P1 fix: 2026-04-17)
    """
    if not isinstance(result, dict):
        return False
    if result.get("error"):
        return False
    if not result.get("order_no"):
        return False
    return True


# ── App ──────────────────────────────────────────────────────

def create_app() -> FastAPI:
    app = FastAPI(title="Q-TRON US 1.0", docs_url=None, redoc_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if STATIC_DIR.exists():
        # Phase 4-D (2026-04-25): mount shared static at /static/shared
        # before /static so the more specific URL prefix wins. Holds qc-*
        # components used by both KR (:8080) and US (:8081) Dashboards.
        from pathlib import Path as _SP
        _SHARED_STATIC = (_SP(__file__).resolve().parent.parent.parent / "shared" / "web" / "static")
        if _SHARED_STATIC.exists():
            app.mount("/static/shared", StaticFiles(directory=str(_SHARED_STATIC)), name="static_shared")
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # ── Startup: health check (A3) ───────────────────────
    # US 용 critical dep (alpaca, psycopg2, pytz) import 검증
    # REQUIRED 누락 시 RuntimeError → 서버 부팅 중단
    # CRITICAL 누락 시 Telegram + DataEvent + 부팅 허용
    @app.on_event("startup")
    async def _startup_health_check():
        try:
            # kr/tools 는 namespace package 로 접근 (project root 이미 sys.path 에 있음)
            from kr.tools.health_check import run_startup_health_check
            run_startup_health_check(scope="us")
        except RuntimeError:
            raise  # REQUIRED 누락 — 부팅 실패 전파
        except Exception as e:
            logger.warning(f"[HEALTH_CHECK] self-check failed: {e}")

    # ── Pages ────────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse(request, "index.html")

    @app.get("/debug", response_class=HTMLResponse)
    async def debug_page(request: Request):
        """US 전용 독립 DEBUG 페이지 (12개 진단 패널)."""
        return templates.TemplateResponse(request, "debug.html")

    @app.get("/surge", response_class=HTMLResponse)
    async def surge_page(request: Request):
        """US Surge Monitor (Phase 2-E 2026-04-25).

        US side bootstrapped as a standalone page so the Extensions card
        on the Dashboard has somewhere to land. Backend simulator port
        from kr/web/surge/* is deferred — this page renders a clear
        Coming-Soon notice plus a cross-server link to the working KR
        Surge at :8080/surge.
        """
        return templates.TemplateResponse(request, "surge.html")

    # ── API: Health ──────────────────────────────────────

    @app.get("/api/health")
    async def health():
        p = _get_provider()
        clock = p.get_clock() or {}
        connected = p.is_connected()
        return {
            "status": "OK" if connected else "DISCONNECTED",
            "market": "US",
            "server_type": p.server_type,
            "is_market_open": clock.get("is_open", False),
            "next_open": clock.get("next_open", ""),
            "next_close": clock.get("next_close", ""),
        }

    # ── DEBUG: Market Context US (B1) ───────────────────
    # KR 의 market_context 는 KOSPI/OHLCV 기준, US 는 Alpaca broker sync 기준.
    # 구조 대칭을 위해 동일 endpoint prefix + 필드 재해석.
    @app.get("/api/debug/market_context")
    async def debug_market_context_us():
        """
        US DEBUG: broker sync + market clock + portfolio cache 상태.
        - broker_snapshot_age_sec: 마지막 계좌 조회 후 경과
        - portfolio_cache_age_sec: UI 캐시 경과
        - is_market_open: 현재 US 장 여부
        - run_mode: OK | DEGRADED (broker sync > 60분 stale 시 DEGRADED)
        """
        import time as _t
        try:
            p = _get_provider()
            clock = p.get_clock() or {}
            connected = p.is_connected()

            now = _t.time()
            portfolio_age = None
            if _portfolio_cache_us.get("ts"):
                portfolio_age = round(now - _portfolio_cache_us["ts"], 1)

            # broker_snapshot_at 추출 (portfolio cache data 에서)
            broker_snapshot_at = None
            broker_age_sec = None
            pdata = _portfolio_cache_us.get("data") or {}
            if isinstance(pdata, dict):
                broker_snapshot_at = pdata.get("broker_snapshot_at")
                if broker_snapshot_at:
                    try:
                        from datetime import datetime, timezone
                        dt = datetime.fromisoformat(broker_snapshot_at.replace("Z", "+00:00"))
                        broker_age_sec = round((datetime.now(timezone.utc) - dt).total_seconds(), 1)
                    except Exception:
                        pass

            # DEGRADED 판정: broker sync 60분 이상 stale OR disconnected
            reasons = []
            if not connected:
                reasons.append("alpaca_disconnected")
            if broker_age_sec is not None and broker_age_sec > 3600:
                reasons.append(f"broker_snapshot_stale({int(broker_age_sec)}s)")
            run_mode = "OK" if not reasons else "DEGRADED"

            return {
                "ok": True,
                "scope": "us",
                "market_context": {
                    "connected": connected,
                    "server_type": p.server_type,
                    "is_market_open": clock.get("is_open", False),
                    "next_open": clock.get("next_open", ""),
                    "next_close": clock.get("next_close", ""),
                    "broker_snapshot_at": broker_snapshot_at,
                    "broker_snapshot_age_sec": broker_age_sec,
                    "portfolio_cache_age_sec": portfolio_age,
                    "run_mode": run_mode,
                    "degraded_reasons": reasons,
                },
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DEBUG: Log tail (D-LOG) ──────────────────────────
    @app.get("/api/debug/logs")
    async def debug_logs(lines: int = Query(50, ge=1, le=500)):
        """us_app_YYYYMMDD.log 의 최근 N 줄."""
        try:
            from datetime import datetime
            from pathlib import Path as _P
            log_dir = _P(__file__).resolve().parent.parent / "logs"
            today = datetime.now().strftime("%Y%m%d")
            log_file = log_dir / f"us_app_{today}.log"
            if not log_file.exists():
                # 전날 파일 fallback
                yday = (datetime.now().replace(hour=0) -
                        _time_module.timedelta(days=1)).strftime("%Y%m%d") if False else ""
                return {"ok": False, "reason": "no log file today", "lines": []}
            # 효율적 tail (파일 전체 읽음 — 작은 파일 가정, 10MB 이하)
            content = log_file.read_text(encoding="utf-8", errors="replace")
            all_lines = content.splitlines()
            tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return {"ok": True, "file": log_file.name, "total_lines": len(all_lines), "lines": tail}
        except Exception as e:
            return {"ok": False, "error": str(e), "lines": []}

    # ── DEBUG: Raw state (D-RAW) ─────────────────────────
    @app.get("/api/debug/raw_state")
    async def debug_raw_state():
        """Status summary 를 raw JSON 으로. UI 에서 복사 가능."""
        try:
            # status summary 와 동일 데이터 (cache 에서)
            now = _time_module.time()
            data = _status_cache.get("data") or {}
            age = round(now - _status_cache.get("ts", 0), 1)
            return {"ok": True, "cache_age_sec": age, "summary": data}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DEBUG: System status (D-SYS) ─────────────────────
    @app.get("/api/debug/sys")
    async def debug_sys():
        """Alpaca system status — account / clock / circuit breaker / rate limit."""
        try:
            p = _get_provider()
            clock = p.get_clock() or {}
            acct = p.query_account_summary() or {}

            # Circuit breaker state
            cb_halted = p._cb_is_halted() if hasattr(p, "_cb_is_halted") else False
            cb_remain = 0
            cb_errors = 0
            if hasattr(p, "_cb_halt_until") and hasattr(p, "_cb_consecutive_auth_err"):
                cb_halt_until = getattr(p, "_cb_halt_until", 0)
                cb_remain = max(0, int(cb_halt_until - _time_module.time())) if cb_halted else 0
                cb_errors = getattr(p, "_cb_consecutive_auth_err", 0)

            # Recent trace summary for rate-limit / latency indicator
            hist = p.get_latency_histogram() if hasattr(p, "get_latency_histogram") else {}

            # API key (masked)
            key_val = getattr(p, "_api_key", "") or ""
            key_masked = (key_val[:4] + "****" + key_val[-4:]) if len(key_val) > 8 else "(short)"

            return {
                "ok": True,
                "api_key": key_masked,
                "server_type": getattr(p, "server_type", "?"),
                "base_url": getattr(p, "_base_url", ""),
                "connected": p.is_connected(),
                "clock": clock,
                "account": {
                    "equity": acct.get("equity"),
                    "cash": acct.get("cash"),
                    "buying_power": acct.get("buying_power"),
                },
                "circuit_breaker": {
                    "halted": cb_halted,
                    "remaining_sec": cb_remain,
                    "consecutive_auth_err": cb_errors,
                },
                "latency": {
                    "avg_ms": hist.get("avg"),
                    "p50_ms": hist.get("p50"),
                    "p95_ms": hist.get("p95"),
                    "sample_count": hist.get("count", 0),
                },
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DEBUG: Request traces (D-TRACE) ──────────────────
    @app.get("/api/debug/traces")
    async def debug_traces(limit: int = Query(50, ge=1, le=200)):
        """Alpaca API 호출 최근 N건 (method / path / status / latency)."""
        try:
            p = _get_provider()
            if not hasattr(p, "get_traces"):
                return {"ok": False, "reason": "tracer not available", "traces": []}
            return {"ok": True, "traces": p.get_traces(limit)}
        except Exception as e:
            return {"ok": False, "error": str(e), "traces": []}

    # ── DEBUG: Latency histogram (D-HIST) ────────────────
    @app.get("/api/debug/histogram")
    async def debug_histogram(bucket_ms: int = Query(100, ge=10, le=1000)):
        """Alpaca request latency 분포 (bucket_ms 단위)."""
        try:
            p = _get_provider()
            if not hasattr(p, "get_latency_histogram"):
                return {"ok": False, "reason": "histogram not available"}
            return {"ok": True, **p.get_latency_histogram(bucket_ms)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DEBUG: WebSocket status (D-WS) ───────────────────
    @app.get("/api/debug/ws")
    async def debug_ws():
        """
        Alpaca Data Stream (WebSocket) 상태.
        us/data/alpaca_data.py 의 스트림 인스턴스 찾아서 상태 보고.
        """
        try:
            # AlpacaData / stream 인스턴스 위치는 프로젝트마다 다름 — best effort
            info = {"available": False, "reason": "data stream module not initialized"}
            try:
                from data import alpaca_data as _ad  # type: ignore
                if hasattr(_ad, "_stream_instance"):
                    s = _ad._stream_instance
                    info = {
                        "available": True,
                        "connected": getattr(s, "connected", None),
                        "subscriptions": list(getattr(s, "subscriptions", []) or []),
                        "last_msg_ts": getattr(s, "last_msg_ts", None),
                        "msg_count": getattr(s, "msg_count", 0),
                        "reconnects": getattr(s, "reconnects", 0),
                    }
                else:
                    info = {"available": False, "reason": "alpaca_data has no _stream_instance attribute"}
            except ImportError:
                pass
            return {"ok": True, **info}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ── DEBUG: Portfolio state diff (D-DIFF) ─────────────
    # 이전 snapshot 과 현재 snapshot 을 비교해 변경된 키 표시
    _diff_prev = {"portfolio": None}

    @app.get("/api/debug/state_diff")
    async def debug_state_diff():
        """Portfolio snapshot diff — 이전 호출 대비 현재 값 변경 키."""
        try:
            now = _time_module.time()
            curr = _portfolio_cache_us.get("data") or {}
            prev = _diff_prev.get("portfolio") or {}

            changes = []
            # 최상위 key 레벨 단순 diff
            all_keys = set(list(prev.keys()) + list(curr.keys()))
            for k in sorted(all_keys):
                pv = prev.get(k)
                cv = curr.get(k)
                if pv != cv:
                    # value 가 리스트나 dict 일 때는 전체 값 대신 "changed" 표시
                    pv_short = _diff_short(pv)
                    cv_short = _diff_short(cv)
                    changes.append({"key": k, "prev": pv_short, "curr": cv_short})

            # 다음 호출을 위해 현재를 prev 로 저장
            _diff_prev["portfolio"] = dict(curr) if isinstance(curr, dict) else curr

            return {
                "ok": True,
                "changes": changes,
                "n_changes": len(changes),
                "snapshot_ts": now,
                "portfolio_cache_age_sec": (
                    round(now - _portfolio_cache_us["ts"], 1)
                    if _portfolio_cache_us.get("ts") else None
                ),
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _diff_short(v):
        """diff 표시용 값 축약."""
        if v is None:
            return None
        if isinstance(v, (int, float, str, bool)):
            s = str(v)
            return s if len(s) <= 60 else s[:57] + "..."
        if isinstance(v, list):
            return f"<list len={len(v)}>"
        if isinstance(v, dict):
            return f"<dict keys={len(v)}>"
        return f"<{type(v).__name__}>"

    # ── DEBUG: Data Events (A5) ──────────────────────────
    # KR 과 동일한 tracker 공유 (shared/data_events.py), 단 프로세스 분리로 이벤트 separate.
    @app.get("/api/debug/data_events")
    async def debug_data_events(
        limit: int = Query(50, ge=1, le=200),
        min_level: str = Query("", description="DEBUG|INFO|WARN|ERROR|CRITICAL"),
        sources: str = Query("", description="comma-separated substring match"),
    ):
        """US DEBUG: 최근 데이터/환경 이벤트 + active escalation 상태."""
        try:
            from shared.data_events import get_events, get_escalation_states
            src_list = [s.strip() for s in sources.split(",") if s.strip()] if sources else None
            return {
                "ok": True,
                "events": get_events(
                    limit=limit,
                    min_level=min_level or None,
                    sources=src_list,
                ),
                "active_escalations": get_escalation_states(),
                "scope": "us",
            }
        except Exception as e:
            return {"ok": False, "error": str(e), "events": []}

    # ── API: Status Summary (KR-style status bar) ─────

    _status_cache = {"data": None, "ts": 0}

    # ENGINE SAFETY: _portfolio_cache_us → UI only (UI_CACHE)
    # ❌ 주문 판단에 cache 사용 금지. broker API 직접 조회만 판단 기준.
    # 방향: broker → engine → _portfolio_cache_us → UI  (역방향 금지)
    _portfolio_cache_us = {"data": None, "ts": 0.0}
    PORTFOLIO_CACHE_TTL_US = 8  # 초 (polling 10초보다 짧게)

    @app.get("/api/status/summary")
    async def status_summary():
        """Consolidated status for the dashboard status bar."""
        import time as _time
        from datetime import datetime as _dt

        now = _time.time()
        # Cache 5s to avoid hammering APIs
        if _status_cache["data"] and now - _status_cache["ts"] < 5:
            return _status_cache["data"]

        p = _get_provider()
        clock = p.get_clock() or {}
        connected = p.is_connected()

        # Account
        acct = {}
        try:
            acct = p.query_account_summary() or {}
        except Exception:
            pass

        # Holdings count
        n_holdings = 0
        try:
            holdings = p.query_account_holdings() or []
            n_holdings = len(holdings)
        except Exception:
            pass

        # BUY gate
        buy_gate, buy_reason, buy_scale = "NORMAL", "", 1.0
        try:
            from strategy.execution_gate import check_buy_permission
            _rt = _get_runtime_data()
            allowed, reason, scale = check_buy_permission(_config, _rt, p)
            buy_gate = "NORMAL" if allowed else "BLOCKED"
            buy_reason = "" if allowed else reason
            buy_scale = scale
        except Exception:
            buy_gate = "UNKNOWN"

        # Regime (from cache if available)
        regime_label, regime_level = "N/A", 0
        spy_price, spy_change = 0, 0
        try:
            if _regime_cache["data"] and now - _regime_cache["ts"] < 120:
                rd = _regime_cache["data"]
            else:
                from regime.collector import collect_market_data
                from regime.predictor import predict_regime
                from regime.actual import calculate_actual
                market_data = collect_market_data(provider=p)
                # v2: EMA + persistence from runtime_state
                _sm = _get_state_mgr()
                _rt = _sm.load_runtime() or {}
                _pred = predict_regime(
                    market_data,
                    prev_ema_score=_rt.get("prev_regime_ema"),
                    prev_regime=_rt.get("prev_regime_level"),
                )
                # Save EMA state
                _sm.update_rebal_state({
                    "prev_regime_ema": _pred["ema_score"],
                    "prev_regime_level": _pred["predicted_regime"],
                })
                rd = {
                    "today": calculate_actual(market_data),
                    "prediction": _pred,
                }
            today = rd.get("today") or {}
            pred = rd.get("prediction") or {}
            if today.get("available"):
                regime_label = today.get("actual_label", "N/A")
                regime_level = today.get("actual_regime", 0)
                spy_price = today.get("spy_price", 0)
                spy_change = today.get("spy_change_pct", 0)
            else:
                regime_label = pred.get("predicted_label", "N/A")
                regime_level = pred.get("predicted_regime", 0)
        except Exception:
            pass

        # Health reason
        reasons = []
        if not connected:
            reasons.append("Broker disconnected")
        if buy_gate == "BLOCKED":
            reasons.append(f"BUY blocked: {buy_reason}")

        if connected and not reasons:
            status = "OK"
            status_title = "Connected"
            status_reason = "System nominal"
        elif connected and reasons:
            status = "WARN"
            status_title = "Warning"
            status_reason = "; ".join(reasons)
        else:
            status = "ERROR"
            status_title = "Disconnected"
            status_reason = "; ".join(reasons) if reasons else "Broker not reachable"

        # P2: Auto Trading State (advisory)
        auto_state = {"enabled": False, "blockers": ["UNEVALUATED"],
                      "risk_level": "UNKNOWN", "strategy_health": "UNKNOWN",
                      "buy_scale": buy_scale, "reason_summary": ""}
        try:
            from risk.auto_trading_gate import compute_auto_trading_state
            from risk.strategy_health import compute_strategy_health
            _rt_for_gate = _get_runtime_data() or {}
            _equity_dd = float(_rt_for_gate.get("equity_dd_pct", 0.0) or 0.0)
            _health = compute_strategy_health(equity_dd_pct=_equity_dd)
            _auto = compute_auto_trading_state(
                runtime=_rt_for_gate, strategy_health=_health,
            )
            auto_state = _auto.to_dict()
            auto_state["strategy_health_detail"] = _health
        except Exception as _ae:
            auto_state["blockers"] = [f"EVAL_ERROR:{type(_ae).__name__}"]

        result = {
            "status": status,
            "status_title": status_title,
            "status_reason": status_reason,
            "server_type": getattr(p, "server_type", "PAPER"),
            "connected": connected,
            "is_market_open": clock.get("is_open", False),
            "next_open": clock.get("next_open", ""),
            "next_close": clock.get("next_close", ""),
            "last_refresh": _dt.now().strftime("%H:%M:%S"),
            "spy": {"price": round(spy_price, 2), "change_pct": round(spy_change, 2)},
            "buy_gate": buy_gate,
            "buy_reason": buy_reason,
            "buy_scale": buy_scale,
            "regime_label": regime_label,
            "regime_level": regime_level,
            "n_holdings": n_holdings,
            "equity": round(acct.get("equity", 0), 2),
            "auto_trading": auto_state,
        }

        _status_cache["data"] = result
        _status_cache["ts"] = now
        return result

    # ── API: Account / Portfolio ─────────────────────────

    @app.get("/api/account")
    async def account():
        p = _get_provider()
        return await asyncio.to_thread(p.query_account_summary)

    @app.get("/api/portfolio")
    async def portfolio():
        now = _time_module.time()
        # cache hit → copy 반환 (mutable 공유 + 2차 오염 차단)
        if _portfolio_cache_us["data"] and now - _portfolio_cache_us["ts"] < PORTFOLIO_CACHE_TTL_US:
            result = dict(_portfolio_cache_us["data"])
            result["cache_age_sec"] = round(now - _portfolio_cache_us["ts"], 1)
            return result

        # cache miss → live fetch (non-blocking)
        p = _get_provider()
        acct     = await asyncio.to_thread(p.query_account_summary)
        holdings = await asyncio.to_thread(p.query_account_holdings)
        result = {
            "market": "US",
            "equity": acct.get("equity", 0),
            "last_equity": acct.get("last_equity", 0),  # 전일 종가 기준 equity
            "cash": acct.get("cash", 0),
            "buying_power": acct.get("buying_power", 0),
            "portfolio_value": acct.get("portfolio_value", 0),
            "n_holdings": len(holdings),
            "holdings": holdings,
            "_data_source": "UI_CACHE",
            "cache_age_sec": 0.0,
        }
        _portfolio_cache_us["data"] = result
        _portfolio_cache_us["ts"] = now
        return dict(result)  # 저장 후에도 copy 반환

    # ── API: Target Portfolio ────────────────────────────

    @app.get("/api/target")
    async def target():
        db = _get_db()
        t = db.get_target_portfolio()
        if not t:
            return {"error": "no target portfolio"}
        return t

    # ── API: Price ───────────────────────────────────────

    @app.get("/api/price/{symbol}")
    async def price(symbol: str):
        p = _get_provider()
        px = p.get_current_price(symbol.upper())
        return {"symbol": symbol.upper(), "price": px}

    # ── API: Orders ──────────────────────────────────────

    @app.get("/api/orders/open")
    async def open_orders():
        p = _get_provider()
        orders = p.query_open_orders()
        return {"orders": orders or []}

    @app.post("/api/test/buy")
    async def test_buy(request: Request):
        body = await request.json()
        symbol = body.get("symbol", "").upper()
        qty = int(body.get("qty", 1))

        # BUY gate check (same gate as rebalance/auto)
        from strategy.execution_gate import check_buy_permission
        _rt = _get_runtime_data()
        allowed, reason, scale = check_buy_permission(_config, _rt, _get_provider())
        if not allowed:
            return {"ok": False, "error": f"BUY blocked: {reason}"}

        p = _get_provider()
        result = p.send_order(symbol, "BUY", qty)
        return result

    @app.post("/api/test/sell")
    async def test_sell(request: Request):
        body = await request.json()
        symbol = body.get("symbol", "").upper()
        qty = int(body.get("qty", 1))
        p = _get_provider()
        result = p.send_order(symbol, "SELL", qty)
        return result

    # ── API: Rebalance ──────────────────────────────────

    @app.get("/api/rebalance/preview")
    async def rebalance_preview():
        """Preview rebalance: show sells + buys without executing."""
        try:
            p = _get_provider()
            db = _get_db()
            target = db.get_target_portfolio()
            if not target:
                return {"ok": False, "error": "No target portfolio"}

            acct = p.query_account_summary()
            if "error" in acct:
                return {"ok": False, "error": "Account query failed"}

            holdings = p.query_account_holdings()
            current = {h["code"]: h for h in holdings}
            equity = acct.get("equity", 0)
            cash = acct.get("cash", 0)

            # Get current prices
            prices = {}
            all_syms = set(current.keys()) | set(target.get("target_tickers", []))
            for sym in all_syms:
                px = p.get_current_price(sym)
                if px > 0:
                    prices[sym] = px

            from strategy.rebalancer import compute_orders
            sells, buys = compute_orders(
                current, target["target_tickers"],
                equity, cash,
                _config.BUY_COST, _config.SELL_COST,
                prices, _config.CASH_BUFFER_RATIO,
            )

            # BUY gate check
            from strategy.execution_gate import check_buy_permission
            _rt = _get_runtime_data()
            buy_allowed, buy_reason, buy_scale = check_buy_permission(_config, _rt, p)

            # ── Categorize: 청산(exits) / 신규(new_entries) / 유지(keeps) ──
            # 방어: 심볼 정규화 (대문자 + strip), 타겟이 dict일 수도 있음
            def _norm(s):
                if isinstance(s, dict):
                    s = s.get("ticker") or s.get("symbol") or s.get("code") or ""
                return str(s).strip().upper()

            held_norm = {_norm(k): k for k in current.keys() if _norm(k)}
            raw_tgt = target.get("target_tickers") or target.get("tickers") or []
            tgt_norm = {_norm(t): t for t in raw_tgt if _norm(t)}

            held = set(held_norm.keys())
            tgt = set(tgt_norm.keys())
            exit_syms = sorted(held - tgt)        # 보유했지만 타겟 아님 → 청산
            new_syms = sorted(tgt - held)         # 타겟이지만 미보유 → 신규
            keep_syms = sorted(held & tgt)        # 둘 다 → 유지

            # ── DEBUG LOG: 0/0/0 디버깅용 ──────────────────────────
            import logging as _log
            _logger = _log.getLogger("us.web.app")
            _logger.warning(
                f"[REBAL_PREVIEW_DBG] held_count={len(held)} tgt_count={len(tgt)} "
                f"exits={len(exit_syms)} new={len(new_syms)} keeps={len(keep_syms)} "
                f"holdings_sample={list(current.keys())[:5]} "
                f"target_sample={list(raw_tgt)[:5] if raw_tgt else []} "
                f"target_keys={list(target.keys())} "
                f"intersection_sample={keep_syms[:5]}"
            )

            def _row(norm_sym):
                orig = held_norm.get(norm_sym, norm_sym)
                h = current.get(orig, {}) or {}
                qty = h.get("quantity", 0) or h.get("qty", 0)
                px = prices.get(orig, 0) or prices.get(norm_sym, 0)
                avg = h.get("avg_price", 0) or h.get("buy_price", 0)
                mv = round(qty * px, 2)
                pnl_pct = round((px / avg - 1) * 100, 2) if avg else 0
                return {
                    "symbol": norm_sym, "qty": qty, "price": round(px, 2),
                    "avg_price": round(avg, 2), "market_value": mv, "pnl_pct": pnl_pct,
                }

            exits = [_row(s) for s in exit_syms]

            # 신규 종목: 가격 + 오늘 등락률 (전일 종가 대비)
            new_entries = []
            for s in new_syms:
                orig = tgt_norm.get(s, s)
                px = prices.get(orig, 0) or prices.get(s, 0)
                # daily change % via snapshot API
                try:
                    chg = p.get_daily_change_pct(orig) if hasattr(p, "get_daily_change_pct") else 0.0
                except Exception:
                    chg = 0.0
                new_entries.append({
                    "symbol": s,
                    "price": round(px, 2),
                    "change_pct": chg,
                })

            keeps = [_row(s) for s in keep_syms]

            return {
                "ok": True,
                "target_date": target.get("date", ""),
                "target_count": len(target.get("target_tickers", [])),
                "sells": [{"symbol": s.ticker, "qty": s.quantity, "reason": s.reason} for s in sells],
                "buys": [{"symbol": b.ticker, "qty": b.quantity, "amount": round(b.target_amount, 2), "reason": b.reason} for b in buys],
                "exits": exits,
                "new_entries": new_entries,
                "keeps": keeps,
                "keep": len(keep_syms),
                "equity": round(equity, 2),
                "cash": round(cash, 2),
                "buy_allowed": buy_allowed,
                "buy_reason": buy_reason,
                "buy_scale": buy_scale,
                "_debug": {
                    "held_count": len(held),
                    "tgt_count": len(tgt),
                    "exits_n": len(exit_syms),
                    "new_n": len(new_syms),
                    "keeps_n": len(keep_syms),
                    "holdings_sample": list(current.keys())[:5],
                    "target_sample": [_norm(t) for t in raw_tgt[:5]] if raw_tgt else [],
                    "target_keys": list(target.keys()),
                },
            }
        except Exception as e:
            import traceback as _tb
            return {"ok": False, "error": str(e), "_traceback": _tb.format_exc()[:500]}

    # (legacy execute v1 제거됨 — v2 at /api/rebalance/execute 사용)

    # ── API: DB Health ───────────────────────────────────

    @app.get("/api/db/health")
    async def db_health():
        db = _get_db()
        return db.health_check()

    # ── API: KR Market (Cross-market overlay) ────────────

    @app.get("/api/kr/portfolio")
    async def kr_portfolio():
        """Proxy to kr dashboard for KR data."""
        try:
            import requests
            resp = requests.get("http://localhost:8080/api/portfolio", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                data["market"] = "KR"
                return data
        except Exception:
            pass
        return {"market": "KR", "error": "kr not available"}

    # ── API: Regime ─────────────────────────────────────

    _regime_cache = {"data": None, "ts": 0}

    @app.get("/api/trades")
    async def api_trades(
        start: str = Query("", description="YYYY-MM-DD"),
        end: str = Query("", description="YYYY-MM-DD"),
        symbol: str = Query("", description="ticker"),
        side: str = Query("", description="BUY|SELL"),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        """US trade history — Phase 4-B.2 (2026-04-25). Mirrors KR
        /api/trades shape sourced from trades_us PG table.

        Returns:
            { total, limit, offset, trades: [...] }
        Trade row shape:
            { id, date, symbol, side, quantity, price, cost,
              reason, mode, order_id, fill_key }
        """
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                where, params = ["1=1"], []
                if start:
                    where.append("date::date >= %s"); params.append(start)
                if end:
                    where.append("date::date <= %s"); params.append(end)
                if symbol:
                    where.append("symbol = %s"); params.append(symbol.upper())
                if side:
                    where.append("side = %s"); params.append(side.upper())
                w = " AND ".join(where)
                cur.execute(f"SELECT COUNT(*) FROM trades_us WHERE {w}", params)
                total = cur.fetchone()[0]
                if total == 0:
                    cur.close()
                    return {"total": 0, "limit": limit, "offset": offset, "trades": []}
                cur.execute(
                    f"SELECT id, date, symbol, side, quantity, price, cost, "
                    f"reason, mode, order_id, fill_key "
                    f"FROM trades_us WHERE {w} "
                    f"ORDER BY date DESC, id DESC "
                    f"LIMIT %s OFFSET %s",
                    params + [limit, offset],
                )
                cols = [d[0] for d in cur.description]
                rows = []
                for r in cur.fetchall():
                    row = dict(zip(cols, r))
                    # Normalize date to ISO string + add code alias for KR-style consumers
                    if row.get("date"):
                        row["date"] = row["date"].isoformat()
                    row["code"] = row.get("symbol")
                    rows.append(row)
                cur.close()
                return {
                    "total": total, "limit": limit, "offset": offset,
                    "trades": rows,
                }
        except Exception as e:
            return {"error": str(e), "trades": [], "total": 0}

    @app.get("/api/trades/summary")
    async def api_trades_summary(
        start: str = Query("", description="YYYY-MM-DD"),
        end: str = Query("", description="YYYY-MM-DD"),
    ):
        """US trade summary statistics — Phase 4-B.2.

        Returns counts and aggregates over the filter window:
            { buy_count, sell_count, total_count, total_quantity,
              total_cost, first_date, last_date }
        """
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                where, params = ["1=1"], []
                if start:
                    where.append("date::date >= %s"); params.append(start)
                if end:
                    where.append("date::date <= %s"); params.append(end)
                w = " AND ".join(where)
                cur.execute(
                    f"SELECT "
                    f"  COUNT(*) FILTER (WHERE side='BUY'), "
                    f"  COUNT(*) FILTER (WHERE side='SELL'), "
                    f"  COUNT(*), "
                    f"  COALESCE(SUM(quantity), 0), "
                    f"  COALESCE(SUM(cost), 0), "
                    f"  MIN(date), MAX(date) "
                    f"FROM trades_us WHERE {w}",
                    params,
                )
                r = cur.fetchone() or (0, 0, 0, 0, 0.0, None, None)
                cur.close()
                return {
                    "buy_count": int(r[0] or 0),
                    "sell_count": int(r[1] or 0),
                    "total_count": int(r[2] or 0),
                    "total_quantity": int(r[3] or 0),
                    "total_cost": float(r[4] or 0),
                    "first_date": r[5].isoformat() if r[5] else None,
                    "last_date": r[6].isoformat() if r[6] else None,
                }
        except Exception as e:
            return {
                "buy_count": 0, "sell_count": 0, "total_count": 0,
                "total_quantity": 0, "total_cost": 0.0,
                "first_date": None, "last_date": None,
                "error": str(e),
            }

    @app.get("/api/charts/equity-unified")
    async def get_equity_unified(days: int = Query(90, ge=7, le=730)):
        """US unified equity curve — Phase 4-C (2026-04-25): mirror of
        kr/web/app.py /api/charts/equity-unified contract so the future
        US Analytics card (Phase 4-B) can render the same Equity Curve
        chart shipped in the KR Dashboard.

        Series shape (same as KR):
            { "live":  { label, kind, pct: [...] },
              "spy":   { label: "SPY", kind: "benchmark", pct: [...] } }

        US has no Lab 9-strategy framework, so only `live` + `spy`
        series ship. Frontend (Phase 4-B port) handles missing strategy
        keys gracefully.

        Source:
        - LIVE equity: equity_history_us (date, equity)
        - SPY benchmark: equity_history_us.spy_close column (recorded
          daily by EOD job).

        When the table is empty (current state — daily commits land
        once US live mode is on for a few days), returns error sentinel
        which the chart component shows as "not enough data yet".
        """
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT date, equity, spy_close FROM equity_history_us "
                    "ORDER BY date ASC"
                )
                live_rows = cur.fetchall()
                cur.close()
            if not live_rows:
                return {
                    "error": "no_us_live_data",
                    "days": days,
                    "baseline_date": None,
                    "dates": [],
                    "series": {},
                    "data_quality": "DEGRADED",
                }
            live_map = {str(r[0]): float(r[1]) for r in live_rows}
            spy_map = {
                str(r[0]): float(r[2]) for r in live_rows if r[2] is not None
            }

            dates_sorted = sorted(live_map.keys())
            if days and len(dates_sorted) > days:
                dates_sorted = dates_sorted[-days:]
            baseline_date = dates_sorted[0]

            def _pct_series(values: dict, dates: list) -> list:
                base = values.get(baseline_date)
                if base is None:
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

            series = {
                "live": {
                    "label": "Gen4 LIVE",
                    "kind": "live",
                    "pct": _pct_series(live_map, dates_sorted),
                },
                "spy": {
                    "label": "SPY",
                    "kind": "benchmark",
                    "pct": _pct_series(spy_map, dates_sorted),
                },
            }
            return {
                "days": days,
                "baseline_date": baseline_date,
                "dates": dates_sorted,
                "series": series,
                "data_quality": "OK",
            }
        except Exception as e:
            return {
                "error": str(e),
                "days": days,
                "series": {},
                "dates": [],
                "data_quality": "DEGRADED",
            }

    @app.get("/api/regime/current")
    async def regime_current():
        """Today's actual + tomorrow's prediction + sectors."""
        import time as _time
        now = _time.time()

        # Cache 60s
        if _regime_cache["data"] and now - _regime_cache["ts"] < 60:
            return _regime_cache["data"]

        try:
            from regime.collector import collect_market_data
            from regime.predictor import predict_regime
            from regime.actual import calculate_actual
            from regime.models import REGIME_COLORS, SECTOR_ETFS

            p = _get_provider()
            market_data = collect_market_data(provider=p)

            today = calculate_actual(market_data)
            # v2: EMA + persistence
            _sm2 = _get_state_mgr()
            _rt2 = _sm2.load_runtime() or {}
            prediction = predict_regime(
                market_data,
                prev_ema_score=_rt2.get("prev_regime_ema"),
                prev_regime=_rt2.get("prev_regime_level"),
            )
            _sm2.update_rebal_state({
                "prev_regime_ema": prediction["ema_score"],
                "prev_regime_level": prediction["predicted_regime"],
            })

            # Sector regime from collected data
            sectors = []
            for sym, info in market_data.get("sectors", {}).items():
                chg = info.get("change_pct", 0)
                if chg > 1.0:
                    sr = "BULL"
                elif chg > 0:
                    sr = "MILDLY BULL"
                elif chg > -1.0:
                    sr = "MILDLY BEAR"
                else:
                    sr = "BEAR"
                sectors.append({
                    "symbol": sym,
                    "name": info.get("name", sym),
                    "change_pct": round(chg, 2),
                    "regime": sr,
                })
            sectors.sort(key=lambda x: -x["change_pct"])

            # Enrich sectors with portfolio holdings
            # yfinance sector names → SECTOR_ETFS display names
            _SECTOR_ALIAS = {
                "Financial Services": "Financials",
                "Healthcare": "Health Care",
                "Consumer Cyclical": "Consumer Disc.",
                "Consumer Defensive": "Consumer Staples",
                "Communication Services": "Communication",
                "Basic Materials": "Materials",
            }
            try:
                from data.db_provider import DbProviderUS
                db = DbProviderUS()
                sector_map = db.get_sector_map()
                holdings = p.query_account_holdings() if p.is_connected() else []

                # Group holdings by display sector name
                hld_by_sector = {}
                for h in holdings:
                    sym = h.get("code", "")
                    raw_sec = sector_map.get(sym, {}).get("sector", "")
                    sec = _SECTOR_ALIAS.get(raw_sec, raw_sec)
                    if sec:
                        hld_by_sector.setdefault(sec, []).append({
                            "symbol": sym,
                            "name": sector_map.get(sym, {}).get("name", sym),
                            "pnl_pct": round(h.get("pnl_pct", 0), 2),
                        })
                # Attach to each sector card
                for s in sectors:
                    s["holdings"] = hld_by_sector.get(s["name"], [])
                    s["holdings_count"] = len(s["holdings"])
            except Exception as e:
                logger.warning(f"[REGIME] Holdings enrichment failed: {e}")

            # Phase 4-A.4 (2026-04-25): expose `tomorrow` as the unified
            # field name per docs/ui_data_contract_20260424.md §4.
            # `prediction` retained as legacy alias for current consumers
            # (us regime.us.js component, etc.).
            result = {
                "today": today,
                "tomorrow": prediction,
                "prediction": prediction,  # legacy alias
                "sectors": sectors,
                "breadth": market_data.get("breadth", {}),
                "colors": REGIME_COLORS,
            }

            _regime_cache["data"] = result
            _regime_cache["ts"] = now
            return result

        except Exception as e:
            logger.error(f"[REGIME] Error: {e}")
            return {"error": str(e)}

    # ── API: Sector Detail (on-demand, like KR /api/theme/{code}) ──

    _sector_detail_cache = {}  # {sector_name: {"data": [...], "ts": float}}

    @app.get("/api/sector/{sector_name}")
    async def sector_detail(sector_name: str):
        """Get stocks in a sector with today's change %. Cached 5 min."""
        import time as _time
        import urllib.parse
        decoded = urllib.parse.unquote(sector_name)
        now = _time.time()

        cached = _sector_detail_cache.get(decoded)
        if cached and now - cached["ts"] < 300:
            return cached["data"]

        try:
            from data.db_provider import DbProviderUS
            db = DbProviderUS()
            sector_map = db.get_sector_map()

            # yfinance sector → display name alias (reverse)
            _ALIAS_REV = {
                "Financials": "Financial Services",
                "Health Care": "Healthcare",
                "Consumer Disc.": "Consumer Cyclical",
                "Consumer Staples": "Consumer Defensive",
                "Communication": "Communication Services",
                "Materials": "Basic Materials",
            }
            db_sector = _ALIAS_REV.get(decoded, decoded)

            # Find stocks in this sector
            symbols = [sym for sym, info in sector_map.items()
                       if info.get("sector") == db_sector]

            if not symbols:
                return {"sector": decoded, "stocks": [], "count": 0}

            # Get today's prices from Alpaca
            p = _get_provider()
            stocks = []
            try:
                from alpaca.data.requests import StockSnapshotRequest
                from alpaca.data import StockHistoricalDataClient
                # Use simple REST call for snapshots
                import requests as _req
                from config import USConfig
                cfg = USConfig()
                headers = {
                    "APCA-API-KEY-ID": cfg.ALPACA_API_KEY,
                    "APCA-API-SECRET-KEY": cfg.ALPACA_SECRET_KEY,
                }
                # Batch snapshots (max ~200 at a time)
                for i in range(0, len(symbols), 100):
                    batch = symbols[i:i+100]
                    resp = _req.get(
                        f"{cfg.ALPACA_DATA_URL}/v2/stocks/snapshots",
                        headers=headers,
                        params={"symbols": ",".join(batch)},
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        snaps = resp.json()
                        for sym, snap in snaps.items():
                            dc = snap.get("dailyBar", {})
                            pdc = snap.get("prevDailyBar", {})
                            close = dc.get("c", 0)
                            prev_close = pdc.get("c", 0)
                            # fallback: dailyBar 미갱신 시 latestTrade 사용
                            if close and prev_close and close == prev_close:
                                lt = snap.get("latestTrade", {})
                                if lt.get("p", 0) > 0:
                                    close = lt["p"]
                            chg = ((close / prev_close - 1) * 100) if prev_close > 0 else 0
                            name = sector_map.get(sym, {}).get("name", sym)
                            stocks.append({
                                "symbol": sym,
                                "name": name,
                                "change_pct": round(chg, 2),
                            })
            except Exception as e:
                logger.warning(f"[SECTOR_DETAIL] Snapshot fetch failed: {e}")
                # Fallback: return symbols without price
                for sym in symbols[:20]:
                    stocks.append({
                        "symbol": sym,
                        "name": sector_map.get(sym, {}).get("name", sym),
                        "change_pct": 0,
                    })

            stocks.sort(key=lambda x: -x["change_pct"])
            result = {"sector": decoded, "stocks": stocks, "count": len(stocks)}
            _sector_detail_cache[decoded] = {"data": result, "ts": now}
            return result

        except Exception as e:
            logger.error(f"[SECTOR_DETAIL] Error: {e}")
            return {"sector": decoded, "stocks": [], "error": str(e)}

    # ── API: Exchange Rate (USD/KRW) ────────────────────

    _fx_cache = {"data": None, "ts": 0}

    @app.get("/api/fx/usdkrw")
    async def fx_usdkrw():
        """Fetch USD/KRW exchange rate from Yahoo Finance."""
        import time as _time
        now = _time.time()

        if _fx_cache["data"] and now - _fx_cache["ts"] < 300:
            return _fx_cache["data"]

        result = {"rate": 0, "change_pct": 0, "available": False}
        try:
            import requests
            url = "https://query1.finance.yahoo.com/v8/finance/chart/USDKRW=X"
            resp = requests.get(url, params={"range": "2d", "interval": "1d"},
                               timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                data = resp.json()
                r = data.get("chart", {}).get("result", [{}])[0]
                closes = r.get("indicators", {}).get("quote", [{}])[0].get("close", [])
                if closes and closes[-1]:
                    result["rate"] = round(closes[-1], 2)
                    result["available"] = True
                    if len(closes) >= 2 and closes[-2]:
                        result["change_pct"] = round((closes[-1] / closes[-2] - 1) * 100, 2)
        except Exception as e:
            logger.warning(f"[FX] USDKRW error: {e}")

        if result["available"]:
            _fx_cache["data"] = result
            _fx_cache["ts"] = now
        return result

    # ── API: Tax Info ───────────────────────────────────

    @app.get("/api/tax/estimate")
    async def tax_estimate():
        """
        US 주식 양도소득세 추정.
        한국 거주자: 매매차익 250만원 공제 후 22% (양도세 20% + 지방세 2%).
        Alpaca에서 realized P&L 조회.
        """
        p = _get_provider()
        acct = p.query_account_summary()
        holdings = p.query_account_holdings()

        # Unrealized P&L (현재 보유)
        unrealized = sum(h.get("pnl", 0) for h in holdings)

        # Realized P&L은 Alpaca activities에서 가져올 수 있지만
        # 현재 단계에서는 간단히 unrealized 기준으로 추정
        fx = await fx_usdkrw()
        rate = fx.get("rate", 1400)

        unrealized_krw = unrealized * rate
        exemption_krw = 2_500_000  # 250만원 기본공제

        taxable_krw = max(0, unrealized_krw - exemption_krw)
        tax_krw = taxable_krw * 0.22  # 양도세 20% + 지방세 2%

        return {
            "unrealized_pnl_usd": round(unrealized, 2),
            "unrealized_pnl_krw": round(unrealized_krw, 0),
            "usdkrw_rate": rate,
            "exemption_krw": exemption_krw,
            "taxable_krw": round(taxable_krw, 0),
            "estimated_tax_krw": round(tax_krw, 0),
            "tax_rate": "22% (양도 20% + 지방 2%)",
            "note": "미실현 기준 추정, 실현 시 변동 가능",
        }

    # ── API: Lab ─────────────────────────────────────────

    @app.get("/api/lab/strategies")
    async def lab_strategies():
        from lab.lab_config import STRATEGY_CONFIGS, STRATEGY_GROUPS
        return {"strategies": STRATEGY_CONFIGS, "groups": STRATEGY_GROUPS}

    @app.post("/api/lab/run")
    async def lab_run(request: Request):
        body = await request.json()
        group = body.get("group", "rebal")
        start = body.get("start_date", "2024-01-01")
        end = body.get("end_date", "2026-04-11")
        force = body.get("force", False)

        # "all" → run each group sequentially, collect results
        if group == "all":
            from lab.runner import run_lab_job
            from lab.lab_config import STRATEGY_GROUPS
            jobs = []
            for g in STRATEGY_GROUPS:
                try:
                    job = run_lab_job(g, start, end, force=force)
                    jobs.append({"job_id": job.job_id, "group": g, "status": job.status})
                except ValueError as e:
                    jobs.append({"group": g, "error": str(e)})
            return {"jobs": jobs, "status": "ALL_STARTED"}

        try:
            from lab.runner import run_lab_job
            job = run_lab_job(group, start, end, force=force)
            return {"job_id": job.job_id, "status": job.status, "config_hash": job.config_hash}
        except ValueError as e:
            return {"error": str(e)}

    @app.get("/api/lab/jobs")
    async def lab_jobs():
        from lab.runner import get_store
        jobs = get_store().list_jobs()
        return {"jobs": [j.to_dict() for j in jobs[:20]]}

    @app.get("/api/lab/jobs/{job_id}")
    async def lab_job_detail(job_id: str):
        from lab.runner import get_store
        job = get_store().get_job(job_id)
        if not job:
            return {"error": "job not found"}
        return job.to_dict()

    @app.get("/lab", response_class=HTMLResponse)
    async def lab_page(request: Request):
        return templates.TemplateResponse(request, "lab.html")

    # ── API: Forward Trading ────────────────────────────

    @app.post("/api/lab/forward/start")
    async def forward_start():
        from lab.forward import ForwardTrader
        ft = ForwardTrader()
        return ft.initialize()

    @app.post("/api/lab/forward/eod")
    async def forward_eod(request: Request):
        body = await request.json() if request.headers.get("content-type") == "application/json" else {}
        force = body.get("force", False)
        eod_date = body.get("date", "")
        if not eod_date:
            from datetime import date
            eod_date = date.today().isoformat()
        from lab.forward import ForwardTrader
        ft = ForwardTrader()
        p = _get_provider()
        result = ft.run_eod(eod_date, provider=p, force=force)
        # EOD 완료/에러 알림
        try:
            from notify.telegram_bot import send
            if isinstance(result, dict):
                if result.get("error"):
                    send(
                        f"⚠️ <b>US Lab EOD Error</b>\n"
                        f"Date: {eod_date}\n"
                        f"{result['error']}",
                        severity="WARN",
                    )
                else:
                    n_strats = len(result.get("strategies_processed", []))
                    send(
                        f"✅ <b>US Lab EOD Complete</b>\n"
                        f"Date: {eod_date}\n"
                        f"Strategies: {n_strats}",
                        severity="INFO",
                    )
        except Exception:
            pass
        return result

    @app.get("/api/lab/forward/state")
    async def forward_state():
        from lab.forward import ForwardTrader
        return ForwardTrader().get_state()

    @app.get("/api/lab/forward/runs")
    async def forward_runs():
        from lab.forward import ForwardTrader
        return {"runs": ForwardTrader().get_runs()}

    @app.get("/api/lab/forward/meta")
    async def forward_meta():
        """Meta Layer summary for UI (observer-only). Drivers 포함."""
        try:
            from lab.meta_summary import build_daily_summary_us
            from lab.forward import _load_meta, _save_meta, RUNS_DIR, ForwardTrader
            meta = _load_meta()
            trade_date = meta.get("last_successful_eod_date", "")
            # Recovery: scan runs/ for latest DONE if meta lost the date
            if not trade_date and RUNS_DIR.exists():
                import json as _json
                for rf in sorted(RUNS_DIR.glob("*.json"), reverse=True):
                    try:
                        rd = _json.loads(rf.read_text())
                        if rd.get("status") == "DONE":
                            trade_date = rd["eod_date"]
                            meta["last_successful_eod_date"] = trade_date
                            meta["day_count"] = max(meta.get("day_count", 0), 1)
                            _save_meta(meta)
                            break
                    except Exception:
                        continue
            if not trade_date:
                return {"ok": False}
            summary = build_daily_summary_us(trade_date)
            if not summary:
                return {"ok": False}

            # ── Daily drivers 주입 (카드 확장 섹션용) ──
            try:
                from web.lab_live.daily_drivers import (
                    build_drivers_for_strategy, build_spy_series, load_sector_map_us,
                )
                from data.db_provider import get_db
                db = get_db()
                spy_series = build_spy_series(db, trade_date, window=30)
                sector_map = load_sector_map_us(db)
                # 포지션 상세 포함 state 조회
                state = ForwardTrader().get_state(include_positions=True)
                strategies = state.get("strategies", {}) or {}
                sfit = summary.get("strategy_fit", {}) or {}
                for sname, sentry in strategies.items():
                    drv = build_drivers_for_strategy(
                        strategy_entry=sentry,
                        sname=sname,
                        trade_date=trade_date,
                        sector_map=sector_map,
                        spy_series=spy_series,
                        db_provider=db,
                        initial_cash=100_000.0,
                        window=30,
                    )
                    if sname in sfit:
                        sfit[sname]["drivers"] = drv
                    else:
                        sfit[sname] = {"drivers": drv}
                summary["strategy_fit"] = sfit
            except Exception as _drv_err:
                summary["drivers_error"] = str(_drv_err)

            return {"ok": True, **summary}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @app.post("/api/lab/forward/reset")
    async def forward_reset():
        from lab.forward import ForwardTrader
        return ForwardTrader().reset()

    @app.post("/api/lab/forward/reset/{strategy}")
    async def forward_reset_strategy(strategy: str):
        from lab.forward import ForwardTrader
        return ForwardTrader().reset(strategy)

    # ── API: Telegram (Dashboard → Mobile) ──────────────

    @app.post("/api/notify/telegram")
    async def send_telegram_text(request: Request):
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

    @app.post("/api/notify/telegram/photo")
    async def send_telegram_photo(request: Request):
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

    # ── API: Rebalance Status / Mode / Execute / Phase ──

    def _get_state_mgr():
        from core.state_manager import StateManagerUS
        return StateManagerUS(_config.STATE_DIR, _config.TRADING_MODE)

    def _calc_d_day(last_rebal: str, rebal_days: int, provider=None):
        """Alpaca calendar 기반 D-day + next_rebalance_date 계산."""
        from core.state_manager import get_business_date_et
        today_bd = get_business_date_et(provider)
        bd_source = "provider" if provider else "fallback"

        if not last_rebal:
            return 0, today_bd, True, today_bd, bd_source

        try:
            p = _get_provider()
            from datetime import date as _date, timedelta as _td
            start = _date.fromisoformat(last_rebal)
            end = start + _td(days=rebal_days * 2 + 10)
            cal = p.get_calendar(
                start=start.isoformat(),
                end=end.isoformat(),
            )
            if cal:
                trading_days = [str(d) if not isinstance(d, str) else d for d in cal]
                # Find index of last_rebal or next after
                try:
                    idx = next(i for i, d in enumerate(trading_days) if d >= last_rebal)
                except StopIteration:
                    idx = 0
                next_idx = idx + rebal_days
                if next_idx < len(trading_days):
                    next_rd = trading_days[next_idx]
                else:
                    next_rd = trading_days[-1] if trading_days else today_bd

                # D-day: trading days remaining
                future = [d for d in trading_days if d > today_bd and d <= next_rd]
                d_day = len(future)
                rebal_due = today_bd >= next_rd
                return d_day, next_rd, rebal_due, today_bd, bd_source
        except Exception as e:
            logger.warning(f"[US_REBAL_STATUS] D-day calc error: {e}")

        # Fallback: simple calendar day estimation
        from datetime import date as _date
        try:
            last = _date.fromisoformat(last_rebal)
            today = _date.fromisoformat(today_bd)
            elapsed = (today - last).days
            d_day = max(0, rebal_days - int(elapsed * 5 / 7))
            next_rd = (last + _td(days=int(rebal_days * 7 / 5))).isoformat()
            return d_day, next_rd, d_day <= 0, today_bd, bd_source
        except Exception:
            return 0, today_bd, True, today_bd, bd_source

    @app.get("/api/summary")
    async def api_summary():
        """US account summary — Phase 4-A.3 (2026-04-25): unified shape
        per docs/ui_data_contract_20260424.md §2. Same fields as KR side.

        Returns:
            { equity, cash, buying_power, unrealized_pnl,
              realized_pnl_today, realized_pnl_total, total_pnl,
              fees_taxes_today, equity_prev, data_quality }

        Source:
        - equity / cash / buying_power: provider.query_account_summary
          (same source as /api/portfolio)
        - unrealized_pnl: sum of holdings.pnl
        - equity_prev: Alpaca account.last_equity (T-1 close)
        - realized_pnl_total: derived from (equity - 100000) base
          (US /api/portfolio convention — total return since inception)
        """
        try:
            now = _time_module.time()
            quality = "OK"
            # Reuse cache when fresh
            if (_portfolio_cache_us["data"]
                and now - _portfolio_cache_us["ts"] < PORTFOLIO_CACHE_TTL_US):
                pdata = _portfolio_cache_us["data"]
                holdings = pdata.get("holdings") or []
                age_sec = now - _portfolio_cache_us["ts"]
            else:
                p = _get_provider()
                acct = await asyncio.to_thread(p.query_account_summary)
                holdings = await asyncio.to_thread(p.query_account_holdings)
                pdata = {
                    "equity": acct.get("equity", 0),
                    "last_equity": acct.get("last_equity", 0),
                    "cash": acct.get("cash", 0),
                    "buying_power": acct.get("buying_power", 0),
                    "holdings": holdings,
                }
                age_sec = 0.0

            if age_sec > 300:
                quality = "STALE"

            equity = float(pdata.get("equity", 0) or 0)
            cash = float(pdata.get("cash", 0) or 0)
            buying_power = float(pdata.get("buying_power", 0) or 0)
            equity_prev = pdata.get("last_equity")
            equity_prev = float(equity_prev) if equity_prev else None
            unrealized_pnl = sum(
                float(h.get("pnl", 0) or 0) for h in holdings
            )
            # US convention: total return since $100k inception base.
            # Phase 4 follow-up: when realized vs unrealized split is
            # exposed by tracker, fill realized_pnl_total properly.
            realized_pnl_total = (equity - 100000.0) - unrealized_pnl

            return {
                "equity": equity,
                "cash": cash,
                "buying_power": buying_power,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl_today": None,  # TODO: tracker integration
                "realized_pnl_total": realized_pnl_total,
                "total_pnl": equity - 100000.0,
                "fees_taxes_today": None,  # TODO: order log aggregation
                "equity_prev": equity_prev,
                "data_quality": quality,
                "cache_age_sec": round(age_sec, 1),
            }
        except Exception as e:
            return {
                "equity": 0, "cash": 0, "buying_power": 0,
                "unrealized_pnl": 0, "realized_pnl_today": None,
                "realized_pnl_total": None, "total_pnl": 0,
                "fees_taxes_today": None, "equity_prev": None,
                "data_quality": "STALE", "error": str(e),
            }

    @app.get("/api/holdings")
    async def api_holdings():
        """US holdings — Phase 4-A.2 (2026-04-25): unified shape per
        docs/ui_data_contract_20260424.md §1. Same fields as KR side.

        Returns:
            { "positions": [
                { symbol, qty, avg_price, last_price, market_value,
                  unrealized_pnl, unrealized_pnl_pct, data_quality },
                ...
            ], "data_quality": "OK" | "DEGRADED" | "STALE" }

        Source: provider.query_account_holdings (same source as
        /api/portfolio). /api/portfolio remains for richer-payload
        consumers; this endpoint is the unified contract target.
        """
        try:
            now = _time_module.time()
            quality = "OK"
            holdings = None
            # Reuse portfolio cache when fresh (same TTL semantics).
            if (_portfolio_cache_us["data"]
                and now - _portfolio_cache_us["ts"] < PORTFOLIO_CACHE_TTL_US):
                holdings = _portfolio_cache_us["data"].get("holdings") or []
                age_sec = now - _portfolio_cache_us["ts"]
            else:
                p = _get_provider()
                holdings = await asyncio.to_thread(p.query_account_holdings)
                age_sec = 0.0

            if age_sec > 300:
                quality = "STALE"

            positions = []
            for h in (holdings or []):
                qty = int(h.get("qty", 0) or 0)
                avg_price = float(h.get("avg_price", 0) or 0)
                last_price = float(h.get("cur_price", 0) or 0)
                market_value = float(h.get("market_value", 0) or (last_price * qty))
                unrealized_pnl = float(h.get("pnl", 0) or 0)
                if avg_price > 0:
                    unrealized_pnl_pct = (last_price / avg_price - 1.0) * 100.0
                else:
                    unrealized_pnl_pct = float(h.get("pnl_pct", 0) or 0)
                positions.append({
                    "symbol": h.get("code") or h.get("symbol", ""),
                    "qty": qty,
                    "avg_price": avg_price,
                    "last_price": last_price,
                    "market_value": market_value,
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_pnl_pct": round(unrealized_pnl_pct, 4),
                    "data_quality": quality,
                })
            return {
                "positions": positions,
                "data_quality": quality,
                "cache_age_sec": round(age_sec, 1),
            }
        except Exception as e:
            return {"positions": [], "data_quality": "STALE", "error": str(e)}

    @app.get("/api/batch/status")
    async def batch_status():
        """US batch status. Phase 4-A.1 (2026-04-25): unified shape per
        docs/ui_data_contract_20260424.md §5 — matches KR contract:
            batch_done, business_date, snapshot_created_at, snapshot_version

        Sourced from the same rebalance state as /api/rebalance/status
        which remains unchanged for richer-payload consumers.
        """
        try:
            from core.state_manager import get_business_date_et
            sm = _get_state_mgr()
            rs = sm.get_rebal_state()
            p = _get_provider()
            today_bd = get_business_date_et(p)
            last_bd = rs.get("last_batch_business_date", "")
            done = bool(last_bd and last_bd == today_bd)
            return {
                "batch_done": done,
                "business_date": today_bd,
                "snapshot_created_at": rs.get("snapshot_created_at", ""),
                "snapshot_version": rs.get("snapshot_version", ""),
                # Legacy alias for tools that grep this name
                "last_batch_business_date": last_bd,
            }
        except Exception as e:
            return {
                "batch_done": False,
                "business_date": "",
                "snapshot_created_at": "",
                "snapshot_version": "",
                "error": str(e),
            }

    @app.get("/api/rebalance/status")
    async def rebalance_status():
        """Consolidated rebal status for dashboard + tray_server."""
        try:
            from core.state_manager import get_business_date_et, compute_batch_fresh
            sm = _get_state_mgr()
            rs = sm.get_rebal_state()
            p = _get_provider()

            today_bd = get_business_date_et(p)
            bd_source = "provider"

            d_day, next_rd, rebal_due, _, _ = _calc_d_day(
                rs.get("last_rebalance_date", ""), _config.REBAL_DAYS, p
            )

            batch_fresh = compute_batch_fresh(rs, today_bd)
            allowed, blocks = sm.compute_execute_allowed(p, _config)

            return {
                "mode": rs.get("rebal_mode", "manual"),
                "phase": rs.get("rebal_phase", "IDLE"),
                "d_day": d_day,
                "rebal_due": rebal_due,
                "batch_fresh": batch_fresh,
                "snapshot_version": rs.get("snapshot_version", ""),
                "snapshot_created_at": rs.get("snapshot_created_at", ""),
                "execute_allowed": allowed,
                "block_reasons": blocks,
                "next_rebalance_date": next_rd,
                "last_rebalance_date": rs.get("last_rebalance_date", ""),
                "last_batch_business_date": rs.get("last_batch_business_date", ""),
                "last_execute_business_date": rs.get("last_execute_business_date", ""),
                "last_execute_result": rs.get("last_execute_result", ""),
                "last_rebal_attempt_snapshot": rs.get("last_rebal_attempt_snapshot", ""),
                "last_rebal_attempt_at": rs.get("last_rebal_attempt_at", ""),
                "last_rebal_attempt_result": rs.get("last_rebal_attempt_result", ""),
                "last_rebal_attempt_count": rs.get("last_rebal_attempt_count", 0),
                "last_rebal_attempt_reason": rs.get("last_rebal_attempt_reason", ""),
                "business_date": today_bd,
                "business_date_source": bd_source,
                "rebal_days": _config.REBAL_DAYS,
            }
        except Exception as e:
            logger.error(f"[US_REBAL_STATUS] {e}")
            return {"error": str(e)}

    @app.post("/api/rebalance/mode")
    async def rebalance_mode(request: Request):
        """Toggle manual/auto."""
        try:
            body = await request.json()
            mode = body.get("mode", "manual")
            if mode not in ("manual", "auto"):
                return {"ok": False, "error": f"Invalid mode: {mode}"}
            sm = _get_state_mgr()
            sm.update_rebal_state({"rebal_mode": mode})
            logger.info(f"[US_REBAL_MODE_SET] mode={mode}")
            return {"ok": True, "mode": mode}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @app.post("/api/rebalance/phase")
    async def rebalance_phase(request: Request):
        """tray_server → batch phase update. Only BATCH_RUNNING, FAILED allowed."""
        try:
            body = await request.json()
            phase = body.get("phase", "")
            if phase not in ("BATCH_RUNNING", "FAILED"):
                return {"ok": False, "error": f"Phase not allowed via API: {phase}"}

            sm = _get_state_mgr()
            ok, reason = sm.transition_phase(phase)
            if not ok:
                return {"ok": False, "error": reason}

            # Extra fields for FAILED
            if phase == "FAILED":
                sm.update_rebal_state({"batch_fresh": False})

            logger.info(f"[US_REBAL_PHASE] → {phase}")
            return {"ok": True, "phase": phase}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    _execute_lock = threading.Lock()  # process-level lock

    @app.post("/api/rebalance/execute")
    async def rebalance_execute_v2(request: Request):
        """
        Execute rebalance with full TOCTOU + idempotency + partial protection.
        """
        import uuid as _uuid
        from core.state_manager import (
            get_business_date_et, compute_batch_fresh, US_ET,
        )
        from datetime import datetime as _dt

        body = await request.json()
        mode = body.get("mode", "sell_and_buy")
        request_id = body.get("request_id", "")
        if not request_id:
            return {"ok": False, "error": "request_id required"}

        sm = _get_state_mgr()
        p = _get_provider()
        today_bd = get_business_date_et(p)

        log_ctx = f"req={request_id[:8]} bd={today_bd}"
        sv = ""  # snapshot_version — set after validation

        def _record_reject(reason_code: str):
            """Validation reject: attempt 기록 (count 미증가)."""
            _sv = sm.get_rebal_state().get("snapshot_version", "")
            sm.update_rebal_state({
                "last_rebal_attempt_snapshot": _sv,
                "last_rebal_attempt_at": _dt.now(US_ET).isoformat(),
                "last_rebal_attempt_result": "REJECTED",
                "last_rebal_attempt_reason": reason_code,
            })

        # ── 검증 8단계 ──
        sm.clear_stale_execute_lock()
        rs = sm.get_rebal_state()

        # 0. Market closed — day-type orders submitted outside regular hours
        # are canceled by Alpaca almost immediately (no fills). This produced
        # the false-positive SUCCESS notification on 2026-04-21 pre-market.
        # Fail fast with a clear reason instead of submitting doomed orders.
        try:
            _clock = p.get_clock() or {}
        except Exception:
            _clock = {}
        if not _clock.get("is_open", False):
            logger.warning(f"[US_REBAL_EXEC_REJECT] market_closed {log_ctx}")
            _record_reject("MARKET_CLOSED")
            return {"ok": False, "error": "Market is closed — day orders would be canceled. Try again during regular hours."}

        # 1. Idempotency
        if request_id == rs.get("last_execute_request_id", ""):
            logger.warning(f"[US_REBAL_DUP_BLOCK] {log_ctx}")
            _record_reject("DUPLICATE_REQUEST")
            return {"ok": False, "error": "Duplicate request_id"}

        # 2. Already executing
        if rs.get("rebal_phase") == "EXECUTING":
            logger.warning(f"[US_REBAL_EXEC_REJECT] phase=EXECUTING {log_ctx}")
            _record_reject("ALREADY_EXECUTING")
            return {"ok": False, "error": "Already executing"}

        # 3. Same business date
        if rs.get("last_execute_business_date", "") == today_bd:
            logger.warning(f"[US_REBAL_EXEC_REJECT] same_date {log_ctx}")
            _record_reject("ALREADY_EXECUTED_TODAY")
            return {"ok": False, "error": "Already executed today"}

        # 4. Batch fresh
        if not compute_batch_fresh(rs, today_bd):
            logger.warning(f"[US_REBAL_EXEC_REJECT] batch_not_fresh {log_ctx}")
            _record_reject("BATCH_NOT_FRESH")
            return {"ok": False, "error": "Batch not fresh"}

        # 5. Same snapshot
        sv = rs.get("snapshot_version", "")
        if sv and sv == rs.get("last_execute_snapshot_version", ""):
            logger.warning(f"[US_REBAL_EXEC_REJECT] same_snapshot {log_ctx}")
            _record_reject("SAME_SNAPSHOT")
            return {"ok": False, "error": "Same snapshot already executed"}

        # 6. Full allowed check
        allowed, blocks = sm.compute_execute_allowed(p, _config)
        if not allowed:
            logger.warning(f"[US_REBAL_EXEC_REJECT] blocks={blocks} {log_ctx}")
            _record_reject(blocks[0] if blocks else "UNKNOWN")
            return {"ok": False, "error": f"Blocked: {', '.join(blocks)}"}

        # ── LOCK 획득 ──
        if not _execute_lock.acquire(timeout=5):
            _record_reject("LOCK_TIMEOUT")
            return {"ok": False, "error": "Lock acquisition timeout"}

        try:
            et_now = _dt.now(US_ET)

            # Phase discipline: BATCH_DONE → DUE → EXECUTING
            cur_phase = sm.get_rebal_state().get("rebal_phase", "IDLE")
            if cur_phase == "BATCH_DONE":
                ok_due, reason_due = sm.transition_phase("DUE")
                if not ok_due:
                    _execute_lock.release()
                    _record_reject(f"DUE_TRANSITION_FAIL:{reason_due}")
                    return {"ok": False, "error": f"Phase transition to DUE failed: {reason_due}"}

            ok_exec, reason_exec = sm.transition_phase_with_updates("EXECUTING", {
                "execute_lock": True,
                "execute_lock_owner": request_id,
                "execute_lock_acquired_at": et_now.isoformat(),
            })
            if not ok_exec:
                _execute_lock.release()
                _record_reject(f"EXEC_TRANSITION_FAIL:{reason_exec}")
                return {"ok": False, "error": f"Phase transition to EXECUTING failed: {reason_exec}"}

            logger.info(f"[US_REBAL_EXEC_START] {log_ctx} mode={mode}")

            # ── DOUBLE-CHECK (TOCTOU) ──
            rs2 = sm.get_rebal_state()
            open_orders = p.query_open_orders() or []
            if open_orders:
                raise ValueError(f"Open orders detected: {len(open_orders)}")

            from strategy.execution_gate import check_buy_permission
            rt_full = sm.load_runtime() or {}
            buy_ok, buy_reason, buy_scale = check_buy_permission(_config, rt_full, p)

            # ── 주문 계산 ──
            db = _get_db()
            target = db.get_target_portfolio()
            if not target:
                raise ValueError("No target portfolio")

            acct = p.query_account_summary()
            holdings = p.query_account_holdings()
            current = {h["code"]: h for h in holdings}
            equity = acct.get("equity", 0)
            cash = acct.get("cash", 0)

            prices = {}
            all_syms = set(current.keys()) | set(target.get("target_tickers", []))
            for sym in all_syms:
                px = p.get_current_price(sym)
                if px and px > 0:
                    prices[sym] = px

            from strategy.rebalancer import compute_orders
            sells, buys = compute_orders(
                current, target["target_tickers"],
                equity, cash,
                _config.BUY_COST, _config.SELL_COST,
                prices, _config.CASH_BUFFER_RATIO,
            )

            # ── SELL ──
            sell_results = []
            sell_submit_ok = 0
            if mode in ("sell_only", "sell_and_buy"):
                for s in sells:
                    try:
                        r = p.send_order(s.ticker, "SELL", s.quantity)
                        ok = is_order_success(r)
                        entry = {"symbol": s.ticker, "qty": s.quantity, "ok": ok}
                        if ok:
                            entry["order_no"] = r.get("order_no", "")
                            sell_submit_ok += 1
                        else:
                            logger.warning(f"[ORDER_FAIL] SELL {s.ticker} x{s.quantity} result={r}")
                        sell_results.append(entry)
                    except Exception as e:
                        sell_results.append({"symbol": s.ticker, "qty": s.quantity, "ok": False, "error": str(e)})

            # ── BUY ──
            buy_results = []
            buy_submit_ok = 0
            buy_blocked = ""
            if mode in ("buy_only", "sell_and_buy"):
                if not buy_ok:
                    buy_blocked = buy_reason
                else:
                    for b in buys:
                        scaled_qty = int(b.quantity * buy_scale) if buy_scale < 1.0 else b.quantity
                        if scaled_qty <= 0:
                            continue
                        try:
                            r = p.send_order(b.ticker, "BUY", scaled_qty)
                            ok = is_order_success(r)
                            entry = {"symbol": b.ticker, "qty": scaled_qty, "ok": ok}
                            if ok:
                                entry["order_no"] = r.get("order_no", "")
                                buy_submit_ok += 1
                            else:
                                logger.warning(f"[ORDER_FAIL] BUY {b.ticker} x{scaled_qty} result={r}")
                            buy_results.append(entry)
                        except Exception as e:
                            buy_results.append({"symbol": b.ticker, "qty": scaled_qty, "ok": False, "error": str(e)})

            # ── Post-submission status verification ──
            # is_order_success() only confirms Alpaca accepted the POST; it does
            # NOT confirm the broker didn't immediately cancel (e.g., pre-market
            # day orders, buying-power rejection, bad symbol). Poll each order
            # ~3s after submission and drop the ones Alpaca killed with 0 fills.
            # Without this, 6 immediately-canceled orders registered as SUCCESS
            # on 2026-04-21 21:15 KST and advanced last_rebalance_date falsely.
            _DEAD = {"canceled", "cancelled", "expired", "rejected", "suspended"}

            def _order_final(order_no: str) -> tuple[bool, int]:
                """Return (alive, filled_qty) for a submitted order.

                alive=False → broker terminated with zero fills (treat as failed).
                On query error we default to alive=True (conservative — let the
                async fill monitor resolve later).
                """
                if not order_no:
                    return (True, 0)
                try:
                    o = p._get(f"/v2/orders/{order_no}")
                    if not o:
                        return (True, 0)
                    status = (o.get("status") or "").lower()
                    filled = int(float(o.get("filled_qty", 0) or 0))
                    if status in _DEAD and filled == 0:
                        return (False, 0)
                    return (True, filled)
                except Exception:
                    return (True, 0)

            if sell_submit_ok or buy_submit_ok:
                await asyncio.sleep(3.0)

            sell_ok_count = 0
            sell_filled = 0
            for r in sell_results:
                if not r.get("ok"):
                    continue
                alive, filled = _order_final(r.get("order_no", ""))
                r["alive"] = alive
                r["filled_qty"] = filled
                if not alive:
                    r["ok"] = False
                    logger.warning(
                        f"[ORDER_KILLED] SELL {r['symbol']} x{r['qty']} "
                        f"order_no={r.get('order_no','')[:8]} canceled/rejected by broker"
                    )
                else:
                    sell_ok_count += 1
                    sell_filled += filled

            buy_ok_count = 0
            buy_filled = 0
            for r in buy_results:
                if not r.get("ok"):
                    continue
                alive, filled = _order_final(r.get("order_no", ""))
                r["alive"] = alive
                r["filled_qty"] = filled
                if not alive:
                    r["ok"] = False
                    logger.warning(
                        f"[ORDER_KILLED] BUY {r['symbol']} x{r['qty']} "
                        f"order_no={r.get('order_no','')[:8]} canceled/rejected by broker"
                    )
                else:
                    buy_ok_count += 1
                    buy_filled += filled

            # ── 결과 판정 ──
            total_sells = len(sells) if mode in ("sell_only", "sell_and_buy") else 0
            total_buys = len(buys) if mode in ("buy_only", "sell_and_buy") else 0
            total_submit = sell_submit_ok + buy_submit_ok  # orders Alpaca accepted
            total_alive = sell_ok_count + buy_ok_count     # orders still alive post-verify
            sell_all_ok = sell_ok_count == total_sells or total_sells == 0
            buy_all_ok = buy_ok_count == total_buys or total_buys == 0 or buy_blocked

            if total_submit > 0 and total_alive == 0:
                # Every submitted order was killed by broker (e.g. pre-market).
                # NOT a success — do not advance last_rebalance_date.
                exec_result = "NO_FILL"
                exec_phase = "FAILED"
            elif sell_all_ok and buy_all_ok and not buy_blocked:
                exec_result = "SUCCESS"
                exec_phase = "EXECUTED"
            elif total_alive > 0:
                exec_result = "PARTIAL"
                exec_phase = "PARTIAL_EXECUTED"
            else:
                exec_result = "FAILED"
                exec_phase = "FAILED"

            # ── 최종 상태 저장 (transition_phase_with_updates) ──
            prev_count = sm.get_rebal_state().get("last_rebal_attempt_count", 0)
            final_updates = {
                "execute_lock": False,
                "execute_lock_owner": "",
                "execute_lock_acquired_at": "",
                "batch_fresh": False,
                "last_execute_business_date": today_bd,
                "last_execute_request_id": request_id,
                "last_execute_snapshot_version": sv,
                "last_execute_result": exec_result,
                # attempt tracking (실행 완료 → count 증가)
                "last_rebal_attempt_snapshot": sv,
                "last_rebal_attempt_at": _dt.now(US_ET).isoformat(),
                "last_rebal_attempt_result": exec_result,
                "last_rebal_attempt_count": prev_count + 1,
                "last_rebal_attempt_reason": "",
            }
            if exec_result in ("SUCCESS", "PARTIAL"):
                final_updates["last_rebalance_date"] = today_bd

            sm.transition_phase_with_updates(exec_phase, final_updates)

            logger.info(
                f"[US_REBAL_EXEC_DONE] {log_ctx} result={exec_result} "
                f"sells={sell_ok_count}/{total_sells}(filled={sell_filled}) "
                f"buys={buy_ok_count}/{total_buys}(filled={buy_filled})"
            )

            # Telegram
            try:
                from notify.telegram_bot import send
                _severity = {
                    "SUCCESS": "INFO",
                    "PARTIAL": "WARN",
                    "NO_FILL": "CRITICAL",
                    "FAILED": "CRITICAL",
                }.get(exec_result, "WARN")
                send(
                    f"<b>Rebalance {exec_result}</b>\n"
                    f"Mode: {mode} | Scale: {buy_scale:.0%}\n"
                    f"Sells: {sell_ok_count}/{total_sells} (filled: {sell_filled})\n"
                    f"Buys: {buy_ok_count}/{total_buys} (filled: {buy_filled})"
                    + (f"\nBUY blocked: {buy_blocked}" if buy_blocked else "")
                    + ("\n⚠ 모든 주문이 취소/거절됨 — last_rebalance_date 유지"
                       if exec_result == "NO_FILL" else ""),
                    _severity,
                )
            except Exception:
                pass

            return {
                "ok": exec_result not in ("FAILED", "NO_FILL"),
                "result": exec_result,
                "sells": sell_results,
                "buys": buy_results,
                "sell_count": sell_ok_count,
                "buy_count": buy_ok_count,
                "sell_filled": sell_filled,
                "buy_filled": buy_filled,
                "buy_blocked": buy_blocked,
                "buy_scale": buy_scale,
            }

        except Exception as e:
            # ── FINALLY: unlock on any failure ──
            logger.error(f"[US_REBAL_EXEC_FAIL] {log_ctx} {e}")
            prev_count = sm.get_rebal_state().get("last_rebal_attempt_count", 0)
            sm.transition_phase_with_updates("FAILED", {
                "execute_lock": False,
                "execute_lock_owner": "",
                "execute_lock_acquired_at": "",
                "last_rebal_attempt_snapshot": sv,
                "last_rebal_attempt_at": _dt.now(US_ET).isoformat(),
                "last_rebal_attempt_result": "FAILED",
                "last_rebal_attempt_count": prev_count + 1,
                "last_rebal_attempt_reason": str(e)[:200],
            })
            return {"ok": False, "error": str(e)}
        finally:
            try:
                _execute_lock.release()
            except RuntimeError:
                pass

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
