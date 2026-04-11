# -*- coding: utf-8 -*-
"""
app.py -- FastAPI Dashboard for Q-TRON US
==========================================
US market dashboard with real-time Alpaca data.
Supports KR/US market toggle for cross-market overview.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # ── Pages ────────────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse(request, "index.html")

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

    # ── API: Account / Portfolio ─────────────────────────

    @app.get("/api/account")
    async def account():
        p = _get_provider()
        return p.query_account_summary()

    @app.get("/api/portfolio")
    async def portfolio():
        p = _get_provider()
        acct = p.query_account_summary()
        holdings = p.query_account_holdings()
        return {
            "market": "US",
            "equity": acct.get("equity", 0),
            "cash": acct.get("cash", 0),
            "buying_power": acct.get("buying_power", 0),
            "portfolio_value": acct.get("portfolio_value", 0),
            "n_holdings": len(holdings),
            "holdings": holdings,
        }

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

    # ── API: DB Health ───────────────────────────────────

    @app.get("/api/db/health")
    async def db_health():
        db = _get_db()
        return db.health_check()

    # ── API: KR Market (Cross-market overlay) ────────────

    @app.get("/api/kr/portfolio")
    async def kr_portfolio():
        """Proxy to Gen04-REST dashboard for KR data."""
        try:
            import requests
            resp = requests.get("http://localhost:8080/api/portfolio", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                data["market"] = "KR"
                return data
        except Exception:
            pass
        return {"market": "KR", "error": "Gen04-REST not available"}

    # ── API: Regime ─────────────────────────────────────

    _regime_cache = {"data": None, "ts": 0}

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
            prediction = predict_regime(market_data)

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

            result = {
                "today": today,
                "prediction": prediction,
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
        return ft.run_eod(eod_date, provider=p, force=force)

    @app.get("/api/lab/forward/state")
    async def forward_state():
        from lab.forward import ForwardTrader
        return ForwardTrader().get_state()

    @app.get("/api/lab/forward/runs")
    async def forward_runs():
        from lab.forward import ForwardTrader
        return {"runs": ForwardTrader().get_runs()}

    @app.post("/api/lab/forward/reset")
    async def forward_reset():
        from lab.forward import ForwardTrader
        return ForwardTrader().reset()

    @app.post("/api/lab/forward/reset/{strategy}")
    async def forward_reset_strategy(strategy: str):
        from lab.forward import ForwardTrader
        return ForwardTrader().reset(strategy)

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
