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
import threading
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


def _get_runtime_data() -> dict:
    """Load latest runtime state for BUY gate evaluation."""
    try:
        from core.state_manager import StateManagerUS
        sm = StateManagerUS(_config.STATE_DIR, _config.TRADING_MODE)
        return sm.load_runtime() or {}
    except Exception:
        return {}


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

    # ── API: Status Summary (KR-style status bar) ─────

    _status_cache = {"data": None, "ts": 0}

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
        }

        _status_cache["data"] = result
        _status_cache["ts"] = now
        return result

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

            return {
                "ok": True,
                "target_date": target.get("date", ""),
                "target_count": len(target.get("target_tickers", [])),
                "sells": [{"symbol": s.ticker, "qty": s.quantity, "reason": s.reason} for s in sells],
                "buys": [{"symbol": b.ticker, "qty": b.quantity, "amount": round(b.target_amount, 2), "reason": b.reason} for b in buys],
                "keep": len(set(current.keys()) & set(target.get("target_tickers", []))),
                "equity": round(equity, 2),
                "cash": round(cash, 2),
                "buy_allowed": buy_allowed,
                "buy_reason": buy_reason,
                "buy_scale": buy_scale,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @app.post("/api/rebalance/execute")
    async def rebalance_execute(request: Request):
        """Execute rebalance: SELL first, then BUY. Manual trigger only."""
        try:
            body = await request.json()
            mode = body.get("mode", "sell_and_buy")  # "sell_only" | "buy_only" | "sell_and_buy"

            p = _get_provider()
            db = _get_db()

            # BUY gate
            from strategy.execution_gate import check_buy_permission
            _rt = _get_runtime_data()
            buy_allowed, buy_reason, buy_scale = check_buy_permission(_config, _rt, p)

            # Open orders check
            open_orders = p.query_open_orders() or []
            if open_orders:
                return {"ok": False, "error": f"Open orders exist ({len(open_orders)}). Cancel first."}

            target = db.get_target_portfolio()
            if not target:
                return {"ok": False, "error": "No target portfolio"}

            acct = p.query_account_summary()
            holdings = p.query_account_holdings()
            current = {h["code"]: h for h in holdings}
            equity = acct.get("equity", 0)
            cash = acct.get("cash", 0)

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

            results = {"sells": [], "buys": [], "sell_count": 0, "buy_count": 0}

            # SELL phase
            if mode in ("sell_only", "sell_and_buy"):
                for s in sells:
                    r = p.send_order(s.ticker, "SELL", s.quantity)
                    ok = bool(r.get("order_no"))
                    results["sells"].append({"symbol": s.ticker, "qty": s.quantity, "ok": ok})
                    if ok:
                        results["sell_count"] += 1

            # BUY phase
            if mode in ("buy_only", "sell_and_buy"):
                if not buy_allowed:
                    results["buy_blocked"] = buy_reason
                else:
                    for b in buys:
                        scaled_qty = int(b.quantity * buy_scale)
                        if scaled_qty <= 0:
                            results["buys"].append({"symbol": b.ticker, "qty": 0, "ok": False, "reason": "scaled_to_zero"})
                            continue
                        r = p.send_order(b.ticker, "BUY", scaled_qty)
                        ok = bool(r.get("order_no"))
                        results["buys"].append({"symbol": b.ticker, "qty": scaled_qty, "ok": ok})
                        if ok:
                            results["buy_count"] += 1

            # Notify
            from notify.telegram_bot import send
            send(f"<b>Rebalance Executed</b>\n"
                 f"Mode: {mode}\n"
                 f"Sells: {results['sell_count']}/{len(sells)}\n"
                 f"Buys: {results['buy_count']}/{len(buys)}\n"
                 f"Scale: {buy_scale:.0%}", "INFO")

            return {"ok": True, **results}

        except Exception as e:
            return {"ok": False, "error": str(e)}

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

        # ── 검증 8단계 ──
        sm.clear_stale_execute_lock()
        rs = sm.get_rebal_state()

        # 1. Idempotency
        if request_id == rs.get("last_execute_request_id", ""):
            logger.warning(f"[US_REBAL_DUP_BLOCK] {log_ctx}")
            return {"ok": False, "error": "Duplicate request_id"}

        # 2. Already executing
        if rs.get("rebal_phase") == "EXECUTING":
            logger.warning(f"[US_REBAL_EXEC_REJECT] phase=EXECUTING {log_ctx}")
            return {"ok": False, "error": "Already executing"}

        # 3. Same business date
        if rs.get("last_execute_business_date", "") == today_bd:
            logger.warning(f"[US_REBAL_EXEC_REJECT] same_date {log_ctx}")
            return {"ok": False, "error": "Already executed today"}

        # 4. Batch fresh
        if not compute_batch_fresh(rs, today_bd):
            logger.warning(f"[US_REBAL_EXEC_REJECT] batch_not_fresh {log_ctx}")
            return {"ok": False, "error": "Batch not fresh"}

        # 5. Same snapshot
        sv = rs.get("snapshot_version", "")
        if sv and sv == rs.get("last_execute_snapshot_version", ""):
            logger.warning(f"[US_REBAL_EXEC_REJECT] same_snapshot {log_ctx}")
            return {"ok": False, "error": "Same snapshot already executed"}

        # 6. Full allowed check
        allowed, blocks = sm.compute_execute_allowed(p, _config)
        if not allowed:
            logger.warning(f"[US_REBAL_EXEC_REJECT] blocks={blocks} {log_ctx}")
            return {"ok": False, "error": f"Blocked: {', '.join(blocks)}"}

        # ── LOCK 획득 ──
        if not _execute_lock.acquire(timeout=5):
            return {"ok": False, "error": "Lock acquisition timeout"}

        try:
            # Save EXECUTING state
            et_now = _dt.now(US_ET)
            sm.update_rebal_state({
                "execute_lock": True,
                "execute_lock_owner": request_id,
                "execute_lock_acquired_at": et_now.isoformat(),
                "rebal_phase": "EXECUTING",
            })
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
            sell_ok_count = 0
            if mode in ("sell_only", "sell_and_buy"):
                for s in sells:
                    try:
                        r = p.send_order(s.ticker, "SELL", s.quantity)
                        ok = ("order_no" in r and "error" not in r) if isinstance(r, dict) else False
                        sell_results.append({"symbol": s.ticker, "qty": s.quantity, "ok": ok})
                        if ok:
                            sell_ok_count += 1
                    except Exception as e:
                        sell_results.append({"symbol": s.ticker, "qty": s.quantity, "ok": False, "error": str(e)})

            # ── BUY ──
            buy_results = []
            buy_ok_count = 0
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
                            ok = ("order_no" in r and "error" not in r) if isinstance(r, dict) else False
                            buy_results.append({"symbol": b.ticker, "qty": scaled_qty, "ok": ok})
                            if ok:
                                buy_ok_count += 1
                        except Exception as e:
                            buy_results.append({"symbol": b.ticker, "qty": scaled_qty, "ok": False, "error": str(e)})

            # ── 결과 판정 ──
            total_sells = len(sells) if mode in ("sell_only", "sell_and_buy") else 0
            total_buys = len(buys) if mode in ("buy_only", "sell_and_buy") else 0
            sell_all_ok = sell_ok_count == total_sells or total_sells == 0
            buy_all_ok = buy_ok_count == total_buys or total_buys == 0 or buy_blocked

            if sell_all_ok and buy_all_ok and not buy_blocked:
                exec_result = "SUCCESS"
                exec_phase = "EXECUTED"
            elif sell_ok_count > 0 or buy_ok_count > 0:
                exec_result = "PARTIAL"
                exec_phase = "PARTIAL_EXECUTED"
            else:
                exec_result = "FAILED"
                exec_phase = "FAILED"

            # ── 최종 상태 저장 ──
            final_updates = {
                "execute_lock": False,
                "execute_lock_owner": "",
                "execute_lock_acquired_at": "",
                "batch_fresh": False,
                "last_execute_business_date": today_bd,
                "last_execute_request_id": request_id,
                "last_execute_snapshot_version": sv,
                "last_execute_result": exec_result,
                "rebal_phase": exec_phase,
            }
            if exec_result in ("SUCCESS", "PARTIAL"):
                final_updates["last_rebalance_date"] = today_bd

            sm.update_rebal_state(final_updates)

            logger.info(
                f"[US_REBAL_EXEC_DONE] {log_ctx} result={exec_result} "
                f"sells={sell_ok_count}/{total_sells} buys={buy_ok_count}/{total_buys}"
            )

            # Telegram
            try:
                from notify.telegram_bot import send
                send(
                    f"<b>Rebalance {exec_result}</b>\n"
                    f"Mode: {mode} | Scale: {buy_scale:.0%}\n"
                    f"Sells: {sell_ok_count}/{total_sells} | Buys: {buy_ok_count}/{total_buys}"
                    + (f"\nBUY blocked: {buy_blocked}" if buy_blocked else ""),
                    "INFO" if exec_result == "SUCCESS" else "WARN",
                )
            except Exception:
                pass

            return {
                "ok": exec_result != "FAILED",
                "result": exec_result,
                "sells": sell_results,
                "buys": buy_results,
                "sell_count": sell_ok_count,
                "buy_count": buy_ok_count,
                "buy_blocked": buy_blocked,
                "buy_scale": buy_scale,
            }

        except Exception as e:
            # ── FINALLY: unlock on any failure ──
            logger.error(f"[US_REBAL_EXEC_FAIL] {log_ctx} {e}")
            sm.update_rebal_state({
                "execute_lock": False,
                "execute_lock_owner": "",
                "execute_lock_acquired_at": "",
                "rebal_phase": "FAILED",
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
