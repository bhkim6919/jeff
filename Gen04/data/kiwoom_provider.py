# -*- coding: utf-8 -*-
"""
Gen4KiwoomProvider
==================
Kiwoom OpenAPI+ broker interface for Gen4.

Trimmed from Gen3 KiwoomProvider — removed:
  - DataProvider ABC / OHLCV TR queries (opt10081, opt20006)
  - Real-time data (SetRealReg / OnReceiveRealData / TickAnalyzer)
  - Reconcile engine (recon_stale tracking, reconcile() method)
  - Stock list cache, investor trend, avg daily volume

Retained verbatim:
  - _call() central wrapper, _decode_kiwoom_str()
  - send_order() + _on_chejan_data() + _process_chejan_fill()
  - query_account_holdings/summary/sellable_qty (opw00018)
  - Ghost order tracking
"""

import time
import traceback
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer


# -- Constants ----------------------------------------------------------------

TR_DELAY            = 0.5    # TR request min interval (sec)
TR_TIMEOUT_SEC      = 20     # Single TR response timeout (sec)
TR_MAX_RETRY        = 3      # Max retry count
TR_MAX_CONSECUTIVE  = 5      # Consecutive timeout threshold

SCREEN_MAP = {
    "opw00018": "9003",   # Account holdings
    "opt20006": "9004",   # Index daily candles (KOSPI/KOSDAQ)
    "opt10075": "9005",   # Open orders (unfilled)
    "opt20005": "9006",   # Index minute candles
}

ORDER_TIMEOUT_SEC = 30


# -- Logger -------------------------------------------------------------------

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

TR_ERROR_LOG = LOG_DIR / f"tr_error_{datetime.today().strftime('%Y%m%d')}.log"

logger = logging.getLogger("Gen4KiwoomProvider")


# -- Custom Exceptions --------------------------------------------------------

class TrTimeoutError(Exception):
    """TR request exceeded max retries without response."""
    pass


class KiwoomApiError(Exception):
    """dynamicCall failure."""
    pass


class ProviderDeadError(Exception):
    """COM object released — provider unusable. No retry."""
    pass


class RateLimitError(Exception):
    """Kiwoom API rate limit exceeded. Retry after delay."""
    pass


# -- Main Class ---------------------------------------------------------------

class Gen4KiwoomProvider:
    """
    Kiwoom OpenAPI+ broker interface for Gen4.

    Usage:
        from api.kiwoom_api_wrapper import create_loggedin_kiwoom
        kiwoom = create_loggedin_kiwoom()
        provider = Gen4KiwoomProvider(kiwoom)
    """

    def __init__(self, kiwoom: QAxWidget, sector_map_path: Optional[str] = None):
        """
        kiwoom: api/kiwoom_api_wrapper.create_loggedin_kiwoom() return value
        sector_map_path: sector_map.json path (None = auto-detect)
        """
        self._k = kiwoom

        # -- COM object lifetime (v7.4) ------------------------------------
        self._alive: bool = True
        self._shutting_down: bool = False

        # TR response wait loop/timer
        self._loop  = QEventLoop()
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._on_timeout)

        # TR result buffers
        self._data:          List[List] = []
        self._single_data:   Dict[str, str] = {}
        self._prev_next:     str        = "0"
        self._timed_out:     bool       = False
        self._msg_rejected:  bool       = False
        self._current_rqname: str       = ""
        self._current_trcode: str       = ""

        # Event handlers
        self._k.OnReceiveTrData.connect(self._on_tr_data)
        self._k.OnReceiveMsg.connect(self._on_msg)

        # Stock info cache
        self._stock_info_cache: Dict[str, Dict] = {}

        # Consecutive timeout counter
        self._consecutive_timeout: int = 0

        # Sector map
        self._sector_map: Dict[str, str] = self._load_sector_map(sector_map_path)
        logger.info("[Gen4KiwoomProvider] sector map: %d tickers", len(self._sector_map))

        # -- Order/fill related --------------------------------------------
        self._order_loop   = QEventLoop()
        self._order_timer  = QTimer()
        self._order_timer.setSingleShot(True)
        self._order_timer.timeout.connect(self._on_order_timeout)

        self._order_state: Dict = self._make_order_state()
        self._order_result: Optional[Dict] = None
        self._ghost_orders: List[Dict] = []
        self._completed_order_nos: set = set()  # terminal states only: FILLED/CANCELLED/REJECTED
        self._seen_chejan_events: Dict[str, set] = {}  # per-order dedup {order_no: set()}
        self._chejan_fallthrough_count: int = 0
        self._chejan_fallthrough_alert_threshold: int = 50
        self._global_chejan_dedup: Dict[str, float] = {}  # {event_key: timestamp} TTL-based dedup
        self._chejan_dedup_ttl: float = 300.0  # 5min TTL — expire stale entries
        self._ghost_fill_callback = None         # external ghost fill handler
        self._ghost_fill_dedup: set = set()      # dedup for ghost fill events
        self._processed_fill_keys: set = set()   # dedup: (code, order_no, exec_qty, exec_price)
        self._pending_chejan: List[Dict] = []    # unmatched events awaiting confirmation

        self._k.OnReceiveChejanData.connect(self._on_chejan_data)

        # -- Real-time data (intraday minute bar collection) -------------------
        self._k.OnReceiveRealData.connect(self._on_real_data)
        self._real_data_callback = None
        self._real_registered_codes: List[str] = []

    # -- Sector map -----------------------------------------------------------

    @staticmethod
    def _load_sector_map(path: Optional[str]) -> Dict[str, str]:
        import json
        if path is None:
            path = Path(__file__).resolve().parent / "sector_map.json"
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("[Gen4KiwoomProvider] sector_map.json load failed: %s", e)
            return {}

    # -- dynamicCall central wrapper ------------------------------------------

    def _call(self, method_sig: str, *args, context: str = ""):
        """
        Kiwoom dynamicCall central wrapper.

        All Kiwoom API calls go through here with list-style args and failure logging.

        Args:
            method_sig: "MethodName(Type,Type,...)" format
            *args: method arguments (individual, not list)
            context: logging context (e.g. "opt10081/...")

        Returns:
            dynamicCall return value

        Raises:
            ProviderDeadError: COM object released
            KiwoomApiError: dynamicCall exception
        """
        if not self._alive:
            raise ProviderDeadError(
                f"provider dead — {method_sig} call blocked"
            )
        try:
            return self._k.dynamicCall(method_sig, list(args))
        except RuntimeError as e:
            if "deleted" in str(e).lower() or "C/C++ object" in str(e):
                self._alive = False
                logger.critical(
                    "[ProviderDead] QAxWidget deleted — alive=False: %s", e,
                )
                raise ProviderDeadError(
                    f"QAxWidget deleted: {method_sig}"
                ) from e
            ctx = f" [{context}]" if context else ""
            logger.error(
                "[dynamicCall FAIL%s] %s args=%s — %s: %s",
                ctx, method_sig, args, type(e).__name__, e,
            )
            raise KiwoomApiError(
                f"dynamicCall failed: {method_sig} args={args}{ctx}"
            ) from e
        except Exception as e:
            ctx = f" [{context}]" if context else ""
            logger.error(
                "[dynamicCall FAIL%s] %s args=%s — %s: %s",
                ctx, method_sig, args, type(e).__name__, e,
            )
            raise KiwoomApiError(
                f"dynamicCall failed: {method_sig} args={args}{ctx}"
            ) from e

    # -- Shutdown (simplified — no SetRealRemove) -----------------------------

    def shutdown(self) -> None:
        """
        Safe shutdown:
        1) _shutting_down=True — block callbacks
        2) signal disconnect
        3) _alive=False — block all _call()
        """
        if self._shutting_down or not self._alive:
            return
        self._shutting_down = True
        logger.info("[Gen4KiwoomProvider] shutdown start")

        for sig_name in ("OnReceiveTrData", "OnReceiveMsg", "OnReceiveChejanData"):
            try:
                getattr(self._k, sig_name).disconnect()
            except (TypeError, RuntimeError):
                pass

        self._alive = False
        logger.info("[Gen4KiwoomProvider] shutdown complete — alive=False")

    @property
    def alive(self) -> bool:
        return self._alive

    # -- Connection check -----------------------------------------------------

    def is_connected(self) -> bool:
        """Kiwoom API connection state. 0=disconnected, 1=connected."""
        try:
            state = self._k.dynamicCall("GetConnectState()")
            return state == 1
        except Exception:
            return False

    def ensure_connected(self) -> bool:
        """
        Detect disconnection and attempt reconnect (max 1 try).
        Returns: True=connected, False=reconnect failed
        """
        if self.is_connected():
            return True

        logger.warning("[Gen4KiwoomProvider] *** disconnection detected — reconnecting ***")
        try:
            ret = self._k.dynamicCall("CommConnect()")
            if ret != 0:
                logger.error("[Gen4KiwoomProvider] CommConnect failed (ret=%d)", ret)
                return False

            loop = QEventLoop()
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(loop.quit)
            timer.start(15000)
            loop.exec_()

            connected = self.is_connected()
            if connected:
                logger.info("[Gen4KiwoomProvider] reconnected successfully")
            else:
                logger.error("[Gen4KiwoomProvider] reconnect failed — 15s timeout")
            return connected

        except Exception as e:
            logger.error("[Gen4KiwoomProvider] reconnect exception: %s", e)
            return False

    # -- Stock info (no TR, master sync functions only) -----------------------

    def get_stock_info(self, code: str) -> dict:
        """
        Stock basic info (no TR — Kiwoom master sync functions only).

        - Name: GetMasterCodeName
        - Listed shares: GetMasterListedStockCnt
        - Price: GetMasterLastPrice
        - Market cap: price x listed shares
        """
        if code in self._stock_info_cache:
            return self._stock_info_cache[code]

        name_raw   = self._call("GetMasterCodeName(QString)", code)
        listed_raw = self._call("GetMasterListedStockCnt(QString)", code)
        price_raw  = self._call("GetMasterLastPrice(QString)", code)

        name       = str(name_raw).strip() if name_raw is not None else ""
        listed     = self._to_int(listed_raw)
        price      = self._to_int(price_raw)
        market_cap = price * listed

        info = {
            "name":          name,
            "sector":        self._sector_map.get(code, "기타"),
            "market":        "",
            "market_cap":    market_cap,
            "listed_shares": listed,
        }

        self._stock_info_cache[code] = info
        return info

    # -- Current price --------------------------------------------------------

    def get_current_price(self, code: str) -> float:
        """Current price (GetMasterLastPrice sync call)."""
        price = self._call("GetMasterLastPrice(QString)", code)
        try:
            return float(str(price).replace(",", "").replace(" ", ""))
        except (TypeError, ValueError):
            return 0.0

    # -- Real-time data (SetRealReg / OnReceiveRealData) ---------------------

    SCREEN_REAL = "8001"

    def register_real(self, codes: List[str], fids: str = "10;27") -> None:
        """Register for real-time price+volume ticks via SetRealReg."""
        if not codes:
            return
        code_str = ";".join(codes)
        self._call(
            "SetRealReg(QString,QString,QString,QString)",
            self.SCREEN_REAL, code_str, fids, "0",
        )
        self._real_registered_codes = list(codes)
        logger.info("[RealData] SetRealReg %d codes, FIDs=%s", len(codes), fids)

    def unregister_real(self) -> None:
        """Unregister all real-time feeds."""
        try:
            self._call(
                "SetRealRemove(QString,QString)", self.SCREEN_REAL, "ALL",
            )
            self._real_registered_codes = []
            logger.info("[RealData] SetRealRemove screen=%s", self.SCREEN_REAL)
        except Exception as e:
            logger.warning("[RealData] unregister_real failed: %s", e)

    def set_real_data_callback(self, callback) -> None:
        """Register external callback: callback(code, price, volume)."""
        self._real_data_callback = callback

    def _on_real_data(self, code: str, real_type: str, real_data: str) -> None:
        """OnReceiveRealData handler — extract FID 10 (price), FID 27 (volume)."""
        if not self._alive or self._real_data_callback is None:
            return
        code = code.strip()
        try:
            price_raw = self._call("GetCommRealData(QString,int)", code, 10)
            vol_raw = self._call("GetCommRealData(QString,int)", code, 27)
            price = abs(float(str(price_raw).strip()))
            volume = abs(int(str(vol_raw).strip()))
            if price > 0:
                self._real_data_callback(code, price, volume)
        except (ValueError, TypeError) as e:
            logger.debug("[REALDATA_PARSE_FAIL] code=%s price_raw=%s vol_raw=%s: %s",
                         code, price_raw, vol_raw, e)
        except Exception as e:
            logger.warning("[REALDATA_PROCESS_FAIL] code=%s: %s", code, e)

    # -- Account number -------------------------------------------------------

    def get_account_no(self) -> str:
        """Return first account number."""
        raw = self._call("GetLoginInfo(QString)", "ACCNO")
        accts = str(raw).strip().rstrip(";").split(";")
        if accts and accts[0]:
            return accts[0]
        logger.error("[Gen4KiwoomProvider] account number query failed")
        return ""

    # -- Account holdings (opw00018) ------------------------------------------

    def query_account_holdings(self) -> List[Dict]:
        """
        Query Kiwoom account holdings (opw00018).
        Returns: [{"code", "name", "qty", "avg_price", "cur_price", "pnl"}, ...]
        """
        account = self.get_account_no()
        if not account:
            logger.error("[Holdings] account number query failed")
            return []

        def _setup():
            self._call("SetInputValue(QString,QString)", "계좌번호", account)
            self._call("SetInputValue(QString,QString)", "비밀번호", "")
            self._call("SetInputValue(QString,QString)", "비밀번호입력매체구분", "00")
            self._call("SetInputValue(QString,QString)", "조회구분", "1")

        try:
            rows = self._request_tr_with_retry(
                trcode="opw00018",
                rqname="계좌평가잔고내역",
                days=9999,
                setup_func=_setup,
            )
        except ProviderDeadError:
            logger.critical("[Holdings] provider dead — opw00018 abort")
            return []
        except (KiwoomApiError, TrTimeoutError, RuntimeError) as e:
            logger.error("[Holdings] opw00018 query failed: %s", e)
            return []

        if not rows:
            logger.info("[Holdings] opw00018: no holdings (normal)")
            return []

        holdings = []
        for row in rows:
            code, name, qty_s, avg_s, cur_s, pnl_s = row
            qty       = abs(self._to_int(qty_s))
            avg_price = abs(self._to_int(avg_s))
            cur_price = abs(self._to_int(cur_s))
            pnl       = self._to_int(pnl_s)

            if qty > 0:
                holdings.append({
                    "code":      code,
                    "name":      name,
                    "qty":       qty,
                    "quantity":  qty,  # dual-key for schema compat
                    "avg_price": avg_price,
                    "cur_price": cur_price,
                    "pnl":       pnl,
                })

        logger.info("[Holdings] %d holdings retrieved", len(holdings))
        return holdings

    # -- Account summary (opw00018 single output) ----------------------------

    def query_account_summary(self) -> Dict:
        """
        Query Kiwoom account summary (opw00018 single output).

        Returns: {
            "추정예탁자산": int,
            "총매입금액":   int,
            "총평가금액":   int,
            "총평가손익금액": int,
            "holdings":     [...],
            "available_cash": int,
            "error":         str,
        }
        """
        account = self.get_account_no()
        if not account:
            return {"error": "account number query failed", "holdings": []}

        self._single_data = {}

        def _setup():
            self._call("SetInputValue(QString,QString)", "계좌번호", account)
            self._call("SetInputValue(QString,QString)", "비밀번호", "")
            self._call("SetInputValue(QString,QString)", "비밀번호입력매체구분", "00")
            self._call("SetInputValue(QString,QString)", "조회구분", "1")

        try:
            rows = self._request_tr_with_retry(
                trcode="opw00018",
                rqname="계좌평가잔고내역",
                days=9999,
                setup_func=_setup,
            )
        except ProviderDeadError:
            logger.critical("[AccountSync] provider dead — opw00018 abort")
            return {"error": "ProviderDeadError", "holdings": []}
        except (KiwoomApiError, TrTimeoutError, RuntimeError) as e:
            logger.error("[AccountSync] opw00018 query failed: %s", e)
            return {"error": str(e), "holdings": []}

        if self._msg_rejected and not rows:
            deposit = abs(self._to_int(self._single_data.get("추정예탁자산", "0")))
            avail   = deposit
            logger.warning(
                "[AccountSync] opw00018 server rejected (msg_rejected) — "
                "deposit=%s, holdings unreliable", f"{deposit:,}",
            )
            return {
                "추정예탁자산": deposit, "총매입금액": 0, "총평가금액": 0,
                "총평가손익금액": 0, "holdings": [], "available_cash": avail,
                "error": "",
                "holdings_reliable": False,
            }

        if not rows and not self._single_data:
            logger.info("[AccountSync] empty account (0 positions, no single_data)")
            return {
                "추정예탁자산": 0, "총매입금액": 0, "총평가금액": 0,
                "총평가손익금액": 0, "holdings": [], "available_cash": 0,
                "error": "empty_account",
            }

        deposit    = abs(self._to_int(self._single_data.get("추정예탁자산", "0")))
        total_buy  = abs(self._to_int(self._single_data.get("총매입금액", "0")))
        total_eval = abs(self._to_int(self._single_data.get("총평가금액", "0")))
        total_pnl  = self._to_int(self._single_data.get("총평가손익금액", "0"))

        holdings = []
        for row in (rows or []):
            code, name, qty_s, avg_s, cur_s, pnl_s = row
            qty       = abs(self._to_int(qty_s))
            avg_price = abs(self._to_int(avg_s))
            cur_price = abs(self._to_int(cur_s))
            pnl       = self._to_int(pnl_s)
            if qty > 0:
                holdings.append({
                    "code": code, "name": name,
                    "qty": qty, "quantity": qty,  # dual-key for schema compat
                    "avg_price": avg_price,
                    "cur_price": cur_price, "pnl": pnl,
                })

        holdings_mkt_val = sum(h["cur_price"] * h["qty"] for h in holdings)
        available_cash = deposit - holdings_mkt_val if deposit > 0 else 0

        logger.info(
            "[AccountSync] deposit=%s, eval=%s, cash=%s, holdings=%d",
            f"{deposit:,}", f"{total_eval:,}", f"{available_cash:,}", len(holdings),
        )

        return {
            "추정예탁자산":    deposit,
            "총매입금액":      total_buy,
            "총평가금액":      total_eval,
            "총평가손익금액":  total_pnl,
            "holdings":       holdings,
            "available_cash": available_cash,
            "error":          "",
            "holdings_reliable": True,
        }

    # -- Sellable quantity ----------------------------------------------------

    def query_sellable_qty(self, code: str) -> Dict:
        """
        v7.6: Per-ticker sellable quantity query (opw00018 based).

        Returns: {
            "code": str,
            "hold_qty": int,
            "sellable_qty": int,
            "source": "opw00018",
            "error": str,
        }
        """
        summary = self.query_account_summary()
        if summary.get("error") and summary["error"] not in ("", "empty_account"):
            return {"code": code, "hold_qty": -1, "sellable_qty": -1,
                    "source": "opw00018", "error": summary["error"]}

        if not summary.get("holdings_reliable", True):
            return {"code": code, "hold_qty": -1, "sellable_qty": -1,
                    "source": "opw00018", "error": "holdings_not_reliable"}

        for h in summary.get("holdings", []):
            if h["code"] == code:
                return {"code": code, "hold_qty": h["qty"],
                        "sellable_qty": -1,
                        "sellable_source": "UNKNOWN_SELLABLE",
                        "source": "opw00018", "error": ""}

        return {"code": code, "hold_qty": 0, "sellable_qty": 0,
                "sellable_source": "BROKER_CONFIRMED_ZERO",
                "source": "opw00018", "error": ""}

    # -- Send order -----------------------------------------------------------

    def send_order(self, code: str, side: str, quantity: int,
                   price: int = 0, hoga_type: str = "03") -> Dict:
        """
        Kiwoom SendOrder + fill wait.

        side:      "BUY" | "SELL"
        hoga_type: "03" = market (default), "00" = limit
        price:     0 for market orders

        Returns: {"order_no": str, "exec_price": float, "exec_qty": int, "error": str}

        On timeout: registers GHOST ORDER — may have filled on broker side,
        so not immediately rejected. Tracked as TIMEOUT_PENDING.
        """
        account = self.get_account_no()
        if not account:
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "account number query failed"}

        order_type = 1 if side == "BUY" else 2
        rqname     = f"{'매수' if side == 'BUY' else '매도'}_{code}"
        screen     = "7001"

        # Initialize order state
        self._order_state = self._make_order_state(
            code=code, side=side, requested_qty=quantity,
            status="REQUESTED", timestamp=datetime.now(),
        )
        self._order_result = None
        # Per-order dedup: clear fill keys for THIS order's fresh tracking.
        # Previous order's ghost fills are handled by ghost_fill_callback.
        self._processed_fill_keys.clear()
        # Keep _global_chejan_dedup — ghost fills from previous orders
        # still need dedup protection across order boundaries

        time.sleep(0.2)  # Min interval between orders

        ret = self._call(
            "SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)",
            rqname, screen, account, order_type, code, quantity, int(price), hoga_type, "",
            context=f"SendOrder/{side}/{code}/{quantity}qty",
        )

        if ret != 0:
            self._order_state["status"] = "REJECTED"
            logger.error("[SendOrder] %s %s %d qty failed ret=%d", side, code, quantity, ret)
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"SendOrder ret={ret}"}

        logger.info("[SendOrder] %s %s %d qty accepted (market, screen=%s)", side, code, quantity, screen)

        # Wait for fill
        self._order_timer.start(ORDER_TIMEOUT_SEC * 1000)
        self._order_loop.exec_()
        self._order_timer.stop()

        # Expire stale pending events from previous orders
        self._expire_stale_pending()

        if self._order_result is None:
            # -- TIMEOUT_UNCERTAIN — no fill at all --
            self._order_state["status"] = "TIMEOUT_UNCERTAIN"
            ghost = self._order_state.copy()
            ghost["applied_qty"] = 0  # ghost callback으로 반영된 수량
            self._ghost_orders.append(ghost)
            # Do NOT add to _completed_order_nos — ghost must remain matchable
            # for late fills. Only terminal states (FILLED/CANCELLED/REJECTED)
            # should mark order_no as completed.

            logger.critical(
                "[TIMEOUT_UNCERTAIN] %s %s %d qty — fill unconfirmed (timeout %ds). "
                "May have filled on broker! Check HTS. order_no=%s",
                side, code, quantity, ORDER_TIMEOUT_SEC,
                self._order_state["order_no"],
            )
            return {"order_no": self._order_state["order_no"],
                    "exec_price": 0.0, "exec_qty": 0,
                    "requested_qty": quantity,
                    "error": f"TIMEOUT_UNCERTAIN — {ORDER_TIMEOUT_SEC}s fill unconfirmed"}

        if self._order_result["exec_qty"] < quantity:
            # -- PARTIAL_TIMEOUT — some fills received, but not all --
            ghost = self._order_state.copy()
            ghost["status"] = "PARTIAL_TIMEOUT"
            ghost["timeout_filled_qty"] = self._order_result["exec_qty"]
            ghost["applied_qty"] = self._order_result["exec_qty"]  # timeout 체결분은 main.py에서 직접 반영
            self._ghost_orders.append(ghost)
            # Mark active order as timed out so subsequent chejan routes to ghost path
            self._order_state["status"] = "PARTIAL_TIMEOUT"
            # Do NOT add to _completed_order_nos — ghost must remain matchable
            # for late fills via fallthrough (FIX 1+2).

            filled = self._order_result["exec_qty"]
            logger.warning(
                "[PARTIAL_TIMEOUT] %s %s %d/%d filled @ %.0f (timeout %ds). "
                "Remainder %d may still fill on broker. order_no=%s",
                side, code, filled, quantity,
                self._order_result["exec_price"], ORDER_TIMEOUT_SEC,
                quantity - filled, self._order_state["order_no"],
            )
            self._order_result["requested_qty"] = quantity
            return self._order_result  # error="" → partial fill applied to portfolio

        # -- Normal fill --
        self._order_state["status"]         = "FILLED"
        self._order_state["order_no"]       = self._order_result["order_no"]
        self._order_state["filled_qty"]     = self._order_result["exec_qty"]
        self._order_state["avg_fill_price"] = self._order_result["exec_price"]

        # Track completed order_no to prevent stale chejan contamination
        if self._order_result["order_no"]:
            self._completed_order_nos.add(self._order_result["order_no"])
            self._drain_pending_chejan(self._order_result["order_no"])

        self._order_result["requested_qty"] = quantity

        logger.info(
            "[OrderState] %s %s FILLED %d/%d qty @ %.0f (order_no=%s)",
            side, code,
            self._order_state["filled_qty"],
            self._order_state["requested_qty"],
            self._order_state["avg_fill_price"],
            self._order_state["order_no"],
        )
        return self._order_result

    # -- Chejan (fill detection) — verbatim -----------------------------------

    def _on_chejan_data(self, gubun, item_cnt, fid_list):
        """
        OnReceiveChejanData event handler.
        gubun: "0" = order/fill notification, "1" = balance notification

        Matching logic (v8 — strict order_no):
          1) Active order match: strict order_no after capture, ticker fallback before
          2) FILLED + post-fill events -> silently ignored
          3) Ghost order match (delayed fill after timeout) -> CRITICAL log
          4) All other events -> silently dropped (no noise logs)
        """
        if not self._alive or self._shutting_down:
            return
        if str(gubun) != "0":
            return

        code = str(self._call("GetChejanData(int)", 9001)).strip()
        code = code.lstrip("A")
        order_no       = str(self._call("GetChejanData(int)", 9203)).strip()
        exec_qty_raw   = str(self._call("GetChejanData(int)", 911)).strip()
        exec_price_raw = str(self._call("GetChejanData(int)", 910)).strip()
        _raw_status    = str(self._call("GetChejanData(int)", 913)).strip()
        order_status   = self._decode_kiwoom_str(_raw_status)

        # Fallback when decoding fails
        if order_status and not order_status.isascii() and order_status == _raw_status:
            if exec_qty_raw and abs(int(exec_qty_raw)) > 0 and exec_price_raw and abs(float(exec_price_raw)) > 0:
                order_status = "체결"
            else:
                order_status = "접수"

        exec_qty   = abs(int(exec_qty_raw))   if exec_qty_raw   else 0
        exec_price = abs(float(exec_price_raw)) if exec_price_raw else 0.0

        # -- Event delay detection --
        try:
            exec_time_raw = str(self._call("GetChejanData(int)", 908)).strip()
            if exec_time_raw and len(exec_time_raw) >= 6:
                from datetime import datetime as _dt
                exec_dt = _dt.now().replace(
                    hour=int(exec_time_raw[:2]),
                    minute=int(exec_time_raw[2:4]),
                    second=int(exec_time_raw[4:6]),
                    microsecond=0)
                delay_sec = (_dt.now() - exec_dt).total_seconds()
                if 0 < delay_sec < 3600 and delay_sec > 5.0:
                    logger.warning(
                        "[EVENT_DELAY_WARNING] %s order_no=%s delay=%.1fs "
                        "(exec_time=%s)", code, order_no, delay_sec, exec_time_raw)
        except Exception:
            pass  # non-critical, don't block chejan processing

        # -- Dedup guard (per-order after capture, global before capture) --
        event_key = (order_no, exec_qty, exec_price, order_status)
        st = self._order_state

        if st["order_no"] and order_no == st["order_no"]:
            # Per-order dedup
            order_dedup = self._seen_chejan_events.setdefault(order_no, set())
            if event_key in order_dedup:
                return
            order_dedup.add(event_key)
        else:
            # Global dedup for pre-capture / unmatched events (TTL-based)
            now = time.time()
            # Expire stale entries periodically
            if len(self._global_chejan_dedup) > 500:
                cutoff = now - self._chejan_dedup_ttl
                expired = [k for k, ts in self._global_chejan_dedup.items() if ts < cutoff]
                for k in expired:
                    del self._global_chejan_dedup[k]
                if expired:
                    logger.debug(f"[CHEJAN_DEDUP_EXPIRE] {len(expired)} stale entries removed")
            if event_key in self._global_chejan_dedup:
                return
            self._global_chejan_dedup[event_key] = now

        # -- 0. Early reject: completed/terminal orders (incl. ghost) --
        if order_no and order_no in self._completed_order_nos:
            return  # fully settled, ignore all further chejan

        # -- 0b. Active ghost shortcut (skip section 1 fallthrough noise) --
        _is_active_ghost = False
        if order_no:
            for _g in self._ghost_orders:
                if (_g.get("order_no") == order_no and
                    _g["status"] in ("TIMEOUT_UNCERTAIN", "PARTIAL_TIMEOUT",
                                     "TIMEOUT_PENDING", "GHOST_FILLING")):
                    _is_active_ghost = True
                    break

        # -- 1. Active order match --
        if not _is_active_ghost and st["status"] in ("REQUESTED", "ACCEPTED", "PARTIAL"):
            if st["order_no"]:
                # order_no captured → strict match only
                if order_no == st["order_no"]:
                    self._process_chejan_fill(code, order_no, exec_qty, exec_price, order_status)
                    return  # fill processed for active order
                else:
                    self._chejan_fallthrough_count += 1
                    if self._chejan_fallthrough_count <= 5 or \
                       self._chejan_fallthrough_count % 20 == 0:
                        logger.info(
                            "[Chejan FALLTHROUGH] order_no mismatch: active=%s "
                            "received=%s code=%s (total=%d)",
                            st["order_no"], order_no, code,
                            self._chejan_fallthrough_count)
                    if self._chejan_fallthrough_count == self._chejan_fallthrough_alert_threshold:
                        logger.warning(
                            "[CHEJAN_FALLTHROUGH_ALERT] %d fallthroughs — "
                            "excessive chejan noise from completed orders",
                            self._chejan_fallthrough_count)
                    # No return — fall through to ghost order matching (section 3)
            else:
                # order_no not yet captured → conservative multi-condition matching
                if not order_no:
                    logger.debug("[ORDER_MATCH_REJECTED] no order_no, code=%s", code)
                    return
                if order_no in self._completed_order_nos:
                    logger.debug("[ORDER_MATCH_REJECTED] stale order_no=%s (completed)", order_no)
                    return

                # Multi-condition matching: code is REQUIRED + 1 more
                if code != st["code"]:
                    # Wrong code — cannot be this order
                    logger.debug("[ORDER_MATCH_REJECTED] code mismatch: "
                                 "active=%s received=%s", st["code"], code)
                    return

                match_score = 1  # code match is baseline (required)
                match_reasons = ["code"]

                # Condition 2: time window (within 5s of order submission)
                if st.get("timestamp"):
                    elapsed = (datetime.now() - st["timestamp"]).total_seconds()
                    if elapsed <= 5.0:
                        match_score += 1
                        match_reasons.append(f"time({elapsed:.1f}s)")

                # Condition 3: qty range match (exec_qty <= requested)
                if 0 < exec_qty <= st["requested_qty"]:
                    match_score += 1
                    match_reasons.append("qty_range")

                if match_score >= 2:  # code + at least 1 more
                    # Confirmed match
                    st["order_no"] = order_no
                    # Initialize per-order dedup with this event
                    self._seen_chejan_events[order_no] = {event_key}
                    logger.info(
                        "[ORDER_MATCH_CONFIRMED] order_no=%s code=%s "
                        "score=%d reasons=[%s]",
                        order_no, code, match_score, ",".join(match_reasons))
                    self._process_chejan_fill(code, order_no, exec_qty, exec_price, order_status)

                    # Replay any pending events for this order_no
                    replay = [p for p in self._pending_chejan
                              if p["order_no"] == order_no]
                    for p in replay:
                        self._pending_chejan.remove(p)
                        logger.info("[ORDER_MATCH_PENDING] replaying %s for %s",
                                    p["order_no"], code)
                        self._process_chejan_fill(
                            p["code"], p["order_no"],
                            p["exec_qty"], p["exec_price"], p["order_status"])
                else:
                    # Not enough evidence — hold in pending
                    self._pending_chejan.append({
                        "order_no": order_no, "code": code,
                        "exec_qty": exec_qty, "exec_price": exec_price,
                        "order_status": order_status, "timestamp": datetime.now(),
                    })
                    logger.info(
                        "[ORDER_MATCH_PENDING] order_no=%s code=%s "
                        "score=%d reasons=[%s] — holding for confirmation",
                        order_no, code, match_score, ",".join(match_reasons))
                return

        # -- 2. FILLED → silently ignore post-fill events --
        if not _is_active_ghost and st["status"] == "FILLED":
            return

        # -- 3. Ghost order match (delayed fill after timeout) --
        for ghost in self._ghost_orders:
            # GHOST_FILLED = terminal → reject further chejan
            if ghost["status"] == "GHOST_FILLED":
                if ghost.get("order_no") == order_no:
                    return  # terminal, ignore
                continue
            if ghost["status"] not in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN",
                                        "PARTIAL_TIMEOUT", "GHOST_FILLING"):
                continue
            # Strict: order_no match only (no ticker fallback for ghosts)
            if ghost.get("order_no") and order_no and order_no == ghost["order_no"]:
                if exec_qty > 0 and exec_price > 0:
                    # Dedup: raw exec_qty + prev_filled 기준
                    _prev_f = ghost.get("filled_qty", 0)
                    ghost_dedup_key = (order_no, _prev_f + exec_qty, exec_qty)
                    if ghost_dedup_key in self._ghost_fill_dedup:
                        logger.debug("[GHOST_FILL_DUP] order_no=%s cum=%d exec=%d",
                                     order_no, _prev_f + exec_qty, exec_qty)
                        return
                    self._ghost_fill_dedup.add(ghost_dedup_key)

                    prev_filled = _prev_f
                    prev_applied = ghost.get("applied_qty", 0)
                    requested = ghost.get("requested_qty", 0)

                    # Pre-clamp: same as active path (min to remaining)
                    remaining = max(0, requested - prev_filled)
                    if remaining <= 0:
                        logger.info(
                            "[GHOST_FILL_SATURATED] %s %s already filled %d/%d, "
                            "ignoring +%d (order_no=%s)",
                            ghost["side"], code, prev_filled, requested,
                            exec_qty, order_no)
                        if ghost["status"] != "GHOST_FILLED":
                            ghost["status"] = "GHOST_FILLED"
                            self._completed_order_nos.add(order_no)
                        return
                    usable_qty = min(exec_qty, remaining)
                    new_filled = prev_filled + usable_qty

                    ghost["filled_qty"] = new_filled
                    ghost["avg_fill_price"] = exec_price

                    # Invariant: applied_qty <= requested (defense-in-depth)
                    if new_filled > requested:
                        logger.critical(
                            "[GHOST_INVARIANT_VIOLATION] %s %s filled=%d > "
                            "requested=%d — should never happen after pre-clamp "
                            "(order_no=%s)",
                            ghost["side"], code, new_filled, requested, order_no)
                        ghost["filled_qty"] = requested
                        new_filled = requested

                    # Terminal check
                    if new_filled >= requested:
                        ghost["status"] = "GHOST_FILLED"
                        delta = requested - prev_applied  # exact remainder
                    else:
                        ghost["status"] = "GHOST_FILLING"
                        delta = new_filled - prev_applied

                    # Invariant check: delta must be non-negative
                    if delta < 0:
                        logger.critical(
                            "[GHOST_NEGATIVE_DELTA] %s %s delta=%d "
                            "prev_applied=%d new_filled=%d requested=%d "
                            "(order_no=%s) — skipping",
                            ghost["side"], code, delta, prev_applied,
                            new_filled, requested, order_no)
                        return

                    logger.warning(
                        "[GHOST_FILL] %s %s exec=%d usable=%d filled=%d/%d "
                        "applied=%d delta=%d @ %.0f (order_no=%s) status=%s",
                        ghost["side"], code, exec_qty, usable_qty,
                        new_filled, requested, prev_applied, delta,
                        exec_price, order_no, ghost["status"])

                    # Dispatch delta to portfolio sync
                    is_terminal = ghost["status"] == "GHOST_FILLED"
                    if self._ghost_fill_callback and delta > 0:
                        try:
                            self._ghost_fill_callback({
                                "order_no": order_no,
                                "code": code,
                                "side": ghost["side"],
                                "exec_qty": delta,
                                "exec_price": exec_price,
                                "requested_qty": requested,
                                "already_recorded_qty": prev_applied,
                                "is_terminal": is_terminal,
                            })
                            ghost["applied_qty"] = prev_applied + delta
                            if is_terminal:
                                self._completed_order_nos.add(order_no)
                                logger.info(
                                    "[GHOST_FILL_FINALIZED] %s %s "
                                    "filled=%d/%d applied=%d (order_no=%s)",
                                    ghost["side"], code, new_filled, requested,
                                    ghost["applied_qty"], order_no)
                        except Exception as e:
                            logger.error("[GHOST_FILL_CALLBACK_ERROR] %s: %s",
                                         code, e, exc_info=True)
                            # applied_qty NOT updated → next chejan will retry
                return

    def _process_chejan_fill(self, code: str, order_no: str,
                             exec_qty: int, exec_price: float, order_status: str) -> None:
        """Process active order fill/partial fill/acceptance.

        Invariants enforced:
          0 <= filled <= requested
          remain = requested - filled >= 0
        """
        st = self._order_state
        requested = st["requested_qty"]

        if exec_qty > 0 and exec_price > 0:
            prev_qty   = self._order_result["exec_qty"]   if self._order_result else 0
            prev_price = self._order_result["exec_price"] if self._order_result else 0.0

            # -- Processed fill dedup (prevents double-apply via fallthrough) --
            # Include prev_qty (cumulative before this fill) to distinguish
            # genuinely separate partial fills with same (qty, price).
            fill_key = (code, order_no, exec_qty, exec_price, prev_qty)
            if fill_key in self._processed_fill_keys:
                logger.info(
                    "[Chejan DUP_IGNORED] %s order_no=%s qty=%d price=%.0f "
                    "cum=%d — already processed",
                    code, order_no, exec_qty, exec_price, prev_qty)
                return
            self._processed_fill_keys.add(fill_key)

            # -- Overfill guard (order_no-scoped) --
            remaining = max(0, requested - prev_qty)
            if remaining <= 0:
                logger.warning(
                    "[OVERFILL_IGNORED] %s %s already filled %d/%d, "
                    "ignoring +%d (order_no=%s)",
                    st["side"], code, prev_qty, requested, exec_qty, order_no,
                )
                return

            usable_qty = min(exec_qty, remaining)
            if usable_qty <= 0:
                return

            # -- Accumulate (using usable_qty only) --
            new_filled = prev_qty + usable_qty
            if new_filled > 0 and prev_qty > 0:
                avg_price = (prev_price * prev_qty + exec_price * usable_qty) / new_filled
            else:
                avg_price = exec_price

            # -- Enforce invariant: filled = min(new_filled, requested) --
            filled = min(new_filled, requested)
            remain = max(0, requested - filled)

            if self._order_result is None:
                self._order_result = {
                    "order_no":   order_no,
                    "exec_price": avg_price,
                    "exec_qty":   filled,
                    "error":      "",
                }
                st["order_no"] = order_no
                logger.info(
                    "[FILL] %s %s requested=%d exec=%d cum_filled=%d @ %.0f (order_no=%s)",
                    st["side"], code, requested, usable_qty, filled, exec_price, order_no,
                )
            else:
                self._order_result["exec_qty"]  = filled
                self._order_result["exec_price"] = avg_price
                logger.info(
                    "[PARTIAL] %s %s requested=%d exec=%d cum_filled=%d remain=%d "
                    "avg=%.0f (order_no=%s)",
                    st["side"], code, requested, usable_qty, filled, remain,
                    avg_price, order_no,
                )

            st["filled_qty"]     = filled
            st["avg_fill_price"] = avg_price

            # -- State transition --
            if remain == 0:
                st["status"] = "FILLED"
                if self._order_loop.isRunning():
                    self._order_loop.quit()
            else:
                st["status"] = "PARTIAL"
                logger.info("[PARTIAL WAIT] %s %d/%d qty (remaining %d)", code, filled, requested, remain)

        elif order_status in ("접수", "확인"):
            st["status"] = "ACCEPTED"
            if order_no and not st["order_no"]:
                st["order_no"] = order_no
                logger.info("[ACCEPTED] %s order_no=%s (awaiting fill)", code, order_no)

    # -- Order state management -----------------------------------------------

    @staticmethod
    def _make_order_state(**kwargs) -> Dict:
        """Create order state struct."""
        state = {
            "order_no":       "",
            "code":           "",
            "side":           "",
            "requested_qty":  0,
            "filled_qty":     0,
            "avg_fill_price": 0.0,
            "status":         "IDLE",
            # IDLE / REQUESTED / ACCEPTED / PARTIAL / FILLED / REJECTED / TIMEOUT_PENDING / GHOST_FILLED
            "timestamp":      None,
        }
        state.update(kwargs)
        return state

    def _drain_pending_chejan(self, order_no: str) -> None:
        """Process any pending chejan events that match this order_no."""
        if not order_no:
            return
        replay = [p for p in self._pending_chejan if p["order_no"] == order_no]
        for p in replay:
            self._pending_chejan.remove(p)
            logger.info("[ORDER_MATCH_PENDING] drain replay %s for %s",
                        p["order_no"], p["code"])
            self._process_chejan_fill(
                p["code"], p["order_no"],
                p["exec_qty"], p["exec_price"], p["order_status"])

    def _expire_stale_pending(self, max_age_sec: float = 60.0) -> None:
        """Remove pending chejan events older than max_age_sec."""
        now = datetime.now()
        expired = [p for p in self._pending_chejan
                   if (now - p["timestamp"]).total_seconds() > max_age_sec]
        for p in expired:
            self._pending_chejan.remove(p)
            logger.info("[ORDER_MATCH_TIMEOUT] expired pending order_no=%s code=%s "
                        "age=%.0fs", p["order_no"], p["code"],
                        (now - p["timestamp"]).total_seconds())

    def set_ghost_fill_callback(self, callback) -> None:
        """Register callback for delayed ghost fills: callback(ghost_info_dict).

        Called when a chejan event arrives for a timed-out order.
        The callback should update tracker + portfolio + state.
        """
        self._ghost_fill_callback = callback

    def get_ghost_orders(self) -> List[Dict]:
        """Unconfirmed orders after timeout. For RuntimeEngine/EOD warnings.
        GHOST_FILLED = fully settled via ghost fills → NOT unresolved.
        Only truly unresolved: TIMEOUT_PENDING, TIMEOUT_UNCERTAIN, GHOST_FILLING.
        """
        return [g for g in self._ghost_orders
                if g["status"] in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN", "GHOST_FILLING")]

    def clear_ghost_orders(self) -> None:
        """Clear ghost list after EOD processing."""
        self._ghost_orders.clear()

    # -- Open order query + cancel (for startup recovery) ---------------------

    def query_open_orders(self) -> Optional[List[Dict]]:
        """Query unfilled/partially-filled orders via opt10075 (미체결요청).
        Returns: list of dicts on success (may be empty []),
                 None on query failure (caller must distinguish).
        """
        account = self.get_account_no()
        if not account:
            logger.error("[OpenOrders] account number query failed")
            return None

        def _setup():
            self._call("SetInputValue(QString,QString)", "계좌번호", account)
            self._call("SetInputValue(QString,QString)", "비밀번호", "")
            self._call("SetInputValue(QString,QString)", "비밀번호입력매체구분", "00")
            self._call("SetInputValue(QString,QString)", "체결구분", "1")  # 1=미체결
            self._call("SetInputValue(QString,QString)", "매매구분", "0")  # 0=전체

        try:
            rows = self._request_tr_with_retry(
                trcode="opt10075",
                rqname="미체결요청",
                days=0,
                setup_func=_setup,
            )
        except Exception as e:
            logger.error("[OpenOrders] query failed: %s", e)
            return None

        # Log raw response for first-time validation
        if rows:
            logger.info("[OpenOrders] raw row count=%d, first_row_cols=%d",
                        len(rows), len(rows[0]) if rows[0] else 0)
            # Log first 3 rows raw for field mapping verification
            for i, row in enumerate(rows[:3]):
                logger.info("[OpenOrders_RAW] row[%d]: %s", i,
                            [str(v).strip()[:20] for v in row[:12]])
        else:
            logger.info("[OpenOrders] empty response (0 rows)")

        results = []
        for row in rows:
            if len(row) < 10:
                logger.warning("[OpenOrders_SKIP] row too short: %d cols", len(row))
                continue
            order_no = str(row[1]).strip()
            code = str(row[2]).strip().replace("A", "")
            side_raw = str(row[5]).strip()
            side = "BUY" if "매수" in side_raw else "SELL" if "매도" in side_raw else side_raw
            status_raw = str(row[9]).strip() if len(row) > 9 else ""
            try:
                qty = abs(int(str(row[6]).strip().replace(",", "")))
                filled = abs(int(str(row[7]).strip().replace(",", "")))
            except (ValueError, IndexError):
                qty, filled = 0, 0
            order_time = str(row[0]).strip()

            # Log every parsed order for verification
            logger.info("[OpenOrders_PARSED] order_no=%s code=%s side=%s "
                        "qty=%d filled=%d remain=%d status=%s time=%s",
                        order_no, code, side, qty, filled,
                        qty - filled, status_raw, order_time)

            if order_no and qty > filled:
                results.append({
                    "order_no": order_no,
                    "code": code,
                    "side": side,
                    "qty": qty,
                    "filled_qty": filled,
                    "remaining": qty - filled,
                    "order_time": order_time,
                    "status_raw": status_raw,
                })

        logger.info("[OpenOrders] %d unfilled orders found", len(results))
        return results

    def cancel_order(self, code: str, order_no: str, qty: int,
                     side: str = "BUY") -> Dict:
        """Cancel an open order. Returns {"ok": bool, "error": str}."""
        account = self.get_account_no()
        if not account:
            return {"ok": False, "error": "account number query failed"}

        # order_type: 3=매수취소, 4=매도취소
        order_type = 3 if side == "BUY" else 4
        rqname = f"취소_{code}_{order_no}"
        screen = "7002"

        try:
            ret = self._call(
                "SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)",
                rqname, screen, account, order_type, code, qty, 0, "00", order_no,
                context=f"Cancel/{side}/{code}/order_no={order_no}",
            )
            if ret == 0:
                logger.info("[CancelOrder] %s %s order_no=%s qty=%d — accepted",
                            side, code, order_no, qty)
                time.sleep(0.3)  # Allow cancel to process
                return {"ok": True, "error": ""}
            else:
                logger.warning("[CancelOrder] %s %s order_no=%s ret=%d",
                               side, code, order_no, ret)
                return {"ok": False, "error": f"ret={ret}"}
        except Exception as e:
            logger.error("[CancelOrder] %s %s order_no=%s — %s",
                         side, code, order_no, e)
            return {"ok": False, "error": str(e)}

    def cancel_all_open_orders(self) -> Optional[int]:
        """Query and cancel ALL open orders, then verify.
        Returns count of successfully cancelled orders, or None if query failed."""
        orders = self.query_open_orders()
        if orders is None:
            logger.critical("[CancelAll] Open order query FAILED — cannot determine stale orders")
            return None
        if len(orders) == 0:
            logger.info("[CancelAll] No open orders to cancel")
            return 0

        logger.warning("[OPEN_ORDERS_BEFORE_CANCEL] n=%d — cancelling all", len(orders))
        cancelled = 0
        for o in orders:
            logger.info("[CancelAll] Cancelling %s %s order_no=%s remain=%d",
                        o["side"], o["code"], o["order_no"], o["remaining"])
            result = self.cancel_order(
                code=o["code"],
                order_no=o["order_no"],
                qty=o["remaining"],
                side=o["side"],
            )
            if result["ok"]:
                cancelled += 1
            else:
                logger.warning("[CancelAll] Failed: %s %s — %s",
                               o["code"], o["order_no"], result["error"])
            time.sleep(0.5)  # Rate limit

        logger.info("[CancelAll] %d/%d cancel requests sent", cancelled, len(orders))

        # Verify: re-query after 3s to confirm cancels took effect
        if cancelled > 0:
            time.sleep(3.0)
            remaining = self.query_open_orders()
            if remaining is None:
                logger.warning("[CancelAll_VERIFY] Re-query failed — cannot confirm cancels")
            elif len(remaining) > 0:
                logger.warning("[CancelAll_VERIFY] %d orders STILL open after cancel!",
                               len(remaining))
                for r in remaining:
                    logger.warning("[CancelAll_VERIFY]   %s %s order_no=%s remain=%d",
                                   r["side"], r["code"], r["order_no"], r["remaining"])
            else:
                logger.info("[CancelAll_VERIFY] All orders cancelled — 0 remaining")

        return cancelled

    # -- TR event handler (opw00018 only) -------------------------------------

    def _on_tr_data(
        self,
        screen_no,
        rqname,
        trcode,
        recordname,
        prev_next,
        *args,
    ):
        """
        OnReceiveTrData event handler.
        Only handles opw00018 (account holdings).
        """
        if not self._alive or self._shutting_down:
            return
        if self._timed_out:
            return

        if trcode != self._current_trcode:
            logger.debug(
                "[TR ignored] expected(%s) received(%s/%s)",
                self._current_trcode, rqname, trcode,
            )
            return

        self._prev_next = prev_next

        if trcode == "opw00018":
            self._parse_opw00018(trcode, rqname)
        elif trcode == "opt20006":
            self._parse_opt20006(trcode, rqname)
        elif trcode == "opt10075":
            self._parse_opt10075(trcode, rqname)

        self._loop.quit()

    def _parse_opw00018(self, trcode: str, rqname: str) -> None:
        """opw00018 account holdings parse (single + multi)."""
        _get = lambda idx, field: self._call(
            "GetCommData(QString,QString,int,QString)", trcode, rqname, idx, field,
        )
        # -- Single output: account summary --
        for field in ["총매입금액", "총평가금액", "추정예탁자산", "총평가손익금액"]:
            self._single_data[field] = str(_get(0, field)).strip()

        # -- Multi output: holdings --
        rows: List[List] = []
        i = 0
        while True:
            code = str(_get(i, "종목번호")).strip().lstrip("A")
            if not code:
                break

            name     = self._decode_kiwoom_str(_get(i, "종목명"))
            qty      = str(_get(i, "보유수량")).strip()
            avg_cost = str(_get(i, "매입가")).strip()
            cur_price= str(_get(i, "현재가")).strip()
            pnl      = str(_get(i, "평가손익")).strip()

            rows.append([code, name, qty, avg_cost, cur_price, pnl])
            i += 1

        self._data.extend(rows)

    def _parse_opt20006(self, trcode: str, rqname: str) -> None:
        """opt20006: index daily candle parse (date, open, high, low, close, volume)."""
        _get = lambda idx, field: self._call(
            "GetCommData(QString,QString,int,QString)", trcode, rqname, idx, field,
        )
        rows: List[List] = []
        i = 0
        while True:
            dt = str(_get(i, "일자")).strip()
            if not dt:
                break
            open_  = str(_get(i, "시가")).strip()
            high_  = str(_get(i, "고가")).strip()
            low_   = str(_get(i, "저가")).strip()
            close_ = str(_get(i, "현재가")).strip()
            vol_   = str(_get(i, "거래량")).strip()
            rows.append([dt, open_, high_, low_, close_, vol_])
            i += 1
        self._data.extend(rows)

    def _parse_opt10075(self, trcode: str, rqname: str) -> None:
        """opt10075: unfilled orders parse.
        Row layout matches query_open_orders() expectations:
          [0]=주문시간, [1]=주문번호, [2]=종목코드, [3]=종목명,
          [4]=주문가격, [5]=매매구분, [6]=주문수량, [7]=체결수량,
          [8]=미체결수량, [9]=주문상태
        """
        _get = lambda idx, field: self._call(
            "GetCommData(QString,QString,int,QString)", trcode, rqname, idx, field,
        )
        rows: List[List] = []
        i = 0
        while True:
            order_no = str(_get(i, "주문번호")).strip()
            if not order_no:
                break
            order_time = str(_get(i, "주문시간")).strip()
            code       = str(_get(i, "종목코드")).strip()
            name       = self._decode_kiwoom_str(_get(i, "종목명"))
            price      = str(_get(i, "주문가격")).strip()
            side       = str(_get(i, "매매구분")).strip()
            qty        = str(_get(i, "주문수량")).strip()
            filled     = str(_get(i, "체결수량")).strip()
            remain     = str(_get(i, "미체결수량")).strip()
            status     = str(_get(i, "주문상태")).strip()
            rows.append([order_time, order_no, code, name, price,
                         side, qty, filled, remain, status])
            i += 1
        self._data.extend(rows)

    # -- KOSPI index close via opt20006 ----------------------------------------

    def _get_index_close(self, index_code: str, index_name: str,
                         trade_date: str = "") -> float:
        """Get index close price via opt20006 TR.
        index_code: '001' for KOSPI, '101' for KOSDAQ.
        Returns close price or 0.0 on failure.
        """
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")

        def _setup():
            self._call("SetInputValue(QString,QString)", "업종코드", index_code)
            self._call("SetInputValue(QString,QString)", "기준일자", trade_date)
            self._call("SetInputValue(QString,QString)", "수정주가구분", "1")

        try:
            rows = self._request_tr_with_retry(
                trcode="opt20006", rqname="업종일봉요청",
                days=1, setup_func=_setup)
            if rows and len(rows) > 0:
                raw_val = abs(float(rows[0][4]))
                # Kiwoom opt20006 returns index * 100 (no decimal)
                close_val = raw_val / 100.0
                logger.info("[%s] close=%.2f (raw=%d, date=%s)",
                            index_name, close_val, int(raw_val), trade_date)
                return close_val
        except Exception as e:
            logger.warning("[%s] opt20006 failed: %s", index_name, e)
        return 0.0

    def get_kospi_close(self, trade_date: str = "") -> float:
        """Get KOSPI index close price."""
        return self._get_index_close("001", "KOSPI", trade_date)

    def get_kosdaq_close(self, trade_date: str = "") -> float:
        """Get KOSDAQ index close price."""
        return self._get_index_close("101", "KOSDAQ", trade_date)

    def get_index_minute_bars(self, index_code: str = "001",
                               trade_date: str = "",
                               tick_range: int = 1) -> List[dict]:
        """Get index minute bars via opt20005 (업종분봉조회).

        Args:
            index_code: '001' = KOSPI, '101' = KOSDAQ
            trade_date: YYYYMMDD (default: today)
            tick_range: 1 = 1-minute bars

        Returns:
            List of {time, open, high, low, close, volume} dicts,
            sorted by time ascending. Empty list on failure.
        """
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")

        def _setup():
            self._call("SetInputValue(QString,QString)", "업종코드", index_code)
            self._call("SetInputValue(QString,QString)", "틱범위", str(tick_range))

        try:
            rows = self._request_tr_with_retry(
                trcode="opt20005", rqname="업종분봉조회",
                days=500, setup_func=_setup)
        except Exception as e:
            logger.warning("[INDEX_MINUTE] opt20005 failed: %s", e)
            return []

        if not rows:
            return []

        result = []
        for row in rows:
            try:
                # opt20005 output: [date_str, open, high, low, close, volume, ...]
                # date_str format: YYYYMMDDHHMMSS
                dt_str = str(row[0]).strip()
                if len(dt_str) < 12:
                    continue
                dt_date = dt_str[:8]
                # Filter to target date only
                if dt_date != trade_date:
                    continue
                hhmm = dt_str[8:10] + ":" + dt_str[10:12]

                vals = [abs(float(v)) / 100.0 for v in row[1:5]]  # OHLC /100
                vol = abs(int(float(row[5]))) if len(row) > 5 else 0

                result.append({
                    "time": hhmm,
                    "datetime": f"{dt_date[:4]}-{dt_date[4:6]}-{dt_date[6:8]} {hhmm}",
                    "open": vals[0],
                    "high": vals[1],
                    "low": vals[2],
                    "close": vals[3],
                    "volume": vol,
                })
            except (ValueError, IndexError):
                continue

        # Sort ascending by time
        result.sort(key=lambda x: x["time"])
        name = "KOSPI" if index_code == "001" else "KOSDAQ"
        logger.info("[%s_MINUTE] %d bars loaded for %s", name, len(result), trade_date)
        return result

    # -- OnReceiveMsg ---------------------------------------------------------

    def _on_msg(self, screen_no, rqname, trcode, msg):
        """
        OnReceiveMsg handler.
        Called when server rejects TR or sends error message.
        Quits loop immediately instead of waiting for 20s timeout.
        """
        msg = str(msg).strip()
        rqname_d = self._decode_kiwoom_str(rqname)
        msg_d    = self._decode_kiwoom_str(msg)
        logger.warning("[OnReceiveMsg] screen=%s rq=%s tr=%s msg=%s", screen_no, rqname_d, trcode, msg_d)

        # Order screen message handling
        if str(screen_no) == "7001":
            # [100000] = order accepted (success) -> wait for chejan fill
            if "[100000]" in msg:
                logger.info("[OnReceiveMsg] order accepted: %s", msg_d)
                return
            # Other ([800033], [RC4025] etc) = error -> immediate reject
            if self._order_result is None:
                self._order_result = {
                    "order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"order rejected: {msg_d}",
                }
                if self._order_loop.isRunning():
                    self._order_loop.quit()
            return

        # TR message matching
        trcode_clean = str(trcode).strip().lower()
        current_clean = str(self._current_trcode).strip().lower()
        logger.debug("[_on_msg] trcode='%s' current='%s' match=%s loop_running=%s",
                     trcode_clean, current_clean, trcode_clean == current_clean,
                     self._loop.isRunning())

        # [100000] = query/order success -> data coming via OnReceiveTrData
        if "[100000]" in msg:
            logger.info("[OnReceiveMsg] query success: %s (tr=%s)", msg_d, trcode)
            return

        if trcode_clean == current_clean and current_clean:
            # [571578] = no data (empty account) -> normal empty result
            if "[571578]" in msg:
                logger.info("[OnReceiveMsg] no data (empty account): %s (tr=%s)", msg_d, trcode)
                self._msg_rejected = True
                if self._loop.isRunning():
                    self._loop.quit()
                return

            # Server error -> quit immediately
            self._msg_rejected = True
            if self._loop.isRunning():
                self._loop.quit()

    # -- Timeout handlers -----------------------------------------------------

    def _on_timeout(self):
        self._timed_out = True
        self._loop.quit()

    def _on_order_timeout(self):
        """Order fill wait timeout."""
        if self._order_loop.isRunning():
            self._order_loop.quit()

    # -- Kiwoom string decode -------------------------------------------------

    @staticmethod
    def _decode_kiwoom_str(s) -> str:
        """Kiwoom COM CP949 -> Latin-1 garbled string recovery.

        Multi-fallback: latin-1->cp949, latin-1->euc-kr, original.
        Returns original on decode failure (caller logs).
        """
        s = str(s).strip()
        if not s or s.isascii():
            return s
        # 1st: latin-1 -> cp949 (most common COM encoding corruption)
        try:
            return s.encode("latin-1").decode("cp949")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        # 2nd: latin-1 -> euc-kr (older Kiwoom versions)
        try:
            return s.encode("latin-1").decode("euc-kr")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        # 3rd: already correct or unrecoverable -> return original
        return s

    # -- TR request with retry (opw00018 only) --------------------------------

    def _request_tr_with_retry(
        self,
        trcode: str,
        rqname: str,
        days: int,
        setup_func,
    ) -> List[List]:
        """
        TR request + timeout/retry handling.

        - ProviderDeadError -> immediate abort (no retry)
        - Up to TR_MAX_RETRY retries
        - TrTimeoutError on exhaustion
        """
        if not self._alive:
            raise ProviderDeadError(
                f"provider dead — {rqname}({trcode}) TR request blocked"
            )

        all_rows:  List[List] = []
        try_count: int        = 0

        screen_no = SCREEN_MAP.get(trcode, "9000")

        while try_count < TR_MAX_RETRY:
            try_count            += 1
            self._data            = []
            self._prev_next       = "0"
            self._timed_out       = False
            self._msg_rejected    = False
            self._current_rqname  = rqname
            self._current_trcode  = trcode

            time.sleep(TR_DELAY)

            try:
                setup_func()
            except ProviderDeadError:
                raise

            try:
                ret = self._call(
                    "CommRqData(QString,QString,int,QString)",
                    rqname, trcode, 0, screen_no,
                    context=f"{rqname}/{trcode}/screen={screen_no}",
                )
            except ProviderDeadError:
                raise
            logger.debug("[CommRqData] %s(%s) screen=%s ret=%s", rqname, trcode, screen_no, ret)

            if ret == -200:
                logger.warning(
                    "[CommRqData -200] rate limit — 1s wait then retry (%d/%d)",
                    try_count, TR_MAX_RETRY,
                )
                time.sleep(1.0)
                if try_count >= TR_MAX_RETRY:
                    raise RateLimitError(
                        f"{rqname}/{trcode} rate limit -200 (consecutive {try_count}x)")
                continue

            if ret != 0:
                logger.error(
                    "[CommRqData fail] %s(%s) ret=%s — request rejected, stopping retry.",
                    rqname, trcode, ret,
                )
                break

            if not self._msg_rejected:
                self._timer.start(int(TR_TIMEOUT_SEC * 1000))
                self._loop.exec_()
                self._timer.stop()

            if self._msg_rejected:
                logger.info("[TR server reject] %s(%s) — empty result (no retry needed)", rqname, trcode)
                break

            if self._timed_out:
                logger.warning(
                    "[TR timeout] %s(%s) - %ds no response (%d/%d)",
                    rqname, trcode, TR_TIMEOUT_SEC, try_count, TR_MAX_RETRY,
                )
                time.sleep(TR_DELAY * try_count)
                self._consecutive_timeout += 1
                if self._consecutive_timeout >= TR_MAX_CONSECUTIVE:
                    raise TrTimeoutError(
                        f"[consecutive timeout {TR_MAX_CONSECUTIVE}x] Kiwoom API unresponsive"
                    )
            else:
                self._consecutive_timeout = 0
                all_rows.extend(self._data)
                if len(all_rows) >= days:
                    break
                if self._prev_next != "2":
                    break

        if not all_rows:
            msg = f"[TR final fail] {rqname}({trcode}) - {TR_MAX_RETRY}x retry all no response."
            logger.error(msg)
            with open(TR_ERROR_LOG, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now()}] {msg}\n"
                    f"{traceback.format_exc()}\n"
                    f"{'-' * 80}\n"
                )

            # opw00018: [571578] empty account -> normal
            if trcode == "opw00018" and self._msg_rejected:
                logger.info(
                    "[Gen4KiwoomProvider] %s(%s) empty account (no holdings) -> empty result",
                    rqname, trcode,
                )
                return []

            # opt10075: 0 open orders is a valid empty result (not an error)
            if trcode == "opt10075" and not self._timed_out:
                logger.info(
                    "[Gen4KiwoomProvider] %s(%s) no open orders -> empty result",
                    rqname, trcode,
                )
                return []

            raise TrTimeoutError(msg)

        return all_rows[:days]

    # -- Helpers --------------------------------------------------------------

    @staticmethod
    def _to_int(val) -> int:
        try:
            return int(str(val).replace(",", "").replace(" ", "") or 0)
        except (ValueError, TypeError):
            return 0
