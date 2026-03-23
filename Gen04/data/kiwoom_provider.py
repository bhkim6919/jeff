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
        self._completed_order_nos: set = set()  # track finished order_nos

        self._k.OnReceiveChejanData.connect(self._on_chejan_data)

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
                    "qty": qty, "avg_price": avg_price,
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

        if self._order_result is None:
            # -- TIMEOUT_UNCERTAIN — fill unconfirmed --
            self._order_state["status"] = "TIMEOUT_UNCERTAIN"
            ghost = self._order_state.copy()
            self._ghost_orders.append(ghost)
            if self._order_state["order_no"]:
                self._completed_order_nos.add(self._order_state["order_no"])

            logger.critical(
                "[TIMEOUT_UNCERTAIN] %s %s %d qty — fill unconfirmed (timeout %ds). "
                "May have filled on broker! Check HTS. order_no=%s",
                side, code, quantity, ORDER_TIMEOUT_SEC,
                self._order_state["order_no"],
            )
            return {"order_no": self._order_state["order_no"],
                    "exec_price": 0.0, "exec_qty": 0,
                    "error": f"TIMEOUT_UNCERTAIN — {ORDER_TIMEOUT_SEC}s fill unconfirmed"}

        # -- Normal fill --
        self._order_state["status"]         = "FILLED"
        self._order_state["order_no"]       = self._order_result["order_no"]
        self._order_state["filled_qty"]     = self._order_result["exec_qty"]
        self._order_state["avg_fill_price"] = self._order_result["exec_price"]

        # Track completed order_no to prevent stale chejan contamination
        if self._order_result["order_no"]:
            self._completed_order_nos.add(self._order_result["order_no"])

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

        st = self._order_state

        # -- 1. Active order match (strict order_no after capture) --
        if st["status"] in ("REQUESTED", "ACCEPTED", "PARTIAL"):
            if st["order_no"]:
                # RULE 2: order_no captured → strict match only
                if order_no == st["order_no"]:
                    self._process_chejan_fill(code, order_no, exec_qty, exec_price, order_status)
                # else: silently ignore (different order)
                return
            else:
                # RULE 1: order_no not yet captured → match by ticker code
                # But reject if order_no belongs to a completed/timed-out order
                if code == st["code"]:
                    if order_no and order_no in self._completed_order_nos:
                        return  # stale chejan from previous order
                    if order_no:
                        st["order_no"] = order_no
                        logger.info("[Chejan] order_no captured: %s (code=%s)", order_no, code)
                    self._process_chejan_fill(code, order_no, exec_qty, exec_price, order_status)
                # else: wrong ticker, silently ignore
                return

        # -- 2. FILLED → silently ignore post-fill events --
        if st["status"] == "FILLED":
            return

        # -- 3. Ghost order match (delayed fill after timeout) --
        for ghost in self._ghost_orders:
            if ghost["status"] not in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN"):
                continue
            # Strict: order_no match only (no ticker fallback for ghosts)
            if ghost.get("order_no") and order_no and order_no == ghost["order_no"]:
                if exec_qty > 0 and exec_price > 0:
                    ghost["status"]         = "GHOST_FILLED"
                    ghost["filled_qty"]     = exec_qty
                    ghost["avg_fill_price"] = exec_price
                    logger.critical(
                        "[GHOST FILL] %s %s %d qty @ %.0f (order_no=%s) — "
                        "delayed fill after timeout! Check HTS!",
                        ghost["side"], code, exec_qty, exec_price, order_no,
                    )
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

            # -- Overfill guard --
            remaining = max(0, requested - prev_qty)
            if remaining <= 0:
                logger.warning(
                    "[OVERFILL IGNORED] %s %s already filled %d/%d, ignoring +%d (order_no=%s)",
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
                    "[FILL] %s %s %d qty @ %.0f (order_no=%s)",
                    st["side"], code, filled, exec_price, order_no,
                )
            else:
                self._order_result["exec_qty"]  = filled
                self._order_result["exec_price"] = avg_price
                logger.info(
                    "[PARTIAL] %s %s filled=%d/%d remain=%d avg=%.0f +%d@%.0f (order_no=%s)",
                    st["side"], code, filled, requested, remain,
                    avg_price, usable_qty, exec_price, order_no,
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

    def get_ghost_orders(self) -> List[Dict]:
        """Unconfirmed orders after timeout. For RuntimeEngine/EOD warnings."""
        return [g for g in self._ghost_orders
                if g["status"] in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN", "GHOST_FILLED")]

    def clear_ghost_orders(self) -> None:
        """Clear ghost list after EOD processing."""
        self._ghost_orders.clear()

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

            raise TrTimeoutError(msg)

        return all_rows[:days]

    # -- Helpers --------------------------------------------------------------

    @staticmethod
    def _to_int(val) -> int:
        try:
            return int(str(val).replace(",", "").replace(" ", "") or 0)
        except (ValueError, TypeError):
            return 0
