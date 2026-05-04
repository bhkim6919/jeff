"""
finance/_deprecated_capital_events.py — QUARANTINED 2026-05-04 (CF0)
=====================================================================

⚠️  DO NOT IMPORT FROM ACTIVE RUNTIME CODE  ⚠️

Status: QUARANTINED. New accounting code MUST live under `kr/accounting/`
(see Accounting Correction Sprint, PR-CF1+).

Quarantine reason
-----------------
The original module (introduced 2026-04-20 via commit 2b0ed97c) defined
`adjust_equity(raw_equity, ...)` which computed:

    adjusted = raw_equity - cumulative_net_deposits_in_window

Per Jeff doctrine 2026-05-04 (Accounting Correction Sprint), this
"raw minus cashflow" pattern is a footgun:
  - replay divergence (recompute from ledger drifts from stored adj)
  - dual truth (raw and adj coexisting → cache mismatch)
  - DD peak continuity broken (deposit moves peak)
  - intraday cashflow boundary undefined
  - PG/CSV drift on rebuild

Q-TRON's accounting truth doctrine instead:
  - raw equity = immutable broker truth
  - returns/DD = computed cashflow-aware (Modified Dietz, 1차)
  - never store equity_adj as a separate time series

Audit trail preserved
---------------------
- `record_event` / `list_events` / `cumulative_by_date` retained for
  historical intent + migration continuity. PG table `capital_events`
  (12 cols, currently 0 rows) is also retained — see migration v014.
- `adjust_equity()` REMOVED entirely. Any future need to query a
  cashflow-adjusted balance must go through the (forthcoming) accounting
  module's Modified Dietz return engine, not by subtracting from raw.

Web endpoints under `/api/capital/*` return HTTP 410 Gone with a pointer
to the new accounting module (see `kr/web/app.py`).

Original use case (preserved for audit context, 2026-04-20):
  Jeff plans to deposit additional cash to Kiwoom live account before
  5월 초 rebalance. Without tracking, daily return and cumulative
  return calculations would misinterpret the deposit as "+X% gain".
  → Resolved by Accounting Correction Sprint (PR-CF1+), NOT by this
  module's adjust_equity.

API (retained, but DO NOT call from active code)
-------------------------------------------------
  - record_event()      → insert one event (test/audit only)
  - list_events()       → list events filtered by mode/market/date range
  - cumulative_by_date() → {date: cumulative_net_deposits}
  - adjust_equity()     → REMOVED 2026-05-04 (CF0)
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from shared.db.pg_base import connection

logger = logging.getLogger("gen4.finance.capital")

VALID_MODES = {"live", "paper", "paper_forward", "backtest"}
VALID_MARKETS = {"KR", "US"}
VALID_EVENT_TYPES = {
    "deposit", "withdraw", "dividend", "interest", "fee", "adjustment",
}

# Sign convention for net cash flow from account owner into account:
# deposit     → +amount  (external cash comes in)
# withdraw    → -amount
# dividend    → +amount  (stock pays you — still external to trading PnL)
# interest    → +amount
# fee         → -amount  (broker fee / admin)
# adjustment  → ±amount  (note explains)
_DIRECTION = {
    "deposit":     +1,
    "withdraw":    -1,
    "dividend":    +1,
    "interest":    +1,
    "fee":         -1,
    "adjustment":   0,  # sign follows amount as-passed
}


def record_event(
    *,
    mode: str,
    market: str,
    event_date: str | date,
    event_type: str,
    amount: float,
    currency: str = "KRW",
    note: str = "",
    recorded_by: str = "jeff",
    source: str = "manual",
    external_ref: Optional[str] = None,
) -> int:
    """Insert one capital event. Returns inserted row id.

    amount should be positive for deposit/dividend/interest/adjustment(+).
    For withdraw/fee/adjustment(-), pass positive amount; the direction
    convention is encoded by event_type. cumulative_by_date() computes
    signed net correctly.
    """
    if mode not in VALID_MODES:
        raise ValueError(f"invalid mode: {mode}")
    if market not in VALID_MARKETS:
        raise ValueError(f"invalid market: {market}")
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"invalid event_type: {event_type}")
    if amount < 0 and event_type != "adjustment":
        raise ValueError(
            f"amount must be >= 0 for {event_type} (sign inferred from type)")

    ed = _to_date(event_date)
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO capital_events
              (mode, market, event_date, event_type, amount, currency,
               note, recorded_by, source, external_ref)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (mode, market, ed, event_type, amount, currency,
              note, recorded_by, source, external_ref))
        (row_id,) = cur.fetchone()
        conn.commit()
        cur.close()
    logger.info(
        f"[CAPITAL_EVENT] id={row_id} {mode}/{market} {event_date} "
        f"{event_type} {amount:+,.0f} {currency} note='{note[:50]}'"
    )
    return row_id


def list_events(
    *,
    mode: Optional[str] = None,
    market: Optional[str] = None,
    date_from: Optional[str | date] = None,
    date_to: Optional[str | date] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """List events with optional filters. Newest first by event_date desc."""
    where: list[str] = []
    params: list = []
    if mode:
        where.append("mode = %s")
        params.append(mode)
    if market:
        where.append("market = %s")
        params.append(market)
    if date_from:
        where.append("event_date >= %s")
        params.append(_to_date(date_from))
    if date_to:
        where.append("event_date <= %s")
        params.append(_to_date(date_to))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT id, mode, market, event_date, event_type, amount::float,
               currency, note, recorded_by, recorded_at, source, external_ref
        FROM capital_events
        {where_sql}
        ORDER BY event_date DESC, id DESC
        LIMIT %s
    """
    params.append(limit)

    out: list[dict] = []
    with connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            if isinstance(d.get("event_date"), date):
                d["event_date"] = d["event_date"].strftime("%Y-%m-%d")
            if isinstance(d.get("recorded_at"), datetime):
                d["recorded_at"] = d["recorded_at"].isoformat(timespec="seconds")
            # signed_amount for convenience
            d["signed_amount"] = _signed(d["event_type"], float(d["amount"]))
            out.append(d)
        cur.close()
    return out


def cumulative_by_date(
    *,
    mode: str,
    market: str,
    date_from: str | date,
    date_to: str | date,
) -> Dict[str, float]:
    """
    Returns {date_string: cumulative_net_capital_flow_until_that_date}.
    Includes dates without events (carries last cumulative forward via
    caller — this function only returns dates that had events).

    Net flow: sum of signed amounts per _DIRECTION convention.
    """
    df = _to_date(date_from)
    dt = _to_date(date_to)
    with connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT event_date, event_type, amount::float
            FROM capital_events
            WHERE mode = %s AND market = %s AND event_date BETWEEN %s AND %s
            ORDER BY event_date, id
        """, (mode, market, df, dt))
        rows = cur.fetchall()
        cur.close()

    daily_net: Dict[str, float] = {}
    for ed, et, amt in rows:
        key = ed.strftime("%Y-%m-%d") if isinstance(ed, date) else str(ed)
        daily_net[key] = daily_net.get(key, 0.0) + _signed(et, float(amt))

    # Cumulative
    cumulative: Dict[str, float] = {}
    running = 0.0
    for d_str in sorted(daily_net.keys()):
        running += daily_net[d_str]
        cumulative[d_str] = round(running, 2)
    return cumulative


# adjust_equity() removed 2026-05-04 (CF0 quarantine).
# The "raw_equity - cumulative_cashflow" pattern is forbidden in active
# Q-TRON code. See module docstring for rationale; see kr/accounting/
# (forthcoming PR-CF1+) for the cashflow-aware return engine that
# replaces this approach.


# ─── helpers ──────────────────────────────────────────────────────

def _signed(event_type: str, amount: float) -> float:
    direction = _DIRECTION.get(event_type, 0)
    if direction == 0:
        return amount  # adjustment — sign as passed
    return direction * abs(amount)


def _to_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def is_db_available() -> bool:
    try:
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM capital_events LIMIT 1")
            cur.fetchone()
            cur.close()
        return True
    except Exception:
        return False
