"""Read-only accounting summary computation.

Computes a snapshot of accounting state from:
  - the configured initial capital
  - the cashflow ledger contents

This module is **read-only**: it never modifies the ledger, the capital
config, or any equity / DD / dashboard state. It returns a dataclass
that downstream consumers (CF2 return engine, CF3 dashboard) can use.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .ledger import CashflowEvent, CashflowLedger, EventType


@dataclass(frozen=True)
class AccountingSummary:
    """Snapshot of accounting state. Immutable."""
    initial_capital: int
    total_deposits: int
    total_withdrawals: int
    total_dividends: int
    total_interest: int
    total_tax_refund: int
    total_manual_adjustment: int  # signed
    net_external_flow: int        # signed sum of all events
    invested_capital: int         # initial_capital + net_external_flow
    event_count: int
    last_event_date: Optional[str]


def compute_summary(
    ledger: CashflowLedger,
    initial_capital: int,
) -> AccountingSummary:
    """Pure function. Reads the ledger and returns a summary dataclass.

    Does NOT modify the ledger, the capital config, or any equity state.
    """
    if not isinstance(initial_capital, int) or initial_capital <= 0:
        raise ValueError(
            f"initial_capital must be positive int, got {initial_capital!r}"
        )

    events: list[CashflowEvent] = ledger.load()

    totals = {
        EventType.DEPOSIT: 0,
        EventType.WITHDRAWAL: 0,
        EventType.DIVIDEND: 0,
        EventType.INTEREST: 0,
        EventType.TAX_REFUND: 0,
        EventType.MANUAL_ADJUSTMENT: 0,
    }
    net_signed = 0
    last_date: Optional[str] = None

    for ev in events:
        if ev.type == EventType.MANUAL_ADJUSTMENT:
            totals[EventType.MANUAL_ADJUSTMENT] += ev.amount  # signed as-passed
        else:
            totals[ev.type] += abs(ev.amount)
        net_signed += ev.signed_amount()
        if last_date is None or ev.event_date > last_date:
            last_date = ev.event_date

    return AccountingSummary(
        initial_capital=initial_capital,
        total_deposits=totals[EventType.DEPOSIT],
        total_withdrawals=totals[EventType.WITHDRAWAL],
        total_dividends=totals[EventType.DIVIDEND],
        total_interest=totals[EventType.INTEREST],
        total_tax_refund=totals[EventType.TAX_REFUND],
        total_manual_adjustment=totals[EventType.MANUAL_ADJUSTMENT],
        net_external_flow=net_signed,
        invested_capital=initial_capital + net_signed,
        event_count=len(events),
        last_event_date=last_date,
    )
