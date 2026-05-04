"""Append-only immutable cashflow ledger.

Storage: JSONL (one event per line) at kr/data/accounting/cashflow_ledger.jsonl.

Append-only contract:
  - Events are written via fsync'd append. No rewrites, deletes, or edits.
  - event_id is unique. Re-appending the same event_id is a no-op
    (idempotent), so callers can retry safely.
  - Load = read all lines. Replay on the same file always yields the
    same list of events in append order.

Validation is fail-closed:
  - append() raises ValueError on malformed input.
  - load() raises ValueError on a malformed line. The ledger refuses
    to operate on partially-corrupt data; the caller decides recovery.

Sign convention (cash flow direction relative to the account):
  - deposit / dividend / interest / tax_refund  → cash IN  (positive)
  - withdrawal                                  → cash OUT (negative)
  - manual_adjustment                           → caller signs `amount`
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional


DEFAULT_LEDGER_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "accounting" / "cashflow_ledger.jsonl"
)


class EventType:
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    TAX_REFUND = "tax_refund"
    MANUAL_ADJUSTMENT = "manual_adjustment"

    ALL = frozenset({
        DEPOSIT, WITHDRAWAL, DIVIDEND, INTEREST, TAX_REFUND, MANUAL_ADJUSTMENT,
    })


# Sign of the contribution to net_external_flow.
# manual_adjustment = 0 means the amount is used as-passed (caller signs).
SIGN_CONVENTION: dict[str, int] = {
    EventType.DEPOSIT: +1,
    EventType.WITHDRAWAL: -1,
    EventType.DIVIDEND: +1,
    EventType.INTEREST: +1,
    EventType.TAX_REFUND: +1,
    EventType.MANUAL_ADJUSTMENT: 0,
}


_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class CashflowEvent:
    """One cashflow event. Immutable after construction."""
    event_date: str           # YYYY-MM-DD
    type: str                 # one of EventType.ALL
    amount: int               # KRW (integer; signed only for manual_adjustment)
    source: str = ""          # e.g., "kakao_bank", "kiwoom_div"
    note: str = ""
    event_id: str = ""        # auto-derived if empty
    recorded_at: str = ""     # ISO8601 UTC; auto-set on creation if empty
    recorded_by: str = "system"
    currency: str = "KRW"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "CashflowEvent":
        required = {"event_date", "type", "amount"}
        missing = required - set(data)
        if missing:
            raise ValueError(f"event missing required fields: {sorted(missing)}")
        return cls(
            event_date=data["event_date"],
            type=data["type"],
            amount=data["amount"],
            source=data.get("source", ""),
            note=data.get("note", ""),
            event_id=data.get("event_id", ""),
            recorded_at=data.get("recorded_at", ""),
            recorded_by=data.get("recorded_by", "system"),
            currency=data.get("currency", "KRW"),
        )

    def signed_amount(self) -> int:
        """Contribution to net_external_flow (signed)."""
        if self.type not in SIGN_CONVENTION:
            raise ValueError(f"unknown type for signed_amount: {self.type!r}")
        sign = SIGN_CONVENTION[self.type]
        if sign == 0:
            return int(self.amount)  # manual_adjustment: caller signs
        return sign * abs(int(self.amount))


def _validate_event(event: CashflowEvent) -> None:
    """Fail-closed validation. Raises ValueError on any defect."""
    # event_date format
    if not isinstance(event.event_date, str) or not _DATE_PATTERN.match(event.event_date):
        raise ValueError(f"event_date must be YYYY-MM-DD, got: {event.event_date!r}")
    # parse to ensure real date
    try:
        datetime.strptime(event.event_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"event_date not a real date: {event.event_date!r} ({e})") from e

    # type
    if event.type not in EventType.ALL:
        raise ValueError(
            f"unknown event type: {event.type!r}. "
            f"Expected one of: {sorted(EventType.ALL)}"
        )

    # amount
    if not isinstance(event.amount, int) or isinstance(event.amount, bool):
        raise ValueError(f"amount must be int (KRW), got: {type(event.amount).__name__}")
    if event.type == EventType.MANUAL_ADJUSTMENT:
        # signed allowed; only zero rejected
        if event.amount == 0:
            raise ValueError("manual_adjustment amount must be non-zero")
    else:
        if event.amount <= 0:
            raise ValueError(
                f"amount must be positive for {event.type} (sign inferred from type); "
                f"got {event.amount}. Use manual_adjustment for signed entries."
            )

    # currency (CF1 KRW only)
    if event.currency != "KRW":
        raise ValueError(f"CF1 supports KRW only; got currency={event.currency!r}")


def _derive_event_id(event: CashflowEvent) -> str:
    canonical = f"{event.event_date}|{event.type}|{event.amount}|{event.source}|{event.note}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


class CashflowLedger:
    """Append-only JSONL ledger.

    Thread-safety: a per-instance lock serializes append() calls within
    a single process. Cross-process safety relies on append-mode +
    fsync per write; concurrent appenders MAY interleave writes at
    line boundaries (each line is one full JSON object), so the file
    remains parseable. Strict cross-process serialization is out of
    CF1 scope (manual events are entered serially in practice).
    """

    def __init__(self, path: Optional[Path] = None):
        self.path: Path = Path(path) if path is not None else DEFAULT_LEDGER_PATH
        self._lock = threading.Lock()

    def append(self, event: CashflowEvent) -> bool:
        """Append event. Returns True if newly written, False if duplicate (no-op).

        Raises ValueError on validation failure. Thread-safe within process.
        """
        _validate_event(event)
        # Normalize: derive event_id if absent, set recorded_at if absent
        event_id = event.event_id or _derive_event_id(event)
        recorded_at = event.recorded_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
        normalized = CashflowEvent(
            event_date=event.event_date,
            type=event.type,
            amount=int(event.amount),
            source=event.source,
            note=event.note,
            event_id=event_id,
            recorded_at=recorded_at,
            recorded_by=event.recorded_by,
            currency=event.currency,
        )

        with self._lock:
            existing_ids = {e.event_id for e in self._load_unsafe()}
            if normalized.event_id in existing_ids:
                return False  # idempotent no-op
            self.path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(normalized.to_dict(), ensure_ascii=False, sort_keys=True)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())
        return True

    def load(self) -> list[CashflowEvent]:
        """Read all events in append order. Raises ValueError on malformed line."""
        with self._lock:
            return self._load_unsafe()

    def _load_unsafe(self) -> list[CashflowEvent]:
        if not self.path.exists():
            return []
        events: list[CashflowEvent] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line_no, raw in enumerate(f, start=1):
                stripped = raw.strip()
                if not stripped:
                    continue
                try:
                    data = json.loads(stripped)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"{self.path}:{line_no}: invalid JSON: {e}"
                    ) from e
                if not isinstance(data, dict):
                    raise ValueError(
                        f"{self.path}:{line_no}: line is not a JSON object"
                    )
                try:
                    event = CashflowEvent.from_dict(data)
                    _validate_event(event)
                except (KeyError, ValueError) as e:
                    raise ValueError(
                        f"{self.path}:{line_no}: invalid event: {e}"
                    ) from e
                events.append(event)
        return events

    def __iter__(self) -> Iterable[CashflowEvent]:
        return iter(self.load())
