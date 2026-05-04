"""Capital state configuration.

The initial capital is a manual configuration value. It is the strategy's
starting equity reference and MUST NOT be auto-derived from broker equity
queries — those reflect current state, not the inception baseline.

File: kr/data/accounting/capital_state.json
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

DEFAULT_CAPITAL_STATE_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "accounting" / "capital_state.json"
)


@dataclass(frozen=True)
class CapitalConfig:
    initial_capital: int
    currency: str
    strategy_start_date: Optional[str]

    @classmethod
    def from_dict(cls, data: dict) -> "CapitalConfig":
        if "initial_capital" not in data:
            raise ValueError("capital_state.json missing required field: initial_capital")
        try:
            initial_capital = int(data["initial_capital"])
        except (TypeError, ValueError) as e:
            raise ValueError(f"initial_capital must be integer KRW: {e!r}") from e
        if initial_capital <= 0:
            raise ValueError(f"initial_capital must be positive: got {initial_capital}")
        currency = data.get("currency", "KRW")
        if not isinstance(currency, str) or not currency:
            raise ValueError(f"currency must be non-empty string, got: {currency!r}")
        # CF1 KR-live-only scope: KRW only
        if currency != "KRW":
            raise ValueError(
                f"CF1 supports KRW only; got currency={currency!r}. "
                f"Multi-currency support is out of CF1 scope."
            )
        ssd = data.get("strategy_start_date")
        if ssd is not None and not isinstance(ssd, str):
            raise ValueError(f"strategy_start_date must be str or null, got: {type(ssd)}")
        return cls(
            initial_capital=initial_capital,
            currency=currency,
            strategy_start_date=ssd,
        )


def load_capital_state(path: Optional[Path] = None) -> CapitalConfig:
    """Load capital state from JSON file. Fail-closed on missing/malformed."""
    if path is None:
        path = DEFAULT_CAPITAL_STATE_PATH
    if not Path(path).exists():
        raise FileNotFoundError(
            f"capital_state.json not found at {path}. "
            f"This file is the strategy inception baseline and must be present. "
            f"It is NOT auto-generated from broker equity."
        )
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"capital_state.json invalid JSON at {path}: {e}") from e
    if not isinstance(data, dict):
        raise ValueError(f"capital_state.json must be a JSON object, got {type(data)}")
    return CapitalConfig.from_dict(data)


def get_initial_capital(path: Optional[Path] = None) -> int:
    """Return initial capital from config. NEVER auto-overwritten by broker.

    The number returned is the manually-configured strategy inception
    capital. Callers that need current equity must query a different
    source (broker, equity_history) — this function is intentionally
    static.
    """
    return load_capital_state(path).initial_capital
