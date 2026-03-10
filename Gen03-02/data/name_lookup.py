"""
data/name_lookup.py
종목코드 → 한국어 종목명 조회 유틸리티.
data/stock_names.json 을 한 번만 로드하고 캐시.
"""

import json
from pathlib import Path
from functools import lru_cache

_NAMES = None  # type: ignore

def _load() -> dict:
    global _NAMES
    if _NAMES is None:
        p = Path(__file__).parent / "stock_names.json"
        if p.exists():
            _NAMES = json.loads(p.read_text(encoding="utf-8"))
        else:
            _NAMES = {}
    return _NAMES

def get_name(code: str, fallback=None) -> str:
    """종목코드 → 종목명. 없으면 fallback(기본: code 그대로)."""
    return _load().get(str(code), fallback if fallback is not None else code)

def fmt(code: str, width=12) -> str:
    """'종목명(코드)' 형식으로 포맷. 콘솔 출력용."""
    name = get_name(code)
    label = f"{name}({code})" if name != code else code
    return label.ljust(width)
