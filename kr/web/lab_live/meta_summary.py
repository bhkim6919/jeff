"""
meta_summary.py -- Gen5 Meta Layer: UI용 summary 생성
=====================================================
DB 데이터 → UI 표시용 적합도/태그 생성.
설명 텍스트는 DB에 저장하지 않음 — 매번 데이터에서 계산.
Observer-only: 추천/비중조절 금지.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from web.lab_live import meta_db

logger = logging.getLogger("lab.meta")

# ── Threshold Config (향후 config 파일 분리 가능) ──────────
THRESHOLDS = {
    "kospi_up": 0.005,
    "kospi_down": -0.005,
    "kospi_crash": -0.01,
    "small_vs_large_high": 0.01,
    "small_vs_large_low": -0.01,
    "adv_ratio_strong": 0.6,
    "adv_ratio_weak": 0.4,
    "adv_ratio_mid": 0.5,
    "breakout_high": 0.05,
    "breakout_mid": 0.03,
    "sector_disp_high": 0.02,
    "sector_disp_low": 0.01,
}

# ── Strategy Fit Rules ───────────────────────────────────────
# (feature, operator, threshold_key_or_value, weight, label)
RULES: Dict[str, list] = {
    "breakout_trend": [
        ("breakout_ratio", ">", "breakout_high", +2, "breakout 종목 다수"),
        ("small_vs_large", ">", "small_vs_large_high", +1, "소형주 우위"),
        ("adv_ratio", "<", "adv_ratio_weak", -1, "ADR 약함"),
    ],
    "liquidity_signal": [
        ("breakout_ratio", ">", "breakout_mid", +1, "돌파 활발"),
        ("small_vs_large", ">", "small_vs_large_high", +1, "소형주 우위"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "상승 종목 다수"),
    ],
    "mean_reversion": [
        ("kospi_return", "<", "kospi_down", +2, "시장 하락 → 반등 기회"),
        ("adv_ratio", "<", "adv_ratio_weak", +1, "과매도 종목 증가"),
        ("breakout_ratio", ">", "breakout_high", -1, "추세장 불리"),
    ],
    "momentum_base": [
        ("kospi_return", ">", "kospi_up", +1, "시장 상승"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "상승 종목 다수"),
        ("small_vs_large", "<", "small_vs_large_low", +1, "대형주 우위"),
    ],
    "lowvol_momentum": [
        ("kospi_return", ">", "kospi_up", +1, "시장 상승"),
        ("sector_dispersion", "<", "sector_disp_high", +1, "균등 장세"),
        ("small_vs_large", "<", "small_vs_large_low", +1, "대형주 우위"),
    ],
    "quality_factor": [
        ("adv_ratio", ">", "adv_ratio_mid", +1, "상승 종목 다수"),
        ("sector_dispersion", "<", "sector_disp_high", +1, "균등 장세"),
        ("kospi_return", "<", "kospi_crash", -1, "급락장 불리"),
    ],
    "hybrid_qscore": [
        ("adv_ratio", ">", "adv_ratio_mid", +1, "상승 종목 다수"),
        ("breakout_ratio", ">", "breakout_mid", +1, "돌파 활발"),
        ("sector_dispersion", ">", "sector_disp_high", +1, "섹터 차별화"),
    ],
    "sector_rotation": [
        ("sector_dispersion", ">", "sector_disp_high", +2, "섹터 순환 뚜렷"),
        ("adv_ratio", ">", "adv_ratio_mid", +1, "상승 종목 다수"),
        ("sector_dispersion", "<", "sector_disp_low", -2, "섹터 균등 → 불리"),
    ],
    "vol_regime": [
        ("regime_label", "==", "BEAR", +2, "BEAR 레짐 방어 유리"),
        ("regime_label", "==", "BULL", +1, "BULL 레짐 순응"),
        ("kospi_return", "<", "kospi_crash", +1, "하락장 방어"),
    ],
}


def build_daily_summary(trade_date: str) -> Optional[dict]:
    """DB에서 읽어 UI용 summary 생성."""
    mc = meta_db.get_market_context(trade_date)
    if not mc:
        return None

    sd = meta_db.get_strategy_daily(trade_date)

    strategy_fit = {}
    for row in sd:
        strategy_fit[row["strategy"]] = _score_fit(row["strategy"], mc)

    data_days = _count_data_days()

    return {
        "trade_date": trade_date,
        "market_tags": _market_tags(mc),
        "market_data": {
            "kospi_return": mc.get("kospi_return"),
            "adv_ratio": mc.get("adv_ratio"),
            "small_vs_large": mc.get("small_vs_large"),
            "breakout_ratio": mc.get("breakout_ratio"),
            "sector_dispersion": mc.get("sector_dispersion"),
            "regime_label": mc.get("regime_label"),
        },
        "strategy_fit": strategy_fit,
        "confidence": _confidence_level(data_days),
        "data_days": data_days,
    }


# ── Market Tags ──────────────────────────────────────────────

def _market_tags(mc: dict) -> List[str]:
    """3~5개 시장 상태 키워드."""
    tags = []
    T = THRESHOLDS

    kr = mc.get("kospi_return")
    if kr is not None:
        if kr > T["kospi_up"]:
            tags.append("시장 상승")
        elif kr < T["kospi_down"]:
            tags.append("시장 하락")
        else:
            tags.append("시장 횡보")

    svl = mc.get("small_vs_large")
    if svl is not None:
        if svl > T["small_vs_large_high"]:
            tags.append("소형주 강세")
        elif svl < T["small_vs_large_low"]:
            tags.append("대형주 강세")

    adv = mc.get("adv_ratio")
    if adv is not None:
        if adv > T["adv_ratio_strong"]:
            tags.append("ADR 강함")
        elif adv < T["adv_ratio_weak"]:
            tags.append("ADR 약함")

    br = mc.get("breakout_ratio")
    if br is not None and br > T["breakout_high"]:
        tags.append("breakout 활발")

    sd = mc.get("sector_dispersion")
    if sd is not None and sd > T["sector_disp_high"]:
        tags.append("섹터 집중")

    return tags


# ── Strategy Fit Scoring ─────────────────────────────────────

def _score_fit(strategy: str, mc: dict) -> dict:
    """Rule 기반 적합도 계산."""
    rules = RULES.get(strategy, [])
    score = 0
    reasons = []

    for feat, op, threshold_key, weight, label in rules:
        val = mc.get(feat)
        if val is None:
            continue

        # Resolve threshold (config key or direct value)
        if isinstance(threshold_key, str):
            threshold = THRESHOLDS.get(threshold_key, threshold_key)
        else:
            threshold = threshold_key

        hit = False
        if op == ">" and isinstance(val, (int, float)) and val > threshold:
            hit = True
        elif op == "<" and isinstance(val, (int, float)) and val < threshold:
            hit = True
        elif op == "==" and val == threshold:
            hit = True

        if hit:
            score += weight
            sign = "+" if weight > 0 else "-"
            reasons.append({"sign": sign, "text": label})

    level = "HIGH" if score >= 2 else "LOW" if score < 0 else "MID"
    return {
        "score": level,
        "score_value": score,
        "reasons": reasons,
    }


# ── Confidence ───────────────────────────────────────────────

def _confidence_level(data_days: int) -> str:
    """데이터 축적량 기반 신뢰도."""
    if data_days >= 120:
        return "HIGH"
    if data_days >= 60:
        return "MID"
    return "LOW"


def _count_data_days() -> int:
    """market_context 테이블의 총 row 수."""
    conn = meta_db._conn()
    try:
        row = conn.execute("SELECT COUNT(*) FROM market_context").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()
