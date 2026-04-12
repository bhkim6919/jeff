# -*- coding: utf-8 -*-
"""
actual.py — T일 실제 레짐 산출 (제프 설계 v2)
================================================
입력: KOSPI/KOSDAQ 수익률 + breadth + 수급 + 스트레스
점수: ret 중심 + 보조 0.5배 + stress 감점
장중 보정: DD/VI 강제 cap/floor
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any, Dict

from regime.calendar import is_after_market_close, previous_trading_day
from regime.models import RegimeLevel, REGIME_LABELS
from regime.storage import save_actual

logger = logging.getLogger("gen4.regime.actual")


# ── Score Functions ───────────────────────────────────────────

def _ret_score(kospi_change: float) -> int:
    """KOSPI 등락률 → -2 ~ +2"""
    if kospi_change > 0.01:
        return 2
    elif kospi_change > 0.003:
        return 1
    elif kospi_change >= -0.003:
        return 0
    elif kospi_change >= -0.01:
        return -1
    else:
        return -2


def _breadth_score(rising: int, falling: int) -> int:
    """상승/하락 종목 비율 → -1, 0, +1"""
    total = rising + falling
    if total <= 0:
        return 0
    ratio = rising / total
    if ratio > 0.60:
        return 1
    elif ratio < 0.40:
        return -1
    return 0


def _flow_score(foreign_inst_net: float) -> int:
    """외인+기관 순매수 금액(억원) → -1, 0, +1"""
    if foreign_inst_net > 500:
        return 1
    elif foreign_inst_net < -500:
        return -1
    return 0


def _stress_penalty(intraday_dd: float = 0, vi_count: int = 0,
                     hoga_imbalance: float = 1.0) -> int:
    """스트레스 감점. 0 ~ -3"""
    penalty = 0
    if intraday_dd < -0.02:  # 장중 -2% 이상 낙폭
        penalty -= 1
    if vi_count >= 3:  # VI 다수 발생
        penalty -= 1
    if hoga_imbalance < 0.5:  # 매도잔량 >>> 매수잔량
        penalty -= 1
    return penalty


def _total_to_regime(total: float) -> RegimeLevel:
    """종합 점수 → 5등급"""
    if total >= 2.0:
        return RegimeLevel.STRONG_BULL
    elif total >= 1.0:
        return RegimeLevel.BULL
    elif total >= -1.0:
        return RegimeLevel.NEUTRAL
    elif total >= -2.0:
        return RegimeLevel.BEAR
    else:
        return RegimeLevel.STRONG_BEAR


def _apply_intraday_override(regime: RegimeLevel,
                              intraday_dd: float,
                              vi_count: int,
                              kospi_change: float,
                              breadth_ratio: float) -> RegimeLevel:
    """장중 강제 보정 (cap/floor)"""
    # 장중 -3% 이상 → 최대 BEAR
    if intraday_dd < -0.03:
        regime = RegimeLevel(min(regime.value, RegimeLevel.BEAR.value))

    # VI 다수 + 종가 약세 → 최대 BEAR
    if vi_count >= 3 and kospi_change < 0:
        regime = RegimeLevel(min(regime.value, RegimeLevel.BEAR.value))

    # 장중 강한 상승 유지 + breadth 강함 → 최소 BULL
    if kospi_change > 0.02 and breadth_ratio > 0.65:
        regime = RegimeLevel(max(regime.value, RegimeLevel.BULL.value))

    return regime


# ── Main Function ─────────────────────────────────────────────

def calculate_actual(provider: Any = None, force: bool = False) -> Dict[str, Any]:
    """
    T일 실제 레짐 산출.
    force=False: 15:30 이후만 실행.
    force=True: 시간 제한 무시 (테스트용).
    """
    if not force and not is_after_market_close():
        return {"error": "장 마감 전 actual 계산 불가", "unavailable": True}

    actual_date = date.today()

    if not provider:
        return {"error": "no provider", "unavailable": True}

    # ── 1. KOSPI 데이터 수집 ──
    try:
        from regime.collector_domestic import collect_kospi, collect_kosdaq
        kospi_today = collect_kospi(provider, actual_date)
    except Exception as e:
        return {"error": f"KOSPI collect failed: {e}", "unavailable": True}

    if not kospi_today.get("ok"):
        return {"error": "KOSPI data unavailable", "unavailable": True}

    today_close = kospi_today["data"]["close"]
    # flu_rt에서 등락률 직접 사용 (전일 대비 %, ka20001 응답)
    kospi_change = kospi_today["data"].get("change_pct", 0)
    prev_close = today_close / (1 + kospi_change) if kospi_change != -1 else 0

    # Breadth from KOSPI response
    rising = kospi_today["data"].get("rising", 0)
    falling = kospi_today["data"].get("falling", 0)
    steady = kospi_today["data"].get("steady", 0)
    total_stocks = rising + falling
    breadth_ratio = rising / total_stocks if total_stocks > 0 else 0.5

    # ── 2. KOSDAQ (보조) ──
    kosdaq_change = 0
    try:
        kosdaq_today = collect_kosdaq(provider, actual_date)
        if kosdaq_today.get("ok"):
            kosdaq_change = kosdaq_today["data"].get("change_pct", 0)
    except Exception:
        pass

    # ── 3. 수급 (현재 REST API 미지원 → 기본값) ──
    # TODO: ka10063(장중투자자별매매) 또는 다른 소스로 구현
    foreign_inst_net = 0  # 억원, 0 = 중립

    # ── 4. 스트레스 (현재 제한적 데이터) ──
    # TODO: intraday DD, VI, 호가불균형은 장중 수집 데이터 필요
    intraday_dd = 0
    vi_count = 0
    hoga_imbalance = 1.0  # 1.0 = 균형

    # ── 5. 점수 산출 ──
    rs = _ret_score(kospi_change)
    bs = _breadth_score(rising, falling)
    fs = _flow_score(foreign_inst_net)
    sp = _stress_penalty(intraday_dd, vi_count, hoga_imbalance)

    total = rs + 0.5 * bs + 0.5 * fs + sp

    # ── 6. 레짐 판정 ──
    regime = _total_to_regime(total)

    # ── 7. 장중 보정 ──
    regime = _apply_intraday_override(
        regime, intraday_dd, vi_count, kospi_change, breadth_ratio
    )

    # ── 8. 저장 + 반환 ──
    record = {
        "market_date": str(actual_date),
        "actual_regime": regime.value,
        "actual_label": regime.name,
        "kospi_change": round(kospi_change, 6),
        "kospi_today": today_close,
        "kospi_prev": prev_close,
        "kosdaq_change": round(kosdaq_change, 6),
        "breadth_ratio": round(breadth_ratio, 4),
        "rising": rising,
        "falling": falling,
        "foreign_inst_net": foreign_inst_net,
        "intraday_dd": intraday_dd,
        "vi_count": vi_count,
        "actual_method": "jeff_v2",
        # Score breakdown
        "scores": {
            "ret_score": rs,
            "breadth_score": bs,
            "flow_score": fs,
            "stress_penalty": sp,
            "total": round(total, 2),
        },
    }

    try:
        save_actual(record)
        logger.info(
            f"[Actual] {actual_date}: {regime.name} "
            f"(total={total:.1f}, ret={rs}, breadth={bs}, flow={fs}, stress={sp})"
        )
    except Exception as e:
        logger.error(f"[Actual] Save failed: {e}")
        record["save_error"] = str(e)

    return record
