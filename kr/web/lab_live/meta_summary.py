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
    sd_map = {row["strategy"]: row for row in sd}

    strategy_fit = {}
    for sname, sd_row in sd_map.items():
        strategy_fit[sname] = _score_fit_combined(sname, mc, sd_row)
        fit = strategy_fit[sname]
        logger.debug(
            f"[META_RETURN_DEBUG] {trade_date} {sname}: "
            f"dr={sd_row.get('daily_return')} cr={sd_row.get('cumul_return')} "
            f"pc={sd_row.get('position_count')} ge={sd_row.get('gross_exposure')} "
            f"dq={fit['data_quality']['status']} mf={fit['score']} "
            f"final={fit['final_score']} final_v={fit['final_score_value']} "
            f"rankable={fit['rankable']}"
        )

    data_days = _count_data_days()

    # ── Recommendation log 저장 ──
    top_strategy = None
    top3 = []
    dq_overall = "?"
    try:
        import json as _json
        # 가중치: final_score_value 기반 (rankable만, softmax-like 정규화)
        rankable = {s: f for s, f in strategy_fit.items() if f.get("rankable", True)}
        weights = {}
        if rankable:
            vals = {s: max(f.get("final_score_value", 0), 0) for s, f in rankable.items()}
            total = sum(vals.values()) or 1
            weights = {s: round(v / total, 4) for s, v in vals.items()}

        sorted_by_score = sorted(
            rankable.items(),
            key=lambda x: x[1].get("final_score_value", -999),
            reverse=True,
        )
        top_strategy = sorted_by_score[0][0] if sorted_by_score else None
        top3 = [s for s, _ in sorted_by_score[:3]]

        # market_fit / perf_health summary (JSON)
        mf_summary = {s: f.get("market_fit", {}).get("score", "?") for s, f in strategy_fit.items()}
        ph_summary = {s: f.get("perf_health", {}).get("penalty", 0) for s, f in strategy_fit.items()}

        # data_quality 종합: 하나라도 BAD면 PARTIAL, 모두 OK면 OK
        dq_statuses = [f.get("data_quality", {}).get("status", "?") for f in strategy_fit.values()]
        if "BAD" in dq_statuses:
            dq_overall = "BAD"
        elif "UNKNOWN" in dq_statuses:
            dq_overall = "PARTIAL"
        else:
            dq_overall = "OK"

        _sv = mc.get("data_snapshot_id", "")
        meta_db.save_recommendation_log({
            "trade_date": trade_date,
            "snapshot_version": _sv,
            "recommended_weights_json": _json.dumps(weights),
            "top_strategy": top_strategy,
            "top3_strategies_json": _json.dumps(top3),
            "confidence_score": round(data_days / 120, 2) if data_days < 120 else 1.0,
            "regime_label": mc.get("regime_label"),
            "market_fit_summary": _json.dumps(mf_summary),
            "perf_health_summary": _json.dumps(ph_summary),
            "data_quality_status": dq_overall,
            "is_valid": 1 if dq_overall == "OK" else 0,
            "selected_source": mc.get("data_snapshot_id", ""),
        })
        logger.info(f"[META_RECOMMEND] {trade_date}: top={top_strategy}, dq={dq_overall}")
    except Exception as e:
        logger.warning(f"[META_RECOMMEND] save failed (non-fatal): {e}")

    # recommendation summary (UI 표시용)
    _rec = {
        "top_strategy": top_strategy,
        "top3": top3,
        "confidence_score": round(data_days / 120, 2) if data_days < 120 else 1.0,
        "data_quality": dq_overall,
        "execution_status": "추천만 제공 (자동 전환 없음)",
    }

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
        "recommendation": _rec,
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


# ── Data Quality Check ──────────────────────────────────────
# 기준값: collector와 동일 (_OUTLIER_THRESHOLD=0.5, mismatch=pos_value<1e-8)
# gross_exposure==0 은 일반적으로 pos_value==0 동치이나,
# 초기 운영 구간에서 DQ_EQUIV 로그로 검증 후 확정 예정.
_OUTLIER_THRESHOLD = 0.5

def _check_data_quality(sd_row: Optional[dict]) -> dict:
    """strategy_daily 행의 데이터 품질 판정."""
    if not sd_row:
        return {"status": "UNKNOWN", "flags": ["NO_DATA"]}

    flags = []
    dr = sd_row.get("daily_return")
    pc = sd_row.get("position_count", 0)
    ge = sd_row.get("gross_exposure")

    if dr is None:
        flags.append("RETURN_NULL")
    elif abs(dr) > _OUTLIER_THRESHOLD:
        flags.append("OUTLIER")

    # mismatch: position 있는데 exposure 0 → snapshot 불일치
    if pc > 0 and ge is not None and abs(ge) < 1e-8:
        flags.append("SNAPSHOT_MISMATCH")

    if "OUTLIER" in flags or "SNAPSHOT_MISMATCH" in flags:
        status = "BAD"
    elif "RETURN_NULL" in flags or "NO_DATA" in flags:
        status = "UNKNOWN"
    else:
        status = "OK"

    return {"status": status, "flags": flags}


# ── Performance Health ──────────────────────────────────────

def _perf_health(sd_row: Optional[dict], dq: dict) -> dict:
    """실제 성과 기반 페널티. data_quality != OK이면 페널티 금지."""
    if not sd_row or dq["status"] != "OK":
        return {"penalty": 0, "reasons": []}

    penalty = 0
    reasons = []
    dr = sd_row.get("daily_return")
    cr = sd_row.get("cumul_return")

    # daily_return 기반 페널티 (시간축 혼합 방지: 당일 데이터 우선)
    if dr is not None:
        if dr <= -0.02:
            penalty = -2
            reasons.append({"sign": "-", "text": f"당일손실 {dr:.1%}"})
        elif dr < 0:
            penalty = -1
            reasons.append({"sign": "-", "text": f"당일손실 {dr:.1%}"})

    # cumul_return은 보조 reason만 (penalty 없음)
    if cr is not None and cr <= -0.05:
        reasons.append({"sign": "-", "text": f"누적손실 {cr:.1%}"})

    return {"penalty": penalty, "reasons": reasons}


# ── Combined Fitness ────────────────────────────────────────

def _score_fit_combined(strategy: str, mc: dict, sd_row: Optional[dict]) -> dict:
    """market_fit + perf_health + data_quality → final_score."""
    mf = _score_fit(strategy, mc)
    dq = _check_data_quality(sd_row)
    ph = _perf_health(sd_row, dq)

    final_value = mf["score_value"] + ph["penalty"]

    # reasons 병합 (우선순위: dq경고 → 괴리 → 성과 → 시장)
    reasons = []

    # 1. data_quality flags
    if dq["status"] == "BAD":
        for f in dq["flags"]:
            reasons.append({"sign": "-", "text": f"데이터경고: {f}"})
    elif dq["status"] == "UNKNOWN":
        reasons.append({"sign": "-", "text": "데이터 부족 (판단 제한)"})

    # 2. divergence warning (dq OK일 때만)
    if dq["status"] == "OK" and sd_row:
        cr = sd_row.get("cumul_return")
        dr = sd_row.get("daily_return")
        if mf["score"] == "HIGH" and ((dr is not None and dr < 0) or (cr is not None and cr < 0)):
            reasons.append({"sign": "-", "text": "⚠ 시장적합 vs 실적 괴리"})
        elif mf["score"] == "LOW" and cr is not None and cr > 0.02:
            reasons.append({"sign": "+", "text": "✓ 시장불리 but 성과 양호"})

    # 3. performance reasons
    reasons.extend(ph["reasons"])

    # 4. market reasons
    reasons.extend(mf["reasons"])

    # final_score 결정
    if dq["status"] == "BAD":
        final_score = "WARN"
        final_value = -999
        rankable = False
    elif dq["status"] == "UNKNOWN" and mf["score"] == "HIGH":
        # UNKNOWN 과대낙관 방지: HIGH → MID 강제 하향
        final_score = "MID"
        rankable = True
    else:
        final_score = "HIGH" if final_value >= 2 else "LOW" if final_value < 0 else "MID"
        rankable = True

    return {
        # 기존 키 (market_fit 의미 보존 — 하위호환)
        "score": mf["score"],
        "score_value": mf["score_value"],
        # 최종 판정
        "final_score": final_score,
        "final_score_value": final_value if rankable else -999,
        "rankable": rankable,
        "reasons": reasons,
        # 상세 레이어
        "market_fit": {"score": mf["score"], "score_value": mf["score_value"]},
        "perf_health": ph,
        "data_quality": dq,
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
    try:
        from shared.db.pg_base import connection
        with connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM meta_market_context")
            row = cur.fetchone()
            cur.close()
            return row[0] if row else 0
    except Exception:
        return 0
