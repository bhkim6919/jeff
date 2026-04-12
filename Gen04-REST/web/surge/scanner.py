# -*- coding: utf-8 -*-
"""
scanner.py -- Surge Stock Scanner
====================================
급등주 TR 스캐너. lab_simulator.py의 ranking fetch 패턴 재사용.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from web.surge.config import SurgeConfig

logger = logging.getLogger("gen4.rest.surge")

# Kiwoom REST API — ka10027/ka10030/ka10032 deprecated (1504 error).
# All ranking via ka00198 + qry_tp: 1=실시간, 2=거래량, 3=거래대금, 5=등락률
_RANKING_API_MAP = {
    "실시간순위": ("ka00198", "/api/dostk/stkinfo", "1"),
    "등락률":    ("ka00198", "/api/dostk/stkinfo", "5"),
    "거래량":    ("ka00198", "/api/dostk/stkinfo", "2"),
    "거래대금":  ("ka00198", "/api/dostk/stkinfo", "3"),
}

# ETF/ETN prefix codes
_ETF_PREFIXES = {"069", "091", "099", "100", "101", "102", "103", "104",
                 "105", "117", "122", "130", "131", "132", "133", "137",
                 "138", "139", "140", "143", "144", "145", "146", "147",
                 "148", "150", "152", "153", "155", "156", "157", "159",
                 "160", "161", "166", "167", "168", "169", "170", "171",
                 "174", "175", "176", "181", "182", "183", "184", "185",
                 "186", "187", "189", "190", "191", "192", "193", "194",
                 "195", "196", "197", "198", "199", "200", "203", "204",
                 "205", "206", "207", "208", "210", "211", "213", "214",
                 "215", "216", "217", "218", "219", "220", "223", "224",
                 "225", "226", "227", "228", "229", "230", "231", "232",
                 "233", "234", "236", "237", "238", "239", "241", "243",
                 "244", "245", "246", "247", "248", "249", "250", "251",
                 "252", "253", "254", "255", "256", "257", "258", "259",
                 "260", "261", "262", "263", "264", "265", "266", "267",
                 "268", "269", "270", "271", "272", "273", "274", "275",
                 "276", "277", "278", "279", "280", "281", "282", "283",
                 "284", "285", "286", "287", "288", "289", "290", "291",
                 "292", "293", "294", "295", "296", "297", "298", "299",
                 "300", "301", "302", "303", "304", "305", "306", "307",
                 "308", "309", "310", "311", "312", "360", "361", "363",
                 "364", "365", "366", "367", "368", "369", "370", "371",
                 "372", "373", "374", "375", "376", "377", "378", "379",
                 "380", "381", "382", "383", "384", "385", "386", "387",
                 "388", "389", "390", "391", "392", "393", "394", "395",
                 "396", "397", "398", "399", "400", "401", "402", "403",
                 "404", "405", "406", "407", "408", "409", "410", "411",
                 "412", "413", "414", "415", "416", "417", "418", "419",
                 "420", "421", "422", "423", "424", "425", "426", "427",
                 "428", "429", "430", "431", "432", "433", "434", "435",
                 "436", "437", "438", "439", "440", "441", "442", "443",
                 "444", "445", "446", "447", "448", "449", "450", "451",
                 "452", "453", "454", "455", "456", "457", "458", "459",
                 "460", "461", "462", "463", "464", "465", "466", "467",
                 "468", "469", "470"}


@dataclass
class SurgeCandidate:
    code: str
    name: str
    price: int
    change_pct: float
    volume: int
    rank: int
    tr_ts: float              # time.time() when TR was received
    # Secondary TR data (filled by enrich_*)
    volume_surge: bool = False       # ka10023 거래량급증 통과 여부
    volume_surge_pct: float = 0.0    # 거래량 증가율 %
    strength: float = 0.0            # ka10046 체결강도 (100=균형, 120+=매수우위)
    strength_pass: bool = False      # 체결강도 기준 통과 여부


class SurgeScanner:
    """Periodically scans ranking TR for surge candidates."""

    def scan(self, provider: Any, config: SurgeConfig) -> List[SurgeCandidate]:
        """
        Fetch ranking from Kiwoom REST API.
        Returns list of SurgeCandidate with tr_ts stamped.
        """
        source = config.ranking_source
        top_n = config.ranking_top_n
        entry = _RANKING_API_MAP.get(source, _RANKING_API_MAP["등락률"])
        api_id, path, qry_tp = entry

        body: Dict[str, Any] = {"qry_tp": qry_tp}

        now = time.time()
        try:
            resp = provider._request(api_id, path, body, related_code="SURGE")
        except Exception as e:
            logger.warning(f"[SURGE_SCAN] Ranking fetch failed: {e}")
            return []

        if not resp or resp.get("return_code") not in (0, None):
            logger.warning(f"[SURGE_SCAN] API error: {resp.get('return_msg', '') if resp else 'null'}")
            return []

        output = resp.get("item_inq_rank", resp.get("output", []))
        if not output:
            return []

        candidates = []
        for i, item in enumerate(output[:top_n]):
            try:
                code = str(item.get("stk_cd", "")).strip()
                name = item.get("stk_nm", "").strip()
                price = abs(int(
                    str(item.get("past_curr_prc", "0")).replace(",", "").replace("+", "") or "0"
                ))
                change_pct = float(item.get("base_comp_chgr", "0") or "0")
                volume = 0  # ka00198 doesn't return volume

                if name and price > 0:
                    candidates.append(SurgeCandidate(
                        code=code.zfill(6) if code else "",
                        name=name,
                        price=price,
                        change_pct=round(change_pct, 2),
                        volume=volume,
                        rank=i + 1,
                        tr_ts=now,
                    ))
            except (ValueError, TypeError) as e:
                logger.debug(f"[SURGE_SCAN] Parse skip: {e}")
                continue

        return candidates


    def enrich_volume_surge(self, provider: Any, candidates: List[SurgeCandidate]) -> None:
        """
        ka10023 (거래량급증) 조회 → 후보에 volume_surge 마킹.
        거래량급증 상위 종목 코드 set과 교차 확인.
        """
        try:
            body = {
                "mrkt_tp": "000",
                "sort_tp": "1",
                "tm_tp": "2",
                "trde_qty_tp": "5",
                "tm": "",
                "stk_cnd": "0",
                "pric_tp": "0",
                "stex_tp": "3",
            }
            resp = provider._request("ka10023", "/api/dostk/rkinfo", body,
                                     related_code="SURGE_VOL")
        except Exception as e:
            logger.warning(f"[SURGE_VOL] ka10023 failed: {e}")
            return

        if not resp or resp.get("return_code") not in (0, None):
            logger.warning(f"[SURGE_VOL] ka10023 rc={resp.get('return_code') if resp else 'None'}")
            return

        output = resp.get("trde_qty_sdnin", [])
        surge_codes: Dict[str, float] = {}
        for item in output[:100]:
            try:
                code = str(item.get("stk_cd", "")).strip().replace("_AL", "")
                rate_str = str(item.get("sdnin_rt", "0")).replace("+", "").replace(",", "")
                if code:
                    surge_codes[code.zfill(6)] = float(rate_str)
            except (ValueError, TypeError):
                continue

        for c in candidates:
            rate = surge_codes.get(c.code, 0)
            if rate >= 50:  # 순매수율 50% 이상
                c.volume_surge = True
                c.volume_surge_pct = rate

        logger.info(f"[SURGE_VOL] ka10023: {len(surge_codes)} surge codes, "
                    f"{sum(1 for c in candidates if c.volume_surge)} matched")

    def enrich_strength(self, provider: Any, candidates: List[SurgeCandidate],
                        min_strength: float = 115.0) -> None:
        """
        ka10046 (체결강도시세시간별) 조회 → 후보에 strength 마킹.
        종목별 조회이므로 후보 종목만 조회.
        """
        enriched = 0
        for c in candidates:
            try:
                resp = provider._request("ka10046", "/api/dostk/mrkcond",
                                         {"stk_cd": c.code},
                                         related_code="SURGE_STR")
            except Exception as e:
                logger.debug(f"[SURGE_STR] ka10046 {c.code} failed: {e}")
                continue

            if not resp or resp.get("return_code") not in (0, None):
                continue

            items = resp.get("cntr_str_tm", [])
            if items:
                latest = items[0]
                try:
                    stren = float(str(latest.get("cntr_str", "0")).replace("+", ""))
                    c.strength = stren
                    if stren >= min_strength:
                        c.strength_pass = True
                    enriched += 1
                except (ValueError, TypeError):
                    pass

            time.sleep(0.25)  # rate limit between per-stock calls

        logger.info(f"[SURGE_STR] ka10046: {enriched}/{len(candidates)} enriched, "
                    f"{sum(1 for c in candidates if c.strength_pass)} passed (>={min_strength})")


def filter_candidates(
    candidates: List[SurgeCandidate],
    config: SurgeConfig,
) -> List[SurgeCandidate]:
    """Apply basic filters: price, change_pct, ETF exclusion."""
    result = []
    for c in candidates:
        if c.price < config.min_price:
            continue
        if c.change_pct < config.min_change_pct:
            continue
        if config.exclude_etf and c.code and c.code[:3] in _ETF_PREFIXES:
            continue
        if not c.code:
            continue  # code 없으면 스킵 (ka00198 종종 code 누락)
        result.append(c)
    return result
