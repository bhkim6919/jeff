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

# Kiwoom REST API IDs (lab_simulator.py에서 가져옴)
_RANKING_API_MAP = {
    "실시간순위": ("ka00198", "/api/dostk/stkinfo"),
    "등락률":    ("ka10027", "/api/dostk/mrkcond"),
    "거래량":    ("ka10030", "/api/dostk/mrkcond"),
    "거래대금":  ("ka10032", "/api/dostk/mrkcond"),
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


class SurgeScanner:
    """Periodically scans ranking TR for surge candidates."""

    def scan(self, provider: Any, config: SurgeConfig) -> List[SurgeCandidate]:
        """
        Fetch ranking from Kiwoom REST API.
        Returns list of SurgeCandidate with tr_ts stamped.
        """
        source = config.ranking_source
        top_n = config.ranking_top_n
        api_id, path = _RANKING_API_MAP.get(source, _RANKING_API_MAP["등락률"])

        if api_id == "ka00198":
            body: Dict[str, Any] = {"qry_tp": "1"}
        else:
            body = {
                "mkt_tp_cd": "0",
                "vol_tp_cd": "0",
                "prc_tp_cd": "0",
                "up_dn_tp": "1",
                "cont_yn": "N",
                "cont_key": "",
            }

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
                if api_id == "ka00198":
                    name = item.get("stk_nm", "").strip()
                    price = abs(int(item.get("past_curr_prc", "0").replace(",", "") or "0"))
                    change_pct = float(item.get("base_comp_chgr", "0") or "0")
                    code = ""
                    volume = 0
                else:
                    code = str(item.get("stk_cd", item.get("shtn_pdno", ""))).strip()
                    name = item.get("stk_nm", item.get("hts_kor_isnm", "")).strip()
                    price = abs(int(item.get("cur_prc", item.get("stck_prpr", 0))))
                    change_pct = float(item.get("flu_rt", item.get("prdy_ctrt", 0)))
                    volume = int(item.get("acml_vol", item.get("acml_vol", 0)))

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
