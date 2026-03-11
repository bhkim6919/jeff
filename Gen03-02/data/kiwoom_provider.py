# -*- coding: utf-8 -*-
"""
KiwoomProvider
==============
Kiwoom OpenAPI+ 기반 DataProvider 구현체.
api/kiwoom_api_wrapper.py 에서 로그인된 kiwoom 객체를 받아 초기화한다.

설계 포인트
-----------
1) 지수(코스피/코스닥) 일봉 TR(opt20006)은 DISABLE_INDEX_TR = True 로 막아두었음.
   - RegimeDetector는 TrTimeoutError를 받아서 시장 상태를 SIDEWAYS로 보수적으로 가정.

2) 종목 기본정보(get_stock_info)는 TR 없이 Kiwoom master 동기 함수만 사용.
   - GetMasterCodeName / GetMasterListedStockCnt / GetMasterLastPrice

3) TR 예외:
   - TrTimeoutError — 런타임 엔진에서 보수적 SIDEWAYS 처리에 활용.
"""

import time
import traceback
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

import pandas as pd
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer

from data.data_provider import DataProvider


# ── 상수 ────────────────────────────────────────────────────────────────────

# Kiwoom 마켓 코드
MARKET_CODE = {
    "KOSPI":  "0",
    "KOSDAQ": "10",
}

# 업종/지수 코드 (opt20006 기준)
INDEX_CODE = {
    "KOSPI":  "001",
    "KOSDAQ": "101",
}

# TR 레이트/타임아웃
TR_DELAY            = 0.5    # TR 요청 간 최소 간격 (초) — 기준서 §12
TR_TIMEOUT_SEC      = 20     # 1회 TR 응답 대기 타임아웃 (초)
TR_MAX_RETRY        = 3      # 최대 재시도 횟수
TR_MAX_CONSECUTIVE  = 5      # 연속 타임아웃 임계값 — 초과 시 배치 중단

# trcode 별 전용 스크린 번호 (동일 스크린 재사용으로 인한 TR 취소 방지)
SCREEN_MAP = {
    "opt10081": "9001",   # 주식일봉차트조회
    "opt20006": "9002",   # 업종일봉조회
}

# 지수 TR 비활성 플래그 (False = 정상 작동, True = SIDEWAYS 강제)
DISABLE_INDEX_TR = False


# ── 로거 설정 ────────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

TR_ERROR_LOG = LOG_DIR / f"tr_error_{datetime.today().strftime('%Y%m%d')}.log"

logger = logging.getLogger("KiwoomProvider")


# ── 커스텀 예외 ──────────────────────────────────────────────────────────────

class TrTimeoutError(Exception):
    """TR 요청이 최대 재시도 횟수를 초과해도 응답 없을 때 발생."""
    pass


# ── 본체 클래스 ─────────────────────────────────────────────────────────────

class KiwoomProvider(DataProvider):
    """
    Kiwoom OpenAPI+ 기반 실데이터 Provider.

    사용 예:
        from api.kiwoom_api_wrapper import create_loggedin_kiwoom
        kiwoom = create_loggedin_kiwoom()
        provider = KiwoomProvider(kiwoom)
    """

    def __init__(self, kiwoom: QAxWidget, sector_map_path: Optional[str] = None):
        """
        kiwoom: api/kiwoom_api_wrapper.create_loggedin_kiwoom() 반환값
        sector_map_path: sector_map.json 경로 (None이면 자동 탐색)
        """
        self._k = kiwoom

        # TR 응답 대기용 루프/타이머
        self._loop  = QEventLoop()
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._on_timeout)

        # TR 결과 저장용 버퍼
        self._data:          List[List] = []
        self._prev_next:     str        = "0"
        self._timed_out:     bool       = False
        self._current_rqname: str       = ""
        self._current_trcode: str       = ""

        # 이벤트 핸들러 연결
        self._k.OnReceiveTrData.connect(self._on_tr_data)
        self._k.OnReceiveMsg.connect(self._on_msg)

        # 종목 기본 정보 캐시
        self._stock_info_cache: Dict[str, Dict] = {}

        # 연속 타임아웃 카운터 (TR_MAX_CONSECUTIVE 초과 시 배치 중단)
        self._consecutive_timeout: int = 0

        # 섹터 맵 로드 (data/sector_map.json)
        self._sector_map: Dict[str, str] = self._load_sector_map(sector_map_path)
        logger.info("[KiwoomProvider] 섹터맵 %d개 종목 로드", len(self._sector_map))

        # ── 주문 체결 관련 ────────────────────────────────────────────────
        self._order_loop   = QEventLoop()
        self._order_timer  = QTimer()
        self._order_timer.setSingleShot(True)
        self._order_timer.timeout.connect(self._on_order_timeout)
        self._order_result: Optional[Dict] = None
        self._order_code:   str = ""
        self._order_qty:    int = 0
        self._k.OnReceiveChejanData.connect(self._on_chejan_data)

    @staticmethod
    def _load_sector_map(path: Optional[str]) -> Dict[str, str]:
        import json
        if path is None:
            path = Path(__file__).resolve().parent / "sector_map.json"
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("[KiwoomProvider] sector_map.json 로드 실패: %s", e)
            return {}

    # ── DataProvider 인터페이스 구현 ─────────────────────────────────────────

    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        업종/지수 일봉 데이터 (opt20006).

        DISABLE_INDEX_TR = True 인 경우:
          TR을 호출하지 않고 TrTimeoutError를 발생시켜
          RegimeDetector가 SIDEWAYS로 폴백하도록 유도한다.
        """
        if DISABLE_INDEX_TR:
            msg = (
                "[KiwoomProvider] 업종일봉요청(opt20006) 비활성화 설정 - "
                f"index={code}, days={days}. RegimeDetector는 SIDEWAYS로 폴백해야 함."
            )
            logger.warning(msg)
            raise TrTimeoutError(msg)

        업종코드 = INDEX_CODE.get(code, "001")
        today = datetime.today().strftime("%Y%m%d")

        def _setup():
            self._k.dynamicCall("SetInputValue(QString,QString)", "업종코드", 업종코드)
            self._k.dynamicCall("SetInputValue(QString,QString)", "기준일자", today)
            self._k.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")

        rows = self._request_tr_with_retry(
            trcode="opt20006",
            rqname="업종일봉요청",
            days=days,
            setup_func=_setup,
        )

        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
        df = self._clean_ohlcv(df)
        return df.tail(days).reset_index(drop=True)

    def get_stock_list(self, market: str) -> list:
        """시장별 종목코드 리스트 (동기 마스터 함수)."""
        mcode = MARKET_CODE.get(market, "0")
        raw   = self._k.dynamicCall("GetCodeListByMarket(QString)", mcode)
        return [c for c in str(raw).strip().split(";") if c]

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """종목 일봉 OHLCV (opt10081 주식일봉차트조회)."""
        today = datetime.today().strftime("%Y%m%d")

        def _setup():
            self._k.dynamicCall("SetInputValue(QString,QString)", "종목코드", code)
            self._k.dynamicCall("SetInputValue(QString,QString)", "기준일자", today)
            self._k.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")

        rows = self._request_tr_with_retry(
            trcode="opt10081",
            rqname="주식일봉차트조회",
            days=days,
            setup_func=_setup,
        )

        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
        df = self._clean_ohlcv(df)
        return df.tail(days).reset_index(drop=True)

    def get_stock_info(self, code: str) -> dict:
        """
        종목 기본 정보 (TR 없이 Kiwoom master 동기 함수만 사용).

        - 이름: GetMasterCodeName
        - 상장주식수: GetMasterListedStockCnt
        - 현재가: GetMasterLastPrice
        - 시가총액: 현재가 x 상장주식수
        """
        if code in self._stock_info_cache:
            return self._stock_info_cache[code]

        name_raw   = self._k.dynamicCall("GetMasterCodeName(QString)", code)
        listed_raw = self._k.dynamicCall("GetMasterListedStockCnt(QString)", code)
        price_raw  = self._k.dynamicCall("GetMasterLastPrice(QString)", code)

        name       = str(name_raw).strip() if name_raw is not None else ""
        listed     = self._to_int(listed_raw)
        price      = self._to_int(price_raw)
        market_cap = price * listed

        info = {
            "name":          name,
            "sector":        self._sector_map.get(code, "기타"),
            "market":        "",
            "market_cap":    market_cap,
            "listed_shares": listed,
        }

        self._stock_info_cache[code] = info
        return info

    def get_avg_daily_volume(self, code: str, days: int = 20) -> float:
        """N일 평균 거래대금 (거래량 × 종가, 단위: 원)."""
        df = self.get_stock_ohlcv(code, days=days)
        if df.empty:
            return 0.0
        return float((df["volume"] * df["close"]).mean())

    def get_current_price(self, code: str) -> float:
        """현재가 (GetMasterLastPrice 동기 호출)."""
        price = self._k.dynamicCall("GetMasterLastPrice(QString)", code)
        try:
            return float(str(price).replace(",", "").replace(" ", ""))
        except (TypeError, ValueError):
            return 0.0

    def get_investor_trend(self, code: str, days: int) -> dict:
        """
        외인/기관 순매수 트렌드.
        현재는 TR 미사용 - 향후 opt10059 등으로 구현 예정.
        """
        if not getattr(self, '_investor_trend_warned', False):
            logger.warning("[KiwoomProvider] get_investor_trend() 미구현 — 이후 출력 억제")
            self._investor_trend_warned = True
        return {}

    # ── 내부: TR 이벤트 핸들러 ───────────────────────────────────────────────

    def _on_tr_data(
        self,
        screen_no,
        rqname,
        trcode,
        recordname,
        prev_next,
        *args,
    ):
        """
        OnReceiveTrData 이벤트 핸들러.
        opt20006 / opt10081 일봉 계열 OHLCV 파싱.
        """
        if self._timed_out:
            return

        # 현재 요청한 TR이 아닌 응답은 무시 (이전 TR 지연 응답 방어)
        if rqname != self._current_rqname or trcode != self._current_trcode:
            logger.debug(
                "[TR 무시] 예상(%s/%s) 수신(%s/%s)",
                self._current_rqname, self._current_trcode, rqname, trcode,
            )
            return

        self._prev_next = prev_next

        rows: List[List] = []
        i = 0
        while True:
            date = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode, rqname, i, "일자",
            )
            date = str(date).strip()
            if not date:
                break

            open_  = self._k.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "시가")
            high_  = self._k.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "고가")
            low_   = self._k.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "저가")
            close_ = self._k.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "현재가")
            vol_   = self._k.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "거래량")

            rows.append([
                date,
                str(open_).strip(),
                str(high_).strip(),
                str(low_).strip(),
                str(close_).strip(),
                str(vol_).strip(),
            ])
            i += 1

        self._data.extend(rows)
        self._loop.quit()

    def _on_timeout(self):
        self._timed_out = True
        self._loop.quit()

    def _on_msg(self, screen_no, rqname, trcode, msg):
        """
        OnReceiveMsg 핸들러.
        서버가 TR을 거부하거나 에러 메시지를 보낼 때 호출됨.
        루프를 즉시 종료해 20초 타임아웃 대신 즉각 에러 확인.
        """
        msg = str(msg).strip()
        logger.warning("[OnReceiveMsg] screen=%s rq=%s tr=%s msg=%s", screen_no, rqname, trcode, msg)

        # 주문 화면 메시지 — 거부/오류 시 주문 대기 즉시 종료
        if str(screen_no) == "7001":
            if any(kw in msg for kw in ["거부", "실패", "오류", "제한"]):
                if self._order_result is None:
                    self._order_result = {
                        "order_no": "", "exec_price": 0.0, "exec_qty": 0,
                        "error": f"주문 거부: {msg}",
                    }
                    if self._order_loop.isRunning():
                        self._order_loop.quit()
            return

        # 현재 대기 중인 TR에 대한 메시지면 루프 종료
        if rqname == self._current_rqname and trcode == self._current_trcode:
            self._timed_out = True
            self._loop.quit()

    # ── 주문 실행 (SendOrder) ─────────────────────────────────────────────────

    def get_account_no(self) -> str:
        """첫 번째 계좌번호 반환."""
        raw = self._k.dynamicCall("GetLoginInfo(QString)", "ACCNO")
        accts = str(raw).strip().rstrip(";").split(";")
        if accts and accts[0]:
            return accts[0]
        logger.error("[KiwoomProvider] 계좌번호 조회 실패")
        return ""

    def send_order(self, code: str, side: str, quantity: int,
                   price: int = 0, hoga_type: str = "03") -> Dict:
        """
        Kiwoom SendOrder 호출 + 체결 대기.

        side:      "BUY" | "SELL"
        hoga_type: "03" = 시장가 (default), "00" = 지정가
        price:     시장가일 때 0

        Returns: {"order_no": str, "exec_price": float, "exec_qty": int, "error": str}
        """
        ORDER_TIMEOUT_SEC = 30

        account = self.get_account_no()
        if not account:
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "계좌번호 조회 실패"}

        order_type = 1 if side == "BUY" else 2   # 1=신규매수, 2=신규매도
        rqname     = f"{'매수' if side == 'BUY' else '매도'}_{code}"
        screen     = "7001"

        # 상태 초기화
        self._order_result = None
        self._order_code   = code
        self._order_qty    = quantity

        time.sleep(0.2)  # 주문 간 최소 간격

        ret = self._k.dynamicCall(
            "SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)",
            rqname, screen, account, order_type, code, quantity, int(price), hoga_type, "",
        )

        if ret != 0:
            logger.error("[SendOrder] %s %s %d주 실패 ret=%d", side, code, quantity, ret)
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"SendOrder ret={ret}"}

        logger.info("[SendOrder] %s %s %d주 접수 (시장가)", side, code, quantity)

        # 체결 대기
        self._order_timer.start(ORDER_TIMEOUT_SEC * 1000)
        self._order_loop.exec_()
        self._order_timer.stop()

        if self._order_result is None:
            logger.warning("[SendOrder] %s 체결 대기 %d초 타임아웃", code, ORDER_TIMEOUT_SEC)
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"체결 대기 {ORDER_TIMEOUT_SEC}초 타임아웃"}

        return self._order_result

    def _on_chejan_data(self, gubun, item_cnt, fid_list):
        """
        OnReceiveChejanData 이벤트 핸들러.
        gubun: "0" = 주문체결통보, "1" = 잔고통보
        """
        if str(gubun) != "0":
            return

        code = str(self._k.dynamicCall("GetChejanData(int)", 9001)).strip()
        code = code.lstrip("A")  # 'A005930' → '005930'

        if code != self._order_code:
            return

        exec_qty_raw   = str(self._k.dynamicCall("GetChejanData(int)", 911)).strip()
        exec_price_raw = str(self._k.dynamicCall("GetChejanData(int)", 910)).strip()
        order_no       = str(self._k.dynamicCall("GetChejanData(int)", 9203)).strip()

        exec_qty   = abs(int(exec_qty_raw))   if exec_qty_raw   else 0
        exec_price = abs(float(exec_price_raw)) if exec_price_raw else 0.0

        if exec_qty > 0 and exec_price > 0:
            self._order_result = {
                "order_no":   order_no,
                "exec_price": exec_price,
                "exec_qty":   exec_qty,
                "error":      "",
            }
            logger.info(
                "[체결] %s %d주 @ %,.0f원 (주문번호: %s)",
                code, exec_qty, exec_price, order_no,
            )
            if self._order_loop.isRunning():
                self._order_loop.quit()

    def _on_order_timeout(self):
        """주문 체결 대기 타임아웃."""
        if self._order_loop.isRunning():
            self._order_loop.quit()

    # ── 내부: TR 요청 공통 처리기 ────────────────────────────────────────────

    def _request_tr_with_retry(
        self,
        trcode: str,
        rqname: str,
        days: int,
        setup_func,
    ) -> List[List]:
        """
        TR 요청 + 타임아웃/재시도 처리.

        - TR_MAX_RETRY 회까지 재시도
        - opt10081은 타임아웃 시 치명 오류 아님 (빈 리스트 반환)
        - 그 외 TR은 TrTimeoutError 발생
        """
        all_rows:  List[List] = []
        try_count: int        = 0

        screen_no = SCREEN_MAP.get(trcode, "9000")

        while try_count < TR_MAX_RETRY:
            try_count            += 1
            self._data            = []
            self._prev_next       = "0"
            self._timed_out       = False
            self._current_rqname  = rqname
            self._current_trcode  = trcode

            time.sleep(TR_DELAY)   # 매 CommRqData 호출 전 반드시 대기

            setup_func()

            ret = self._k.dynamicCall(
                "CommRqData(QString,QString,int,QString)",
                rqname, trcode, 0, screen_no,
            )
            logger.debug("[CommRqData] %s(%s) screen=%s ret=%s", rqname, trcode, screen_no, ret)

            if ret == -200:
                logger.warning(
                    "[CommRqData -200] 시세과부하 — 1초 대기 후 재시도 (%d/%d회)",
                    try_count, TR_MAX_RETRY,
                )
                time.sleep(1.0)
                continue

            if ret != 0:
                logger.error(
                    "[CommRqData 실패] %s(%s) ret=%s — 요청 거부됨, 재시도 중단.",
                    rqname, trcode, ret,
                )
                break

            self._timer.start(int(TR_TIMEOUT_SEC * 1000))
            self._loop.exec_()
            self._timer.stop()

            if self._timed_out:
                logger.warning(
                    "[TR 타임아웃] %s(%s) - %d초 응답 없음 (%d/%d회)",
                    rqname, trcode, TR_TIMEOUT_SEC, try_count, TR_MAX_RETRY,
                )
                # 기준서 §12: retry_delay = TR_DELAY × retry_count
                time.sleep(TR_DELAY * try_count)
                self._consecutive_timeout += 1
                if self._consecutive_timeout >= TR_MAX_CONSECUTIVE:
                    raise TrTimeoutError(
                        f"[연속 타임아웃 {TR_MAX_CONSECUTIVE}회] Kiwoom API 응답 불가 — 배치 중단"
                    )
            else:
                self._consecutive_timeout = 0   # 성공 시 카운터 리셋
                all_rows.extend(self._data)
                if len(all_rows) >= days:
                    break
                if self._prev_next != "2":
                    break

        if not all_rows:
            msg = f"[TR 최종 실패] {rqname}({trcode}) - {TR_MAX_RETRY}회 재시도 모두 무응답."
            logger.error(msg)
            with open(TR_ERROR_LOG, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now()}] {msg}\n"
                    f"{traceback.format_exc()}\n"
                    f"{'-' * 80}\n"
                )

            # opt10081은 경고 후 빈 데이터 반환 (대량 종목 조회 시 일부 실패 허용)
            if trcode == "opt10081":
                logger.warning(
                    "[KiwoomProvider] %s(%s) 타임아웃 -> 빈 데이터 반환 후 계속 진행",
                    rqname, trcode,
                )
                return []

            raise TrTimeoutError(msg)

        return all_rows[:days]

    # ── 공통 정제 함수 ──────────────────────────────────────────────────────

    @staticmethod
    def _clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].apply(
                lambda x: int(str(x).replace(",", "").replace(" ", "") or 0)
            )
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df

    @staticmethod
    def _to_int(val) -> int:
        try:
            return int(str(val).replace(",", "").replace(" ", "") or 0)
        except Exception:
            return 0
