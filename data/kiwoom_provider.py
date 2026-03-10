# -*- coding: utf-8 -*-
"""
KiwoomProvider
==============
DataProvider 인터페이스의 Kiwoom OpenAPI+ 실구현체.

사용 전제:
  - Kiwoom OpenAPI+ 설치 및 로그인 완료
  - QApplication 인스턴스가 이미 존재해야 함
  - Python 3.9 32비트 환경

현재 설계 포인트
----------------
1) 지수(코스피/코스닥) 일봉 TR(opt20006)은 DISABLE_INDEX_TR = True 로 막아두었음.
   - MarketAnalyzer는 TrTimeoutError를 받아서 시장 상태를 SIDEWAYS로 보수적으로 가정.
   - pykrx/웹 JSON 에러로 인한 중단을 피하기 위한 임시 안전 모드.

2) 종목 기본정보(get_stock_info)는 opt10001 TR을 전혀 사용하지 않고,
   - GetMasterCodeName
   - GetMasterListedStockCnt
   - GetMasterLastPrice
   - (가능하면) GetStockMarketKind
   만 사용해서 TR 타임아웃 리스크를 제거.

3) 향후 외국인/기관 데이터 등 추가 TR은 별도 캐시/오프라인 수집 프로세스로 빼는 것이 안전.
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

from core.data_provider import DataProvider


# ── 상수 ────────────────────────────────────────────────────────────────────

# Kiwoom 마켓 코드
MARKET_CODE = {
    "KOSPI": "0",
    "KOSDAQ": "10",
}

# 업종/지수 코드 (opt20006 기준)
INDEX_CODE = {
    "KOSPI": "001",
    "KOSDAQ": "101",
}

# TR 레이트/타임아웃 관련
TR_DELAY       = 0.22   # TR 요청 간 최소 간격 (키움 제한: 1초 5건 기준으로 여유 있게)
TR_TIMEOUT_SEC = 20     # 1회 TR 응답 대기 타임아웃 (초)
TR_MAX_RETRY   = 3      # 최대 재시도 횟수

# 지수 TR 완전 비활성 플래그 (True이면 opt20006은 아예 호출하지 않음)
DISABLE_INDEX_TR = True


# ── 로거 설정 ────────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).resolve().parent / "logs"
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
        app = QApplication(sys.argv)
        kiwoom_ax = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        # CommConnect 등 로그인 완료 후...
        provider = KiwoomProvider(kiwoom_ax)
    """

    def __init__(self, kiwoom: QAxWidget):
        self._k = kiwoom

        # TR 응답 대기용 루프/타이머
        self._loop = QEventLoop()
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._on_timeout)

        # TR 결과 저장용 버퍼
        self._data: List[Dict] = []
        self._prev_next: str = "0"
        self._timed_out: bool = False

        # 이벤트 핸들러 연결
        self._k.OnReceiveTrData.connect(self._on_tr_data)

        # 종목 기본 정보 캐시 (master 함수 결과)
        self._stock_info_cache: Dict[str, Dict] = {}

    # ── DataProvider 인터페이스 구현 ─────────────────────────────────────────

    # 1) 지수 일봉 (opt20006)  → 현재는 비활성화 (SIDEWAYS 폴백용)
    def get_index_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """
        업종/지수 일봉 데이터 (opt20006)

        DISABLE_INDEX_TR = True 인 경우:
          - 실제 TR을 호출하지 않고 TrTimeoutError를 발생시켜
            MarketAnalyzer가 SIDEWAYS로 폴백하도록 유도한다.
        """
        if DISABLE_INDEX_TR:
            msg = (
                "[KiwoomProvider] 업종일봉요청(opt20006) 비활성화 설정 — "
                f"index={code}, days={days}. MarketAnalyzer는 SIDEWAYS로 폴백해야 함."
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
            parse_mode="ohlcv",
        )

        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
        df = self._clean_ohlcv(df)
        return df.tail(days).reset_index(drop=True)

    # 2) 종목 리스트 (동기 마스터 함수)
    def get_stock_list(self, market: str) -> list:
        mcode = MARKET_CODE.get(market, "0")
        raw = self._k.dynamicCall("GetCodeListByMarket(QString)", mcode)
        codes = [c for c in str(raw).strip().split(";") if c]
        return codes

    # 3) 종목 일봉 (opt10081)
    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
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
            parse_mode="ohlcv",
        )

        if not rows:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
        df = self._clean_ohlcv(df)
        return df.tail(days).reset_index(drop=True)

    # 4) 종목 기본 정보 (TR 없이 master 함수로만 처리)
    def get_stock_info(self, code: str) -> dict:
        """
        종목 기본 정보 (TR 없이 Kiwoom master 함수만 사용).

        - 이름: GetMasterCodeName
        - 상장주식수: GetMasterListedStockCnt
        - 현재가: GetMasterLastPrice
        - 시가총액: 현재가 × 상장주식수

        ※ GetStockMarketKind 는 KHOpenAPI 컨트롤에 없어서 호출하지 않는다.
        """

        # 1) 캐시 체크
        if code in self._stock_info_cache:
            return self._stock_info_cache[code]

        # 2) Kiwoom master 동기 함수 호출
        name_raw   = self._k.dynamicCall("GetMasterCodeName(QString)", code)
        listed_raw = self._k.dynamicCall("GetMasterListedStockCnt(QString)", code)
        price_raw  = self._k.dynamicCall("GetMasterLastPrice(QString)", code)

        name   = str(name_raw).strip() if name_raw is not None else ""
        listed = self._to_int(listed_raw)
        price  = self._to_int(price_raw)

        market_cap = price * listed  # 원 단위 시가총액

        info = {
            "name":          name,
            "sector":        "",          # TR 미사용이므로 일단 공백
            "market":        "",          # 시장 구분은 현재 사용 안 함
            "market_cap":    market_cap,
            "listed_shares": listed,
        }

        self._stock_info_cache[code] = info
        return info

    # 5) 외국인/기관 데이터 (현재는 TR 미사용, 빈 데이터프레임 리턴)
    def get_foreign_institution_data(self, code: str, days: int) -> pd.DataFrame:
        """
        향후 opt10059 등으로 구현 예정.

        현재는 TR 타임아웃 리스크를 피하기 위해,
        컬럼만 맞춘 빈 DataFrame을 반환한다.
        """
        logger.warning(
            "[KiwoomProvider] get_foreign_institution_data(%s, %d) 아직 미구현 → 빈 DataFrame 반환",
            code,
            days,
        )
        return pd.DataFrame(columns=["date", "foreign", "institution"])

    # 6) 최근 N일 평균 거래량 (일봉 기반)
    def get_avg_daily_volume(self, code: str, days: int = 20) -> int:
        df = self.get_stock_ohlcv(code, days=days)
        if df.empty:
            return 0
        return int(df["volume"].mean())

    # 7) 현재가 (동기 마스터 함수)
    def get_current_price(self, code: str) -> Optional[int]:
        price = self._k.dynamicCall("GetMasterLastPrice(QString)", code)
        try:
            return int(str(price).replace(",", "").replace(" ", ""))
        except (TypeError, ValueError):
            return None

    # ── 내부: TR 이벤트 핸들러/유틸 ─────────────────────────────────────────

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

        parse_mode 은 _request_tr_with_retry 에서 설정하고,
        여기서는 "일자/시가/고가/저가/현재가/거래량" 형태의 OHLCV만 파싱한다.
        (지금 구조에서는 opt20006, opt10081 등 일봉 계열만 이 경로로 사용)
        """
        if self._timed_out:
            # 이미 타임아웃 처리된 경우 이 응답은 무시
            return

        self._prev_next = prev_next

        rows: List[List] = []
        i = 0
        while True:
            date = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "일자",
            )
            date = str(date).strip()
            if not date:
                break

            open_ = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "시가",
            )
            high_ = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "고가",
            )
            low_ = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "저가",
            )
            close_ = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "현재가",
            )
            vol_ = self._k.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode,
                rqname,
                i,
                "거래량",
            )

            rows.append(
                [
                    date,
                    str(open_).strip(),
                    str(high_).strip(),
                    str(low_).strip(),
                    str(close_).strip(),
                    str(vol_).strip(),
                ]
            )
            i += 1

        self._data.extend(rows)
        self._loop.quit()

    def _on_timeout(self):
        self._timed_out = True
        self._loop.quit()

    def _request_tr_with_retry(
        self,
        trcode: str,
        rqname: str,
        days: int,
        setup_func,
        parse_mode: str = "ohlcv",
    ) -> List[List]:
        """
        TR 요청 + 타임아웃/재시도 처리.

        parse_mode:
          - "ohlcv": _on_tr_data에서 파싱한 [date, open, high, low, close, volume] 구조 사용
        """
        all_rows: List[List] = []
        try_count = 0

        while try_count < TR_MAX_RETRY:
            try_count += 1
            self._data = []
            self._prev_next = "0"
            self._timed_out = False

            # 입력값 세팅
            setup_func()

            # TR 요청
            self._k.dynamicCall(
                "CommRqData(QString,QString,int,QString)",
                rqname,
                trcode,
                0,
                "9000",
            )

            # 타임아웃 타이머 시작
            self._timer.start(int(TR_TIMEOUT_SEC * 1000))
            self._loop.exec_()
            self._timer.stop()

            if self._timed_out:
                logger.warning(
                    "[TR 타임아웃] %s(%s) — %d초 응답 없음 (%d/%d회)",
                    rqname,
                    trcode,
                    TR_TIMEOUT_SEC,
                    try_count,
                    TR_MAX_RETRY,
                )
            else:
                # 정상 응답 → 데이터 누적
                all_rows.extend(self._data)
                if len(all_rows) >= days:
                    break

                # 다음 데이터 더 없음
                if self._prev_next != "2":
                    break

            # 다음 TR까지 딜레이
            time.sleep(TR_DELAY)

        if not all_rows:
            msg = f"[TR 최종 실패] {rqname}({trcode}) — {TR_MAX_RETRY}회 재시도 모두 무응답."
            logger.error(msg)
            with open(TR_ERROR_LOG, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now()}] {msg}\n"
                    f"{traceback.format_exc()}\n"
                    f"{'-' * 80}\n"
                )

            # ⚠ opt10081(주식일봉차트조회)는 라이브 운용에서 대량 호출되므로
            #    타임아웃을 '致命 오류'로 보지 않고, 경고 후 빈 데이터 반환으로 완화한다.
            if trcode == "opt10081" or rqname == "주식일봉차트조회":
                logger.warning(
                    "[KiwoomProvider] %s(%s) 타임아웃 → 빈 데이터 반환 후 계속 진행",
                    rqname,
                    trcode,
                )
                return []

            # 그 외 TR은 기존대로 치명 오류 처리
            raise TrTimeoutError(msg)

            # 현재 구현에서는 OHLCV 일봉 계열만 사용
        if parse_mode == "ohlcv":
            return all_rows[:days]

        return all_rows[:days]

    # ── 공통 정제 함수 ──────────────────────────────────────────────────────

    @staticmethod
    def _clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.rename(
            columns={
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            },
            inplace=True,
        )

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