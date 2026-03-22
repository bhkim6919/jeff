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
    "opw00018": "9003",   # 계좌평가잔고내역
}

ORDER_TIMEOUT_SEC = 30

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


class KiwoomApiError(Exception):
    """dynamicCall 실패 시 발생."""
    pass


class ProviderDeadError(Exception):
    """COM 객체가 해제되어 provider가 더 이상 사용 불가할 때 발생. retry 금지."""
    pass


class RateLimitError(Exception):
    """Kiwoom API 호출 제한(초당/분당) 초과 시 발생. 잠시 대기 후 retry 가능."""
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

        # ── COM 객체 수명 관리 (v7.4) ──────────────────────────────────
        self._alive: bool = True           # False → 모든 API 호출 차단
        self._shutting_down: bool = False  # shutdown 진행 중 플래그

        # TR 응답 대기용 루프/타이머
        self._loop  = QEventLoop()
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._on_timeout)

        # TR 결과 저장용 버퍼
        self._data:          List[List] = []
        self._single_data:   Dict[str, str] = {}   # opw00018 single output 버퍼
        self._prev_next:     str        = "0"
        self._timed_out:     bool       = False
        self._msg_rejected:  bool       = False
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

        # 주문 상태 구조체 (v7 보강)
        self._order_state: Dict = self._make_order_state()
        self._order_result: Optional[Dict] = None   # chejan 콜백 결과 버퍼
        self._ghost_orders: List[Dict] = []          # timeout 후 미확인 주문

        self._k.OnReceiveChejanData.connect(self._on_chejan_data)

        # ── 실시간 데이터 (체결강도 등) ──────────────────────────────────
        self._k.OnReceiveRealData.connect(self._on_real_data)
        self._real_data_callback = None   # 외부 콜백 (TickAnalyzer 등)

        # ── Reconcile 실패 추적 ─────────────────────────────────────────
        self._recon_consecutive_fail: int = 0
        self._recon_last_success: Optional[datetime] = None
        self._recon_stale: bool = False  # 연속 5회 이상 실패 시 True

    @staticmethod
    def _load_sector_map(path: Optional[str]) -> Dict[str, str]:
        import json
        if path is None:
            path = Path(__file__).resolve().parent / "sector_map.json"
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("[KiwoomProvider] sector_map.json 로드 실패: %s", e)
            return {}

    # ── dynamicCall 중앙 래퍼 ─────────────────────────────────────────────────

    def _call(self, method_sig: str, *args, context: str = ""):
        """
        Kiwoom dynamicCall 중앙 래퍼.

        모든 Kiwoom API 호출을 list-style 로 통일하고, 실패 시 컨텍스트 로깅.

        Args:
            method_sig: "MethodName(Type,Type,...)" 형식
            *args: 메서드 인자들 (list 아닌 개별 인자로 전달)
            context: 로깅용 추가 컨텍스트 (예: "opt10081/주식일봉차트조회")

        Returns:
            dynamicCall 반환값

        Raises:
            ProviderDeadError: COM 객체가 해제된 상태에서 호출 시
            KiwoomApiError: dynamicCall 자체가 예외를 발생시킬 때
        """
        if not self._alive:
            raise ProviderDeadError(
                f"provider dead — {method_sig} 호출 차단"
            )
        try:
            return self._k.dynamicCall(method_sig, list(args))
        except RuntimeError as e:
            # QAxWidget C++ 객체 삭제 후 접근 → provider dead 확정
            if "deleted" in str(e).lower() or "C/C++ object" in str(e):
                self._alive = False
                logger.critical(
                    "[ProviderDead] QAxWidget 삭제 감지 — alive=False: %s", e,
                )
                raise ProviderDeadError(
                    f"QAxWidget deleted: {method_sig}"
                ) from e
            ctx = f" [{context}]" if context else ""
            logger.error(
                "[dynamicCall FAIL%s] %s args=%s — %s: %s",
                ctx, method_sig, args, type(e).__name__, e,
            )
            raise KiwoomApiError(
                f"dynamicCall 실패: {method_sig} args={args}{ctx}"
            ) from e
        except Exception as e:
            ctx = f" [{context}]" if context else ""
            logger.error(
                "[dynamicCall FAIL%s] %s args=%s — %s: %s",
                ctx, method_sig, args, type(e).__name__, e,
            )
            raise KiwoomApiError(
                f"dynamicCall 실패: {method_sig} args={args}{ctx}"
            ) from e

    # ── shutdown (v7.4) ──────────────────────────────────────────────────────

    def shutdown(self) -> None:
        """
        안전한 종료 순서:
        1) _shutting_down=True → 콜백 진입 차단
        2) 실시간 데이터 해제 (SetRealRemove)
        3) signal disconnect
        4) _alive=False → 이후 _call() 완전 차단
        """
        if self._shutting_down or not self._alive:
            return
        self._shutting_down = True
        logger.info("[KiwoomProvider] shutdown 시작")

        # 실시간 해제 (에러 무시)
        try:
            self._k.dynamicCall(
                "SetRealRemove(QString,QString)", self.SCREEN_REAL, "ALL",
            )
        except Exception:
            pass

        # signal disconnect (에러 무시)
        for sig_name in ("OnReceiveRealData", "OnReceiveTrData",
                         "OnReceiveMsg", "OnReceiveChejanData"):
            try:
                getattr(self._k, sig_name).disconnect()
            except (TypeError, RuntimeError):
                pass

        self._alive = False
        self._real_data_callback = None
        logger.info("[KiwoomProvider] shutdown 완료 — alive=False")

    @property
    def alive(self) -> bool:
        return self._alive

    # ── 연결 상태 체크 ──────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        """Kiwoom API 연결 상태 확인. 0=미연결, 1=연결."""
        try:
            state = self._k.dynamicCall("GetConnectState()")
            return state == 1
        except Exception:
            return False

    def ensure_connected(self) -> bool:
        """
        연결 끊김 감지 시 재접속 시도 (최대 1회).
        반환: True=연결 정상, False=재접속 실패
        """
        if self.is_connected():
            return True

        logger.warning("[KiwoomProvider] *** 연결 끊김 감지 — 재접속 시도 ***")
        try:
            ret = self._k.dynamicCall("CommConnect()")
            if ret != 0:
                logger.error("[KiwoomProvider] CommConnect 실패 (ret=%d)", ret)
                return False

            # 재접속 대기 (로그인 팝업 없이 자동 재접속되는 경우)
            loop = QEventLoop()
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(loop.quit)
            timer.start(15000)   # 최대 15초 대기
            loop.exec_()

            connected = self.is_connected()
            if connected:
                logger.info("[KiwoomProvider] 재접속 성공")
            else:
                logger.error("[KiwoomProvider] 재접속 실패 — 15초 타임아웃")
            return connected

        except Exception as e:
            logger.error("[KiwoomProvider] 재접속 예외: %s", e)
            return False

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
            self._call("SetInputValue(QString,QString)", "업종코드", 업종코드)
            self._call("SetInputValue(QString,QString)", "기준일자", today)
            self._call("SetInputValue(QString,QString)", "수정주가구분", "1")

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
        raw   = self._call("GetCodeListByMarket(QString)", mcode)
        return [c for c in str(raw).strip().split(";") if c]

    def get_stock_ohlcv(self, code: str, days: int) -> pd.DataFrame:
        """종목 일봉 OHLCV (opt10081 주식일봉차트조회)."""
        today = datetime.today().strftime("%Y%m%d")

        def _setup():
            self._call("SetInputValue(QString,QString)", "종목코드", code)
            self._call("SetInputValue(QString,QString)", "기준일자", today)
            self._call("SetInputValue(QString,QString)", "수정주가구분", "1")

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

        name_raw   = self._call("GetMasterCodeName(QString)", code)
        listed_raw = self._call("GetMasterListedStockCnt(QString)", code)
        price_raw  = self._call("GetMasterLastPrice(QString)", code)

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
        price = self._call("GetMasterLastPrice(QString)", code)
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

    # ── 실시간 데이터 (SetRealReg / OnReceiveRealData) ──────────────────────

    SCREEN_REAL = "8001"

    def register_real(self, codes: List[str], fids: str = "228") -> None:
        """SetRealReg: 실시간 데이터 수신 등록."""
        if not codes:
            return
        code_str = ";".join(codes)
        self._call(
            "SetRealReg(QString,QString,QString,QString)",
            self.SCREEN_REAL, code_str, fids, "0",
        )
        # v7.9: 실시간 피드 헬스체크 — 종목별 마지막 수신 시각 추적
        import time as _time
        self._real_last_recv: dict = getattr(self, '_real_last_recv', {})
        for c in codes:
            self._real_last_recv[c] = _time.monotonic()
        self._real_registered_codes = list(codes)
        self._real_registered_fids = fids
        logger.info("[RealData] SetRealReg %d codes, FIDs=%s", len(codes), fids)

    def unregister_real(self) -> None:
        """SetRealRemove: 실시간 데이터 수신 해제."""
        self._call(
            "SetRealRemove(QString,QString)", self.SCREEN_REAL, "ALL",
        )
        logger.info("[RealData] SetRealRemove screen=%s", self.SCREEN_REAL)

    def set_real_data_callback(self, callback) -> None:
        """외부 콜백 등록 (TickAnalyzer.on_tick_strength 등)."""
        self._real_data_callback = callback

    def _on_real_data(self, code: str, real_type: str, real_data: str) -> None:
        """OnReceiveRealData 이벤트 핸들러."""
        if not self._alive or self._shutting_down:
            return
        if self._real_data_callback is None:
            return
        code = code.strip()
        # v7.9: 실시간 피드 수신 시각 갱신
        import time as _time
        if hasattr(self, '_real_last_recv'):
            self._real_last_recv[code] = _time.monotonic()
        try:
            value_str = self._call(
                "GetCommRealData(QString,int)", code, 228,
            )
            value = float(str(value_str).strip())
            self._real_data_callback(code, value)
        except (ValueError, TypeError):
            pass
        except (ProviderDeadError, RuntimeError, KiwoomApiError):
            self._real_data_callback = None
            logger.warning("[RealData] provider dead — 콜백 비활성화")

    def check_real_feed_health(self, stale_sec: float = 300.0) -> List[str]:
        """v7.9: 실시간 피드 헬스체크 — stale_sec 이상 미수신 종목 반환."""
        import time as _time
        if not hasattr(self, '_real_last_recv'):
            return []
        now = _time.monotonic()
        stale_codes = []
        for code, last_ts in self._real_last_recv.items():
            if now - last_ts > stale_sec:
                stale_codes.append(code)
        return stale_codes

    def re_register_real(self, codes: List[str] = None) -> None:
        """v7.9: 실시간 피드 재등록 (stale 종목 복구용)."""
        target = codes or getattr(self, '_real_registered_codes', [])
        fids = getattr(self, '_real_registered_fids', '228')
        if target:
            self.register_real(target, fids)
            logger.info("[RealData] 재등록: %d종목", len(target))

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
        trcode별 분기: OHLCV / opw00018(계좌잔고) 등.
        """
        if not self._alive or self._shutting_down:
            return
        if self._timed_out:
            return

        # 현재 요청한 TR이 아닌 응답은 무시 (이전 TR 지연 응답 방어)
        # NOTE: rqname은 cp949 인코딩 깨짐 가능 → trcode만 비교
        if trcode != self._current_trcode:
            logger.debug(
                "[TR 무시] 예상(%s) 수신(%s/%s)",
                self._current_trcode, rqname, trcode,
            )
            return

        self._prev_next = prev_next

        if trcode == "opw00018":
            self._parse_opw00018(trcode, rqname)
        else:
            self._parse_ohlcv(trcode, rqname)

        self._loop.quit()

    def _parse_ohlcv(self, trcode: str, rqname: str) -> None:
        """opt10081 / opt20006 OHLCV 파싱."""
        rows: List[List] = []
        i = 0
        _get = lambda idx, field: self._call(
            "GetCommData(QString,QString,int,QString)", trcode, rqname, idx, field,
        )
        while True:
            date = str(_get(i, "일자")).strip()
            if not date:
                break

            open_  = _get(i, "시가")
            high_  = _get(i, "고가")
            low_   = _get(i, "저가")
            close_ = _get(i, "현재가")
            vol_   = _get(i, "거래량")

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

    def _parse_opw00018(self, trcode: str, rqname: str) -> None:
        """opw00018 계좌평가잔고내역 파싱 (single + multi)."""
        _get = lambda idx, field: self._call(
            "GetCommData(QString,QString,int,QString)", trcode, rqname, idx, field,
        )
        # ── Single output: 계좌 요약 정보 ────────────────────────────────
        for field in ["총매입금액", "총평가금액", "추정예탁자산", "총평가손익금액"]:
            self._single_data[field] = str(_get(0, field)).strip()

        # ── Multi output: 보유 종목 ──────────────────────────────────────
        rows: List[List] = []
        i = 0
        while True:
            code = str(_get(i, "종목번호")).strip().lstrip("A")
            if not code:
                break

            name     = self._decode_kiwoom_str(_get(i, "종목명"))
            qty      = str(_get(i, "보유수량")).strip()
            avg_cost = str(_get(i, "매입가")).strip()
            cur_price= str(_get(i, "현재가")).strip()
            pnl      = str(_get(i, "평가손익")).strip()

            rows.append([code, name, qty, avg_cost, cur_price, pnl])
            i += 1

        self._data.extend(rows)

    def _on_timeout(self):
        self._timed_out = True
        self._loop.quit()

    @staticmethod
    def _decode_kiwoom_str(s) -> str:
        """Kiwoom COM CP949 -> Latin-1 garbled string recovery.

        v7.9: 다중 폴백 — latin-1→cp949, latin-1→euc-kr, 원본 반환.
        디코드 실패 시 원본 그대로 반환 (로깅은 호출부에서).
        """
        s = str(s).strip()
        if not s or s.isascii():
            return s
        # 1차: latin-1 → cp949 (가장 흔한 COM 인코딩 깨짐 패턴)
        try:
            return s.encode("latin-1").decode("cp949")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        # 2차: latin-1 → euc-kr (일부 구버전 Kiwoom)
        try:
            return s.encode("latin-1").decode("euc-kr")
        except (UnicodeDecodeError, UnicodeEncodeError):
            pass
        # 3차: 이미 올바른 한글이거나 복구 불가 → 원본 반환
        return s

    def _on_msg(self, screen_no, rqname, trcode, msg):
        """
        OnReceiveMsg 핸들러.
        서버가 TR을 거부하거나 에러 메시지를 보낼 때 호출됨.
        루프를 즉시 종료해 20초 타임아웃 대신 즉각 에러 확인.
        """
        msg = str(msg).strip()
        rqname_d = self._decode_kiwoom_str(rqname)
        msg_d    = self._decode_kiwoom_str(msg)
        logger.warning("[OnReceiveMsg] screen=%s rq=%s tr=%s msg=%s", screen_no, rqname_d, trcode, msg_d)

        # 주문 화면 메시지 처리
        # NOTE: msg 코드([100000] 등)는 ASCII이므로 원본 msg에서 판별
        if str(screen_no) == "7001":
            # [100000] = 주문완료(성공) → chejan 체결 대기로 넘김 (거부 아님!)
            if "[100000]" in msg:
                logger.info("[OnReceiveMsg] 주문 접수 성공: %s", msg_d)
                return   # chejan 체결 이벤트에서 처리
            # 그 외 ([800033], [RC4025] 등) = 에러 → 즉시 거부
            if self._order_result is None:
                self._order_result = {
                    "order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"order rejected: {msg_d}",
                }
                if self._order_loop.isRunning():
                    self._order_loop.quit()
            return

        # 현재 대기 중인 TR에 대한 메시지면 루프 종료
        # rqname은 인코딩 깨질 수 있으므로 trcode만 비교
        trcode_clean = str(trcode).strip().lower()
        current_clean = str(self._current_trcode).strip().lower()
        logger.debug("[_on_msg] trcode='%s' current='%s' match=%s loop_running=%s",
                     trcode_clean, current_clean, trcode_clean == current_clean,
                     self._loop.isRunning())

        # [100000] = 조회/주문 성공 → OnReceiveTrData에서 데이터 수신 예정이므로 무시
        if "[100000]" in msg:
            logger.info("[OnReceiveMsg] 조회 성공: %s (tr=%s)", msg_d, trcode)
            return

        if trcode_clean == current_clean and current_clean:
            # [571578] = 모의투자 해당조회내역이 없습니다 → 빈 결과 (에러 아님)
            # 포지션 0개일 때 opw00018에서 정상적으로 반환되는 메시지
            if "[571578]" in msg:
                logger.info("[OnReceiveMsg] 조회 결과 없음 (빈 계좌): %s (tr=%s)", msg_d, trcode)
                self._msg_rejected = True   # 빈 결과로 처리 (재시도 불필요)
                if self._loop.isRunning():
                    self._loop.quit()
                return

            # 서버 에러 메시지 → 즉시 종료 (20초 타임아웃 방지)
            self._msg_rejected = True
            if self._loop.isRunning():
                self._loop.quit()

    # ── 주문 상태 관리 ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_order_state(**kwargs) -> Dict:
        """주문 상태 구조체 생성. 모든 주문 관련 정보를 단일 dict로 관리."""
        state = {
            "order_no":       "",
            "code":           "",
            "side":           "",
            "requested_qty":  0,
            "filled_qty":     0,
            "avg_fill_price": 0.0,
            "status":         "IDLE",
            # IDLE / REQUESTED / ACCEPTED / PARTIAL / FILLED / REJECTED / TIMEOUT_PENDING / GHOST_FILLED
            "timestamp":      None,
        }
        state.update(kwargs)
        return state

    def get_ghost_orders(self) -> List[Dict]:
        """timeout 후 미확인 주문 목록. RuntimeEngine/EOD에서 경고 출력용."""
        return [g for g in self._ghost_orders
                if g["status"] in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN", "GHOST_FILLED")]

    def clear_ghost_orders(self) -> None:
        """EOD 처리 후 ghost 목록 초기화."""
        self._ghost_orders.clear()

    # ── 계좌 조회 + Reconciliation ──────────────────────────────────────────────

    def query_account_holdings(self) -> List[Dict]:
        """
        Kiwoom 계좌 보유 종목 조회 (opw00018).
        Returns: [{"code", "name", "qty", "avg_price", "cur_price", "pnl"}, ...]

        실패 시 연속 실패 카운터 누적:
          1회: WARNING / 3회: ERROR / 5회+: RECON_STALE 플래그
        """
        account = self.get_account_no()
        if not account:
            logger.error("[Reconcile] 계좌번호 조회 실패")
            self._record_recon_failure("계좌번호 조회 실패")
            return []

        def _setup():
            self._call("SetInputValue(QString,QString)", "계좌번호", account)
            self._call("SetInputValue(QString,QString)", "비밀번호", "")
            self._call("SetInputValue(QString,QString)", "비밀번호입력매체구분", "00")
            self._call("SetInputValue(QString,QString)", "조회구분", "1")

        try:
            rows = self._request_tr_with_retry(
                trcode="opw00018",
                rqname="계좌평가잔고내역",
                days=9999,
                setup_func=_setup,
            )
        except ProviderDeadError:
            logger.critical("[Reconcile] provider dead — opw00018 요청 abort")
            self._record_recon_failure("ProviderDeadError")
            return []
        except (KiwoomApiError, TrTimeoutError, RuntimeError) as e:
            logger.error("[Reconcile] opw00018 조회 실패: %s", e)
            self._record_recon_failure(str(e))
            return []

        if not rows:
            # BUG-4 FIX: 보유 종목 0개는 정상 상태 — 실패로 기록하지 않음
            # (타임아웃/거부는 예외 경로에서 이미 처리됨)
            logger.info("[Reconcile] opw00018 응답: 보유 종목 없음 (정상)")
            self._recon_consecutive_fail = 0
            self._recon_last_success = datetime.now()
            if self._recon_stale:
                logger.info("[Reconcile] RECON_STALE 해제 — 빈 포지션 정상 응답")
                self._recon_stale = False
            return []

        # 성공 → 카운터 리셋
        self._recon_consecutive_fail = 0
        self._recon_last_success = datetime.now()
        if self._recon_stale:
            logger.info("[Reconcile] RECON_STALE 해제 — opw00018 응답 정상 복구")
            self._recon_stale = False

        holdings = []
        for row in rows:
            code, name, qty_s, avg_s, cur_s, pnl_s = row
            qty       = abs(self._to_int(qty_s))
            avg_price = abs(self._to_int(avg_s))
            cur_price = abs(self._to_int(cur_s))
            pnl       = self._to_int(pnl_s)

            if qty > 0:
                holdings.append({
                    "code":      code,
                    "name":      name,
                    "qty":       qty,
                    "avg_price": avg_price,
                    "cur_price": cur_price,
                    "pnl":       pnl,
                })

        logger.info("[Reconcile] Kiwoom 보유종목 %d개 조회 완료", len(holdings))
        return holdings

    def _record_recon_failure(self, reason: str) -> None:
        """Reconcile 실패 누적 기록. 연속 실패 수준별 경고."""
        self._recon_consecutive_fail += 1
        n = self._recon_consecutive_fail

        last_ok = (
            self._recon_last_success.strftime("%H:%M:%S")
            if self._recon_last_success else "없음"
        )

        if n < 3:
            logger.warning(
                "[Reconcile] opw00018 실패 (%d회 연속, 마지막 성공=%s): %s",
                n, last_ok, reason,
            )
        elif n < 5:
            logger.error(
                "[Reconcile] opw00018 연속 %d회 실패! 마지막 성공=%s: %s",
                n, last_ok, reason,
            )
        else:
            if not self._recon_stale:
                self._recon_stale = True
                logger.critical(
                    "[Reconcile] RECON_STALE — opw00018 연속 %d회 실패! "
                    "계좌-엔진 동기화 신뢰 불가. 마지막 성공=%s. "
                    "신규 매수 차단 권고!",
                    n, last_ok,
                )
            else:
                logger.error(
                    "[Reconcile] RECON_STALE 지속 (%d회 연속 실패)", n,
                )

    def query_account_summary(self) -> Dict:
        """
        Kiwoom 계좌 요약 조회 (opw00018 single output).

        Returns: {
            "추정예탁자산": int,     # 총 계좌 가치 (현금 + 포지션 시가)
            "총매입금액":   int,     # 포지션 총 매입가
            "총평가금액":   int,     # 포지션 총 시가
            "총평가손익금액": int,   # 미실현 손익
            "holdings":     [...],  # query_account_holdings() 결과
            "available_cash": int,  # 추정 가용 현금 = 추정예탁자산 - 총평가금액
            "error":         str,
        }
        """
        account = self.get_account_no()
        if not account:
            return {"error": "계좌번호 조회 실패", "holdings": []}

        self._single_data = {}  # 초기화

        def _setup():
            self._call("SetInputValue(QString,QString)", "계좌번호", account)
            self._call("SetInputValue(QString,QString)", "비밀번호", "")
            self._call("SetInputValue(QString,QString)", "비밀번호입력매체구분", "00")
            self._call("SetInputValue(QString,QString)", "조회구분", "1")

        try:
            rows = self._request_tr_with_retry(
                trcode="opw00018",
                rqname="계좌평가잔고내역",
                days=9999,
                setup_func=_setup,
            )
        except ProviderDeadError:
            logger.critical("[AccountSync] provider dead — opw00018 요청 abort")
            return {"error": "ProviderDeadError", "holdings": []}
        except (KiwoomApiError, TrTimeoutError, RuntimeError) as e:
            logger.error("[AccountSync] opw00018 조회 실패: %s", e)
            return {"error": str(e), "holdings": []}

        # BUG-5 v2: _msg_rejected=True면 서버가 보유종목 multi-row를 거부한 것.
        # single_data(예탁자산 등)는 유효할 수 있으므로, 예탁자산은 반환하되
        # holdings_reliable=False 플래그로 caller가 교차검증하도록 한다.
        if self._msg_rejected and not rows:
            deposit = abs(self._to_int(self._single_data.get("추정예탁자산", "0")))
            avail   = deposit  # holdings 0이므로 전액 현금
            logger.warning(
                "[AccountSync] opw00018 서버 거부 (msg_rejected) — "
                "예탁자산=%s, holdings 목록 신뢰 불가", f"{deposit:,}",
            )
            return {
                "추정예탁자산": deposit, "총매입금액": 0, "총평가금액": 0,
                "총평가손익금액": 0, "holdings": [], "available_cash": avail,
                "error": "",
                "holdings_reliable": False,
            }

        # single data 파싱
        # [571578] 조회내역 없음: rows=[], _single_data={} → 빈 계좌 정상 응답
        if not rows and not self._single_data:
            logger.info("[AccountSync] 빈 계좌 (포지션 0, single_data 없음) — 정상 빈 결과 반환")
            return {
                "추정예탁자산": 0, "총매입금액": 0, "총평가금액": 0,
                "총평가손익금액": 0, "holdings": [], "available_cash": 0,
                "error": "empty_account",
            }

        deposit    = abs(self._to_int(self._single_data.get("추정예탁자산", "0")))
        total_buy  = abs(self._to_int(self._single_data.get("총매입금액", "0")))
        total_eval = abs(self._to_int(self._single_data.get("총평가금액", "0")))
        total_pnl  = self._to_int(self._single_data.get("총평가손익금액", "0"))

        # holdings 파싱
        holdings = []
        for row in (rows or []):
            code, name, qty_s, avg_s, cur_s, pnl_s = row
            qty       = abs(self._to_int(qty_s))
            avg_price = abs(self._to_int(avg_s))
            cur_price = abs(self._to_int(cur_s))
            pnl       = self._to_int(pnl_s)
            if qty > 0:
                holdings.append({
                    "code": code, "name": name,
                    "qty": qty, "avg_price": avg_price,
                    "cur_price": cur_price, "pnl": pnl,
                })

        # 가용 현금 추정: 추정예탁자산 - 보유종목 시가 합계
        holdings_mkt_val = sum(h["cur_price"] * h["qty"] for h in holdings)
        available_cash = deposit - holdings_mkt_val if deposit > 0 else 0

        logger.info(
            "[AccountSync] 추정예탁자산=%s, 총평가금액=%s, 가용현금=%s, 보유종목=%d개",
            f"{deposit:,}", f"{total_eval:,}", f"{available_cash:,}", len(holdings),
        )

        return {
            "추정예탁자산":    deposit,
            "총매입금액":      total_buy,
            "총평가금액":      total_eval,
            "총평가손익금액":  total_pnl,
            "holdings":       holdings,
            "available_cash": available_cash,
            "error":          "",
            "holdings_reliable": True,
        }

    @property
    def is_recon_stale(self) -> bool:
        """Reconcile 연속 실패 5회 이상이면 True. RuntimeEngine에서 신규 진입 제한에 활용."""
        return self._recon_stale

    @property
    def recon_status(self) -> Dict:
        """Reconcile 상태 요약 (리포트용)."""
        return {
            "consecutive_fail": self._recon_consecutive_fail,
            "last_success": (
                self._recon_last_success.isoformat()
                if self._recon_last_success else None
            ),
            "stale": self._recon_stale,
        }

    def query_sellable_qty(self, code: str) -> Dict:
        """
        v7.6: 종목별 매도가능수량 조회 (opw00018 기반).
        계좌 전체 holdings를 조회하여 해당 종목의 매도가능수량을 반환.

        Returns: {
            "code": str,
            "hold_qty": int,       # 브로커 보유 수량
            "sellable_qty": int,   # 매도 가능 수량 (T+2 반영)
            "source": "opw00018",
            "error": str,          # 오류 시 메시지
        }
        """
        summary = self.query_account_summary()
        if summary.get("error") and summary["error"] not in ("", "empty_account"):
            return {"code": code, "hold_qty": -1, "sellable_qty": -1,
                    "source": "opw00018", "error": summary["error"]}

        if not summary.get("holdings_reliable", True):
            return {"code": code, "hold_qty": -1, "sellable_qty": -1,
                    "source": "opw00018", "error": "holdings_not_reliable"}

        for h in summary.get("holdings", []):
            if h["code"] == code:
                # v7.9: opw00018은 매도가능수량을 직접 제공하지 않음
                # hold_qty는 확정, sellable_qty는 미확인(-1)으로 반환
                # caller가 fallback 정책 결정
                return {"code": code, "hold_qty": h["qty"],
                        "sellable_qty": -1,
                        "sellable_source": "UNKNOWN_SELLABLE",
                        "source": "opw00018", "error": ""}

        # 해당 종목이 holdings에 없음
        return {"code": code, "hold_qty": 0, "sellable_qty": 0,
                "sellable_source": "BROKER_CONFIRMED_ZERO",
                "source": "opw00018", "error": ""}

    def reconcile(self, portfolio, auto_absorb_ghost: bool = True) -> Dict:
        """
        내부 포트폴리오와 실계좌 비교. 불일치 감지 및 ghost order 해소.

        auto_absorb_ghost: True이면 ghost fill 확인된 포지션을 자동으로
            내부 포트에 반영. False이면 감지만 하고 수동 확인 요구.

        Returns: {
            "match": bool,
            "kiwoom_only": [...],    # 계좌에만 있는 종목 (ghost fill 가능)
            "engine_only": [...],    # 엔진에만 있는 종목 (phantom position)
            "qty_mismatch": [...],   # 수량 불일치
            "ghost_resolved": [...], # ghost order 중 계좌 확인된 건
        }
        """
        holdings = self.query_account_holdings()

        # v7.9: 빈 계좌와 조회 실패를 구분
        # query_account_holdings()는 성공 시 [] 반환 + _recon_consecutive_fail=0
        # 실패 시 [] 반환 + _recon_consecutive_fail 증가
        if not holdings:
            if self._recon_consecutive_fail > 0:
                # 조회 실패 — 스킵 (이전 동작 유지)
                logger.warning(
                    "[Reconcile] 계좌 조회 실패 — reconciliation 스킵 "
                    "(연속 실패 %d회, stale=%s)",
                    self._recon_consecutive_fail, self._recon_stale,
                )
                return {"match": not self._recon_stale, "kiwoom_only": [], "engine_only": [],
                        "qty_mismatch": [], "ghost_resolved": [],
                        "recon_stale": self._recon_stale}
            # 빈 계좌 정상 응답 — engine_only 검출 진행
            # holdings=[] 이므로 kiwoom_map={}, engine_only에 전부 잡힘
            logger.info("[Reconcile] 빈 계좌 정상 — engine_only 검출 진행")

        # Kiwoom 보유 → dict
        kiwoom_map = {h["code"]: h for h in holdings}
        # 내부 포트 → dict
        engine_map = {}
        for code, pos in portfolio.positions.items():
            engine_map[code] = {
                "code": code,
                "qty":  pos.quantity,
                "avg_price": pos.avg_price,
            }

        kiwoom_codes = set(kiwoom_map.keys())
        engine_codes = set(engine_map.keys())

        # ── 비교 ──────────────────────────────────────────────────────
        kiwoom_only   = []  # 계좌에만 있음
        engine_only   = []  # 엔진에만 있음
        qty_mismatch  = []  # 수량 불일치
        ghost_resolved = []

        # 계좌에만 있는 종목 (ghost fill 가능)
        for code in kiwoom_codes - engine_codes:
            h = kiwoom_map[code]
            kiwoom_only.append(h)
            logger.warning(
                "[Reconcile] KIWOOM_ONLY %s(%s) %d주 @ %,d원 — 엔진에 없음!",
                h["name"], code, h["qty"], h["avg_price"],
            )

        # 엔진에만 있는 종목 (phantom position)
        for code in engine_codes - kiwoom_codes:
            e = engine_map[code]
            engine_only.append(e)
            logger.warning(
                "[Reconcile] ENGINE_ONLY %s %d주 @ %,.0f원 — 계좌에 없음!",
                code, e["qty"], e["avg_price"],
            )

        # 양쪽에 있지만 수량 불일치
        for code in kiwoom_codes & engine_codes:
            h = kiwoom_map[code]
            e = engine_map[code]
            if h["qty"] != e["qty"]:
                qty_mismatch.append({
                    "code": code,
                    "name": h["name"],
                    "kiwoom_qty": h["qty"],
                    "engine_qty": e["qty"],
                    "diff": h["qty"] - e["qty"],
                })
                logger.warning(
                    "[Reconcile] QTY_MISMATCH %s(%s) Kiwoom=%d vs Engine=%d (diff=%+d)",
                    h["name"], code, h["qty"], e["qty"], h["qty"] - e["qty"],
                )

        # ── Ghost order 해소 ───────────────────────────────────────────
        for ghost in self._ghost_orders:
            if ghost["status"] not in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN"):
                continue
            ghost_code = ghost["code"]
            if ghost_code in kiwoom_map:
                h = kiwoom_map[ghost_code]
                ghost["status"]         = "GHOST_FILLED"
                ghost["filled_qty"]     = h["qty"]
                ghost["avg_fill_price"] = h["avg_price"]
                ghost_resolved.append({
                    "code": ghost_code, "side": ghost["side"],
                    "qty": h["qty"], "avg_price": h["avg_price"],
                })
                # v7.4: ghost fill 자동 흡수
                if auto_absorb_ghost and ghost_code not in engine_codes:
                    sector = self._sector_map.get(ghost_code, "기타")
                    portfolio.update_position(
                        ghost_code, sector, h["qty"], h["avg_price"], ghost["side"],
                    )
                    logger.critical(
                        "[Reconcile] GHOST ABSORBED %s %s %d주 @ %,d원 — "
                        "내부 포트에 자동 반영 완료",
                        ghost["side"], ghost_code, h["qty"], h["avg_price"],
                    )
                else:
                    logger.critical(
                        "[Reconcile] GHOST RESOLVED %s %s %d주 @ %,d원 — "
                        "계좌에서 확인됨. 포트 수동 동기화 필요!",
                        ghost["side"], ghost_code, h["qty"], h["avg_price"],
                    )

        is_match = not (kiwoom_only or engine_only or qty_mismatch)

        if is_match:
            logger.info("[Reconcile] OK — 내부 포트 = 실계좌 (%d종목 일치)", len(engine_codes))
        else:
            logger.warning(
                "[Reconcile] MISMATCH — kiwoom_only=%d, engine_only=%d, qty_diff=%d, ghost=%d",
                len(kiwoom_only), len(engine_only), len(qty_mismatch), len(ghost_resolved),
            )

        return {
            "match":          is_match,
            "kiwoom_only":    kiwoom_only,
            "engine_only":    engine_only,
            "qty_mismatch":   qty_mismatch,
            "ghost_resolved": ghost_resolved,
        }

    # ── 주문 실행 (SendOrder) ─────────────────────────────────────────────────

    def get_account_no(self) -> str:
        """첫 번째 계좌번호 반환."""
        raw = self._call("GetLoginInfo(QString)", "ACCNO")
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

        timeout 시 GHOST ORDER 등록 — 실계좌에서 체결됐을 수 있으므로
        즉시 REJECTED 처리하지 않고 TIMEOUT_PENDING 상태로 추적.
        """
        ORDER_TIMEOUT_SEC = 30

        account = self.get_account_no()
        if not account:
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": "계좌번호 조회 실패"}

        order_type = 1 if side == "BUY" else 2   # 1=신규매수, 2=신규매도
        rqname     = f"{'매수' if side == 'BUY' else '매도'}_{code}"
        screen     = "7001"

        # 주문 상태 초기화
        self._order_state = self._make_order_state(
            code=code, side=side, requested_qty=quantity,
            status="REQUESTED", timestamp=datetime.now(),
        )
        self._order_result = None

        time.sleep(0.2)  # 주문 간 최소 간격

        ret = self._call(
            "SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)",
            rqname, screen, account, order_type, code, quantity, int(price), hoga_type, "",
            context=f"SendOrder/{side}/{code}/{quantity}주",
        )

        if ret != 0:
            self._order_state["status"] = "REJECTED"
            logger.error("[SendOrder] %s %s %d주 실패 ret=%d", side, code, quantity, ret)
            return {"order_no": "", "exec_price": 0.0, "exec_qty": 0,
                    "error": f"SendOrder ret={ret}"}

        logger.info("[SendOrder] %s %s %d주 접수 (시장가, screen=%s)", side, code, quantity, screen)

        # 체결 대기
        self._order_timer.start(ORDER_TIMEOUT_SEC * 1000)
        self._order_loop.exec_()
        self._order_timer.stop()

        if self._order_result is None:
            # ── TIMEOUT_UNCERTAIN — 체결 미확인 (실패로 확정하지 않음) ──
            self._order_state["status"] = "TIMEOUT_UNCERTAIN"
            ghost = self._order_state.copy()
            self._ghost_orders.append(ghost)

            logger.critical(
                "[TIMEOUT_UNCERTAIN] %s %s %d주 — 체결 미확인 (timeout %ds). "
                "실계좌에서 체결됐을 수 있음! HTS 확인 필요. order_no=%s",
                side, code, quantity, ORDER_TIMEOUT_SEC,
                self._order_state["order_no"],
            )
            return {"order_no": self._order_state["order_no"],
                    "exec_price": 0.0, "exec_qty": 0,
                    "error": f"TIMEOUT_UNCERTAIN — {ORDER_TIMEOUT_SEC}초 체결 미확인"}

        # ── 정상 체결 — 상태 확정 ──────────────────────────────────────
        self._order_state["status"]         = "FILLED"
        self._order_state["order_no"]       = self._order_result["order_no"]
        self._order_state["filled_qty"]     = self._order_result["exec_qty"]
        self._order_state["avg_fill_price"] = self._order_result["exec_price"]

        logger.info(
            "[OrderState] %s %s FILLED %d/%d주 @ %,.0f원 (order_no=%s)",
            side, code,
            self._order_state["filled_qty"],
            self._order_state["requested_qty"],
            self._order_state["avg_fill_price"],
            self._order_state["order_no"],
        )
        return self._order_result

    def _on_chejan_data(self, gubun, item_cnt, fid_list):
        """
        OnReceiveChejanData 이벤트 핸들러.
        gubun: "0" = 주문체결통보, "1" = 잔고통보

        매칭 로직 (v7 보강):
          1) 현재 활성 주문 매칭 (주문번호 → 종목코드 폴백)
          2) Ghost order 매칭 (timeout 후 지연 체결 감지)
          3) FILLED 이후 동일 주문번호 중복 이벤트 → DEBUG (정상)
          4) 주문번호가 다른데 동일 종목 충돌 → WARNING
          5) 기타 미매칭 → INFO
        """
        if not self._alive or self._shutting_down:
            return
        if str(gubun) != "0":
            return

        code = str(self._call("GetChejanData(int)", 9001)).strip()
        code = code.lstrip("A")  # 'A005930' → '005930'
        order_no       = str(self._call("GetChejanData(int)", 9203)).strip()
        exec_qty_raw   = str(self._call("GetChejanData(int)", 911)).strip()
        exec_price_raw = str(self._call("GetChejanData(int)", 910)).strip()
        _raw_status    = str(self._call("GetChejanData(int)", 913)).strip()
        order_status   = self._decode_kiwoom_str(_raw_status)

        # v7.9: 디코딩 실패 시 FID 값 기반 폴백
        if order_status and not order_status.isascii() and order_status == _raw_status:
            # 디코딩 실패 — exec_qty > 0 이면 "체결"로 추정
            if exec_qty_raw and abs(int(exec_qty_raw)) > 0 and exec_price_raw and abs(float(exec_price_raw)) > 0:
                order_status = "체결"
            else:
                order_status = "접수"
            logger.debug("[Chejan] FID 913 디코딩 실패 (raw=%r) → 폴백: %s", _raw_status, order_status)

        exec_qty   = abs(int(exec_qty_raw))   if exec_qty_raw   else 0
        exec_price = abs(float(exec_price_raw)) if exec_price_raw else 0.0

        st = self._order_state  # 단축 참조

        # ── 1. 현재 활성 주문 매칭 ─────────────────────────────────────
        if st["status"] in ("REQUESTED", "ACCEPTED", "PARTIAL"):
            matched = False

            if st["order_no"] and order_no:
                # 1차: 주문번호 기반
                if order_no == st["order_no"]:
                    matched = True
                else:
                    logger.debug(
                        "[Chejan] 주문번호 불일치 (대기=%s, 수신=%s, code=%s) — "
                        "이전 주문의 지연 이벤트일 가능성 높음",
                        st["order_no"], order_no, code,
                    )
            elif code == st["code"]:
                # 2차: 종목코드 폴백
                matched = True
                if order_no and not st["order_no"]:
                    st["order_no"] = order_no
                    logger.info("[Chejan] 주문번호 캡처: %s (code=%s)", order_no, code)

            if matched:
                self._process_chejan_fill(code, order_no, exec_qty, exec_price, order_status)
                return

        # ── 2. FILLED 이후 동일 주문번호 중복 이벤트 → 정상 (DEBUG만) ──
        if st["status"] == "FILLED":
            if st["order_no"] and order_no == st["order_no"]:
                logger.debug(
                    "[Chejan] FILLED 후 중복 이벤트 무시 — order_no=%s, code=%s",
                    order_no, code,
                )
                return
            # FILLED 상태에서 동일 종목이지만 다른 주문번호 → INFO
            if code == st["code"]:
                logger.info(
                    "[Chejan] FILLED 후 동일 종목 이벤트 — "
                    "code=%s, 활성order=%s, 수신order=%s",
                    code, st["order_no"], order_no,
                )
                return

        # ── 3. Ghost order 매칭 (timeout 후 지연 체결) ─────────────────
        for ghost in self._ghost_orders:
            if ghost["status"] not in ("TIMEOUT_PENDING", "TIMEOUT_UNCERTAIN"):
                continue

            ghost_match = False
            if ghost.get("order_no") and order_no and order_no == ghost["order_no"]:
                ghost_match = True
            elif code == ghost["code"]:
                ghost_match = True

            if ghost_match and exec_qty > 0 and exec_price > 0:
                ghost["status"]         = "GHOST_FILLED"
                ghost["filled_qty"]     = exec_qty
                ghost["avg_fill_price"] = exec_price
                ghost["order_no"]       = order_no

                logger.critical(
                    "[GHOST FILL] %s %s %d주 @ %.0f원 (order_no=%s) — "
                    "timeout 후 지연 체결 감지! 내부 포트 미반영 상태. "
                    "HTS에서 수동 확인 필요!",
                    ghost["side"], code, exec_qty, exec_price, order_no,
                )
                return

        # ── 4. 완전 미매칭 — 주문번호가 다른데 동일 종목 충돌 ──
        #     v7.9: (code, order_no) 기준 최초 1회만 WARNING, 이후 전부 DEBUG
        if code == st["code"] and order_no != st.get("order_no", ""):
            _conflict_key = f"{code}_{order_no}"
            if not hasattr(self, '_chejan_conflict_logged'):
                self._chejan_conflict_logged = set()
            if _conflict_key not in self._chejan_conflict_logged:
                self._chejan_conflict_logged.add(_conflict_key)
                logger.warning(
                    "[Chejan] 동일 종목 다른 주문번호 충돌 — "
                    "활성(code=%s, order=%s, status=%s) / 수신(order=%s, status=%s)",
                    st["code"], st["order_no"], st["status"], order_no, order_status,
                )
            else:
                logger.debug(
                    "[Chejan] 동일 종목 충돌 반복 (억제) — code=%s, order=%s",
                    code, order_no,
                )
            return

        # ── 5. 기타 미매칭 → INFO (이전 주문의 지연 이벤트일 가능성) ────
        logger.info(
            "[Chejan] 미매칭 이벤트 — 활성(code=%s,status=%s) / 수신(code=%s,order=%s)",
            st["code"], st["status"], code, order_no,
        )

    def _process_chejan_fill(self, code: str, order_no: str,
                             exec_qty: int, exec_price: float, order_status: str) -> None:
        """활성 주문의 체결/부분체결/접수 처리."""
        st = self._order_state

        if exec_qty > 0 and exec_price > 0:
            if self._order_result is not None:
                # 부분체결: 누적
                prev_qty   = self._order_result["exec_qty"]
                prev_price = self._order_result["exec_price"]
                total_qty  = prev_qty + exec_qty
                avg_price  = (prev_price * prev_qty + exec_price * exec_qty) / total_qty
                self._order_result["exec_qty"]  = total_qty
                self._order_result["exec_price"] = avg_price

                st["status"]         = "PARTIAL"
                st["filled_qty"]     = total_qty
                st["avg_fill_price"] = avg_price

                remain = st["requested_qty"] - total_qty
                logger.info(
                    "[PARTIAL] %s %s filled=%d/%d remain=%d avg=%,.0f last_qty=%d last_price=%,.0f (order_no=%s)",
                    st["side"], code, total_qty, st["requested_qty"], remain,
                    avg_price, exec_qty, exec_price, order_no,
                )
            else:
                self._order_result = {
                    "order_no":   order_no,
                    "exec_price": exec_price,
                    "exec_qty":   exec_qty,
                    "error":      "",
                }
                st["status"]         = "PARTIAL" if exec_qty < st["requested_qty"] else "FILLED"
                st["filled_qty"]     = exec_qty
                st["avg_fill_price"] = exec_price
                st["order_no"]       = order_no

                logger.info(
                    "[FILL] %s %s %d주 @ %,.0f원 (order_no=%s)",
                    st["side"], code, exec_qty, exec_price, order_no,
                )

            # 전량 체결 → 루프 종료
            if self._order_result["exec_qty"] >= st["requested_qty"]:
                st["status"] = "FILLED"
                if self._order_loop.isRunning():
                    self._order_loop.quit()
            else:
                remain = st["requested_qty"] - self._order_result["exec_qty"]
                logger.info(
                    "[PARTIAL WAIT] %s %d/%d주 (잔여 %d주)",
                    code, self._order_result["exec_qty"], st["requested_qty"], remain,
                )

        elif order_status in ("접수", "확인"):
            st["status"] = "ACCEPTED"
            if order_no and not st["order_no"]:
                st["order_no"] = order_no
                logger.info("[ACCEPTED] %s order_no=%s (체결 대기 중)", code, order_no)

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

        - ProviderDeadError → 즉시 abort (retry 금지)
        - TR_MAX_RETRY 회까지 재시도
        - opt10081은 타임아웃 시 치명 오류 아님 (빈 리스트 반환)
        - 그 외 TR은 TrTimeoutError 발생
        """
        if not self._alive:
            raise ProviderDeadError(
                f"provider dead — {rqname}({trcode}) TR 요청 차단"
            )

        all_rows:  List[List] = []
        try_count: int        = 0

        screen_no = SCREEN_MAP.get(trcode, "9000")

        while try_count < TR_MAX_RETRY:
            try_count            += 1
            self._data            = []
            self._prev_next       = "0"
            self._timed_out       = False
            self._msg_rejected    = False
            self._current_rqname  = rqname
            self._current_trcode  = trcode

            time.sleep(TR_DELAY)   # 매 CommRqData 호출 전 반드시 대기

            try:
                setup_func()
            except ProviderDeadError:
                raise  # retry 금지, 즉시 상위로 전파

            try:
                ret = self._call(
                    "CommRqData(QString,QString,int,QString)",
                    rqname, trcode, 0, screen_no,
                    context=f"{rqname}/{trcode}/screen={screen_no}",
                )
            except ProviderDeadError:
                raise  # retry 금지, 즉시 상위로 전파
            logger.debug("[CommRqData] %s(%s) screen=%s ret=%s", rqname, trcode, screen_no, ret)

            if ret == -200:
                logger.warning(
                    "[CommRqData -200] 시세과부하 — 1초 대기 후 재시도 (%d/%d회)",
                    try_count, TR_MAX_RETRY,
                )
                time.sleep(1.0)
                if try_count >= TR_MAX_RETRY:
                    raise RateLimitError(
                        f"{rqname}/{trcode} 시세과부하 -200 (연속 {try_count}회)")
                continue

            if ret != 0:
                logger.error(
                    "[CommRqData 실패] %s(%s) ret=%s — 요청 거부됨, 재시도 중단.",
                    rqname, trcode, ret,
                )
                break

            # OnReceiveMsg가 CommRqData 직후 동기 호출될 수 있음
            # → loop 시작 전에 이미 거부됐으면 loop 진입 불필요
            if not self._msg_rejected:
                self._timer.start(int(TR_TIMEOUT_SEC * 1000))
                self._loop.exec_()
                self._timer.stop()

            if self._msg_rejected:
                logger.info("[TR 서버거부] %s(%s) — 빈 결과 (재시도 불필요)", rqname, trcode)
                break

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

            # opw00018: [571578] 조회내역 없음 → 포지션 0개 정상 응답
            if trcode == "opw00018" and self._msg_rejected:
                logger.info(
                    "[KiwoomProvider] %s(%s) 빈 계좌 (보유종목 없음) → 빈 결과 반환",
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
        except (ValueError, TypeError):
            return 0
