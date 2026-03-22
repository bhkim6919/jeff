"""
Q-TRON Gen3 main.py
====================

실행 모드:
  python main.py              → LIVE  모드 (Kiwoom 로그인, 09:00 ~ 15:30)
  python main.py --pykrx      → PYKRX 모드 (장외 테스트, 실데이터)
  python main.py --mock       → MOCK  모드 (API 없이 구조 확인)
  python main.py --batch      → BATCH 모드 (신호 생성, 18:00 ~ 20:30)

Gen3 핵심 개념:
  - BATCH: 매일 18:00 이후 유니버스 수집 + Q-Score 계산 → signals_YYYYMMDD.csv 생성
  - RUNTIME: 09:00 시작 시 signals.csv 로드 → 레짐 체크 → 진입/청산 실행
"""

import sys
import os
import io
import argparse
import traceback
import threading
import collections
from datetime import datetime, time as dtime

# Windows CP949 콘솔에서 유니코드 문자(—, ⚠ 등) 인코딩 오류 방지
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")


class _TeeWriter:
    """stdout/stderr → console + log file 동시 출력."""
    def __init__(self, original_buffer, log_file):
        # 원본 바이너리 버퍼(stdout.buffer)에 직접 쓰기 → 인코딩 문제 회피
        self._buf = original_buffer
        self._log = log_file
        self.encoding = "utf-8"

    def write(self, text):
        if not text:
            return
        raw = text.encode("utf-8", errors="replace")
        try:
            self._buf.write(raw)
            self._buf.flush()
        except Exception:
            pass
        try:
            self._log.write(text)
            self._log.flush()
        except Exception:
            pass

    def flush(self):
        try:
            self._buf.flush()
        except Exception:
            pass
        try:
            self._log.flush()
        except Exception:
            pass

    @property
    def buffer(self):
        return self._buf


# ── 글로벌 예외 훅 (Qt/Thread 경계 예외 캡처) ─────────────────────────────────
# 최근 예외 기록 링버퍼 — error_live HTML 컨텍스트 보강용
_RECENT_LOG_LINES = collections.deque(maxlen=30)

_original_excepthook = sys.excepthook


def _global_excepthook(exc_type, exc_value, exc_tb):
    """sys.excepthook 재정의 — 캐치되지 않은 예외를 파일로 저장."""
    tb_text = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"\n[UNCAUGHT EXCEPTION] {exc_type.__name__}: {exc_value}")
    print(tb_text)
    try:
        _save_uncaught_report(exc_type, exc_value, tb_text, ts, "sys.excepthook")
    except Exception:
        pass
    _original_excepthook(exc_type, exc_value, exc_tb)


def _threading_excepthook(args):
    """threading.excepthook — worker thread 예외 캡처."""
    tb_text = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    thread_name = args.thread.name if args.thread else "unknown"
    print(f"\n[THREAD EXCEPTION] thread={thread_name} {args.exc_type.__name__}: {args.exc_value}")
    print(tb_text)
    try:
        _save_uncaught_report(args.exc_type, args.exc_value, tb_text, ts,
                              f"threading.excepthook (thread={thread_name})")
    except Exception:
        pass


def _save_uncaught_report(exc_type, exc_value, tb_text: str, ts: str, source: str):
    """캐치되지 않은 예외 → error_uncaught_*.html 저장."""
    log_dir = Path(__file__).resolve().parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"error_uncaught_{ts}.html"
    import json
    ctx = {
        "source": source,
        "exc_type": exc_type.__name__,
        "exc_repr": repr(exc_value),
        "recent_log": list(_RECENT_LOG_LINES),
    }
    html = f"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"/><title>UNCAUGHT {ts}</title>
<style>body{{font-family:'Malgun Gothic',sans-serif;background:#f5f5f5;color:#111;padding:16px;}}
h1{{color:#b91c1c;}}pre{{background:#111;color:#f5f5f5;padding:12px;white-space:pre-wrap;}}
section{{background:#fff;padding:12px 16px;margin-bottom:12px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.06);}}</style></head><body>
<section><h1>Uncaught Exception</h1>
<p><strong>Source:</strong> {source}</p>
<p><strong>Time:</strong> {ts}</p>
<p><strong>Type:</strong> {exc_type.__name__}</p>
<p><strong>Message:</strong> {str(exc_value)[:500]}</p></section>
<section><h3>Context</h3><pre>{json.dumps(ctx, ensure_ascii=False, indent=2)}</pre></section>
<section><h3>Traceback</h3><pre>{tb_text}</pre></section></body></html>"""
    path.write_text(html, encoding="utf-8")
    print(f"[Reporter] 미캐치 예외 리포트 → {path}")


sys.excepthook = _global_excepthook
if hasattr(threading, 'excepthook'):
    threading.excepthook = _threading_excepthook


from pathlib import Path
from typing import Any, Dict, Optional
import time as time_module

# ── 경로 설정 ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from config import Gen3Config

# ── 로그 디렉토리 ─────────────────────────────────────────────────────────────
LOG_DIR = BASE_DIR / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

ERROR_FLAG_PATH = LOG_DIR / "LAST_ERROR.flag"


# ── 에러 리포트 ───────────────────────────────────────────────────────────────

def save_error_report(mode: str, exc: Exception,
                      context: Optional[Dict[str, Any]] = None) -> str:
    import json

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"error_{mode.lower()}_{ts}.html"
    path     = LOG_DIR / filename

    title    = f"Q-TRON Gen3 ERROR ({mode}) - {ts}"
    exc_type = type(exc).__name__
    exc_msg  = str(exc)
    exc_repr = repr(exc)

    # ── traceback 다중 소스 캡처 (우선순위: context 주입 → format_exc → chain) ──
    tb_text = ""
    if context and context.get("_traceback_captured"):
        tb_text = context["_traceback_captured"]
    if not tb_text:
        tb_text = traceback.format_exc()
    if not tb_text or tb_text.strip() == "NoneType: None":
        # except 블록 밖이거나 Qt 슬롯 경계에서 삼켜진 경우
        tb_text = f"(format_exc() empty — exc chain follows)\n"
        tb_text += f"type: {exc_type}\nrepr: {exc_repr}\nstr:  {exc_msg}\n"
        if exc.__traceback__:
            tb_text += "".join(traceback.format_tb(exc.__traceback__))
        # chained exception
        if exc.__cause__:
            tb_text += f"\n--- Caused by: {type(exc.__cause__).__name__}: {exc.__cause__}\n"
            if exc.__cause__.__traceback__:
                tb_text += "".join(traceback.format_tb(exc.__cause__.__traceback__))

    # ── 진단 컨텍스트 보강 ──────────────────────────────────────────
    diag = {
        "exc_type":       exc_type,
        "exc_repr":       exc_repr,
        "thread":         threading.current_thread().name,
        "recent_log":     list(_RECENT_LOG_LINES),
    }
    if context:
        diag["runtime_context"] = context

    ctx_html = f"<section><h3>Context</h3><pre>{json.dumps(diag, ensure_ascii=False, indent=2, default=str)}</pre></section>"

    html = f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<style>
body{{font-family:'Malgun Gothic',sans-serif;background:#f5f5f5;color:#111;padding:16px;}}
h1{{color:#b91c1c;}}
pre{{background:#111;color:#f5f5f5;padding:12px;white-space:pre-wrap;}}
section{{background:#fff;padding:12px 16px;margin-bottom:12px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,.06);}}
</style>
</head>
<body>
<section>
  <h1>Q-TRON Gen3 에러 리포트</h1>
  <p><strong>발생시각:</strong> {ts}</p>
  <p><strong>실행모드:</strong> {mode}</p>
  <p><strong>예외타입:</strong> {exc_type}</p>
  <p><strong>repr:</strong> {exc_repr}</p>
  <p><strong>메시지:</strong> {exc_msg}</p>
  <p><strong>Thread:</strong> {threading.current_thread().name}</p>
</section>
{ctx_html}
<section><h3>Traceback</h3><pre>{tb_text}</pre></section>
</body>
</html>"""
    path.write_text(html, encoding="utf-8")
    print(f"[Reporter] 에러 리포트 → {path}")
    return str(path)


def set_error_flag(path: str) -> None:
    try:
        ERROR_FLAG_PATH.write_text(path, encoding="utf-8")
    except Exception as e:
        print(f"[WARN] LAST_ERROR.flag 기록 실패: {e}")


def clear_error_flag() -> None:
    try:
        if ERROR_FLAG_PATH.exists():
            ERROR_FLAG_PATH.unlink()
    except Exception as e:
        print(f"[WARN] LAST_ERROR.flag 삭제 실패: {e}")


def warn_last_error() -> None:
    if not ERROR_FLAG_PATH.exists():
        return
    try:
        last = ERROR_FLAG_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        last = "(읽기 실패)"
    print("=" * 60)
    print("[경고] 이전 실행에서 에러가 발생했습니다.")
    print(f"       마지막 에러 리포트: {last}")
    print("=" * 60)


# ── 유틸 ─────────────────────────────────────────────────────────────────────

def _print_header(mode: str) -> None:
    print("=" * 60)
    print(f"  Q-TRON Gen3  [{mode}]  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


def wait_until(target_h: int = 9, target_m: int = 0) -> None:
    """target_h:target_m 까지 루프 대기 (Ctrl+C 로 취소 불가)."""
    now    = datetime.now()
    target = datetime.combine(now.date(), dtime(target_h, target_m))
    if now >= target:
        print(f"[Scheduler] {now.strftime('%H:%M')} → 즉시 실행")
        return
    remaining = (target - now).total_seconds()
    print(f"[Scheduler] {target_h:02d}:{target_m:02d} 까지 대기 ({remaining/60:.0f}분)")
    last_print = time_module.time()
    while True:
        now = datetime.now()
        if now >= target:
            print(f"[Scheduler] {target_h:02d}:{target_m:02d} 도달 → 실행")
            return
        remaining = (target - now).total_seconds()
        # 10분마다만 남은 시간 출력
        now_ts = time_module.time()
        if now_ts - last_print >= 600:
            print(f"[Scheduler] 남은 시간: {remaining/60:.0f}분")
            last_print = now_ts
        try:
            time_module.sleep(60 if remaining > 60 else 10)
        except KeyboardInterrupt:
            print("[Scheduler] Ctrl+C 감지 — 대기 유지 (창을 닫아 강제 종료)")
            time_module.sleep(3)


def wait_until_with_qt(target_h: int, target_m: int, app) -> None:
    """
    Qt 이벤트 루프를 살리면서 대기 (OnReceiveRealData 수신 필수).
    기존 wait_until()은 time.sleep()으로 Qt 이벤트를 차단하므로,
    체결강도 관측 중에는 이 함수를 사용해야 한다.
    """
    from datetime import date
    target = datetime.combine(date.today(), dtime(target_h, target_m))
    if datetime.now() >= target:
        print(f"[Scheduler] {target_h:02d}:{target_m:02d} → 즉시 진행")
        return

    print(f"[Scheduler] {target_h:02d}:{target_m:02d} 까지 Qt 이벤트 처리하며 대기 (체결강도 수집 중)")
    last_print = 0.0
    while datetime.now() < target:
        app.processEvents()    # OnReceiveRealData 콜백 처리
        remaining = (target - datetime.now()).total_seconds()
        # 5분마다 남은 시간 출력
        import time as _t
        now_ts = _t.time()
        if now_ts - last_print >= 300:
            print(f"[Scheduler] 체결강도 관측 중... 남은 시간: {remaining/60:.1f}분")
            last_print = now_ts
        time_module.sleep(1.0)

    print(f"[Scheduler] {target_h:02d}:{target_m:02d} 도달 → 체결강도 관측 완료")


# ── RUNTIME 공통 ──────────────────────────────────────────────────────────────

def _run_runtime(provider, mode_label: str, skip_market_hours: bool = False,
                 open_browser: bool = False, fresh_state: bool = False) -> None:
    """공통 런타임 실행."""
    import glob, webbrowser as _wb
    from runtime.runtime_engine import RuntimeEngine

    config = Gen3Config.load()
    config.paper_trading = True

    engine = RuntimeEngine(config, provider, skip_market_hours=skip_market_hours,
                           fresh_state=fresh_state)
    result: Optional[Dict[str, Any]] = None

    try:
        result = engine.run()
        _print_result(result)
        engine.end_of_day()

        if open_browser:
            reports = sorted(glob.glob(str(LOG_DIR / "report_daily_*.html")))
            if reports:
                _wb.open(f"file:///{reports[-1].replace(chr(92), '/')}")

        if result.get("status") == "ERROR":
            exc = RuntimeError(result.get("message", "RuntimeEngine status == ERROR"))
            rpt = save_error_report(mode_label, exc, result)
            set_error_flag(rpt)
        else:
            clear_error_flag()

    except Exception as e:
        print(f"\n[CRITICAL] {mode_label} 예외:\n  {type(e).__name__}: {e}")
        rpt = save_error_report(mode_label, e, result)
        set_error_flag(rpt)


def _print_result(result: Dict[str, Any]) -> None:
    _snap = result.get("snap_ts", "")
    _tick = result.get("tick_source", "")
    _tags = []
    if _snap:
        _tags.append(f"snap={_snap}")
    if _tick:
        _tags.append(f"tick_source={_tick}")
    _tag_str = f"  {' '.join(_tags)}" if _tags else ""
    print(f"\n[RuntimeEngine 결과]{_tag_str}")
    print("  status :", result.get("status"))
    print("  message:", result.get("message"))
    for k, v in result.get("portfolio", {}).items():
        print(f"    {k}: {v}")


# ── MOCK 모드 ─────────────────────────────────────────────────────────────────

def run_mock() -> None:
    warn_last_error()
    _print_header("MOCK (체결강도 + 모니터링 루프 테스트)")

    from data.mock_provider import MockProvider
    from runtime.runtime_engine import RuntimeEngine

    # 변동성 5%로 높여서 SL 트리거 테스트 가능하게
    provider = MockProvider(volatility=0.05)
    config   = Gen3Config.load()
    config.paper_trading = True

    engine = RuntimeEngine(config, provider, skip_market_hours=True, fresh_state=True)
    result: Optional[Dict[str, Any]] = None

    # ── Phase 0.5: 체결강도 관측 (mock: 즉시 완료) ────────────────────────
    tick_analyzer = engine.run_observation_phase()
    if tick_analyzer:
        # Mock 추가 데이터 생성 (5회)
        for _ in range(5):
            time_module.sleep(0.2)
            tick_analyzer.feed_mock_tick()

    # ── Phase 1: 매수 (체결강도 필터 적용) ────────────────────────────────
    try:
        result = engine.run_entries(tick_analyzer=tick_analyzer)
        _print_result(result)
    except Exception as e:
        print(f"[MOCK ERROR] {type(e).__name__}: {e}")
        return

    # ── Phase 2: 모니터링 루프 테스트 (10사이클 × 2초) ────────────────────
    MOCK_CYCLES   = 10
    MOCK_INTERVAL = 2   # seconds
    print(f"\n[Mock Monitor] 테스트 루프 시작 "
          f"({MOCK_CYCLES}사이클, {MOCK_INTERVAL}초 간격, 변동성 5%)")

    for i in range(MOCK_CYCLES):
        time_module.sleep(MOCK_INTERVAL)
        try:
            cycle = engine.run_monitor_cycle()
            if cycle.get("status") == "HARD_STOP":
                print("[Mock Monitor] HARD_STOP — 루프 종료")
                break
            if not engine.portfolio.positions:
                print("[Mock Monitor] 전 포지션 청산 완료 — 루프 종료")
                break
        except Exception as e:
            print(f"[Mock Monitor ERROR] {type(e).__name__}: {e}")

    # ── Phase 3: EOD ──────────────────────────────────────────────────────
    engine.end_of_day()

    import glob, webbrowser as _wb
    reports = sorted(glob.glob(str(LOG_DIR / "report_daily_*.html")))
    if reports:
        _wb.open(f"file:///{reports[-1].replace(chr(92), '/')}")


# ── PYKRX 모드 ────────────────────────────────────────────────────────────────

def run_pykrx() -> None:
    try:
        from data.pykrx_provider import PykrxProvider
    except ImportError:
        print("[ERROR] data/pykrx_provider.py 를 찾을 수 없습니다.")
        sys.exit(1)

    warn_last_error()
    _print_header("PYKRX (실데이터 · 장외 테스트)")
    print("[PYKRX] KRX 데이터 수집 중...")
    _run_runtime(PykrxProvider(), "PYKRX", skip_market_hours=True)


# ── LIVE 모드 ─────────────────────────────────────────────────────────────────

def run_live() -> None:
    # Tee: console + log file 동시 출력 (타임스탬프별 히스토리 보존)
    _ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_path_history = LOG_DIR / f"run_live_{_ts}.log"
    _log_path_latest = LOG_DIR / "run_live.log"
    try:
        _out_buf = sys.stdout.buffer
        _err_buf = sys.stderr.buffer
        _log_f = open(_log_path_history, "w", encoding="utf-8")
        sys.stdout = _TeeWriter(_out_buf, _log_f)
        sys.stderr = _TeeWriter(_err_buf, _log_f)
        # 오래된 히스토리 정리 (최근 30개 유지)
        _old = sorted(LOG_DIR.glob("run_live_*.log"))[:-30]
        for f in _old:
            f.unlink(missing_ok=True)
    except Exception:
        pass  # fallback: console only

    try:
        from PyQt5.QtWidgets import QApplication
        from data.kiwoom_provider import KiwoomProvider
        from api.kiwoom_api_wrapper import create_loggedin_kiwoom
    except ImportError as e:
        print(f"[ERROR] Kiwoom/PyQt5 모듈 없음: {e}")
        sys.exit(1)

    if datetime.now().time() >= dtime(15, 30):
        print("[INFO] 장 종료 이후 → 자동 종료. 장외 테스트는 --pykrx 사용.")
        sys.exit(0)

    warn_last_error()
    _print_header("LIVE (Kiwoom)")

    app    = QApplication.instance() or QApplication(sys.argv)

    # ── Qt 이벤트 루프 내 Python 예외 캡처 훅 ──────────────────────
    def _qt_message_handler(msg_type, context, message):
        """qInstallMessageHandler — Qt 내부 경고/에러를 Python 로그로 전환."""
        from PyQt5.QtCore import QtWarningMsg, QtCriticalMsg, QtFatalMsg
        if msg_type in (QtCriticalMsg, QtFatalMsg):
            print(f"[Qt CRITICAL] {message} (file={context.file}, line={context.line})")
        elif msg_type == QtWarningMsg:
            print(f"[Qt WARNING] {message}")

    try:
        from PyQt5.QtCore import qInstallMessageHandler
        qInstallMessageHandler(_qt_message_handler)
    except Exception:
        pass

    config = Gen3Config.load()
    config.paper_trading = False   # Kiwoom SendOrder 실주문 활성화

    print("[Kiwoom] 로그인 시작...")
    print("[Kiwoom] 로그인 팝업이 뜨면 ID/PW 입력 후 확인 버튼을 누르세요.")
    print("[Kiwoom]   ※ 팝업이 콘솔 창 뒤에 숨어 있을 수 있습니다 — 작업표시줄 확인!")
    try:
        kiwoom = create_loggedin_kiwoom()
    except Exception as e:
        print(f"\n[ERROR] Kiwoom 로그인 실패: {type(e).__name__}: {e}")
        print("  - Kiwoom OpenAPI+ 가 설치되어 있는지 확인하세요.")
        print("  - 32비트 Python 환경에서 실행 중인지 확인하세요.")
        print("  - 이전에 키움 프로그램이 이미 실행 중이면 종료 후 재시도하세요.")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    # 계좌비밀번호 입력 대기 (ShowAccountWindow 팝업)
    input("[Kiwoom] Account PW -> [Register] -> close popup -> press Enter... ")
    print("[Kiwoom] Account password registered.")

    provider = KiwoomProvider(kiwoom)
    print("[Kiwoom] 로그인 완료. 09:00 까지 대기.")

    wait_until(9, 0)

    from runtime.runtime_engine import RuntimeEngine
    engine = RuntimeEngine(config, provider)
    result: Optional[Dict[str, Any]] = None

    # ── Phase 0.5: 체결강도 관측 (09:00 ~ 10:00) ─────────────────────────
    tick_analyzer = None
    if getattr(config, 'TICK_ENABLED', False):
        tick_analyzer = engine.run_observation_phase()
        if tick_analyzer:
            observe_minutes = getattr(config, 'TICK_OBSERVE_MINUTES', 60)
            entry_hour = 9 + observe_minutes // 60
            entry_minute = observe_minutes % 60
            print(f"[LIVE] 체결강도 관측 시작 → {entry_hour:02d}:{entry_minute:02d} 까지 대기")
            wait_until_with_qt(entry_hour, entry_minute, app)

    # ── Phase 1: 매수 (체결강도 필터 적용) ────────────────────────────────
    try:
        result = engine.run_entries(tick_analyzer=tick_analyzer)
        _print_result(result)

        if result.get("status") == "ERROR":
            ctx = dict(result)
            ctx["_traceback_captured"] = result.get("_traceback", "")
            exc = RuntimeError(result.get("message", ""))
            rpt = save_error_report("LIVE", exc, ctx)
            set_error_flag(rpt)
        else:
            clear_error_flag()

    except Exception as e:
        tb_full = traceback.format_exc()
        print(f"\n[CRITICAL] LIVE 예외: {type(e).__name__}: {e}")
        print(tb_full)
        try:
            engine.state_mgr.save_portfolio(engine.portfolio)
            print("[RECOVERY] 포지션 상태 저장 완료")
        except Exception as save_err:
            print(f"[RECOVERY FAILED] 상태 저장 실패: {save_err}")
        err_ctx = result if result else {}
        err_ctx["_traceback_captured"] = tb_full
        err_ctx["_exc_repr"] = repr(e)
        rpt = save_error_report("LIVE", e, err_ctx)
        set_error_flag(rpt)

    # ── Phase 2: 장중 모니터링 루프 (~ 15:20) ─────────────────────────────
    MONITOR_INTERVAL = 60       # seconds
    HEARTBEAT_INTERVAL = 600    # 10분마다 상태 출력
    MAX_CONSECUTIVE_ERRORS = 3  # BUG-9 FIX: 연속 에러 N회 → safe mode
    print(f"\n[Monitor] 장중 모니터링 시작 (60초 간격 체크, 10분 간격 출력, 15:20 종료)")

    _last_heartbeat = time_module.time()
    _cycle_count = 0
    _consecutive_errors = 0

    while True:
        now = datetime.now().time()
        if now >= dtime(15, 20):
            print("[Monitor] 15:20 도달 — 모니터링 종료")
            break
        if not engine.portfolio.positions:
            print("[Monitor] 보유 포지션 없음 — 대기 중")

        try:
            time_module.sleep(MONITOR_INTERVAL)
        except KeyboardInterrupt:
            print("[Monitor] Ctrl+C — 모니터링 중단 (EOD 처리 진행)")
            break

        try:
            # 연결 상태 체크 (LIVE 모드 — provider에 is_connected가 있는 경우)
            if hasattr(provider, 'ensure_connected') and not provider.ensure_connected():
                now_str = datetime.now().strftime("%H:%M:%S")
                print(f"[Monitor {now_str}] *** API 연결 끊김 — 재접속 실패. 상태 저장 후 대기 ***")
                engine.state_mgr.save_portfolio(engine.portfolio)
                time_module.sleep(30)   # 30초 후 재시도
                continue

            cycle = engine.run_monitor_cycle()
            _cycle_count += 1

            # BUG-9 FIX: 연속 에러 카운팅
            if cycle.get("status") == "ERROR":
                _consecutive_errors += 1
                if _consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    n_pos = len(engine.portfolio.positions)
                    eq = engine.portfolio.get_current_equity()
                    print(f"[SAFE_MODE] 연속 {_consecutive_errors}회 에러 -> SAFE MODE")
                    print(f"[SAFE_MODE] 신규 진입: 차단")
                    print(f"[SAFE_MODE] 기존 포지션 모니터링: 중단 (stale price 리스크)")
                    print(f"[SAFE_MODE] 보유 {n_pos}개 포지션, 총자산 {eq:,.0f}원")
                    print(f"[SAFE_MODE] 포지션 상태 저장 후 모니터링 루프 종료")
                    try:
                        engine.state_mgr.save_portfolio(engine.portfolio)
                        print(f"[SAFE_MODE] 상태 저장 완료")
                    except Exception:
                        print(f"[SAFE_MODE] 상태 저장 실패!")
                    rpt = save_error_report("LIVE", RuntimeError(
                        f"연속 {_consecutive_errors}회 모니터 에러 -> SAFE_MODE"), cycle)
                    set_error_flag(rpt)
                    print(f"[SAFE_MODE] 에러 리포트: {rpt}")
                    break
            else:
                _consecutive_errors = 0

            if cycle.get("status") == "HARD_STOP":
                print("[Monitor] HARD_STOP — 모니터링 종료")
                break

            # 10분 heartbeat (청산 없는 평상시)
            now_ts = time_module.time()
            if now_ts - _last_heartbeat >= HEARTBEAT_INTERVAL:
                n_pos = cycle.get("positions", 0)
                eq = cycle.get("equity", 0)
                pnl = cycle.get("pnl", 0)
                now_str = datetime.now().strftime("%H:%M")
                print(f"[Heartbeat {now_str}] {n_pos}개 포지션 | "
                      f"총자산 {eq:,.0f}원 | 일간 {pnl:.2%}")
                # Signal Dashboard (10분 간격)
                engine.tracker.update_prices(engine.portfolio)
                engine.tracker.print_dashboard(compact=True)
                _last_heartbeat = now_ts

        except Exception as e:
            _consecutive_errors += 1
            print(f"[Monitor ERROR] ({_consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}) "
                  f"{type(e).__name__}: {e}")
            try:
                engine.state_mgr.save_portfolio(engine.portfolio)
            except Exception:
                pass
            if _consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                n_pos = len(engine.portfolio.positions)
                eq = engine.portfolio.get_current_equity()
                print(f"[SAFE_MODE] 연속 {_consecutive_errors}회 예외 -> SAFE MODE")
                print(f"[SAFE_MODE] 보유 {n_pos}개 포지션, 총자산 {eq:,.0f}원")
                print(f"[SAFE_MODE] 모니터링 루프 종료")
                rpt = save_error_report("LIVE", e)
                set_error_flag(rpt)
                print(f"[SAFE_MODE] 에러 리포트: {rpt}")
                break

    # ── Phase 3: 장 마감 (EOD) ─────────────────────────────────────────────
    try:
        engine.end_of_day()
    except Exception as e:
        print(f"[EOD ERROR] {type(e).__name__}: {e}")

    # BUG-6: 실시간 구독 해제 safety-net (HARD_STOP 등으로 누락된 경우 대비)
    try:
        provider.unregister_real()
    except Exception:
        pass

    # 로그 히스토리 → run_live.log 최신본 복사
    try:
        import shutil
        _log_f.flush()
        shutil.copy2(str(_log_path_history), str(_log_path_latest))
    except Exception:
        pass

    # Qt 이벤트 루프 종료 후 정상 종료 (app.exec_() 로 무한 대기하지 않음)
    app.quit()
    sys.exit(0)


# ── BATCH 모드 ────────────────────────────────────────────────────────────────

def run_batch() -> None:
    """
    배치 모드 (pykrx): 18:00 ~ 20:30
    유니버스 수집 → Q-Score 계산 → signals_YYYYMMDD.csv 생성
    """
    _print_header("BATCH (신호 생성 / pykrx)")
    from batch.batch_runner import BatchRunner
    from data.pykrx_provider import PykrxProvider

    config   = Gen3Config.load()
    provider = PykrxProvider()
    runner   = BatchRunner(config, provider)
    result   = runner.run()

    print("\n[Batch 결과]")
    print(f"  생성된 신호: {result.get('signal_count', 0)}개")
    print(f"  파일: {result.get('signal_file', 'N/A')}")
    if result.get("errors"):
        print(f"  에러: {result['errors']}")
        sys.exit(1)
    sys.exit(0)


def run_kiwoom_batch(sample: int = 0) -> None:
    """
    배치 모드 (Kiwoom): Kiwoom API 로그인 후 유니버스/Q-Score/signals.csv 생성.
    평일 야간·주말에도 전일 기준 데이터 수집 가능.
    """
    try:
        from PyQt5.QtWidgets import QApplication
        from data.kiwoom_provider import KiwoomProvider
        from api.kiwoom_api_wrapper import create_loggedin_kiwoom
    except ImportError as e:
        print(f"[ERROR] Kiwoom/PyQt5 모듈 없음: {e}")
        sys.exit(1)

    warn_last_error()
    _print_header("KIWOOM-BATCH (신호 생성 / Kiwoom API)")

    app    = QApplication.instance() or QApplication(sys.argv)
    config = Gen3Config.load()

    print("[Kiwoom] 로그인 시작 — 팝업 완료 후 배치가 자동 시작됩니다.")
    kiwoom   = create_loggedin_kiwoom()
    provider = KiwoomProvider(kiwoom)
    print("[Kiwoom] 로그인 완료. 배치 파이프라인 시작.")

    from batch.batch_runner import BatchRunner
    runner = BatchRunner(config, provider, max_stocks=sample)
    result = runner.run()

    print("\n[Kiwoom Batch 결과]")
    print(f"  생성된 신호: {result.get('signal_count', 0)}개")
    print(f"  파일: {result.get('signal_file', 'N/A')}")
    if result.get("errors"):
        print(f"  에러: {result['errors']}")

    # QApplication 정리 후 종료
    app.quit()
    sys.exit(0)


# ── 엔트리 포인트 ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Q-TRON Gen3 트레이딩 시스템",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 예시:
  python main.py                  LIVE 모드 (Kiwoom, 장중)
  python main.py --pykrx          PYKRX 모드 (실데이터, 장외 테스트)
  python main.py --mock           MOCK 모드 (구조 확인)
  python main.py --batch          BATCH 모드 (pykrx, 18:00~)
  python main.py --kiwoom-batch   BATCH 모드 (Kiwoom API, 평일/주말 모두 가능)
        """
    )
    parser.add_argument("--pykrx",        action="store_true", help="pykrx 실데이터 모드")
    parser.add_argument("--mock",         action="store_true", help="MockProvider 모드")
    parser.add_argument("--batch",        action="store_true", help="Batch 신호 생성 모드 (pykrx)")
    parser.add_argument("--kiwoom-batch", action="store_true", help="Batch 신호 생성 모드 (Kiwoom API)")
    parser.add_argument("--sample", type=int, default=0, metavar="N", help="유니버스 샘플 N개만 처리 (테스트용)")
    args = parser.parse_args()

    if args.mock:
        run_mock()
    elif args.pykrx:
        run_pykrx()
    elif args.batch:
        run_batch()
    elif args.kiwoom_batch:
        run_kiwoom_batch(sample=args.sample)
    else:
        run_live()
