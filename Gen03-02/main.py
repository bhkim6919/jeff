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
from datetime import datetime, time as dtime

# Windows CP949 콘솔에서 유니코드 문자(—, ⚠ 등) 인코딩 오류 방지
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
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
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"error_{mode.lower()}_{ts}.html"
    path     = LOG_DIR / filename

    title    = f"Q-TRON Gen3 ERROR ({mode}) - {ts}"
    exc_type = type(exc).__name__
    exc_msg  = str(exc)
    tb_text  = traceback.format_exc()

    ctx_html = ""
    if context:
        import json
        ctx_html = f"<section><h3>Context</h3><pre>{json.dumps(context, ensure_ascii=False, indent=2)}</pre></section>"

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
  <p><strong>메시지:</strong> {exc_msg}</p>
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
    print(f"[Scheduler] {target_h:02d}:{target_m:02d} 까지 대기 중 (Ctrl+C 비활성)")
    while True:
        now = datetime.now()
        if now >= target:
            print(f"[Scheduler] {target_h:02d}:{target_m:02d} 도달 → 실행")
            return
        remaining = (target - now).total_seconds()
        print(f"[Scheduler] 남은 시간: {remaining/60:.1f}분")
        try:
            time_module.sleep(60 if remaining > 60 else 10)
        except KeyboardInterrupt:
            print("[Scheduler] Ctrl+C 감지 — 대기 유지 (창을 닫아 강제 종료)")
            time_module.sleep(3)


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
    print("\n[RuntimeEngine 결과]")
    print("  status :", result.get("status"))
    print("  message:", result.get("message"))
    for k, v in result.get("portfolio", {}).items():
        print(f"    {k}: {v}")


# ── MOCK 모드 ─────────────────────────────────────────────────────────────────

def run_mock() -> None:
    warn_last_error()
    _print_header("MOCK")
    from data.mock_provider import MockProvider
    _run_runtime(MockProvider(), "MOCK", skip_market_hours=True, open_browser=True,
                 fresh_state=True)


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
    _print_header("LIVE (Kiwoom / PAPER)")

    app    = QApplication.instance() or QApplication(sys.argv)
    config = Gen3Config.load()

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
    provider = KiwoomProvider(kiwoom)
    print("[Kiwoom] 로그인 완료. 09:00 까지 대기.")

    wait_until(9, 0)

    from runtime.runtime_engine import RuntimeEngine
    engine = RuntimeEngine(config, provider)
    result: Optional[Dict[str, Any]] = None

    try:
        result = engine.run()
        _print_result(result)
        engine.end_of_day()

        if result.get("status") == "ERROR":
            exc = RuntimeError(result.get("message", ""))
            rpt = save_error_report("LIVE", exc, result)
            set_error_flag(rpt)
        else:
            clear_error_flag()

    except Exception as e:
        print(f"\n[CRITICAL] LIVE 예외: {type(e).__name__}: {e}")
        rpt = save_error_report("LIVE", e, result)
        set_error_flag(rpt)

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
