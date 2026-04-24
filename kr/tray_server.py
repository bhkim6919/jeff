"""
tray_server.py -- Q-TRON REST Server with System Tray Icon (win32 native)
=========================================================================
Windows system tray (notification area) with right-click menu.

Usage:
  pythonw.exe tray_server.py          (background, no console)
  python.exe tray_server.py           (foreground, with console)
  09_rest_server_bg.bat               (bat launcher)
"""
from __future__ import annotations

import os
import sys

# pythonw.exe: sys.stdout/stderr are None, which crashes uvicorn's log config
# (it calls sys.stdout.isatty()). Redirect to devnull before any imports.
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")

import ctypes
import json
import logging
import subprocess
import tempfile
import threading
import time
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# 2026-04-24 Jeff 보고 건: 빈 cmd.exe 창이 떠 있다가 몇 분 뒤 사라짐.
# 원인: Windows 에서 `subprocess.run(["netstat"…])` 등 native 커맨드가
# 기본적으로 새 console window 를 spawn. pythonw.exe 로 tray 가 떠 있어도
# 자식 프로세스만 보임. CREATE_NO_WINDOW 로 일괄 억제한다.
_NO_WINDOW = subprocess.CREATE_NO_WINDOW if hasattr(subprocess, "CREATE_NO_WINDOW") else 0

# ── Path setup (must precede any kr.* / shared.* import) ──
sys.path.insert(0, str(Path(__file__).resolve().parent))  # audit:allow-syspath: bootstrap-locator
import _bootstrap_path  # noqa: F401  -- side-effect: adds project root for `shared.*`

import win32api
import win32con
import win32gui
import win32gui_struct

# ── Config ──
PORT = 8080
HOST = "0.0.0.0"
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "data" / "logs"
XVAL_DIR = BASE_DIR / "data" / "xval"
APP_NAME = "Q-TRON REST"
BATCH_HOUR = 16  # 16:00 (장 마감 후)
BATCH_MINUTE = 5
# 실패 시 다음 재시도까지 최소 대기 시간. 30초 tick 루프에서 공격적 재시도 방지.
BATCH_FAIL_BACKOFF_MIN = 5

# Daily self-test (PG ping + telegram on failure) — runs before market open,
# after overnight batch/backup, in a low-traffic window so a transient PG
# hiccup is caught before live/batch depends on it.
SELF_TEST_HOUR = 8
SELF_TEST_MINUTE = 0

# ── Lab Live/Forward EOD Auto-Schedule ───────────────────────
KR_LAB_EOD_HOUR = 15   # KST 15:35 (장 마감 + 5분)
KR_LAB_EOD_MINUTE = 35
US_LAB_EOD_HOUR = 16   # ET 16:05 (장 마감 + 5분)
US_LAB_EOD_MINUTE = 5
LAB_EOD_WINDOW_SEC = 60      # 트리거 윈도우 (초)
LAB_EOD_RETRY_BACKOFF_MIN = 5  # fail 시 다음 재시도 지연(분)
LAB_EOD_MAX_FAILS = 3         # 당일 최대 시도 횟수

# ── US Server Config ─────────────────────────────────────────
US_PORT = 8081
US_BASE_DIR = Path(__file__).resolve().parent.parent / "us"
US_PYTHON = US_BASE_DIR / ".venv" / "Scripts" / "python.exe"

# ── KR Live (Gen4 trading engine) auto-attach config ─────────
# Tray spawns kr/main.py --live as a subprocess, restarted automatically
# whenever the KR market is open. Engine has its own 16:00 KST gate
# (startup_phase.py:59) — tray respects that and waits until next session
# instead of restart-looping. Goal: operator can reboot PC anytime; tray
# keeps the live engine attached without manual `python main.py --live`.
KR_LIVE_BASE_DIR = Path(__file__).resolve().parent  # kr/
KR_LIVE_PYTHON = Path(__file__).resolve().parent.parent / ".venv64" / "Scripts" / "python.exe"
KR_LIVE_START_HOUR = 8        # 08:30 KST — earliest tray will spawn live engine
KR_LIVE_START_MINUTE = 30
KR_LIVE_END_HOUR = 16          # 16:00 KST — engine's own startup gate; do not spawn after
KR_LIVE_RETRY_COOLDOWN_SEC = 300  # 5 min cooldown after a crash before next attempt
KR_LIVE_MAX_ATTEMPTS_PER_DAY = 3

# Menu item IDs
ID_STATUS = 1001
ID_DASHBOARD = 1002
ID_XVAL = 1003
ID_OPEN_LOG = 1004
ID_OPEN_LOG_DIR = 1005
ID_COPY_LOG = 1006
ID_BATCH_NOW = 1007
ID_BATCH_TOGGLE = 1008
ID_RESTART = 1009
ID_QUIT = 1010
ID_SELF_TEST_NOW = 1011
# US menu IDs
ID_US_DASHBOARD = 1020
ID_US_START = 1021
ID_US_STOP = 1022
ID_US_BATCH = 1023
ID_US_LIVE_START = 1024
ID_US_LIVE_STOP = 1025
ID_UNIFIED = 1026
ID_US_AUTO_TOGGLE = 1027
ID_GATE_OBSERVER = 1028
ID_US_RESTART = 1028
ID_RESTART_ALL = 1029
ID_US_LOG_DIR = 1030
# KR Live menu IDs
ID_KR_LIVE_START = 1040
ID_KR_LIVE_STOP = 1041
ID_KR_LIVE_RESTART = 1042
ID_KR_LIVE_AUTO_TOGGLE = 1043
ID_KR_LIVE_LOG = 1044


class Win32TrayServer:
    """Native Win32 system tray icon + uvicorn server."""

    def __init__(self):
        self._server_thread: Optional[threading.Thread] = None
        self._uvicorn_server = None
        self._start_time = datetime.now()
        self._running = False
        self._hwnd = None
        self._batch_auto = True  # EOD 자동 batch 활성화
        self._batch_running = False
        self._batch_today_done = False
        self._batch_last_done_date: Optional[date] = None  # KR batch 완료일
        self._batch_last_done_date = self._load_kr_batch_done_date()
        self._batch_last_fail_at: Optional[datetime] = None  # fail backoff 기준점
        # Backup state
        self._backup_today_done = False
        self._backup_running = False
        # Self-test state (daily PG ping)
        self._self_test_today_done = False
        self._self_test_running = False
        # US server state
        self._us_process: Optional[subprocess.Popen] = None
        self._us_running = False
        # US auto batch+rebal
        self._us_batch_auto = True
        self._us_batch_running = False
        self._us_batch_last_done_date: Optional[date] = None  # US batch 완료일
        self._us_batch_last_done_date = self._load_us_batch_done_date()
        # REJECTED 로그 throttle: (snapshot, reason) → last_log_time
        self._us_catchup_reject_log: dict = {}
        # KR Lab Live EOD auto schedule (v4.1)
        self._kr_lab_eod_lock = threading.Lock()
        self._kr_lab_eod_last_done_date: Optional[date] = None  # 초기값, 아래 loader로 설정
        self._kr_lab_eod_retry_until: Optional[datetime] = None
        self._kr_lab_eod_fail_count: int = 0
        self._kr_lab_eod_fail_count_date: Optional[date] = None
        # US Lab Forward EOD auto schedule (v4.1)
        self._us_lab_eod_lock = threading.Lock()
        self._us_lab_eod_last_done_date: Optional[date] = None
        self._us_lab_eod_retry_until: Optional[datetime] = None
        self._us_lab_eod_fail_count: int = 0
        self._us_lab_eod_fail_count_date: Optional[date] = None
        # US live state
        self._us_live_process: Optional[subprocess.Popen] = None
        self._us_live_running = False
        # KR live engine state (kr/main.py --live, auto-attach by tray)
        self._kr_live_process: Optional[subprocess.Popen] = None
        self._kr_live_running = False
        self._kr_live_auto = True   # default ON; menu can toggle
        self._kr_live_ended_today = False  # set on clean EOD / after-hours exit
        self._kr_live_last_attempt_at: Optional[datetime] = None
        self._kr_live_attempt_count_today = 0
        self._kr_live_attempt_count_date: Optional[date] = None
        self._kr_live_last_exit_reason: str = ""  # diagnostic
        self._kr_live_log_fh = None       # file handle (closed on stop)
        self._kr_live_log_path: Optional[Path] = None
        # Restart guards
        self._kr_restarting = False
        self._us_restarting = False
        self._logger = self._setup_logging()
        # Lab EOD persistence loader (logger 설정 후)
        self._kr_lab_eod_last_done_date = self._load_kr_lab_eod_done_date()
        self._us_lab_eod_last_done_date = self._load_us_lab_eod_done_date()
        self._shutdown_sent = False
        self._register_shutdown_hooks()

    def _register_shutdown_hooks(self):
        """비정상 종료(PC 꺼짐, kill 등) 시 텔레그램 알림."""
        import atexit
        atexit.register(self._on_exit)

        try:
            import win32api
            def _console_handler(ctrl_type):
                # CTRL_CLOSE_EVENT=2, CTRL_LOGOFF_EVENT=5, CTRL_SHUTDOWN_EVENT=6
                reasons = {2: "Console closed", 5: "Logoff", 6: "System shutdown"}
                reason = reasons.get(ctrl_type, f"Signal {ctrl_type}")
                self._send_shutdown_alert(reason)
                return False  # 기본 핸들러 진행
            win32api.SetConsoleCtrlHandler(_console_handler, True)
        except Exception:
            pass  # win32api 없으면 atexit만 사용

    def _on_exit(self):
        """atexit 핸들러: 정상 종료 시 알림 (이미 전송됐으면 skip)."""
        if not self._shutdown_sent:
            self._send_shutdown_alert("Process exit (atexit)")

    # ── Process-truth status ────────────────────────────────────
    def _is_us_process_alive(self) -> bool:
        """US 서버 프로세스가 실제로 살아있는지 확인.

        Tray 재연결 후 self._us_process가 None인 경우에도
        포트 기반 헬스체크로 실제 살아있음을 감지한다.
        """
        if self._us_process is not None:
            return self._us_process.poll() is None
        # Fallback: tray 재연결 시 subprocess 참조가 없어도 포트로 확인
        try:
            import urllib.request as _ur
            _ur.urlopen(f"http://localhost:{US_PORT}/api/health", timeout=2).close()
            return True
        except Exception:
            return False

    def _is_us_live_alive(self) -> bool:
        """US Live 프로세스가 실제로 살아있는지 확인."""
        if self._us_live_process is None:
            return False
        return self._us_live_process.poll() is None

    def _get_kr_status(self) -> str:
        return "Running" if self._running else "Stopped"

    def _get_us_status(self) -> str:
        if self._is_us_process_alive():
            return "Running"
        # Sync flag if process died
        if self._us_running:
            self._us_running = False
        return "Stopped"

    def _wait_process_exit(self, check_fn, timeout=5.0) -> bool:
        """프로세스 종료를 실제로 확인 (sleep 대신 polling)."""
        for _ in range(int(timeout / 0.5)):
            if not check_fn():
                return True
            time.sleep(0.5)
        return False

    def _setup_logging(self) -> logging.Logger:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        today = date.today().strftime("%Y%m%d")
        log_file = LOG_DIR / f"rest_api_{today}.log"

        logger = logging.getLogger("gen4.tray")
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )

        if not logger.handlers:
            fh = logging.FileHandler(str(log_file), encoding="utf-8", mode="a")
            fh.setFormatter(fmt)
            logger.addHandler(fh)

            if sys.stdout is not None:
                # R20 (2026-04-23): reconfigure stdout to UTF-8 with replace
                # to prevent UnicodeEncodeError on cp949 Windows consoles.
                try:
                    if hasattr(sys.stdout, "reconfigure"):
                        sys.stdout.reconfigure(
                            encoding="utf-8", errors="replace"
                        )
                except Exception:
                    pass
                ch = logging.StreamHandler(sys.stdout)
                ch.setFormatter(fmt)
                logger.addHandler(ch)

        # Prevent double-write: rest_logger.setup_rest_logging() attaches a
        # handler to the `gen4` parent, which would catch gen4.tray propagation
        # → same line written twice. gen4.tray already has its own handler.
        logger.propagate = False

        return logger

    # ── Icon (Neon Q with status colors) ────────────────────

    # Status → color mapping
    ICON_COLORS = {
        "ok":        {"glow": "#00E676", "ring": "#00C853", "text": "#FFFFFF"},  # green neon
        "error":     {"glow": "#FF1744", "ring": "#D50000", "text": "#FFFFFF"},  # red neon
        "connecting":{"glow": "#FFEA00", "ring": "#FFD600", "text": "#000000"},  # yellow neon
        "batch":     {"glow": "#76FF03", "ring": "#FFD600", "text": "#FFFFFF"},  # green+yellow
    }

    def _create_neon_icon(self, status: str = "ok") -> int:
        """Create a neon Q icon with status-dependent color."""
        try:
            from PIL import Image, ImageDraw, ImageFont, ImageFilter
            size = 64
            colors = self.ICON_COLORS.get(status, self.ICON_COLORS["ok"])

            # Glow layer (larger, blurred)
            glow = Image.new("RGBA", (size, size), (0, 0, 0, 0))
            gd = ImageDraw.Draw(glow)
            gd.ellipse([4, 4, size - 4, size - 4], fill=None,
                       outline=colors["glow"], width=3)
            glow = glow.filter(ImageFilter.GaussianBlur(radius=3))

            # Main layer
            img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
            draw = ImageDraw.Draw(img)

            # Dark background circle
            draw.ellipse([6, 6, size - 6, size - 6], fill=(20, 20, 30, 220))

            # Neon ring
            draw.ellipse([4, 4, size - 4, size - 4], fill=None,
                         outline=colors["ring"], width=2)

            # Batch = dual color ring (green + yellow alternating effect)
            if status == "batch":
                draw.arc([4, 4, size - 4, size - 4], 0, 180,
                         fill="#76FF03", width=2)
                draw.arc([4, 4, size - 4, size - 4], 180, 360,
                         fill="#FFD600", width=2)

            # Q text with neon glow
            try:
                font = ImageFont.truetype("arial.ttf", 32)
            except Exception:
                font = ImageFont.load_default()
            bbox = draw.textbbox((0, 0), "Q", font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            tx = (size - tw) // 2
            ty = (size - th) // 2 - 2

            # Text glow (draw text slightly larger in glow color)
            draw.text((tx, ty), "Q", fill=colors["glow"], font=font)

            # Composite: glow behind main
            result = Image.alpha_composite(glow, img)

            # Save
            ico_dir = BASE_DIR / "data"
            ico_dir.mkdir(parents=True, exist_ok=True)
            ico_path = ico_dir / f"qtron_tray_{status}.ico"
            result.save(str(ico_path), format="ICO", sizes=[(64, 64)])

            return win32gui.LoadImage(
                0, str(ico_path),
                win32con.IMAGE_ICON, 0, 0,
                win32con.LR_LOADFROMFILE | win32con.LR_DEFAULTSIZE,
            )
        except Exception as e:
            self._logger.warning(f"[TRAY] Neon icon failed: {e}, using default")
            return win32gui.LoadIcon(0, win32con.IDI_APPLICATION)

    def _create_icon(self) -> int:
        """Default icon (green neon = OK)."""
        return self._create_neon_icon("ok")

    def _update_icon(self, status: str):
        """Change tray icon to reflect current status."""
        try:
            icon = self._create_neon_icon(status)
            win32gui.Shell_NotifyIcon(
                win32gui.NIM_MODIFY,
                (self._hwnd, 0, win32gui.NIF_ICON, 0, icon),
            )
            self._current_icon_status = status
        except Exception as e:
            self._logger.warning(f"[TRAY] Icon update failed: {e}")

    def _start_blink(self, colors: list, interval: float = 0.7):
        """Start blinking between colors. colors = ["ok", "batch"] etc."""
        self._blink_active = True
        self._blink_colors = colors
        self._blink_idx = 0

        def _blink_loop():
            while self._blink_active:
                status = self._blink_colors[self._blink_idx % len(self._blink_colors)]
                try:
                    self._update_icon(status)
                except Exception:
                    pass
                self._blink_idx += 1
                time.sleep(interval)
        threading.Thread(target=_blink_loop, daemon=True).start()

    def _stop_blink(self, final_status: str = "ok"):
        """Stop blinking, set final icon."""
        self._blink_active = False
        time.sleep(0.1)
        self._update_icon(final_status)

    # ── Win32 Window + Message Loop ───────────────────────────

    def _create_window(self) -> int:
        wc = win32gui.WNDCLASS()
        wc.lpfnWndProc = {
            win32con.WM_DESTROY: self._on_destroy,
            win32con.WM_COMMAND: self._on_command,
            win32con.WM_USER + 20: self._on_tray_event,
        }
        wc.lpszClassName = "QTronTrayClass"
        wc.hInstance = win32api.GetModuleHandle(None)

        class_atom = win32gui.RegisterClass(wc)
        hwnd = win32gui.CreateWindow(
            class_atom, APP_NAME,
            win32con.WS_OVERLAPPED, 0, 0, 0, 0,
            0, 0, wc.hInstance, None,
        )
        return hwnd

    def _add_tray_icon(self):
        icon = self._create_icon()
        flags = win32gui.NIF_ICON | win32gui.NIF_MESSAGE | win32gui.NIF_TIP | win32gui.NIF_INFO
        nid = (
            self._hwnd, 0, flags,
            win32con.WM_USER + 20,
            icon,
            f"{APP_NAME} :{PORT}",  # tooltip
            f"REST Server running on :{PORT}\nRight-click for menu.",  # balloon text
            5,  # balloon timeout (seconds, min 5 on modern Windows)
            APP_NAME,  # balloon title
            win32gui.NIIF_INFO,  # balloon icon type
        )
        try:
            win32gui.Shell_NotifyIcon(win32gui.NIM_ADD, nid)
        except Exception as e:
            self._logger.error(f"[TRAY] Shell_NotifyIcon failed: {e}")

    def _remove_tray_icon(self):
        try:
            win32gui.Shell_NotifyIcon(
                win32gui.NIM_DELETE,
                (self._hwnd, 0),
            )
        except Exception:
            pass

    def _update_tooltip(self, text: str):
        try:
            nid = (
                self._hwnd, 0,
                win32gui.NIF_TIP,
                win32con.WM_USER + 20,
                0,
                text[:127],  # max 128 chars
            )
            win32gui.Shell_NotifyIcon(win32gui.NIM_MODIFY, nid)
        except Exception:
            pass

    def _show_balloon(self, title: str, msg: str):
        try:
            nid = (
                self._hwnd, 0,
                win32gui.NIF_INFO,
                win32con.WM_USER + 20,
                0,
                "",  # tip
                msg[:255],
                5,
                title[:63],
                win32gui.NIIF_INFO,
            )
            win32gui.Shell_NotifyIcon(win32gui.NIM_MODIFY, nid)
        except Exception:
            pass

    # ── Event Handlers ────────────────────────────────────────

    def _on_tray_event(self, hwnd, msg, wparam, lparam):
        if lparam == win32con.WM_RBUTTONUP:
            self._show_context_menu()
        elif lparam == win32con.WM_LBUTTONDBLCLK:
            self._action_status()
        return True

    def _on_command(self, hwnd, msg, wparam, lparam):
        cmd_id = win32api.LOWORD(wparam)
        actions = {
            ID_STATUS: self._action_status,
            ID_DASHBOARD: self._action_dashboard,
            ID_OPEN_LOG_DIR: self._action_open_log_dir,
            ID_BATCH_NOW: self._action_batch_now,
            ID_BATCH_TOGGLE: self._action_batch_toggle,
            ID_SELF_TEST_NOW: self._action_self_test_now,
            ID_RESTART: self._action_restart,
            ID_QUIT: self._action_quit,
            # US
            ID_US_DASHBOARD: self._action_us_dashboard,
            ID_US_BATCH: self._action_us_batch,
            ID_US_AUTO_TOGGLE: self._action_us_auto_toggle,
            ID_US_RESTART: self._action_us_restart,
            ID_US_LOG_DIR: self._action_us_log_dir,
            # KR Live (Gen4 trading engine, auto-attach via tray)
            ID_KR_LIVE_START: self._action_kr_live_start,
            ID_KR_LIVE_STOP: self._action_kr_live_stop,
            ID_KR_LIVE_RESTART: self._action_kr_live_restart,
            ID_KR_LIVE_AUTO_TOGGLE: self._action_kr_live_auto_toggle,
            ID_KR_LIVE_LOG: self._action_kr_live_log,
            # Global
            ID_UNIFIED: self._action_unified,
            ID_RESTART_ALL: self._action_restart_all,
            ID_GATE_OBSERVER: self._action_gate_observer,
        }
        action = actions.get(cmd_id)
        if action:
            action()
        return True

    def _on_destroy(self, hwnd, msg, wparam, lparam):
        # Cleanup: stop US server + KR server before exit
        self._running = False
        if self._is_us_live_alive():
            try:
                self._action_us_live_stop()
            except Exception:
                pass
        if self._is_us_process_alive():
            try:
                self._us_process.terminate()
                self._us_process.wait(timeout=3)
            except Exception:
                try:
                    self._us_process.kill()
                except Exception:
                    pass
            self._us_running = False
        self._stop_server()
        self._remove_tray_icon()
        win32gui.PostQuitMessage(0)
        return True

    def _show_context_menu(self):
        menu = win32gui.CreatePopupMenu()
        MF = win32con.MF_STRING
        MF_SEP = win32con.MF_SEPARATOR
        MF_GRAY = win32con.MF_GRAYED
        MF_POP = win32con.MF_POPUP

        # ── KR Market submenu ──
        kr_sub = win32gui.CreatePopupMenu()
        win32gui.AppendMenu(kr_sub, MF, ID_DASHBOARD, "Open Dashboard")
        win32gui.AppendMenu(kr_sub, MF, ID_OPEN_LOG_DIR, "Open Log Folder")
        win32gui.AppendMenu(kr_sub, MF_SEP, 0, "")
        from zoneinfo import ZoneInfo
        _kr_done = self._is_kr_batch_locked()
        _kr_weekend = datetime.now(ZoneInfo("Asia/Seoul")).weekday() >= 5
        _kr_market_open = not self._is_after_kr_close()
        _kr_locked = self._batch_running or _kr_done or _kr_market_open
        kr_batch_flags = MF | (MF_GRAY if _kr_locked else 0)
        kr_batch_label = "Run Batch Now" + (
            " (running...)" if self._batch_running else
            " (done today)" if _kr_done else
            " (주말)" if _kr_weekend else
            " (장 마감 전)" if _kr_market_open else ""
        )
        win32gui.AppendMenu(kr_sub, kr_batch_flags, ID_BATCH_NOW, kr_batch_label)
        kr_auto_label = f"Auto Batch [{'ON' if self._batch_auto else 'OFF'}]"
        win32gui.AppendMenu(kr_sub, MF, ID_BATCH_TOGGLE, kr_auto_label)
        st_flags = MF | (MF_GRAY if self._self_test_running else 0)
        st_label = "Run Self-Test Now" + (
            " (running...)" if self._self_test_running else
            " (done today)" if self._self_test_today_done else ""
        )
        win32gui.AppendMenu(kr_sub, st_flags, ID_SELF_TEST_NOW, st_label)
        win32gui.AppendMenu(kr_sub, MF_SEP, 0, "")
        # ── KR Live (Gen4 trading engine) controls ──
        _kr_live_alive = self._is_kr_live_alive()
        _kr_live_window = self._is_kr_trading_window()
        _kr_live_label = "KR Live: " + (
            "RUNNING" if _kr_live_alive else
            f"OFFLINE ({self._kr_live_last_exit_reason})" if self._kr_live_last_exit_reason else
            "OFFLINE (waiting for window)" if not _kr_live_window else
            "OFFLINE"
        )
        win32gui.AppendMenu(kr_sub, MF | MF_GRAY, 0, _kr_live_label)
        _kr_live_start_label = "Start KR Live" + ("" if _kr_live_window else " (after-hours)")
        win32gui.AppendMenu(
            kr_sub, MF | (MF_GRAY if _kr_live_alive else 0),
            ID_KR_LIVE_START, _kr_live_start_label,
        )
        win32gui.AppendMenu(
            kr_sub, MF | (0 if _kr_live_alive else MF_GRAY),
            ID_KR_LIVE_STOP, "Stop KR Live",
        )
        win32gui.AppendMenu(
            kr_sub, MF | (0 if _kr_live_alive else MF_GRAY),
            ID_KR_LIVE_RESTART, "Restart KR Live",
        )
        _kr_live_auto_label = f"KR Live Auto [{'ON' if self._kr_live_auto else 'OFF'}]"
        win32gui.AppendMenu(kr_sub, MF, ID_KR_LIVE_AUTO_TOGGLE, _kr_live_auto_label)
        win32gui.AppendMenu(kr_sub, MF, ID_KR_LIVE_LOG, "Open KR Live Log")
        win32gui.AppendMenu(kr_sub, MF_SEP, 0, "")
        kr_restart_flags = MF | (MF_GRAY if self._kr_restarting else 0)
        win32gui.AppendMenu(kr_sub, kr_restart_flags, ID_RESTART, "Restart Server")
        kr_status = self._get_kr_status()
        win32gui.AppendMenu(menu, MF_POP, kr_sub, f"KR Market [{kr_status}]")

        # ── US Market submenu ──
        us_sub = win32gui.CreatePopupMenu()
        win32gui.AppendMenu(us_sub, MF, ID_US_DASHBOARD, "Open Dashboard")
        win32gui.AppendMenu(us_sub, MF, ID_US_LOG_DIR, "Open Log Folder")
        win32gui.AppendMenu(us_sub, MF_SEP, 0, "")
        _us_done = self._is_us_batch_locked()
        _us_weekend = datetime.now(ZoneInfo("US/Eastern")).weekday() >= 5  # ZoneInfo already imported above
        _us_market_open = not self._is_after_us_close()
        _us_locked = self._us_batch_running or _us_done or _us_market_open
        us_batch_flags = MF | (MF_GRAY if _us_locked else 0)
        us_batch_label = "Run Batch Now" + (
            " (running...)" if self._us_batch_running else
            " (done today)" if _us_done else
            " (주말)" if _us_weekend else
            " (장 마감 전)" if _us_market_open else ""
        )
        win32gui.AppendMenu(us_sub, us_batch_flags, ID_US_BATCH, us_batch_label)
        us_auto_label = f"Auto Batch [{'ON' if self._us_batch_auto else 'OFF'}]"
        win32gui.AppendMenu(us_sub, MF, ID_US_AUTO_TOGGLE, us_auto_label)
        win32gui.AppendMenu(us_sub, MF_SEP, 0, "")
        us_restart_flags = MF | (MF_GRAY if self._us_restarting else 0)
        win32gui.AppendMenu(us_sub, us_restart_flags, ID_US_RESTART, "Restart Server")
        us_status = self._get_us_status()
        live_tag = " [Live]" if self._is_us_live_alive() else ""
        win32gui.AppendMenu(menu, MF_POP, us_sub, f"US Market [{us_status}]{live_tag}")

        # ── Global ──
        win32gui.AppendMenu(menu, MF_SEP, 0, "")
        win32gui.AppendMenu(menu, MF, ID_UNIFIED, "Open Unified Dashboard")
        win32gui.AppendMenu(menu, MF, ID_GATE_OBSERVER, "AUTO GATE Report (Latest)")
        win32gui.AppendMenu(menu, MF, ID_RESTART_ALL, "Restart All")
        win32gui.AppendMenu(menu, MF, ID_QUIT, "Shutdown All")

        # Required for menu to work properly
        win32gui.SetForegroundWindow(self._hwnd)
        pos = win32gui.GetCursorPos()
        win32gui.TrackPopupMenu(
            menu, win32con.TPM_LEFTALIGN,
            pos[0], pos[1], 0, self._hwnd, None,
        )
        win32gui.PostMessage(self._hwnd, win32con.WM_NULL, 0, 0)
        win32gui.DestroyMenu(menu)

    # ── Actions ───────────────────────────────────────────────

    def _action_status(self):
        uptime = self._uptime_str()
        log_size = self._get_log_size()
        xval = self._get_xval_summary()
        msg = f"Uptime: {uptime}\nPort: {PORT}\nLog: {log_size}\n{xval}"
        self._show_balloon("Q-TRON Status", msg)

    def _action_dashboard(self):
        webbrowser.open(f"http://localhost:{PORT}")

    def _action_xval(self):
        info = self._get_xval_summary()
        self._show_balloon("Q-TRON XVAL", info or "No XVAL data yet.")

    def _action_open_log(self):
        log_file = self._today_log()
        if log_file.exists():
            os.startfile(str(log_file))
        else:
            self._show_balloon("Q-TRON", "No log file for today.")

    def _action_open_log_dir(self):
        if LOG_DIR.exists():
            os.startfile(str(LOG_DIR))

    def _action_copy_log(self):
        path_str = str(self._today_log())
        try:
            subprocess.run(
                ["clip"], input=path_str.encode("utf-8"), timeout=3,
                creationflags=_NO_WINDOW,
            )
            self._show_balloon("Q-TRON", f"Copied:\n{path_str}")
        except Exception as e:
            self._show_balloon("Q-TRON", f"Copy failed: {e}")

    def _action_batch_now(self):
        """Run batch immediately in background thread."""
        if self._batch_running:
            self._show_balloon("Q-TRON", "Batch already running.")
            return
        threading.Thread(target=self._run_batch, daemon=True).start()

    def _action_batch_toggle(self):
        """Toggle auto-batch on/off."""
        self._batch_auto = not self._batch_auto
        state = "ON" if self._batch_auto else "OFF"
        self._logger.info(f"[TRAY] Auto-batch {state}")
        self._show_balloon("Q-TRON", f"Auto Batch: {state}\n({BATCH_HOUR:02d}:{BATCH_MINUTE:02d} daily)")

    def _action_self_test_now(self):
        """Run PG ping self-test immediately (manual trigger)."""
        if self._self_test_running:
            self._show_balloon("Q-TRON", "Self-test already running.")
            return
        self._logger.info("[TRAY_SELF_TEST] Manual trigger")
        threading.Thread(target=self._run_self_test, daemon=True).start()

    def _run_backup(self):
        """Execute daily backup (background thread)."""
        self._backup_running = True
        self._logger.info("[BACKUP_START] Daily backup started")
        try:
            import sys as _sys
            _sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backup"))  # audit:allow-syspath: backup/ subdir is outside both kr/us trees
            from daily_backup import run_backup
            ok, summary = run_backup()
            if ok:
                self._logger.info(f"[BACKUP_DONE] {summary[:100]}")
                self._show_balloon("Q-TRON Backup", "Daily backup completed")
            else:
                self._logger.error(f"[BACKUP_FAIL] {summary[:200]}")
                self._show_balloon("Q-TRON Backup", "BACKUP FAILED — check logs")
        except Exception as e:
            self._logger.error(f"[BACKUP_ERROR] {e}")
            self._show_balloon("Q-TRON Backup", f"Backup error: {e}")
        finally:
            self._backup_running = False
            self._backup_today_done = True

    def _run_self_test(self):
        """Daily PG ping. Logs OK/FAIL and alerts Telegram on failure."""
        self._self_test_running = True
        t0 = time.time()
        try:
            from shared.db.pg_base import connection
            with connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            elapsed_ms = int((time.time() - t0) * 1000)
            self._logger.info(
                f"[TRAY_SELF_TEST_OK] db=kr elapsed_ms={elapsed_ms}"
            )
        except Exception as e:
            elapsed_ms = int((time.time() - t0) * 1000)
            self._logger.error(
                f"[TRAY_SELF_TEST_FAIL] db=kr elapsed_ms={elapsed_ms} err={e}"
            )
            try:
                import requests
                from dotenv import load_dotenv
                env_path = Path(__file__).resolve().parent / ".env"
                load_dotenv(env_path)
                bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
                chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
                if bot_token and chat_id:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    text = (
                        f"⚠️ Q-TRON KR self-test FAIL\n"
                        f"시간: {ts}\n"
                        f"elapsed_ms: {elapsed_ms}\n"
                        f"err: {str(e)[:200]}"
                    )
                    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                    requests.post(url, json={
                        "chat_id": chat_id,
                        "text": text,
                        "disable_web_page_preview": True,
                    }, timeout=5)
            except Exception as _te:
                self._logger.warning(
                    f"[TRAY_SELF_TEST_ALERT_ERR] {_te}"
                )
        finally:
            self._self_test_running = False
            self._self_test_today_done = True

    def _run_batch(self):
        """Execute batch (background thread)."""
        self._batch_running = True
        self._logger.info("[BATCH_START] Batch execution started")
        self._show_balloon("Q-TRON Batch", "Batch started...")
        self._start_blink(["ok", "batch"], interval=0.7)

        try:
            # Guard: project root has a legacy config.py (QTronConfig / Gen2) that
            # shadows kr/config.py (Gen4Config) if sys.modules has cached it.
            # _bootstrap_path puts kr/ first in sys.path, but sys.modules can
            # still hold a stale root reference from earlier imports. Evict and
            # re-resolve to guarantee Gen4Config is found.
            _stale = sys.modules.pop("config", None)
            if _stale is not None:
                _stale_file = str(getattr(_stale, "__file__", "") or "").replace("/", "\\")
                _expected = str(Path(__file__).resolve().parent / "config.py").replace("/", "\\")
                if _stale_file and _stale_file != _expected:
                    self._logger.warning(
                        f"[BATCH_IMPORT_GUARD] evicted stale config from {_stale_file}"
                    )
            from config import Gen4Config
            from lifecycle.batch import run_batch

            config = Gen4Config()
            config.ensure_dirs()
            result = run_batch(config, fast=True)

            if result:
                n = len(result.get("target_tickers", []))
                self._logger.info(f"[BATCH_DONE] {n} target stocks selected")
                self._show_balloon("Q-TRON Batch",
                                   f"Batch complete: {n} target stocks")
            else:
                self._logger.warning("[BATCH_DONE] No result")
                self._show_balloon("Q-TRON Batch", "Batch complete (no result)")

            self._batch_today_done = True
            from zoneinfo import ZoneInfo
            _kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            _kst_today = _kst_now.date()
            self._batch_last_done_date = _kst_today
            self._batch_last_fail_at = None  # 성공 → backoff 해제

            # ── KR Lab EOD 복구 arm ────────────────────────────────
            # Lab EOD auto는 15:35 KST 윈도우 + MAX_FAILS(3)로 abandoned 될 수 있음
            # (예: 배치 지연/재시작으로 Lab EOD 실행 시점에 데이터 준비 안 됨).
            # 배치가 방금 성공했으므로 "데이터 freshness" 조건은 재충족 — 당일 중
            # Lab EOD를 재개할 수 있는 상태로 복원한다.
            #
            # 건드리는 3 상태:
            #   - done(today, fail<MAX): 이미 성공 → 건드리지 않음
            #   - done(today, fail>=MAX): abandoned → 마커 클리어
            #   - not done today: retry_until을 지금으로 세팅 → 다음 30s tick 트리거
            try:
                _is_abandoned = (
                    self._kr_lab_eod_last_done_date == _kst_today
                    and self._kr_lab_eod_fail_count >= LAB_EOD_MAX_FAILS
                )
                _not_yet_today = self._kr_lab_eod_last_done_date != _kst_today
                if _is_abandoned or _not_yet_today:
                    if _is_abandoned:
                        self._kr_lab_eod_last_done_date = None
                        self._kr_lab_eod_fail_count = 0
                    self._kr_lab_eod_retry_until = _kst_now
                    self._logger.info(
                        f"[KR_LAB_EOD_POST_BATCH_ARM] "
                        f"abandoned={_is_abandoned} arming retry at "
                        f"{_kst_now.isoformat(timespec='seconds')}"
                    )
            except Exception as _ae:
                self._logger.warning(f"[KR_LAB_EOD_POST_BATCH_ARM_ERR] {_ae}")

            # AUTO GATE advisory observation - single producer hook (post-EOD).
            # Writes logs/gate_observer/YYYYMMDD.json and (if diff) sends Telegram.
            try:
                # sys.path already prepared by _bootstrap_path at top of file
                from tools.gate_observer import run_today as _gate_run
                _payload = _gate_run(send_telegram=True, logger_override=self._logger)
                if _payload is None:
                    self._logger.info("[GATE_OBSERVER] skipped (already ran today)")
                else:
                    self._logger.info(
                        "[GATE_OBSERVER] c_stage_ready=%s streak=%d/%d",
                        _payload["decision_flags"]["c_stage_ready"],
                        _payload["c_stage_streak"],
                        _payload["c_stage_streak_required"],
                    )
            except Exception as _ge:
                self._logger.warning(f"[GATE_OBSERVER] producer failed: {_ge}")

        except Exception as e:
            self._batch_last_fail_at = datetime.now()  # backoff 기준점
            self._logger.error(f"[BATCH_FAIL] {e}")
            import traceback
            self._logger.error(traceback.format_exc())
            self._show_balloon("Q-TRON Batch", f"Batch FAILED: {e}")
            self._stop_blink("error")
            return
        finally:
            self._batch_running = False
            if hasattr(self, '_blink_active') and self._blink_active:
                self._stop_blink("ok")

    def _action_restart(self):
        if self._kr_restarting:
            return
        self._kr_restarting = True
        self._logger.info("[TRAY] KR Restart requested")
        self._show_balloon("Q-TRON KR", "Restarting KR server...")
        try:
            self._stop_server()
            # uvicorn은 in-process이므로 _running 플래그로 확인
            for _ in range(10):
                if not self._running:
                    break
                time.sleep(0.5)
            self._start_server()
            self._show_balloon("Q-TRON KR", "KR server restarted.")
        finally:
            self._kr_restarting = False

    def _action_us_restart(self):
        if self._us_restarting:
            return
        self._us_restarting = True
        self._logger.info("[TRAY] US Restart requested")
        self._show_balloon("Q-TRON US", "Restarting US server...")
        try:
            self._action_us_stop()
            if not self._wait_process_exit(self._is_us_process_alive, timeout=5.0):
                self._logger.warning("[US] Process did not exit within 5s")
            self._action_us_start()
            self._show_balloon("Q-TRON US", "US server restarted.")
        finally:
            self._us_restarting = False

    def _action_restart_all(self):
        if self._kr_restarting or self._us_restarting:
            self._show_balloon("Q-TRON", "Restart already in progress.")
            return
        self._kr_restarting = True
        self._us_restarting = True
        self._logger.info("[TRAY] Restart All requested")
        self._show_balloon("Q-TRON", "Restarting all servers...")
        try:
            # Stop both
            self._action_us_stop()
            self._stop_server()
            # Wait for actual exit
            self._wait_process_exit(self._is_us_process_alive, timeout=5.0)
            for _ in range(10):
                if not self._running:
                    break
                time.sleep(0.5)
            # Start both
            self._start_server()
            self._action_us_start()
            self._show_balloon("Q-TRON", "All servers restarted.")
        finally:
            self._kr_restarting = False
            self._us_restarting = False

    def _action_us_log_dir(self):
        us_log_dir = US_BASE_DIR / "logs"
        if us_log_dir.exists():
            os.startfile(str(us_log_dir))
        elif LOG_DIR.exists():
            # Fallback to KR log dir
            os.startfile(str(LOG_DIR))
            self._show_balloon("Q-TRON US", f"US log folder not found.\nOpened KR logs instead.")
        else:
            self._show_balloon("Q-TRON US", "Log folder not found.")

    def _action_quit(self):
        self._logger.info("[TRAY] Shutdown All requested")
        self._send_shutdown_alert("Shutdown All requested (manual)")
        self._running = False
        # Stop KR live engine first (stops trail-stop monitoring + EOD)
        if self._is_kr_live_alive():
            self._action_kr_live_stop()
        # Stop US live
        if self._is_us_live_alive():
            self._action_us_live_stop()
        # Stop US server
        if self._is_us_process_alive():
            self._action_us_stop()
        self._stop_server()
        win32gui.DestroyWindow(self._hwnd)

    def _send_shutdown_alert(self, reason: str = "unknown"):
        """서버 종료 시 텔레그램 알림. 중복 전송 방지."""
        if self._shutdown_sent:
            return
        self._shutdown_sent = True
        try:
            import requests
            from dotenv import load_dotenv
            import os
            env_path = Path(__file__).resolve().parent / ".env"
            load_dotenv(env_path)
            bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
            if not bot_token or not chat_id:
                self._logger.warning("[TRAY] Telegram credentials not set")
                return

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            text = (
                f"🚨 Q-TRON 서버 종료\n"
                f"시간: {ts}\n"
                f"사유: {reason}"
            )
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            resp = requests.post(url, json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }, timeout=5)

            if resp.status_code == 200:
                self._logger.info(f"[TRAY] Shutdown alert sent: {reason}")
            else:
                self._logger.warning(f"[TRAY] Shutdown alert failed: HTTP {resp.status_code}")
        except Exception as e:
            self._logger.warning(f"[TRAY] Shutdown alert error: {e}")

    # ── US Server Actions ────────────────────────────────────────

    def _action_us_dashboard(self):
        webbrowser.open(f"http://localhost:{US_PORT}")

    def _action_us_start(self):
        if self._us_running:
            self._show_balloon("Q-TRON US", "US server already running.")
            return
        if not US_PYTHON.exists():
            self._show_balloon("Q-TRON US", f"Python not found:\n{US_PYTHON}")
            return

        # 포트 선점 프로세스 강제 종료 (KR과 동일 패턴)
        self._kill_port_user(US_PORT)

        # stderr를 로그 파일로 리다이렉트 (DEVNULL 금지 — 실패 원인 가림 방지)
        us_logs_dir = US_BASE_DIR / "logs"
        try:
            us_logs_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        stderr_path = us_logs_dir / f"us_tray_stderr_{time.strftime('%Y%m%d')}.log"

        # 깨끗한 env: 부모(KR venv) 변수 제거 — US venv와 충돌 방지
        clean_env = dict(os.environ)
        for var in ("PYTHONPATH", "VIRTUAL_ENV", "PYTHONHOME"):
            clean_env.pop(var, None)

        try:
            stderr_fh = open(stderr_path, "a", encoding="utf-8", buffering=1)
            stderr_fh.write(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S')} US server start =====\n")
            self._us_process = subprocess.Popen(
                [str(US_PYTHON), "-X", "utf8", "-m", "uvicorn",
                 "web.app:app", "--host", HOST, "--port", str(US_PORT)],
                cwd=str(US_BASE_DIR),
                stdout=stderr_fh,
                stderr=stderr_fh,
                env=clean_env,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            self._us_stderr_fh = stderr_fh  # keep reference so GC doesn't close

            # spawn 후 health check: 2초 대기 후 poll() 로 즉사 감지
            time.sleep(2.0)
            rc = self._us_process.poll()
            if rc is not None:
                # 프로세스가 이미 죽음 — stderr 마지막 줄 표시
                tail_msg = "(no stderr captured)"
                try:
                    stderr_fh.flush()
                    with open(stderr_path, "r", encoding="utf-8", errors="replace") as _rf:
                        _lines = _rf.readlines()[-10:]
                        tail_msg = "".join(_lines).strip()[-300:]
                except Exception:
                    pass
                self._us_running = False
                self._us_process = None
                self._logger.error(f"[US] Start failed (rc={rc}). Tail: {tail_msg}")
                self._show_balloon(
                    "Q-TRON US",
                    f"Start failed (exit {rc}). See {stderr_path.name}",
                )
                try:
                    from web.data_events import emit_event, Level
                    emit_event(
                        source="STARTUP.us",
                        level=Level.CRITICAL,
                        code="spawn_failed",
                        message=f"US server spawn 실패 (exit {rc})",
                        details={"rc": rc, "stderr_tail": tail_msg, "log_file": str(stderr_path)},
                        telegram=True,
                    )
                except Exception:
                    pass
                return

            self._us_running = True
            self._logger.info(f"[US] Server started on :{US_PORT} (PID {self._us_process.pid})")
            self._show_balloon("Q-TRON US", f"US server started on :{US_PORT}")
            # Auto-start Live (항상 켜야 함: trail stop, fill 감시)
            threading.Timer(3.0, self._action_us_live_start).start()
        except Exception as e:
            self._logger.error(f"[US] Start failed: {e}")
            self._show_balloon("Q-TRON US", f"Start failed: {e}")

    def _action_us_stop(self):
        if not self._us_running or not self._us_process:
            self._show_balloon("Q-TRON US", "US server not running.")
            return
        # Stop Live first (trail stop 감시 중단)
        if self._us_live_running:
            self._action_us_live_stop()
        try:
            self._us_process.terminate()
            self._us_process.wait(timeout=5)
        except Exception:
            try:
                self._us_process.kill()
            except Exception:
                pass
        self._us_running = False
        self._us_process = None
        self._logger.info("[US] Server + Live stopped")
        self._show_balloon("Q-TRON US", "US server + live stopped.")

    def _action_us_batch(self):
        """Run US batch in background."""
        if self._us_batch_running:
            self._show_balloon("Q-TRON US", "Batch already running...")
            return
        if not US_PYTHON.exists():
            self._show_balloon("Q-TRON US", f"Python not found:\n{US_PYTHON}")
            return
        self._us_batch_running = True
        self._logger.info("[US_BATCH] Manual batch started")
        self._show_balloon("Q-TRON US", "US batch starting...")

        def _run():
            try:
                result = subprocess.run(
                    [str(US_PYTHON), "-X", "utf8", "main.py", "--batch"],
                    cwd=str(US_BASE_DIR),
                    capture_output=True, text=True, timeout=600,
                )
                if result.returncode == 0:
                    self._logger.info("[US_BATCH] Complete")
                    self._show_balloon("Q-TRON US", "Batch complete!")
                    from zoneinfo import ZoneInfo
                    self._us_batch_last_done_date = datetime.now(ZoneInfo("US/Eastern")).date()
                else:
                    self._logger.error(f"[US_BATCH] Failed: {result.stderr[:200]}")
                    self._show_balloon("Q-TRON US", f"Batch failed:\n{result.stderr[:100]}")
            except Exception as e:
                self._logger.error(f"[US_BATCH] Error: {e}")
                self._show_balloon("Q-TRON US", f"Batch error: {e}")
            finally:
                self._us_batch_running = False

        threading.Thread(target=_run, daemon=True).start()

    def _action_us_auto_toggle(self):
        """Toggle US auto batch/rebal."""
        self._us_batch_auto = not self._us_batch_auto
        state = "ON" if self._us_batch_auto else "OFF"
        self._logger.info(f"[TRAY] US Auto Batch/Rebal {state}")
        self._show_balloon("Q-TRON US", f"Auto Batch/Rebal: {state}")

    def _load_kr_batch_done_date(self) -> Optional[date]:
        """시작 시 KR batch 완료일 복원 (오늘 target_portfolio 파일 존재 확인)."""
        try:
            from zoneinfo import ZoneInfo
            kst_today = datetime.now(ZoneInfo("Asia/Seoul")).date()
            today_str = kst_today.strftime("%Y%m%d")
            signals_dir = BASE_DIR / "data" / "signals"
            if (signals_dir / f"target_portfolio_{today_str}.json").exists():
                return kst_today
        except Exception:
            pass
        return None

    def _load_us_batch_done_date(self) -> Optional[date]:
        """시작 시 US batch 완료일 복원 — post-close 배치만 '완료'로 간주."""
        try:
            from zoneinfo import ZoneInfo
            import json as _json
            et_today = datetime.now(ZoneInfo("US/Eastern")).date()
            rt_path = US_BASE_DIR / "state" / "runtime_state_us_paper.json"
            if not rt_path.exists():
                rt_path = US_BASE_DIR / "state" / "runtime_state_us_live.json"
            if rt_path.exists():
                rt = _json.loads(rt_path.read_text(encoding="utf-8"))
                last_bd = rt.get("last_batch_business_date", "")
                created_at = rt.get("snapshot_created_at", "")
                if last_bd == et_today.strftime("%Y-%m-%d") and created_at:
                    try:
                        created = datetime.fromisoformat(created_at)
                        if created.tzinfo is None:
                            created = created.replace(tzinfo=ZoneInfo("US/Eastern"))
                        created_et = created.astimezone(ZoneInfo("US/Eastern"))
                        close_et = datetime(
                            et_today.year, et_today.month, et_today.day,
                            16, 0, 0, tzinfo=ZoneInfo("US/Eastern"),
                        )
                        if created_et >= close_et:
                            return et_today
                    except Exception:
                        pass
        except Exception:
            pass
        return None

    # ── Lab EOD Loaders (v4.1) ────────────────────────────────

    def _load_kr_lab_eod_done_date(self) -> Optional[date]:
        """KR Lab Live head.json::last_run_date 기준 오늘 완료 여부 복원."""
        try:
            import json as _json
            kst_today = self._now_kst().date()
            head_path = BASE_DIR / "data" / "lab_live" / "head.json"
            if head_path.exists():
                head = _json.loads(head_path.read_text(encoding="utf-8"))
                last_run = head.get("last_run_date", "")
                if last_run == kst_today.strftime("%Y-%m-%d"):
                    self._logger.info(
                        f"[KR_LAB_EOD_AUTO_LOAD_DONE] persisted_date={last_run}"
                    )
                    return kst_today
        except Exception:
            pass
        return None

    def _load_us_lab_eod_done_date(self) -> Optional[date]:
        """US Lab Forward meta.json::last_successful_eod_date 기준 복원."""
        try:
            import json as _json
            et_today = self._now_et().date()
            meta_path = US_BASE_DIR / "lab" / "state" / "forward" / "meta.json"
            if meta_path.exists():
                meta = _json.loads(meta_path.read_text(encoding="utf-8"))
                last_eod = meta.get("last_successful_eod_date", "")
                if last_eod == et_today.strftime("%Y-%m-%d"):
                    self._logger.info(
                        f"[US_LAB_EOD_AUTO_LOAD_DONE] persisted_date={last_eod}"
                    )
                    return et_today
        except Exception:
            pass
        return None

    # ── Lab EOD Log/Fail Recorders (v4.1) ─────────────────────

    def _log_kr_lab_done_final(self, reason: str) -> None:
        """KR Lab EOD 종결 로그. reason ∈ {success, skipped, skipped_unexpected, abandoned}"""
        self._logger.info(
            f"[KR_LAB_EOD_AUTO_DONE_FINAL] date={self._kr_lab_eod_last_done_date} "
            f"reason={reason} fail_count={self._kr_lab_eod_fail_count}"
        )

    def _log_us_lab_done_final(self, reason: str) -> None:
        self._logger.info(
            f"[US_LAB_EOD_AUTO_DONE_FINAL] date={self._us_lab_eod_last_done_date} "
            f"reason={reason} fail_count={self._us_lab_eod_fail_count}"
        )

    def _record_kr_lab_fail(self, reason: str) -> None:
        """KR Lab EOD 실패 기록. MAX 도달 시 당일 포기 처리 (done_date=today)."""
        self._kr_lab_eod_fail_count += 1
        kst_today = self._now_kst().date()
        if self._kr_lab_eod_fail_count >= LAB_EOD_MAX_FAILS:
            self._kr_lab_eod_last_done_date = kst_today  # 포기 = 당일 처리 완료
            self._logger.warning(
                f"[KR_LAB_EOD_AUTO_ABANDONED] "
                f"fail={self._kr_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS} "
                f"date={kst_today} reason={reason[:120]}"
            )
            try:
                from notify.telegram_bot import send
                send(
                    f"⚠️ <b>KR Lab EOD Auto-run Abandoned</b>\n"
                    f"Date: {kst_today}\n"
                    f"Fails: {self._kr_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS}\n"
                    f"Last reason: {reason[:100]}",
                    severity="WARN",
                )
            except Exception:
                pass
            self._log_kr_lab_done_final("abandoned")
        else:
            self._kr_lab_eod_retry_until = (
                self._now_kst() + timedelta(minutes=LAB_EOD_RETRY_BACKOFF_MIN)
            )
            self._logger.warning(
                f"[KR_LAB_EOD_AUTO_FAIL] "
                f"fail={self._kr_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS} "
                f"reason={reason[:120]} retry_after={LAB_EOD_RETRY_BACKOFF_MIN}min"
            )

    def _record_us_lab_fail(self, reason: str) -> None:
        self._us_lab_eod_fail_count += 1
        et_today = self._now_et().date()
        if self._us_lab_eod_fail_count >= LAB_EOD_MAX_FAILS:
            self._us_lab_eod_last_done_date = et_today
            self._logger.warning(
                f"[US_LAB_EOD_AUTO_ABANDONED] "
                f"fail={self._us_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS} "
                f"date={et_today} reason={reason[:120]}"
            )
            try:
                from notify.telegram_bot import send
                send(
                    f"⚠️ <b>US Lab EOD Auto-run Abandoned</b>\n"
                    f"Date: {et_today}\n"
                    f"Fails: {self._us_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS}\n"
                    f"Last reason: {reason[:100]}",
                    severity="WARN",
                )
            except Exception:
                pass
            self._log_us_lab_done_final("abandoned")
        else:
            self._us_lab_eod_retry_until = (
                self._now_et() + timedelta(minutes=LAB_EOD_RETRY_BACKOFF_MIN)
            )
            self._logger.warning(
                f"[US_LAB_EOD_AUTO_FAIL] "
                f"fail={self._us_lab_eod_fail_count}/{LAB_EOD_MAX_FAILS} "
                f"reason={reason[:120]} retry_after={LAB_EOD_RETRY_BACKOFF_MIN}min"
            )

    # ── Lab EOD Run Methods (v4.1) ────────────────────────────

    def _run_kr_lab_eod(self):
        """KR Lab Live EOD (9전략) — localhost API 호출."""
        if not self._kr_lab_eod_lock.acquire(blocking=False):
            return
        try:
            kst_today = self._now_kst().date()
            self._maybe_reset_kr_lab_eod_fail(kst_today)
            self._logger.info(
                f"[KR_LAB_EOD_AUTO_START] fail_count={self._kr_lab_eod_fail_count}"
            )
            import requests as _req

            # Health check
            try:
                h = _req.get(f"http://localhost:{PORT}/api/health", timeout=3)
                h.raise_for_status()
            except Exception as he:
                self._record_kr_lab_fail(f"health: {he}")
                return

            # 1. 시뮬레이터 init (idempotent)
            try:
                _req.post(
                    f"http://localhost:{PORT}/api/lab/live/start",
                    json={}, timeout=30,
                )
            except Exception as se:
                self._record_kr_lab_fail(f"start: {se}")
                return

            # 2. Run daily
            try:
                r = _req.post(
                    f"http://localhost:{PORT}/api/lab/live/run-daily",
                    json={"update_ohlcv": True}, timeout=180,
                )
            except Exception as re_:
                self._record_kr_lab_fail(f"run-daily: {re_}")
                return

            if not r.ok:
                self._record_kr_lab_fail(f"HTTP {r.status_code}")
                return

            try:
                result = r.json()
            except Exception as je:
                self._record_kr_lab_fail(f"json decode: {je}")
                return

            if result.get("ok"):
                self._kr_lab_eod_last_done_date = kst_today
                self._logger.info(
                    f"[KR_LAB_EOD_AUTO_MARK_DONE] memory_date={kst_today} "
                    f"trades={result.get('trades', 0)}"
                )
                self._log_kr_lab_done_final("success")
                self._check_kr_lab_data_quality(result, kst_today)
            elif result.get("skipped"):
                self._kr_lab_eod_last_done_date = kst_today
                if self._kr_lab_eod_fail_count == 0:
                    self._logger.info("[KR_LAB_EOD_AUTO_SKIPPED] already done")
                    self._log_kr_lab_done_final("skipped")
                else:
                    # Retry 중 skipped는 예상 외 — sync mismatch 감지
                    self._logger.warning(
                        f"[KR_LAB_EOD_AUTO_SKIPPED_UNEXPECTED] "
                        f"memory={self._kr_lab_eod_last_done_date} api_skipped=True "
                        f"snapshot={result.get('snapshot_version', '')}"
                    )
                    self._log_kr_lab_done_final("skipped_unexpected")
            else:
                self._record_kr_lab_fail(
                    f"unexpected response: {str(result)[:100]}"
                )
        finally:
            self._kr_lab_eod_lock.release()

    def _run_us_lab_eod(self):
        """US Lab Forward EOD (10전략) — localhost US server API 호출."""
        if not self._us_lab_eod_lock.acquire(blocking=False):
            return
        try:
            et_today = self._now_et().date()
            self._maybe_reset_us_lab_eod_fail(et_today)
            self._logger.info(
                f"[US_LAB_EOD_AUTO_START] fail_count={self._us_lab_eod_fail_count}"
            )
            import requests as _req

            # 1. 포트폴리오 init
            try:
                _req.post(
                    f"http://localhost:{US_PORT}/api/lab/forward/start",
                    json={}, timeout=30,
                )
            except Exception as se:
                self._record_us_lab_fail(f"start: {se}")
                return

            # 2. EOD 실행
            try:
                r = _req.post(
                    f"http://localhost:{US_PORT}/api/lab/forward/eod",
                    json={}, timeout=180,
                )
            except Exception as re_:
                self._record_us_lab_fail(f"eod: {re_}")
                return

            if not r.ok:
                self._record_us_lab_fail(f"HTTP {r.status_code}")
                return

            try:
                result = r.json()
            except Exception as je:
                self._record_us_lab_fail(f"json decode: {je}")
                return

            # US API의 정상 응답은 status code 200 + ok=True or skipped 필드
            # forward.py run_eod() 구현에 따라 키 이름은 다를 수 있으나,
            # status code 200이면 일단 성공으로 간주.
            if result.get("error"):
                self._record_us_lab_fail(
                    f"error field: {str(result.get('error'))[:100]}"
                )
                return

            self._us_lab_eod_last_done_date = et_today
            if result.get("skipped") and self._us_lab_eod_fail_count > 0:
                self._logger.warning(
                    f"[US_LAB_EOD_AUTO_SKIPPED_UNEXPECTED] "
                    f"memory={self._us_lab_eod_last_done_date} "
                    f"result={str(result)[:120]}"
                )
                self._log_us_lab_done_final("skipped_unexpected")
            else:
                self._logger.info(
                    f"[US_LAB_EOD_AUTO_MARK_DONE] memory_date={et_today} "
                    f"result={str(result)[:120]}"
                )
                self._log_us_lab_done_final(
                    "skipped" if result.get("skipped") else "success"
                )
                if not result.get("skipped"):
                    self._check_us_lab_data_quality(result, et_today)
        finally:
            self._us_lab_eod_lock.release()

    def _check_kr_lab_data_quality(self, result: dict, expected_date) -> None:
        """KR Lab EOD 응답 데이터 품질 감지 → alert.

        source="lab_live", reason으로 상태 구분.
        날짜 비교: date.fromisoformat() 사용 (문자열 비교 금지).
        Schema 누락: warning 로그 (silent 방지).
        """
        try:
            from notify.helpers import alert_data_failure
        except Exception:
            return

        if "selected_source" not in result or "data_last_date" not in result:
            self._logger.warning(
                "[KR_LAB_DATA_QUALITY_SKIP] selected_source or data_last_date missing "
                f"keys={list(result.keys())}"
            )
            return

        selected_source    = result.get("selected_source", "")
        data_last_date_raw = result.get("data_last_date", "")
        snapshot           = str(result.get("snapshot_version", ""))[:60]

        from datetime import date as _date
        try:
            data_date = _date.fromisoformat(str(data_last_date_raw)) if data_last_date_raw else None
        except (ValueError, TypeError) as e:
            self._logger.warning(
                f"[KR_LAB_DATA_QUALITY_PARSE_FAIL] data_last_date='{data_last_date_raw}': {e}"
            )
            data_date = None

        if selected_source == "CSV":
            alert_data_failure(
                "lab_live",
                "CSV fallback: DB stale",
                {"data_last_date": data_last_date_raw,
                 "snapshot": snapshot,
                 "expected": str(expected_date)},
            )

        if data_date is not None and data_date < expected_date:
            alert_data_failure(
                "lab_live",
                f"stale data: {data_date} < {expected_date}",
                {"source": selected_source, "snapshot": snapshot},
            )

    def _check_us_lab_data_quality(self, result: dict, expected_date) -> None:
        """US Lab EOD 응답 데이터 품질 감지 → alert."""
        try:
            from notify.helpers import alert_data_failure
        except Exception:
            return

        if "selected_source" not in result or "data_last_date" not in result:
            self._logger.warning(
                "[US_LAB_DATA_QUALITY_SKIP] selected_source or data_last_date missing "
                f"keys={list(result.keys())}"
            )
            return

        data_last_date_raw = result.get("data_last_date", "")
        selected_source    = result.get("selected_source", "")

        from datetime import date as _date
        try:
            data_date = _date.fromisoformat(str(data_last_date_raw)) if data_last_date_raw else None
        except (ValueError, TypeError) as e:
            self._logger.warning(
                f"[US_LAB_DATA_QUALITY_PARSE_FAIL] data_last_date='{data_last_date_raw}': {e}"
            )
            data_date = None

        if data_date is not None and data_date < expected_date:
            alert_data_failure(
                "us_lab",
                f"stale data: {data_date} < {expected_date}",
                {"source": selected_source},
            )

    def _is_kr_batch_locked(self) -> bool:
        """KR batch 완료 후 다음 장 마감(15:30 KST) 전까지 버튼 비활성."""
        if self._batch_last_done_date is None:
            return False
        try:
            from zoneinfo import ZoneInfo
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            unlock_dt = datetime(
                self._batch_last_done_date.year,
                self._batch_last_done_date.month,
                self._batch_last_done_date.day,
                15, 30, 0,
                tzinfo=ZoneInfo("Asia/Seoul"),
            ) + timedelta(days=1)
            return kst_now < unlock_dt
        except Exception:
            return False

    def _is_us_batch_locked(self) -> bool:
        """US batch 완료 후 다음 장 마감(15:30 ET) 전까지 버튼 비활성."""
        if self._us_batch_last_done_date is None:
            return False
        try:
            from zoneinfo import ZoneInfo
            et_now = datetime.now(ZoneInfo("US/Eastern"))
            unlock_dt = datetime(
                self._us_batch_last_done_date.year,
                self._us_batch_last_done_date.month,
                self._us_batch_last_done_date.day,
                15, 30, 0,
                tzinfo=ZoneInfo("US/Eastern"),
            ) + timedelta(days=1)
            return et_now < unlock_dt
        except Exception:
            return False

    def _is_us_batch_time(self) -> bool:
        """US/Eastern 기준 장 마감(16:00) 이후 윈도우.
        실제 중복 실행은 _us_batch_last_done_date / API status로 차단."""
        try:
            from zoneinfo import ZoneInfo
            et_now = datetime.now(ZoneInfo("US/Eastern"))
            if et_now.weekday() >= 5:  # weekend
                return False
            # 16:05 ET 이후 하루 내내 허용 (already-done 가드로 중복 방지)
            return (et_now.hour > 16) or (et_now.hour == 16 and et_now.minute >= 5)
        except Exception:
            return False

    def _is_after_kr_close(self) -> bool:
        """KR 장 마감(15:30 KST) 이후인가 — weekday only."""
        try:
            from zoneinfo import ZoneInfo
            kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
            if kst_now.weekday() >= 5:
                return False
            return (kst_now.hour > 15) or (kst_now.hour == 15 and kst_now.minute >= 30)
        except Exception:
            return False

    def _is_after_us_close(self) -> bool:
        """US 장 마감(16:00 ET) 이후인가 — weekday only."""
        try:
            from zoneinfo import ZoneInfo
            et_now = datetime.now(ZoneInfo("US/Eastern"))
            if et_now.weekday() >= 5:
                return False
            return et_now.hour >= 16
        except Exception:
            return False

    # ── Lab EOD Auto-Schedule Helpers (v4.1) ──────────────────

    def _now_kst(self) -> datetime:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Seoul"))

    def _now_et(self) -> datetime:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("US/Eastern"))

    def _is_within_window(self, now: datetime, target_h: int, target_m: int,
                          window_sec: int) -> bool:
        """now가 today의 target_h:target_m ~ +window_sec 범위 내인지."""
        from datetime import time as _time
        target = datetime.combine(now.date(), _time(target_h, target_m),
                                  tzinfo=now.tzinfo)
        delta = (now - target).total_seconds()
        return 0 <= delta < window_sec

    def _maybe_reset_kr_lab_eod_fail(self, today: date) -> None:
        if self._kr_lab_eod_fail_count_date != today:
            self._kr_lab_eod_fail_count = 0
            self._kr_lab_eod_fail_count_date = today
            self._kr_lab_eod_retry_until = None

    def _maybe_reset_us_lab_eod_fail(self, today: date) -> None:
        if self._us_lab_eod_fail_count_date != today:
            self._us_lab_eod_fail_count = 0
            self._us_lab_eod_fail_count_date = today
            self._us_lab_eod_retry_until = None

    def _us_api(self, path, method="GET", json_body=None, timeout=10):
        """US server API helper."""
        import requests as _req
        url = f"http://localhost:{US_PORT}{path}"
        if method == "POST":
            return _req.post(url, json=json_body or {}, timeout=timeout).json()
        return _req.get(url, timeout=timeout).json()

    def _run_us_batch_and_rebal(self):
        """US batch → snapshot_version → auto rebal if due."""
        if self._us_batch_running:
            return
        self._us_batch_running = True

        try:
            import uuid as _uuid

            # 1. same-day check — pre-market 배치는 재실행, post-close 배치만 skip
            try:
                status = self._us_api("/api/rebalance/status")
                today_bd = status.get("business_date", "")
                last_bd = status.get("last_batch_business_date", "")
                created_at = status.get("snapshot_created_at", "")
                if last_bd == today_bd and created_at:
                    from zoneinfo import ZoneInfo
                    try:
                        created = datetime.fromisoformat(created_at)
                        if created.tzinfo is None:
                            created = created.replace(tzinfo=ZoneInfo("US/Eastern"))
                        created_et = created.astimezone(ZoneInfo("US/Eastern"))
                        # business_date의 16:00 ET
                        y, m, d = [int(x) for x in today_bd.split("-")]
                        close_et = datetime(y, m, d, 16, 0, 0, tzinfo=ZoneInfo("US/Eastern"))
                        if created_et >= close_et:
                            # Sync cache so scheduler 30s tick doesn't retrigger.
                            # Without this, every tick probes /status (phase != BATCH_DONE
                            # after rebal advances) and spawns this method again,
                            # polluting the log with [US_BATCH_START] skip: ... repeats.
                            self._us_batch_last_done_date = datetime.now(
                                ZoneInfo("US/Eastern")
                            ).date()
                            self._logger.info(
                                f"[US_BATCH_START] skip: post-close batch already done "
                                f"for {today_bd} (created={created_et.isoformat()})"
                            )
                            return
                        else:
                            self._logger.info(
                                f"[US_BATCH_START] pre-market batch detected "
                                f"(created={created_et.isoformat()} < close={close_et.isoformat()}) "
                                f"→ re-running post-close"
                            )
                    except Exception as ep:
                        self._logger.warning(f"[US_BATCH_START] created_at parse failed: {ep}")
                        # parse 실패 시 보수적으로 skip 안 함 → 재실행 허용
            except Exception as e:
                self._logger.warning(f"[US_BATCH_START] status check failed: {e}")
                # Continue anyway — batch will handle its own checks

            # 2. Phase → BATCH_RUNNING
            try:
                self._us_api("/api/rebalance/phase", "POST", {"phase": "BATCH_RUNNING"})
            except Exception as e:
                self._logger.warning(f"[US_REBAL_PHASE] BATCH_RUNNING failed: {e}")

            self._logger.info("[US_BATCH_START]")
            self._show_balloon("Q-TRON US", "Auto batch starting...")

            # 3. Run batch (retry 2회)
            batch_ok = False
            for attempt in range(2):
                try:
                    result = subprocess.run(
                        [str(US_PYTHON), "-X", "utf8", "main.py", "--batch"],
                        cwd=str(US_BASE_DIR),
                        capture_output=True, text=True, timeout=600,
                    )
                    if result.returncode == 0:
                        batch_ok = True
                        self._logger.info(f"[US_BATCH_OK] attempt={attempt+1}")
                        break
                    else:
                        self._logger.warning(
                            f"[US_BATCH_FAIL] attempt={attempt+1}: {result.stderr[:200]}"
                        )
                except Exception as e:
                    self._logger.error(f"[US_BATCH_FAIL] attempt={attempt+1}: {e}")

            if not batch_ok:
                # Phase → FAILED
                try:
                    self._us_api("/api/rebalance/phase", "POST", {"phase": "FAILED"})
                except Exception:
                    pass
                self._show_balloon("Q-TRON US", "Auto batch FAILED")
                return

            # 4. snapshot_version은 batch(main.py)가 이미 저장 — tray는 읽기만
            from zoneinfo import ZoneInfo
            self._us_batch_last_done_date = datetime.now(ZoneInfo("US/Eastern")).date()
            self._show_balloon("Q-TRON US", "Batch complete! Running Lab EOD...")

            # 4.5. Lab Forward EOD — 배치 성공 시만 실행 (unified pipeline)
            # 배치 실패 시 Lab EOD를 건너뜀으로써 상태 불일치 방지.
            # standalone Lab EOD 스케줄러는 배치 완료일에만 fallback 허용.
            try:
                self._logger.info("[US_LAB_EOD_IN_BATCH] Starting Lab Forward EOD...")
                _eod_r = self._us_api(
                    "/api/lab/forward/eod", "POST", {}, timeout=180
                )
                if _eod_r.get("error"):
                    self._logger.warning(
                        f"[US_LAB_EOD_IN_BATCH] non-fatal error: {str(_eod_r.get('error'))[:120]}"
                    )
                else:
                    self._us_lab_eod_last_done_date = datetime.now(
                        ZoneInfo("US/Eastern")
                    ).date()
                    self._logger.info(
                        f"[US_LAB_EOD_IN_BATCH] done: {str(_eod_r)[:120]}"
                    )
            except Exception as _lab_e:
                self._logger.warning(f"[US_LAB_EOD_IN_BATCH] non-critical: {_lab_e}")

            self._show_balloon("Q-TRON US", "Lab EOD done! Checking rebal...")

            # 5. Auto rebal 판단
            try:
                status = self._us_api("/api/rebalance/status")
                mode = status.get("mode", "manual")
                rebal_due = status.get("rebal_due", False)
                exec_allowed = status.get("execute_allowed", False)

                self._logger.info(
                    f"[US_REBAL_STATUS] mode={mode} due={rebal_due} "
                    f"allowed={exec_allowed} blocks={status.get('block_reasons', [])}"
                )

                if mode == "auto" and rebal_due and exec_allowed:
                    req_id = str(_uuid.uuid4())
                    self._logger.info(f"[US_REBAL_EXEC_REQ] req={req_id[:8]}")

                    # Execute with retry (같은 request_id → idempotent)
                    exec_result = None
                    for retry in range(2):
                        try:
                            exec_result = self._us_api(
                                "/api/rebalance/execute", "POST",
                                {"mode": "sell_and_buy", "request_id": req_id},
                                timeout=120,
                            )
                            if exec_result.get("ok"):
                                self._logger.info(
                                    f"[US_REBAL_EXEC_DONE] result={exec_result.get('result')}"
                                )
                                self._show_balloon(
                                    "Q-TRON US",
                                    f"Rebal {exec_result.get('result', '?')}: "
                                    f"S={exec_result.get('sell_count',0)} B={exec_result.get('buy_count',0)}"
                                )
                                break
                            else:
                                err = exec_result.get("error", "?")
                                self._logger.warning(
                                    f"[US_REBAL_EXEC_REJECT] retry={retry} error={err}"
                                )
                        except Exception as e:
                            self._logger.error(f"[US_REBAL_EXEC_FAIL] retry={retry}: {e}")

                    if not exec_result or not exec_result.get("ok"):
                        self._show_balloon("Q-TRON US", "Auto rebal: not executed (check logs)")
                else:
                    reason = "manual" if mode != "auto" else (
                        "not due" if not rebal_due else "blocked"
                    )
                    self._logger.info(f"[US_REBAL_EXEC_REQ] skip: {reason}")
                    self._show_balloon("Q-TRON US", f"Batch OK. Rebal: {reason}")

            except Exception as e:
                self._logger.error(f"[US_REBAL_STATUS] check failed: {e}")
                self._show_balloon("Q-TRON US", "Batch OK. Rebal check failed.")

        finally:
            self._us_batch_running = False

    def _run_us_auto_rebal_only(self):
        """BATCH_DONE catchup: snapshot 기반 idempotency로 1회 실행."""
        if self._us_batch_running:
            return
        self._us_batch_running = True  # guard reentry
        try:
            import uuid as _uuid

            status = self._us_api("/api/rebalance/status")
            phase = status.get("phase", "IDLE")
            mode = status.get("mode", "manual")
            rebal_due = status.get("rebal_due", False)
            exec_allowed = status.get("execute_allowed", False)
            current_sv = status.get("snapshot_version", "")
            last_sv = status.get("last_rebal_attempt_snapshot", "")
            last_result = status.get("last_rebal_attempt_result", "")
            last_count = status.get("last_rebal_attempt_count", 0)

            self._logger.debug(
                f"[US_REBAL_CATCHUP_CHECK] phase={phase} mode={mode} "
                f"due={rebal_due} allowed={exec_allowed} "
                f"sv={current_sv[:20] if current_sv else ''} "
                f"last_sv={last_sv[:20] if last_sv else ''} "
                f"last_result={last_result} count={last_count}"
            )

            # Gate 1: basic eligibility
            if phase != "BATCH_DONE" or mode != "auto":
                return
            if not rebal_due or not exec_allowed:
                # REJECTED throttle: 동일 (sv, reason) 5분 dedup
                reason = "not_due" if not rebal_due else "exec_blocked"
                throttle_key = (current_sv, reason)
                now_ts = time.time()
                last_log = self._us_catchup_reject_log.get(throttle_key, 0)
                if now_ts - last_log > 300:  # 5분
                    self._logger.info(
                        f"[US_REBAL_CATCHUP_SKIP_EXEC_BLOCKED] "
                        f"due={rebal_due} allowed={exec_allowed} "
                        f"blocks={status.get('block_reasons', [])}"
                    )
                    self._us_catchup_reject_log[throttle_key] = now_ts
                return

            # Gate 2: snapshot 기반 idempotency
            if current_sv and current_sv == last_sv:
                if last_result in ("SUCCESS", "PARTIAL"):
                    self._logger.info(
                        f"[US_REBAL_CATCHUP_SKIP_ALREADY_ATTEMPTED] "
                        f"sv={current_sv[:20]} result={last_result}"
                    )
                    return
                elif last_result == "FAILED" and last_count >= 2:
                    self._logger.info(
                        f"[US_REBAL_CATCHUP_SKIP_RETRY_EXHAUSTED] "
                        f"sv={current_sv[:20]} count={last_count}"
                    )
                    return
                elif last_result == "FAILED":
                    self._logger.info(
                        f"[US_REBAL_CATCHUP_RETRY_FAILED] "
                        f"sv={current_sv[:20]} count={last_count}"
                    )
                # REJECTED: 재시도 허용 (count 미소진)

            # Gate 통과 → 실행
            req_id = str(_uuid.uuid4())
            self._logger.info(
                f"[US_REBAL_CATCHUP_TRIGGER] req={req_id[:8]} "
                f"sv={current_sv[:20]}"
            )
            self._show_balloon("Q-TRON US", "Catchup rebal starting...")

            try:
                exec_result = self._us_api(
                    "/api/rebalance/execute", "POST",
                    {"mode": "sell_and_buy", "request_id": req_id},
                    timeout=120,
                )
                if exec_result.get("ok"):
                    self._logger.info(
                        f"[US_REBAL_CATCHUP_RESULT] "
                        f"result={exec_result.get('result')} "
                        f"S={exec_result.get('sell_count', 0)} "
                        f"B={exec_result.get('buy_count', 0)}"
                    )
                    self._show_balloon(
                        "Q-TRON US",
                        f"Catchup rebal {exec_result.get('result', '?')}: "
                        f"S={exec_result.get('sell_count', 0)} "
                        f"B={exec_result.get('buy_count', 0)}"
                    )
                else:
                    err = exec_result.get("error", "?")
                    self._logger.warning(
                        f"[US_REBAL_CATCHUP_RESULT] rejected: {err}"
                    )
                    self._show_balloon("Q-TRON US", f"Catchup rebal: {err}")
            except Exception as e:
                self._logger.error(f"[US_REBAL_CATCHUP_RESULT] error: {e}")
                self._show_balloon("Q-TRON US", "Catchup rebal: connection error")

        except Exception as e:
            self._logger.error(f"[US_REBAL_CATCHUP_CHECK] error: {e}")
        finally:
            self._us_batch_running = False

    def _action_us_live_start(self):
        """Start US Live mode as background subprocess."""
        if self._us_live_running:
            self._show_balloon("Q-TRON US", "US Live already running.")
            return
        if not US_PYTHON.exists():
            self._show_balloon("Q-TRON US", f"Python not found:\n{US_PYTHON}")
            return
        try:
            self._us_live_process = subprocess.Popen(
                [str(US_PYTHON), "-X", "utf8", "main.py", "--live"],
                cwd=str(US_BASE_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            self._us_live_running = True
            self._logger.info(f"[US_LIVE] Started (PID {self._us_live_process.pid})")
            self._show_balloon("Q-TRON US", "US Live mode started (monitor + trail stop)")
        except Exception as e:
            self._logger.error(f"[US_LIVE] Start failed: {e}")
            self._show_balloon("Q-TRON US", f"Live start failed: {e}")

    def _action_us_live_stop(self):
        """Stop US Live mode gracefully (SIGINT → terminate)."""
        if not self._us_live_running or not self._us_live_process:
            self._show_balloon("Q-TRON US", "US Live not running.")
            return
        try:
            # Try graceful first (CTRL_BREAK on Windows)
            import signal
            try:
                os.kill(self._us_live_process.pid, signal.CTRL_BREAK_EVENT)
                self._us_live_process.wait(timeout=10)
            except Exception:
                self._us_live_process.terminate()
                self._us_live_process.wait(timeout=5)
        except Exception:
            try:
                self._us_live_process.kill()
            except Exception:
                pass
        self._us_live_running = False
        self._us_live_process = None
        self._logger.info("[US_LIVE] Stopped")
        self._show_balloon("Q-TRON US", "US Live stopped.")

    # ── KR Live (Gen4 trading engine) auto-attach ────────────────
    # Pattern: tray spawns kr/main.py --live as subprocess, monitors its
    # lifecycle, classifies exit reason, retries only on real crashes.
    # The engine has its own 16:00 KST startup gate — tray respects it.

    def _is_kr_live_alive(self) -> bool:
        """Check whether the KR live engine subprocess is still running."""
        if self._kr_live_process is None:
            return False
        return self._kr_live_process.poll() is None

    def _is_kr_trading_window(self) -> bool:
        """True iff now is inside the daily window where tray may spawn KR live.

        Window: weekday 08:30 ≤ now < 16:00 KST. Holidays are not separately
        checked here — the engine's own is_trading_day() exits cleanly with
        "Non-trading day" if today is closed, and the tray's exit classifier
        marks the day as ended (no further attempts).
        """
        now = datetime.now()
        if now.weekday() >= 5:  # Sat/Sun
            return False
        if now.hour < KR_LIVE_START_HOUR:
            return False
        if now.hour == KR_LIVE_START_HOUR and now.minute < KR_LIVE_START_MINUTE:
            return False
        if now.hour >= KR_LIVE_END_HOUR:
            return False
        return True

    def _classify_kr_live_exit(self) -> str:
        """Tail the KR live log to figure out *why* the subprocess exited.

        Drives retry policy: clean exits (after-hours / non-trading / EOD)
        block further attempts today; crashes are retryable up to N times
        with cooldown. Without this classifier, tray would restart-loop
        through the engine's own 16:00 gate forever.
        """
        if not self._kr_live_log_path or not self._kr_live_log_path.exists():
            return "UNKNOWN"
        try:
            with open(self._kr_live_log_path, "r", encoding="utf-8", errors="replace") as f:
                tail = f.readlines()[-80:]
            joined = "".join(tail)
        except Exception:
            return "UNKNOWN"
        if "After 16:00" in joined or "After market hours" in joined:
            return "AFTER_HOURS"
        if "Non-trading day" in joined:
            return "NON_TRADING_DAY"
        # startup_phase.py:397 uses "Broker sync FAILED" (uppercase) —
        # do case-insensitive match so a future caps tweak doesn't break us.
        if "broker sync failed" in joined.lower():
            return "BROKER_FAIL"
        if "[LIVE_END] system exit" in joined:
            return "EOD_COMPLETE"
        if "Shadow test complete" in joined:
            return "SHADOW_COMPLETE"  # shouldn't happen here, defensive
        return "UNKNOWN"

    def _action_kr_live_start(self):
        """Spawn kr/main.py --live as background subprocess.

        Stdout+stderr captured to a date-stamped log so the exit classifier
        can read the SystemExit reason after the process ends. Without
        this redirect (subprocess.DEVNULL), the tray cannot tell an
        after-hours exit apart from a real crash and would retry-loop.
        """
        if self._kr_live_running and self._is_kr_live_alive():
            self._show_balloon("Q-TRON KR", "KR Live already running.")
            return
        if not KR_LIVE_PYTHON.exists():
            self._show_balloon("Q-TRON KR", f"Python not found:\n{KR_LIVE_PYTHON}")
            self._logger.error(f"[KR_LIVE] Python missing: {KR_LIVE_PYTHON}")
            return

        # Daily attempt counter reset
        today = date.today()
        if self._kr_live_attempt_count_date != today:
            self._kr_live_attempt_count_date = today
            self._kr_live_attempt_count_today = 0
            self._kr_live_ended_today = False

        self._kr_live_attempt_count_today += 1
        self._kr_live_last_attempt_at = datetime.now()

        # Per-day log file; subprocess writes via fh, classifier reads via path
        try:
            log_dir = LOG_DIR
            log_dir.mkdir(parents=True, exist_ok=True)
            self._kr_live_log_path = log_dir / f"kr_live_tray_{today.strftime('%Y%m%d')}.log"
            self._kr_live_log_fh = open(
                self._kr_live_log_path, "a", encoding="utf-8", buffering=1
            )
            self._kr_live_log_fh.write(
                f"\n===== {datetime.now().isoformat(timespec='seconds')} "
                f"KR live spawn (attempt {self._kr_live_attempt_count_today}"
                f"/{KR_LIVE_MAX_ATTEMPTS_PER_DAY}) =====\n"
            )
        except Exception as _log_err:
            self._logger.error(f"[KR_LIVE] log open failed: {_log_err}")
            self._kr_live_log_fh = None

        # Clean env: avoid leaking VIRTUAL_ENV from KR REST process into the
        # engine subprocess (engine uses its own .venv64 interpreter).
        clean_env = dict(os.environ)
        for var in ("PYTHONPATH", "VIRTUAL_ENV", "PYTHONHOME"):
            clean_env.pop(var, None)

        try:
            # CREATE_NEW_PROCESS_GROUP — needed so CTRL_BREAK_EVENT in
            # _action_kr_live_stop reaches the engine without affecting tray
            # itself.
            # CREATE_NO_WINDOW — hide the cmd window that would otherwise
            # flash on every spawn; Jeff is on a business trip during the
            # first auto-spawns and a popping console would be confusing.
            # Both flags are required together.
            self._kr_live_process = subprocess.Popen(
                [str(KR_LIVE_PYTHON), "-X", "utf8", "main.py", "--live"],
                cwd=str(KR_LIVE_BASE_DIR),
                stdout=(self._kr_live_log_fh or subprocess.DEVNULL),
                stderr=subprocess.STDOUT,
                env=clean_env,
                creationflags=(
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    | subprocess.CREATE_NO_WINDOW
                ),
            )
            self._kr_live_running = True
            self._logger.info(
                f"[KR_LIVE] Spawned (PID {self._kr_live_process.pid}, "
                f"attempt {self._kr_live_attempt_count_today})"
            )
            self._show_balloon("Q-TRON KR", "KR Live started")
        except Exception as e:
            self._logger.error(f"[KR_LIVE] Spawn failed: {e}")
            self._show_balloon("Q-TRON KR", f"KR Live spawn failed: {e}")
            self._kr_live_running = False
            self._kr_live_process = None
            if self._kr_live_log_fh:
                try:
                    self._kr_live_log_fh.close()
                except Exception:
                    pass
                self._kr_live_log_fh = None

    def _action_kr_live_stop(self):
        """Stop KR live subprocess gracefully (CTRL_BREAK → terminate)."""
        if not self._is_kr_live_alive():
            self._show_balloon("Q-TRON KR", "KR Live not running.")
            self._kr_live_running = False
            self._kr_live_process = None
            return
        try:
            import signal as _sig
            try:
                os.kill(self._kr_live_process.pid, _sig.CTRL_BREAK_EVENT)
                self._kr_live_process.wait(timeout=10)
            except Exception:
                self._kr_live_process.terminate()
                self._kr_live_process.wait(timeout=5)
        except Exception:
            try:
                self._kr_live_process.kill()
            except Exception:
                pass
        self._kr_live_running = False
        self._kr_live_process = None
        if self._kr_live_log_fh:
            try:
                self._kr_live_log_fh.close()
            except Exception:
                pass
            self._kr_live_log_fh = None
        self._logger.info("[KR_LIVE] Stopped")
        self._show_balloon("Q-TRON KR", "KR Live stopped")

    def _action_kr_live_restart(self):
        """Stop + start cycle. Resets the daily attempt counter so an
        operator-initiated restart isn't penalized by prior crash budget."""
        self._action_kr_live_stop()
        time.sleep(1.0)
        # Reset daily budget so manual restart isn't blocked by earlier crashes
        self._kr_live_attempt_count_today = 0
        self._kr_live_ended_today = False
        self._action_kr_live_start()

    def _action_kr_live_auto_toggle(self):
        """Toggle whether the background scheduler will spawn KR live.

        When OFF, the menu still lets operator start manually; tray just
        won't intervene. Useful when Jeff wants to inspect engine state
        without the scheduler restarting it.
        """
        self._kr_live_auto = not self._kr_live_auto
        state_str = "ON" if self._kr_live_auto else "OFF"
        self._logger.info(f"[KR_LIVE_AUTO] toggled → {state_str}")
        self._show_balloon("Q-TRON KR", f"KR Live auto-attach: {state_str}")

    def _action_kr_live_log(self):
        """Open today's KR live tray log in the system's default editor."""
        if self._kr_live_log_path and self._kr_live_log_path.exists():
            try:
                os.startfile(str(self._kr_live_log_path))
            except Exception as _e:
                self._show_balloon("Q-TRON KR", f"Log open failed: {_e}")
        else:
            self._show_balloon("Q-TRON KR", "No KR live log yet.")

    def _check_kr_live_lifecycle(self):
        """Tick-loop helper called every 30s by _background_scheduler.

        Two responsibilities:
          (a) Detect that a previously spawned subprocess has exited and
              classify the reason — so the daily-attempt counter and the
              ``ended_today`` flag stay correct. Without this the tray
              would re-spawn through the engine's own gates.
          (b) When KR live is not running and the trading window is open,
              spawn it (subject to attempt budget + crash cooldown).
        """
        # (a) Detect exit
        if (self._kr_live_process is not None
                and self._kr_live_process.poll() is not None
                and self._kr_live_running):
            rc = self._kr_live_process.poll()
            reason = self._classify_kr_live_exit()
            self._kr_live_last_exit_reason = reason
            self._kr_live_running = False
            if self._kr_live_log_fh:
                try:
                    self._kr_live_log_fh.close()
                except Exception:
                    pass
                self._kr_live_log_fh = None
            self._logger.info(
                f"[KR_LIVE] subprocess exited rc={rc} reason={reason}"
            )
            # Clean exits → no further attempts today
            if reason in ("AFTER_HOURS", "NON_TRADING_DAY", "EOD_COMPLETE"):
                self._kr_live_ended_today = True
                self._logger.info(
                    f"[KR_LIVE] {reason} — no further attempts until next session"
                )

        # (b) Decide whether to spawn now
        if not self._kr_live_auto:
            return
        if self._is_kr_live_alive():
            return
        if not self._is_kr_trading_window():
            return
        if self._kr_live_ended_today:
            return
        if self._kr_live_attempt_count_today >= KR_LIVE_MAX_ATTEMPTS_PER_DAY:
            return
        # Crash cooldown: wait at least RETRY_COOLDOWN seconds since last attempt
        if self._kr_live_last_attempt_at is not None:
            since = (datetime.now() - self._kr_live_last_attempt_at).total_seconds()
            if since < KR_LIVE_RETRY_COOLDOWN_SEC:
                return
        self._logger.info(
            f"[KR_LIVE_AUTO] window open + not running → spawn "
            f"(attempt {self._kr_live_attempt_count_today + 1}"
            f"/{KR_LIVE_MAX_ATTEMPTS_PER_DAY})"
        )
        self._action_kr_live_start()

    def _action_unified(self):
        """Open Unified Dashboard in browser."""
        webbrowser.open(f"http://localhost:{PORT}/unified")

    def _action_gate_observer(self):
        """AUTO GATE latest report — READ-ONLY consumer of logs/gate_observer/*.json."""
        try:
            from datetime import datetime as _dt
            # sys.path already prepared by _bootstrap_path at top of file
            from tools.gate_observer import load_latest, render_text, status_header
            data = load_latest()
            if not data:
                self._show_balloon(
                    "Gate Observer",
                    "NO REPORT TODAY (producer deferred or not run)")
                return
            lines = [
                "AUTO GATE Report (Latest) — READ-ONLY",
                "=" * 50,
                status_header(data),
                f"producer: {data.get('producer','?')}   "
                f"window: {data['window']['since']} -> {data['window']['until']}",
                "",
            ]
            for m in ("KR", "US"):
                lines.append(render_text(data[m]))
                lines.append("")
            report = "\n".join(lines)
            out = _root / "kr" / "logs" / f"gate_observer_view_{_dt.now():%Y%m%d_%H%M%S}.txt"
            out.write_text(report, encoding="utf-8")
            os.startfile(str(out))
        except Exception as e:
            self._show_balloon("Gate Observer failed", str(e))

    # ── Server Control ────────────────────────────────────────

    def _start_server(self):
        import uvicorn
        try:
            self._update_icon("connecting")
        except Exception:
            pass
        config = uvicorn.Config(
            "web.app:app",
            host=HOST, port=PORT,
            log_level="warning",
            access_log=False,
        )
        self._uvicorn_server = uvicorn.Server(config)
        self._server_thread = threading.Thread(
            target=self._uvicorn_server.run, daemon=True,
        )
        self._server_thread.start()
        self._start_time = datetime.now()
        self._running = True
        self._blink_active = False  # init
        self._logger.info(f"[TRAY] Server started on :{PORT}")
        # Start Telegram bot polling
        try:
            from notify.telegram_bot import start_polling
            start_polling(interval=3.0)
            self._logger.info("[TRAY] Telegram bot polling started")
        except Exception as e:
            self._logger.warning(f"[TRAY] Telegram polling failed: {e}")
        try:
            self._update_icon("ok")
        except Exception:
            pass

    def _stop_server(self):
        if self._uvicorn_server:
            self._uvicorn_server.should_exit = True
            if self._server_thread:
                self._server_thread.join(timeout=5)
            self._uvicorn_server = None
            self._logger.info("[TRAY] Server stopped")

    # ── Helpers ───────────────────────────────────────────────

    def _uptime_str(self) -> str:
        d = datetime.now() - self._start_time
        h = int(d.total_seconds() // 3600)
        m = int((d.total_seconds() % 3600) // 60)
        return f"{h}h{m}m"

    def _today_log(self) -> Path:
        return LOG_DIR / f"rest_api_{date.today().strftime('%Y%m%d')}.log"

    def _get_log_size(self) -> str:
        f = self._today_log()
        return f"{f.stat().st_size / 1024:.0f}KB" if f.exists() else "N/A"

    def _get_xval_summary(self) -> str:
        for d in (date.today(), date.today() - timedelta(days=1)):
            p = XVAL_DIR / f"xval_summary_{d.strftime('%Y%m%d')}.json"
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    return (
                        f"XVAL: {data.get('eligible_samples', 0)} samples, "
                        f"zero={data.get('eligible_diff_zero_rate', 0):.1%}, "
                        f"critical={data.get('critical_diff_count', 0)}"
                    )
                except Exception:
                    pass
        return "XVAL: no data"

    def _kill_port_user(self, port: int = None):
        """지정 포트 리스너 강제 종료. port 미지정 시 KR PORT."""
        target = port if port is not None else PORT
        try:
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True, timeout=5,
                creationflags=_NO_WINDOW,
            )
            for line in result.stdout.splitlines():
                if f":{target} " in line and "LISTENING" in line:
                    pid = int(line.split()[-1])
                    if pid != os.getpid():
                        subprocess.run(
                            ["taskkill", "/PID", str(pid), "/F"],
                            capture_output=True, timeout=5,
                            creationflags=_NO_WINDOW,
                        )
                        self._logger.info(f"[TRAY] Killed PID {pid} on port {target}")
                        time.sleep(1)
                    break
        except Exception:
            pass

    # ── Main Entry ────────────────────────────────────────────

    def run(self):
        self._kill_port_user()

        from data.rest_logger import setup_rest_logging
        setup_rest_logging()

        # Startup health check — critical dep import 검증
        # REQUIRED 누락 시 RuntimeError로 여기서 중단 (tray가 실행 자체 안 됨)
        # CRITICAL 누락 시 부팅은 허용, Telegram + DataEvent 알림
        try:
            from tools.health_check import run_startup_health_check
            run_startup_health_check(scope="kr")
        except RuntimeError as _hc_fatal:
            self._logger.critical(f"[HEALTH_CHECK] FATAL: {_hc_fatal}")
            raise
        except Exception as _hc_err:
            self._logger.warning(f"[HEALTH_CHECK] check itself failed: {_hc_err}")

        self._start_server()

        # Auto-start US server
        if US_PYTHON.exists():
            try:
                self._action_us_start()
                self._logger.info("[TRAY] US server auto-started")
            except Exception as e:
                self._logger.warning(f"[TRAY] US server auto-start failed: {e}")

        # Create hidden window for message processing
        self._hwnd = self._create_window()
        self._add_tray_icon()

        self._logger.info("[TRAY] System tray icon created")

        # Background scheduler: tooltip update + auto-batch
        def _background_scheduler():
            # Pipeline Orchestrator hook (Phase 3+4, 2026-04-21).
            # Env `QTRON_PIPELINE`:
            #   unset/0  → disabled (no-op)
            #   1        → shadow: orchestrator writes state alongside
            #              legacy auto-triggers below
            #   2+       → primary: orchestrator owns scheduling; legacy
            #              batch/backup/KR-EOD/US-EOD blocks SKIP to
            #              prevent double-firing.
            # See kr/docs/PIPELINE_ORCHESTRATOR_PLAN.md §3.
            try:
                from pipeline.tray_integration import (
                    is_primary as _pipeline_is_primary,
                    notify_if_enabled as _pipeline_notify,
                    tick_if_enabled as _pipeline_tick,
                )
            except Exception as _pipe_imp_err:
                _pipeline_tick = None
                _pipeline_notify = None
                _pipeline_is_primary = lambda: False  # noqa: E731
                self._logger.warning(
                    f"[PIPELINE_TRAY_IMPORT_FAIL] {_pipe_imp_err}"
                )

            while self._running:
                time.sleep(30)

                # Pipeline Orchestrator tick. Never raises. Writes state
                # regardless of mode; only scheduling ownership differs.
                if _pipeline_tick is not None:
                    _pipeline_tick()

                # Telegram transition notifier — emits DONE / ABANDONED
                # messages after each tick. Safe on failure (returns []).
                if _pipeline_notify is not None:
                    _pipeline_notify()

                # Snapshot primary-mode flag once per tick so legacy
                # trigger blocks below can uniformly skip under PRIMARY.
                _orch_primary = False
                try:
                    _orch_primary = _pipeline_is_primary()
                except Exception:
                    _orch_primary = False

                # Tooltip
                batch_flag = " | Batch ON" if self._batch_auto else ""
                self._update_tooltip(
                    f"{APP_NAME} :{PORT} | Up {self._uptime_str()}{batch_flag}"
                )

                # Auto-batch: check every 30s if it's time.
                # Orchestrator PRIMARY mode owns batch scheduling — skip
                # legacy trigger to avoid double-firing.
                if _orch_primary:
                    pass
                elif self._batch_auto and not self._batch_running and not self._batch_today_done:
                    now = datetime.now()
                    if now.weekday() < 5:  # Mon-Fri only
                        if now.hour == BATCH_HOUR and now.minute >= BATCH_MINUTE:
                            # Fail backoff: 직전 실패 후 BATCH_FAIL_BACKOFF_MIN 경과 전엔 skip.
                            # 30초 tick × 55min 윈도우 = 최대 110회 재시도를 ~11회로 축소.
                            _in_backoff = (
                                self._batch_last_fail_at is not None
                                and (now - self._batch_last_fail_at).total_seconds()
                                    < BATCH_FAIL_BACKOFF_MIN * 60
                            )
                            if not _in_backoff:
                                self._logger.info("[BATCH_AUTO] Scheduled batch triggered")
                                threading.Thread(target=self._run_batch, daemon=True).start()

                # Auto-backup: 17:00 daily.
                # Orchestrator PRIMARY mode owns backup scheduling.
                if _orch_primary:
                    pass
                elif not self._backup_running and not self._backup_today_done:
                    now = datetime.now()
                    if now.hour == 17 and now.minute >= 0:
                        self._logger.info("[BACKUP_AUTO] Daily backup triggered")
                        threading.Thread(target=self._run_backup, daemon=True).start()

                # Daily self-test: 08:00 KST (PG ping)
                if not self._self_test_running and not self._self_test_today_done:
                    now = datetime.now()
                    if now.hour == SELF_TEST_HOUR and now.minute >= SELF_TEST_MINUTE:
                        self._logger.info("[TRAY_SELF_TEST] Scheduled ping triggered")
                        threading.Thread(target=self._run_self_test, daemon=True).start()

                # Reset daily flags at midnight
                if datetime.now().hour == 0:
                    if self._batch_today_done:
                        self._batch_today_done = False
                    if self._backup_today_done:
                        self._backup_today_done = False
                    if self._self_test_today_done:
                        self._self_test_today_done = False
                    # REJECTED 로그 throttle 캐시 리셋
                    self._us_catchup_reject_log.clear()
                    # KR live daily counters reset (also handled in _action
                    # _kr_live_start when date rolls, but mirror here so the
                    # 00:00 tick is the explicit reset event in logs).
                    if self._kr_live_attempt_count_date != date.today():
                        self._kr_live_attempt_count_date = date.today()
                        self._kr_live_attempt_count_today = 0
                        self._kr_live_ended_today = False
                        self._logger.info("[KR_LIVE_AUTO] daily counters reset")

                # KR Live auto-attach: spawn engine when market window opens,
                # detect clean exits (after-hours / EOD) so we don't restart-loop
                # through the engine's own startup gate.
                try:
                    self._check_kr_live_lifecycle()
                except Exception as _kr_live_err:
                    self._logger.error(f"[KR_LIVE_AUTO] tick error: {_kr_live_err}")

                # US Auto-batch: ET 18:00~18:30 (close + 2h)
                if (self._us_batch_auto and not self._us_batch_running
                        and self._is_us_process_alive() and self._is_us_batch_time()):
                    from zoneinfo import ZoneInfo as _ZI
                    _et_today = datetime.now(_ZI("US/Eastern")).date()
                    if self._us_batch_last_done_date == _et_today:
                        pass  # 오늘 이미 완료
                    else:
                        # server phase=BATCH_DONE 이면 실제 완료 상태 — date sync 후 skip (무한 retry 방지)
                        _phase_done = False
                        try:
                            _st = self._us_api("/api/rebalance/status", timeout=3)
                            if _st.get("phase") == "BATCH_DONE":
                                self._us_batch_last_done_date = _et_today
                                self._logger.info(
                                    f"[US_BATCH_AUTO_SYNC] phase=BATCH_DONE detected — "
                                    f"last_done_date={_et_today}"
                                )
                                _phase_done = True
                        except Exception as _e:
                            self._logger.debug(f"[US_BATCH_AUTO] status probe: {_e}")
                        if not _phase_done:
                            self._logger.info("[US_BATCH_AUTO] Scheduled batch triggered")
                            threading.Thread(target=self._run_us_batch_and_rebal, daemon=True).start()

                # US Auto-rebal: BATCH_DONE catchup (snapshot 기반 idempotency)
                if (self._us_batch_auto and not self._us_batch_running
                        and self._is_us_process_alive()):
                    try:
                        status = self._us_api("/api/rebalance/status")
                        phase = status.get("phase", "IDLE")

                        if phase == "BATCH_DONE" and status.get("mode") == "auto":
                            current_sv = status.get("snapshot_version", "")
                            last_sv = status.get("last_rebal_attempt_snapshot", "")
                            last_result = status.get("last_rebal_attempt_result", "")
                            last_count = status.get("last_rebal_attempt_count", 0)

                            should_attempt = (
                                current_sv != last_sv  # 새 배치
                                or (last_result == "FAILED" and last_count < 2)
                                or last_result == "REJECTED"
                            )

                            if should_attempt:
                                threading.Thread(
                                    target=self._run_us_auto_rebal_only,
                                    daemon=True,
                                ).start()
                    except Exception:
                        pass  # US server not ready yet

                # ── Lab Live EOD Auto (KR, v4.1) ──
                # Orchestrator PRIMARY mode owns lab_eod_kr scheduling —
                # kr_can_try is forced False below to suppress legacy
                # trigger. Fail-count / retry state tracking still runs
                # so a cutover rollback leaves no stale counters.
                try:
                    kst_now = self._now_kst()
                    kst_today = kst_now.date()
                    self._maybe_reset_kr_lab_eod_fail(kst_today)
                    kr_done = (self._kr_lab_eod_last_done_date == kst_today)
                    kr_is_initial = self._is_within_window(
                        kst_now, KR_LAB_EOD_HOUR, KR_LAB_EOD_MINUTE,
                        LAB_EOD_WINDOW_SEC,
                    )
                    kr_is_retry = (
                        self._kr_lab_eod_retry_until is not None
                        and kst_now >= self._kr_lab_eod_retry_until
                    )
                    kr_can_try = (
                        not _orch_primary
                        and not kr_done
                        and self._kr_lab_eod_fail_count < LAB_EOD_MAX_FAILS
                        and kst_now.weekday() < 5
                        and (kr_is_initial or kr_is_retry)
                    )
                    if kr_can_try:
                        self._logger.info(
                            f"[KR_LAB_EOD_AUTO_TRIGGER] "
                            f"{'initial' if kr_is_initial else 'retry'} "
                            f"fail={self._kr_lab_eod_fail_count}"
                        )
                        if kr_is_retry:
                            # retry 소비: 다음 fail 전까지 재트리거 없음
                            self._kr_lab_eod_retry_until = None
                        threading.Thread(
                            target=self._run_kr_lab_eod, daemon=True,
                        ).start()
                except Exception as _kre:
                    self._logger.warning(f"[KR_LAB_EOD_AUTO_SCHEDULE_ERR] {_kre}")

                # ── Lab Forward EOD Auto (US, v4.1) ──
                if self._is_us_process_alive():
                    try:
                        et_now = self._now_et()
                        et_today = et_now.date()
                        self._maybe_reset_us_lab_eod_fail(et_today)
                        us_done = (self._us_lab_eod_last_done_date == et_today)
                        us_is_initial = self._is_within_window(
                            et_now, US_LAB_EOD_HOUR, US_LAB_EOD_MINUTE,
                            LAB_EOD_WINDOW_SEC,
                        )
                        us_is_retry = (
                            self._us_lab_eod_retry_until is not None
                            and et_now >= self._us_lab_eod_retry_until
                        )
                        # batch_done_today: 오늘 배치가 완료된 경우만 standalone 허용.
                        # 배치 미완료 시 Lab EOD는 _run_us_batch_and_rebal 내부에서만 실행됨.
                        _us_batch_done_today = (
                            self._us_batch_last_done_date == et_today
                        )
                        us_can_try = (
                            not _orch_primary
                            and not us_done
                            and self._us_lab_eod_fail_count < LAB_EOD_MAX_FAILS
                            and et_now.weekday() < 5
                            and (us_is_initial or us_is_retry)
                            and _us_batch_done_today  # 배치 성공 후에만 standalone fallback
                        )
                        if us_can_try:
                            self._logger.info(
                                f"[US_LAB_EOD_AUTO_TRIGGER] "
                                f"{'initial' if us_is_initial else 'retry'} "
                                f"fail={self._us_lab_eod_fail_count}"
                            )
                            if us_is_retry:
                                self._us_lab_eod_retry_until = None
                            threading.Thread(
                                target=self._run_us_lab_eod, daemon=True,
                            ).start()
                    except Exception as _use:
                        self._logger.warning(f"[US_LAB_EOD_AUTO_SCHEDULE_ERR] {_use}")

                # US process health check (process-truth)
                if self._us_running and not self._is_us_process_alive():
                    self._us_running = False
                    self._logger.warning("[US] Process exited unexpectedly")
                    self._show_balloon("Q-TRON US", "US server stopped unexpectedly!")
                if self._us_live_running and not self._is_us_live_alive():
                    self._us_live_running = False
                    self._logger.warning("[US_LIVE] Process exited unexpectedly")

        threading.Thread(target=_background_scheduler, daemon=True).start()

        # Win32 message loop (blocks)
        win32gui.PumpMessages()

        self._logger.info("[TRAY] Exited")


if __name__ == "__main__":
    try:
        server = Win32TrayServer()
        server.run()
    except Exception as e:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        crash_log = LOG_DIR / "tray_crash.log"
        import traceback
        crash_log.write_text(
            f"{datetime.now()}: {e}\n{traceback.format_exc()}",
            encoding="utf-8",
        )
        sys.exit(1)
