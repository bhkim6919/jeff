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

import ctypes
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

# ── Path setup ──
sys.path.insert(0, str(Path(__file__).resolve().parent))

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

# ── US Server Config ─────────────────────────────────────────
US_PORT = 8081
US_BASE_DIR = Path(__file__).resolve().parent.parent / "us"
US_PYTHON = US_BASE_DIR / ".venv" / "Scripts" / "python.exe"

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
# US menu IDs
ID_US_DASHBOARD = 1020
ID_US_START = 1021
ID_US_STOP = 1022
ID_US_BATCH = 1023
ID_US_LIVE_START = 1024
ID_US_LIVE_STOP = 1025
ID_UNIFIED = 1026
ID_US_AUTO_TOGGLE = 1027
ID_US_RESTART = 1028
ID_RESTART_ALL = 1029
ID_US_LOG_DIR = 1030


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
        # Backup state
        self._backup_today_done = False
        self._backup_running = False
        # US server state
        self._us_process: Optional[subprocess.Popen] = None
        self._us_running = False
        # US auto batch+rebal
        self._us_batch_auto = True
        self._us_batch_running = False
        # US live state
        self._us_live_process: Optional[subprocess.Popen] = None
        self._us_live_running = False
        # Restart guards
        self._kr_restarting = False
        self._us_restarting = False
        self._logger = self._setup_logging()

    # ── Process-truth status ────────────────────────────────────
    def _is_us_process_alive(self) -> bool:
        """US 서버 프로세스가 실제로 살아있는지 확인."""
        if self._us_process is None:
            return False
        return self._us_process.poll() is None

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
                ch = logging.StreamHandler(sys.stdout)
                ch.setFormatter(fmt)
                logger.addHandler(ch)

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
            ID_RESTART: self._action_restart,
            ID_QUIT: self._action_quit,
            # US
            ID_US_DASHBOARD: self._action_us_dashboard,
            ID_US_BATCH: self._action_us_batch,
            ID_US_AUTO_TOGGLE: self._action_us_auto_toggle,
            ID_US_RESTART: self._action_us_restart,
            ID_US_LOG_DIR: self._action_us_log_dir,
            # Global
            ID_UNIFIED: self._action_unified,
            ID_RESTART_ALL: self._action_restart_all,
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
        kr_batch_flags = MF | (MF_GRAY if self._batch_running else 0)
        kr_batch_label = "Run Batch Now" + (" (running...)" if self._batch_running else "")
        win32gui.AppendMenu(kr_sub, kr_batch_flags, ID_BATCH_NOW, kr_batch_label)
        kr_auto_label = f"Auto Batch [{'ON' if self._batch_auto else 'OFF'}]"
        win32gui.AppendMenu(kr_sub, MF, ID_BATCH_TOGGLE, kr_auto_label)
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
        us_batch_flags = MF | (MF_GRAY if self._us_batch_running else 0)
        us_batch_label = "Run Batch Now" + (" (running...)" if self._us_batch_running else "")
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
            subprocess.run(["clip"], input=path_str.encode("utf-8"), timeout=3)
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

    def _run_backup(self):
        """Execute daily backup (background thread)."""
        self._backup_running = True
        self._logger.info("[BACKUP_START] Daily backup started")
        try:
            import sys as _sys
            _sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backup"))
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

    def _run_batch(self):
        """Execute batch (background thread)."""
        self._batch_running = True
        self._logger.info("[BATCH_START] Batch execution started")
        self._show_balloon("Q-TRON Batch", "Batch started...")
        self._start_blink(["ok", "batch"], interval=0.7)

        try:
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

        except Exception as e:
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
        self._running = False
        # Stop US live first
        if self._is_us_live_alive():
            self._action_us_live_stop()
        # Stop US server
        if self._is_us_process_alive():
            self._action_us_stop()
        self._stop_server()
        win32gui.DestroyWindow(self._hwnd)

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
        try:
            self._us_process = subprocess.Popen(
                [str(US_PYTHON), "-X", "utf8", "-m", "uvicorn",
                 "web.app:app", "--host", HOST, "--port", str(US_PORT)],
                cwd=str(US_BASE_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
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
        if not US_PYTHON.exists():
            self._show_balloon("Q-TRON US", f"Python not found:\n{US_PYTHON}")
            return
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
                else:
                    self._logger.error(f"[US_BATCH] Failed: {result.stderr[:200]}")
                    self._show_balloon("Q-TRON US", f"Batch failed:\n{result.stderr[:100]}")
            except Exception as e:
                self._logger.error(f"[US_BATCH] Error: {e}")
                self._show_balloon("Q-TRON US", f"Batch error: {e}")

        threading.Thread(target=_run, daemon=True).start()

    def _action_us_auto_toggle(self):
        """Toggle US auto batch/rebal."""
        self._us_batch_auto = not self._us_batch_auto
        state = "ON" if self._us_batch_auto else "OFF"
        self._logger.info(f"[TRAY] US Auto Batch/Rebal {state}")
        self._show_balloon("Q-TRON US", f"Auto Batch/Rebal: {state}")

    def _is_us_batch_time(self) -> bool:
        """US/Eastern 기준 장 마감(16:00) + 120min 이후 윈도우."""
        try:
            from zoneinfo import ZoneInfo
            et_now = datetime.now(ZoneInfo("US/Eastern"))
            if et_now.weekday() >= 5:  # weekend
                return False
            # 18:00~18:30 ET 윈도우 (1회 이벤트)
            return et_now.hour == 18 and et_now.minute < 30
        except Exception:
            return False

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

            # 1. same-day check (영속: API에서 last_batch_business_date 확인)
            try:
                status = self._us_api("/api/rebalance/status")
                today_bd = status.get("business_date", "")
                if status.get("last_batch_business_date", "") == today_bd:
                    self._logger.info(f"[US_BATCH_START] skip: already done for {today_bd}")
                    return
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
            self._show_balloon("Q-TRON US", "Batch complete! Checking rebal...")

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

    def _action_unified(self):
        """Open Unified Dashboard in browser."""
        webbrowser.open(f"http://localhost:{PORT}/unified")

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

    def _kill_port_user(self):
        try:
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if f":{PORT}" in line and "LISTENING" in line:
                    pid = int(line.split()[-1])
                    if pid != os.getpid():
                        subprocess.run(
                            ["taskkill", "/PID", str(pid), "/F"],
                            capture_output=True, timeout=5,
                        )
                        self._logger.info(f"[TRAY] Killed PID {pid} on port {PORT}")
                        time.sleep(1)
                    break
        except Exception:
            pass

    # ── Main Entry ────────────────────────────────────────────

    def run(self):
        self._kill_port_user()

        from data.rest_logger import setup_rest_logging
        setup_rest_logging()

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
            while self._running:
                time.sleep(30)

                # Tooltip
                batch_flag = " | Batch ON" if self._batch_auto else ""
                self._update_tooltip(
                    f"{APP_NAME} :{PORT} | Up {self._uptime_str()}{batch_flag}"
                )

                # Auto-batch: check every 30s if it's time
                if self._batch_auto and not self._batch_running and not self._batch_today_done:
                    now = datetime.now()
                    if now.weekday() < 5:  # Mon-Fri only
                        if now.hour == BATCH_HOUR and now.minute >= BATCH_MINUTE:
                            self._logger.info("[BATCH_AUTO] Scheduled batch triggered")
                            threading.Thread(target=self._run_batch, daemon=True).start()

                # Auto-backup: 17:00 daily
                if not self._backup_running and not self._backup_today_done:
                    now = datetime.now()
                    if now.hour == 17 and now.minute >= 0:
                        self._logger.info("[BACKUP_AUTO] Daily backup triggered")
                        threading.Thread(target=self._run_backup, daemon=True).start()

                # Reset daily flags at midnight
                if datetime.now().hour == 0:
                    if self._batch_today_done:
                        self._batch_today_done = False
                    if self._backup_today_done:
                        self._backup_today_done = False

                # US Auto-batch: ET 18:00~18:30 (close + 2h)
                if (self._us_batch_auto and not self._us_batch_running
                        and self._is_us_process_alive() and self._is_us_batch_time()):
                    self._logger.info("[US_BATCH_AUTO] Scheduled batch triggered")
                    threading.Thread(target=self._run_us_batch_and_rebal, daemon=True).start()

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
