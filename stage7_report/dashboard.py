"""
Dashboard
=========
Reporter의 HTML을 브라우저로 자동 오픈하는 헬퍼.
장 종료 후 리포트 확인용.
"""

import os
import webbrowser
import glob


LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")


def open_latest_report():
    """logs/ 폴더에서 가장 최근 HTML 리포트를 브라우저로 오픈."""
    log_dir = os.path.abspath(LOG_DIR)
    files   = sorted(glob.glob(os.path.join(log_dir, "report_*.html")))

    if not files:
        print("[Dashboard] 리포트 파일 없음")
        return

    latest = files[-1]
    print(f"[Dashboard] 브라우저로 오픈 → {latest}")
    webbrowser.open(f"file:///{latest.replace(os.sep, '/')}")


def open_report(path: str):
    """특정 경로의 리포트를 브라우저로 오픈."""
    webbrowser.open(f"file:///{os.path.abspath(path).replace(os.sep, '/')}")
