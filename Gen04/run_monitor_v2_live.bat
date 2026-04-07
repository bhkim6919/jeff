@echo off
chcp 65001 > nul
title Q-TRON Mission Control v2 [LIVE]

cd /d "%~dp0"
C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe -u monitor_gui_v2.py --mode live

pause
