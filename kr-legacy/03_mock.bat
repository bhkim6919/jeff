@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [3] Gen4 Mock - State Check (no broker)
echo  %DATE% %TIME%
echo ============================================================
echo.

:: Launch GUI monitor in separate window
start "" /B "%PYTHON%" -u monitor_gui_v2.py --mode paper_test

:: Start Mock engine (foreground)
"%PYTHON%" -u main.py --mock
pause
