@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  Q-TRON Gen4 Core - Backtest (2019~2026)
echo  %DATE% %TIME%
echo ============================================================

"%PYTHON%" -u main.py --backtest --start 2019-01-02 --end 2026-03-20

echo.
echo Results: report\output\
pause
