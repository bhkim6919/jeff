@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [5] Gen4 Backtest (2019~2026)
echo  %DATE% %TIME%
echo ============================================================
echo.

"%PYTHON%" -u main.py --backtest --start 2019-01-02 --end 2026-03-20
pause
