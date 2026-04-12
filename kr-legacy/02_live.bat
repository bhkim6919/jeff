@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [2] Gen4 LIVE - Kiwoom Trading + Monitor + EOD
echo  %DATE% %TIME%
echo  Rebalance + Trail Stop + Daily Report
echo ============================================================
echo.

:: Start Monitor GUI v2 (background)
start "" /B "%PYTHON%" -u monitor_gui_v2.py --mode live

:: Start LIVE engine (foreground)
"%PYTHON%" -u main.py --live
echo.
echo ============================================================
echo  LIVE session ended.
echo ============================================================
pause
