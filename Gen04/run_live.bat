@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  Q-TRON Gen4 Core - LIVE Mode (Kiwoom)
echo  %DATE% %TIME%
echo  WARNING: Real order execution!
echo ============================================================
echo.

set /p CONFIRM="Start LIVE mode? (y/n): "
if /i not "%CONFIRM%"=="y" (
    echo Cancelled.
    goto :end
)

"%PYTHON%" -u main.py --live

:end
pause
