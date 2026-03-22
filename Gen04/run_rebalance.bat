@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  Q-TRON Gen4 Core - Force Rebalance
echo  %DATE% %TIME%
echo  WARNING: Immediate rebalance execution!
echo ============================================================
echo.

set /p CONFIRM="Force rebalance now? (y/n): "
if /i not "%CONFIRM%"=="y" (
    echo Cancelled.
    goto :end
)

"%PYTHON%" -u main.py --rebalance

:end
pause
