@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [1F] Gen4 Batch FAST - Signal Only (skip reports)
echo  %DATE% %TIME%
echo ============================================================
echo.

"%PYTHON%" -u main.py --batch --fast
if errorlevel 1 (
    echo.
    echo === BATCH FAILED ===
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Batch (fast) complete.
echo ============================================================
pause
