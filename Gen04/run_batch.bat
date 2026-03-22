@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  Q-TRON Gen4 Core - Batch Pipeline
echo  %DATE% %TIME%
echo ============================================================

echo.
echo [Batch] Scoring + Target Portfolio...
"%PYTHON%" -u main.py --batch
if errorlevel 1 (
    echo ERROR: Batch failed.
    goto :error
)

echo.
echo ============================================================
echo  Batch complete.
echo ============================================================

goto :end

:error
echo.
echo === BATCH FAILED ===
pause
exit /b 1

:end
pause
