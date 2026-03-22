@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen03-02"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe
set LOGFILE=data\logs\batch_v7_%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%.log

if not exist data\logs mkdir data\logs

echo ============================================================
echo  Q-TRON Gen3 v7 Batch Pipeline
echo  %DATE% %TIME%
echo ============================================================

echo.
echo [Step 1/3] KRX data download...
"%PYTHON%" -u update_data_incremental.py
if errorlevel 1 (
    echo ERROR: Data download failed.
    goto :error
)
echo [Step 1/3] Done.

echo.
echo [Step 2/3] Building v7 signals...
"%PYTHON%" -u gen3_signal_builder.py
if errorlevel 1 (
    echo ERROR: Signal build failed.
    goto :error
)
echo [Step 2/3] Done.

echo.
echo [Step 3/3] Top20 collect + MA report...
"%PYTHON%" -u run_top20_batch.py --html
if errorlevel 1 (
    echo WARNING: Top20 report had issues (non-critical).
)
echo [Step 3/3] Done.

echo.
echo ============================================================
echo  Batch complete. Ready for tomorrow live run.
echo ============================================================

goto :end

:error
echo.
echo === BATCH FAILED ===
pause
exit /b 1

:end
pause
