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
echo [Step 1/2] KRX data download...
"%PYTHON%" -u update_data_incremental.py 2>&1
if errorlevel 1 (
    echo ERROR: Data download failed.
    goto :error
)

echo.
echo [Step 2/2] Building v7 signals...
"%PYTHON%" -u gen3_signal_builder.py 2>&1
if errorlevel 1 (
    echo ERROR: Signal build failed.
    goto :error
)

echo.
echo ============================================================
echo  Batch complete. Ready for tomorrow live run.
echo ============================================================
goto :end

:error
echo.
echo === BATCH FAILED ===
exit /b 1

:end
