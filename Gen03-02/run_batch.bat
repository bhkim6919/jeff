@echo off
title QTRON-PYKRX-BATCH
cd /d "%~dp0"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo [QTRON] Starting pykrx batch ...
"%PYTHON%" main.py --batch

echo.
echo [QTRON] Done.
pause
