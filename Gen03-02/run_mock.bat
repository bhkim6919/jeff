@echo off
title QTRON-MOCK
cd /d "%~dp0"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo [QTRON] Starting mock ...
"%PYTHON%" main.py --mock

echo.
echo [QTRON] Done.
pause
