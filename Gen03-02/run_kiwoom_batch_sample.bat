@echo off
title QTRON-KIWOOM-BATCH-SAMPLE10
cd /d "%~dp0"

set PYTHON32=C:\Users\User\AppData\Local\Programs\Python\Python39-32\python.exe

if not exist "%PYTHON32%" (
    echo [ERROR] 32bit Python not found: %PYTHON32%
    pause
    exit /b 1
)

echo [QTRON] Starting kiwoom-batch (sample 10) ...
"%PYTHON32%" main.py --kiwoom-batch --sample 10

echo.
echo [QTRON] Done.
pause
