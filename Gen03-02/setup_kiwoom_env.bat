@echo off
title QTRON-KIWOOM-ENV-SETUP
cd /d "%~dp0"

set PYTHON32=C:\Users\User\AppData\Local\Programs\Python\Python39-32\python.exe

if not exist "%PYTHON32%" (
    echo [ERROR] 32bit Python not found: %PYTHON32%
    pause
    exit /b 1
)

echo [SETUP] Installing packages into 32bit Python ...
echo.

"%PYTHON32%" -m pip install --upgrade pip
"%PYTHON32%" -m pip install PyQt5
"%PYTHON32%" -m pip install pandas numpy

echo.
echo [SETUP] Done. Now run run_kiwoom_batch.bat
pause
