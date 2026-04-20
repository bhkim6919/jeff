@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\pythonw.exe
set PORT=8080

:: Kill existing process on port
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    echo [PORT] %PORT% occupied by PID %%a - killing...
    taskkill /PID %%a /F >nul 2>&1
    timeout /t 1 /nobreak >nul
)

echo ============================================================
echo  Q-TRON REST Server (System Tray)
echo  http://localhost:%PORT%
echo  %DATE% %TIME%
echo ============================================================

:: Start with hidden console window (python.exe + start /min)
start "Q-TRON REST" /min "%PYTHON%" tray_server.py

echo.
echo  Started. Green Q icon in system tray.
echo  Right-click Q icon for menu.
echo  This window will close.
echo.
timeout /t 3 /nobreak >nul
exit
