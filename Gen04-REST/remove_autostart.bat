@echo off
chcp 65001 > nul

set SHORTCUT=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\Q-TRON REST Server.lnk

if exist "%SHORTCUT%" (
    del "%SHORTCUT%"
    echo [OK] Autostart removed.
) else (
    echo [INFO] Autostart not registered.
)

pause
