@echo off
chcp 65001 > nul

echo ============================================================
echo  Q-TRON REST Server - Autostart Setup
echo ============================================================
echo.

set STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
set SCRIPT=C:\Q-TRON-32_ARCHIVE\Gen04-REST\09_rest_server_bg.bat
set SHORTCUT=%STARTUP%\Q-TRON REST Server.lnk

:: Create shortcut using PowerShell
powershell -NoProfile -Command "$ws = New-Object -ComObject WScript.Shell; $sc = $ws.CreateShortcut('%SHORTCUT%'); $sc.TargetPath = '%SCRIPT%'; $sc.WorkingDirectory = 'C:\Q-TRON-32_ARCHIVE\Gen04-REST'; $sc.WindowStyle = 7; $sc.Description = 'Q-TRON REST Server (System Tray)'; $sc.Save()"

if exist "%SHORTCUT%" (
    echo  [OK] Autostart registered.
    echo  Location: %SHORTCUT%
    echo.
    echo  PC boot -^> login -^> Q-TRON REST auto-start
    echo.
    echo  To remove: delete the shortcut from
    echo    %STARTUP%
    echo  Or run: remove_autostart.bat
) else (
    echo  [FAIL] Shortcut creation failed.
)

echo.
pause
