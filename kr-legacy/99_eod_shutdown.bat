@echo off
chcp 65001 > nul
set LOGFILE=C:\Q-TRON-32_ARCHIVE\Gen04\data\logs\eod_shutdown.log

echo ============================================================ >> "%LOGFILE%"
echo  [EOD_SHUTDOWN] %DATE% %TIME% >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"

:: Step 1: Kill monitor GUI if running (graceful)
tasklist /FI "WINDOWTITLE eq *monitor*" 2>nul | find /I "python" >nul
if not errorlevel 1 (
    echo [EOD] Closing monitor GUI... >> "%LOGFILE%"
    taskkill /FI "WINDOWTITLE eq *monitor*" /T 2>>"%LOGFILE%"
    timeout /t 5 /nobreak >nul
)

:: Step 2: Verify LIVE engine has exited (it should have by EOD 15:35)
tasklist /FI "WINDOWTITLE eq *LIVE*" 2>nul | find /I "python" >nul
if not errorlevel 1 (
    echo [EOD_WARN] LIVE engine still running at shutdown time >> "%LOGFILE%"
    echo [EOD_WARN] Skipping shutdown — manual check required >> "%LOGFILE%"
    exit /b 1
)

echo [EOD_CLEAN] All processes terminated >> "%LOGFILE%"

:: Step 3: Ask user before shutdown (3min timeout → auto-shutdown)
echo [EOD_SHUTDOWN] Asking user for confirmation (180s timeout)... >> "%LOGFILE%"
powershell -command "Add-Type -AssemblyName PresentationFramework; $timer = New-Object System.Windows.Threading.DispatcherTimer; $w = New-Object System.Windows.Window; $w.Title = 'Q-TRON EOD'; $w.Width = 420; $w.Height = 200; $w.WindowStartupLocation = 'CenterScreen'; $w.Topmost = $true; $sp = New-Object System.Windows.Controls.StackPanel; $sp.Margin = '20'; $global:sec = 180; $tb = New-Object System.Windows.Controls.TextBlock; $tb.Text = \"PC를 종료합니다.`n취소하려면 [취소] 버튼을 누르세요.`n`n남은 시간: 3:00\"; $tb.FontSize = 14; $tb.TextWrapping = 'Wrap'; $tb.Margin = '0,0,0,15'; $sp.Children.Add($tb); $bp = New-Object System.Windows.Controls.StackPanel; $bp.Orientation = 'Horizontal'; $bp.HorizontalAlignment = 'Center'; $btnOk = New-Object System.Windows.Controls.Button; $btnOk.Content = '지금 종료'; $btnOk.Width = 100; $btnOk.Height = 35; $btnOk.Margin = '0,0,15,0'; $btnOk.Add_Click({ $w.Tag = 'shutdown'; $w.Close() }); $bp.Children.Add($btnOk); $btnCancel = New-Object System.Windows.Controls.Button; $btnCancel.Content = '취소'; $btnCancel.Width = 100; $btnCancel.Height = 35; $btnCancel.Add_Click({ $w.Tag = 'cancel'; $w.Close() }); $bp.Children.Add($btnCancel); $sp.Children.Add($bp); $w.Content = $sp; $timer.Interval = [TimeSpan]::FromSeconds(1); $timer.Add_Tick({ $global:sec--; $m = [math]::Floor($global:sec/60); $s = $global:sec%%60; $tb.Text = \"PC를 종료합니다.`n취소하려면 [취소] 버튼을 누르세요.`n`n남은 시간: ${m}:$($s.ToString('D2'))\"; if($global:sec -le 0){ $w.Tag = 'shutdown'; $w.Close() } }); $timer.Start(); $w.ShowDialog() | Out-Null; $timer.Stop(); if($w.Tag -eq 'cancel'){ exit 1 } else { exit 0 }"
if errorlevel 1 (
    echo [EOD_CANCELLED] User cancelled shutdown >> "%LOGFILE%"
    exit /b 0
)

echo [EOD_SHUTDOWN] Shutting down now... >> "%LOGFILE%"
shutdown /s /t 10 /c "Q-TRON EOD shutdown"

exit /b 0
