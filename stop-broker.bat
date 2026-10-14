@echo off
rem stop.bat stops only the web app and leaves this app's Mosquitto broker running on purpose,
rem so RUCKUS One stays connected and does not have to reconnect (which can take many minutes).
rem Run this file when you really want the broker stopped too, e.g. before changing Mosquitto itself.
setlocal
set "CONF=%~dp0logs\lbs_broker\mosquitto.conf"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$c=$env:CONF; Get-CimInstance Win32_Process -Filter \"Name='mosquitto.exe'\" | Where-Object { $_.CommandLine -and $_.CommandLine.ToLower().Contains($c.ToLower()) } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Host ('Stopped broker PID ' + $_.ProcessId) }"
if exist "%~dp0logs\lbs_broker\broker.json" del "%~dp0logs\lbs_broker\broker.json"
endlocal
