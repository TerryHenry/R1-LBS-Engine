@echo off
taskkill /FI "WINDOWTITLE eq RUCKUS Location Services*" /T /F >nul 2>&1
for /f "tokens=5" %%p in ('netstat -ano ^| findstr :8080 ^| findstr LISTENING') do taskkill /PID %%p /F >nul 2>&1
