@echo off
setlocal
where mosquitto.exe >nul 2>&1
if %errorlevel%==0 (
  echo Mosquitto is already available on PATH.
  mosquitto -h | findstr /B /C:"mosquitto" | more
  pause
  exit /b 0
)
where winget.exe >nul 2>&1
if %errorlevel%==0 (
  echo Attempting to install Eclipse Mosquitto using winget...
  winget install --id EclipseFoundation.Mosquitto --exact --source winget
  if %errorlevel%==0 (
    echo Mosquitto installation completed. Restart RUCKUS Location Services.
    pause
    exit /b 0
  )
)
echo Automatic installation was not available.
echo Open the official Eclipse Mosquitto Windows downloads page:
echo https://mosquitto.org/download/
start "" "https://mosquitto.org/download/"
pause
endlocal
