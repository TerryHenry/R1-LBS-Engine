@echo off
setlocal
set CFG=%USERPROFILE%\AppData\Local\RuckusLocationServices\settings.json
if not exist "%CFG%" (
 echo No persistent configuration exists yet.
 pause
 exit /b 0
)
notepad "%CFG%"
endlocal
