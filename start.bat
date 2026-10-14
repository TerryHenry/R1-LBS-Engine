@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Creating virtual environment...
  py -3 -m venv .venv
  if errorlevel 1 python -m venv .venv
)
if not exist .venv\Scripts\python.exe (
  echo ERROR: Python 3 is required.
  pause
  exit /b 1
)
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
  echo ERROR: Dependency installation failed.
  pause
  exit /b 1
)
start "RUCKUS Location Services Browser" cmd /c "timeout /t 2 /nobreak >nul & start http://127.0.0.1:8080"
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8080
endlocal
