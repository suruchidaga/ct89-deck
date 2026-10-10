@echo off
rem CT89 Deck: installs what it needs the first time, then starts the dashboard.
cd /d "%~dp0"
where py >nul 2>nul
if errorlevel 1 (
  echo Python is not installed. Get it from https://www.python.org/downloads/ ^(tick "Add python.exe to PATH"^), then run this again.
  pause
  exit /b 1
)
if not exist ".installed" (
  echo Installing required packages ^(first run only^)...
  py -m pip install -r requirements.txt
  if errorlevel 1 (
    echo Package install failed. Please send this window's text to the person who shared CT89 Deck.
    pause
    exit /b 1
  )
  echo done> ".installed"
)
echo Starting CT89 Deck. Keep this window open while you use the dashboard; close it to stop.
py server.py
pause
