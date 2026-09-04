call "c:\programdata\anaconda3\condabin\conda.bat" activate
cd /d "%~dp0"

:: Kill any leftover processes from a previous run
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":4200 " ^| findstr "LISTENING"') do taskkill /f /pid %%a 2>nul
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":5000 " ^| findstr "LISTENING"') do taskkill /f /pid %%a 2>nul
wmic process where "commandline like '%%serve_flows%%'" delete 2>nul
wmic process where "commandline like '%%prefect server%%'" delete 2>nul
timeout /t 2 /nobreak >nul

:: Parse optional --port=XXXX argument (default 5000)
set FLASK_PORT=5000
for %%A in (%*) do (
  echo %%A | findstr /b "--port=" >nul && for /f "tokens=2 delims==" %%B in ("%%A") do set FLASK_PORT=%%B
)

set PREFECT_API_URL=http://127.0.0.1:4200/api
set PREFECT_API_DATABASE_TIMEOUT=300

:: Create a local instrument_conf.py from the tracked default on first run
if not exist instrument_conf.py copy instrument_conf.default.py instrument_conf.py

:: Start Prefect server in the background
start /b uv run prefect server start
timeout /t 3 /nobreak >nul

:: Start flow server in the background
start /b uv run python serve_flows.py

:: Start the Flask app (foreground, Ctrl+C won't prompt Y/N)
cmd /c uv run python main.py

:: Clean up on exit (always runs after main.py stops)
for /f "tokens=5" %%a in ('netstat -aon 2^>nul ^| findstr ":4200 " ^| findstr "LISTENING"') do taskkill /f /pid %%a 2>nul
wmic process where "commandline like '%%serve_flows%%'" delete 2>nul
wmic process where "commandline like '%%prefect server%%'" delete 2>nul
