@echo off
REM Scrape the public Yahoo league page and refresh docs/live_standings_2026.html.
REM Meant to run on a schedule (every 10 minutes) - see scripts/schedule_live_standings.ps1.
REM Publishes only when the data actually moved, so idle runs cost nothing.

setlocal
cd /d "%~dp0.."

set LOG=%TEMP%\live_standings.log
echo. >> "%LOG%"
echo ===== %DATE% %TIME% ===== >> "%LOG%"

python scripts\scrape_live_standings.py --skip-unchanged >> "%LOG%" 2>&1
if errorlevel 1 (
  echo scrape FAILED >> "%LOG%"
  exit /b 1
)

REM Nothing rewritten means nothing to publish.
set FILES=docs/live_standings_2026.html docs/data/live_standings_2026.json docs/data/playoff_odds_history_2026.json
for /f %%i in ('git status --porcelain -- %FILES%') do goto :publish
echo no change - skipping commit >> "%LOG%"
REM still push anything committed by hand that has not gone out yet
for /f %%i in ('git rev-list --count @{u}..HEAD') do set AHEAD=%%i
if not "%AHEAD%"=="0" goto :pushonly
exit /b 0

:pushonly
if "%LIVE_STANDINGS_NO_PUSH%"=="1" exit /b 0
echo pushing %AHEAD% pending commit(s) >> "%LOG%"
git push >> "%LOG%" 2>&1
exit /b 0

:publish
if "%LIVE_STANDINGS_NO_PUSH%"=="1" (
  echo data changed - push disabled, leaving changes in the working tree >> "%LOG%"
  exit /b 0
)
git add %FILES% >> "%LOG%" 2>&1
git commit -m "live standings auto-update" >> "%LOG%" 2>&1
git push >> "%LOG%" 2>&1
if errorlevel 1 (
  echo PUSH FAILED >> "%LOG%"
  exit /b 1
)
echo published >> "%LOG%"
exit /b 0
