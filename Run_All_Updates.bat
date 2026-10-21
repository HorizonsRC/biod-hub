@echo off
REM ============================================================
REM BioD Hub - run every dashboard update script.
REM Wrapper for Run_All_Updates.py, for the Windows scheduled task.
REM
REM Task Scheduler cannot pick the ArcGIS Pro conda environment on its
REM own, so this names that python.exe explicitly and runs the repo as
REM the working directory (the scripts use relative paths).
REM
REM Any arguments are passed straight through, so the task action can be
REM   Run_All_Updates.bat                 (everything enabled in STEPS)
REM   Run_All_Updates.bat --dry-run       (list the steps, run nothing)
REM   Run_All_Updates.bat --only hub      (one step)
REM
REM Exits with the runner's own exit code, so a failed step shows as a
REM failed task in Task Scheduler (Last Run Result 0x1).
REM ============================================================

setlocal

REM %LOCALAPPDATA% expands to the signed-in user's AppData\Local, so this works
REM for any user with the same named Pro environment.
set "PYEXE=%LOCALAPPDATA%\ESRI\conda\envs\arcpro-scripts-3-5\python.exe"
set "REPO=%~dp0"
set "WRAPLOG=%REPO%logs\runner\wrapper.log"

REM The scripts log macrons and dashes; without this the pipe below chokes.
set "PYTHONIOENCODING=utf-8"

if not exist "%REPO%logs\runner" mkdir "%REPO%logs\runner"

echo. >> "%WRAPLOG%"
echo ==== %DATE% %TIME% : start %* >> "%WRAPLOG%"

if not exist "%PYEXE%" (
    echo ERROR: python not found at %PYEXE% >> "%WRAPLOG%"
    echo ERROR: python not found at %PYEXE%
    endlocal
    exit /b 2
)

cd /d "%REPO%"
"%PYEXE%" Run_All_Updates.py %*
set "RC=%ERRORLEVEL%"

echo ==== %DATE% %TIME% : finished, exit code %RC% >> "%WRAPLOG%"

endlocal & exit /b %RC%
