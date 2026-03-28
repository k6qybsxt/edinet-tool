@echo off
setlocal

set BACKUP_ROOT=D:\EDINET_Backup
set PROJECT_SRC=C:\Users\silve\EDINET_Pipeline
set REF_SRC=D:\EDINET_Data\reference

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HHmmss"') do set TS=%%i

set DEST=%BACKUP_ROOT%\%TS%

mkdir "%DEST%"
mkdir "%DEST%\EDINET_Pipeline"
mkdir "%DEST%\reference"

robocopy "%PROJECT_SRC%" "%DEST%\EDINET_Pipeline" /E /XD .venv .git __pycache__ data logs .vscode
robocopy "%REF_SRC%" "%DEST%\reference" /E

echo Backup completed: %DEST%
pause