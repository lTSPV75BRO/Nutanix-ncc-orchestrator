@echo off
setlocal EnableExtensions
rem Windows launcher for binaryGO.txt. Uses Git Bash so the same script
rem runs on Linux, macOS, and Windows and still cross-compiles all three.
set "SCRIPT=%~dp0binaryGO.txt"

where bash >nul 2>&1
if not errorlevel 1 goto havebash

if exist "%ProgramFiles%\Git\bin\bash.exe" goto gitpf
if exist "%ProgramFiles(x86)%\Git\bin\bash.exe" goto git86
if exist "%LocalAppData%\Programs\Git\bin\bash.exe" goto gitlocal

echo binaryGO.cmd: bash was not found. Install Git for Windows, then re-run.
echo From WSL you can also run: bash binaryGO.txt
exit /b 1

:havebash
bash "%SCRIPT%" %*
exit /b %ERRORLEVEL%

:gitpf
"%ProgramFiles%\Git\bin\bash.exe" "%SCRIPT%" %*
exit /b %ERRORLEVEL%

:git86
"%ProgramFiles(x86)%\Git\bin\bash.exe" "%SCRIPT%" %*
exit /b %ERRORLEVEL%

:gitlocal
"%LocalAppData%\Programs\Git\bin\bash.exe" "%SCRIPT%" %*
exit /b %ERRORLEVEL%
