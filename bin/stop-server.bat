@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off
if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
popd

REM -----------------------------------------------------------------------------
REM See if we have the capabilities of running the powershell scripts
for /F "delims=" %%i in ('powershell Get-ExecutionPolicy') do set PERMISSION=%%i
if "%PERMISSION%" == "Unrestricted" goto runPowerShell
goto runLegacy

REM -----------------------------------------------------------------------------
:runPowerShell
REM Need to generate a random title for this command-prompt to determine its pid.
REM We detach and re-attach the console in stop-server.ps1 to send ctrl+c to the
REM running cassandra process and need to re-attach here to print results.
set /A rand=%random% %% (100000 - 1 + 1) + 1
TITLE %rand%
FOR /F "tokens=2 delims= " %%A IN ('TASKLIST /FI ^"WINDOWTITLE eq %rand%^" /NH') DO set PID=%%A

REM Start with /B -> the control+c event we generate in stop-server.ps1 percolates
REM up and hits this external batch file if we call powershell directly.
start /WAIT /B powershell /file "%CASSANDRA_HOME%/bin/stop-server.ps1" -batchpid %PID% %*
goto finally

REM -----------------------------------------------------------------------------
:runLegacy
echo WARNING! Powershell script execution unavailable.
echo    Please use 'powershell Set-ExecutionPolicy Unrestricted'
echo    on this user-account to run cassandra with fully featured
echo    functionality on this platform.

echo Cannot stop server without powershell access.
goto finally

:finally
ENDLOCAL
