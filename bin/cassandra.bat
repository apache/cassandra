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

set ARG=%1
if /i "%ARG%" == "LEGACY" goto runLegacy
set INSTALL="INSTALL"
set UNINSTALL="UNINSTALL"

pushd %~dp0..
if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
popd

REM -----------------------------------------------------------------------------
REM See if we have access to run unsigned powershell scripts
for /F "delims=" %%i in ('powershell Get-ExecutionPolicy') do set PERMISSION=%%i
if "%PERMISSION%" == "Unrestricted" goto runPowerShell
goto runLegacy

REM -----------------------------------------------------------------------------
:runPowerShell
echo Detected powershell execution permissions.  Running with enhanced startup scripts.
set errorlevel=
powershell /file "%CASSANDRA_HOME%\bin\cassandra.ps1" %*
exit /b %errorlevel%

REM -----------------------------------------------------------------------------
:runLegacy
echo WARNING! Powershell script execution unavailable.
echo    Please use 'powershell Set-ExecutionPolicy Unrestricted'
echo    on this user-account to run cassandra with fully featured
echo    functionality on this platform.

echo Starting with legacy startup options

if NOT DEFINED CASSANDRA_MAIN set CASSANDRA_MAIN=org.apache.cassandra.service.CassandraDaemon
if NOT DEFINED JAVA_HOME goto :err

REM -----------------------------------------------------------------------------
REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -javaagent:"%CASSANDRA_HOME%\lib\jamm-0.3.0.jar"^
 -Xms2G^
 -Xmx2G^
 -XX:+HeapDumpOnOutOfMemoryError^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:SurvivorRatio=8^
 -XX:MaxTenuringThreshold=1^
 -XX:CMSInitiatingOccupancyFraction=75^
 -XX:+UseCMSInitiatingOccupancyOnly^
 -Dlogback.configurationFile=logback.xml^
 -Dcassandra.jmx.local.port=7199
REM **** JMX REMOTE ACCESS SETTINGS SEE: https://wiki.apache.org/cassandra/JmxSecurity ***
REM -Dcom.sun.management.jmxremote.port=7199^
REM -Dcom.sun.management.jmxremote.ssl=false^
REM -Dcom.sun.management.jmxremote.authenticate=true^
REM -Dcom.sun.management.jmxremote.password.file=C:\jmxremote.password

REM ***** CLASSPATH library setting *****
REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%CASSANDRA_HOME%\conf"

REM For each jar in the CASSANDRA_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%CASSANDRA_HOME%\lib\*.jar") do call :append "%%i"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath
REM Include the build\classes\main directory so it works in development
set CASSANDRA_CLASSPATH=%CLASSPATH%;"%CASSANDRA_HOME%\build\classes\main";"%CASSANDRA_HOME%\build\classes\thrift"
set CASSANDRA_PARAMS=-Dcassandra -Dcassandra-foreground=yes
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.logdir="%CASSANDRA_HOME%\logs"
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.storagedir="%CASSANDRA_HOME%\data"

if /i "%ARG%" == "INSTALL" goto doInstallOperation
if /i "%ARG%" == "UNINSTALL" goto doInstallOperation

echo Starting Cassandra Server
"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% "%CASSANDRA_MAIN%"
goto finally

REM -----------------------------------------------------------------------------
:doInstallOperation
set SERVICE_JVM="cassandra"
rem location of Prunsrv
set PATH_PRUNSRV=%CASSANDRA_HOME%\bin\daemon\
set PR_LOGPATH=%PATH_PRUNSRV%

rem Allow prunsrv to be overridden
if "%PRUNSRV%" == "" set PRUNSRV=%PATH_PRUNSRV%prunsrv

echo trying to delete service if it has been created already
"%PRUNSRV%" //DS//%SERVICE_JVM%
rem quit if we're just going to uninstall
if /i "%ARG%" == "UNINSTALL" goto finally

echo Installing %SERVICE_JVM%. If you get registry warnings, re-run as an Administrator
"%PRUNSRV%" //IS//%SERVICE_JVM%

echo Setting startup parameters for %SERVICE_JVM%
set cmd="%PRUNSRV%" //US//%SERVICE_JVM% ^
 --Jvm=auto --StdOutput auto --StdError auto ^
 --Classpath=%CASSANDRA_CLASSPATH% ^
 --StartMode=jvm --StartClass=%CASSANDRA_MAIN% --StartMethod=main ^
 --StopMode=jvm --StopClass=%CASSANDRA_MAIN%  --StopMethod=stop

REM convert ' -' into ';-' so we can tokenize on semicolon as we may have spaces in folder names
set tempOptions=%JAVA_OPTS: -=;-%
REM Append the JAVA_OPTS, each with independent ++JvmOptions as delimited list fails for some options
:optStrip
for /F "tokens=1* delims=;" %%a in ("%tempOptions%") do (
    set JVMOPTIONS=%JVMOPTIONS% ++JvmOptions=%%a
    set tempOptions=%%b
)
if defined tempOptions goto :optStrip

REM do the same for CASSANDRA_PARAMS
set tempOptions=%CASSANDRA_PARAMS: -=;-%

:paramStrip
for /F "tokens=1* delims=;" %%a in ("%tempOptions%") do (
    set JVMOPTIONS=%JVMOPTIONS% ++JvmOptions=%%a
    set tempOptions=%%b
)
if defined tempOptions goto :paramStrip

%cmd% %JVMOPTIONS%

echo Installation of %SERVICE_JVM% is complete
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

REM -----------------------------------------------------------------------------
:finally

ENDLOCAL
