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
set INSTALL="INSTALL"
set UNINSTALL="UNINSTALL"

pushd %~dp0..
if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
popd

if NOT DEFINED CASSANDRA_MAIN set CASSANDRA_MAIN=org.apache.cassandra.thrift.CassandraDaemon
if NOT DEFINED JAVA_HOME goto err

REM ***** JAVA options *****
set JAVA_OPTS=-ea^
 -javaagent:"%CASSANDRA_HOME%\lib\jamm-0.2.5.jar"^
 -Xms1G^
 -Xmx1G^
 -XX:+HeapDumpOnOutOfMemoryError^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:SurvivorRatio=8^
 -XX:MaxTenuringThreshold=1^
 -XX:CMSInitiatingOccupancyFraction=75^
 -XX:+UseCMSInitiatingOccupancyOnly^
 -Dcom.sun.management.jmxremote.port=7199^
 -Dcom.sun.management.jmxremote.ssl=false^
 -Dcom.sun.management.jmxremote.authenticate=false^
 -Dlog4j.configuration=log4j-server.properties^
 -Dlog4j.defaultInitOverride=true

REM ***** CLASSPATH library setting *****

REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%CASSANDRA_HOME%\conf"

REM For each jar in the CASSANDRA_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%CASSANDRA_HOME%\lib\*.jar") do call :append "%%i"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:okClasspath
REM Include the build\classes\main directory so it works in development
set CASSANDRA_CLASSPATH=%CLASSPATH%;"%CASSANDRA_HOME%\build\classes\main";"%CASSANDRA_HOME%\build\classes\thrift"
set CASSANDRA_PARAMS=-Dcassandra -Dcassandra-foreground=yes
if /i "%ARG%" == "INSTALL" goto doInstallOperation
if /i "%ARG%" == "UNINSTALL" goto doInstallOperation
goto runDaemon


:runDaemon
echo Starting Cassandra Server
"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp %CASSANDRA_CLASSPATH% "%CASSANDRA_MAIN%"
goto finally

:doInstallOperation
set SERVICE_JVM="cassandra"
rem location of Prunsrv
set PATH_PRUNSRV=%CASSANDRA_HOME%\bin\daemon\
set PR_LOGPATH=%PATH_PRUNSRV%

rem fix up java ops replace ' -' with ' ;-'
set JAVA_OPTS_DELM=%JAVA_OPTS: -=;-%

rem Allow prunsrv to be overridden
if "%PRUNSRV%" == "" set PRUNSRV=%PATH_PRUNSRV%prunsrv

echo trying to delete service if it has been created already
"%PRUNSRV%" //DS//%SERVICE_JVM%
rem quit if we're just going to uninstall
if /i "%ARG%" == "UNINSTALL" goto finally

echo.
echo Installing %SERVICE_JVM%. If you get registry warnings, re-run as an Administrator
"%PRUNSRV%" //IS//%SERVICE_JVM%
echo Setting the parameters for %SERVICE_JVM%
rem set PR_CLASSPATH=%CASSANDRA_CLASSPATH%
"%PRUNSRV%" //US//%SERVICE_JVM% ^
 --Jvm=auto --StdOutput auto --StdError auto ^
 --Classpath=%CASSANDRA_CLASSPATH% ^
 --StartMode=jvm --StartClass=%CASSANDRA_MAIN% --StartMethod=main ^
 --StopMode=jvm --StopClass=%CASSANDRA_MAIN%  --StopMethod=stop ^
 ++JvmOptions=%JAVA_OPTS_DELM% ++JvmOptions=-DCassandra ^
 --PidFile pid.txt
 
echo Installation of %SERVICE_JVM% is complete
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
