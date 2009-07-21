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

SETLOCAL

if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
if NOT DEFINED CASSANDRA_CONF set CASSANDRA_CONF=%CASSANDRA_HOME%\conf
if NOT DEFINED CASSANDRA_MAIN set CASSANDRA_MAIN=org.apache.cassandra.service.CassandraDaemon
if NOT DEFINED JAVA_HOME goto err

REM ***** JAVA options *****

set JAVA_OPTS=^
 -ea^
 -Xdebug^
 -Xrunjdwp:transport=dt_socket,server=y,address=8888,suspend=n^
 -Xms128m^
 -Xmx1G^
 -XX:SurvivorRatio=8^
 -XX:+AggressiveOpts^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:CMSInitiatingOccupancyFraction=1^
 -XX:+CMSParallelRemarkEnabled^
 -XX:+HeapDumpOnOutOfMemoryError^
 -Dcom.sun.management.jmxremote.port=8080^
 -Dcom.sun.management.jmxremote.ssl=false^
 -Dcom.sun.management.jmxremote.authenticate=false

REM ***** CLASSPATH library setting *****

REM Shorten lib path for old platforms
subst P: "%CASSANDRA_HOME%\lib"
P:
set CLASSPATH=P:\

for %%i in (*.jar) do call :append %%i
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;P:\%*
goto :eof

:okClasspath
set CASSANDRA_CLASSPATH=%CASSANDRA_HOME%;%CASSANDRA_CONF%;%CLASSPATH%;%CASSANDRA_HOME%\build\classes
set CASSANDRA_PARAMS=-Dcassandra -Dstorage-config="%CASSANDRA_CONF%"
goto runDaemon

:runDaemon
echo Starting Cassandra Server
"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CASSANDRA_PARAMS% -cp "%CASSANDRA_CLASSPATH%" "%CASSANDRA_MAIN%"
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
