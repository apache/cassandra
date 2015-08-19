@REM
@REM Licensed to the Apache Software Foundation (ASF) under one or more
@REM contributor license agreements. See the NOTICE file distributed with
@REM this work for additional information regarding copyright ownership.
@REM The ASF licenses this file to You under the Apache License, Version 2.0
@REM (the "License"); you may not use this file except in compliance with
@REM the License. You may obtain a copy of the License at
@REM
@REM http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.

@echo off
if "%OS%" == "Windows_NT" setlocal

pushd "%~dp0"
call cassandra.in.bat

if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%~dp0..
if NOT DEFINED JAVA_HOME goto :err

set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.logdir="%CASSANDRA_HOME%\logs"
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% -Dcassandra.storagedir="%CASSANDRA_HOME%\data"

"%JAVA_HOME%\bin\java" -cp %CASSANDRA_CLASSPATH% %CASSANDRA_PARAMS% -Dlogback.configurationFile=logback-tools.xml org.apache.cassandra.tools.NodeTool %*
goto finally

:err
echo The JAVA_HOME environment variable must be set to run this program!
pause

:finally
ENDLOCAL & set RC=%ERRORLEVEL%
exit /B %RC%
