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

if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%\..\..
if NOT DEFINED STRESS_HOME set STRESS_HOME=%CD%

set CLASSPATH="%STRESS_HOME%\build\classes"
set CLASSPATH=%CLASSPATH%;"%CASSANDRA_HOME%\build\classes\main"
set CLASSPATH=%CLASSPATH%;"%CASSANDRA_HOME%\build\classes\thrift"
for %%i in ("%CASSANDRA_HOME%\lib\*.jar") do call :append "%%i"
    set CLASSPATH=%CLASSPATH%;"%%i"
goto start

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:start
"%JAVA_HOME%\bin\java" -cp %CLASSPATH% org.apache.cassandra.stress.Stress %*
