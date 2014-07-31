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
pushd %~dp0..
if NOT DEFINED CASSANDRA_HOME set CASSANDRA_HOME=%CD%
popd

if NOT DEFINED CASSANDRA_CONF set CASSANDRA_CONF="%CASSANDRA_HOME%\conf"

REM the default location for commitlogs, sstables, and saved caches
REM if not set in cassandra.yaml
set cassandra_storagedir="%CASSANDRA_HOME%\data"

REM JAVA_HOME can optionally be set here
REM set JAVA_HOME="<directory>"

REM ***** CLASSPATH library setting *****

REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="%CASSANDRA_HOME%\conf"

REM For each jar in the CASSANDRA_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%CASSANDRA_HOME%\lib\*.jar") do call :append "%%i"
goto :okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:okClasspath

REM Include the build\classes\main directory so it works in development
set CASSANDRA_CLASSPATH=%CLASSPATH%;"%CASSANDRA_HOME%\build\classes\main";%CASSANDRA_CONF%;"%CASSANDRA_HOME%\build\classes\thrift"

REM Add the default storage location.  Can be overridden in conf\cassandra.yaml
set CASSANDRA_PARAMS=%CASSANDRA_PARAMS% "-Dcassandra.storagedir=%CASSANDRA_HOME%\data"
