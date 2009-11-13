@echo off
SETLOCAL

SET CASSANDRA_LIBS=%CASSANDRA_HOME%\lib

FOR %%a IN (%CASSANDRA_HOME%\lib\*.jar) DO  call :append %%~fa
java -cp %CASSANDRA_LIBS% org.apache.cassandra.cli.CliMain

:append
SET CASSANDRA_LIBS=%CASSANDRA_LIBS%;%1%2
goto :finally


:finally

ENDLOCAL
