@echo off

set DSDIR=%~dp0..

"%DSDIR%\python\python.exe" "%DSDIR%\apache-cassandra\bin\cqlsh" %*