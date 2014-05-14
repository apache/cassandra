#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
param (
    [switch]$install,
    [switch]$uninstall,
    [switch]$help,
    [switch]$verbose,
    [switch]$f,
    [string]$p,
    [string]$H,
    [string]$E
)

$pidfile = "pid.txt"

#-----------------------------------------------------------------------------
Function ValidateArguments
{
    if ($install -and $uninstall)
    {
        exit
    }
    if ($help)
    {
        PrintUsage
    }
}

#-----------------------------------------------------------------------------
Function PrintUsage
{
    echo @"
usage: cassandra.ps1 [-f] [-h] [-p pidfile] [-H dumpfile] [-E errorfile] [-install | -uninstall] [-help]
    -f              Run cassandra in foreground
    -install        install cassandra as a service
    -uninstall      remove cassandra service
    -p              pidfile tracked by server and removed on close
    -H              change JVM HeapDumpPath
    -E              change JVM ErrorFile
    -help           print this message
    -verbose        Show detailed command-line parameters for cassandra run

    NOTE: installing cassandra as a service requires Commons Daemon Service Runner
        available at http://commons.apache.org/proper/commons-daemon/"
"@
    exit
}

#-----------------------------------------------------------------------------
# Note: throughout these scripts we're replacing \ with /.  This allows clean
# operation on both command-prompt and cygwin-based environments.
Function Main
{
    ValidateArguments

    . "$env:CASSANDRA_HOME/bin/source-conf.ps1"
    $conf = Find-Conf
    if ($verbose)
    {
        echo "Sourcing cassandra config file: $conf"
    }
    . $conf

    SetCassandraEnvironment
    $pidfile = "$env:CASSANDRA_HOME/$pidfile"

    # Other command line params
    if ($H)
    {
        $env:JVM_OPTS = $env:JVM_OPTS + " -XX:HeapDumpPath=$H"
    }
    if ($E)
    {
        $env:JVM_OPTS = $env:JVM_OPTS + " -XX:ErrorFile=$E"
    }
    if ($p)
    {
        $pidfile = "$p"
        $env:CASSANDRA_PARAMS = $env:CASSANDRA_PARAMS + " -Dcassandra-pidfile=$pidfile"
    }

    if ($install -or $uninstall)
    {
        HandleInstallation
    }
    else
    {
        CleanOldRun
        RunCassandra($f)
    }
}

#-----------------------------------------------------------------------------
Function HandleInstallation
{
    $SERVICE_JVM = "cassandra"
    $PATH_PRUNSRV = "$env:CASSANDRA_HOME/bin/daemon/"
    $PR_LOGPATH = $serverPath

    if (!$env:PRUNSRV)
    {
        $env:PRUNSRV="$PATH_PRUNSRV/prunsrv"
    }

    echo "Attempting to delete existing $SERVICE_JVM service..."
    Start-Sleep -s 2
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList "//DS//$SERVICE_JVM" -PassThru -WindowStyle Hidden

    # Quit out if this is uninstall only
    if ($uninstall)
    {
        return
    }

    echo "Installing [$SERVICE_JVM]. If you get registry warnings, re-run as an Administrator"
    Start-Sleep -s 2
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList "//IS//$SERVICE_JVM" -PassThru -WindowStyle Hidden

    echo "Setting the parameters for [$SERVICE_JVM]"
    Start-Sleep -s 2

    # Broken multi-line for convenience - glued back together in a bit
    $args = @"
//US//$SERVICE_JVM
 --Jvm=auto --StdOutput auto --StdError auto
 --Classpath=$env:CLASSPATH
 --StartMode=jvm --StartClass=$env:CASSANDRA_MAIN --StartMethod=main
 --StopMode=jvm --StopClass=$env:CASSANDRA_MAIN  --StopMethod=stop
 ++JvmOptions=$env:JVM_OPTS ++JvmOptions=-DCassandra
 --PidFile $pidfile
"@
    $args = $args -replace [Environment]::NewLine, ""
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList $args -PassThru -WindowStyle Hidden

    echo "Installation of [$SERVICE_JVM] is complete"
}

#-----------------------------------------------------------------------------
Function CleanOldRun
{
    # see if we already have an instance of cassandra running from this folder
    if (Test-Path $pidfile)
    {
        $a = Get-Content $pidfile

        # file is there but empty
        if ($a -eq $null)
        {
            Remove-Item $pidfile
            return
        }

        $proc = Get-Process -Id $a -ErrorAction SilentlyContinue
        if ($proc)
        {
            echo "ERROR!  There is already an instance of cassandra running from this folder with pid: $a.  Please use stop-server.bat to stop this instance before starting cassandra."
            exit
        }
        else
        {
            Remove-Item $pidfile
        }
    }
}

#-----------------------------------------------------------------------------
Function RunCassandra([string]$foreground)
{
    echo "Starting cassandra server"
    $cmd = @"
$env:JAVA_BIN
"@
    $arg1 = $env:JVM_OPTS
    $arg2 = $env:CASSANDRA_PARAMS
    $arg3 = "-cp $env:CLASSPATH"
    $arg4 = @"
"$env:CASSANDRA_MAIN"
"@

    $proc = $null

    if ($verbose)
    {
        echo "Running cassandra with: [$cmd $arg1 $arg2 $arg3 $arg4]"
    }

    if ($foreground -ne "False")
    {
        $cygwin = $false
        try
        {
            $uname = uname -o
            $cygwin = $true
        }
        catch
        {
            # Failed at uname call, not in cygwin
        }

        if ($cygwin)
        {
            # if running on cygwin, we cannot capture ctrl+c signals as mintty traps them and then
            # SIGKILLs processes, so we'll need to record our $pidfile file for future
            # stop-server usage
            if (!$p)
            {
                $arg2 = $arg2 + " -Dcassandra-pidfile=$pidfile"
            }
            echo @"
*********************************************************************
*********************************************************************
Warning!  Running cassandra.bat -f on cygwin usually breaks control+c
functionality.  You'll need to use:
    stop-server.bat -p $pidfile
to stop your server or kill the java.exe instance.
*********************************************************************
*********************************************************************"
"@
            # Note: we can't pause here and force user confirmation for a similar reason as there's a
            # layer of indirection between powershell and stdin.
        }

        $arg2 = $arg2 + " -Dcassandra-foreground=yes"

        $pinfo = New-Object System.Diagnostics.ProcessStartInfo
        $pinfo.FileName = "$env:JAVA_BIN"
        $pinfo.RedirectStandardInput = $true
        $pinfo.UseShellExecute = $false
        $pinfo.Arguments = $arg1,$arg2,$arg3,$arg4
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pinfo
        $p.Start() | Out-Null
        echo $p.Id > $pidfile
        $p.WaitForExit()
    }
    else
    {
        $proc = Start-Process -FilePath "$cmd" -ArgumentList $arg1,$arg2,$arg3,$arg4 -PassThru -WindowStyle Hidden

        # Always store the pid, even if we're not registering it with the server
        # The startup script uses this pid file as a protection against duplicate startup from the same folder
        try
        {
            echo $proc.Id > $pidfile
        }
        catch
        {
            echo @"
WARNING! Failed to write pidfile to $pidfile.  stop-server.bat and
    startup protection will not be available.
"@
        }

        $cassPid = $proc.Id
        if (-Not ($proc) -or $cassPid -eq "")
        {
            echo "Error starting cassandra."
            echo "Run with -verbose for more information about runtime environment"
        }
        elseif ($foreground -eq "False")
        {
            echo "Started cassandra successfully with pid: $cassPid"
        }
    }
}

#-----------------------------------------------------------------------------
Main
