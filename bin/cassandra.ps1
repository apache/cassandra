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
    [switch]$v,
    [switch]$s,
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
usage: cassandra.ps1 [-f] [-h] [-p pidfile] [-H dumpfile] [-D arg] [-E errorfile] [-install | -uninstall] [-help]
    -f              Run cassandra in foreground
    -install        install cassandra as a service
    -uninstall      remove cassandra service
    -p              pidfile tracked by server and removed on close (defaults to pid.txt)
    -H              change JVM HeapDumpPath
    -D              items to append to JVM_OPTS
    -E              change JVM ErrorFile
    -v              Print cassandra version and exit
    -s              Show detailed jvm environment information during launch
    -help           print this message

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

    # support direct run of .ps1 file w/out batch file
    if ($env:CASSANDRA_HOME -eq $null)
    {
        $scriptDir = Split-Path $script:MyInvocation.MyCommand.Path
        $env:CASSANDRA_HOME = (Get-Item $scriptDir).parent.FullName
    }
    . "$env:CASSANDRA_HOME\bin\source-conf.ps1"

    $conf = Find-Conf
    if ($s)
    {
        echo "Sourcing cassandra config file: $conf"
    }
    . $conf

    SetCassandraEnvironment
    if ($v)
    {
        PrintVersion
        exit
    }
    $pidfile = "$env:CASSANDRA_HOME\$pidfile"

    $logdir = "$env:CASSANDRA_HOME/logs"
    $storagedir = "$env:CASSANDRA_HOME/data"
    $env:CASSANDRA_PARAMS = $env:CASSANDRA_PARAMS + " -Dcassandra.logdir=""$logdir"" -Dcassandra.storagedir=""$storagedir"""

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
        $env:CASSANDRA_PARAMS = $env:CASSANDRA_PARAMS + ' -Dcassandra-pidfile="' + "$pidfile" + '"'
    }

    # Parse -D JVM_OPTS
    for ($i = 0; $i -lt $script:args.Length; ++$i)
    {
        if ($script:args[$i].Substring(0,2) -eq "-D")
        {
            $param = $script:args[$i].Substring(2)
            $env:JVM_OPTS = "$env:JVM_OPTS -D$param"
        }
    }

    if ($install -or $uninstall)
    {
        HandleInstallation
    }
    else
    {
        RunCassandra($f)
    }
}

#-----------------------------------------------------------------------------
Function HandleInstallation
{
    $SERVICE_JVM = """cassandra"""
    $PATH_PRUNSRV = "$env:CASSANDRA_HOME\bin\daemon"
    $PR_LOGPATH = $serverPath

    if (-Not (Test-Path $PATH_PRUNSRV\prunsrv.exe))
    {
        Write-Warning "Cannot find $PATH_PRUNSRV\prunsrv.exe.  Please download package from http://www.apache.org/dist/commons/daemon/binaries/windows/ to install as a service."
        Break
    }

    If (-NOT ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator"))
    {
        Write-Warning "Cannot perform installation without admin credentials.  Please re-run as administrator."
        Break
    }
    if (!$env:PRUNSRV)
    {
        $env:PRUNSRV="$PATH_PRUNSRV\prunsrv"
    }

    $regPath = "HKLM:\SYSTEM\CurrentControlSet\services\Tcpip\Parameters\"

    echo "Attempting to delete existing $SERVICE_JVM service..."
    Start-Sleep -s 2
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList "//DS//$SERVICE_JVM" -PassThru -WindowStyle Hidden

    echo "Reverting to default TCP keepalive settings (2 hour timeout)"
    Remove-ItemProperty -Path $regPath -Name KeepAliveTime -EA SilentlyContinue

    # Quit out if this is uninstall only
    if ($uninstall)
    {
        return
    }

    echo "Installing [$SERVICE_JVM]."
    Start-Sleep -s 2
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList "//IS//$SERVICE_JVM" -PassThru -WindowStyle Hidden

    echo "Setting launch parameters for [$SERVICE_JVM]"
    Start-Sleep -s 2

    # Change delim from " -" to ";-" in JVM_OPTS for prunsrv
    $env:JVM_OPTS = $env:JVM_OPTS -replace " -", ";-"
    $env:JVM_OPTS = $env:JVM_OPTS -replace " -", ";-"

    # Strip off leading ; if it's there
    $env:JVM_OPTS = $env:JVM_OPTS.TrimStart(";")

    # Broken multi-line for convenience - glued back together in a bit
    $args = @"
//US//$SERVICE_JVM
 --Jvm=auto --StdOutput auto --StdError auto
 --Classpath=$env:CLASSPATH
 --StartMode=jvm --StartClass=$env:CASSANDRA_MAIN --StartMethod=main
 --StopMode=jvm --StopClass=$env:CASSANDRA_MAIN  --StopMethod=stop
 ++JvmOptions=$env:JVM_OPTS ++JvmOptions=-DCassandra
 --PidFile "$pidfile"
"@
    $args = $args -replace [Environment]::NewLine, ""
    $proc = Start-Process -FilePath "$env:PRUNSRV" -ArgumentList $args -PassThru -WindowStyle Hidden

    echo "Setting KeepAliveTimer to 5 minutes for TCP keepalive"
    Set-ItemProperty -Path $regPath -Name KeepAliveTime -Value 300000

    echo "Installation of [$SERVICE_JVM] is complete"
}

#-----------------------------------------------------------------------------
Function PrintVersion()
{
    Write-Host "Cassandra Version: " -NoNewLine
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = "$env:JAVA_BIN"
    $pinfo.UseShellExecute = $false
    $pinfo.Arguments = "-cp $env:CLASSPATH org.apache.cassandra.tools.GetVersion"
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()
}

#-----------------------------------------------------------------------------
Function RunCassandra([string]$foreground)
{
    echo "Starting cassandra server"
    $cmd = @"
$env:JAVA_BIN
"@
    $arg1 = $env:CASSANDRA_PARAMS
    $arg2 = $env:JVM_OPTS
    $arg3 = "-cp $env:CLASSPATH"
    $arg4 = @"
"$env:CASSANDRA_MAIN"
"@

    $proc = $null

    if ($s)
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

        $exitCode = $?

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
            exit 1
        }

        if (-Not $exitCode)
        {
            exit 1
        }
    }
}

#-----------------------------------------------------------------------------
Main
