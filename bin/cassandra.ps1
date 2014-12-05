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

    # Parse -D and -X JVM_OPTS
    for ($i = 0; $i -lt $script:args.Length; ++$i)
    {
        if ($script:args[$i].StartsWith("-D") -Or $script:args[$i].StartsWith("-X"))
        {
            $env:JVM_OPTS = "$env:JVM_OPTS " + $script:args[$i]
        }
    }

    if ($install -or $uninstall)
    {
        HandleInstallation
    }
    else
    {
        VerifyPortsAreAvailable
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

    $args = @"
//US//$SERVICE_JVM
 --Jvm=auto --StdOutput auto --StdError auto
 --Classpath=$env:CLASSPATH
 --StartMode=jvm --StartClass=$env:CASSANDRA_MAIN --StartMethod=main
 --StopMode=jvm --StopClass=$env:CASSANDRA_MAIN  --StopMethod=stop
 --PidFile "$pidfile"
"@

    # Include cassandra params
    $prunArgs = "$env:CASSANDRA_PARAMS $env:JVM_OPTS"

    # Change to semicolon delim as we can't split on space due to potential spaces in directory names
    $prunArgs = $prunArgs -replace " -", ";-"

    # JvmOptions w/multiple semicolon delimited items isn't working correctly.  storagedir and logdir were
    # both being ignored / failing to parse on startup.  See CASSANDRA-8115
    $split_opts = $prunArgs.Split(";")
    foreach ($arg in $split_opts)
    {
        $args += " ++JvmOptions=$arg"
    }

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

    if ($foreground)
    {
        $cygwin = $false
        try
        {
            $uname = uname -o
            if ($uname.CompareTo("Cygwin") -eq 0)
            {
                $cygwin = $true
            }
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
                echo "Detected cygwin runtime environment.  Adding -Dcassandra-pidfile=$pidfile to JVM params as control+c trapping on mintty is inconsistent"
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
            echo $_.Exception.Message
            exit 1
        }

        if (-Not $exitCode)
        {
            exit 1
        }
    }
}

#-----------------------------------------------------------------------------
Function VerifyPortsAreAvailable
{
    # Need to confirm 5 different ports are available or die if any are currently bound
    # From cassandra.yaml:
    #   storage_port
    #   ssl_storage_port
    #   native_transport_port
    #   rpc_port, which we'll match to rpc_address
    # and from env: JMX_PORT which we cache in our environment during SetCassandraEnvironment for this check
    $toMatch = @("storage_port:","ssl_storage_port:","native_transport_port:","rpc_port")
    $yaml = Get-Content "$env:CASSANDRA_CONF\cassandra.yaml"

    $listenAddress = ""
    $rpcAddress = ""
    foreach ($line in $yaml)
    {
        if ($line -match "^listen_address:")
        {
            $args = $line -Split ":"
            $listenAddress = $args[1] -replace " ", ""
        }
        if ($line -match "^rpc_address:")
        {
            $args = $line -Split ":"
            $rpcAddress = $args[1] -replace " ", ""
        }
    }
    if ([string]::IsNullOrEmpty($listenAddress))
    {
        Write-Error "Failed to parse listen_address from cassandra.yaml to check open ports.  Aborting startup."
        Exit
    }
    if ([string]::IsNullOrEmpty($rpcAddress))
    {
        Write-Error "Failed to parse rpc_address from cassandra.yaml to check open ports.  Aborting startup."
        Exit
    }

    foreach ($line in $yaml)
    {
        foreach ($match in $toMatch)
        {
            if ($line -match "^$match")
            {
                if ($line.contains("rpc"))
                {
                    CheckPort $rpcAddress $line
                }
                else
                {
                    CheckPort $listenAddress $line
                }
            }
        }
    }
    if ([string]::IsNullOrEmpty($env:JMX_PORT))
    {
        Write-Error "No JMX_PORT is set in environment.  Aborting startup."
        Exit
    }
    CheckPort $listenAddress "jmx_port: $env:JMX_PORT"
}

#-----------------------------------------------------------------------------
Function CheckPort([string]$listenAddress, [string]$configLine)
{
    $split = $configLine -Split ":"
    if ($split.Length -ne 2)
    {
        echo "Invalid cassandra.yaml config line parsed while checking for available ports:"
        echo "$configLine"
        echo "Aborting startup"
        Exit
    }
    else
    {
        $port = $split[1] -replace " ", ""

        # start an async connect to the ip/port combo, give it 25ms, and error out if it succeeded
        $tcpobject = new-Object system.Net.Sockets.TcpClient
        $connect = $tcpobject.BeginConnect($listenAddress, $port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne(25, $false)

        if (!$wait)
        {
            # still trying to connect, if it's not serviced in 25ms we'll assume it's not open
            $tcpobject.Close()
        }
        else
        {
            $tcpobject.EndConnect($connect) | out-Null
            echo "Cassandra port already in use ($configLine).  Aborting"
            Exit
        }
    }
}

#-----------------------------------------------------------------------------
Function ValidateArguments
{
    if ($install -and $uninstall)
    {
        echo "Cannot install and uninstall"
        exit
    }
    if ($help)
    {
        PrintUsage
    }
}

Function CheckEmptyParam($param)
{
    if ([String]::IsNullOrEmpty($param))
    {
        echo "Invalid parameter: empty value"
        PrintUsage
    }
}

for ($i = 0; $i -lt $args.count; $i++)
{
    # Skip JVM args
    if ($args[$i].StartsWith("-D") -Or $args[$i].StartsWith("-X"))
    {
        continue;
    }
    Switch($args[$i])
    {
        "-install"          { $install = $True }
        "-uninstall"        { $uninstall = $True }
        "-help"             { PrintUsage }
        "-v"                { $v = $True }
        "-f"                { $f = $True }
        "-s"                { $s = $True }
        "-p"                { $p = $args[++$i]; CheckEmptyParam($p) }
        "-H"                { $H = $args[++$i]; CheckEmptyParam($H) }
        "-E"                { $E = $args[++$i]; CheckEmptyParam($E) }
        default
        {
            "Invalid argument: " + $args[$i];
            if (-Not $args[$i].startsWith("-"))
            {
                echo "Note: All options require -"
            }
            exit
        }
    }
}
$pidfile = "pid.txt"

Main
