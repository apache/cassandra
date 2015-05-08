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

# NOTE: All param tuning can be done in the SetCassandraEnvironment Function below

#-----------------------------------------------------------------------------
Function SetCassandraHome()
{
    if (! $env:CASSANDRA_HOME)
    {
        $cwd = [System.IO.Directory]::GetCurrentDirectory()
        $cwd = Split-Path $cwd -parent
        $env:CASSANDRA_HOME = $cwd -replace "\\", "/"
    }
}

#-----------------------------------------------------------------------------
Function SetCassandraMain()
{
    if (! $env:CASSANDRA_MAIN)
    {
        $env:CASSANDRA_MAIN="org.apache.cassandra.service.CassandraDaemon"
    }
}

#-----------------------------------------------------------------------------
Function BuildClassPath
{
    $cp = """$env:CASSANDRA_HOME\conf"""
    foreach ($file in Get-ChildItem "$env:CASSANDRA_HOME\lib\*.jar")
    {
        $file = $file -replace "\\", "/"
        $cp = $cp + ";" + """$file"""
    }

    # Add build/classes/main so it works in development
    $cp = $cp + ";" + """$env:CASSANDRA_HOME\build\classes\main"";""$env:CASSANDRA_HOME\build\classes\thrift"""
    $env:CLASSPATH=$cp
}

#-----------------------------------------------------------------------------
Function CalculateHeapSizes
{
    # Check if swapping is enabled on the host and warn if so - reference CASSANDRA-7316

    $osInfo = Get-WmiObject -class "Win32_computersystem"
    $autoPage = $osInfo.AutomaticManagedPageFile

    if ($autoPage)
    {
        echo "*---------------------------------------------------------------------*"
        echo "*---------------------------------------------------------------------*"
        echo ""
        echo "    WARNING!  Automatic page file configuration detected."
        echo "    It is recommended that you disable swap when running Cassandra"
        echo "    for performance and stability reasons."
        echo ""
        echo "*---------------------------------------------------------------------*"
        echo "*---------------------------------------------------------------------*"
    }
    else
    {
        $pageFileInfo = Get-WmiObject -class "Win32_PageFileSetting" -EnableAllPrivileges
        $pageFileCount = $PageFileInfo.Count
        if ($pageFileInfo)
        {
            $files = @()
            $sizes = @()
            $hasSizes = $FALSE

            # PageFileCount isn't populated and obj comes back as single if there's only 1
            if ([string]::IsNullOrEmpty($PageFileCount))
            {
                $PageFileCount = 1
                $files += $PageFileInfo.Name
                if ($PageFileInfo.MaximumSize -ne 0)
                {
                    $hasSizes = $TRUE
                    $sizes += $PageFileInfo.MaximumSize
                }
            }
            else
            {
                for ($i = 0; $i -le $PageFileCount; $i++)
                {
                    $files += $PageFileInfo[$i].Name
                    if ($PageFileInfo[$i].MaximumSize -ne 0)
                    {
                        $hasSizes = $TRUE
                        $sizes += $PageFileInfo[$i].MaximumSize
                    }
                }
            }

            echo "*---------------------------------------------------------------------*"
            echo "*---------------------------------------------------------------------*"
            echo ""
            echo "    WARNING!  $PageFileCount swap file(s) detected"
            for ($i = 0; $i -lt $PageFileCount; $i++)
            {
                $toPrint = "        Name: " + $files[$i]
                if ($hasSizes)
                {
                    $toPrint = $toPrint + " Size: " + $sizes[$i]
                    $toPrint = $toPrint -replace [Environment]::NewLine, ""
                }
                echo $toPrint
            }
            echo "    It is recommended that you disable swap when running Cassandra"
            echo "    for performance and stability reasons."
            echo ""
            echo "*---------------------------------------------------------------------*"
            echo "*---------------------------------------------------------------------*"
        }
    }

    # Validate that we need to run this function and that our config is good
    if ($env:MAX_HEAP_SIZE -and $env:HEAP_NEWSIZE)
    {
        return
    }
    if (($env:MAX_HEAP_SIZE -and !$env:HEAP_NEWSIZE) -or (!$env:MAX_HEAP_SIZE -and $env:HEAP_NEWSIZE))
    {
        echo "Please set or unset MAX_HEAP_SIZE and HEAP_NEWSIZE in pairs.  Aborting startup."
        exit 1
    }

    $memObject = Get-WMIObject -class win32_physicalmemory
    if ($memObject -eq $null)
    {
        echo "WARNING!  Could not determine system memory.  Defaulting to 2G heap, 512M newgen.  Manually override in conf\cassandra-env.ps1 for different heap values."
        $env:MAX_HEAP_SIZE = "2048M"
        $env:HEAP_NEWSIZE = "512M"
        return
    }

    $memory = ($memObject | Measure-Object Capacity -Sum).sum
    $memoryMB = [Math]::Truncate($memory / (1024*1024))

    $cpu = gwmi Win32_ComputerSystem | Select-Object NumberOfLogicalProcessors
    $systemCores = $cpu.NumberOfLogicalProcessors

    # set max heap size based on the following
    # max(min(1/2 ram, 1024MB), min(1/4 ram, 8GB))
    # calculate 1/2 ram and cap to 1024MB
    # calculate 1/4 ram and cap to 8192MB
    # pick the max
    $halfMem = [Math]::Truncate($memoryMB / 2)
    $quarterMem = [Math]::Truncate($halfMem / 2)

    if ($halfMem -gt 1024)
    {
        $halfMem = 1024
    }
    if ($quarterMem -gt 8192)
    {
        $quarterMem = 8192
    }

    $maxHeapMB = ""
    if ($halfMem -gt $quarterMem)
    {
        $maxHeapMB = $halfMem
    }
    else
    {
        $maxHeapMB = $quarterMem
    }
    $env:MAX_HEAP_SIZE = [System.Convert]::ToString($maxHeapMB) + "M"

    # Young gen: min(max_sensible_per_modern_cpu_core * num_cores, 1/4
    $maxYGPerCore = 100
    $maxYGTotal = $maxYGPerCore * $systemCores
    $desiredYG = [Math]::Truncate($maxHeapMB / 4)

    if ($desiredYG -gt $maxYGTotal)
    {
        $env:HEAP_NEWSIZE = [System.Convert]::ToString($maxYGTotal) + "M"
    }
    else
    {
        $env:HEAP_NEWSIZE = [System.Convert]::ToString($desiredYG) + "M"
    }
}

#-----------------------------------------------------------------------------
Function ParseJVMInfo
{
    # grab info about the JVM
    $pinfo = New-Object System.Diagnostics.ProcessStartInfo
    $pinfo.FileName = "$env:JAVA_BIN"
    $pinfo.RedirectStandardError = $true
    $pinfo.RedirectStandardOutput = $true
    $pinfo.UseShellExecute = $false
    $pinfo.Arguments = "-version"
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()
    $stderr = $p.StandardError.ReadToEnd()

    $sa = $stderr.Split("""")
    $env:JVM_VERSION = $sa[1]

    if ($stderr.Contains("OpenJDK"))
    {
        $env:JVM_VENDOR = "OpenJDK"
    }
    elseif ($stderr.Contains("Java(TM)"))
    {
        $env:JVM_VENDOR = "Oracle"
    }
    else
    {
        $JVM_VENDOR = "other"
    }

    $pa = $sa[1].Split("_")
    $env:JVM_PATCH_VERSION=$pa[1]

    # get 64-bit vs. 32-bit
    $pinfo.Arguments = "-d64 -version"
    $pArch = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()
    $stderr = $p.StandardError.ReadToEnd()

    if ($stderr.Contains("Error"))
    {
        $env:JVM_ARCH = "32-bit"
    }
    else
    {
        $env:JVM_ARCH = "64-bit"
    }
}

#-----------------------------------------------------------------------------
Function SetCassandraEnvironment
{
    echo "Setting up Cassandra environment"
    if (Test-Path Env:\JAVA_HOME)
    {
        $env:JAVA_BIN = "$env:JAVA_HOME\bin\java.exe"
    }
    elseif (Get-Command "java.exe")
    {
        $env:JAVA_BIN = "java.exe"
    }
    else
    {
        echo "ERROR!  No JAVA_HOME set and could not find java.exe in the path."
        exit
    }
    SetCassandraHome
    $env:CASSANDRA_CONF = "$env:CASSANDRA_HOME\conf"
    $env:CASSANDRA_PARAMS="-Dcassandra -Dlogback.configurationFile=logback.xml"

    $logdir = "$env:CASSANDRA_HOME\logs"
    $storagedir = "$env:CASSANDRA_HOME\data"
    $env:CASSANDRA_PARAMS = $env:CASSANDRA_PARAMS + " -Dcassandra.logdir=""$logdir"" -Dcassandra.storagedir=""$storagedir"""

    SetCassandraMain
    BuildClassPath

    # Override these to set the amount of memory to allocate to the JVM at
    # start-up. For production use you may wish to adjust this for your
    # environment. MAX_HEAP_SIZE is the total amount of memory dedicated
    # to the Java heap; HEAP_NEWSIZE refers to the size of the young
    # generation. Both MAX_HEAP_SIZE and HEAP_NEWSIZE should be either set
    # or not (if you set one, set the other).
    #
    # The main trade-off for the young generation is that the larger it
    # is, the longer GC pause times will be. The shorter it is, the more
    # expensive GC will be (usually).
    #
    # The example HEAP_NEWSIZE assumes a modern 8-core+ machine for decent
    # times. If in doubt, and if you do not particularly want to tweak, go
    # 100 MB per physical CPU core.

    #$env:MAX_HEAP_SIZE="4096M"
    #$env:HEAP_NEWSIZE="800M"
    CalculateHeapSizes

    ParseJVMInfo
    # add the jamm javaagent
    if (($env:JVM_VENDOR -ne "OpenJDK") -or ($env:JVM_VERSION.CompareTo("1.6.0") -eq 1) -or
        (($env:JVM_VERSION -eq "1.6.0") -and ($env:JVM_PATCH_VERSION.CompareTo("22") -eq 1)))
    {
        $env:JVM_OPTS = "$env:JVM_OPTS -javaagent:""$env:CASSANDRA_HOME\lib\jamm-0.3.0.jar"""
    }

    # enable assertions.  disabling this in production will give a modest
    # performance benefit (around 5%).
    $env:JVM_OPTS = "$env:JVM_OPTS -ea"

    # Specifies the default port over which Cassandra will be available for
    # JMX connections.
    $JMX_PORT="7199"

    # store in env to check if it's avail in verification
    $env:JMX_PORT=$JMX_PORT

    $env:JVM_OPTS = "$env:JVM_OPTS -Dlog4j.defaultInitOverride=true"

    # some JVMs will fill up their heap when accessed via JMX, see CASSANDRA-6541
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+CMSClassUnloadingEnabled"

    # enable thread priorities, primarily so we can give periodic tasks
    # a lower priority to avoid interfering with client workload
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseThreadPriorities"
    # allows lowering thread priority without being root on linux - probably
    # not necessary on Windows but doesn't harm anything.
    # see http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workar
    $env:JVM_OPTS="$env:JVM_OPTS -XX:ThreadPriorityPolicy=42"

    # min and max heap sizes should be set to the same value to avoid
    # stop-the-world GC pauses during resize.
    $env:JVM_OPTS="$env:JVM_OPTS -Xms$env:MAX_HEAP_SIZE"
    $env:JVM_OPTS="$env:JVM_OPTS -Xmx$env:MAX_HEAP_SIZE"
    $env:JVM_OPTS="$env:JVM_OPTS -Xmn$env:HEAP_NEWSIZE"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError"

    # Per-thread stack size.
    $env:JVM_OPTS="$env:JVM_OPTS -Xss256k"

    # Larger interned string table, for gossip's benefit (CASSANDRA-6410)
    $env:JVM_OPTS="$env:JVM_OPTS -XX:StringTableSize=1000003"

    # GC tuning options
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseParNewGC"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseConcMarkSweepGC"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+CMSParallelRemarkEnabled"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:SurvivorRatio=8"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:MaxTenuringThreshold=1"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:CMSInitiatingOccupancyFraction=75"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseCMSInitiatingOccupancyOnly"
    $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseTLAB"
    if (($env:JVM_VERSION.CompareTo("1.7") -eq 1) -and ($env:JVM_ARCH -eq "64-Bit"))
    {
        $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseCondCardMark"
    }
    if ( (($env:JVM_VERSION.CompareTo("1.7") -ge 0) -and ($env:JVM_PATCH_VERSION.CompareTo("60") -ge 0)) -or
         ($env:JVM_VERSION.CompareTo("1.8") -ge 0))
    {
        $env:JVM_OPTS="$env:JVM_OPTS -XX:+CMSParallelInitialMarkEnabled -XX:+CMSEdenChunksRecordAlways"
    }

    # GC logging options -- uncomment to enable
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintGCDetails"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintGCDateStamps"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintHeapAtGC"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintTenuringDistribution"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintGCApplicationStoppedTime"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+PrintPromotionFailure"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:PrintFLSStatistics=1"
    # $currentDate = (Get-Date).ToString('yyyy.MM.dd')
    # $env:JVM_OPTS="$env:JVM_OPTS -Xloggc:$env:CASSANDRA_HOME/logs/gc-$currentDate.log"

    # If you are using JDK 6u34 7u2 or later you can enable GC log rotation
    # don't stick the date in the log name if rotation is on.
    # $env:JVM_OPTS="$env:JVM_OPTS -Xloggc:$env:CASSANDRA_HOME/logs/gc.log"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseGCLogFileRotation"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:NumberOfGCLogFiles=10"
    # $env:JVM_OPTS="$env:JVM_OPTS -XX:GCLogFileSize=10M"

    # Configure the following for JEMallocAllocator and if jemalloc is not available in the system
    # library path.
    # set LD_LIBRARY_PATH=<JEMALLOC_HOME>/lib/
    # $env:JVM_OPTS="$env:JVM_OPTS -Djava.library.path=<JEMALLOC_HOME>/lib/"

    # uncomment to have Cassandra JVM listen for remote debuggers/profilers on port 1414
    # $env:JVM_OPTS="$env:JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1414"

    # Prefer binding to IPv4 network intefaces (when net.ipv6.bindv6only=1). See
    # http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6342561 (short version:
    # comment out this entry to enable IPv6 support).
    $env:JVM_OPTS="$env:JVM_OPTS -Djava.net.preferIPv4Stack=true"

    # jmx: metrics and administration interface
    #
    # add this if you're having trouble connecting:
    # $env:JVM_OPTS="$env:JVM_OPTS -Djava.rmi.server.hostname=<public name>"
    #
    # see
    # https://blogs.oracle.com/jmxetc/entry/troubleshooting_connection_problems_in_jconsole
    # for more on configuring JMX through firewalls, etc. (Short version:
    # get it working with no firewall first.)
    #
    # Due to potential security exploits, Cassandra ships with JMX accessible
    # *only* from localhost.  To enable remote JMX connections, uncomment lines below
    # with authentication and ssl enabled. See https://wiki.apache.org/cassandra/JmxSecurity 
    #
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.password.file=C:/jmxremote.password"
    $env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT -XX:+DisableExplicitGC"

    $env:JVM_OPTS="$env:JVM_OPTS $env:JVM_EXTRA_OPTS"

    $env:JVM_OPTS = "$env:JVM_OPTS -Dlog4j.configuration=log4j-server.properties"
}
