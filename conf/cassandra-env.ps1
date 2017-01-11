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

    if ((($env:MAX_HEAP_SIZE -and !$env:HEAP_NEWSIZE) -or (!$env:MAX_HEAP_SIZE -and $env:HEAP_NEWSIZE)) -and ($using_cms -eq $true))
    {
        echo "Please set or unset MAX_HEAP_SIZE and HEAP_NEWSIZE in pairs.  Aborting startup."
        exit 1
    }

    $memObject = Get-WMIObject -class win32_physicalmemory
    if ($memObject -eq $null)
    {
        echo "WARNING!  Could not determine system memory.  Defaulting to 2G heap, 512M newgen.  Manually override in conf\jvm.options for different heap values."
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
    $pinfo.Arguments = "-d64 -version"
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $pinfo
    $p.Start() | Out-Null
    $p.WaitForExit()
    $stderr = $p.StandardError.ReadToEnd()

    $env:JVM_ARCH = "64-bit"

    if ($stderr.Contains("Error"))
    {
        # 32-bit JVM. re-run w/out -d64
        echo "Failed 64-bit check. Re-running to get version from 32-bit"
        $pinfo.Arguments = "-version"
        $p = New-Object System.Diagnostics.Process
        $p.StartInfo = $pinfo
        $p.Start() | Out-Null
        $p.WaitForExit()
        $stderr = $p.StandardError.ReadToEnd()
        $env:JVM_ARCH = "32-bit"
    }

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
    $subVersion = $pa[1]
    # Deal with -b (build) versions
    if ($subVersion -contains '-')
    {
        $patchAndBuild = $subVersion.Split("-")
        $subVersion = $patchAndBuild[0]
    }
    $env:JVM_PATCH_VERSION = $subVersion
}

#-----------------------------------------------------------------------------
Function SetCassandraEnvironment
{
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
    # to the Java heap. HEAP_NEWSIZE refers to the size of the young
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

    #GC log path has to be defined here since it needs to find CASSANDRA_HOME
    $env:JVM_OPTS="$env:JVM_OPTS -Xloggc:""$env:CASSANDRA_HOME/logs/gc.log"""

    # Read user-defined JVM options from jvm.options file
    $content = Get-Content "$env:CASSANDRA_CONF\jvm.options"
    for ($i = 0; $i -lt $content.Count; $i++)
    {
        $line = $content[$i]
        if ($line.StartsWith("-"))
        {
            $env:JVM_OPTS = "$env:JVM_OPTS $line"
        }
    }

    $defined_xmn = $env:JVM_OPTS -like '*Xmn*'
    $defined_xmx = $env:JVM_OPTS -like '*Xmx*'
    $defined_xms = $env:JVM_OPTS -like '*Xms*'
    $using_cms = $env:JVM_OPTS -like '*UseConcMarkSweepGC*'

    #$env:MAX_HEAP_SIZE="4096M"
    #$env:HEAP_NEWSIZE="800M"
    CalculateHeapSizes

    ParseJVMInfo

    # We only set -Xms and -Xmx if they were not defined on jvm.options file
    # If defined, both Xmx and Xms should be defined together.
    if (($defined_xmx -eq $false) -and ($defined_xms -eq $false))
    {
        $env:JVM_OPTS="$env:JVM_OPTS -Xms$env:MAX_HEAP_SIZE"
        $env:JVM_OPTS="$env:JVM_OPTS -Xmx$env:MAX_HEAP_SIZE"
    }
    elseif (($defined_xmx -eq $false) -or ($defined_xms -eq $false))
    {
        echo "Please set or unset -Xmx and -Xms flags in pairs on jvm.options file."
        exit
    }

    # We only set -Xmn flag if it was not defined in jvm.options file
    # and if the CMS GC is being used
    # If defined, both Xmn and Xmx should be defined together.
    if (($defined_xmn -eq $true) -and ($defined_xmx -eq $false))
    {
        echo "Please set or unset -Xmx and -Xmn flags in pairs on jvm.options file."
        exit
    }
    elseif (($defined_xmn -eq $false) -and ($using_cms -eq $true))
    {
        $env:JVM_OPTS="$env:JVM_OPTS -Xmn$env:HEAP_NEWSIZE"
    }

    if (($env:JVM_ARCH -eq "64-Bit") -and ($using_cms -eq $true))
    {
        $env:JVM_OPTS="$env:JVM_OPTS -XX:+UseCondCardMark"
    }

    # Add sigar env - see Cassandra-7838
    $env:JVM_OPTS = "$env:JVM_OPTS -Djava.library.path=""$env:CASSANDRA_HOME\lib\sigar-bin"""

    # Confirm we're on high performance power plan, warn if not
    # Change to $true to suppress this warning
    $suppressPowerWarning = $false
    if (!$suppressPowerWarning)
    {
        $currentProfile = powercfg /GETACTIVESCHEME
        if (!$currentProfile.Contains("High performance"))
        {
            echo "*---------------------------------------------------------------------*"
            echo "*---------------------------------------------------------------------*"
            echo ""
            echo "    WARNING! Detected a power profile other than High Performance."
            echo "    Performance of this node will suffer."
            echo "    Modify conf\cassandra.env.ps1 to suppress this warning."
            echo ""
            echo "*---------------------------------------------------------------------*"
            echo "*---------------------------------------------------------------------*"
        }
    }

    # provides hints to the JIT compiler
    $env:JVM_OPTS = "$env:JVM_OPTS -XX:CompileCommandFile=$env:CASSANDRA_CONF\hotspot_compiler"

    # add the jamm javaagent
    if (($env:JVM_VENDOR -ne "OpenJDK") -or ($env:JVM_VERSION.CompareTo("1.6.0") -eq 1) -or
        (($env:JVM_VERSION -eq "1.6.0") -and ($env:JVM_PATCH_VERSION.CompareTo("22") -eq 1)))
    {
        $env:JVM_OPTS = "$env:JVM_OPTS -javaagent:""$env:CASSANDRA_HOME\lib\jamm-0.3.0.jar"""
    }

    # set jvm HeapDumpPath with CASSANDRA_HEAPDUMP_DIR
    if ($env:CASSANDRA_HEAPDUMP_DIR)
    {
        $unixTimestamp = [int64](([datetime]::UtcNow)-(get-date "1/1/1970")).TotalSeconds
        $env:JVM_OPTS="$env:JVM_OPTS -XX:HeapDumpPath=$env:CASSANDRA_HEAPDUMP_DIR\cassandra-$unixTimestamp-pid$pid.hprof"
    }

    if ($env:JVM_VERSION.CompareTo("1.8.0") -eq -1 -or [convert]::ToInt32($env:JVM_PATCH_VERSION) -lt 40)
    {
        echo "Cassandra 3.0 and later require Java 8u40 or later."
        exit
    }

    # Specifies the default port over which Cassandra will be available for
    # JMX connections.
    $JMX_PORT="7199"

    # store in env to check if it's avail in verification
    $env:JMX_PORT=$JMX_PORT

    # Configure the following for JEMallocAllocator and if jemalloc is not available in the system
    # library path.
    # set LD_LIBRARY_PATH=<JEMALLOC_HOME>/lib/
    # $env:JVM_OPTS="$env:JVM_OPTS -Djava.library.path=<JEMALLOC_HOME>/lib/"

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
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
    #
    # JMX SSL options
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.protocols=<enabled-protocols>"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=<enabled-cipher-suites>"
    #$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.keyStore=C:/keystore"
    #$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>"
    #$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.trustStore=C:/truststore"
    #$env:JVM_OPTS="$env:JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>"
    #
    # JMX auth options
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
    ## Basic file based authn & authz
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.password.file=C:/jmxremote.password"
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcom.sun.management.jmxremote.access.file=C:/jmxremote.access"

    ## Custom auth settings which can be used as alternatives to JMX's out of the box auth utilities.
    ## JAAS login modules can be used for authentication by uncommenting these two properties.
    ## Cassandra ships with a LoginModule implementation - org.apache.cassandra.auth.CassandraLoginModule -
    ## which delegates to the IAuthenticator configured in cassandra.yaml
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"
    #$env:JVM_OPTS="$env:JVM_OPTS -Djava.security.auth.login.config=C:/cassandra-jaas.config"

    ## Cassandra also ships with a helper for delegating JMX authz calls to the configured IAuthorizer,
    ## uncomment this to use it. Requires one of the two authentication options to be enabled
    #$env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"

    # Default JMX setup, bound to local loopback address only
    $env:JVM_OPTS="$env:JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT"

    $env:JVM_OPTS="$env:JVM_OPTS $env:JVM_EXTRA_OPTS"
}
