# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

calculate_heap_sizes()
{
    case "`uname`" in
        Linux)
            system_memory_in_mb=`free -m | awk '/:/ {print $2;exit}'`
            system_cpu_cores=`egrep -c 'processor([[:space:]]+):.*' /proc/cpuinfo`
        ;;
        FreeBSD)
            system_memory_in_bytes=`sysctl hw.physmem | awk '{print $2}'`
            system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
            system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
        ;;
        SunOS)
            system_memory_in_mb=`prtconf | awk '/Memory size:/ {print $3}'`
            system_cpu_cores=`psrinfo | wc -l`
        ;;
        Darwin)
            system_memory_in_bytes=`sysctl hw.memsize | awk '{print $2}'`
            system_memory_in_mb=`expr $system_memory_in_bytes / 1024 / 1024`
            system_cpu_cores=`sysctl hw.ncpu | awk '{print $2}'`
        ;;
        *)
            # assume reasonable defaults for e.g. a modern desktop or
            # cheap server
            system_memory_in_mb="2048"
            system_cpu_cores="2"
        ;;
    esac

    # some systems like the raspberry pi don't report cores, use at least 1
    if [ "$system_cpu_cores" -lt "1" ]
    then
        system_cpu_cores="1"
    fi

    # set max heap size based on the following
    # max(min(1/2 ram, 1024MB), min(1/4 ram, 8GB))
    # calculate 1/2 ram and cap to 1024MB
    # calculate 1/4 ram and cap to 8192MB
    # pick the max
    half_system_memory_in_mb=`expr $system_memory_in_mb / 2`
    quarter_system_memory_in_mb=`expr $half_system_memory_in_mb / 2`
    if [ "$half_system_memory_in_mb" -gt "1024" ]
    then
        half_system_memory_in_mb="1024"
    fi
    if [ "$quarter_system_memory_in_mb" -gt "8192" ]
    then
        quarter_system_memory_in_mb="8192"
    fi
    if [ "$half_system_memory_in_mb" -gt "$quarter_system_memory_in_mb" ]
    then
        max_heap_size_in_mb="$half_system_memory_in_mb"
    else
        max_heap_size_in_mb="$quarter_system_memory_in_mb"
    fi
    MAX_HEAP_SIZE="${max_heap_size_in_mb}M"
}

# Determine the sort of JVM we'll be running on.
java_ver_output=`"${JAVA:-java}" -version 2>&1`
jvmver=`echo "$java_ver_output" | grep '[openjdk|java] version' | awk -F'"' 'NR==1 {print $2}'`
JVM_VERSION=${jvmver%_*}
JVM_PATCH_VERSION=${jvmver#*_}

if [ "$JVM_VERSION" \< "1.8" ] ; then
    echo "Cassandra 3.0 and later require Java 8u40 or later."
    exit 1;
fi

if [ "$JVM_VERSION" \< "1.8" ] && [ "$JVM_PATCH_VERSION" \< "40" ] ; then
    echo "Cassandra 3.0 and later require Java 8u40 or later."
    exit 1;
fi

# Override these to set the amount of memory to allocate to the JVM at
# start-up. For production use you may wish to adjust this for your
# environment. MAX_HEAP_SIZE is the total amount of memory dedicated
# to the Java heap.

#MAX_HEAP_SIZE="4G"

# Set this to control the amount of arenas per-thread in glibc
#export MALLOC_ARENA_MAX=4

# only calculate the size if it's not set manually
if [ "x$MAX_HEAP_SIZE" = "x" ] ; then
    calculate_heap_sizes
fi

if [ "x$MALLOC_ARENA_MAX" = "x" ] ; then
    export MALLOC_ARENA_MAX=4
fi

# Cassandra uses an installed jemalloc via LD_PRELOAD / DYLD_INSERT_LIBRARIES by default to improve off-heap
# memory allocation performance. The following code searches for an installed libjemalloc.dylib/.so/.1.so using
# Linux and OS-X specific approaches.
# To specify your own libjemalloc in a different path, configure the fully qualified path in CASSANDRA_LIBJEMALLOC.
# To disable jemalloc at all set CASSANDRA_LIBJEMALLOC=-
#
#CASSANDRA_LIBJEMALLOC=
#
find_library()
{
    pattern=$1
    path=$(echo ${2} | tr ":" " ")

    find $path -regex "$pattern" -print 2>/dev/null | head -n 1
}
case "`uname -s`" in
    Linux)
        if [ -z $CASSANDRA_LIBJEMALLOC ] ; then
            which ldconfig > /dev/null 2>&1
            if [ $? = 0 ] ; then
                # e.g. for CentOS
                dirs="/lib64 /lib /usr/lib64 /usr/lib `ldconfig -v 2>/dev/null | grep -v ^$'\t' | sed 's/^\([^:]*\):.*$/\1/'`"
            else
                # e.g. for Debian, OpenSUSE
                dirs="/lib64 /lib /usr/lib64 /usr/lib `cat /etc/ld.so.conf /etc/ld.so.conf.d/*.conf | grep '^/'`"
            fi
            dirs=`echo $dirs | tr " " ":"`
            CASSANDRA_LIBJEMALLOC=$(find_library '.*/libjemalloc\.so\(\.1\)*' $dirs)
        fi
        if [ ! -z $CASSANDRA_LIBJEMALLOC ] ; then
            if [ "-" != "$CASSANDRA_LIBJEMALLOC" ] ; then
                export LD_PRELOAD=$CASSANDRA_LIBJEMALLOC
            fi
        fi
    ;;
    Darwin)
        if [ -z $CASSANDRA_LIBJEMALLOC ] ; then
            CASSANDRA_LIBJEMALLOC=$(find_library '.*/libjemalloc\.dylib' $DYLD_LIBRARY_PATH:${DYLD_FALLBACK_LIBRARY_PATH-$HOME/lib:/usr/local/lib:/lib:/usr/lib})
        fi
        if [ ! -z $CASSANDRA_LIBJEMALLOC ] ; then
            if [ "-" != "$CASSANDRA_LIBJEMALLOC" ] ; then
                export DYLD_INSERT_LIBRARIES=$CASSANDRA_LIBJEMALLOC
            fi
        fi
    ;;
esac

# Here we create the arguments that will get passed to the jvm when
# starting cassandra.

# enable assertions.  disabling this in production will give a modest
# performance benefit (around 5%).
JVM_OPTS="$JVM_OPTS -ea"

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JVM_OPTS="$JVM_OPTS -Xms${MAX_HEAP_SIZE}"
JVM_OPTS="$JVM_OPTS -Xmx${MAX_HEAP_SIZE}"

# Per-thread stack size.
JVM_OPTS="$JVM_OPTS -Xss256k"

# Use the Hotspot garbage-first collector.
JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"

# Have the JVM do less remembered set work during STW, instead
# preferring concurrent GC. Reduces p99.9 latency.
JVM_OPTS="$JVM_OPTS -XX:G1RSetUpdatingPauseTimePercent=5"

# The JVM maximum is 8 PGC threads and 1/4 of that for ConcGC.
# Machines with > 10 cores may need additional threads. Increase to <= full cores.
#JVM_OPTS="$JVM_OPTS -XX:ParallelGCThreads=16"
#JVM_OPTS="$JVM_OPTS -XX:ConcGCThreads=16"

# Main G1GC tunable: lowering the pause target will lower throughput and vise versa.
# 200ms is the JVM default and lowest viable setting
# 1000ms increases throughput. Keep it smaller than the timeouts in cassandra.yaml.
JVM_OPTS="$JVM_OPTS -XX:MaxGCPauseMillis=500"

# Save CPU time on large (>= 16GB) heaps by delaying region scanning
# until the heap is 70% full. The default in Hotspot 8u40 is 40%.
#JVM_OPTS="$JVM_OPTS -XX:InitiatingHeapOccupancyPercent=70"

# Make sure all memory is faulted and zeroed on startup.
# This helps prevent soft faults in containers and makes
# transparent hugepage allocation more effective.
JVM_OPTS="$JVM_OPTS -XX:+AlwaysPreTouch"

# Biased locking does not benefit Cassandra.
JVM_OPTS="$JVM_OPTS -XX:-UseBiasedLocking"

# Larger interned string table, for gossip's benefit (CASSANDRA-6410)
JVM_OPTS="$JVM_OPTS -XX:StringTableSize=1000003"

# Enable thread-local allocation blocks and allow the JVM to automatically
# resize them at runtime.
JVM_OPTS="$JVM_OPTS -XX:+UseTLAB -XX:+ResizeTLAB"

# http://www.evanjones.ca/jvm-mmap-pause.html
JVM_OPTS="$JVM_OPTS -XX:+PerfDisableSharedMem"

# provides hints to the JIT compiler
JVM_OPTS="$JVM_OPTS -XX:CompileCommandFile=$CASSANDRA_CONF/hotspot_compiler"

# add the jamm javaagent
JVM_OPTS="$JVM_OPTS -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.0.jar"

# enable thread priorities, primarily so we can give periodic tasks
# a lower priority to avoid interfering with client workload
JVM_OPTS="$JVM_OPTS -XX:+UseThreadPriorities"
# allows lowering thread priority without being root.  see
# http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workaround.html
JVM_OPTS="$JVM_OPTS -XX:ThreadPriorityPolicy=42"

# set jvm HeapDumpPath with CASSANDRA_HEAPDUMP_DIR
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError"
if [ "x$CASSANDRA_HEAPDUMP_DIR" != "x" ]; then
    JVM_OPTS="$JVM_OPTS -XX:HeapDumpPath=$CASSANDRA_HEAPDUMP_DIR/cassandra-`date +%s`-pid$$.hprof"
fi

# GC logging options -- uncomment to enable
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails"
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCDateStamps"
# JVM_OPTS="$JVM_OPTS -XX:+PrintHeapAtGC"
# JVM_OPTS="$JVM_OPTS -XX:+PrintTenuringDistribution"
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCApplicationStoppedTime"
# JVM_OPTS="$JVM_OPTS -XX:+PrintPromotionFailure"
# JVM_OPTS="$JVM_OPTS -XX:PrintFLSStatistics=1"
# JVM_OPTS="$JVM_OPTS -Xloggc:/var/log/cassandra/gc.log"
# JVM_OPTS="$JVM_OPTS -XX:+UseGCLogFileRotation"
# JVM_OPTS="$JVM_OPTS -XX:NumberOfGCLogFiles=10"
# JVM_OPTS="$JVM_OPTS -XX:GCLogFileSize=10M"

# Configure the following for JEMallocAllocator and if jemalloc is not available in the system 
# library path (Example: /usr/local/lib/). Usually "make install" will do the right thing. 
# export LD_LIBRARY_PATH=<JEMALLOC_HOME>/lib/
# JVM_OPTS="$JVM_OPTS -Djava.library.path=<JEMALLOC_HOME>/lib/"

# uncomment to have Cassandra JVM listen for remote debuggers/profilers on port 1414
# JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=1414"

# uncomment to have Cassandra JVM log internal method compilation (developers only)
# JVM_OPTS="$JVM_OPTS -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation"
# JVM_OPTS="$JVM_OPTS -XX:+UnlockCommercialFeatures -XX:+FlightRecorder"

# Prefer binding to IPv4 network intefaces (when net.ipv6.bindv6only=1). See
# http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6342561 (short version:
# comment out this entry to enable IPv6 support).
JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=true"

# jmx: metrics and administration interface
#
# add this if you're having trouble connecting:
# JVM_OPTS="$JVM_OPTS -Djava.rmi.server.hostname=<public name>"
#
# see
# https://blogs.oracle.com/jmxetc/entry/troubleshooting_connection_problems_in_jconsole
# for more on configuring JMX through firewalls, etc. (Short version:
# get it working with no firewall first.)
#
# Cassandra ships with JMX accessible *only* from localhost.  
# To enable remote JMX connections, uncomment lines below
# with authentication and/or ssl enabled. See https://wiki.apache.org/cassandra/JmxSecurity 
#
LOCAL_JMX=yes

# Specifies the default port over which Cassandra will be available for
# JMX connections.
# For security reasons, you should not expose this port to the internet.  Firewall it if needed.
JMX_PORT="7199"

if [ "$LOCAL_JMX" = "yes" ]; then
  JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT -XX:+DisableExplicitGC"
else
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT"
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"
#  JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStore=/path/to/keystore"
#  JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>"
#  JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStore=/path/to/truststore"
#  JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>"
#  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"
#  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.registry.ssl=true"
#  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.protocols=<enabled-protocols>"
#  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=<enabled-cipher-suites>"
fi

# To use mx4j, an HTML interface for JMX, add mx4j-tools.jar to the lib/
# directory.
# See http://wiki.apache.org/cassandra/Operations#Monitoring_with_MX4J
# By default mx4j listens on 0.0.0.0:8081. Uncomment the following lines
# to control its listen address and port.
#MX4J_ADDRESS="-Dmx4jaddress=127.0.0.1"
#MX4J_PORT="-Dmx4jport=8081"

# Cassandra uses SIGAR to capture OS metrics CASSANDRA-7838
# for SIGAR we have to set the java.library.path
# to the location of the native libraries.
JVM_OPTS="$JVM_OPTS -Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin"

JVM_OPTS="$JVM_OPTS $MX4J_ADDRESS"
JVM_OPTS="$JVM_OPTS $MX4J_PORT"
JVM_OPTS="$JVM_OPTS $JVM_EXTRA_OPTS"
