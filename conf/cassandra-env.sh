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
    if [ "$system_cpu_cores" -lt "1" ] ; then
        system_cpu_cores="1"
    fi

    # Heap size: min(1/2 ram, CMS ? 16G : 31G)
    # CMS Young gen: 1/2 * heap size
    if [ $USING_CMS -eq 0 ] ; then
        heap_limit="15872"
    else
        heap_limit="31744"
    fi
    half_system_memory_in_mb=`expr $system_memory_in_mb / 2`
    quarter_system_memory_in_mb=`expr $system_memory_in_mb / 4`
    if [ "$half_system_memory_in_mb" -gt "$heap_limit" ] ; then
        CALCULATED_MAX_HEAP_SIZE="${heap_limit}M"
        CALCULATED_MAX_DIRECT_MEMORY_SIZE="`expr $heap_limit / 2`M"
        CALCULATED_CMS_HEAP_NEWSIZE="8G"
    else
        CALCULATED_MAX_HEAP_SIZE="${half_system_memory_in_mb}M"
        CALCULATED_MAX_DIRECT_MEMORY_SIZE="${quarter_system_memory_in_mb}M"
        CALCULATED_CMS_HEAP_NEWSIZE="`expr $half_system_memory_in_mb / 4`M"
    fi
}

# Sets the path where logback and GC logs are written.
if [ "x$CASSANDRA_LOG_DIR" = "x" ] ; then
    CASSANDRA_LOG_DIR="$CASSANDRA_HOME/logs"
fi

#GC log path has to be defined here because it needs to access CASSANDRA_HOME
# See description of https://bugs.openjdk.java.net/browse/JDK-8046148 for details about the syntax
# The following is the equivalent to -XX:+PrintGCDetails -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=10M
echo "$JVM_OPTS" | grep -qe "-[X]log:gc"
if [ "$?" = "1" ] ; then # [X] to prevent ccm from replacing this line
    # only add -Xlog:gc if it's not mentioned in jvm-server.options file
    mkdir -p ${CASSANDRA_LOG_DIR}
    JVM_OPTS="$JVM_OPTS -Xlog:gc=info,heap*=trace,age*=debug,safepoint=info,promotion*=trace:file=${CASSANDRA_LOG_DIR}/gc.log:time,uptime,pid,tid,level:filecount=10,filesize=10485760"
fi

# Check what parameters were defined on jvm-server.options file to avoid conflicts
echo $JVM_OPTS | grep -q Xmn
DEFINED_XMN=$?
echo $JVM_OPTS | grep -q Xmx
DEFINED_XMX=$?
echo $JVM_OPTS | grep -q Xms
DEFINED_XMS=$?
echo $JVM_OPTS | grep -q MaxDirectMemorySize
DEFINED_MAX_DIRECT_MEMORY_SIZE=$?
echo $JVM_OPTS | grep -q ParallelGCThreads
DEFINED_PARALLEL_GC_THREADS=$?
echo $JVM_OPTS | grep -q ConcGCThreads
DEFINED_CONC_GC_THREADS=$?
echo $JVM_OPTS | grep -q UseConcMarkSweepGC
USING_CMS=$?
echo $JVM_OPTS | grep -q +UseG1GC
USING_G1=$?

calculate_heap_sizes

# Override these to set the amount of memory to allocate to the JVM at
# start-up. For production use you may wish to adjust this for your
# environment. MAX_HEAP_SIZE is the total amount of memory dedicated
# to the Java heap. HEAP_NEWSIZE refers to the size of the young
# generation when CMS is used.
#
# When using G1 only MAX_HEAP_SIZE may be set, and HEAP_NEWSIZE must be
# left unset.
#
# When using CMS both MAX_HEAP_SIZE and HEAP_NEWSIZE should be either set
# or not (if you set one, set the other).

#MAX_HEAP_SIZE="20G"
#HEAP_NEWSIZE="10G"
#MAX_DIRECT_MEMORY_SIZE="10G"

# Set this to control the amount of arenas per-thread in glibc
#export MALLOC_ARENA_MAX=4

# Warn on an erroneously set HEAP_NEWSIZE when using G1
if [ "x$HEAP_NEWSIZE" != "x" -a $USING_G1 -eq 0 ]; then
    echo "HEAP_NEWSIZE has erroneously been set and will be ignored in combination with G1 (see cassandra-env.sh)"
fi

# Only use the calculated size if it's not set manually
if [ "x$MAX_HEAP_SIZE" = "x" ] && [ "x$HEAP_NEWSIZE" = "x" -o $USING_G1 -eq 0 ]; then
    MAX_HEAP_SIZE="$CALCULATED_MAX_HEAP_SIZE"
    HEAP_NEWSIZE="$CALCULATED_CMS_HEAP_NEWSIZE"
elif [ "x$MAX_HEAP_SIZE" = "x" ] ||  [ "x$HEAP_NEWSIZE" = "x" -a $USING_G1 -ne 0 ]; then
    echo "please set or unset MAX_HEAP_SIZE and HEAP_NEWSIZE in pairs when using CMS GC (see cassandra-env.sh)"
    exit 1
fi

if [ "x$MAX_DIRECT_MEMORY_SIZE" = "x" ]; then
    MAX_DIRECT_MEMORY_SIZE="$CALCULATED_MAX_DIRECT_MEMORY_SIZE"
fi

if [ "x$MALLOC_ARENA_MAX" = "x" ] ; then
    export MALLOC_ARENA_MAX=4
fi

# We only set -Xms and -Xmx if they were not defined on jvm-server.options file
# If defined, both Xmx and Xms should be defined together.
if [ $DEFINED_XMX -ne 0 ] && [ $DEFINED_XMS -ne 0 ]; then
     JVM_OPTS="$JVM_OPTS -Xms${MAX_HEAP_SIZE}"
     JVM_OPTS="$JVM_OPTS -Xmx${MAX_HEAP_SIZE}"
elif [ $DEFINED_XMX -ne 0 ] || [ $DEFINED_XMS -ne 0 ]; then
     echo "Please set or unset -Xmx and -Xms flags in pairs on jvm-server.options file."
     exit 1
fi

if [ $DEFINED_MAX_DIRECT_MEMORY_SIZE -ne 0 ]; then
     JVM_OPTS="$JVM_OPTS -XX:MaxDirectMemorySize=${MAX_DIRECT_MEMORY_SIZE}"
fi

# We only set -Xmn flag if it was not defined in jvm-server.options file
# and CMS is being used.  If defined, both Xmn and Xmx must be defined together.
if [ $DEFINED_XMN -eq 0 ] && [ $DEFINED_XMX -ne 0 ]; then
    echo "Please set or unset -Xmx and -Xmn flags in pairs on jvm-server.options file."
    exit 1
elif [ $DEFINED_XMN -ne 0 ] && [ $USING_CMS -eq 0 ]; then
    JVM_OPTS="$JVM_OPTS -Xmn${HEAP_NEWSIZE}"
fi

# We fail to start if -Xmn is used with G1
if [ $DEFINED_XMN -eq 0 ] && [ $USING_G1 -eq 0 ]; then
    # It is not recommended to set the young generation size if using the
    # G1 GC, since that will override the target pause-time goal.
    # Instead floor the young generation size with -XX:NewSize
    # More info: http://www.oracle.com/technetwork/articles/java/g1gc-1984535.html
    echo "It is not recommended to set -Xmn with the G1 garbage collector. See comments for -Xmn in jvm-server.options for details."
    exit 1
fi

if [ $USING_G1 -eq 0 ] && [ $DEFINED_PARALLEL_GC_THREADS -ne 0 ] && [ $DEFINED_CONC_GC_THREADS -ne 0 ] ; then
    # Set ParallelGCThreads and ConcGCThreads equal to number of cpu cores.
    # Setting both to the same value is important to reduce STW durations.
    JVM_OPTS="$JVM_OPTS -XX:ParallelGCThreads=$system_cpu_cores -XX:ConcGCThreads=$system_cpu_cores"
fi

if [ "$JVM_ARCH" = "64-Bit" ] && [ $USING_CMS -eq 0 ]; then
    JVM_OPTS="$JVM_OPTS -XX:+UseCondCardMark"
fi

# provides hints to the JIT compiler
JVM_OPTS="$JVM_OPTS -XX:CompileCommandFile=$CASSANDRA_CONF/hotspot_compiler"

# add the jamm javaagent
JVM_OPTS="$JVM_OPTS -javaagent:$CASSANDRA_HOME/lib/jamm-0.4.0.jar"


if [ "x$CASSANDRA_HEAPDUMP_DIR" = "x" ]; then
    CASSANDRA_HEAPDUMP_DIR="$CASSANDRA_LOG_DIR"
fi
JVM_OPTS="$JVM_OPTS -XX:HeapDumpPath=$CASSANDRA_HEAPDUMP_DIR/cassandra-`date +%s`-pid$$.hprof"

# stop the jvm on OutOfMemoryError as it can result in some data corruption
# uncomment the preferred option
# ExitOnOutOfMemoryError and CrashOnOutOfMemoryError require a JRE greater or equals to 1.7 update 101 or 1.8 update 92
# For OnOutOfMemoryError we cannot use the JVM_OPTS variables because bash commands split words
# on white spaces without taking quotes into account
# JVM_OPTS="$JVM_OPTS -XX:+ExitOnOutOfMemoryError"
# JVM_OPTS="$JVM_OPTS -XX:+CrashOnOutOfMemoryError"
JVM_ON_OUT_OF_MEMORY_ERROR_OPT="-XX:OnOutOfMemoryError=kill -9 %p"

# print an heap histogram on OutOfMemoryError
# JVM_OPTS="$JVM_OPTS -Dcassandra.printHeapHistogramOnOutOfMemoryError=true"

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
if [ "x$LOCAL_JMX" = "x" ]; then
    LOCAL_JMX=yes
fi

# Specifies the default port over which Cassandra will be available for
# JMX connections.
# For security reasons, you should not expose this port to the internet.  Firewall it if needed.
JMX_PORT="7199"

if [ "$LOCAL_JMX" = "yes" ]; then
  JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.local.port=$JMX_PORT"
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
else
  JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.remote.port=$JMX_PORT"
  # if ssl is enabled the same port cannot be used for both jmx and rmi so either
  # pick another value for this property or comment out to use a random port (though see CASSANDRA-7087 for origins)
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"

  # turn on JMX authentication. See below for further options
  JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=true"

  # jmx ssl options
  #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=true"
  #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.need.client.auth=true"
  #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.protocols=<enabled-protocols>"
  #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=<enabled-cipher-suites>"
  #JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStore=/path/to/keystore"
  #JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.keyStorePassword=<keystore-password>"
  #JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStore=/path/to/truststore"
  #JVM_OPTS="$JVM_OPTS -Djavax.net.ssl.trustStorePassword=<truststore-password>"
fi

# jmx authentication and authorization options. By default, auth is only
# activated for remote connections but they can also be enabled for local only JMX
## Basic file based authn & authz
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"
#JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file=/etc/cassandra/jmxremote.access"
## Custom auth settings which can be used as alternatives to JMX's out of the box auth utilities.
## JAAS login modules can be used for authentication by uncommenting these two properties.
## Cassandra ships with a LoginModule implementation - org.apache.cassandra.auth.CassandraLoginModule -
## which delegates to the IAuthenticator configured in cassandra.yaml. See the sample JAAS configuration
## file cassandra-jaas.config
#JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"
#JVM_OPTS="$JVM_OPTS -Djava.security.auth.login.config=$CASSANDRA_CONF/cassandra-jaas.config"

## Cassandra also ships with a helper for delegating JMX authz calls to the configured IAuthorizer,
## uncomment this to use it. Requires one of the two authentication options to be enabled
#JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"

# To use mx4j, an HTML interface for JMX, add mx4j-tools.jar to the lib/
# directory.
# See http://cassandra.apache.org/doc/latest/operating/metrics.html#jmx
# By default mx4j listens on the broadcast_address, port 8081. Uncomment the following lines
# to control its listen address and port.
#MX4J_ADDRESS="127.0.0.1"
#MX4J_PORT="8081"

# Cassandra uses SIGAR to capture OS metrics CASSANDRA-7838
# for SIGAR we have to set the java.library.path
# to the location of the native libraries.
JVM_OPTS="$JVM_OPTS -Djava.library.path=$CASSANDRA_HOME/lib/sigar-bin"

if [ "x$MX4J_ADDRESS" != "x" ]; then
    if [ "$(echo "$MX4J_ADDRESS" | grep -c "\-Dmx4jaddress")" = "1" ]; then
        # Backward compatible with the older style #13578
        JVM_OPTS="$JVM_OPTS $MX4J_ADDRESS"
    else
        JVM_OPTS="$JVM_OPTS -Dmx4jaddress=$MX4J_ADDRESS"
    fi
fi
if [ "x$MX4J_PORT" != "x" ]; then
    if [ "$(echo "$MX4J_PORT" | grep -c "\-Dmx4jport")" = "1" ]; then
        # Backward compatible with the older style #13578
        JVM_OPTS="$JVM_OPTS $MX4J_PORT"
    else
        JVM_OPTS="$JVM_OPTS -Dmx4jport=$MX4J_PORT"
    fi
fi

JVM_OPTS="$JVM_OPTS $JVM_EXTRA_OPTS"

