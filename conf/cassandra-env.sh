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

calculate_heap_size()
{
    case "`uname`" in
        Linux)
            system_memory_in_mb=`free -m | awk '/Mem:/ {print $2}'`
            MAX_HEAP_SIZE=$((system_memory_in_mb / 2))M
            return 0
        ;;
        FreeBSD)
            system_memory_in_bytes=`sysctl hw.physmem | awk '{print $2}'`
            MAX_HEAP_SIZE=$((system_memory_in_bytes / 1024 / 1024 / 2))M
            return 0
        ;;
        *)
            MAX_HEAP_SIZE=1024M
            return 1
        ;;
    esac
}

# The amount of memory to allocate to the JVM at startup, you almost
# certainly want to adjust this for your environment. If left commented
# out, the heap size will be automatically determined by calculate_heap_size
# MAX_HEAP_SIZE="4G"

if [ "x$MAX_HEAP_SIZE" = "x" ]; then
    calculate_heap_size
fi

# Specifies the default port over which Cassandra will be available for
# JMX connections.
JMX_PORT="8080"

# To use mx4j, an HTML interface for JMX, add mx4j-tools.jar to the lib/ directory.
# By default mx4j listens on 0.0.0.0:8081. Uncomment the following lines to control
# its listen address and port.
#MX4J_ADDRESS="-Dmx4jaddress=0.0.0.0"
#MX4J_PORT="-Dmx4jport=8081"


# Here we create the arguments that will get passed to the jvm when
# starting cassandra.

# enable assertions.  disabling this in production will give a modest
# performance benefit (around 5%).
JVM_OPTS="$JVM_OPTS -ea"

# enable thread priorities, primarily so we can give periodic tasks
# a lower priority to avoid interfering with client workload
JVM_OPTS="$JVM_OPTS -XX:+UseThreadPriorities"
# allows lowering thread priority without being root.  see
# http://tech.stolsvik.com/2010/01/linux-java-thread-priorities-workaround.html
JVM_OPTS="$JVM_OPTS -XX:ThreadPriorityPolicy=42"

# min and max heap sizes should be set to the same value to avoid
# stop-the-world GC pauses during resize, and so that we can lock the
# heap in memory on startup to prevent any of it from being swapped
# out.
JVM_OPTS="$JVM_OPTS -Xms$MAX_HEAP_SIZE"
JVM_OPTS="$JVM_OPTS -Xmx$MAX_HEAP_SIZE"
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError" 

if [ "`uname`" = "Linux" ] ; then
    # reduce the per-thread stack size to minimize the impact of Thrift
    # thread-per-client.  (Best practice is for client connections to
    # be pooled anyway.) Only do so on Linux where it is known to be
    # supported.
    JVM_OPTS="$JVM_OPTS -Xss128k"
fi

# GC tuning options
JVM_OPTS="$JVM_OPTS -XX:+UseParNewGC" 
JVM_OPTS="$JVM_OPTS -XX:+UseConcMarkSweepGC" 
JVM_OPTS="$JVM_OPTS -XX:+CMSParallelRemarkEnabled" 
JVM_OPTS="$JVM_OPTS -XX:SurvivorRatio=8" 
JVM_OPTS="$JVM_OPTS -XX:MaxTenuringThreshold=1"
JVM_OPTS="$JVM_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JVM_OPTS="$JVM_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

# GC logging options -- uncomment to enable
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails"
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCTimeStamps"
# JVM_OPTS="$JVM_OPTS -XX:+PrintClassHistogram"
# JVM_OPTS="$JVM_OPTS -XX:+PrintTenuringDistribution"
# JVM_OPTS="$JVM_OPTS -XX:+PrintGCApplicationStoppedTime"
# JVM_OPTS="$JVM_OPTS -Xloggc:/var/log/cassandra/gc.log"

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
# http://blogs.sun.com/jmxetc/entry/troubleshooting_connection_problems_in_jconsole
# for more on configuring JMX through firewalls, etc. (Short version:
# get it working with no firewall first.)
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.ssl=false" 
JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false" 
JVM_OPTS="$JVM_OPTS $MX4J_ADDRESS" 
JVM_OPTS="$JVM_OPTS $MX4J_PORT" 
