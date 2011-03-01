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

cassandra_home=`dirname $0`/..

# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=$cassandra_home/test/conf

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
cassandra_bin=$cassandra_home/build/classes/main
cassandra_bin=$cassandra_bin:$cassandra_home/build/classes/thrift
#cassandra_bin=$cassandra_home/build/cassandra.jar

# The java classpath (required)
CLASSPATH=$CASSANDRA_CONF:$cassandra_bin

for jar in $cassandra_home/lib/*.jar $cassandra_home/build/lib/jars/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

# Arguments to pass to the JVM
JVM_OPTS=" \
        -ea \
        -Xdebug \
        -Xrunjdwp:transport=dt_socket,server=y,address=8898,suspend=n \
        -Xms128M \
        -Xmx1G \
        -Xss128k \
        -XX:SurvivorRatio=8 \
        -XX:TargetSurvivorRatio=90 \
        -XX:+AggressiveOpts \
        -XX:+UseParNewGC \
        -XX:+UseConcMarkSweepGC \
        -XX:CMSInitiatingOccupancyFraction=1 \
        -XX:+CMSParallelRemarkEnabled \
        -XX:+HeapDumpOnOutOfMemoryError \
        -Dcom.sun.management.jmxremote.port=8090 \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Dcom.sun.management.jmxremote.authenticate=false"
