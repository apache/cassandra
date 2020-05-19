#!/bin/sh

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

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    # Locations (in order) to use when searching for an include file.
    for include in "`dirname "$0"`/cassandra.in.sh" \
                   "$HOME/.cassandra.in.sh" \
                   /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh; do
        if [ -r "$include" ]; then
            . "$include"
            break
        fi
    done
elif [ -r "$CASSANDRA_INCLUDE" ]; then
    . "$CASSANDRA_INCLUDE"
fi

if [ -z "$CASSANDRA_CONF" -o -z "$CLASSPATH" ]; then
    echo "You must set the CASSANDRA_CONF and CLASSPATH vars" >&2
    exit 1
fi

# Run cassandra-env.sh to pick up JMX_PORT
if [ -f "$CASSANDRA_CONF/cassandra-env.sh" ]; then
    JVM_OPTS_SAVE=$JVM_OPTS
    MAX_HEAP_SIZE_SAVE=$MAX_HEAP_SIZE
    . "$CASSANDRA_CONF/cassandra-env.sh"
    MAX_HEAP_SIZE=$MAX_HEAP_SIZE_SAVE
    JVM_OPTS=$JVM_OPTS_SAVE
fi

# JMX Port passed via cmd line args (-p 9999 / --port 9999 / --port=9999)
# should override the value from cassandra-env.sh
ARGS=""
JVM_ARGS=""
while true
do
  if [ "x" = "x$1" ]; then break; fi
  case $1 in
    -D*)
      JVM_ARGS="$JVM_ARGS $1"
      ;;
    *)
      ARGS="$ARGS $1"
      ;;
  esac
  shift
done

if [ "x$MAX_HEAP_SIZE" = "x" ]; then
    MAX_HEAP_SIZE="512m"
fi

"$JAVA" $JAVA_AGENT -ea -da:net.openhft... -cp "$CLASSPATH" $JVM_OPTS -Xmx$MAX_HEAP_SIZE \
        -Dlog4j.configurationFile=log4j2-tools.xml \
        $JVM_ARGS \
        org.apache.cassandra.fqltool.FullQueryLogTool $ARGS

# vi:ai sw=4 ts=4 tw=0 et
