#!/bin/bash
#
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
#
#
# /etc/init.d/cassandra
#
# Startup script for Cassandra
#
# chkconfig: 2345 80 20
# description: Starts and stops Cassandra
# pidfile: /var/run/cassandra/cassandra.pid

### BEGIN INIT INFO
# Provides:          cassandra
# Required-Start:    $remote_fs $network $named $time
# Required-Stop:     $remote_fs $network $named $time
# Should-Start:      ntp mdadm
# Should-Stop:       ntp mdadm
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: distributed storage system for structured data
# Description:       Cassandra is a distributed (peer-to-peer) system for
#                    the management and storage of structured data.
### END INIT INFO

. /etc/rc.d/init.d/functions

export CASSANDRA_HOME=/usr/share/cassandra
export CASSANDRA_CONF=/etc/cassandra/conf
export CASSANDRA_INCLUDE=$CASSANDRA_HOME/cassandra.in.sh
export CASSANDRA_OWNR=cassandra
NAME="cassandra"
log_file=/var/log/cassandra/cassandra.log
pid_file=/var/run/cassandra/cassandra.pid
lock_file=/var/lock/subsys/$NAME
CASSANDRA_PROG=/usr/sbin/cassandra

# The first existing directory is used for JAVA_HOME if needed.
JVM_SEARCH_DIRS="/usr/lib/jvm/jre /usr/lib/jvm/jre-1.8.* /usr/lib/jvm/java-1.8.*/jre"

# Read configuration variable file if it is present
[ -r /etc/default/$NAME ] && . /etc/default/$NAME

# If JAVA_HOME has not been set, try to determine it.
if [ -z "$JAVA_HOME" ]; then
    # If java is in PATH, use a JAVA_HOME that corresponds to that. This is
    # both consistent with how the upstream startup script works, and with
    # the use of alternatives to set a system JVM (as is done on Debian and
    # Red Hat derivatives).
    java="`/usr/bin/which java 2>/dev/null`"
    if [ -n "$java" ]; then
        java=`readlink --canonicalize "$java"`
        JAVA_HOME=`dirname "\`dirname \$java\`"`
    else
        # No JAVA_HOME set and no java found in PATH; search for a JVM.
        for jdir in $JVM_SEARCH_DIRS; do
            if [ -x "$jdir/bin/java" ]; then
                JAVA_HOME="$jdir"
                break
            fi
        done
        # if JAVA_HOME is still empty here, punt.
    fi
fi
JAVA="$JAVA_HOME/bin/java"
export JAVA_HOME JAVA

case "$1" in
    start)
        # Cassandra startup
        echo -n "Starting Cassandra: "
        [ -d `dirname "$pid_file"` ] || \
            install -m 755 -o $CASSANDRA_OWNR -g $CASSANDRA_OWNR -d `dirname $pid_file`
        runuser -u $CASSANDRA_OWNR -- $CASSANDRA_PROG -p $pid_file > $log_file 2>&1
        retval=$?
        chown root.root $pid_file
        [ $retval -eq 0 ] && touch $lock_file
        echo "OK"
        ;;
    stop)
        # Cassandra shutdown
        echo -n "Shutdown Cassandra: "
        runuser -u $CASSANDRA_OWNR -- kill `cat $pid_file`
        retval=$?
        [ $retval -eq 0 ] && rm -f $lock_file
        for t in `seq 40`; do
            status -p $pid_file cassandra > /dev/null 2>&1
            retval=$?
            if [ $retval -eq 3 ]; then
                echo "OK"
                exit 0
            else
                sleep 0.5
            fi;
        done
        # Adding a sleep here to give jmx time to wind down (CASSANDRA-4483). Not ideal...
        # Adam Holmberg suggests this, but that would break if the jmx port is changed
        # for t in `seq 40`; do netstat -tnlp | grep "0.0.0.0:7199" > /dev/null 2>&1 && sleep 0.1 || break; done
        sleep 5
        status -p $pid_file cassandra > /dev/null 2>&1
        retval=$?
        if [ $retval -eq 3 ]; then
            echo "OK"
        else
            echo "ERROR: could not stop $NAME"
            exit 1
        fi
        ;;
    reload|restart)
        $0 stop
        $0 start
        ;;
    status)
        status -p $pid_file cassandra
        exit $?
        ;;
    *)
        echo "Usage: `basename $0` start|stop|status|restart|reload"
        exit 1
esac

exit 0

