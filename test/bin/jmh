#!/bin/bash
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

jvmoptions_variant="-server"
export CASSANDRA_HOME="$(cd $(dirname "$0")/../../; pwd)"
. $CASSANDRA_HOME/bin/cassandra.in.sh

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -n "$JAVA_HOME" ]; then
    # Why we can't have nice things: Solaris combines x86 and x86_64
    # installations in the same tree, using an unconventional path for the
    # 64bit JVM.  Since we prefer 64bit, search the alternate path first,
    # (see https://issues.apache.org/jira/browse/CASSANDRA-4638).
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables. >&2
    exit 1;
fi

# If numactl is available, use it. For Cassandra, the priority is to
# avoid disk I/O. Even for the purpose of CPU efficiency, we don't
# really have CPU<->data affinity anyway. Also, empirically test that numactl
# works before trying to use it (CASSANDRA-3245).
NUMACTL_ARGS=${NUMACTL_ARGS:-"--localalloc"}
if which numactl >/dev/null 2>/dev/null && numactl $NUMACTL_ARGS ls / >/dev/null 2>/dev/null
then
    NUMACTL="numactl $NUMACTL_ARGS"
else
    NUMACTL=""
fi

if [ -z "$CASSANDRA_CONF" -o -z "$CLASSPATH" ]; then
    echo "You must set the CASSANDRA_CONF and CLASSPATH vars" >&2
    exit 1
fi

if [ -f "$CASSANDRA_CONF/cassandra-env.sh" ]; then
    . "$CASSANDRA_CONF/cassandra-env.sh"
fi

# Special-case path variables.
case "`uname`" in
    CYGWIN*)
        CLASSPATH=`cygpath -p -w "$CLASSPATH"`
        CASSANDRA_CONF=`cygpath -p -w "$CASSANDRA_CONF"`
    ;;
esac

# Cassandra uses an installed jemalloc via LD_PRELOAD / DYLD_INSERT_LIBRARIES by default to improve off-heap
# memory allocation performance. The following code searches for an installed libjemalloc.dylib/.so/.1.so using
# Linux and OS-X specific approaches.
# To specify your own libjemalloc in a different path, configure the fully qualified path in CASSANDRA_LIBJEMALLOC.
# To disable jemalloc preload at all, set CASSANDRA_LIBJEMALLOC=-
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
                dirs="/lib64 /lib /usr/lib64 /usr/lib `ldconfig -v 2>/dev/null | grep -v '^\s' | sed 's/^\([^:]*\):.*$/\1/'`"
            else
                # e.g. for Debian, OpenSUSE
                dirs="/lib64 /lib /usr/lib64 /usr/lib `cat /etc/ld.so.conf /etc/ld.so.conf.d/*.conf | grep '^/'`"
            fi
            dirs=`echo $dirs | tr " " ":"`
            CASSANDRA_LIBJEMALLOC=$(find_library '.*/libjemalloc\.so\(\.1\)*' $dirs)
        fi
        if [ ! -z $CASSANDRA_LIBJEMALLOC ] ; then
            export JVM_OPTS="$JVM_OPTS -Dcassandra.libjemalloc=$CASSANDRA_LIBJEMALLOC"
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
            export JVM_OPTS="$JVM_OPTS -Dcassandra.libjemalloc=$CASSANDRA_LIBJEMALLOC"
            if [ "-" != "$CASSANDRA_LIBJEMALLOC" ] ; then
                export DYLD_INSERT_LIBRARIES=$CASSANDRA_LIBJEMALLOC
            fi
        fi
    ;;
esac

cassandra_parms="-Dlogback.configurationFile=$CASSANDRA_HOME/test/conf/logback-jmh.xml"
cassandra_parms="$cassandra_parms -Dcassandra.logdir=$CASSANDRA_HOME/logs"
cassandra_parms="$cassandra_parms -Dcassandra.storagedir=$cassandra_storagedir"
cassandra_parms="$cassandra_parms -Dcassandra-foreground=yes"
cassandra_parms="$cassandra_parms -XX:+PreserveFramePointer"

# Create log directory, some tests require that
mkdir -p $CASSANDRA_HOME/logs

if [ ! -f $CASSANDRA_HOME/build/apache-cassandra-*.jar ] ; then
    echo "$CASSANDRA_HOME/build/apache-cassandra-*.jar does not exist - execute 'ant jar' first"
    exit 1
fi

CLASSPATH="$CLASSPATH:$CASSANDRA_HOME/test/conf/"
CLASSPATH="$CLASSPATH:$CASSANDRA_HOME/build/test/classes/"
CLASSPATH="$CLASSPATH:$CASSANDRA_HOME/build/test/lib/jars/*"

exec $NUMACTL "$JAVA" -cp "$CLASSPATH" org.openjdk.jmh.Main -jvmArgs="$cassandra_parms $JVM_OPTS" "$@"

# vi:ai sw=4 ts=4 tw=0 et
