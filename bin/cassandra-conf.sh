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


# OPTIONS:
#   -f: start in foreground
#   -p <filename>: log the pid to a file (useful to kill it later)
#   -v: print version string and exit

# CONTROLLING STARTUP:
# 
# This script relies on few environment variables to determine startup
# behavior, those variables are:
#
#   CLASSPATH -- A Java classpath containing everything necessary to run.
#   EXTRA_CLASSPATH -- A Java classpath with anything to be appended to CLASSPATH
#   JVM_OPTS -- Additional arguments to the JVM for heap size, etc
#   JVM_ON_OUT_OF_MEMORY_ERROR_OPT -- The OnOutOfMemoryError JVM option if specified
#   CASSANDRA_CONF -- Directory containing Cassandra configuration files.
#   CASSANDRA_LOG_DIR -- Directory containing logs(default: $CASSANDRA_HOME/logs).
#
# As a convenience, a fragment of shell is sourced in order to set one or
# more of these variables. This so-called `include' can be placed in a 
# number of locations and will be searched for in order. The highest 
# priority search path is the same directory as the startup script, and
# since this is the location of the sample in the project tree, it should
# almost work Out Of The Box.
#
# Any serious use-case though will likely require customization of the
# include. For production installations, it is recommended that you copy
# the sample to one of /usr/share/cassandra/cassandra.in.sh,
# /usr/local/share/cassandra/cassandra.in.sh, or 
# /opt/cassandra/cassandra.in.sh and make your modifications there.
#
# Another option is to specify the full path to the include file in the
# environment. For example:
#
#   $ CASSANDRA_INCLUDE=/path/to/in.sh cassandra -p /var/run/cass.pid
#
# Note: This is particularly handy for running multiple instances on a 
# single installation, or for quick tests.
#
# Finally, developers and enthusiasts who frequently run from an SVN 
# checkout, and do not want to locally modify bin/cassandra.in.sh, can put
# a customized include file at ~/.cassandra.in.sh.
#
# If you would rather configure startup entirely from the environment, you
# can disable the include by exporting an empty CASSANDRA_INCLUDE, or by 
# ensuring that no include files exist in the aforementioned search list.
# Be aware that you will be entirely responsible for populating the needed
# environment variables.

# NB: Developers should be aware that this script should remain compatible with
# POSIX sh and Solaris sh. This means, in particular, no $(( )) and no $( ).

# Unset any grep options that may include `--color=always` per say.
# Using `unset GREP_OPTIONS` will also work on the non-deprecated use case
# of setting a new grep alias.
# See CASSANDRA-14487 for more details.
unset GREP_OPTIONS

show_env() {
    retval=0
    if [ -n "$ENV_DEBUG" ] ; then
        echo ""
        echo "ENVIRONMENT"
        echo "-----------"
        echo "CASSANDRA_CONF=$CASSANDRA_CONF"
    fi
    
    if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
        echo "cassandra.in.sh not found or CASSANDRA_INCLUDE not set" >&2
        retval=1
    else
        if [ -n "$ENV_DEBUG" ] ; then
            echo "CASSANDRA_INCLUDE=$CASSANDRA_INCLUDE"
        fi
    fi

    if [ -n "$ENV_DEBUG" ] ; then
        echo "CASSANDRA_LOG_DIR=$CASSANDRA_LOG_DIR"

        if [ -z $NUMACTL ]; then
            echo "numactl not found"
        fi

        while [ -n "$1" ] ; do
            eval echo "$1=\$$1"
            shift
        done
        
        if [ -n "$ENV_SHOW" ]
        then
            for n in $ENV_SHOW 
            do
                eval echo "${n}=\$${n}"
            done
        fi
        echo "-----------"
        echo ""
    fi
    if [ $retval -ne 0 ] ; then
        exit $retval
    fi
}

# locate and load the CASSANDRA_INCLUDE or cassandra.in.sh file.

if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
    # Locations (in order) to use when searching for an include file.
    for include in "`dirname "$0"`/cassandra.in.sh" \
                   "$HOME/.cassandra.in.sh" \
                   /usr/share/cassandra/cassandra.in.sh \
                   /usr/local/share/cassandra/cassandra.in.sh \
                   /opt/cassandra/cassandra.in.sh; do
        if [ -r "$include" ]; then
            CASSANDRA_INCLUDE="$include $CASSANDRA_INCLUDE"
            . "$include"
            break
        fi
    done
# ...otherwise, source the specified include.
elif [ -r "$CASSANDRA_INCLUDE" ]; then
    . "$CASSANDRA_INCLUDE"
fi

# If numactl is available, use it. For Cassandra, the priority is to
# avoid disk I/O. Even for the purpose of CPU efficiency, we don't
# really have CPU<->data affinity anyway. Also, empirically test that numactl
# works before trying to use it (CASSANDRA-3245).
NUMACTL_ARGS=${NUMACTL_ARGS:-"--interleave=all"}
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

# preserve vars that we may need from before cassandra-env
if [ -n "$ENV_PRESERVE" ]
then
    for n in $ENV_PRESERVE 
    do
        eval "${n}_SAVE=\$$n"
    done
fi

if [ -f "$CASSANDRA_CONF/cassandra-env.sh" ]; then
    . "$CASSANDRA_CONF/cassandra-env.sh"
fi

# restore vars that we may need from before cassandra-env
if [ -n "$ENV_PRESERVE" ]
then
    for n in $ENV_PRESERVE 
    do
        eval "${n}=\$${n}_SAVE"
    done
fi


if [ -z "$CASSANDRA_LOG_DIR" ]; then
  CASSANDRA_LOG_DIR=$CASSANDRA_HOME/logs
fi