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

# A script to include the cassandra.in.sh file, set NUMACTL, and load 
#          $CASSANDRA_CONF/cassandra-env.sh"
#
# Defines a validate_env() function to verify critical environment vars are set
# and display them if desired

## Environment variables:
# CASSANDRA_ENV_DEBUG: Show all var settings during the validate_env() call.
# CASSANDRA_ENV_PRESERVE: A list of environment variables to preserve from being set
#       by $CASSANDRA_CONF/cassandra-env.sh"
# CASSANDRA_ENV_SHOW: A list of additional environment variables to display during validate_env.

## validate_env
## Args:  Arguments are additional environment variables to show.
##
## Validates that required environment variables are set. 
## Required Environment Vars:
##      CASSANDRA_INCLUDE
##      CASSANDRA_HOME
##      CASSANDRA_LOG_DIR
## 
## Environment Vars:
##      CASSANDRA_ENV_DEBUG   Enables display of variables if this var is set.
##      CASSANDRA_ENV_SHOW    List of additional variables to display.
##
validate_env() {
    retval=0
    if [ -n "$CASSANDRA_ENV_DEBUG" ] ; then
        echo "ENVIRONMENT"
        echo "-----------"
        echo "PWD=$PWD"
        echo "realpath="`realpath -e $0`
        echo "CASSANDRA_CONF=$CASSANDRA_CONF"
    fi
    
    if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
        echo ">>> cassandra.in.sh not found and CASSANDRA_INCLUDE not set" >&2
        retval=1
    else
        if [ -n "$CASSANDRA_ENV_DEBUG" ] ; then
            echo "CASSANDRA_INCLUDE=$CASSANDRA_INCLUDE"
        fi
    fi
    
    if [ "x$CASSANDRA_HOME" = "x" ]; then
        echo ">>> CASSANDRA_HOME not set" >&2
        retval=1
    else
        if [ -n "$CASSANDRA_ENV_DEBUG" ] ; then
            echo "CASSANDRA_HOME=$CASSANDRA_HOME"
        fi
    fi

    if [ "x$CASSANDRA_LOG_DIR" = "x" ]; then
        echo ">>> CASSANDRA_LOG_DIR not set" >&2
        retval=1
    else
        if [ -n "$CASSANDRA_ENV_DEBUG" ] ; then
            echo "CASSANDRA_LOG_DIR=$CASSANDRA_LOG_DIR"
        fi
    fi

    if [ -n "$CASSANDRA_ENV_DEBUG" ] ; then
        if [ -z $NUMACTL ]; then
            echo "numactl not found"
        fi

        while [ -n "$1" ] ; do
            eval echo "$1=\$$1"
            shift
        done
        
        if [ -n "$CASSANDRA_ENV_SHOW" ] ; then
            for n in $CASSANDRA_ENV_SHOW
            do
                eval echo "${n}=\$${n}"
            done
        fi
        echo "-----------"
    fi
    if [ $retval -ne 0 ] ; then
        exit $retval
    fi
}

# locate and load the CASSANDRA_INCLUDE or cassandra.in.sh file.
# because this file is sourced the $0 is the original scripts name and path

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
     if [ "x$CASSANDRA_INCLUDE" = "x" ]; then
         echo "cassandra.in.sh not found and CASSANDRA_INCLUDE not set" >&2
     fi
    exit 1
fi

# preserve vars that we may need from before cassandra-env
if [ -n "$CASSANDRA_ENV_PRESERVE" ]
then
    for n in $CASSANDRA_ENV_PRESERVE
    do
        eval "${n}_SAVE=\$$n"
    done
fi

if [ -f "$CASSANDRA_CONF/cassandra-env.sh" ]; then
    . "$CASSANDRA_CONF/cassandra-env.sh"
fi

# restore vars that we may need from before cassandra-env
if [ -n "$CASSANDRA_ENV_PRESERVE" ]
then
    for n in $CASSANDRA_ENV_PRESERVE
    do
        eval "${n}=\$${n}_SAVE"
    done
fi


if [ -z "$CASSANDRA_LOG_DIR" ]; then
  CASSANDRA_LOG_DIR=$CASSANDRA_HOME/logs
fi
