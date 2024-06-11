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

show_help()
{
    echo "Usage: $0 [-p <pidfile>] | [-l] | [-e] | [-h]"
    echo "  -p <pidfile>    Stop the process using the specified pidfile"
    echo "  -l              Stop the process with the name like 'cassandra'"
    echo "  -e              Stop the process with the name equal 'CassandraDaemon'"
    echo "  -h              Show the help message"
}

kill_processes()
{
  pids=$(pgrep -u "$(whoami)" -f "${1}")
  if [ -n "$pids" ]; then
    echo "$pids" | xargs kill -9
  fi
}

if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

case "$1" in
    -p)
        if [ -z "$2" ]; then
            echo "missing pidfile argument after -p"
            exit 1
        fi
        if [ ! -f "$2" ]; then
          echo "pidfile $2 does not exist"
          exit 1
        fi
        # if you are using the cassandra start script with -p, this
        # is the best way to stop:
        kill "$(cat "$2")"
        ;;
    -l)
        # you can run something like this, but
        # this is a shotgun approach and will kill other processes
        # with cassandra in their name or arguments too:
        kill_processes cassandra
        ;;
    -e)
        kill_processes org.apache.cassandra.service.CassandraDaemon
        ;;
    -h)
        show_help
        exit 0
        ;;
    *)
        show_help
        exit 1
        ;;
esac

exit 0
