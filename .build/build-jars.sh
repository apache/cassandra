#!/bin/sh -e
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

# temporary between CASSANDRA-18133 and CASSANDRA-18594

print_help() {
  echo "Usage: $0 [-c|--clean] [-s|--summary] [-h|--help]"
  echo "   -c, --clean    Remove locally created artifacts (ant clean) before building"
  echo "   -s, --summary  Print a summary of failures instead of the full ant output"
  echo "   -h, --help     Print help"
}

# arguments, with defaults
clean=false
summary=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    -c|--clean)   clean=true; shift ;;
    -s|--summary) summary=true; shift ;;
    -h|--help)    print_help; exit 0 ;;
    *)            echo >&2 "Unknown argument $1"; print_help >&2; exit 1 ;;
  esac
done

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname -- "$0")/..)"

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }

# run ant, summarizing failures when --summary (summary exit code mirrors the build)
run_ant() {
  if ${summary}; then
    ant -f "${CASSANDRA_DIR}/build.xml" "$@" 2>&1 | "${CASSANDRA_DIR}/.build/sh/ant-log-summary.py" -
  else
    ant -f "${CASSANDRA_DIR}/build.xml" "$@"
  fi
}

# execute
if ${clean}; then
  run_ant clean
fi
run_ant jar
exit $?
