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

# pre-conditions
[ "x" != "x${DIST_DIR}" ] || { echo "DIST_DIR must be defined"; exit 1 ; }
[ "x" != "x${TEST_SCRIPT}" ] || { echo "TEST_SCRIPT must be defined"; exit 1 ; }
[ "x" != "x${CASSANDRA_DIR}" ] || { echo "CASSANDRA_DIR must be defined"; exit 1 ; }

# usage
if [ "$1" == "-h" ]; then
   echo "$0 [-h] ..."
   echo " this script is used by run-tests.sh (in the same directory) as a wrapper delegating the execution of the ${TEST_SCRIPT}. all arguments are passed through as-is to ${TEST_SCRIPT}"
   exit 1
fi

pushd "${CASSANDRA_DIR}" >/dev/null

echo "Running ${TEST_SCRIPT} $@"
.build/${TEST_SCRIPT} "$@"
status=$?
if [ -d "${DIST_DIR}/test/logs" ]; then
    find "${DIST_DIR}/test/logs" -type f -name "*.log" | xargs xz -qq
fi
popd >/dev/null
exit ${status}