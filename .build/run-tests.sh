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

#
# Wrapper script for running a split or regexp of tests (excluding python dtests)
# This is kept for backwards compatibility pre-C18249 en will be removed. Use run-tests-enhaced.sh instead

# help
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-tests.sh test_type [split_chunk|test_regexp]"
    echo ""
    echo "        default split_chunk is 1/1"
    exit 1
fi

test_type="-a ${1}"

if [[ -z ${2} ]]; then
  test_list=""
elif [[ -n ${2} && "${2}" =~ ^[0-9]+/[0-9]+$ ]]; then
  test_list="-c ${2}";
else
  test_list="-t ${2}";
fi

# shellcheck disable=SC2086
.build/run-tests-enhanced.sh ${test_type} ${test_list}