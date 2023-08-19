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

. ci_functions.sh

# Will generate a list of all the tests that differ on the active branch from the provided base branch across all suites

# A user / env can manually override this by providing a space delimited array of relative pathed file names in the
# REPEAT_TESTS environment variable

# TODO BEFORE COMMIT: Work through and clean this up / test it
# TODO Make this support python w/a 3rd arg

usage() {
    echo "usage: generate_diff_test_list <base_branch> <base_dir>"
    exit 1
}


# Sanity check that the referenced branch exists
if ! git show "${base_branch}" -- >&/dev/null; then
    echo -e "\n\nUnknown base branch: ${base_branch}. Unable to detect changed tests.\n"
    echo    "Please use the '-b' option to choose an existing branch name"
    echo    "(e.g. origin/${base_branch}, apache/${base_branch}, etc.)."
    exit 2
fi

echo
echo "Detecting new or modified tests with git diff --diff-filter=AMR ${base_branch}...HEAD:"
add_diff_tests "REPEATED_UTESTS" "test/unit/" "org.apache.cassandra"
add_diff_tests "REPEATED_UTESTS_LONG" "test/long/" "org.apache.cassandra"
add_diff_tests "REPEATED_UTESTS_STRESS" "tools/stress/test/unit/" "org.apache.cassandra.stress"
add_diff_tests "REPEATED_UTESTS_FQLTOOL" "tools/fqltool/test/unit/" "org.apache.cassandra.fqltool"
add_diff_tests "REPEATED_SIMULATOR_DTESTS" "test/simulator/test/" "org.apache.cassandra.simulator.test"
add_diff_tests "REPEATED_JVM_DTESTS" "test/distributed/" "org.apache.cassandra.distributed.test"
add_diff_tests "REPEATED_JVM_UPGRADE_DTESTS" "test/distributed/" "org.apache.cassandra.distributed.upgrade"
add_diff_tests "REPEATED_PYTHON_DTESTS" "" ""