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


set -ex

echo "ANT_TARGET: $ANT_TARGET"
echo "TEST_NAME: $TEST_NAME"
echo "TEST_METHODS: $TEST_METHODS"
echo "EXCLUDED_TEST_PATHS: $EXCLUDED_TEST_PATHS"

export -n ANT_TARGET TEST_NAME TEST_METHODS

# env setup (Java11, ant)
source "./.jenkins/env_setup.sh"
j11Setup
antSetup

ant realclean
# update ports
./.jenkins/update_ports_for_unit_tests.sh

if [ "$#" -eq 0 ]; then
    # generate Jacoco code coverage report
    ANT_ARGS=("codecoverage")
    ANT_ARGS+=("-Dtaskname=${ANT_TARGET:-test}")

    if [ -n "$TEST_NAME" ]; then
        ANT_ARGS+=("-Dtest.name=${TEST_NAME}")
    fi
    if [ -n "$TEST_METHODS" ]; then
        ANT_ARGS+=("-Dtest.methods=${TEST_METHODS}")
    fi
    if [ -n "$EXCLUDED_TEST_PATHS" ]; then
        ANT_ARGS+=("-Dexcluded.test.paths=${EXCLUDED_TEST_PATHS}")
    fi
    ant "${ANT_ARGS[@]}"
else
    ant "$@"
fi

# upload code coverage xml , and the full jacoco tar to buildkite artifact
mkdir "build/coverage"
cp "build/jacoco/report.xml" "build/coverage/report-${ANT_TARGET:-test}.xml"
# rename jacoco.exec
mv build/jacoco/jacoco.exec build/jacoco/jacoco-utest-${ANT_TARGET:-test}.exec

# try to generate coverage report
./.jenkins/coverage_report_generator.sh

