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
echo "UNIT_TEST_GROUP_ID: $UNIT_TEST_GROUP_ID"
echo "TEST_TASK_NAME: $TEST_TASK_NAME"

# Get the directory of the current script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

export PROJECT_DIR
# use java 11
export CASSANDRA_USE_JDK11=true

# generic-udj is migrated to Debian 12, which is using Java 17 by default. We want to switch to Java 11
JAVA_HOME="$(readlink -f $HOME/java_home/jdk_11)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
java --version

# download ant 1.10
wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
tar xzvf apache-ant-1.10.12-bin.tar.gz
ANT_HOME="$(readlink -f apache-ant-1.10.12)"
export ANT_HOME

# update PATH
export PATH="$ANT_HOME/bin:$JAVA_HOME/bin:$PATH"

ant realclean

ant build

cd "$PROJECT_DIR"

# update ports
./.jenkins/update_ports_for_unit_tests.sh "${UNIT_TEST_GROUP_ID}"
# check ports updated
git diff

dir_path="$PROJECT_DIR/test/unit"

output_file_prefix="unit_tests"

./.jenkins/get_all_tests.sh "${dir_path}" "${output_file_prefix}" 10

selected_test_file="${dir_path}/${output_file_prefix}_part_$(printf '%04d' $UNIT_TEST_GROUP_ID).txt"

selected_java_files_to_test=$(wc -l < "$selected_test_file")

if [ "$selected_java_files_to_test" -le 0 ]; then
    echo "Error: No tests found in the selected file."
    exit 1
fi

# run jvm dtest with jacoco report generated
ant codecoverage -Dtaskname="$TEST_TASK_NAME" -Dtest.timeout=900000 -Dtest.classlistfile="$selected_test_file" -Dtest.classlistprefix=unit

# upload code coverage xml , and the full jacoco tar to buildkite artifact
mkdir "build/coverage"
cp "build/jacoco/report.xml" "build/coverage/report-utest-${TEST_TASK_NAME}-group${UNIT_TEST_GROUP_ID}.xml"
# rename jacoco.exec
mv build/jacoco/jacoco.exec build/jacoco/jacoco-utest-${TEST_TASK_NAME}-group${UNIT_TEST_GROUP_ID}.exec

# try to generate coverage report
./.jenkins/coverage_report_generator.sh
