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
echo "DTEST_GROUP_ID: $DTEST_GROUP_ID"

# env setup (PROJECT_DIR, Java11, ant)
source "./.jenkins/env_setup.sh"
j11Setup
antSetup

ant realclean
ant build

cd "$PROJECT_DIR"

dir_path="$PROJECT_DIR/test/distributed"

output_file_prefix="distributed_tests"

# update the port so it won't conflict
./.jenkins/update_ports_for_jvm_dtests.sh "${DTEST_GROUP_ID}"

./.jenkins/get_all_tests.sh "${dir_path}" "${output_file_prefix}" 20

selected_test_file="${dir_path}/${output_file_prefix}_part_$(printf '%04d' $DTEST_GROUP_ID).txt"

selected_java_files_to_test=$(wc -l < "$selected_test_file")

if [ "$selected_java_files_to_test" -le 0 ]; then
    echo "Error: No tests found in the selected file."
    exit 1
fi

# run jvm dtest with jacoco report generated
ant codecoverage -Dtaskname=testclasslist -Dtest.timeout=900000 -Dtest.classlistfile="$selected_test_file" -Dtest.classlistprefix=distributed

# upload code coverage xml , and the full jacoco tar to buildkite artifact
mkdir "build/coverage"
cp "build/jacoco/report.xml" "build/coverage/report-dtest-group${DTEST_GROUP_ID}.xml"
# rename jacoco.exec
mv build/jacoco/jacoco.exec build/jacoco/jacoco-dtest-group${DTEST_GROUP_ID}.exec

# try to generate coverage report
./.jenkins/coverage_report_generator.sh
