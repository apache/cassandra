#! /bin/bash
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

set -eo pipefail

set -x

PROJECT_ROOT="${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}"
PROJECT_DIR="${PROJECT_ROOT}"
export PROJECT_DIR

source "$PROJECT_ROOT/.jenkins/env_setup.sh"
j11Setup
antSetup

ant realclean
ant jacoco-run -Dtaskname=test

cd "$PROJECT_ROOT"
ant build

dir_path="$PROJECT_DIR/test/distributed"
output_file_prefix="distributed_tests"

./.jenkins/get_all_tests.sh "${dir_path}" "${output_file_prefix}" 20
selected_test_file="${dir_path}/${output_file_prefix}"
ant jacoco-run -Dtaskname=testclasslist  -Dtest.timeout=900000  -Dtest.classlistfile="$selected_test_file"  -Dtest.classlistprefix=distributed
ant jacoco-init jacoco-merge jacoco-report

# increase timeout for sonar scanner
export SONAR_SCANNER_TIME_PER_WORK_UNIT_SECONDS=600