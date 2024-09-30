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

echo "PHAB Diff ID: $PHAB_DIFF_ID"
echo "Test PHAB Diff ID: $TEST_PHAB_DIFF_ID"

FILE_PHAB_COMMENT="build/comment/phabricator-comment-utest-jvmdtest"

python3 --version
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade setuptools
pip3 install -r .jenkins/code_coverage/requirements.txt

if [ -z "$TEST_PHAB_DIFF_ID" ]; then
  DIFF_ID=$PHAB_DIFF_ID
else
  DIFF_ID=$TEST_PHAB_DIFF_ID
fi
echo "Diff ID to collect coverage report: $DIFF_ID"

python3 .jenkins/code_coverage/coverage_report_generator.py --diff_id $DIFF_ID

if [ -f /tmp/buildkite_build_ids.json ]; then
    output=$(cat /tmp/buildkite_build_ids.json)
    echo "JSON content: $output"
    BUILDKITE_BUILD_IDS=($(echo "$output" | jq -r '.[]'))
else
    echo "JSON file not found!"
    exit 1
fi

echo "Buildkite build ids: ${BUILDKITE_BUILD_IDS[@]}"

for build_phid in "${BUILDKITE_BUILD_IDS[@]}"; do
    echo "Downloading artifact for build PHID: $build_phid"
    buildkite-agent artifact download "build/jacoco/*.exec" . --build "$build_phid" --agent-access-token $BUILDKITE_AGENT_ACCESS_TOKEN
done

# generic-udj is migrated to Debian 12, which is using Java 17 by default. We want to switch to Java 11
JAVA_HOME="$(readlink -f $HOME/java_home/jdk_11)"
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
java --version

# use java 11
export CASSANDRA_USE_JDK11=true

# download ant 1.10
wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
tar xzvf apache-ant-1.10.12-bin.tar.gz
ANT_HOME="$(readlink -f apache-ant-1.10.12)"
export ANT_HOME

# update PATH
export PATH="$ANT_HOME/bin:$JAVA_HOME/bin:$PATH"

# create build/jacoco file
ant jacoco-init
BASE_NAMES=""
for file in build/jacoco/*.exec; do
    base_name=$(basename "$file" .exec)
    BASE_NAMES="$BASE_NAMES $base_name"
done

ant build
ant jacoco-report

ls build/jacoco

# Phabricator comment
if [[ ! -f "build/comment" ]]; then
    mkdir "build/comment"
fi
comment=$(python3 ".jenkins/code_coverage/parse_jacoco_html.py" "-p" "build/jacoco/index.html" "-t" "utest+jvmdtest")
echo "$comment" >> "$FILE_PHAB_COMMENT"
echo "" >> "$FILE_PHAB_COMMENT"
BASE_NAMES=$(echo $BASE_NAMES | tr ' ' '\n' | sort | xargs)
echo "jacoco.exec files found for: **$BASE_NAMES**" >> "$FILE_PHAB_COMMENT"
echo "" >> "$FILE_PHAB_COMMENT"
echo "Result might be partial. You may re-generate the report after the builds are completed with https://code.uberinternal.com/harbormaster/plan/23269/ (Run Plan Manually > copy paste the revision number DXXX)" >> "$FILE_PHAB_COMMENT"
