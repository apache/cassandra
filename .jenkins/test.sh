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

## download oracle jdk 8
#curl http://artifactory.uber.internal:4587/artifactory/libs-release-local/oracle/server-jdk-linux-x64/1.8.0_111/server-jdk-linux-x64-1.8.0_111.tar.gz -o server-jdk-linux-x64-1.8.0_111.tar.gz
#tar xzvf server-jdk-linux-x64-1.8.0_111.tar.gz
#JAVA_HOME="$(readlink -f jdk1.8.0_111)"
#export JAVA_HOME

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

# TODO: Ideally this report should be generated from a separate build waiting other builds finished.
# TODO: Currently generic-udj doesn't support customized pipeline for micro-repo and Phabricator doesn't have this
# TODO: wait mechanism. So we assume the in-jvm dtests should already finished at this time and we'll be able to get the artifacts.
# TODO: i.e. the report result might be partial.
# add phab comments
if [[ -f "build/jacoco/report.xml" ]] && [[ "${ANT_TARGET:-test}" == "test" ]]; then
  if [[ ! -f "build/comment" ]]; then
    mkdir "build/comment"
  fi
  FILE_PHAB_COMMENT="build/comment/phabricator-comment-${ANT_TARGET:-test}"

  # setup python env
  python3 --version
  set -e
  virtualenv --python=python3 venv
  source venv/bin/activate
  pip3 install --upgrade setuptools
  pip3 install -r .jenkins/code_coverage/requirements.txt

  # try to get code coverage for other builds
  echo "Diff ID to collect coverage report: $PHAB_DIFF_ID"
  python3 .jenkins/code_coverage/coverage_report_generator.py --diff_id $PHAB_DIFF_ID

  if [ -f /tmp/buildkite_build_ids.json ]; then
      output=$(cat /tmp/buildkite_build_ids.json)
      echo "JSON content: $output"
      BUILDKITE_BUILD_IDS=($(echo "$output" | jq -r '.[]'))
  else
      echo "JSON file not found!"
      break
  fi

  echo "Buildkite build ids: ${BUILDKITE_BUILD_IDS[@]}"

  # There might be no artifact for download here (build hasn't finished) and we don't fail the script here
  set +e
  for build_phid in "${BUILDKITE_BUILD_IDS[@]}"; do
      echo "Downloading artifact for build PHID: $build_phid"
      buildkite-agent artifact download "build/jacoco/*.exec" . --build "$build_phid"
  done
  set -e

  BASE_NAMES=""
  for file in build/jacoco/*.exec; do
      base_name=$(basename "$file" .exec)
      BASE_NAMES="$BASE_NAMES $base_name"
  done

  ant build
  ant jacoco-report

  comment=$(python3 ".jenkins/code_coverage/parse_jacoco_html.py" "-p" "build/jacoco/index.html" "-t" "${ANT_TARGET:-test}")
  echo "$comment" >> "$FILE_PHAB_COMMENT"
  echo "jacoco.exec files found for: **$BASE_NAMES**" >> "$FILE_PHAB_COMMENT"
  echo "This report is generated automatically after unit test is finished and result might be partial. You may re-generate the report after the builds are completed with https://code.uberinternal.com/harbormaster/plan/23269/ (Run Plan Manually > copy paste the revision number DXXX)" >> "$FILE_PHAB_COMMENT"
fi

