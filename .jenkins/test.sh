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

# add phab comment for unit tests
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

  comment=$(python3 ".jenkins/code_coverage/parse_jacoco_html.py" "-p" "build/jacoco/index.html" "-t" "${ANT_TARGET:-test}")
  echo "$comment" >> "$FILE_PHAB_COMMENT"
fi

# rename jacoco report xml+html file and upload it to buildkite artifact
mkdir "build/coverage"
cp "build/jacoco/report.xml" "build/coverage/report-${ANT_TARGET:-test}.xml"
cp "build/jacoco/index.html" "build/coverage/index-${ANT_TARGET:-test}.html"
