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

# TODO:
#   - Clean up to make it valid (args, flow)
#   - Handle the afterward (store results and artifacts)
#   -
#  run_repeated_dtest:
#    parameters:
#      tests:
#        type: string
#      vnodes:
#        type: string
#      upgrade:
#        type: string
#      count:
#        type: string
#      stop_on_failure:
#        type: string
#      extra_dtest_args:
#        type: string
#        default: ""
#    steps:
#      - run:
#          name: Run repeated Python DTests
#          no_output_timeout: 15m
#          command: |
#
# Deal with:
#- store_test_results:
#path: /tmp/results
#- store_artifacts:
#path: /tmp/dtest
#destination: dtest
#- store_artifacts:
#path: ~/cassandra-dtest/logs
#destination: dtest_logs

repeat_python_dtests() {
if [ "<<parameters.tests>>" == "<nil>" ]; then
  echo "Repeated dtest name hasn't been defined, exiting without running any test"
elif [ "<<parameters.count>>" == "<nil>" ]; then
  echo "Repeated dtest count hasn't been defined, exiting without running any test"
elif [ "<<parameters.count>>" -le 0 ]; then
  echo "Repeated dtest count is lesser or equals than zero, exiting without running any test"
else

  # Calculate the number of test iterations to be run by the current parallel runner.
  # Since we are running the same test multiple times there is no need to use `circleci tests split`.
  count=$((<<parameters.count>> / CIRCLE_NODE_TOTAL))
  if (($CIRCLE_NODE_INDEX < (<<parameters.count>> % CIRCLE_NODE_TOTAL))); then
    count=$((count+1))
  fi

  if (($count <= 0)); then
    echo "No tests to run in this runner"
  else
    echo "Running <<parameters.tests>> $count times"

    source ~/env3.6/bin/activate
    export PATH=$JAVA_HOME/bin:$PATH

    java -version
    cd ~/cassandra-dtest
    mkdir -p /tmp/dtest

    echo "env: $(env)"
    echo "** done env"
    mkdir -p /tmp/results/dtests

    tests_arg=$(echo <<parameters.tests>> | sed -e "s/,/ /g")

    stop_on_failure_arg=""
    if <<parameters.stop_on_failure>>; then
      stop_on_failure_arg="-x"
    fi

    vnodes_args=""
    if <<parameters.vnodes>>; then
      vnodes_args="--use-vnodes --num-tokens=16"
    fi

    upgrade_arg=""
    if <<parameters.upgrade>>; then
      upgrade_arg="--execute-upgrade-tests --upgrade-target-version-only --upgrade-version-selection all"
    fi

    # we need the "set -o pipefail" here so that the exit code that circleci will actually use is from pytest and not the exit code from tee
    set -o pipefail && cd ~/cassandra-dtest && pytest $vnodes_args --count=$count $stop_on_failure_arg $upgrade_arg --log-cli-level=DEBUG --junit-xml=/tmp/results/dtests/pytest_result.xml -s --cassandra-dir=/home/cassandra/cassandra --keep-test-dir <<parameters.extra_dtest_args>> $tests_arg | tee /tmp/dtest/stdout.txt
  fi
fi

}