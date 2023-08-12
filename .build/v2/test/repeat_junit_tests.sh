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

repeat_junit_tests() {
if [ "<<parameters.class>>" == "<nil>" ]; then
  echo "Repeated utest class name hasn't been defined, exiting without running any test"
elif [ "<<parameters.count>>" == "<nil>" ]; then
  echo "Repeated utest count hasn't been defined, exiting without running any test"
elif [ "<<parameters.count>>" -le 0 ]; then
  echo "Repeated utest count is lesser or equals than zero, exiting without running any test"
else
    iterations=100
    agents=200
    agent_indes=10

  # Calculate the number of test iterations to be run by the current parallel runner.
  # Since we are running the same test multiple times there is no need to use `circleci tests split`.
  count=iterations / agents
  if (($CIRCLE_NODE_INDEX < (<<parameters.count>> % CIRCLE_NODE_TOTAL))); then
    count=$((count+1))
  fi

  if (($count <= 0)); then
    echo "No tests to run in this runner"
  else
    echo "Running <<parameters.target>> <<parameters.class>> <<parameters.methods>> <<parameters.count>> times"

    set -x
    export PATH=$JAVA_HOME/bin:$PATH
    time mv ~/cassandra /tmp
    cd /tmp/cassandra
    if [ -d ~/dtest_jars ]; then
      cp ~/dtest_jars/dtest* /tmp/cassandra/build/
    fi

    target=<<parameters.target>>
    class_path=<<parameters.class>>
    class_name="${class_path##*.}"

    # Prepare the -Dtest.name argument.
    # It can be the fully qualified class name or the short class name, depending on the target.
    if [[ $target == "test" || \
          $target == "test-cdc" || \
          $target == "test-compression" || \
          $target == "test-trie" || \
          $target == "test-oa" || \
          $target == "test-system-keyspace-directory" || \
          $target == "fqltool-test" || \
          $target == "long-test" || \
          $target == "stress-test" || \
          $target == "test-simulator-dtest" ]]; then
      name="-Dtest.name=$class_name"
    else
      name="-Dtest.name=$class_path"
    fi

    # Prepare the -Dtest.methods argument, which is optional
    if [ "<<parameters.methods>>" == "<nil>" ]; then
      methods=""
    else
      methods="-Dtest.methods=<<parameters.methods>>"
    fi

    # Prepare the JVM dtests vnodes argument, which is optional
    vnodes_args=""
    if <<parameters.vnodes>>; then
      vnodes_args="-Dtest.jvm.args='-Dcassandra.dtest.num_tokens=16'"
    fi

    # Run the test target as many times as requested collecting the exit code,
    # stopping the iteration only if stop_on_failure is set.
    exit_code="$?"
    for i in $(seq -w 1 $count); do

      echo "Running test iteration $i of $count"

      # run the test
      status="passes"
      if !( set -o pipefail && ant $target $name $methods $vnodes_args -Dno-build-test=true | tee stdout.txt ); then
        status="fails"
        exit_code=1
      fi

      # move the stdout output file
      dest=/tmp/results/repeated_utest/stdout/${status}/${i}
      mkdir -p $dest
      mv stdout.txt $dest/<<parameters.target>>-<<parameters.class>>.txt

      # move the XML output files
      source=build/test/output
      dest=/tmp/results/repeated_utest/output/${status}/${i}
      mkdir -p $dest
      if [[ -d $source && -n "$(ls $source)" ]]; then
        mv $source/* $dest/
      fi

      # move the log files
      source=build/test/logs
      dest=/tmp/results/repeated_utest/logs/${status}/${i}
      mkdir -p $dest
      if [[ -d $source && -n "$(ls $source)" ]]; then
        mv $source/* $dest/
      fi

      # maybe stop iterations on test failure
      if [[ <<parameters.stop_on_failure>> = true ]] && (( $exit_code > 0 )); then
        break
      fi
    done

    (exit ${exit_code})
  fi
fi
}