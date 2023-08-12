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

# Top level TODO:
#   - Bring in ASF CI impl (is there one?)
#   - See what in here needs to be cleaned up
#   - test and wire up call from cassandra_ci.yaml

# $1 string: junit target to repeat
# $2 int: iterations to repeat
# $3 int: total executor count. Used to calculate how many repetitions we should do
# $4 string: test file listing all tests
repeat_new_or_modified_junit() {
    check_argument "$1" "Missing param 1: target"
    check_argument "$2" "Missing param 2: iterations"
    check_argument "$3" "Missing param 3: executors"
    check_argument "$4" "Missing param 4: test_file"

    target=$1
    count=$2
    executors=$3
    test_file=$4

    set -x
    export PATH=$JAVA_HOME/bin:$PATH
    time mv ~/cassandra /tmp
    cd /tmp/cassandra
    if [ -d ~/dtest_jars ]; then
      cp ~/dtest_jars/dtest* /tmp/cassandra/build/
    fi

    # Calculate the number of test iterations to be run by the current parallel runner.
    count=$count / $executors
    if [ $count -eq 0 ]; then
        count=1;
    fi

    # Put manually specified tests and automatically detected tests together, removing duplicates
    tests=$(echo ${test_file} | sed -e "s/<nil>//" | sed -e "s/ //" | tr "," "\n" | tr " " "\n" | sort -n | uniq -u)
    echo "Tests to be repeated: ${tests}"

    # Prepare the JVM dtests vnodes argument, which is optional.
    vnodes_args=""
    if [[ ${NUM_TOKENS} -ne 0 ]] ; then
      vnodes_args="-Dtest.jvm.args='-Dcassandra.dtest.num_tokens=${num_tokens}'"
    fi

    # Prepare the testtag for the target, used by the test macro in build.xml to group the output files
    testtag=""
    if [[ $target == "test-cdc" ]]; then
      testtag="cdc"
    elif [[ $target == "test-compression" ]]; then
      testtag="compression"
    elif [[ $target == "test-system-keyspace-directory" ]]; then
      testtag="system_keyspace_directory"
    elif [[ $target == "test-trie" ]]; then
      testtag="trie"
    elif [[ $target == "test-oa" ]]; then
      testtag="oa"
    fi

    # Run each test class as many times as requested.
    exit_code="$?"
    for test in $tests; do

        # Split class and method names from the test name
        if [[ $test =~ "#" ]]; then
          class=${test%"#"*}
          method=${test#*"#"}
        else
          class=$test
          method=""
        fi

        # The build.xml targets for test and test-jvm subsets differ in their naming from base test commands.
        # TODO: Consider unifying those so the same test target in build.xml can run either
        ant_testsome=$target
        if [[ "$ant_testsome" -eq "test" ]]; then ant_testsome="testsome"; fi
        if [[ "$ant_testsome" -eq "test-jvm" ]]; then ant_testsome="test-jvm-dtest-some"; fi

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
          name_arg="-Dtest.name=${class##*.}"
        else
          name_arg="-Dtest.name=$class"
        fi

        # Prepare the -Dtest.methods argument, which is optional
        if [[ $method == "" ]]; then
          methods_arg=""
        else
          methods_arg="-Dtest.methods=$method"
        fi

        for i in $(seq -w 1 $count); do
          echo "Running test $test, iteration $i of $count"

          # run the test
          status="passes"
          if ! ( set -o pipefail && \
                ant "${ant_testsome}" $name_arg $methods_arg $vnodes_args -Dno-build-test=true | \
                tee stdout.txt \
              ); then
            status="fails"
            exit_code=1
          fi

          # move the stdout output file
          dest=/tmp/results/repeated_utests/stdout/${status}/${i}
          mkdir -p $dest
          mv stdout.txt $dest/${test}.txt

          # move the XML output files
          source=build/test/output/${testtag}
          dest=/tmp/results/repeated_utests/output/${status}/${i}
          mkdir -p $dest
          if [[ -d $source && -n "$(ls $source)" ]]; then
            mv $source/* $dest/
          fi

          # move the log files
          source=build/test/logs/${testtag}
          dest=/tmp/results/repeated_utests/logs/${status}/${i}
          mkdir -p $dest
          if [[ -d $source && -n "$(ls $source)" ]]; then
            mv $source/* $dest/
          fi

          # maybe stop iterations on test failure

          if [[ "${REPEATED_TESTS_STOP_ON_FAILURE}" = true ]] && (( $exit_code > 0 )); then
            break
          fi
        done
    done
    (exit ${exit_code})
}