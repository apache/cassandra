#!/usr/bin/env bash
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

source assert.sh

##############################################################################
# Helper functions for use in our build scripting
##############################################################################

#-----------------------------------------------------------------------------
# TOP LEVEL OPTIONAL ENV VARS
#
# DTEST_JAR_PATH: directory that may contain ${DTEST_DIR}/dtest* to copy into the C* build if they're needed
# PYTHON_VERSION: 3.8, 3.11, etc. Needed if running python dtests.
#
#
# ENVIRONMENTAL ASSUMPTIONS
# ~/cassandra-dtest exists if you're running dtests

# Top level TODO:
#   - Add xml parsing functionality as a first-class citizen
#   - Parse out properties we'll need from build.xml authoritatively (test.unit.src, timeouts, etc)
#   - A broad testing and polish pass for each of these individual methods; confirm their functionality locally
#   - Consider adding a top level env sanity check we run that checks the above things when sourced and fails out if something's missing

# $1 int: count expected
# $2 int: count actual
confirm_argument_count() {
    if [ "$#" -ne 2 ]; then
        echo "Must pass 2 arguments to ${FUNCNAME[0]}. Error in ${FUNCNAME[1]}"
        exit 1
    fi
    assert_equals $1 $2 "Unexpected number of arguments passed in to ${FUNCNAME[1]}"
}

# Confirm that a given variable exists
# $2: Message to print on error
# $1: Variable to check for definition
# return: string echo of input variable
check_argument() {
    if [ $# != 2 ]; then
        echo "Invalid call to check_argument in ${FUNCNAME[1]}. Expect message and variable to check."
        exit 1
    fi

    if [ -z "$1" ]; then
        echo "$2"
        exit 1
    fi

    echo "$1"
}

# Confirm a given file exists on disk; error out if it doesn't
# $1: Path to check
confirm_file_exists() {
    local file=$(check_argument "$1" "Must provide file name to check")
    if [ ! -f "$file" ]; then
        echo "File does not exist: ${file}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

# Confirm a given directory exists on disk; error out if it doesn't
# $1: Path to check
confirm_directory_exists() {
    local dir=$(check_argument "$1" "Must provide directory name to check")
    if [ ! -d "$dir" ]; then
        echo "Directory does not exist: ${dir}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

confirm_directory_does_not_exist() {
    local dir=$(check_argument "$1" "Must provide directory name to check")
    if [ -d "$dir" ]; then
        echo "Directory already exists: ${dir}. Cannot proceed (caller: ${FUNCNAME[1]})"
        exit
    fi
}

debug_log() {
    if [ -n "$DEBUG" ]; then
        echo "DEBUG: $1"
    fi
}

# Prints out detailed log information about the CI env
log_environment() {
    echo '*** id ***'
    id
    echo '*** cat /proc/cpuinfo ***'
    cat /proc/cpuinfo
    echo '*** free -m ***'
    free -m
    echo '*** df -m ***'
    df -m
    echo '*** ifconfig -a ***'
    ifconfig -a
    echo '*** uname -a ***'
    uname -a
    echo '*** mount ***'
    mount
    echo '*** env ***'
    env
    echo '*** java ***'
    which java
    java -version
}

# Does a shallow clone of the specified repo / branch
# $1 url of repo
# $2 branch name
# $3 dest dir to clone to
#
# TODO:
#   - Pull in ASF CI impl; diff and test; merge
shallow_clone_branch() {
    local url=$(check_argument "$1" "Must provide a url to clone")
    local branch=$(check_argument "$2" "Must provide a branch to clone")
    local dest=$(check_argument "$3" "Must provide a destination to clone the branch to")
    git clone --single-branch --depth 1 --branch "$branch" "$url" "$dest"
}

# Loops a few times to prevent mavent-ant-task download failures.
# Note: requires ant in the path
#
# TODO:
#   - Pull in ASF CI impl; diff and test; merge
build_cassandra() {
    export PATH=$JAVA_HOME/bin:$PATH
    for x in $(seq 1 3); do
        ant clean realclean jar
        RETURN="$?"
        if [ "${RETURN}" -eq "0" ]; then
            break
        fi
    done

    # Exit, if we didn't build successfully
    if [ "${RETURN}" -ne "0" ]; then
        echo "Build failed with exit code: ${RETURN}"
        exit ${RETURN}
    fi
}

# TODO:
#   - Pull in ASF CI impl; diff and test; merge
run_jvm_dtest_build() {
    ant realclean
    ant jar dtest-jar -Dno-checkstyle=true -Drat.skip=true
}

# Builds dtest jars for input branches, storing then in DTEST_JAR_PATH
# $1 [] of reference branches to build
# $2 string url where branch under test can be found
# $2 string: name of branch to test
# $3 string: destination directory to place dtest jars
#
# Requires "ant" in the path
#
# TODO:
#   - Pull in ASF CI impl and diff to this; blend
build_dtest_jars() {
    local branches=("${@:1}")
    if [ -v branches ]; then
        echo "Must provide branches to build_dtest_jars."
        exit 1
    fi
    local test_url=$(check_argument "$2" "Must provide a url for the branch under test")
    local user_branch=$(check_argument "$3" "Must provide a user branch to build dtest jars for")
    local build_dest=$(check_argument "$4" "Must provide destination directory to build and place dtest jars")

    if [ ! -d "$DTEST_JAR_PATH" ]; then
        mkdir "$DTEST_JAR_PATH"
    fi
    cd "$DTEST_JAR_PATH"

    export PATH=$JAVA_HOME/bin:$PATH
    mkdir "$build_dest"
    git remote add apache https://github.com/apache/cassandra.git
    for branch in "${branches[@]}"; do
        # check out the correct cassandra version:
        git remote set-branches --add apache "$branch"
        git fetch --depth 1 apache "$branch"
        git checkout "$branch"
        git clean -fd
        loop_command run_jvm_dtest_build 3 "cp build/dtest*.jar ~/dtest_jars" "Failed to build dtest branch $branch"
    done

    # Build the dtest-jar for the branch under test
    ant realclean
    git checkout origin/$user_branch
    git clean -fd
    loop_command run_jvm_dtest_build 3 "cp build/dtest*.jar ~/dtest_jars" "Failed to build dtest jar for active branch: $user_branch"
    ls -l "${DTEST_JAR_PATH}/dtest_jars"
}

# $1 string: directory to run static analysis on
#
# TODO:
#   - Pull in ASF CI impl; merge
#   - Should this even be it's own target or can we just ditch it?
#       If this doesn't mutate / change much... /shrug
run_static_analysis() {
    local check_dir=$(check_argument "$1" "Must provide directory to provide static analysis against.")
    export PATH=$JAVA_HOME/bin:$PATH
    ant check
}

# For a given target, run the unit tests associated with them.
# This covers the "test" target and its permutations, not in-jvm nor python dtests
#
# $1 string: target name to run. This is the ant -p output, target name= from build.xml
# $2 string: target directory name to build, relative to current path. This will be moved to /tmp/$2 if it already exists
# $3 string[]: array of string arguments to pass to the build command
# $4 int: numeric index of this agent. Used during parallelization to determine which split subset to run post split
# $5 string: classlist prefix. Valid values are in <target name="testclasslist-{*}> tags.
#       oa, compression, trie, cdc, etc
# TODO:
#   - Make dtest_jar source dir a param / runtime configurable
#   - Pull in ASF CI impl and compare; merge different functionality as needed
#   - Make build.xml authoritative on timeout and target -> ant -p target translation (i.e. remove classlist prefix requirement)
run_unit_suite() {
    local ant_target=$(check_argument "$1" "Must provide a target name from build.xml to build.")
    local target_dir=$(check_argument "$2" "Must provide a target directory in which to build.")
    local arguments=("${@:3}")
    if [ -v arguments ]; then
        echo "Must provide arguments for unit test execution."
        exit 1
    fi
    local agent_index=$(check_argument "$4" "Must provide an integer index for this host to subselect a split from tests.")
    local classlist_prefix=$(check_argument "$5" "Must provide a classlist file name to sub-select tests from.")

    # build.xml remains authoritative for our timeouts
    test_timeout=$(grep 'name="test.${classlist_prefix}.timeout"' build.xml | awk -F'"' '{print $4}' || true)
    if [ -z "$test_timeout" ]; then
        test_timeout=$(grep 'name="test.timeout"' build.xml | awk -F'"' '{print $4}')
    fi
    cmd="ant ${ant_target} ${arguments} \
        -Dtest.timeout=${test_timeout} \
        -Dtest.classlistfile=/tmp/java_tests_${agent_index}_final.txt \
        -Dtest.classlistprefix=${classlist_prefix} \
        -Dno-build-test=true"
    prep_cassandra_dir_and_run "$cassandra_dir" "$cmd"
}

# Many of our tests require the cassandra dir to be moved to /tmp, combined with dtest jars, and then
# execute a simple command line. That muscle is factored out here to prevent duplication and ease maintenance.
#
# $1 string: relative path for cassandra dir w/built repo
# $2 string: command to run
prep_cassandra_dir_and_run() {
    local cassandra_dir=$(check_argument "$1" "Must provide a local cassandra directory.")
    confirm_directory_exists "$cassandra_dir"

    local cmd_to_run=$(check_argument "$2" "Must provide a command to evaluate.")

    tmp_dir="/tmp/${cassandra_dir}"
    mv "$cassandra_dir" "$tmp_dir"
    cd "$tmp_dir" || exit 2

    if [ -n "$DTEST_JAR_PATH" ]; then
        cp "${DTEST_JAR_PATH}/dtest*" "${tmp_dir}/build/"
    fi

    eval "$cmd_to_run"
}

# $1 string: cassandra directory where artifacts are built and where to run tests
# TODO:
#  - Pull in ASF CI impl and compare; merge as needed
#   - update .yaml file to point to this script for cmd instead of it's run target atm
run_simulator_tests() {
    confirm_argument_count 1 "$#"
    cmd="ant test-simulator-dtest -Dno-build-test=true"
    prep_cassandra_dir_and_run "$1" "$cmd"
}

# $1 string: cassandra directory where things are built and where to run tests
run_cqlsh_tests() {
    confirm_argument_count 1 "$#"
    cmd="ant jar -Dno-checkstyle=true -Drat.skip=true -Dant.gen-doc.skip=true -Djavadoc.skip=true &&\
        ./pylib/cassandra-cqlsh-tests.sh $(pwd)"
    prep_cassandra_dir_and_run "$1" "$cmd"
}

# Docker images or other build image are expected to provide all of these; this is a failsafe to
# make sure the python env is what's needed if there's changes and images aren't updated.
prep_virtualenv_and_deps() {
    confirm_directory_exists "$HOME/cassandra-dtest"
    pip3 install --exists-action w --upgrade -r ~/cassandra-dtest/requirements.txt
    pip3 uninstall -y cqlsh
    pip3 freeze
}

# This code (along with all the steps) is expected to be independently executed on every agent.
# Each agent needs to have an integer index at runtime to know which number in the runtime it is so it
# can determine which subset of tests to select and run locally.
#
# $1 string: Directory with cassandra-dtest built
# $2 string: classlist prefix. Valid values are in <target name="testclasslist-{*}> tags.
# $3 (optional) string: Any extra filtering needed to prune out tests
#
# TODO:
#   - ... why do we rm -fr the upgrade_tests here on circleci's config? See config_template.yml command
#   - Look up ASF CI; merge and integrate any requirements here
#       We're not going to be able to rely on the circle logic for test splitting as it relies on circleci's cmdline application
#       Well, actually hold that thought. The CLI is MIT licensed: https://github.com/CircleCI-Public/circleci-cli/blob/develop/LICENSE
#           so we _could_ use it authoritatively for splits. Let's keep that in mind.
generate_java_test_splits() {
    local dtest_dir=$(check_argument "$1" "Must provide cassandra-dtest location")
    local classlist_prefix=$(check_argument "$2" "Must provide a classlist_prefix")
    local extra_filtering=""

    # extra filtering is optional
    if [ "$#" -eq 3 ]; then
        extra_filtering="$3"
    fi

    # TODO: Keep or remove this? This is in the circle yaml
    # rm -fr ~/cassandra-dtest/upgrade_tests
    echo "***java tests***"

    # get all of our unit test filenames
    # Circle impl. We obviously won't rely on that here w/out circleci command
    # set -eo pipefail && circleci tests glob "$HOME/cassandra/test/<<parameters.classlistprefix>>/**/*.java" > /tmp/all_java_unit_tests.txt
    # TODO: Impl. Take from ASF CI or take from... somewhere else?

    # split up the unit tests into groups based on the number of containers we have
    # set -eo pipefail && circleci tests split --split-by=timings --timings-type=filename --index=${CIRCLE_NODE_INDEX} --total=${CIRCLE_NODE_TOTAL} /tmp/all_java_unit_tests.txt > /tmp/java_tests_${CIRCLE_NODE_INDEX}.txt
    # set -eo pipefail && cat /tmp/java_tests_${CIRCLE_NODE_INDEX}.txt |\
    #     sed "s;^/home/cassandra/cassandra/test/<<parameters.classlistprefix>>/;;g" | grep "Test\.java$" <<parameters.extra_filters>> > /tmp/java_tests_${CIRCLE_NODE_INDEX}_final.txt

    # TODO: We likely don't need the agent_index here
    # echo "** /tmp/java_tests_${agent_index}_final.txt"
    # cat /tmp/java_tests_${agent_index}_final.txt
}

# This code (along with all the steps) is independently executed on every agent
# so the goal here is to get the script to return the tests *this* container will run
#
# $1 (optional) [] of strings of args to append
generate_python_test_splits() {
    confirm_directory_exists "~/cassandra-dtest"
    cd ~/cassandra-dtest

    confirm_file_exists "~/env${PYTHON_VERSION}/bin/activate"
    source ~/env${PYTHON_VERSION}/bin/activate

    local extra_env_args=()
    if [ "$#" -ne 0 ]; then
        export "${@:1}"
    fi

    export PATH=$JAVA_HOME/bin:$PATH

    echo "***Collected DTests***"

    # Circle:
    # set -eo pipefail && ./run_dtests.py <<parameters.run_dtests_extra_args>> --dtest-print-tests-only --dtest-print-tests-output=/tmp/all_dtest_tests_<<parameters.file_tag>>_raw --cassandra-dir=../cassandra
    # if [ -z '<<parameters.tests_filter_pattern>>' ]; then
    # mv /tmp/all_dtest_tests_<<parameters.file_tag>>_raw /tmp/all_dtest_tests_<<parameters.file_tag>>
    # else
    # grep -e '<<parameters.tests_filter_pattern>>' /tmp/all_dtest_tests_<<parameters.file_tag>>_raw > /tmp/all_dtest_tests_<<parameters.file_tag>> || { echo "Filter did not match any tests! Exiting build."; exit 0; }
    # fi
    # set -eo pipefail && circleci tests split --split-by=timings --timings-type=classname /tmp/all_dtest_tests_<<parameters.file_tag>> > /tmp/split_dtest_tests_<<parameters.file_tag>>.txt
    # cat /tmp/split_dtest_tests_<<parameters.file_tag>>.txt | tr '\n' ' ' > /tmp/split_dtest_tests_<<parameters.file_tag>>_final.txt
    # cat /tmp/split_dtest_tests_<<parameters.file_tag>>_final.txt

    # ASF:
    # ./run_dtests.py --cassandra-dir=${CASSANDRA_DIR} ${DTEST_ARGS} --dtest-print-tests-only --dtest-print-tests-output=${DIST_DIR}/test_list.txt 2>&1 > ${DIST_DIR}/test_stdout.txt
    # PYTEST_OPTS="-vv --log-cli-level=DEBUG --junit-xml=${DIST_DIR}/test/output/nosetests.xml --junit-prefix=${DTEST_TARGET} -s"
    # pytest ${PYTEST_OPTS} --cassandra-dir=${CASSANDRA_DIR} --keep-failed-test-dir ${DTEST_ARGS} ${SPLIT_TESTS} 2>&1 | tee -a ${DIST_DIR}/test_stdout.txt
}

# Lists out all the files matching *Test.java in the given input directory.
# This is the authoritative way we generate a list of all valid tests for a given suite.
#
# $1 string: cassandra directory in which to search
# $2 string: suite name / directory name to search. Will look in $root/test/$suite
list_tests() {
    local cassandra_directory=$(check_argument "$1" "Must provide cassandra directory to enumerate tests.")
    if [ -d "${cassandra_directory}" ]; then echo "Cannot find ${cassandra_directory}; cannot enumerate tests." && exit 1; fi

    local suite_name=$(check_argument "$2" "Must provide a suite name to enumerate test files.")
    local combined_test_dir="${cassandra_directory}/tests/${suite_name}"
    if [ -d "${combined_test_dir}" ]; then echo "Cannot find ${combined_test_dir}; cannot enumerate tests." && exit 1; fi

    # TODO: Parse out the testmacrohelper from build.xml for a given macrodef name="testlist-*", so we can use build.xml as our authoritative way to list out tests
    # Until this is done, anyone who's listing out tests in a CI system should make sure they're matching the dir, timeout, and exclude= filters for the suite from build.xml
}

# For a given command, loops the provided number of times and notifies on return success or failure
# $1 string: code to eval repeatedly
# $2 int: number of times to repeat
# $3 string: code to eval on success
# $4 string: failure message to print on error
loop_command() {
    local func=$(check_argument "$1" "Must provide a string command to execute")
    local retries=$(check_argument "$2" "Must provide number of times to retry command")
    local success_func=$(check_argument "$3" "Must provide func name to run on success")
    local error_message=$(check_argument "$4" "Must provide error message for command failure case")

    for x in $(seq 1 $retries); do
        eval "$func"
        RETURN="$?"
        if [ "${RETURN}" -eq "0" ]; then
            eval "$success_func"
            break
        fi
    done

    # Exit, if we didn't build successfully
    if [ "${RETURN}" -ne "0" ]; then
        echo "$error_message: ${RETURN}"
        exit ${RETURN}
    fi
}