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
# This is the reference canonical "build verbs" for our CI system in C*.
#
# The implementations here represent the _correct_ way to run things. For a CI
# system to qualify as an approved system for precommit runs, the combination
# of precommit pipelines and jobs in cassandra_ci.yaml and the shell runtime
# here must be run to confirm that the tests as run in whatever environment will
# match what's run in our reference CI system.
#
# The methods here are kept private and separate from the ci_public_api.sh members
# to help reinforce the fact that things in this file should not be changed in any
# CI systems. If changes are needed (i.e. something is hardcoded or not required
# for run), they should be factored out and parameterized upstream in open-source.
##############################################################################

#-----------------------------------------------------------------------------
# The base reference configuration .yaml; shouldn't need to change this
DEFAULT_YAML=${DEFAULT_YAML:~"./cassandra_ci.yaml"}

# If you need to override params, prefer the following
YAML_OVERRIDES=${YAML_OVERRIDES:""}

source functions.sh

# Every job should confirm the environment and setup env vars from the provided .yaml before running
setup_environment() {
    check_command ant
    check_command gsplit
    check_command shuf
    check_command java

    if [[ $CI_AGENT_INDEX -le -1 ]]; then
        echo "CI_AGENT_INDEX is an invalid value: $CI_AGENT_INDEX. Cannot proceed." && exit 1
    fi

    if [[ $CI_AGENT_INDEX -le $CI_AGENT_COUNT ]]; then
        echo "CI_AGENT_INDEX ($CI_AGENT_INDEX) is invalid; must be <= CI_AGENT_COUNT ($CI_AGENT_COUNT). Cannot proceed." && exit 1
    fi

    if [[ -z $DEFAULT_YAML ]]; then
        echo "DEFAULT_YAML is not defined; cannot run CI" && exit 1
    fi

    # Initialize all our optional env vars to based on env, override, or default .yaml in that order
    for key in $(yq e '.default_env_vars | keys | .[]' "$DEFAULT_YAML"); do
        init_env_var "$key" "default_env_vars.$key"
    done

    # In case there's keys in the override that aren't in the base, double-check those
    for key in $(yq e '.default_env_vars | keys | .[]' "$YAML_OVERRIDES"); do
        init_env_var "$key" "default_env_vars.$key"
    done

    # There's some default env vars that we _require_ to be defined though their actual value is optional. Confirm.
    if [ -z "${JVM_URL}" ]; then echo "Cannot run without a defined JVM_URL. Add to default_env_vars in config yaml. Aborting." && exit 1; fi
    if [ -z "${JVM_BRANCH}" ]; then echo "Cannot run without a defined JVM_BRANCH. Add to default_env_vars in config yaml. Aborting." && exit 1; fi
    if [ -z "${NUM_TOKENS}" ]; then echo "Cannot run without a defined NUM_TOKENS. Add to default_env_vars in config yaml. Aborting." && exit 1; fi

    # For required parameters, we don't respect anything in an override file or in the local environment variables
    for key in $(yq e '.required_env_vars | keys | .[]' "$DEFAULT_YAML"); do
        local yaml_value=$(yq "required_env_vars.$key" "$DEFAULT_YAML")
        declare -g "$key=$yaml_value"
    done

    CLASSLIST_FILE_JAVA="${TMP_RUN_DIR}/java_testnames.txt"
    CLASSLIST_FILE_PYTHON_DTESTS="${TMP_RUN_DIR}/tmp.python_testnames.txt"
    REPEAT_TEST_LIST="${TMP_RUN_DIR}/repeat_testnames.txt"

    # Clear out any existing RESULTS_DIR
    rm -rf "${RESULTS_DIR}"
    mkdir -p "${RESULTS_DIR}"

    # This is pulled from the generate-test-report target in build.xml's fileset param
    PYTHON_RESULTS_DIR="$(_test_dir)/output"
    mkdir -p "$PYTHON_RESULTS_DIR"
    JVM_RESULTS_DIR="${RESULTS_DIR}/jvm_tests/"
    mkdir -p "$JVM_RESULTS_DIR"

    BUILD_XML="${CASSANDRA_DIR}/build.xml"

    _init_env_from_job
}

# A variety of metrics and versions we like to have documented on each build
log_environment() {
    _log_env_val "id"
    _log_env_val "cat /proc/cpuinfo"
    _log_env_val "free -m"
    _log_env_val "df -m"
    _log_env_val "ifconfig -a"
    _log_env_val "uname -a"
    _log_env_val "mount"
    _log_env_val "env"
    _log_env_val "ant -version"
    _log_env_val "git -version"
    _log_env_val "java -version"
    _log_env_val "javac -version"
}

# Clears out and resets temp, logs env vars, clones branch. This should be run before most suites.
# Note: The specific implementation here is subject to change in any given environment assuming you still log out the
# contents of the env and make sure a checkout of the branch under test exists after this method is run.
pretest_setup() {
    local dest=""
    if [ -z "$CASSANDRA_DIR" ]; then
        if [ -z "$1" ]; then
            echo "Cannot run pretest_setup without either an argument or CASSANDRA_DIR being defined. Aborting." && exit 1
        fi
        dest=$1
    else
        dest="$CASSANDRA_DIR"
    fi

    _reset_temp
    log_environment

    if [ -d "${dest}" ];then
        echo "Targeted cassandra dir [${dest}] already exists during pretest_setup. Aborting." && exit 1
    fi

    # Note: $JVM_URL will serve as our "origin" remote value
    _shallow_clone_branch "$JVM_URL" "$JVM_BRANCH" "$dest"

    # Clone our dtests if this is a python dtest
    if [ -n "${PYTHON_DTEST_DIR}" ]; then
        _shallow_clone_branch "$PYTHON_URL" "$PYTHON_BRANCH" "$PYTHON_DTEST_DIR"
    fi

    cd_with_check "$dest"
}

# Canonical reference for how we do our style checking
check_style() {
    cd_with_check "$CASSANDRA_DIR"
    ant -f "${CASSANDRA_DIR}/build.xml" check dependency-check
}

# Loops a few times to prevent mavent-ant-task download failures.
build_cassandra() {
    cd_with_check "$CASSANDRA_DIR"
    for x in $(seq 1 3); do
        ant clean realclean jar
        RETURN="$?"
        if [ "${RETURN}" -eq "0" ]; then
            break
        fi
    done

    # Exit, if we didn't build successfully
    if [ "${RETURN}" -ne "0" ]; then
        echo "Build failed with exit code: $RETURN" && exit ${RETURN}
    fi
}

# Builds dtest jars for input branches, ultimately copying them to $CASSANDRA_DIR/build where they're needed. We need to
# do this in a separate cloned repo as it wipes out the contents of the repo when switching branches and we need all .jar
# files combined in one location.
build_dtest_jars() {
    if [ ! -d "$DTEST_JAR_PATH" ]; then
        mkdir "$DTEST_JAR_PATH"
    fi

    cd_with_check "$TMP_RUN_DIR"
    _shallow_clone_branch "$JVM_URL" "$JVM_BRANCH" "dtest_build"
    cd_with_check "dtest_build"
    git remote add apache "$JVM_DTEST_REFERENCE_URL"
    IFS=' ' read -ra branches <<< "$SUPPORTED_CASSANDRA_VERSIONS"
    for branch in "${branches[@]}"; do
        git remote set-branches --add apache "$branch"
        git fetch --depth 1 apache "$branch"
        git checkout "$branch"
        git clean -fd
        loop_command _run_jvm_dtest_build 3 "cp build/dtest*.jar ${DTEST_JAR_PATH}" "Failed to build dtest branch $branch"
    done

    # Build the dtest-jar for the branch under test
    ant realclean
    git remote add to_test "$JVM_URL"
    git checkout "to_test/${JVM_BRANCH}"
    git clean -fd
    loop_command _run_jvm_dtest_build 3 "cp build/dtest*.jar ${DTEST_JAR_PATH}" "Failed to build dtest jar for active branch: $JVM_BRANCH"
    cp -r "${DTEST_JAR_PATH}/*" "${CASSANDRA_DIR}/build"
    ls -l "${CASSANDRA_DIR}/build"
}

# For a given target, run the tests associated with them. This covers both unit and distributed jvm-based tests.
run_jvm_tests() {
    local split_test_file=$(_jvm_test_split_for_agent)

    # If we don't have any work to do, bail out.
    if [ ! -f "$split_test_file" ]; then return; fi

    local suite_timeout=$(_timeout_for $TEST_TIMEOUT)
    if [ -z $suite_timeout ]; then
        suite_timeout=$(_timeout_for "test.timeout")
        echo "WARNING: Failed to parse test timeout for $TEST_TIMEOUT. Defaulting to test.timeout value in build.xml: ${suite_timeout}"
    fi

    _run_jvm_ant "$suite_timeout" "$split_test_file"
    _generate_test_report
}

# Generates diffs between BASE_* and checked out branch; the list of tests that comes out of this that are either
# modified or new are then repeated to hit the REPEAT_COUNT for the job type.
repeat_jvm_tests() {
    _generate_jvm_diff_tests
    count=$(_repeat_count)
    echo "Running $REPEAT_TEST_LIST ($(wc -l $REPEAT_TEST_LIST) tests) $count times"

    local suite_timeout=_timeout_for $TEST_TIMEOUT
    # Run the test target as many times as requested collecting the exit code, stopping the iteration only if
    # stop_on_failure is set. This is a dumb loop at this point but should be sufficient since we won't go through
    # the rebuild or check cycle on each run.
    local exit_code="$?"
    for i in $(seq -w 1 $count); do
        echo "Running test iteration $i of $count"

        # run the test
        status="passes"
        _run_jvm_ant "$suite_timeout" "$REPEAT_TEST_LIST" | tee stdout.txt
        if [ "$?" != 0 ]; then
            status="fails"
            exit_code=1
        fi

        # Collect results based on our status and run number
        dest="${RESULTS_DIR}/repeated_utest/stdout/${status}/${i}"
        mkdir -p $dest

        mv stdout.txt $dest/${ANT_TARGET}_stdout.txt
        mv build/test/output/* $dest/
        mv build/test/logs/* $dest/

        # maybe stop iterations on test failure
        if [[ "$REPEATED_TESTS_STOP_ON_FAILURE" = true ]] && (( $exit_code > 0 )); then
            break
        fi
    done
    _generate_test_report
}

# Runs the python dtests given the active PYTHON_VERSION
run_python_dtests() {
    _prep_virtualenv_and_deps
    split_test_file=$(_python_test_split_for_agent)

    # If we don't have any work to do, bail out.
    if [ ! -f "$split_test_file" ]; then return; fi

    set -e #enable immediate exit if venv setup fails
    _source_python_env
    cd_with_check "${DTEST_PYTHON_PATH}"

    _run_python_pytest
    _clean_python_test_results
    _generate_test_report
}

# Generates a diff from BASE_* to current branch under test, and based on that, repeats the list of tests REPEAT_COUNT
# number of times div the # of agents in the environment to make sure the tests are executed to the frequency expected.
repeat_python_dtests() {
    _generate_python_diff_tests
    count=$(_repeat_count)

    test_count=$(wc -l $REPEAT_TEST_LIST)
    echo "Running $REPEAT_TEST_LIST ($test_count unique tests)) $count times"
    _source_python_env
    cd_with_check ${PYTHON_DTEST_DIR}
    # We need to get the contents of our $REPEAT_TEST_LIST into the tests_arg var
    mapfile -t test_list < $REPEAT_TEST_LIST
    _run_python_pytest $count
}

# Canonical reference on how we run the pytest command
# $1 int: optional count to repeat tests; defaults to 1 if not provided
_run_python_pytest() {
    local count=1
    if [[ -n "$1" ]]; then count=$1; fi

    # we need the "set -o pipefail" here so that the exit code will be from pytest and not the code from tee
    set -o pipefail
    pytest \
        ${PYTEST_OPTS} \
        --cassandra-dir=${CASSANDRA_DIR} \
        --keep-failed-test-dir \
        --capture=no \
        --count=$count \
        ${DTEST_ARGS} \
        --log-level="DEBUG" \
        --junit-xml=${PYTHON_RESULTS_DIR}/nosetests.xml \
        ${split_test_file} 2>&1 \
        | tee -a ${PYTHON_RESULTS_DIR}/pytest_stdout.txt
}

# TODO:
#  - Pull in ASF CI impl and compare; merge as needed
#   - update .yaml file to point to this script for cmd instead of it's run target atm
run_simulator_tests() {
    local url=$(check_argument "$1" "the url of the repo to clone and build")
    local branch=$(check_argument "$2" "the branch to build")
    local dest=$(check_argument "$3" "destination directory to clone code to")

    pretest_setup "$url" "$branch" "$dest"
    _merge_runtime_environment "${CASSANDRA_DIR}"
    ant test-simulator-dtest -Dno-build-test=true
}

# #1 string: "true" if you want to use cython
run_cqlshlib_tests() {
    if [[ -n "$1" && "$1" == "true" ]]; then
        export cython="yes"
    fi

    pretest_setup
    _merge_runtime_environment "${CASSANDRA_DIR}"
    ant clean
    ant jar -Dno-checkstyle=true -Drat.skip=true -Dant.gen-doc.skip=true -Djavadoc.skip=true
    ./pylib/cassandra-cqlsh-tests.sh "$(pwd)"
}

# $1 string: ant target to run sequentially
run_target_sequentially() {
    local target=$(check_argument "$1" "ant target to run")
    ant "$target" -Dno-build-test=true
}

#-----------------------------------------------------------------------------
# PRIVATE FUNCTIONS
#
# Do not rely on these within "steps:" within a job configuration in the cassandra_ci.yaml
#-----------------------------------------------------------------------------
_init_env_from_job() {
    echo "TODO: Implement"
    # TODO: Implement.
    # 1. Pull env: values
    # 2. Pull env_override values
    # 3. Build out before, run, and after arrays from values on the job
}

_log_env_val() {
    local command=$(check_argument "$1" "command to log value")
    echo "*** ${command} ***"
    eval $command 2>&1
}

# Does a shallow clone of the specified repo / branch
_shallow_clone_branch() {
    local url=$(check_argument "$1" "url to clone")
    local branch=$(check_argument "$2" "branch to clone")
    local dest=$(check_argument "$3" "destination to clone the branch to")
    if [ -d "${dest}" ]; then
        echo "Cannot clone $url into dest: $dest. Destination already exists. Aborting." && exit 1
    fi
    git clone --single-branch --depth 1 --branch "$branch" "$url" "$dest"
}

# This code (along with all the steps) is expected to be independently executed on every agent. This relies on:
#   1: the TEST_DIR being defined as the subdirectory under $CASSANDRA_DIR/test/$TEST_DIR of the suite
#   2: an optional TEST_FILTER being defined as a filter to run the tests through before writing them to a file
_split_jvm_tests() {
    seed=_seed_for_test_sort
    find "${CASSANDRA_DIR}/test/${TEST_DIR}" -name "*Test.java" ${TEST_FILTER:-} | shuf --random-source=$seed >> ${CLASSLIST_FILE_JAVA}

    local msg="***${TEST_DIR} tests***"
    if [ -n "${TEST_FILTER}" ]; then
        msg="***${TEST_DIR} tests (filter=${TEST_FILTER})***"
    fi
    echo $msg
    cat "${CLASSLIST_FILE_JAVA}"

    local total_tests=$(wc -l < "${CLASSLIST_FILE_JAVA}")
    local lines_per_file=$((total_tests / "${CI_AGENT_COUNT}"))
    split -l $lines_per_file -d ${CLASSLIST_FILE_JAVA} "${TMP_RUN_DIR}/java_testnames_split_"
}

# Wraps up the splitting and retrieval of a specific agent's split file into one convenient accessor
_jvm_test_split_for_agent() {
    if [ ! -f "${CLASSLIST_FILE_JAVA}" ]; then
        _split_jvm_tests
    fi

    local split_test_file="${TMP_RUN_DIR}/java_testnames_split_${CI_AGENT_INDEX}"
    if [ ! -f "${split_test_file}" ]; then
        echo "Cannot find jvm split for agent ${CI_AGENT_INDEX} on job ${ANT_TARGET}; you may have more agents than splits. \
            Consider tuning your parallelism. \
            File not found: ${split_test_file}."
        echo ""
    fi
    cat "***Split file for agent ${CI_AGENT_INDEX}: $split_test_file. Test count: $(wc -l $split_test_file)***"
    echo "$split_test_file"
}

# Canonical reference on how we run ant for jvm-based tests. This specifically doesn't build the tests so expects them
# to be built prior to this run.
_run_jvm_ant() {
    local suite_timeout=$(check_argument "$1" "timeout for the test in ms")
    local test_file=$(check_argument "$2" "classlist file containing all the tests to run")

    ant "${ANT_TARGET}" "${RUN_ARGUMENTS}" \
            -Dtest.timeout="${suite_timeout}" \
            -Dtest.classlistfile="${split_test_file}" \
            -Dtest.classlistprefix="${TEST_CLASSLIST_PREFIX}" \
            -Dno-build-test=true \
        || echo "failed ${ANT_TARGET} ${split_test_file}"
}

# This is our canonical jvm dtest jar building set of commands
_build_jvm_dtest() {
    ant realclean
    ant jar dtest-jar -Dno-checkstyle=true -Drat.skip=true
}

# For the url, branch, and sha under test (JVM_*), we'll diff against the
# indicated BASE_* (url, branch), and calculate what tests have changed (either added
# or changed) between the two branches. That'll become our testclasslist that all agents
# receive, and the number of times those agents run those tests will be determined based
# on the CI_AGENT_COUNT vs. REPEAT_COUNT env (see _repeat_count)
_generate_jvm_diff_tests() {
    # Pull out only added, modified, or renamed files that differ between our base branch and patch for our active test suite
    diff=$(git --no-pager diff --name-only --diff-filter=AMR ${BASE_BRANCH}...HEAD ${TEST_DIR})
    tests=$(echo "$diff" | \
        grep ".java" | \                # only include .java files
        sed -e "s/\\.java}//" | \       # strip off file extension
        sed -e "s,^${TEST_DIR},," | \   # Strip off the leading path for the TEST_DIR, leaving just package
        tr '/' '.' | \                  # Turn / into .
        awk '{sub(/^\./, ""); print}')  # And strip off our leading . so we get a full package name
    echo $tests > $REPEAT_TEST_LIST
}

# This code (along with all the steps) is expected to be independently executed on every agent, so it needs to be deterministic
# for a given URL/BRANCH/SHA.
_split_python_tests() {
    cd_with_check "$CASSANDRA_DTEST_DIR"

    # TODO: Consider providing another mechanism for test sorting that is aware of the runtime of tests and buckets to hit an average target length
    cd_with_check ${CASSANDRA_DTEST_DIR}
    set -eo pipefail && \
        ./run_dtests.py "${DTEST_ARGS}" \
        --dtest-print-tests-only \
        --dtest-print-tests-output="${CLASSLIST_FILE_PYTHON_DTESTS}.RAW" \
        --cassandra-dir="${CASSANDRA_DIR}"

    # We shuffle the test list up so we don't have back-to-back lexicographical test runs
    seed=_seed_for_test_sort
    shuf --random-source=$seed "${CLASSLIST_FILE_PYTHON_DTESTS}.RAW" > "${CLASSLIST_FILE_PYTHON_DTESTS}"

    # Filter out anything the user's asking us to remove on this config
    if [ -n "$PYTHON_TEST_EXCLUSION_REGEX" ]; then
        grep -e "$PYTHON_TEST_EXCLUSION_REGEX" "$CLASSLIST_FILE_PYTHON_DTESTS}" >> "${CLASSLIST_FILE_PYTHON_DTESTS}.FILTERED"
        cat "${CLASSLIST_FILE_PYTHON_DTESTS}.FILTERED" > "${CLASSLIST_FILE_PYTHON_DTESTS}"
    fi

    local msg="***${CASSANDRA_DTEST_DIR} tests***"
    if [ -n "${PYTHON_TEST_EXCLUSION_REGEX}" ]; then
        msg="***${TEST_DIR} tests (filter=${PYTHON_TEST_EXCLUSION_REGEX})***"
    fi
    echo $msg
    cat "${CLASSLIST_FILE_PYTHON_DTESTS}"

    local total_tests=$(wc -l < "${CLASSLIST_FILE_PYTHON_DTESTS}")
    local lines_per_file=$((total_tests / "${CI_AGENT_COUNT}"))
    split -l $lines_per_file -d ${CLASSLIST_FILE_PYTHON_DTESTS} "${TMP_RUN_DIR}/python_testnames_split_"
}

# Wraps up the splitting and retrieval of a specific agent's split file into one convenient accessor
_python_test_split_for_agent() {
    if [ ! -f "${CLASSLIST_FILE_PYTHON_DTESTS}" ]; then
        _split_python_tests
    fi

    local split_test_file="${TMP_RUN_DIR}/python_testnames_split_${CI_AGENT_INDEX}"
    if [ ! -f "${split_test_file}" ]; then
        echo "Cannot find python dtest split for agent ${CI_AGENT_INDEX}; you may have more agents than splits. \
            Consider tuning your parallelism. \
            File not found: ${split_test_file}."
        echo ""
    fi
    _log_env_val "cat ${split_test_file}"
    echo "$split_test_file"
}

# See _generate_jvm_diff_tests. Substitute python.
_generate_python_diff_tests() {
    cd_with_check ${PYTHON_DTEST_DIR}
    # Pull out only added, modified, or renamed files that differ between our base branch and patch for our active test suite
    diff=$(git --no-pager diff --name-only --diff-filter=AMR ${BASE_BRANCH}...HEAD .)
    tests=$(echo "$diff" | \
            grep ".py" | \        # only include .py files
            sed -e "s/\\\.//g")   # Strip off the leading path, leaving just python file name
    echo $tests > "$REPEAT_TEST_LIST"
}

# Nukes a venv if we have it, copying over an existing fresh one if it's there or setting up a new one. Then runs through
# what in requirements.txt from the cassandra-dtest dir in case something's been added there if things are pre-baked into
# a docker image.
_prep_virtualenv_and_deps() {
    confirm_directory_exists "${PYTHON_DTEST_DIR}"
    rm -rf "${RESULTS_DIR}/venv"
    virtualenv-clone "${PYTHON_ENV_PREFIX}${PYTHON_VERSION}" ${RESULTS_DIR}/venv || virtualenv --python=python${PYTHON_VERSION} ${RESULTS_DIR}/venv
    source "${RESULTS_DIR}/venv/bin/activate"
    pip3 install --exists-action w --upgrade -r "${PYTHON_DTEST_DIR}/requirements.txt"
    pip3 uninstall -y cqlsh
    pip3 freeze
}

# We don't want lexicographical sorting on tests names, but we _do_ want reproducibility on the ordering of our test run.
# As a balance, we rely on the md5 of build.xml as we don't expect it to change that frequently, so we hopefully get the
# best of both worlds; some stability in the randomization of test ordering, but also reproducibility of a specific
# sha for a branch under test.
# TODO: Consider providing a test_seed file with some randomized contents we can use as input to shuf to get full stability
_seed_for_test_sort() {
    if [ ! -f "${CASSANDRA_DIR}/build.xml" ]; then
        echo "Cannot find build.xml in $CASSANDRA_DIR; cannot generate md5 to seed our test list sort. Aborting."
        exit 1
    fi
    md5 -q "${CASSANDRA_DIR}/build.xml"
}

# For a given command, loops the provided number of times and notifies on return success or failure
_loop_command() {
    local func=$(check_argument "$1" "string command to repeat")
    local retries=$(check_argument "$2" "number of times to retry command")
    local success_func=$(check_argument "$3" "code to eval on success")
    local error_message=$(check_argument "$4" "error message for command failure case")

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
        echo "$error_message: ${RETURN}" && exit ${RETURN}
    fi
}

# Pulls the timeout value in ms from build.xml in the cassandra_dir
# return value is a string expected to be captured in $()
_timeout_for() {
    local param=$(check_argument "$1" "full N.timeout string value to parse a timeout for out of $BUILD_XML")
    retrieve_value_from_xml "$param" "$BUILD_XML"
}

# TODO: Look into this bash var name expansion from what's coming from the .xml. Probably not working here.
_test_dir() {
    local base_dir=retrieve_value_from_xml "basedir" "$BUILD_XML"
    local build_dir=retrieve_value_from_xml "build.dir" "$BUILD_XML"
    local test_dir=retrieve_value_from_xml "test.dir" "$BUILD_XML"
    echo "test_dir: $test_dir"
    echo ${!test_dir}
}

# Blasts out whatever the env has set for tmp dir; don't want build data to accumulate over time
_reset_temp() {
    rm -rf "${CASSANDRA_CI_TMP_ROOT}"
    mkdir -p "${CASSANDRA_CI_TMP_ROOT}"
    TMP_RUN_DIR=$(mktemp -d ${CASSANDRA_CI_TMP_ROOT}/build.XXXX)
}

_repeat_count() {
    repeat_local=$REPEAT_COUNT / CI_AGENT_COUNT
    if [ $repeat_local -eq 0 ]; then
        echo "WARNING: repeat count is 0. Agent count: $CI_AGENT_COUNT, repeat count: $REPEAT_COUNT. Setting to 1."
        repeat_local=1
    fi
    echo $repeat_local
}

_source_python_env() {
    source "${PYTHON_ENV_PREFIX}${PYTHON_VERSION}/bin/activate"
}

_clean_python_test_results() {
    # remove <testsuites> wrapping elements. `ant generate-unified-test-report` doesn't like it`
    pytest_junit="${PYTHON_RESULTS_DIR}/nosetests.xml"
    if [ ! -f "$pytest_junit" ]; then
        echo "Cannot find $pytest_junit. Cannot generate test report."
        return
    fi
    sed -r "s/<[\/]?testsuites>//g" "$pytest_junit" > "$pytest_junit.cleaned"
    cat "$pytest_junit.cleaned" > _test_dir "$pytest_junit"
}

_generate_test_report() {
    # merge all unit xml files into one, and print summary test numbers
    ant -quiet -silent generate-unified-test-report
}
