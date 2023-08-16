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
# [ENVIRONMENT VARIABLES]
# See cassandra_ci.yaml:*default_env_vars and cassandra_ci.yaml:*required_env_vars for available env vars in build scripts
# You can provide a space delimited list of dtest test branches in your env and job, otherwise we'll default to supported branches
if [ -z "$DTEST_TEST_BRANCHES" ]; then
    DTEST_TEST_BRANCHES=("${SUPPORTED_BRANCHES[@]}")
else
    IFS=' ' read -ra DTEST_TEST_BRANCHES <<< "$DTEST_TEST_BRANCHES"
fi

# The base reference configuration .yaml; shouldn't need to change this
CASSANDRA_CI_YAML=${CASSANDRA_CI_YAML:~"./cassandra_ci.yaml"}

# If you need to override params, prefer the following
CASSANDRA_CI_YAML_OVERRIDES=${CASSANDRA_CI_YAML_OVERRIDES:""}

# Top level TODO:
#   - Add xml parsing functionality as a first-class citizen
#   - Parse out properties we'll need from build.xml authoritatively (test.unit.src, timeouts, etc)
#   - A broad testing and polish pass for each of these individual methods; confirm their functionality locally
#   - Consider adding a top level env sanity check we run that checks the above things when sourced and fails out if something's missing
#   - Think through how to translate the "where to store artifacts" stuff from circle
#       IF this is something we want or need to do. Is it circle specific? Should it be here in this .sh?
#
# Low priority TODO:
#   - Do we need this from run-tests.sh?
#       cheap trick to ensure dependency libraries are in place. allows us to stash only project specific build artifacts.
#       ant -quiet -silent resolver-dist-lib
#   - Can we leverage the ant generate-unified-test-report? in run-tests.sh
#       ant -quiet -silent generate-unified-test-report
#   - ANT_OPTS is set in docker run-tests.sh but doesn't look to be referenced anywhere oddly.
#           ANT_OPTS="-Dtesttag.extra=.arch=$(arch).python${python_version}"

# Top Level DONE:
#   + Move non-ci specific functions into functions.sh

source functions.sh

# Every job should confirm the environment and setup env vars from the provided .yaml before running
setup_environment() {
    if [[ -z $CASSANDRA_CI_YAML ]]; then
        echo "CASSANDRA_CI_YAML is not defined; cannot run CI"
        exit 1
    fi

    init_env_var "$URL_TO_TEST" "repos.cassandra.url"
    init_env_var "$BRANCH_TO_TEST" "repos.cassandra.url"
    init_env_var "$NUM_TOKENS" "repos.cassandra.url"

    # Initialize all our optional env vars to based on env, override, or default .yaml in that order
    for key in $(yq e '.default_env_vars | keys | .[]' "$CASSANDRA_CI_YAML"); do
        init_env_var "$key" "default_env_vars.$key"
    done

    # In case there's keys in the override that aren't in the base, double-check those
    for key in $(yq e '.default_env_vars | keys | .[]' "$CASSANDRA_CI_YAML_OVERRIDES"); do
        init_env_var "$key" "default_env_vars.$key"
    done

    # For required parameters, we don't respect anything in an override file or in the local environment variables
    for key in $(yq e '.required_env_vars | keys | .[]' "$CASSANDRA_CI_YAML"); do
        local yaml_value=$(yq "required_env_vars.$key" "$CASSANDRA_CI_YAML")
        declare -g "$key=$yaml_value"
    done

    CLASSLIST_FILE_JAVA="${CASSANDRA_CI_TMP_DIR}/tmp.java_classlist_full.txt"
    CLASSLIST_FILE_PYTHON_DTESTS="${CASSANDRA_CI_TMP_DIR}/tmp.python_dtest_classlist_full.txt"
}

# A variety of metrics and versions we like to have documented on each build
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
    echo '*** ant ***'
    ant -version
    echo '*** git ***'
    git -version
    echo '*** java ***'
    which java
    java -version 2>&1
    javac -version 2>&1
}

# Clones branch, builds, and performs static analysis. This should be run before most suites.
# TODO:
#   - Should we clear before each run? Is there an argument for persistence?
pretest_setup() {
    local dest=${${DIST_DIR}:~$(check_argument "$3" "destination directory to clone code to")}

    _clear_temp_dir
    log_environment
    _shallow_clone_branch "$url" "$branch" "$dest"
    cd "$dest" || exit 2
    _build_cassandra
    ant -f "${CASSANDRA_DIR}/build.xml" check dependency-check
}

# Builds dtest jars for input branches, storing then in DTEST_JAR_PATH
# $1 string url where branch under test can be found
# $2 string: name of branch to test
#
# Requires "ant" in the path
#
# TODO:
#   - Pull in ASF CI impl and diff to this; blend
build_dtest_jars() {
    local test_url=$(check_argument "$1" "url for the branch under test")
    local user_branch=$(check_argument "$2" "user branch to build dtest jars for")

    if [ ! -d "$DTEST_JAR_PATH" ]; then
        mkdir "$DTEST_JAR_PATH"
    fi
    cd "$DTEST_JAR_PATH" || exit 2

    export PATH=$JAVA_HOME/bin:$PATH
    mkdir "$build_dest"
    git remote add apache https://github.com/apache/cassandra.git
    for branch in "${branches[@]}"; do
        # check out the correct cassandra version:
        git remote set-branches --add apache "$branch"
        git fetch --depth 1 apache "$branch"
        git checkout "$branch"
        git clean -fd
        loop_command _run_jvm_dtest_build 3 "cp build/dtest*.jar ~/dtest_jars" "Failed to build dtest branch $branch"
    done

    # Build the dtest-jar for the branch under test
    ant realclean
    git checkout "origin/${user_branch}"
    git clean -fd
    loop_command _run_jvm_dtest_build 3 "cp build/dtest*.jar ~/dtest_jars" "Failed to build dtest jar for active branch: $user_branch"
    ls -l "${DTEST_JAR_PATH}/dtest_jars"
}

# For a given target, run the tests associated with them. This covers unit and distributed jvm-based tests.
#
# TODO:
#   - Make dtest_jar source dir a param / runtime configurable
#   - Pull in ASF CI impl and compare; merge different functionality as needed
#   - Make build.xml authoritative on timeout and target -> ant -p target translation (i.e. remove classlist prefix requirement)
run_jvm_tests() {
    # build.xml remains authoritative for our timeouts
    # [CIRCLE]:
    # test_timeout=$(grep 'name="test.${classlist_prefix}.timeout"' build.xml | awk -F'"' '{print $4}' || true)
    # if [ -z "$test_timeout" ]; then
    #   test_timeout=$(grep 'name="test.timeout"' build.xml | awk -F'"' '{print $4}')
    # fi
    # [ASF]:
    #   _timeout_for() {
    #       grep "name=\"${1}\"" build.xml | awk -F'"' '{print $4}'
    #   }
    #
    # TODO: Reconcile
    #   - Is the functionality the same between ASF and Circle? If so, let's merge it up.
    #   - Does this handle distributed tests correctly?

    _combine_artifacts_in_temp "$cassandra_dir"
    # [RUNNING]
    # CircleCI
    ant "${ant_target}" "${arguments}" \
        -Dtest.timeout="${test_timeout}" \
        -Dtest.classlistfile="/tmp/java_tests_${agent_index}_final.txt" \
        -Dtest.classlistprefix="${classlist_prefix}" \
        -Dno-build-test=true
    # ASF
    # ant $_testlist_target
    #   -Dtest.classlistprefix="${_target_prefix}"
    #   -Dtest.classlistfile=<(echo "${testlist}")
    #   -Dtest.timeout="${_test_timeout}"
    #   ${ANT_TEST_OPTS}
    #
    # TODO: Reconcile differences:
    #   - just confirm {arguments} on the Circle side and {ANT_TEST_OPTS} on the ASF side match
}

# Runs the python dtests given the requested version and any env vars associated with it. Those vars can be found in the
# config .yaml for differentiation on different test configurations.
#
# $1 string: python version
# $2 (optional)[] string: env options to append
#
# TODO:
#   - Figure out runtime that's non-circle (the split thing seems redundant?)
#   - ASF merge in and integrate. Maybe. Or just have this supercede
run_python_dtests() {
    local python_version=$(check_argument "$1" "python version")
    echo "cat ${CLASSLIST_FILE_PYTHON_DTESTS}"
    cat "${CLASSLIST_FILE_PYTHON_DTESTS}"

    source "~/env${python_version}/bin/activate"
    export PATH=$JAVA_HOME/bin:$PATH

    if [ "$#" -ne 1 ]; then
        export "${@:2}"
    fi

    set -e #enable immediate exit if venv setup fails
    java -version
    cd "${DTEST_PYTHON_PATH}" || exit 2
    mkdir -p "${CASSANDRA_CI_TMP_DIR}/dtest"

    echo "env: $(env)"
    echo "** done env"
    mkdir -p "${CASSANDRA_CI_TMP_DIR}/results/dtests"

    # export SPLIT_TESTS=`cat /tmp/split_dtest_tests_<<parameters.file_tag>>_final.txt`
    if [ ! -z "$SPLIT_TESTS" ]; then
        # CIRCLE:
        #   pytest <<parameters.pytest_extra_args>>
        #       --log-level="DEBUG"
        #       --junit-xml=/tmp/results/dtests/pytest_result_<<parameters.file_tag>>.xml
        #       -s
        #       --cassandra-dir=/home/cassandra/cassandra
        #       --keep-test-dir $SPLIT_TESTS 2>&1 | tee /tmp/dtest/stdout.txt
        #
        # ASF:
        #   PYTEST_OPTS="-vv --log-cli-level=DEBUG --junit-xml=${DIST_DIR}/test/output/nosetests.xml --junit-prefix=${DTEST_TARGET} -s"
        #   pytest ${PYTEST_OPTS} --cassandra-dir=${CASSANDRA_DIR} --keep-failed-test-dir ${DTEST_ARGS} ${SPLIT_TESTS} 2>&1 | tee -a ${DIST_DIR}/test_stdout.txt
        #
        # TODO: Reconcile differences:
        #   - ASF has --junit-prefix= in pytest opts. Is this vestigial or needed?
        #   - Circle has --keep-test-dir, ASF has --keep-failed-test-dir
        echo ""
    else
        echo "Tune your parallelism, there are more containers than test classes. Nothing to do in this container"
        (exit 1)
    fi
}

# TODO:
#  - Pull in ASF CI impl and compare; merge as needed
#   - update .yaml file to point to this script for cmd instead of it's run target atm
run_simulator_tests() {
    local url=$(check_argument "$1" "the url of the repo to clone and build")
    local branch=$(check_argument "$2" "the branch to build")
    local dest=$(check_argument "$3" "destination directory to clone code to")

    pretest_setup "$url" "$branch" "$dest"
    build_dtest_jars "$url" "$branch"
    _merge_runtime_environment "${CASSANDRA_DIR}"
    ant test-simulator-dtest -Dno-build-test=true
}

# $1 string: true for cython
run_cqlshlib_tests() {
    pretest_setup
    confirm_argument_count 1 "$#"
    _merge_runtime_environment "${CASSANDRA_DIR}"
    ant clean
    if [ "$1" == "true" ]; then
        export cython="yes"
    fi
    ant jar -Dno-checkstyle=true -Drat.skip=true -Dant.gen-doc.skip=true -Djavadoc.skip=true
    ./pylib/cassandra-cqlsh-tests.sh "$(pwd)"
}

# $1 string: ant target to run sequentially
run_target_sequentially() {
    local target=$(check_argument "$1" "ant target to run")
    ant "$target" -Dno-build-test=true
}

process_jvm_test_results() {
    # TODO: Impl
}

# TODO: header, argument handling, see if this is valuable at all
process_python_test_results() {
    # merge all unit xml files into one, and print summary test numbers
    pushd ${CASSANDRA_DIR}/ >/dev/null
    # remove <testsuites> wrapping elements. `ant generate-unified-test-report` doesn't like it`
    sed -r "s/<[\/]?testsuites>//g" ${DIST_DIR}/test/output/nosetests.xml > /tmp/nosetests.xml
    cat /tmp/nosetests.xml > ${DIST_DIR}/test/output/nosetests.xml
    ant -quiet -silent generate-unified-test-report
}

# This code (along with all the steps) is expected to be independently executed on every agent.
# Each agent needs to have an integer index at runtime to know which number in the runtime it is so it
# can determine which subset of tests to select and run locally.
#
# $1 string: classlist prefix. Valid values are in <target name="testclasslist-{*}> tags.
# $2 (optional) string: Any extra filtering needed to prune out tests
#
# TODO:
#   - ... why do we rm -fr the upgrade_tests here on circleci's config? See config_template.yml command
#   - Look up ASF CI; merge and integrate any requirements here
#       We're not going to be able to rely on the circle logic for test splitting as it relies on circleci's cmdline application
#       Well, actually hold that thought. The CLI is MIT licensed: https://github.com/CircleCI-Public/circleci-cli/blob/develop/LICENSE
#           so we _could_ use it authoritatively for splits. Let's keep that in mind.
split_jvm_tests() {
    local classlist_prefix=$(check_argument "$1" "classlist_prefix")
    local extra_filtering=""

    # extra filtering is optional
    if [ "$#" -eq 3 ]; then
        extra_filtering="$2"
    fi

    # TODO: Keep or remove this? This is in the circle yaml
    # rm -fr ~/cassandra-dtest/upgrade_tests
    echo "***java tests***"

    # get all of our unit test filenames
    # TODO: Impl. Take from ASF CI or take from... somewhere else?
    # ASF:    find "test/${type}" -name "*Test.java" ${TEST_FILTER:-} | sed "s;^test/${type}/;;" | sort -R > ${TEST_LIST_FILE}
    # CIRCLE: circleci tests glob "$HOME/cassandra/test/<<parameters.classlistprefix>>/**/*.java" > /tmp/all_java_unit_tests.txt

    # split up the unit tests into groups based on the number of containers we have
    # set -eo pipefail && circleci tests split --split-by=timings --timings-type=filename --index=${CIRCLE_NODE_INDEX} --total=${CIRCLE_NODE_TOTAL} /tmp/all_java_unit_tests.txt > /tmp/java_tests_${CIRCLE_NODE_INDEX}.txt
    # set -eo pipefail && cat /tmp/java_tests_${CIRCLE_NODE_INDEX}.txt |\
    #     sed "s;^/home/cassandra/cassandra/test/<<parameters.classlistprefix>>/;;g" | grep "Test\.java$" <<parameters.extra_filters>> > /tmp/java_tests_${CIRCLE_NODE_INDEX}_final.txt

    # TODO: We likely don't need the agent_index here
    # echo "** /tmp/java_tests_${agent_index}_final.txt"
    # cat /tmp/java_tests_${agent_index}_final.txt

    # ASF Split logic:
    #   split_cmd=split
    #   if [[ "${_split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
    #     ( split --help 2>&1 ) | grep -q "r/K/N" || split_cmd=gsplit
    #     command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; exit 1; }
    #     ${split_cmd} -n r/${_split_chunk}
    #   elif [[ "x" != "x${_split_chunk}" ]] ; then
    #     grep -e "${_split_chunk}"
    #   else
    #     echo
    #   fi

}

# This code (along with all the steps) is independently executed on every agent
# so the goal here is to get the script to return the tests *this* container will run
#
# $1 (optional) [] of strings of args to append
split_python_tests() {
    confirm_directory_exists "~/cassandra-dtest"
    cd ~/cassandra-dtest

    confirm_file_exists "~/env${PYTHON_VERSION}/bin/activate"
    source ~/env${PYTHON_VERSION}/bin/activate

    if [ "$#" -ne 0 ]; then
        export "${@:1}"
    fi

    export PATH=$JAVA_HOME/bin:$PATH

    echo "***Collected DTests***"

    # [LISTING TESTS]
    # Circle:
    # set -eo pipefail && ./run_dtests.py <<parameters.run_dtests_extra_args>> --dtest-print-tests-only --dtest-print-tests-output=/tmp/all_dtest_tests_<<parameters.file_tag>>_raw --cassandra-dir=../cassandra
    # if [ -z '<<parameters.tests_filter_pattern>>' ]; then
    #   mv /tmp/all_dtest_tests_<<parameters.file_tag>>_raw /tmp/all_dtest_tests_<<parameters.file_tag>>
    # else
    #   grep -e '<<parameters.tests_filter_pattern>>' /tmp/all_dtest_tests_<<parameters.file_tag>>_raw > /tmp/all_dtest_tests_<<parameters.file_tag>> || { echo "Filter did not match any tests! Exiting build."; exit 0; }
    # fi
    # set -eo pipefail && circleci tests split --split-by=timings --timings-type=classname /tmp/all_dtest_tests_<<parameters.file_tag>> > /tmp/split_dtest_tests_<<parameters.file_tag>>.txt
    # cat /tmp/split_dtest_tests_<<parameters.file_tag>>.txt | tr '\n' ' ' > /tmp/split_dtest_tests_<<parameters.file_tag>>_final.txt
    # cat /tmp/split_dtest_tests_<<parameters.file_tag>>_final.txt

    # ASF:
    # ./run_dtests.py --cassandra-dir=${CASSANDRA_DIR} ${DTEST_ARGS} --dtest-print-tests-only --dtest-print-tests-output=${DIST_DIR}/test_list.txt 2>&1 > ${DIST_DIR}/test_stdout.txt
    # ASF Split tests logic:
    # if [[ "${DTEST_SPLIT_CHUNK}" =~ ^[0-9]+/[0-9]+$ ]]; then
    #     split_cmd=split
    #     ( split --help 2>&1 ) | grep -q "r/K/N" || split_cmd=gsplit
    #     command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; exit 1; }
    #     SPLIT_TESTS=$(${split_cmd} -n r/${DTEST_SPLIT_CHUNK} ${DIST_DIR}/test_list.txt)
    # elif [[ "x" != "x${DTEST_SPLIT_CHUNK}" ]] ; then
    #     SPLIT_TESTS=$(grep -e "${DTEST_SPLIT_CHUNK}" ${DIST_DIR}/test_list.txt)
    #     [[ "x" != "x${SPLIT_TESTS}" ]] || { echo "no tests match regexp \"${DTEST_SPLIT_CHUNK}\""; exit 1; }
    # else
    #     SPLIT_TESTS=$(cat ${DIST_DIR}/test_list.txt)
    # fi
}

#-----------------------------------------------------------------------------
# PRIVATE FUNCTIONS
#
# Do not rely on these within steps: inside a job in the .yaml file
#-----------------------------------------------------------------------------

# Does a shallow clone of the specified repo / branch
#
# TODO:
#   - Pull in ASF CI impl; diff and test; merge
_shallow_clone_branch() {
    local url=$(check_argument "$1" "url to clone")
    local branch=$(check_argument "$2" "branch to clone")
    local dest=$(check_argument "$3" "destination to clone the branch to")
    git clone --single-branch --depth 1 --branch "$branch" "$url" "$dest"
}

# Loops a few times to prevent mavent-ant-task download failures.
# Assumptions:
#   - requires ant in the path
#   - Expects to be inside desired cassandra dir
#
# TODO:
#   - Pull in ASF CI impl; diff and test; merge
_build_cassandra() {
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
_run_jvm_dtest_build() {
    ant realclean
    # Circle
    ant jar dtest-jar -Dno-checkstyle=true -Drat.skip=true
    # ASF
    # ant jar dtest-jar ${ANT_TEST_OPTS} -Dbuild.dir=${TMP_DIR}/cassandra-dtest-jars/build
    #
    # TODO: Reconcile
    #   - On circle we skip checkstyle and rat. That ok?
    #   - ANT_TEST_OPTS may differ. Looks like they're just "no-build-test=true and -Dtmp.dir="
}


# We take our built cassandra dir and move it to temp, combining it with dtest jars if they're present
# This leaves our pwd as the combined C* dir, ready for tests to run
#
# $1 string: relative path for cassandra dir w/built repo
# $2 string: command to run
_merge_runtime_environment() {
    local cassandra_dir=$(check_argument "$1" "local cassandra directory.")
    confirm_directory_exists "$cassandra_dir"

    local cmd_to_run=$(check_argument "$2" "command to evaluate.")

    mv -f "$cassandra_dir" "$CASSANDRA_CI_TMP_DIR"
    cd "$CASSANDRA_CI_TMP_DIR" || exit 2

    if [ -n "$DTEST_JAR_PATH" ]; then
        cp "${DTEST_JAR_PATH}/dtest*" "${CASSANDRA_CI_TMP_DIR}/build/"
    fi
}

# Docker images or other build image are expected to provide all of these; this is a failsafe to
# make sure the python env is what's needed if there's changes and images aren't updated.
#
# TODO: Reconcile ASF and Circle
#   - ASF nukes and rebuilds on each run.
_prep_virtualenv_and_deps() {
    confirm_directory_exists "$HOME/cassandra-dtest"
    # [CIRCLE]
    pip3 install --exists-action w --upgrade -r ~/cassandra-dtest/requirements.txt
    pip3 uninstall -y cqlsh
    pip3 freeze

    # [ASF]
    # fresh virtualenv and test logs results everytime
    # [[ "/" == "${DIST_DIR}" ]] || rm -rf "${DIST_DIR}/venv" "${DIST_DIR}/test/{html,output,logs}"
    # re-use when possible the pre-installed virtualenv found in the cassandra-ubuntu2004_test docker image
    # virtualenv-clone ${BUILD_HOME}/env${python_version} ${DIST_DIR}/venv || virtualenv --python=python${python_version} ${DIST_DIR}/venv
    # source ${DIST_DIR}/venv/bin/activate
    # pip3 install --exists-action w -r ${CASSANDRA_DTEST_DIR}/requirements.txt
    # pip3 freeze
}


# Lists out all the files matching *Test.java in the given input directory.
# This is the authoritative way we generate a list of all valid tests for a given suite.
_list_tests() {
    local cassandra_directory=$(check_argument "$1" "cassandra directory to enumerate tests.")
    local suite_name=$(check_argument "$2" "suite name to enumerate test files.")

    if [ -d "${cassandra_directory}" ]; then echo "Cannot find ${cassandra_directory}; cannot enumerate tests." && exit 1; fi
    local combined_test_dir="${cassandra_directory}/tests/${suite_name}"

    if [ -d "${combined_test_dir}" ]; then echo "Cannot find ${combined_test_dir}; cannot enumerate tests." && exit 1; fi

    # TODO: Parse out the testmacrohelper from build.xml for a given macrodef name="testlist-*", so we can use build.xml as our authoritative way to list out tests
    # Until this is done, anyone who's listing out tests in a CI system should make sure they're matching the dir, timeout, and exclude= filters for the suite from build.xml
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
        echo "$error_message: ${RETURN}"
        exit ${RETURN}
    fi
}

# Pulls the timeout value in ms from build.xml in the cassandra_dir
# return value is a string expected to be captured in $()
_timeout_for() {
    test_target=$(check_argument "$1" "the target from build.xml to check timeout for (i.e. test.timeout, test.long.timeout, etc)")
    grep "name=\"${test_target}\"" "${CASSANDRA_DIR}/build.xml" | awk -F'"' '{print $4}'
}

# Blasts out whatever the env has set for tmp dir; don't want build data to accumulate over time
_clear_temp_dir() {
    rm -rf "${CASSANDRA_CI_TMP_DIR}"
}

# For a given environment variable, we assign based on the following fallthrough:
# 1: If it's defined in the env, use that
# 2: If it's defined in the yaml override, use that
# 3: Else use what's in the base reference .yaml for CI
_init_env_var() {
    local to_init=$(check_argument "$1" "the env var to set")
    local yaml_path=$(check_argument "$2" "the optional yaml path to pull from if not set in local env")

    if [ -z "${!to_init}" ]; then
        local yaml_value=_retrieve_param_from_yaml "$yaml_path"
        declare -g "$to_init=$yaml_value"
    fi
}

# Will pull from the yaml override if present, defaults to base CASSANDRA_CI_YAML if no override file is supplied.
# This method does not allow for not finding the value inside either of the .yaml files. If it's not found it'll exit out.
#
# return value is a string expected to be captured in $()
_retrieve_param_from_yaml() {
    local member=$(check_argument "$1" "the path to the member inside the .yaml to retrieve")

    result=$(yq "$member" "$CASSANDRA_CI_YAML")
    if [[ -n "${CASSANDRA_CI_YAML_OVERRIDES}" ]]; then
        override=$(yq "$member" "$CASSANDRA_CI_YAML_OVERRIDES")
        if [[ -n "$override" ]]; then
            result="$override"
        fi
    fi

    if [[ -z "$result" ]]; then
        echo "ERROR. Cannot retrieve results from either ${CASSANDRA_CI_YAML} or ${CASSANDRA_CI_YAML_OVERRIDES} for member: ${member}. Cannot proceed."
        exit 1
    fi

    echo "$result"
}

_retrieve_array_from_yaml() {
    local key=$(check_argument "$1" "the key pointing to the array in the .yaml file")
    local target=$(check_argument "$2" "the variable name of the global array in which to store the .yaml results")
    mapfile -t temp_array < <(yq e ".${key}[]" "this.yaml")
    declare -ag "${target}=(\"${temp_array[@]}\")"
}
