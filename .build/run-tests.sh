#!/bin/bash
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
# Wrapper script for running a split or regexp of tests (excluding python dtests)
#

[ $DEBUG ] && set -x

set -o errexit
set -o pipefail

[ $DEBUG ] && set -x

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/..)"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${CASSANDRA_DIR}/build"

# pre-conditions
command -v ant >/dev/null 2>&1 || { error 1 "ant needs to be installed"; }
command -v git >/dev/null 2>&1 || { error 1 "git needs to be installed"; }
command -v uuidgen >/dev/null 2>&1 || test -f /proc/sys/kernel/random/uuid || { error 1 "uuidgen needs to be installed"; }
[ -d "${CASSANDRA_DIR}" ] || { error 1 "Directory ${CASSANDRA_DIR} must exist"; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { error 1 "${CASSANDRA_DIR}/build.xml must exist"; }
[ -d "${DIST_DIR}" ] || { mkdir -p "${DIST_DIR}" ; }


error() {
  echo >&2 $2;
  set -x
  exit $1
}

print_help() {
  echo "Usage: $0 [-a|-t|-c|-e|-i|-b|-s|-h]"
  echo "   -a Test target type: test, test-compression, test-cdc, ..."
  echo "   -t Test name regexp to run."
  echo "   -c Chunk to run in the form X/Y: Run chunk X from a total of Y chunks."
  echo "   -b Specify the base git branch for comparison when determining changed tests to"
  echo "      repeat. Defaults to ${BASE_BRANCH}. Note that this option is not used when"
  echo "      the '-a' option is specified."
  echo "   -s Skip automatic detection of changed tests. Useful when you need to repeat a few ones,"
  echo "      or when there are too many changed tests the CI env to handle."
  echo "   -e <key=value> Environment variables to be used in the repeated runs:"
  echo "                   -e REPEATED_TESTS_STOP_ON_FAILURE=false"
  echo "                   -e REPEATED_TESTS_COUNT=500"
  echo "                  If you want to specify multiple environment variables simply add multiple -e options."
  echo "   -i Ignore unknown environment variables"
  echo "   -h Print help"
}


# legacy argument handling
case ${1} in
  "build_dtest_jars" | "stress-test" | "fqltool-test" | "microbench" | "test-burn" | "long-test" | "cqlsh-test" | "simulator-dtest" | "test" | "test-cdc" | "test-compression" | "test-oa" | "test-system-keyspace-directory" | "test-latest" | "jvm-dtest" | "jvm-dtest-upgrade" | "jvm-dtest-novnode" | "jvm-dtest-upgrade-novnode")
    test_type="-a ${1}"
    if [[ -z ${2} ]]; then
      test_list=""
    elif [[ -n ${2} && "${2}" =~ ^[0-9]+/[0-9]+$ ]]; then
      test_list="-c ${2}";
    else
      test_list="-t ${2}";
    fi
    echo "Using deprecated legacy arguments.  Please update to new parameter format: ${test_type} ${test_list}"
    $0 ${test_type} ${test_list}
    exit $?
esac


env_vars=""
has_env_vars=false
check_env_vars=true
detect_changed_tests=true
while getopts "a:t:c:e:ib:shj:" opt; do
  case $opt in
    a ) test_target="$OPTARG"
        ;;
    t ) test_name_regexp="$OPTARG"
        ;;
    c ) chunk="$OPTARG"
        ;;
    e ) if (! ($has_env_vars)); then
          env_vars="$OPTARG"
        else
          env_vars="$env_vars|$OPTARG"
        fi
        has_env_vars=true
        ;;
    b ) BASE_BRANCH="$OPTARG"
        ;;
    i ) check_env_vars=false
        ;;
    s ) detect_changed_tests=false
        ;;
    h ) print_help
        exit 0
        ;;
    j ) ;; # To avoid failing on java_version param from docker/run_tests.sh
    \?) error 1 "Invalid option: -$OPTARG"
        ;;
  esac
done
shift $((OPTIND-1))
if [ "$#" -ne 0 ]; then
  error 1 "Unexpected arguments"
fi

# validate environment variables
if $has_env_vars && $check_env_vars; then
  for entry in $(echo $env_vars | tr "|" "\n"); do
    key=$(echo $entry | tr "=" "\n" | sed -n 1p)
    case $key in
      "REPEATED_TESTS_STOP_ON_FAILURE" | "REPEATED_TESTS_COUNT" )
        [[ ${test_target} == *"-repeat" ]] || { error 1 "'-e REPEATED_*' variables only valid against *-repeat target types"; }
        ;;
      *)
        error 1 "unrecognized environment variable name: $key"
        ;;
    esac
  done
fi

# print debug information on versions
ant -version
git --version
java -version  2>&1
javac -version  2>&1

# set the OFFLINE env var (to anything) to allow running jvm-dtest-upgrade offline
[ "x" != "x${OFFLINE}" ] && echo "WARNING: running in offline mode. jvm-dtest-upgrade results may be stale."

# lists all tests for the specific test type
_list_tests() {
  local -r classlistprefix="$1"
  find "test/${classlistprefix}" -name '*Test.java' | sed "s;^test/${classlistprefix}/;;g" | sort
}

_split_tests() {
  local -r _split_chunk="$1"
  split_cmd=split
  if [[ "${_split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
    ( split --help 2>&1 ) | grep -q "r/K/N" || split_cmd=gsplit
    command -v ${split_cmd} >/dev/null 2>&1 || { error 1 "${split_cmd} needs to be installed"; }
    ${split_cmd} -n r/${_split_chunk}
  elif [[ "x" != "x${_split_chunk}" ]] ; then
    grep -e "${_split_chunk}"
  else
    echo
  fi
}

_timeout_for() {
  grep "name=\"${1}\"" build.xml | awk -F'"' '{print $4}'
}

_get_env_var() {
  [[ ${env_vars} =~ ${1}=([^|]+) ]]
  echo "${BASH_REMATCH[1]}"
}

_build_all_dtest_jars() {

    # build the dtest-jar for the branch under test. remember to `ant clean` if you want a new dtest jar built
    dtest_jar_version=$(grep 'property\s*name=\"base.version\"' build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
    if [ -f "${DIST_DIR}/dtest-${dtest_jar_version}.jar" ] ; then
        echo "Skipping dtest jar build for branch under test as ${DIST_DIR}/dtest-${dtest_jar_version}.jar already exists"
    else
        ant jar dtest-jar ${ANT_TEST_OPTS} -Dbuild.dir=${TMP_DIR}/cassandra-dtest-jars/build
        cp "${TMP_DIR}/cassandra-dtest-jars/build/dtest-${dtest_jar_version}.jar" ${DIST_DIR}/
    fi

    if [ -d ${TMP_DIR}/cassandra-dtest-jars/.git ] && [ "https://github.com/apache/cassandra.git" == "$(git -C ${TMP_DIR}/cassandra-dtest-jars remote get-url origin)" ] ; then
      echo "Reusing ${TMP_DIR}/cassandra-dtest-jars for past branch dtest jars"
      if [ "x" == "x${OFFLINE}" ] ; then
        until git -C ${TMP_DIR}/cassandra-dtest-jars fetch --quiet origin ; do echo "git -C ${TMP_DIR}/cassandra-dtest-jars fetch failed… trying again… " ; done
      fi
    else
        echo "Cloning cassandra to ${TMP_DIR}/cassandra-dtest-jars for past branch dtest jars"
        rm -fR ${TMP_DIR}/cassandra-dtest-jars
        pushd $TMP_DIR >/dev/null
        until git clone --quiet --depth 1 --no-single-branch https://github.com/apache/cassandra.git cassandra-dtest-jars ; do echo "git clone failed… trying again… " ; done
        popd >/dev/null
    fi

    # cassandra-4 branches need CASSANDRA_USE_JDK11 to allow jdk11
    [ "${java_version}" -eq 11 ] && export CASSANDRA_USE_JDK11=true

    pushd ${TMP_DIR}/cassandra-dtest-jars >/dev/null
    for branch in cassandra-4.0 cassandra-4.1 cassandra-5.0 trunk ; do
        git clean -qxdff && git reset --hard HEAD  || echo "failed to reset/clean ${TMP_DIR}/cassandra-dtest-jars… continuing…"
        git checkout --quiet $branch
        dtest_jar_version=$(grep 'property\s*name=\"base.version\"' build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
        if [ -f "${DIST_DIR}/dtest-${dtest_jar_version}.jar" ] ; then
            echo "Skipping dtest jar build for branch ${branch} as ${DIST_DIR}/dtest-${dtest_jar_version}.jar already exists"
            continue
        fi
        # redefine the build.dir to local build folder, rightmost definition wins with java command line system properties
        ant realclean -Dbuild.dir=${TMP_DIR}/cassandra-dtest-jars/build
        ant jar dtest-jar ${ANT_TEST_OPTS} -Dbuild.dir=${TMP_DIR}/cassandra-dtest-jars/build
        cp "${TMP_DIR}/cassandra-dtest-jars/build/dtest-${dtest_jar_version}.jar" ${DIST_DIR}/
    done
    popd >/dev/null
    ls -l ${DIST_DIR}/dtest*.jar
    unset CASSANDRA_USE_JDK11
}

_run_testlist() {
    local _target_prefix=$1
    local _testlist_target=$2
    local _test_name_regexp=$3
    local _split_chunk=$4
    local _test_timeout=$5
    local _test_iterations=${6:-1}

    # are we running ${_test_name_regexp} or ${_split_chunk}
    if [ -n "${_test_name_regexp}" ]; then
      echo "Running tests: ${_test_name_regexp} (${_test_iterations} times)"
      # test regexp can come in csv
      for i in ${_test_name_regexp//,/ }; do
        [ -n "${testlist}" ] && testlist="${testlist}"$'\n'
        testlist="${testlist}$( _list_tests "${_target_prefix}" | _split_tests "${i}")"
      done
      [[ -z "${testlist}" ]] && error 1 "No tests found in test name regexp: ${_test_name_regexp}"
    else
      [ -n "${_split_chunk}" ] || { error 1 "Neither name regexp or split chunk defined"; }
      echo "Running split: ${_split_chunk}"
      testlist="$( _list_tests "${_target_prefix}" | _split_tests "${_split_chunk}")"
      if [[ -z "${testlist}" ]]; then
        # something has to run in the split to generate a junit xml result
        echo "Hacking ${_target_prefix} ${_testlist_target} to run only first test found as no tests in split ${_split_chunk} were found"
        testlist="$( _list_tests "${_target_prefix}" | sed -n 1p)"
      fi
    fi

    local -r _results_uuid="$(command -v uuidgen >/dev/null 2>&1 && uuidgen || cat /proc/sys/kernel/random/uuid)"
    local failures=0
    for ((i=0; i < _test_iterations; i++)); do
      [ "${_test_iterations}" -eq 1 ] || printf "–––– run ${i}\n"
      set +o errexit
      ant "$_testlist_target" -Dtest.classlistprefix="${_target_prefix}" -Dtest.classlistfile=<(echo "${testlist}") -Dtest.timeout="${_test_timeout}" ${ANT_TEST_OPTS}
      ant_status=$?
      set -o errexit
      if [[ $ant_status -ne 0 ]]; then
        echo "failed ${_target_prefix} ${_testlist_target} ${split_chunk} ${_test_name_regexp}"

        # Only store logs for failed tests on repeats to save up space
        if [ "${_test_iterations}" -gt 1 ]; then
          # Get this test results and rename file with iteration and 'fail'
          find "${DIST_DIR}"/test/output/ -type f -name "*.xml" -not -name "*fail.xml" -print0 | while read -r -d $'\0' file; do
            mv "${file}" "${file%.xml}-${_results_uuid}-${i}-fail.xml"
          done
          find "${DIST_DIR}"/test/logs/ -type f -name "*.log" -not -name "*fail.log" -print0 | while read -r -d $'\0' file; do
            mv "${file}" "${file%.log}-${_results_uuid}-${i}-fail.log"
          done

          if [ "$(_get_env_var 'REPEATED_TESTS_STOP_ON_FAILURE')" == true ]; then
            error 0 "fail fast, after ${i} successful runs"
          fi
          let failures+=1
        fi
      fi
    done
    [ "${_test_iterations}" -eq 1 ] || printf "––––\nfailure rate: ${failures}/${_test_iterations}\n"
}

_main() {
  # parameters
  local -r target="${test_target/-repeat/}"
  local -r split_chunk="${chunk:-'1/1'}" # Chunks formatted as "K/N" for the Kth chunk of N chunks

  # check split_chunk is compatible with target (if not a regexp)
  if [[ "${_split_chunk}" =~ ^\d+/\d+$ ]] && [[ "1/1" != "${split_chunk}" ]] ; then
    case ${target} in
      "stress-test" | "fqltool-test" | "microbench" | "cqlsh-test" | "simulator-dtest")
          error 1 "Target ${target} does not suport splits."
          ;;
        *)
          ;;
    esac
  fi

  # "-repeat" is a reserved suffix on target types
  if [[ ${test_target} == *"-repeat" ]] ; then
    [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] && { error 1 "Repeated tests not valid with splits"; }
    if [[ -z "${test_name_regexp}" ]] ; then
      error 1 "Repeated tests requires use of -t option"
    fi
    local -r repeat_count="$(_get_env_var 'REPEATED_TESTS_COUNT')"
  else
    test_name_regexp="${test_name_regexp:-}"
  fi

  pushd ${CASSANDRA_DIR}/ >/dev/null

  # jdk check
  local -r java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
  local -r version=$(grep 'property\s*name=\"base.version\"' build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
  local -r java_version_default=`grep 'property\s*name="java.default"' build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

  if [ "${java_version}" -eq 17 ] && [[ "${target}" == "jvm-dtest-upgrade" ]] ; then
    error 1 "Invalid JDK${java_version}. Only overlapping supported JDKs can be used when upgrading, as the same jdk must be used over the upgrade path."
  fi

  # check project is already built. no cleaning is done, so jenkins unstash works, beware.
  [[ -f "${DIST_DIR}/apache-cassandra-${version}.jar" ]] || [[ -f "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar" ]] || { error 1 "Project must be built first. Use \`ant jar\`. Build directory is ${DIST_DIR} with: $(ls ${DIST_DIR} | xargs)"; }

  # check if dist artifacts exist, this breaks the dtests
  [[ -d "${DIST_DIR}/dist" ]] && { error 1 "tests don't work when build/dist ("${DIST_DIR}/dist") exists (from \`ant artifacts\`)"; }

  # ant test setup
  export TMP_DIR="${DIST_DIR}/tmp"
  [ -d ${TMP_DIR} ] || mkdir -p "${TMP_DIR}"
  export ANT_TEST_OPTS="-Dno-build-test=true -Dtmp.dir=${TMP_DIR} -Dbuild.test.output.dir=${DIST_DIR}/test/output/${target}"

  # fresh virtualenv and test logs results everytime
  [[ "/" == "${DIST_DIR}" ]] || rm -rf "${DIST_DIR}/test/{html,output,logs,reports}"

  # cheap trick to ensure dependency libraries are in place. allows us to stash only project specific build artifacts.
  #  also recreate some of the non-build files we need
  ant -quiet -silent resolver-dist-lib _createVersionPropFile

  case ${target} in
    "stress-test")
      # hard fail on test compilation, but dont fail the test run as unstable test reports are processed
      ant stress-build-test ${ANT_TEST_OPTS}
      ant $target ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "fqltool-test")
      # hard fail on test compilation, but dont fail the test run so unstable test reports are processed
      ant fqltool-build-test ${ANT_TEST_OPTS}
      ant $target ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "microbench")
      ant $target ${ANT_TEST_OPTS} -Dmaven.test.failure.ignore=true
      ;;
    "test")
      _run_testlist "unit" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-cdc")
      _run_testlist "unit" "testclasslist-cdc" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-compression")
      _run_testlist "unit" "testclasslist-compression" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-oa")
      _run_testlist "unit" "testclasslist-oa" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-system-keyspace-directory")
      _run_testlist "unit" "testclasslist-system-keyspace-directory" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-latest")
      _run_testlist "unit" "testclasslist-latest" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')" "${repeat_count}"
      ;;
    "test-burn")
      _run_testlist "burn" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.burn.timeout')" "${repeat_count}"
      ;;
    "long-test")
      _run_testlist "long" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.long.timeout')" "${repeat_count}"
      ;;
    "simulator-dtest")
      ant test-simulator-dtest ${ANT_TEST_OPTS} || echo "failed ${target}"
      ;;
    "jvm-dtest" | "jvm-dtest-novnode")
      [ "jvm-dtest-novnode" == "${target}" ] || ANT_TEST_OPTS="${ANT_TEST_OPTS} -Dcassandra.dtest.num_tokens=16"
      if [[ -z "${test_name_regexp}" ]] ; then
        test_name_regexp=$( _list_tests "distributed" | grep -v "upgrade" | _split_tests "${split_chunk}")
        if [[ -z "${test_name_regexp}" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { error 1 "No tests match ${test_name_regexp}"; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest to run only first test found as no tests in split ${split_chunk} were found"
          test_name_regexp="$( _list_tests "distributed"  | grep -v "upgrade" | sed -n 1p)"
        fi
      fi
      _run_testlist "distributed" "testclasslist" "${test_name_regexp}" "" "$(_timeout_for 'test.distributed.timeout')" "${repeat_count}"
      ;;
    "build_dtest_jars")
      _build_all_dtest_jars
      ;;
    "jvm-dtest-upgrade" | "jvm-dtest-upgrade-novnode")
      _build_all_dtest_jars
      [ "jvm-dtest-upgrade-novnode" == "${target}" ] || ANT_TEST_OPTS="${ANT_TEST_OPTS} -Dcassandra.dtest.num_tokens=16"
      if [[ -z "${test_name_regexp}" ]] ; then
        test_name_regexp=$( _list_tests "distributed" | grep "upgrade" | _split_tests "${split_chunk}")
        if [[ -z "${test_name_regexp}" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { error 1 "No tests match ${test_name_regexp}"; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest-upgrade to run only first test found as no tests in split ${split_chunk} were found"
          test_name_regexp="$( _list_tests "distributed"  | grep "upgrade" | sed -n 1p)"
        fi
      fi
      _run_testlist "distributed" "testclasslist" "${test_name_regexp}" "" "$(_timeout_for 'test.distributed.timeout')" "${repeat_count}"
      ;;
    "cqlsh-test")
      ./pylib/cassandra-cqlsh-tests.sh $(pwd)
      ;;
    *)
      error 1 "unrecognized test type \"${target}\""
      ;;
  esac

  # merge all unit xml files into one, and print summary test numbers
  ant -quiet -silent generate-test-report

  popd  >/dev/null
}

_main "$@"
