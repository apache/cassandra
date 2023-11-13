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

set -o errexit
set -o pipefail

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/..)"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${CASSANDRA_DIR}/build"

die ()
{
  echo "ERROR: $*"
  print_help
  exit 1
}

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; die; }
command -v git >/dev/null 2>&1 || { echo >&2 "git needs to be installed"; die; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; die; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; die; }
[ -d "${DIST_DIR}" ] || { mkdir -p "${DIST_DIR}" ; }

print_help() {
  echo "Usage: $0 [-a|-t|-c|-e|-i|-b|-s|-h]"
  echo "   -a Ant test target group: test, tes-compression, test-cdc,..."
  echo "   -t Test name regexp to run."
  echo "   -c Chunk to run in the form X/Y: Run chunk X from a total of Y chunks."
  echo "   -b Specify the base git branch for comparison when determining changed tests to"
  echo "      repeat. Defaults to ${BASE_BRANCH}. Note that this option is not used when"
  echo "      the '-a' option is specified."
  echo "   -s Skip automatic detection of changed tests. Useful when you need to repeat a few ones,"
  echo "      or when there are too many changed tests the CI env to handle."
  echo "   -e <key=value> Environment variables to be used in the repeated runs:"
#  echo "                   -e DTEST_BRANCH=CASSANDRA-8272"
#  echo "                   -e DTEST_REPO=https://github.com/adelapena/cassandra-dtest.git"
  echo "                   -e REPEATED_TESTS_STOP_ON_FAILURE=false"
  echo "                   -e REPEATED_TESTS=org.apache.cassandra.cql3.ViewTest,ForceCompactionTest"
  echo "                   -e REPEATED_TESTS_COUNT=500"
 # echo "                   -e REPEATED_DTESTS=cdc_test.py cqlsh_tests/test_cqlsh.py::TestCqlshSmoke"
 # echo "                   -e REPEATED_DTESTS_COUNT=500"
 # echo "                   -e REPEATED_LARGE_DTESTS=replace_address_test.py::TestReplaceAddress::test_replace_stopped_node"
 # echo "                   -e REPEATED_LARGE_DTESTS=100"
 # echo "                   -e REPEATED_UPGRADE_DTESTS=upgrade_tests/cql_tests.py upgrade_tests/paging_test.py"
 # echo "                   -e REPEATED_UPGRADE_DTESTS_COUNT=25"
  echo "                   -e REPEATED_ANT_TEST_TARGET=testsome"
  echo "                   -e REPEATED_ANT_TEST_CLASS=org.apache.cassandra.cql3.ViewTest"
  echo "                   -e REPEATED_ANT_TEST_METHODS=testCompoundPartitionKey,testStaticTable"
  echo "                   -e REPEATED_ANT_TEST_VNODES=false"
  echo "                   -e REPEATED_ANT_TEST_COUNT=500"
  echo "                  If you want to specify multiple environment variables simply add multiple -e options."
  echo "   -i Ignore unknown environment variables"
  echo "   -h Print help"
}

env_vars=""
has_env_vars=false
check_env_vars=true
detect_changed_tests=true
while getopts "a:t:c:e:ib:shj:" opt; do
  case $opt in
      a ) ant_target="$OPTARG"
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
      \?) die "Invalid option: -$OPTARG"
          ;;
  esac
done
shift $((OPTIND-1))
if [ "$#" -ne 0 ]; then
    die "Unexpected arguments"
fi

# validate environment variables
if $has_env_vars && $check_env_vars; then
  for entry in $(echo $env_vars | tr "|" "\n"); do
    key=$(echo $entry | tr "=" "\n" | head -n 1)
    if [ "$key" != "DTEST_REPO" ] &&
       [ "$key" != "DTEST_BRANCH" ] &&
       [ "$key" != "REPEATED_TESTS_STOP_ON_FAILURE" ] &&
       [ "$key" != "REPEATED_TESTS" ] &&
       [ "$key" != "REPEATED_TESTS_COUNT" ] &&
       [ "$key" != "REPEATED_DTESTS" ] &&
       [ "$key" != "REPEATED_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_LARGE_DTESTS" ] &&
       [ "$key" != "REPEATED_LARGE_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS" ] &&
       [ "$key" != "REPEATED_UPGRADE_DTESTS_COUNT" ] &&
       [ "$key" != "REPEATED_ANT_TEST_TARGET" ] &&
       [ "$key" != "REPEATED_ANT_TEST_CLASS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_METHODS" ] &&
       [ "$key" != "REPEATED_ANT_TEST_VNODES" ] &&
       [ "$key" != "REPEATED_ANT_TEST_COUNT" ]; then
      die "Unrecognised environment variable name: $key"
    fi
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
    command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; die; }
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
    for branch in cassandra-4.0 cassandra-4.1 cassandra-5.0 ; do
        git clean -qxdff && git reset --hard HEAD || echo "failed to reset/clean ${TMP_DIR}/cassandra-dtest-jars… continuing…"
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

    # Here we have to deal with several cases
    #   1. Run repeats: cvs tests
    #   2. Run split
    #   3. Run single test
    if [ "${_test_iterations}" -gt 1 ] ; then
      # TODO: Support repeat at test method level
      echo "Running reapted tests: ${_test_name_regexp} chunk ${_split_chunk} ${_test_iterations} times."

      #Repeat tests can come in csv
      for i in ${_test_name_regexp//,/ }; do
          testlist="${testlist}"$'\n'"$( _list_tests "${_target_prefix}" | _split_tests "${i}")"
      done
    elif [ -n "${_test_name_regexp}" ]; then
      echo "Running test: ${_test_name_regexp}"
      testlist="$( _list_tests "${_target_prefix}" | _split_tests "${_test_name_regexp}")"
    elif [ -n "${_split_chunk}" ]; then
      echo "Running split: ${_split_chunk}"
      testlist="$( _list_tests "${_target_prefix}" | _split_tests "${_split_chunk}")"
    fi

    if [[ "${_split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
      if [[ -z "${testlist}" ]]; then
        # something has to run in the split to generate a junit xml result
        echo "Hacking ${_target_prefix} ${_testlist_target} to run only first test found as no tests in split ${_split_chunk} were found"
        testlist="$( _list_tests "${_target_prefix}" | head -n1)"
      fi
    else
      if [[ -z "${testlist}" ]]; then
        echo "No tests match ${_test_name_regexp} ${_split_chunk}"
        die
      fi
    fi

    local -r _results_uuid=$(cat /proc/sys/kernel/random/uuid)
    for ((i=0; i < _test_iterations; i++)); do
      ant "$_testlist_target" -Dtest.classlistprefix="${_target_prefix}" -Dtest.classlistfile=<(echo "${testlist}") -Dtest.timeout="${_test_timeout}" "${ANT_TEST_OPTS}"
      ant_status=$?
      if [[ $ant_status -ne 0 ]]; then
        echo "failed ${_target_prefix} ${_testlist_target}  ${split_chunk}"

        # Only store logs for failed tests on repeats to save up space
        if [ "${_test_iterations}" -gt 1 ]; then
          # Get this test results and rename file with iteration and 'fail'
          find "${DIST_DIR}"/test/output/ -type f -not -name "*fail.xml" -print0 | while read -r -d $'\0' file; do
              mv "${file}" "${file%.xml}-${_results_uuid}-${i}-fail.xml"
          done
          find "${DIST_DIR}"/test/logs/ -type f -not -name "*fail.log" -print0 | while read -r -d $'\0' file; do
              mv "${file}" "${file%.log}-${_results_uuid}-${i}-fail.log"
          done

          if [ "$(_get_env_var 'REPEATED_TESTS_STOP_ON_FAILURE')" == true ]; then
            die
          fi
        fi
      fi
    done
}

_main() {
  # parameters
  local -r target="${ant_target:-}"
  local -r split_chunk="${chunk:-'1/1'}" # Optional: pass in chunk or regexp to test. Chunks formatted as "K/N" for the Kth chunk of N chunks
  local -r test_name_regexp="${test_name_regexp:-}"

  # check split_chunk is compatible with target (if not a regexp). TODO support repeats for these
  if [[ "${_split_chunk}" =~ ^\d+/\d+$ ]] && [[ "1/1" != "${split_chunk}" ]] ; then
    case ${target} in
      "stress-test" | "fqltool-test" | "microbench" | "cqlsh-test")
          echo "Target ${target} does not suport splits."
          die
          ;;
        *)
          ;;
    esac
  fi

  pushd ${CASSANDRA_DIR}/ >/dev/null

  # jdk check
  local -r java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
  local -r version=$(grep 'property\s*name=\"base.version\"' build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
  local -r java_version_default=`grep 'property\s*name="java.default"' build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

  if [ "${java_version}" -eq 17 ] && [[ "${target}" == "jvm-dtest-upgrade" ]] ; then
    echo "Invalid JDK${java_version}. Only overlapping supported JDKs can be used when upgrading, as the same jdk must be used over the upgrade path."
    die
  fi

  # check project is already built. no cleaning is done, so jenkins unstash works, beware.
  [[ -f "${DIST_DIR}/apache-cassandra-${version}.jar" ]] || [[ -f "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar" ]] || { echo "Project must be built first. Use \`ant jar\`. Build directory is ${DIST_DIR} with: $(ls ${DIST_DIR})"; die; }

  # check if dist artifacts exist, this breaks the dtests
  [[ -d "${DIST_DIR}/dist" ]] && { echo "tests don't work when build/dist ("${DIST_DIR}/dist") exists (from \`ant artifacts\`)"; die; }

  # ant test setup
  export TMP_DIR="${DIST_DIR}/tmp"
  [ -d ${TMP_DIR} ] || mkdir -p "${TMP_DIR}"
  export ANT_TEST_OPTS="-Dno-build-test=true -Dtmp.dir=${TMP_DIR}"

  # fresh virtualenv and test logs results everytime
  [[ "/" == "${DIST_DIR}" ]] || rm -rf "${DIST_DIR}/test/{html,output,logs}"

  # cheap trick to ensure dependency libraries are in place. allows us to stash only project specific build artifacts.
  ant -quiet -silent resolver-dist-lib

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
      _run_testlist "unit" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-repeat")
      _run_testlist "unit" "testclasslist" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-cdc")
      _run_testlist "unit" "testclasslist-cdc" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-cdc-repeat")
      _run_testlist "unit" "testclasslist-cdc" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-compression")
      _run_testlist "unit" "testclasslist-compression" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-compression-repeat")
      _run_testlist "unit" "testclasslist-compression" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-oa")
      _run_testlist "unit" "testclasslist-oa" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-oa-repeat")
      _run_testlist "unit" "testclasslist-oa" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-system-keyspace-directory")
      _run_testlist "unit" "testclasslist-system-keyspace-directory" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-system-keyspace-directory-repeat")
      _run_testlist "unit" "testclasslist-system-keyspace-directory" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-trie")
      _run_testlist "unit" "testclasslist-trie" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-trie-repeat")
      _run_testlist "unit" "testclasslist-trie" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "test-burn")
      _run_testlist "burn" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.burn.timeout')"
      ;;
    "test-burn-repeat")
      _run_testlist "burn" "testclasslist" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "long-test")
      _run_testlist "long" "testclasslist" "${test_name_regexp}" "${split_chunk}" "$(_timeout_for 'test.long.timeout')"
      ;;
    "long-test-repeat")
      _run_testlist "long" "testclasslist" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "jvm-dtest")
      if [ -n "${test_name_regexp}" ]; then
        testlist=$( _list_tests "distributed" | grep -v "upgrade" | _split_tests "${test_name_regexp}")
      elif [ -n "${split_chunk}" ]; then
        testlist=$( _list_tests "distributed" | grep -v "upgrade" | _split_tests "${split_chunk}")
      fi

      if [[ -z "$testlist" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { echo "No tests match ${split_chunk}"; die; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest to run only first test found as no tests in split ${split_chunk} were found"
          testlist="$( _list_tests "distributed"  | grep -v "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout="$(_timeout_for "test.distributed.timeout")" -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "jvm-dtest-repeat")
      _run_testlist "distributed" "testclasslist" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.distributed.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "jvm-dtest-upgrade")
      _build_all_dtest_jars
      if [ -n "${test_name_regexp}" ]; then
        testlist=$( _list_tests "distributed"  | grep "upgrade" | _split_tests "${test_name_regexp}")
      elif [ -n "${split_chunk}" ]; then
        testlist=$( _list_tests "distributed"  | grep "upgrade" | _split_tests "${split_chunk}")
      fi

      if [[ -z "${testlist}" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { echo "No tests match ${split_chunk}"; die; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest-upgrade to run only first test found as no tests in split ${split_chunk} were found"
          testlist="$( _list_tests "distributed"  | grep "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout="$(_timeout_for "test.distributed.timeout")" -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "jvm-dtest-upgrade-repeat")
      _build_all_dtest_jars
      _run_testlist "distributed" "testclasslist" "$(_get_env_var 'REPEATED_TESTS')" "${split_chunk}" "$(_timeout_for 'test.distributed.timeout')" "$(_get_env_var 'REPEATED_TESTS_COUNT')"
      ;;
    "cqlsh-test")
      ./pylib/cassandra-cqlsh-tests.sh $(pwd)
      ;;
    *)
      echo "Unrecognized test type \"${target}\""
      exit 1
      ;;
  esac

  # merge all unit xml files into one, and print summary test numbers
  ant -quiet -silent generate-unified-test-report

  popd  >/dev/null
}

_main "$@"
