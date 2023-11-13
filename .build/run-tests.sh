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

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
command -v git >/dev/null 2>&1 || { echo >&2 "git needs to be installed"; exit 1; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }
[ -d "${DIST_DIR}" ] || { mkdir -p "${DIST_DIR}" ; }


# help
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-tests.sh test_type [split_chunk|test_regexp]"
    echo ""
    echo "        default split_chunk is 1/1"
    exit 1
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
    command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; exit 1; }
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
    local _split_chunk=$3
    local _test_timeout=$4
    testlist="$( _list_tests "${_target_prefix}" | _split_tests "${_split_chunk}")"
    if [[ "${_split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
      if [[ -z "${testlist}" ]]; then
        # something has to run in the split to generate a junit xml result
        echo "Hacking ${_target_prefix} ${_testlist_target} to run only first test found as no tests in split ${_split_chunk} were found"
        testlist="$( _list_tests "${_target_prefix}" | head -n1)"
      fi
    else
      if [[ -z "${testlist}" ]]; then
        echo "No tests match ${_split_chunk}"
        exit 1
      fi
    fi
    ant $_testlist_target -Dtest.classlistprefix="${_target_prefix}" -Dtest.classlistfile=<(echo "${testlist}") -Dtest.timeout="${_test_timeout}" ${ANT_TEST_OPTS} || echo "failed ${_target_prefix} ${_testlist_target}  ${split_chunk}"
}

_main() {
  # parameters
  local -r target="${1:-}"

  local -r split_chunk="${2:-'1/1'}" # Optional: pass in chunk or regexp to test. Chunks formatted as "K/N" for the Kth chunk of N chunks
  # check split_chunk is compatible with target (if not a regexp)
  if [[ "${_split_chunk}" =~ ^\d+/\d+$ ]] && [[ "1/1" != "${split_chunk}" ]] ; then
    case ${target} in
      "stress-test" | "fqltool-test" | "microbench" | "cqlsh-test" | "simulator-dtest")
          echo "Target ${target} does not suport splits."
          exit 1
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
    exit 1
  fi

  # check project is already built. no cleaning is done, so jenkins unstash works, beware.
  [[ -f "${DIST_DIR}/apache-cassandra-${version}.jar" ]] || [[ -f "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar" ]] || { echo "Project must be built first. Use \`ant jar\`. Build directory is ${DIST_DIR} with: $(ls ${DIST_DIR})"; exit 1; }

  # check if dist artifacts exist, this breaks the dtests
  [[ -d "${DIST_DIR}/dist" ]] && { echo "tests don't work when build/dist ("${DIST_DIR}/dist") exists (from \`ant artifacts\`)"; exit 1; }

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
      _run_testlist "unit" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-cdc")
      _run_testlist "unit" "testclasslist-cdc" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-compression")
      _run_testlist "unit" "testclasslist-compression" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-oa")
      _run_testlist "unit" "testclasslist-oa" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-system-keyspace-directory")
      _run_testlist "unit" "testclasslist-system-keyspace-directory" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-trie")
      _run_testlist "unit" "testclasslist-trie" "${split_chunk}" "$(_timeout_for 'test.timeout')"
      ;;
    "test-burn")
      _run_testlist "burn" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.burn.timeout')"
      ;;
    "long-test")
      _run_testlist "long" "testclasslist" "${split_chunk}" "$(_timeout_for 'test.long.timeout')"
      ;;
    "simulator-dtest")
      ant test-simulator-dtest ${ANT_TEST_OPTS} || echo "failed ${target}"
      ;;
    "jvm-dtest" | "jvm-dtest-novnode")
      [ "jvm-dtest-novnode" == "${target}" ] || ANT_TEST_OPTS="${ANT_TEST_OPTS} -Dcassandra.dtest.num_tokens=16"
      testlist=$( _list_tests "distributed" | grep -v "upgrade" | _split_tests "${split_chunk}")
      if [[ -z "$testlist" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { echo "No tests match ${split_chunk}"; exit 1; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest to run only first test found as no tests in split ${split_chunk} were found"
          testlist="$( _list_tests "distributed"  | grep -v "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout=$(_timeout_for "test.distributed.timeout") -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "jvm-dtest-upgrade" | "jvm-dtest-upgrade-novnode")
      _build_all_dtest_jars
      [ "jvm-dtest-upgrade-novnode" == "${target}" ] || ANT_TEST_OPTS="${ANT_TEST_OPTS} -Dcassandra.dtest.num_tokens=16"
      testlist=$( _list_tests "distributed"  | grep "upgrade" | _split_tests "${split_chunk}")
      if [[ -z "${testlist}" ]]; then
          [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]] || { echo "No tests match ${split_chunk}"; exit 1; }
          # something has to run in the split to generate a junit xml result
          echo "Hacking jvm-dtest-upgrade to run only first test found as no tests in split ${split_chunk} were found"
          testlist="$( _list_tests "distributed"  | grep "upgrade" | head -n1)"
      fi
      ant testclasslist -Dtest.classlistprefix=distributed -Dtest.timeout=$(_timeout_for "test.distributed.timeout") -Dtest.classlistfile=<(echo "${testlist}") ${ANT_TEST_OPTS} || echo "failed ${target} ${split_chunk}"
      ;;
    "cqlsh-test")
      ./pylib/cassandra-cqlsh-tests.sh $(pwd)
      ;;
    *)
      echo "unrecognized test type \"${target}\""
      exit 1
      ;;
  esac

  # merge all unit xml files into one, and print summary test numbers
  ant -quiet -silent generate-unified-test-report

  popd  >/dev/null
}

_main "$@"
