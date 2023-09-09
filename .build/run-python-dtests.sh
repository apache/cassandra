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
# Wrapper script for running a split or regexp of a pytest run from cassandra-dtest
#

################################
#
# Prep
#
################################


# help
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-python-dtest.sh test_type [split_chunk|test_regexp]"
    echo ""
    echo "        default split_chunk is 1/1"
    exit 1
fi

# Pass in target to run, defaults to dtest
DTEST_TARGET="${1:-dtest}"
# Optional: pass in chunk to test, formatted as "K/N" for the Kth chunk of N chunks
DTEST_SPLIT_CHUNK="$2"

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/..)"
[ "x${CASSANDRA_DTEST_DIR}" != "x" ] || CASSANDRA_DTEST_DIR="${CASSANDRA_DIR}/../cassandra-dtest"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${CASSANDRA_DIR}/build"

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="1024M"
export CCM_HEAP_NEWSIZE="512M"
export NUM_TOKENS="16"
#Have Cassandra skip all fsyncs to improve test performance and reliability
export CASSANDRA_SKIP_SYNC=true
export TMPDIR="$(mktemp -d ${DIST_DIR}/run-python-dtest.XXXXXX)"
unset CASSANDRA_HOME

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
command -v virtualenv >/dev/null 2>&1 || { echo >&2 "virtualenv needs to be installed"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }
[ -d "${DIST_DIR}" ] || { mkdir -p "${DIST_DIR}" ; }

java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
version=$(grep 'property\s*name=\"base.version\"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
java_version_default=`grep 'property\s*name="java.default"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "${java_version}" -eq 17 ] && [[ "${target}" == "dtest-upgrade" ]] ; then
    echo "Invalid JDK${java_version}. Only overlapping supported JDKs can be used when upgrading, as the same jdk must be used over the upgrade path."
    exit 1
fi

python_version=$(python -V 2>&1 | awk '{print $2}' | awk -F'.' '{print $1"."$2}')
python_regx_supported_versions="^(3.7|3.8|3.11)$"
[[ $python_version =~ $python_regx_supported_versions ]] || { echo "Python ${python_version} not supported."; exit 1; }

# check project is already built. no cleaning is done, so jenkins unstash works, beware.
[[ -f "${DIST_DIR}/apache-cassandra-${version}.jar" ]] || [[ -f "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar" ]] || { echo "Project must be built first. Use \`ant jar\`. Build directory is ${DIST_DIR} with: $(ls ${DIST_DIR})"; exit 1; }

# check if dist artifacts exist, this breaks the dtests
[[ -d "${DIST_DIR}/dist" ]] && { echo "dtests don't work when build/dist ("${DIST_DIR}/dist") exists (from \`ant artifacts\`)"; exit 1; }

# print debug information on versions
java -version
ant -version
python --version
virtualenv --version

# cheap trick to ensure dependency libraries are in place. allows us to stash only project specific build artifacts.
ant -quiet -silent resolver-dist-lib

# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails

# fresh virtualenv and test logs results everytime
[[ "/" == "${DIST_DIR}" ]] || rm -rf "${DIST_DIR}/venv" "${DIST_DIR}/test/{html,output,logs}"

# re-use when possible the pre-installed virtualenv found in the cassandra-ubuntu2004_test docker image
virtualenv-clone ${BUILD_HOME}/env${python_version} ${DIST_DIR}/venv || virtualenv --python=python${python_version} ${DIST_DIR}/venv
source ${DIST_DIR}/venv/bin/activate
pip3 install --exists-action w -r ${CASSANDRA_DTEST_DIR}/requirements.txt
pip3 freeze

################################
#
# Main
#
################################

cd ${CASSANDRA_DTEST_DIR}

set +e # disable immediate exit from this point
if [ "${DTEST_TARGET}" = "dtest" ]; then
    DTEST_ARGS="--use-vnodes --num-tokens=${NUM_TOKENS} --skip-resource-intensive-tests"
elif [ "${DTEST_TARGET}" = "dtest-novnode" ]; then
    DTEST_ARGS="--skip-resource-intensive-tests --keep-failed-test-dir"
elif [ "${DTEST_TARGET}" = "dtest-offheap" ]; then
    DTEST_ARGS="--use-vnodes --num-tokens=${NUM_TOKENS} --use-off-heap-memtables --skip-resource-intensive-tests"
elif [ "${DTEST_TARGET}" = "dtest-large" ]; then
    DTEST_ARGS="--use-vnodes --num-tokens=${NUM_TOKENS} --only-resource-intensive-tests --force-resource-intensive-tests"
elif [ "${DTEST_TARGET}" = "dtest-large-novnode" ]; then
    DTEST_ARGS="--only-resource-intensive-tests --force-resource-intensive-tests"
elif [ "${DTEST_TARGET}" = "dtest-upgrade" ]; then
    DTEST_ARGS="--use-vnodes --num-tokens=${NUM_TOKENS} --execute-upgrade-tests --execute-upgrade-tests-only --upgrade-target-version-only --upgrade-version-selection all"
elif [ "${DTEST_TARGET}" = "dtest-upgrade-novnode" ]; then
    DTEST_ARGS="--execute-upgrade-tests --execute-upgrade-tests-only --upgrade-target-version-only --upgrade-version-selection all"
elif [ "${DTEST_TARGET}" = "dtest-upgrade-large" ]; then
    DTEST_ARGS="--use-vnodes --num-tokens=${NUM_TOKENS} --execute-upgrade-tests --execute-upgrade-tests-only --upgrade-target-version-only --upgrade-version-selection all --only-resource-intensive-tests --force-resource-intensive-tests"
elif [ "${DTEST_TARGET}" = "dtest-upgrade-novnode-large" ]; then
    DTEST_ARGS="--execute-upgrade-tests --execute-upgrade-tests-only --upgrade-target-version-only --upgrade-version-selection all --only-resource-intensive-tests --force-resource-intensive-tests"
else
    echo "Unknown dtest target: ${DTEST_TARGET}"
    exit 1
fi

touch ${DIST_DIR}/test_list.txt
./run_dtests.py --cassandra-dir=${CASSANDRA_DIR} ${DTEST_ARGS} --dtest-print-tests-only --dtest-print-tests-output=${DIST_DIR}/test_list.txt 2>&1 > ${DIST_DIR}/test_stdout.txt
[[ $? -eq 0 ]] || { cat ${DIST_DIR}/test_stdout.txt ; exit 1; }

if [[ "${DTEST_SPLIT_CHUNK}" =~ ^[0-9]+/[0-9]+$ ]]; then
    split_cmd=split
    ( split --help 2>&1 ) | grep -q "r/K/N" || split_cmd=gsplit
    command -v ${split_cmd} >/dev/null 2>&1 || { echo >&2 "${split_cmd} needs to be installed"; exit 1; }
    SPLIT_TESTS=$(${split_cmd} -n r/${DTEST_SPLIT_CHUNK} ${DIST_DIR}/test_list.txt)
elif [[ "x" != "x${DTEST_SPLIT_CHUNK}" ]] ; then
    SPLIT_TESTS=$(grep -e "${DTEST_SPLIT_CHUNK}" ${DIST_DIR}/test_list.txt)
    [[ "x" != "x${SPLIT_TESTS}" ]] || { echo "no tests match regexp \"${DTEST_SPLIT_CHUNK}\""; exit 1; }
else
    SPLIT_TESTS=$(cat ${DIST_DIR}/test_list.txt)
fi


PYTEST_OPTS="-vv --log-cli-level=DEBUG --junit-xml=${DIST_DIR}/test/output/nosetests.xml --junit-prefix=${DTEST_TARGET} -s"
pytest ${PYTEST_OPTS} --cassandra-dir=${CASSANDRA_DIR} --keep-failed-test-dir ${DTEST_ARGS} ${SPLIT_TESTS} 2>&1 | tee -a ${DIST_DIR}/test_stdout.txt

# tar up any ccm logs for easy retrieval
if ls ${TMPDIR}/*/test/*/logs/* &>/dev/null ; then
    mkdir -p ${DIST_DIR}/test/logs
    tar -C ${TMPDIR} -cJf ${DIST_DIR}/test/logs/ccm_logs.tar.xz ${TMPDIR}/*/test/*/logs
fi

# merge all unit xml files into one, and print summary test numbers
pushd ${CASSANDRA_DIR}/ >/dev/null
# remove <testsuites> wrapping elements. `ant generate-unified-test-report` doesn't like it`
sed -r "s/<[\/]?testsuites>//g" ${DIST_DIR}/test/output/nosetests.xml > ${TMPDIR}/nosetests.xml
cat ${TMPDIR}/nosetests.xml > ${DIST_DIR}/test/output/nosetests.xml
ant -quiet -silent generate-unified-test-report
popd  >/dev/null

################################
#
# Clean
#
################################

[[ "${TMPDIR}" == *"${DIST_DIR}/run-python-dtest."* ]] && rm -rf "${TMPDIR}"
unset TMPDIR
deactivate

# Exit cleanly for usable "Unstable" status
exit 0
