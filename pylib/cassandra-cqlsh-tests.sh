#!/bin/bash -x
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

################################
#
# Prep
#
################################

WORKSPACE=$1

if [ "${WORKSPACE}" = "" ]; then
    echo "Specify Cassandra source directory"
    exit
fi

PYTHON_VERSION=python3

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export CCM_CONFIG_DIR=${WORKSPACE}/.ccm
export NUM_TOKENS="16"
export CASSANDRA_DIR=${WORKSPACE}
export TESTSUITE_NAME="cqlshlib.${PYTHON_VERSION}"

if [ -z "$CASSANDRA_USE_JDK11" ]; then
    export CASSANDRA_USE_JDK11=false
fi

if [ "$CASSANDRA_USE_JDK11" = true ] ; then
    TESTSUITE_NAME="${TESTSUITE_NAME}.jdk11"
else
    TESTSUITE_NAME="${TESTSUITE_NAME}.jdk8"
    unset JAVA11_HOME
fi

ant -buildfile ${CASSANDRA_DIR}/build.xml realclean
# Loop to prevent failure due to maven-ant-tasks not downloading a jar..
for x in $(seq 1 3); do
    ant -buildfile ${CASSANDRA_DIR}/build.xml jar
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

# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails
virtualenv --python=$PYTHON_VERSION venv
source venv/bin/activate
# 3.11 needs the newest pip
curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_VERSION

pip install -r ${CASSANDRA_DIR}/pylib/requirements.txt
pip freeze

if [ "$cython" = "yes" ]; then
    TESTSUITE_NAME="${TESTSUITE_NAME}.cython"
    pip install "Cython>=0.29.15,<3.0"
    cd pylib/; python setup.py build_ext --inplace
    cd ${WORKSPACE}
else
    TESTSUITE_NAME="${TESTSUITE_NAME}.no_cython"
fi

################################
#
# Main
#
################################

ccm remove test || true # in case an old ccm cluster is left behind
ccm create test -n 1 --install-dir=${CASSANDRA_DIR}
ccm updateconf "user_defined_functions_enabled: true"
ccm updateconf "scripted_user_defined_functions_enabled: true"

version_from_build=$(ccm node1 versionfrombuild)
export pre_or_post_cdc=$(python -c """from distutils.version import LooseVersion
print (\"postcdc\" if LooseVersion(\"${version_from_build}\") >= \"3.8\" else \"precdc\")
""")
case "${pre_or_post_cdc}" in
    postcdc)
        ccm updateconf "cdc_enabled: true"
        ;;
    precdc)
        :
        ;;
    *)
        echo "${pre_or_post_cdc}" is an invalid value.
        exit 1
        ;;
esac

ccm start --wait-for-binary-proto

cd ${CASSANDRA_DIR}/pylib/cqlshlib/

set +e # disable immediate exit from this point
pytest --junitxml=${WORKSPACE}/cqlshlib.xml
RETURN="$?"

sed -i "s/testsuite errors=\(\".*\"\) failures=\(\".*\"\) hostname=\(\".*\"\) name=\"pytest\"/testsuite errors=\1 failures=\2 hostname=\3 name=\"${TESTSUITE_NAME}\"/g" ${WORKSPACE}/cqlshlib.xml
sed -i "s/testcase classname=\"cqlshlib./testcase classname=\"${TESTSUITE_NAME}./g" ${WORKSPACE}/cqlshlib.xml

ccm remove

################################
#
# Clean
#
################################

# /virtualenv
deactivate

# circleci needs non-zero exit on failures, jenkins need zero exit to process the test failures
if ! command -v circleci >/dev/null 2>&1
then
    exit 0
else
    exit ${RETURN}
fi
