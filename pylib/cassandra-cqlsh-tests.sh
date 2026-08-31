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
PYTHON_VERSION=$2

if [ "${WORKSPACE}" = "" ]; then
    echo "Specify Cassandra source directory"
    exit
fi

if [ "${PYTHON_VERSION}" = "" ]; then
    PYTHON_VERSION=python3
fi

if [ "${PYTHON_VERSION}" != "python3" -a "${PYTHON_VERSION}" != "python2" ]; then
    echo "Specify Python version python3 or python2"
    exit
fi

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export CCM_CONFIG_DIR=${WORKSPACE}/.ccm
export NUM_TOKENS="32"
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
# Use the stdlib venv module (python3.X-venv is installed for every python in the image)
# instead of the `virtualenv` tool: the image's apt virtualenv is distro-patched to never
# download wheels and is too old to seed the newest pythons, and mixing a pip-installed
# virtualenv with the image's own dist-info entrypoints is version-minefield.  ensurepip
# seeds a usable pip without any network access; the get-pip step below then brings the
# venv's pip up to the newest release supporting this python.
$PYTHON_VERSION -m venv venv
source venv/bin/activate
# 3.11 needs the newest pip, 3.8 and older have specific legacy get-pip urls
PYTHON_MAJOR_MINOR=$($PYTHON_VERSION -V 2>&1 | awk '{print $2}' | cut -d. -f1,2)
if [[ "$(printf '%s\n' "$PYTHON_MAJOR_MINOR" "3.8" | sort -V | head -n1)" == "$PYTHON_MAJOR_MINOR" ]]; then
    curl -sS https://bootstrap.pypa.io/pip/${PYTHON_MAJOR_MINOR}/get-pip.py | $PYTHON_VERSION
else
    curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_VERSION
fi

# current setuptools no longer ships pkg_resources, which these old-style (setup.py +
# ez_setup.py) git packages need: build against a venv-local setuptools that still has it,
# instead of pip's isolated build env (which would fetch the latest, broken-for-this one).
# The git requirements are also installed non-editable: cassandra-driver predates PEP 660,
# and old pips (3.8's get-pip) fall back to `setup.py develop` for editable VCS installs,
# which breaks against ccm's packaging<21 pin.  pbr and Cython are the packages' declared
# build requirements (normally fetched into pip's isolated build env, which we bypass).
pip install "setuptools>=64,<81" pbr "Cython>=0.29.15,<3.0"
while IFS= read -r git_req ; do
    [ -n "${git_req}" ] && pip install --no-build-isolation "$(echo "${git_req}" | sed 's/^-e[[:space:]]*//')"
done < <(grep '^-e git' ${CASSANDRA_DIR}/pylib/requirements.txt)
pip install --no-build-isolation -r <(grep -v '^-e git' ${CASSANDRA_DIR}/pylib/requirements.txt)
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
ccm updateconf "enable_user_defined_functions: true"
ccm updateconf "enable_scripted_user_defined_functions: true"

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
pytest
RETURN="$?"

ccm remove
# hack around --xunit-prefix-with-testsuite-name not being available in nose 1.3.7
sed -i "s/testsuite name=\"nosetests\"/testsuite name=\"${TESTSUITE_NAME}\"/g" nosetests.xml
sed -i "s/testcase classname=\"cqlshlib./testcase classname=\"${TESTSUITE_NAME}./g" nosetests.xml
mv nosetests.xml ${WORKSPACE}/cqlshlib.xml

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
