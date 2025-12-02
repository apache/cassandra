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

set -x

DTEST_BRANCH=master
TOTAL_GROUP_NUMBER=20

echo "CASSANDRA_BRANCH: $CASSANDRA_BRANCH"
echo "DTEST_BRANCH: $DTEST_BRANCH"
echo "DTEST_GROUP_ID: $DTEST_GROUP_ID"

# env setup (Java11)
source "./.jenkins/env_setup.sh"
j11Setup

# clone dtest repo
git clean -xdff
git clone -b "${DTEST_BRANCH:-master}" \
    gitolite@code.uber.internal:infra/cassandra-dtest

GRADLE_ARGS=("-PCQLGateway")

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export NUM_TOKENS="32"

if [ "x$CASSANDRA_MESOS_VERSION" = "x" ] && [ "x$CASSANDRA_MESOS_BRANCH" = "x" ]; then
    CASSANDRA_DIR="$PROJECT_DIR"
else
    echo "CASSANDRA_MESOS_VERSION: $CASSANDRA_MESOS_VERSION"
    echo "CASSANDRA_MESOS_BRANCH: $CASSANDRA_MESOS_BRANCH"
    if [ "x$CASSANDRA_MESOS_BRANCH" = "x" ]; then
        if ! wget "http://artifactory.uber.internal:4587/artifactory/libs-release-local/com/uber/cassandra/cassandra3/$CASSANDRA_MESOS_VERSION/cassandra3-$CASSANDRA_MESOS_VERSION.tar.gz"; then
            echo "Failed to download cassandra-mesos distribution $CASSANDRA_MESOS_VERSION. Exiting..."
            exit 1
        fi
        tar zxvf "cassandra3-$CASSANDRA_MESOS_VERSION.tar.gz"
        for APACHE_CASSANDRA_DIR in "$PROJECT_DIR"/apache-cassandra-*; do
            CASSANDRA_DIR="$APACHE_CASSANDRA_DIR"
            break
        done
    else
        git clone -b "${CASSANDRA_MESOS_BRANCH:-master}" \
            gitolite@code.uber.internal:infra/cassandra-mesos
        pushd cassandra-mesos
        # XXX: This requires a snapshot build of the following cassandra version
        # Use the cassandra-publish Jenkins job to publish a latest snapshot
        # before running this so that the latest change can be picked up
        sed -i 's/CassandraVersion=\([0-9\.]*\).*/CassandraVersion=\1-SNAPSHOT/' \
            gradle.properties
        ./gradlew "${GRADLE_ARGS[@]}" clean build --stacktrace
        popd
        CMBIN_DIR="$PROJECT_DIR/cassandra-mesos/bin"
        for APACHE_CASSANDRA_DIR in "$CMBIN_DIR"/apache-cassandra-*; do
            CASSANDRA_DIR="$APACHE_CASSANDRA_DIR"
            break
        done
    fi
    if [ ! -d "$CASSANDRA_DIR" ]; then
        echo "$CASSANDRA_DIR is not a directory. Exiting..."
        exit 1
    fi
    CASSANDRA_NAME="$(basename "$CASSANDRA_DIR")"
    CASSANDRA_VERSION=${CASSANDRA_NAME##apache-cassandra-}

    # CCM reads build.xml for version, so we copy it and change the version in place
    cp "$PROJECT_DIR/build.xml" "$CASSANDRA_DIR"
    # shellcheck disable=SC1117
    sed -i "s#<property name=\"base.version\" value=\"\(.*\)\"/>#<property name=\"base.version\" value=\"$CASSANDRA_VERSION\"/>#" \
        "$CASSANDRA_DIR/build.xml"

    # Use vanilla cassandra files
    cp "$PROJECT_DIR/bin/cassandra" "$CASSANDRA_DIR/bin/cassandra"
    cp "$PROJECT_DIR/conf/logback-tools.xml" "$CASSANDRA_DIR/conf/logback-tools.xml"

    # Download byteman for test environment required by dtest and CCM
    mkdir -p "$CASSANDRA_DIR/build/lib/jars"
    pushd "$CASSANDRA_DIR/build/lib/jars"
    set -e
    wget "https://repo1.maven.org/maven2/org/jboss/byteman/byteman/3.0.3/byteman-3.0.3.jar"
    wget "https://repo1.maven.org/maven2/org/jboss/byteman/byteman-bmunit/3.0.3/byteman-bmunit-3.0.3.jar"
    wget "https://repo1.maven.org/maven2/org/jboss/byteman/byteman-install/3.0.3/byteman-install-3.0.3.jar"
    wget "https://repo1.maven.org/maven2/org/jboss/byteman/byteman-submit/3.0.3/byteman-submit-3.0.3.jar"
    set +e
    popd

    # Delete lines of production heap size settings to use the defaults
    sed -i -e '/-Xms30500M/d' -e '/-Xmx30500M/d' -e '/-Xmn1024M/d' \
        "$CASSANDRA_DIR/conf/jvm.options"

    # Tune jvm.options.uber for dtest to pass instead of deleting it because
    # we want our environment to be as close to production as possible

    # For bootstrap_test.TestBootstrap.simultaneous_bootstrap_test
    sed -i 's/-Dcassandra.consistent.rangemovement=false/-Dcassandra.consistent.rangemovement=true/g' \
        "$CASSANDRA_DIR/conf/jvm.options.uber"

    # For commitlog_test.TestCommitLog.test_bad_crc
    sed -i 's/-Dcassandra.commitlog.ignorereplayerrors=true/-Dcassandra.commitlog.ignorereplayerrors=false/g' \
        "$CASSANDRA_DIR/conf/jvm.options.uber"

    # For configuration_test.TestConfiguration.change_durable_writes_test, see
    # also cassandra-dtest/README.md
    sed -i 's/-XX:+PerfDisableSharedMem/#-XX:+PerfDisableSharedMem/g' \
        "$CASSANDRA_DIR/conf/jvm.options.uber"

    # This is temporary fix for dtest: once T764004 and D1683281 are done, the
    # following can be cleaned up
    rm "$CASSANDRA_DIR/lib/asm-3.1.jar"

    # export required environment variables for jmxtrans
    export UBER_DATACENTER=test
    export HOSTNAME=localhost
fi
export CASSANDRA_DIR

# Set up Ant and build Cassandra from source if CASSANDRA_MESOS_VERSION is not set
if [ "x$CASSANDRA_MESOS_VERSION" = "x" ] && [ "x$CASSANDRA_MESOS_BRANCH" = "x" ]; then
    antSetup

    # Loop to prevent failure due to maven-ant-tasks not downloading a jar..
    for _ in $(seq 1 3); do
        ant clean jar
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
else
    echo 'CASSANDRA_MESOS_VERSION or CASSANDRA_MESOS_BRANCH is set, so skipped configuring Ant and building from source.'
fi

python3 --version
# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails
virtualenv --python=python3 ~/dtest
# shellcheck disable=SC1091
source ~/dtest/bin/activate
#pip install --upgrade setuptools
pip install -r cassandra-dtest/requirements.txt
pip freeze

cd cassandra-dtest
set +e # disable immediate exit from this point

pytest --cassandra-dir="${CASSANDRA_DIR}"  --use-vnodes --num-tokens=16 --splits $TOTAL_GROUP_NUMBER --group $DTEST_GROUP_ID --store-durations --durations-path latest_test_durations -x

RETURN="$?"
if [ "${RETURN}" -ne "0" ]; then
    echo "Build failed with exit code: ${RETURN}"
    exit ${RETURN}
fi

# /virtualenv
deactivate

mkdir test_durations_dir
cat .test_durations
mv .test_durations test_durations_dir/test_durations${DTEST_GROUP_ID}

# Exit cleanly for usable "Unstable" status
exit 0
