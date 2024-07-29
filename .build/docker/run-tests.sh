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

# A wrapper script to run-tests.sh (or dtest-python.sh) in docker.
#  Can split (or grep) the test list into multiple docker runs, collecting results.

[ $DEBUG ] && set -x

# help
if [ "$#" -lt 1 ] || [ "$#" -gt 3 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-tests.sh test_type [split_chunk|test_regexp] [java_version]"
    echo ""
    echo "        default split_chunk is 1/1"
    echo "        default java_version is what 'java.default' specifies in build.xml"
    exit 1
fi

error() {
    echo >&2 $2;
    set -x
    exit $1
}

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname "$0")/../..)"
[ "x${cassandra_dtest_dir}" != "x" ] || cassandra_dtest_dir="${cassandra_dir}/../cassandra-dtest"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
[ "x${m2_dir}" != "x" ] || m2_dir="${HOME}/.m2/repository"
[ -d "${build_dir}" ] || { mkdir -p "${build_dir}" ; }
[ -d "${m2_dir}" ] || { mkdir -p "${m2_dir}" ; }

# pre-conditions
command -v docker >/dev/null 2>&1 || { error 1 "docker needs to be installed"; }
command -v bc >/dev/null 2>&1 || { error 1 "bc needs to be installed"; }
command -v timeout >/dev/null 2>&1 || { error 1 "timeout needs to be installed"; }
(docker info >/dev/null 2>&1) || { error 1 "docker needs to running"; }
[ -f "${cassandra_dir}/build.xml" ] || { error 1 "${cassandra_dir}/build.xml must exist"; }
[ -f "${cassandra_dir}/.build/run-tests.sh" ] || { error 1 "${cassandra_dir}/.build/run-tests.sh must exist"; }

# arguments
target=$1
split_chunk="1/1"
[ "$#" -gt 1 ] && split_chunk=$2
java_version=$3

test_script="run-tests.sh"
java_version_default=`grep 'property\s*name="java.default"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "x${java_version}" == "x" ] ; then
    echo "Defaulting to java ${java_version_default}"
    java_version="${java_version_default}"
fi

regx_java_version="(${java_version_supported//,/|})"
if [[ ! "${java_version}" =~ $regx_java_version ]]; then
    error 1 "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
fi

# allow python version override, otherwise default to current python version or 3.8
if [ "x" == "x${python_version}" ] ; then
    command -v python >/dev/null 2>&1 && python_version="$(python -V 2>&1 | awk '{print $2}' | awk -F'.' '{print $1"."$2}')"
    python_version="${python_version:-3.8}"
fi

# print debug information on versions
docker --version

pushd ${cassandra_dir}/.build >/dev/null

# build test image
dockerfile="ubuntu2004_test.docker"
image_tag="$(md5sum docker/${dockerfile} | cut -d' ' -f1)"
image_name="apache/cassandra-${dockerfile/.docker/}:${image_tag}"
docker_mounts="-v ${cassandra_dir}:/home/cassandra/cassandra -v "${build_dir}":/home/cassandra/cassandra/build -v ${m2_dir}:/home/cassandra/.m2/repository"
# HACK hardlinks in overlay are buggy, the following mount prevents hardlinks from being used. ref $TMP_DIR in .build/run-tests.sh
docker_mounts="${docker_mounts} -v "${build_dir}/tmp":/home/cassandra/cassandra/build/tmp"

# Look for existing docker image, otherwise build
if ! ( [[ "$(docker images -q ${image_name} 2>/dev/null)" != "" ]] ) ; then
  echo "Build image not found locally, pulling image ${image_name}..."
  if ! ( docker pull -q ${image_name} >/dev/null 2>/dev/null ) ; then
    # Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
    echo "Building docker image..."
    until docker build -t ${image_name} -f docker/${dockerfile} .  ; do
      echo "docker build failed… trying again in 10s… "
      sleep 10
    done
    echo "Docker image ${image_name} has been built"
  else
    echo "Successfully pulled build image."
  fi
else
  echo "Found build image locally."
fi

pushd ${cassandra_dir} >/dev/null

# Optional lookup of Jenkins environment to see how many executors on this machine. `jenkins_executors=1` is used for anything non-jenkins.
jenkins_executors=1
if [[ ! -z ${JENKINS_URL+x} ]] && [[ ! -z ${NODE_NAME+x} ]] ; then
    fetched_jenkins_executors=$(curl -s --retry 9 --retry-connrefused --retry-delay 1 "${JENKINS_URL}/computer/${NODE_NAME}/api/json?pretty=true" | grep 'numExecutors' | awk -F' : ' '{print $2}' | cut -d',' -f1)
    # use it if we got a valid number (despite retry settings the curl above can still fail
    [[ ${fetched_jenkins_executors} =~ '^[0-9]+$' ]] && jenkins_executors=${fetched_jenkins_executors}
fi

# find host's available cores and mem
cores=$(docker run --rm alpine:3.19.1 nproc --all) || { error 1 "Unable to check available CPU cores"; }

case $(uname) in
    "Linux")
        mem=$(docker run --rm alpine:3.19.1 free -b | grep Mem: | awk '{print $2}') || { error 1 "Unable to check available memory"; }
        ;;
    "Darwin")
        mem=$(sysctl -n hw.memsize) || { error 1 "Unable to check available memory"; }
        ;;
    *)
        error 1 "Unsupported operating system, expected Linux or Darwin"
esac

# figure out resource limits, scripts, and mounts for the test type
docker_flags="-m 5g --memory-swap 5g"
case ${target} in
    "build_dtest_jars")
    ;;
    "stress-test" | "fqltool-test" )
        [[ ${mem} -gt $((1 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { error 1 "${target} require minimum docker memory 1g (per jenkins executor (${jenkins_executors})), found ${mem}"; }
    ;;
    # test-burn doesn't have enough tests in it to split beyond 8, and burn and long we want a bit more resources anyway
    "microbench" | "test-burn" | "long-test" | "cqlsh-test" )
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { error 1 "${target} require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; }
    ;;
    "simulator-dtest")
        [[ ${mem} -gt $((15 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { error 1 "${target} require minimum docker memory 16g (per jenkins executor (${jenkins_executors})), found ${mem}"; }
        docker_flags="-m 15g --memory-swap 15g"
    ;;
    "dtest" | "dtest-novnode" | "dtest-latest" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" | "dtest-upgrade-novnode"| "dtest-upgrade-large" | "dtest-upgrade-novnode-large")
        [ -f "${cassandra_dtest_dir}/dtest.py" ] || { error 1 "${cassandra_dtest_dir}/dtest.py not found. please specify 'cassandra_dtest_dir' to point to the local cassandra-dtest source"; }
        test_script="run-python-dtests.sh"
        docker_mounts="${docker_mounts} -v ${cassandra_dtest_dir}:/home/cassandra/cassandra-dtest"
        [[ ${mem} -gt $((15 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { error 1 "${target} require minimum docker memory 16g (per jenkins executor (${jenkins_executors})), found ${mem}"; }
        docker_flags="-m 15g --memory-swap 15g"
    ;;
    "test"| "test-cdc" | "test-compression" | "test-oa" | "test-system-keyspace-directory" | "test-latest" | "jvm-dtest" | "jvm-dtest-upgrade" | "jvm-dtest-novnode" | "jvm-dtest-upgrade-novnode")
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { error 1 "${target} require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; }
    ;;
    *)
    error 1 "unrecognized test type \"${target}\""
    ;;
esac

docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} )" | bc)
docker_cpus_limit=$(docker info | grep CPUs | cut -d" " -f3)
if (( $(echo "${docker_cpus} > ${docker_cpus_limit}" |bc -l) )) ; then
    echo "WARNING: requested more cpus (${docker_cpus}) than docker cpu limit (${docker_cpus_limit}), reducing cpus…"
    docker_cpus=${docker_cpus_limit}
fi

# hack: long-test does not handle limited CPUs
if [ "${target}" != "long-test" ] ; then
    docker_flags="--cpus=${docker_cpus} ${docker_flags}"
fi

docker_flags="${docker_flags} -d --rm"

# make sure build_dir is good
mkdir -p "${build_dir}/tmp" || true
mkdir -p "${build_dir}/test/logs" || true
mkdir -p "${build_dir}/test/output" || true
mkdir -p "${build_dir}/test/reports" || true
chmod -R ag+rwx "${build_dir}"

# define testtag.extra so tests can be aggregated together. (jdk is already appended in build.xml)
case "${target}" in
    "cqlsh-test" | "dtest" | "dtest-novnode" | "dtest-latest" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" | "dtest-upgrade-large" | "dtest-upgrade-novnode" | "dtest-upgrade-novnode-large" )
        ANT_OPTS="-Dtesttag.extra=_$(arch)_python${python_version/./-}"
        # intentionally not TMP_DIR
        DTEST_TMPDIR_LOCAL="$(mktemp -d ${build_dir}/run-python-dtest.XXXXXX)"
    ;;
    "jvm-dtest-novnode" | "jvm-dtest-upgrade-novnode" )
        ANT_OPTS="-Dtesttag.extra=_$(arch)_novnode"
    ;;
    *)
        ANT_OPTS="-Dtesttag.extra=_$(arch)"
    ;;
esac

# cython can be used for cqlsh-test
if [ "$cython" == "yes" ]; then
    [ "${target}" == "cqlsh-test" ] || { error 1 "cython is only supported for cqlsh-test"; }
    ANT_OPTS="${ANT_OPTS}_cython"
else
    cython="no"
fi

# the docker container's env
docker_envs="--env TEST_SCRIPT=${test_script} --env JAVA_VERSION=${java_version} --env PYTHON_VERSION=${python_version} --env cython=${cython} --env ANT_OPTS=\"${ANT_OPTS}\""
if [ -n "${DTEST_TMPDIR_LOCAL}" ] ; then
    DTEST_TMPDIR_REMOTE="$(sed "s:${build_dir}:/home/cassandra/cassandra/build:" <<< ${DTEST_TMPDIR_LOCAL})"
    docker_envs="${docker_envs} --env TMPDIR=${DTEST_TMPDIR_REMOTE} --env CCM_CONFIG_DIR=${DTEST_TMPDIR_REMOTE}/.ccm"
fi
[ $DEBUG ] && docker_envs="${docker_envs} --env DEBUG=1"

split_str="0_0"
if [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
    split_str="${split_chunk/\//_}"
fi

# git worktrees need their original working directory (in its original path)
if [ -f ${cassandra_dir}/.git ] ; then
    git_location="$(cat ${cassandra_dir}/.git | awk -F".git" '{print $1}' | awk '{print $2}')"
    docker_volume_opt="${docker_volume_opt} -v${git_location}:${git_location}"
fi

random_string="$(LC_ALL=C tr -dc A-Za-z0-9 </dev/urandom | head -c 6 ; echo '')"

container_name="cassandra_${dockerfile/.docker/}_${target}_jdk${java_version/./-}_arch-$(arch)_python${python_version/./-}_${split_str}__${random_string}"

logfile="${build_dir}/test/logs/docker_attach_${container_name}.log"

# Docker commands:
#  set java to java_version
#  execute the run_script
docker_command="source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
            \${CASSANDRA_DIR}/.build/docker/_docker_init_tests.sh ${target} ${split_chunk} ; exit \$?"

# start the container, timeout after 4 hours
docker_id=$(docker run --name ${container_name} ${docker_flags} ${docker_envs} ${docker_mounts} ${docker_volume_opt} ${image_name} sleep 4h)

echo "Running container ${container_name} ${docker_id}"

docker exec --user root ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_create_user.sh cassandra $(id -u) $(id -g)" | tee -a ${logfile}
docker exec --user root ${container_name} update-alternatives --set python /usr/bin/python${python_version} | tee -a ${logfile}

if [ -n "${DTEST_TMPDIR_LOCAL}" ] && [[ "${target}" =~ ^dtest-upgrade ]] ; then
    # prepopulate a tmp ccm repository directory, if running dtest-upgrade tests
    docker exec --user cassandra ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_copy_ccm_repositories.sh" | tee -a ${logfile}
    trap 'nohup rm -rf "${DTEST_TMPDIR_LOCAL}/.ccm/repository" >/dev/null 2>&1 &' EXIT
fi

# capture logs and status
set -o pipefail
docker exec --user cassandra ${container_name} bash -c "${docker_command}" | tee -a ${logfile}
status=$?
set +o pipefail

if [ "$status" -ne 0 ] ; then
    echo "${docker_id} failed (${status}), debug…"
    docker inspect ${docker_id}
    echo "–––"
    docker logs ${docker_id}
    echo "–––"
    docker ps -a
    echo "–––"
    docker info
    echo "–––"
    echo "Failure."
fi
# docker stop in background, ignore errors
( nohup docker stop ${docker_id} >/dev/null 2>/dev/null & )

xz -f ${logfile} 2>/dev/null

popd >/dev/null
popd >/dev/null
echo "+ exit ${status}"
exit ${status}
