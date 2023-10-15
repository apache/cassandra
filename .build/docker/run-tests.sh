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
#
# A wrapper script to run-tests.sh (or dtest-python.sh) in docker.
#  Can split (or grep) the test list into multiple docker runs, collecting results.
#
# Each split chunk may be further parallelised over docker containers based on the host's available cpu and memory (and the test type).
#  Define env variable DISABLE_INNER_SPLITS to disable inner splitting.
#

# help
if [ "$#" -lt 1 ] || [ "$#" -gt 3 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-tests.sh test_type [split_chunk|test_regexp] [java_version]"
    echo ""
    echo "        default split_chunk is 1/1"
    echo "        default java_version is what 'java.default' specifies in build.xml"
    exit 1
fi

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname "$0")/../..)"
[ "x${cassandra_dtest_dir}" != "x" ] || cassandra_dtest_dir="${cassandra_dir}/../cassandra-dtest"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
[ -d "${build_dir}" ] || { mkdir -p "${build_dir}" ; }

# pre-conditions
command -v docker >/dev/null 2>&1 || { echo >&2 "docker needs to be installed"; exit 1; }
command -v bc >/dev/null 2>&1 || { echo >&2 "bc needs to be installed"; exit 1; }
command -v timeout >/dev/null 2>&1 || { echo >&2 "timeout needs to be installed"; exit 1; }
(docker info >/dev/null 2>&1) || { echo >&2 "docker needs to running"; exit 1; }
[ -f "${cassandra_dir}/build.xml" ] || { echo >&2 "${cassandra_dir}/build.xml must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/run-tests.sh" ] || { echo >&2 "${cassandra_dir}/.build/run-tests.sh must exist"; exit 1; }

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
    echo "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
    exit 1
fi

# allow python version override, otherwise default to current python version or 3.7
if [ "x" == "x${python_version}" ] ; then
    command -v python >/dev/null 2>&1 && python_version="$(python -V 2>&1 | awk '{print $2}' | awk -F'.' '{print $1"."$2}')"
    python_version="${python_version:-3.7}"
fi

# print debug information on versions
docker --version

pushd ${cassandra_dir}/.build >/dev/null

# build test image
dockerfile="ubuntu2004_test.docker"
image_tag="$(md5sum docker/${dockerfile} | cut -d' ' -f1)"
image_name="apache/cassandra-${dockerfile/.docker/}:${image_tag}"
docker_mounts="-v ${cassandra_dir}:/home/cassandra/cassandra -v "${build_dir}":/home/cassandra/cassandra/build -v ${HOME}/.m2/repository:/home/cassandra/.m2/repository"
# HACK hardlinks in overlay are buggy, the following mount prevents hardlinks from being used. ref $TMP_DIR in .build/run-tests.sh
docker_mounts="${docker_mounts} -v "${build_dir}/tmp":/home/cassandra/cassandra/build/tmp"

# Look for existing docker image, otherwise build
if ! ( [[ "$(docker images -q ${image_name} 2>/dev/null)" != "" ]] ) ; then
  # try docker login to increase dockerhub rate limits
  timeout -k 5 5 docker login >/dev/null 2>/dev/null
  if ! ( docker pull -q ${image_name} >/dev/null 2>/dev/null ) ; then
    # Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
    until docker build -t ${image_name} -f docker/${dockerfile} .  ; do
        echo "docker build failed… trying again in 10s… "
        sleep 10
    done
  fi
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
cores=1
command -v nproc >/dev/null 2>&1 && cores=$(nproc --all)
mem=1
# linux
command -v free >/dev/null 2>&1 && mem=$(free -b | grep Mem: | awk '{print $2}')
# macos
sysctl -n hw.memsize >/dev/null 2>&1 && mem=$(sysctl -n hw.memsize)

# figure out resource limits, scripts, and mounts for the test type
case ${target} in
    # test-burn doesn't have enough tests in it to split beyond 8, and burn and long we want a bit more resources anyway
    "stress-test" | "fqltool-test" | "microbench" | "test-burn" | "long-test" | "cqlsh-test" )
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
    ;;
    "dtest" | "dtest-novnode" | "dtest-offheap" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" | "dtest-upgrade-novnode"| "dtest-upgrade-large" | "dtest-upgrade-novnode-large" )
        [ -f "${cassandra_dtest_dir}/dtest.py" ] || { echo >&2 "${cassandra_dtest_dir}/dtest.py must exist"; exit 1; }
        [[ ${mem} -gt $((15 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "dtests require minimum docker memory 16g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
        test_script="run-python-dtests.sh"
        docker_mounts="${docker_mounts} -v ${cassandra_dtest_dir}:/home/cassandra/cassandra-dtest"
        # check that ${cassandra_dtest_dir} is valid
        [ -f "${cassandra_dtest_dir}/dtest.py" ] || { echo >&2 "${cassandra_dtest_dir}/dtest.py not found. please specify 'cassandra_dtest_dir' to point to the local cassandra-dtest source"; exit 1; }
    ;;
    "test"| "test-cdc" | "test-compression" | "test-oa" | "test-system-keyspace-directory" | "test-tries" | "jvm-dtest" | "jvm-dtest-upgrade")
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
        max_docker_runs_by_cores=$( echo "sqrt( ${cores} / ${jenkins_executors} )" | bc )
        max_docker_runs_by_mem=$(( ${mem} / ( 5 * 1024 * 1024 * 1024 * ${jenkins_executors} ) ))
    ;;
    *)
    echo "unrecognized test type \"${target}\""
    exit 1
    ;;
esac

docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} )" | bc)

# hack: long-test does not handle limited CPUs
if [ "${target}" == "long-test" ] ; then
    docker_flags="-m 5g --memory-swap 5g"
elif [[ "${target}" =~ dtest* ]] ; then
    docker_flags="--cpus=${docker_cpus} -m 15g --memory-swap 15g"
else
    docker_flags="--cpus=${docker_cpus} -m 5g --memory-swap 5g"
fi

docker_flags="${docker_flags} --env-file build/env.list -d --rm"

# make sure build_dir is good
mkdir -p ${build_dir}/tmp || true
mkdir -p ${build_dir}/test/logs || true
mkdir -p ${build_dir}/test/output || true
chmod -R ag+rwx ${build_dir}

# cython can be used for cqlsh-test
if [ "$cython" == "yes" ]; then
    [ "${target}" == "cqlsh-test" ] || { echo "cython is only supported for cqlsh-test"; exit 1; }
else
    cython="no"
fi

# the docker container's env
touch build/env.list
cat > build/env.list <<EOF
TEST_SCRIPT=${test_script}
JAVA_VERSION=${java_version}
PYTHON_VERSION=${python_version}
cython=${cython}
ANT_OPTS="-Dtesttag.extra=.arch=$(arch).python${python_version}"
EOF

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
docker_id=$(docker run --name ${container_name} ${docker_flags} ${docker_mounts} ${docker_volume_opt} ${image_name} sleep 4h)

echo "Running container ${container_name} ${docker_id}"

docker exec --user root ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_create_user.sh cassandra $(id -u) $(id -g)" | tee -a ${logfile}
docker exec --user root ${container_name} update-alternatives --set python /usr/bin/python${python_version} | tee -a ${logfile}

# capture logs and pid for container
docker exec --user cassandra ${container_name} bash -c "${docker_command}" | tee -a ${logfile}
status=$?

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
exit ${status}
