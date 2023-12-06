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
# Creates the artifacts, performing additional QA checks
#
# Usage: _docker_run.sh <docker_image_name> <script_to_execute> <java_version>

################################
#
# Prep
#
################################

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname "$0")/../..)"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
[ -d "${build_dir}" ] || { mkdir -p "${build_dir}" ; }

java_version_default=`grep 'property\s*name="java.default"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "$1" == "-h" ]; then
   echo "$0 [-h] <dockerfile> <run_script> [<java version>]"
   echo " this script is used by check|build*.sh scripts (in the same directory) as a wrapper delegating the container run of the <dockerfile> and execution of the <run_script>, and using [<java version>] is specified"
   exit 1
fi

# arguments
dockerfile=$1
run_script=$2
java_version=$3

# pre-conditions
command -v docker >/dev/null 2>&1 || { echo >&2 "docker needs to be installed"; exit 1; }
command -v timeout >/dev/null 2>&1 || { echo >&2 "timeout needs to be installed"; exit 1; }
(docker info >/dev/null 2>&1) || { echo >&2 "docker needs to running"; exit 1; }
[ -f "${cassandra_dir}/build.xml" ] || { echo >&2 "${cassandra_dir}/build.xml must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/docker/${dockerfile}" ] || { echo >&2 "${cassandra_dir}/.build/docker/${dockerfile} must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/${run_script}" ] || { echo >&2 "${cassandra_dir}/.build/${run_script} must exist"; exit 1; }
[ "${build_dir:0:1}" == "/" ] || { echo >&2 "\$build_dir must be provided as an absolute path, was ${build_dir}"; exit 1; }

if [ "x${java_version}" == "x" ] ; then
    echo "Defaulting to java ${java_version_default}"
    java_version="${java_version_default}"
fi

regx_java_version="(${java_version_supported//,/|})"
if [[ ! "${java_version}" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
   exit 1
fi

# print debug information on versions
docker --version

# make sure build_dir is good
chmod -R ag+rwx ${build_dir}


################################
#
# Main
#
################################

# git worktrees need their original working directory (in its original path)
if [ -f ${cassandra_dir}/.git ] ; then
    git_location="$(cat ${cassandra_dir}/.git | awk -F".git" '{print $1}' | awk '{print $2}')"
    docker_volume_opt="${docker_volume_opt} -v${git_location}:${git_location}"
fi

pushd ${cassandra_dir}/.build >/dev/null

image_tag="$(md5sum docker/${dockerfile} | cut -d' ' -f1)"
image_name="apache/cassandra-${dockerfile/.docker/}:${image_tag}"

# Look for existing docker image, otherwise build
if ! ( [[ "$(docker images -q ${image_name} 2>/dev/null)" != "" ]] ) ; then
  # try docker login to increase dockerhub rate limits
  timeout -k 5 5 docker login >/dev/null
  if ! ( docker pull -q ${image_name} >/dev/null 2>/dev/null ) ; then
    # Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
    until docker build -t ${image_name} -f docker/${dockerfile} .  ; do
        echo "docker build failed… trying again in 10s… "
        sleep 10
    done
  fi
fi

# Run build script through docker
random_string="$(LC_ALL=C tr -dc A-Za-z0-9 </dev/urandom | head -c 6 ; echo '')"
run_script_name=$(echo ${run_script} | sed  's/.sh//' | sed 's/_//')
container_name="cassandra_${dockerfile/.docker/}_${un_script_name}_jdk${java_version}__${random_string}"

# Docker commands:
#  change ant's build directory to $DIST_DIR
#  set java to java_version
#  execute the run_script
docker_command="export ANT_OPTS=\"-Dbuild.dir=\${DIST_DIR} ${CASSANDRA_DOCKER_ANT_OPTS}\" ; \
                source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
                \${CASSANDRA_DIR}/.build/${run_script} ${@:4} ; exit \$? "

# run without the default seccomp profile
# re-use the host's maven repository
container_id=$(docker run --name ${container_name} -d --security-opt seccomp=unconfined --rm \
    -v "${cassandra_dir}":/home/build/cassandra -v ~/.m2/repository/:/home/build/.m2/repository/ -v "${build_dir}":/dist \
    ${docker_volume_opt} \
    ${image_name} sleep 1h)

echo "Running container ${container_name} ${container_id}"

docker exec --user root ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_create_user.sh build $(id -u) $(id -g)"
docker exec --user build ${container_name} bash -c "${docker_command}"
RETURN=$?

# docker stop in background, ignore errors
( nohup docker stop ${container_name} >/dev/null 2>/dev/null & )
popd >/dev/null
[ $RETURN -eq 0 ] && echo "Build directory found at ${build_dir}"
exit $RETURN
