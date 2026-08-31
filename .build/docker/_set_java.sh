#!/bin/bash -e
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

################################
#
# Prep
#
################################

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || { CASSANDRA_DIR="$(pwd)"; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }

# pre-conditions
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }

java_version_default=`grep 'property\s*name="java.default"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "$1" == "-h" ]; then
   echo "$0 [-h] [<java version>]"
   echo " if Java version is not set, it is set to ${java_version_default} by default, valid ${java_version_supported}"
   echo
   echo " this script is used internally by other scripts in the same directory to ensure the correct java version is used inside the docker container"
   exit 1
fi

# arguments
java_version=$1

[ "x${java_version}" != "x" ] || java_version="${java_version_default}"
regx_java_version="(${java_version_supported//,/|})"
if [[ ! "$java_version" =~ $regx_java_version ]]; then
   echo "Error: Java version is not in ${java_version_supported}, it is set to $java_version"
   exit 1
fi

################################
#
# Main
#
################################

if [ "${java_version}" = "8" ] && [ -x /opt/java/openjdk8/bin/javac ]; then
    # Also accept a site-provided JDK without downloading another copy.
    export JAVA_HOME=/opt/java/openjdk8
    export PATH="${JAVA_HOME}/bin:${PATH}"
elif [ "${java_version}" = "8" ] && ! compgen -G '/usr/lib/jvm/java-8-openjdk-*/bin/javac' >/dev/null; then
    # The shared build images do not carry JDK 8. Fetch it after container startup into
    # the Maven-backed cache, which is reused by every matrix cell on this workspace.
    export JAVA_HOME="$("${CASSANDRA_DIR}/.build/docker/_ensure_jdk8.sh" "${HOME}/.m2/repository/.ci-jdks")"
    export PATH="${JAVA_HOME}/bin:${PATH}"
elif grep "^ID=" /etc/os-release | grep -q 'debian\|ubuntu' ; then
    sudo update-java-alternatives --set java-1.${java_version}.0-openjdk-$(dpkg --print-architecture)
    export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
else
    sudo alternatives --set java $(alternatives --display java | grep "family java-${java_version}-openjdk" | cut -d' ' -f1)
    sudo alternatives --set javac $(alternatives --display javac | grep "family java-${java_version}-openjdk" | cut -d' ' -f1)
    export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
fi
echo "Using Java ${java_version} from ${JAVA_HOME}"
