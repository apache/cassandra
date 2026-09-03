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

[ $DEBUG ] && set -x

if [ "$1" == "-h" ]; then
   echo "$0 [-h] [<java_version>]"
   echo " build debian packages"
   exit 1
fi

echo
echo "==="
echo "WARNING: this script modifies local versioned files"
echo "==="
echo

#
# Creates the debian package

# debian/rules runs `ant realclean`, which must be able to remove build/ itself.
# Use the checkout's normal build/ directory for Ant and a separate bind mount for
# finished packages; mounting build/ itself at /dist would make realclean fail with EBUSY.
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname -- "$0")/../..)"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
package_dir="${build_dir}.debian-packages"
rm -rf "${package_dir}"
mkdir -p "${package_dir}"

build_dir="${package_dir}" CASSANDRA_DOCKER_USE_DEFAULT_BUILD_DIR=true \
    $(dirname -- "$0")/_docker_run.sh bullseye-build.docker docker/_build-debian.sh "$1"
status=$?
if [ ${status} -eq 0 ]; then
    mkdir -p "${build_dir}"
    for artifact in "${package_dir}"/*; do
        [ -e "${artifact}" ] && mv "${artifact}" "${build_dir}/"
    done
    rm -rf "${package_dir}"
fi
exit ${status}
