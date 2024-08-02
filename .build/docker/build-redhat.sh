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


if [ "$1" == "-h" ]; then
   echo "$0 [-h] [rpm] [<java_version>]"
   echo " build redhat packages"
   exit 1
fi

# arguments
rpm_dist=$1
java_version=$2

if [ -n "${rpm_dist}" ] && [ "rpm" != "${rpm_dist}" ]; then
   echo >&2 "Only rpm is a valid dist_type arguments. Got ${rpm_dist}"
   exit 1
fi

echo
echo "==="
echo "WARNING: this script modifies local versioned files"
echo "==="
echo

#
# Creates the redhat package

$(dirname "$0")/_docker_run.sh almalinux-build.docker docker/_build-redhat.sh "${java_version}"
exit $?
