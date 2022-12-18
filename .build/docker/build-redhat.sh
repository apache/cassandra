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
   echo "$0 [-h] [rpm|noboolean] [<java_version>]"
   echo " build redhat packages, specify noboolean for centos7 compatibility"
   exit 1
fi

# arguments
rpm_dist=$1
java_version=$2

if [ "${rpm_dist}" == "noboolean" ]; then
    dist_name="centos7"
else # noboolean
    dist_name="almalinux"
fi


echo
echo "==="
echo "WARNING: this script modifies local versioned files"
echo "==="
echo

#
# Creates the redhat package

$(dirname "$0")/_docker_run.sh ${dist_name}-build.docker docker/_build-redhat.sh "${java_version}" ${rpm_dist}
exit $?
