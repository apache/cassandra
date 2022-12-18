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

################################
#
# Prep
#
################################

if [ "$1" == "-h" ]; then
   echo "$0 [-h] <username> <uid> <gid>"
   echo " this script is used internally by other scripts in the same directory to create a user with the running host user's same uid and gid"
   exit 1
fi

# arguments
username=$1
uid=$2
gid=$3

################################
#
# Main
#
################################

if grep "^ID=" /etc/os-release | grep -q 'debian\|ubuntu' ; then
   adduser --quiet --disabled-login --no-create-home --uid $uid --gecos ${username} ${username}
   groupmod --non-unique -g $gid $username
   gpasswd -a ${username} sudo >/dev/null
else
   adduser --no-create-home --uid $uid ${username}
fi

# sudo priviledges
echo "${username} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/${username}
chmod 0440 /etc/sudoers.d/${username}
mkdir -p ${BUILD_HOME}/docker ${DIST_DIR} ${BUILD_HOME}/.ssh

# we need to make SSH less strict to prevent various dtests from failing when they attempt to
# git clone a given commit/tag/etc
echo 'Host *\n UserKnownHostsFile /dev/null\n StrictHostKeyChecking no' > ${BUILD_HOME}/.ssh/config

# proper permissions
chown ${username}:${username} ${BUILD_HOME} ${BUILD_HOME}/docker ${DIST_DIR} ${BUILD_HOME}/.ssh ${BUILD_HOME}/.ssh/config
chmod og+wx ${BUILD_HOME} ${DIST_DIR}
chmod 600 ${BUILD_HOME}/.ssh/config

# disable git directory ownership checks
su ${username} -c "git config --global safe.directory '*'"
