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
   echo "$0 [-h]"
   echo " this script is used internally by other scripts in the same directory to copy the image's ccm repositories into the dtest tmpdir"
   exit 1
fi

command -v rsync >/dev/null 2>&1 || { echo >&2 "rsync needs to be installed"; exit 1; }

################################
#
# Main
#
################################


# prepopulate a tmp ccm repository directory from whats already in the image
#
# the image has all versions and branches in ~/.ccm/repository already
# the container doesn't re-use ~/.ccm/repository so to avoid any writes in the containerfs
# so we rsync what's important from ~/.ccm/repository to the configured ${CCM_CONFIG_DIR}
# this appears to take a long time, but should be faster than ccm downloading
# rsync is used as it's the friendlier approach against open file ulimits
echo -n "prepopulating ${CCM_CONFIG_DIR}/repository/ for upgrade tests…"
mkdir -p "${CCM_CONFIG_DIR}/repository/"
rsync -rptgoL --include '[1-9]*' ${HOME}/.ccm/repository ${CCM_CONFIG_DIR}/
rsync -rptgoL --include '_git_cache_apache' ${HOME}/.ccm/repository ${CCM_CONFIG_DIR}/
rsync -rptgoL --include 'gitCOLON*' ${HOME}/.ccm/repository ${CCM_CONFIG_DIR}/
echo " complete"
