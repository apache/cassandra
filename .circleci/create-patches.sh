#!/usr/bin/env bash
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

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

"$bin"/circle-ci-config.py MIDRES --stdout > "$bin"/config-2_1.yml.MIDRES
"$bin"/circle-ci-config.py HIGHER --stdout > "$bin"/config-2_1.yml.HIGHER

rc=0
diff "$bin"/config-2_1.yml "$bin"/config-2_1.yml.MIDRES > "$bin"/config-2_1.yml.mid_res.patch || rc=$?
if [[ $rc != 1 ]]; then
  echo "Unexpected output of diff! Expected rc=1 (changes) but rc=$rc"
fi
diff "$bin"/config-2_1.yml "$bin"/config-2_1.yml.HIGHER > "$bin"/config-2_1.yml.high_res.patch || rc=$?
if [[ $rc != 1 ]]; then
  echo "Unexpected output of diff! Expected rc=1 (changes) but rc=$rc"
fi

rm "$bin"/config-2_1.yml.MIDRES "$bin"/config-2_1.yml.HIGHER

