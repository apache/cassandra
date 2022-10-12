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

accord_repo='https://github.com/bdeggleston/cassandra-accord.git'
accord_branch='metadata-persistence'
accord_src="$bin/cassandra-accord"

checkout() {
  cd "$accord_src"
    git checkout "$accord_branch"
    echo "$accord_branch" > .BRANCH
  cd -
}

_main() {
  # have we already cloned?
  if [[ ! -e "$accord_src" ]] || [[ $(cat "$accord_src/.REPO" || true) != "$accord_repo" ]]; then
    rm -rf "$accord_src" || true
    git clone "$accord_repo" "$accord_src"
    echo "$accord_repo" > "$accord_src/.REPO"
    checkout
  fi
  if [[ $(cat "$accord_src"/.BRANCH || true) != "$accord_branch" ]]; then
    checkout
  fi
  cd "$accord_src"
  # are there changes?
  git pull --rebase origin "$accord_branch"
  if [[ $(git rev-parse HEAD) != $(cat .SHA || true) ]]; then
    ./gradlew clean install -x test
    git rev-parse HEAD > .SHA
  fi
}

_main "$@"
