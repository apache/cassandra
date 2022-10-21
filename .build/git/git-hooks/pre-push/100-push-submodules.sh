#!/usr/bin/env bash
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

# Redirect output to stderr.
exec 1>&2

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

_main() {
  # In case the usage happens at a different layer, make sure to cd to the toplevel
  local root_dir
  root_dir="$(git rev-parse --show-toplevel)"
  cd "$root_dir"

  if [[ ! -e .gitmodules ]]; then
    # nothing to see here, look away!
    return 0
  fi

  local -r cmd='
branch="$(git rev-parse --abbrev-ref HEAD)"
[[ "$branch" == "HEAD" ]] && exit 0

default_remote="$(git config --local --get branch."${branch}".remote || true)"
remote="${default_remote:-origin}"

git push --atomic "$remote" "$branch"
'
  git submodule foreach --recursive "$cmd"
}

_main "$@"
