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

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

_usage() {
  cat <<EOF
Usage: $(basename $0) [submodule path] [git repo URL] [git branch]
EOF
 exit 1
}

_main() {
  if [[ $# -ne 3 ]]; then
    _usage
  fi
  local -r path="$1"
  local -r url="$2"
  local -r branch="$3"

  local home
  home="$(git rev-parse --show-toplevel)"
  cd "$home"

  git submodule set-url "${path}" "${url}"
  git submodule set-branch --branch "${branch}" "${path}"
  git submodule update --remote
}

_main "$@"
