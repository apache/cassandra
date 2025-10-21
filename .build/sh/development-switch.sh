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

error() {
  echo -e "$*" 1>&2
  exit 1
}

_usage() {
  cat <<EOF
Usage: $(basename $0) (options)* (submodule)*

Options:
  --jira        JIRA used for development, will checkout if not done yet
  -h|--help     This help page
EOF
  exit 1
}

_is_main_branch() {
  local -r name="$1"
  [[ "$name" == cassandra-* ]] && return 0
  [[ "$name" == "trunk" ]] && return 0
  [[ "$name" == "cep-15-accord" ]] && return 0
  return 1
}

_main() {
  local home
  home="$(git rev-parse --show-toplevel)"
  cd "$home"

  local branch
  branch="$(git rev-parse --abbrev-ref HEAD)"
  # loop over args, executing as in order of execution
  while [ $# -gt 0 ]; do
    case "$1" in
      -h|--help)
        _usage
        ;;
      --jira)
        if [[ "$2" != "$branch" ]]; then
          git checkout -b "$2"
          branch="$2"
        fi
        shift 2
        ;;
      *)
        break
        ;;
    esac
  done
  while _is_main_branch "$branch" ; do
    echo "Currently on $branch, which does not look like a development brarnch; what JIRA are you working on? "
    read jira
    if [[ ! -z "${jira:-}" ]]; then
      git checkout -b "$jira"
      branch="$jira"
      break
    fi
  done
  local submodules
  submodules=( $(git config --file "$home"/.gitmodules --get-regexp path | awk '{ print $2 }') )
  local to_change=()
  if [[ $# -gt 0 ]]; then
    local exists
    for a in "$@"; do
      exists=false
      for sub in "${submodules[@]}"; do
        if [[ "$sub" == "$a" ]]; then
          exists=true
          break
        fi
      done
      [ "$exists" == false ] && error "git submodule $a does not exist"
      to_change+=( "$a" )
    done
  else
    for sub in "${submodules[@]}"; do
      to_change+=( "$sub" )
    done
  fi
  local submodule_branch
  local name
  for path in "${to_change[@]}"; do
    name="$(basename "$path")"
    git submodule set-url "${path}" "../cassandra-${name}.git"
    git submodule set-branch --branch "${branch}" "${path}"
    cd "$path"
      submodule_branch="$(git rev-parse --abbrev-ref HEAD)"
      if [[ "${submodule_branch}" != "${branch}" ]]; then
        git checkout -b "$branch"
      fi
    cd -
  done
}

_main "$@"
