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

##
## When working with submodules the top level project (Apache Cassandra) needs to commit all submodule
## changes so the top level knows what SHA to use.  When working in a development environment it is
## common that multiple commits will exist in both projects, if the submodule has its history
## rewritten, then historic top level commits are no longer valid unless the SHAs are pushed to a
## remote repo; this is what the script attempts to do, make sure all SHAs added to the
## Apache Cassandra are backed up to a remote repo to make the Cassandra SHA buildable.
##

# Redirect output to stderr.
exec 1>&2


#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

_log() {
  echo -e "[pre-commit]\t$*"
}

error() {
  _log "$@" 1>&2
  exit 1
}

# Status Table
# A         Added
# C         Copied
# D         Deleted
# M         Modified
# R         Renamed
# T         Type Changed (i.e. regular file, symlink, submodule, …<200b>)
# U         Unmerged
# X         Unknown
# B         Broken
_main() {
  # In case the usage happens at a different layer, make sure to cd to the toplevel
  local root_dir
  root_dir="$(git rev-parse --show-toplevel)"
  cd "$root_dir"

  [[ ! -e .gitmodules ]] && return 0
  local enabled=$(git config --bool cassandra.pre-commit.verify-submodules.enabled || echo true)
  [ "$enabled" == "false" ] && return 0
  local submodules=( $(git config --file .gitmodules --get-regexp path | awk '{ print $2 }') )

  local is_submodule=false
  local git_sub_dir
  local git_sha
  while read status file; do
    is_submodule=false
    for to_check in "${submodules[*]}"; do
      if [[ "$to_check" == "$file" ]]; then
        is_submodule=true
        break
      fi
    done
    if $is_submodule; then
      local enabled=$(git config --bool cassandra.pre-commit.verify-submodule-${file}.enabled || echo true)
      [ "$enabled" == "false" ] && continue
      _log "Submodule detected: ${file} with status ${status}; attempting a push"
      _log "\tTo disable pushes, run"
      _log "\t\tgit config --local cassandra.pre-commit.verify-submodules.enabled false"
      _log "\tOr"
      _log "\t\tgit config --local cassandra.pre-commit.verify-submodule-${file}.enabled false"
      set -x
      git_sub_dir="${file}/.git"
      branch="$(git config -f .gitmodules "submodule.${file}.branch")"
      [[ -z "${branch:-}" ]] && error "Submodule ${file} does not define a branch"
      git_sha="$(git --git-dir "${git_sub_dir}" rev-parse HEAD)"
      git --git-dir "${git_sub_dir}" fetch origin
      git --git-dir "${git_sub_dir}" branch "origin/${branch}" --contains "${git_sha}" || error "Git commit ${git_sha} not found in $(git remote get-url origin) on branch ${branch}"
    fi
  done < <(git diff --cached --name-status)
}

_main "$@"
