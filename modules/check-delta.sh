#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

bin="$(cd "$(dirname "$0")" > /dev/null; pwd)"

_main() {
  local -r other='/Users/dcapwell/src/github/apache/cassandra-accord/trunk'
  local checked=('')
  for f in $(git diff --name-only); do
    echo "Checking $f"
    diff "$f" "${other}/${f}" || true
    checked+=("$f")
  done
  local skip=false
  for f in $(cd "$other"; git diff --name-only); do
    skip=false 
    for c in "${checked[@]}"; do
      if [[ "$c" == "$f" ]]; then
        skip=true
        break
      fi
    done
    if [[ $skip == true ]]; then
      continue
    fi
    echo "Checking $f"
    diff "$f" "${other}/${f}" || true
  done
}

_main "$@"
