#!/usr/bin/env bash

#set -o xtrace
set -o errexit
set -o pipefail
set -o nounset

home="$(cd "$(dirname "$0")"/.. > /dev/null; pwd)"

cd "$home"/modules/accord
git ls-tree -r HEAD --name-only | sed 's;^;modules/accord/;'

