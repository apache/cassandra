#!/bin/sh -e
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

print_help() {
  echo "Usage: $0 [-s|--summary] [-h|--help]"
  echo "   -s, --summary  Print a summary of failures instead of the full ant output"
  echo "   -h, --help     Print help"
}

# arguments, with defaults
summary=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    -s|--summary) summary=true; shift ;;
    -h|--help)    print_help; exit 0 ;;
    *)            echo >&2 "Unknown argument $1"; print_help >&2; exit 1 ;;
  esac
done

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || { CASSANDRA_DIR="$(dirname -- "$0")/.."; }

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/.build/sh/_run-ant.sh" ] || { echo >&2 "${CASSANDRA_DIR}/.build/sh/_run-ant.sh must exist"; exit 1; }

# defines run_ant(), which reads ${CASSANDRA_DIR} and ${summary}
# shellcheck source=.build/sh/_run-ant.sh
. "${CASSANDRA_DIR}/.build/sh/_run-ant.sh"

# execute. the check target runs rat-check, checkstyle and checkstyle-test,
# and depends on _main-jar, build-test and gen-asciidoc. see build.xml
# memory needs to fit within the specified container size, see .jenkins/Jenkinsfile
# dependency-check # FIXME dependency-check now requires NVD key downloaded first
# append, as .build/docker/_docker_run.sh puts -Dbuild.dir=${DIST_DIR} in ANT_OPTS
export ANT_OPTS="${ANT_OPTS:-} -Xmx2g -XX:+PrintClassHistogram -XX:OnOutOfMemoryError='kill -QUIT %p'"
run_ant check
exit $?
