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

#
# Creates ci_summary.html
# This expects a folder hierarchy that separates test types/targets.
#  For example:
#    build/test/output/test
#    build/test/output/jvm-dtest
#    build/test/output/dtest
#
# The ci_summary.html file, along with the results_details.tar.xz,
#  are the sharable artefacts used to satisfy pre-commit CI from a private CI.
#

# variables, with defaults
[ "x${CASSANDRA_DIR}" != "x" ] || CASSANDRA_DIR="$(readlink -f $(dirname "$0")/../..)"
[ "x${DIST_DIR}" != "x" ] || DIST_DIR="${CASSANDRA_DIR}/build"

# pre-conditions
command -v ant >/dev/null 2>&1 || { echo >&2 "ant needs to be installed"; exit 1; }
[ -d "${CASSANDRA_DIR}" ] || { echo >&2 "Directory ${CASSANDRA_DIR} must exist"; exit 1; }
[ -f "${CASSANDRA_DIR}/build.xml" ] || { echo >&2 "${CASSANDRA_DIR}/build.xml must exist"; exit 1; }
[ -d "${DIST_DIR}" ] || { mkdir -p "${DIST_DIR}" ; }

# generate CI summary file
cd ${DIST_DIR}/

cat >${DIST_DIR}/ci_summary.html <<EOL
<html>
<head></head>
<body>
<h1>CI Summary</h1>
<h2>sha:  $(git ls-files -s ${CASSANDRA_DIR} | git hash-object --stdin)</h2>
<h2>branch: $(git -C ${CASSANDRA_DIR} branch --remote --verbose --no-abbrev --contains | sed -rne 's/^[^\/]*\/([^\ ]+).*$/\1/p')</h2>
<h2>repo: $(git -C ${CASSANDRA_DIR} remote get-url origin)</h2>
<h2>Date: $(date)</h2>
</body>
</html>
...
EOL

${CASSANDRA_DIR}/.build/ci/ci_parser.py --mute --input ${DIST_DIR}/test/output/ --output ${DIST_DIR}/ci_summary.html

exit $?

