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
# Defines run_ant(), shared by .build/build-jars.sh and .build/check-code.sh.
# Source it, do not execute it.  The caller must set ${CASSANDRA_DIR} and ${summary}.
# ${DIST_DIR} is optional, and holds the log of a failed run.
#

# run ant, summarizing failures when ${summary} is true. ant's exit code is
# authoritative: a pipeline would report the summary's status, and ant can die without
# printing "BUILD FAILED" (an out-of-memory kill, for example), which the summary
# reads as a pass. POSIX sh has neither pipefail nor PIPESTATUS, hence the log file.
run_ant() {
  if ! ${summary}; then
    ant -f "${CASSANDRA_DIR}/build.xml" "$@"
    return $?
  fi
  # keep the log in the build directory. .build/docker/_docker_run.sh bind mounts
  # ${DIST_DIR}, so a --rm container destroys any path under /tmp before it is read
  _out_dir="${DIST_DIR:-${CASSANDRA_DIR}/build}"
  mkdir -p "${_out_dir}"
  _log="${_out_dir}/ant.log"
  ant -f "${CASSANDRA_DIR}/build.xml" "$@" > "${_log}" 2>&1 && _rc=0 || _rc=$?
  echo "=== ant $* ==="
  "${CASSANDRA_DIR}/.build/sh/ant-log-summary.py" "${_log}" || true
  if [ ${_rc} -eq 0 ] ; then
    rm -f "${_log}"
  else
    # state the verdict, as the summary can find nothing to report and still say SUCCESSFUL
    echo "ant exited ${_rc}. full output: ${_log}"
  fi
  return ${_rc}
}
