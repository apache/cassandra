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
# Assertions shared by the .build/sh/test/stub-*-cases.sh harnesses.
# Source it, do not execute it.  The caller must set ${STUB_INVOCATIONS}, the file
# its stub appends one line to per call, and must end with `report <harness name>`.
#

failures=0
LAST_OUT=""
LAST_RC=0

# run a command, keeping its combined output and its exit code for the expect_* helpers
run_case() {
  LAST_OUT="$("$@" 2>&1)"
  LAST_RC=$?
}

pass() { printf 'ok    %s\n' "$1"; }

fail() {
  printf 'FAIL  %s\n' "$1"
  printf '%s\n' "${LAST_OUT}" | sed 's/^/      | /'
  failures=$((failures + 1))
}

expect_rc() {
  if [ "${LAST_RC}" -eq "$2" ]; then pass "$1"; else fail "$1: expected rc $2, got ${LAST_RC}"; fi
}

expect_out() {
  case "${LAST_OUT}" in
    *"$2"*) pass "$1" ;;
    *)      fail "$1: output lacks '$2'" ;;
  esac
}

expect_no_out() {
  case "${LAST_OUT}" in
    *"$2"*) fail "$1: output must not contain '$2'" ;;
    *)      pass "$1" ;;
  esac
}

expect_invocation() {
  if grep -q -- "$2" "${STUB_INVOCATIONS}"; then pass "$1"; else fail "$1: no call matched '$2'"; fi
}

expect_no_invocation() {
  if grep -q -- "$2" "${STUB_INVOCATIONS}"; then fail "$1: a call matched '$2'"; else pass "$1"; fi
}

report() {
  if [ "${failures}" -eq 0 ]; then
    echo "all $1 cases passed"
    exit 0
  fi
  echo >&2 "${failures} $1 case(s) failed"
  exit 1
}
