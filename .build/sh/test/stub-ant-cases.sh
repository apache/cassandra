#!/bin/bash
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
# Exercise the .build/*.sh wrapper scripts against a stub `ant`.
#
# The stub replays canned output and a canned exit code, so these cases test the
# shell plumbing (argument parsing, exit codes, target selection) in seconds and
# without a JDK build.  The summary scripts themselves are unit tested in
# .build/sh/test/test_log_summary.py.
#
# Usage: .build/sh/test/stub-ant-cases.sh
#

set -o nounset
set -o pipefail

export CASSANDRA_DIR="$(cd -- "$(dirname -- "$0")/../../.." && pwd)"
STUB_DIR="$(mktemp -d)"
trap 'rm -rf "${STUB_DIR}"' EXIT

export STUB_LOG_DIR="${STUB_DIR}/logs"
export STUB_INVOCATIONS="${STUB_DIR}/invocations"
export STUB_ANT_OPTS="${STUB_DIR}/ant-opts"
export DIST_DIR="${STUB_DIR}/dist"
mkdir -p "${STUB_DIR}/bin" "${STUB_DIR}/empty" "${STUB_LOG_DIR}" "${DIST_DIR}"

# defines run_case(), the expect_* assertions and report()
# shellcheck source=.build/sh/test/_test-harness.sh
. "$(dirname -- "$0")/_test-harness.sh"

# pre-conditions. run-tests.sh needs a JDK and git before it reaches any ant call
for _tool in java javac git; do
  command -v "${_tool}" >/dev/null 2>&1 || { echo >&2 "${_tool} needs to be installed"; exit 1; }
done

# the cases name these tests, so a rename must fail here and not deep inside a case
for _fixture in test/unit/org/apache/cassandra/service/StorageServiceServerTest.java \
                test/microbench/org/apache/cassandra/test/microbench/AutoBoxingBench.java ; do
  [ -f "${CASSANDRA_DIR}/${_fixture}" ] \
    || { echo >&2 "fixture ${_fixture} is gone; update the test names in $0"; exit 1; }
done

# the stub ant. For every non-option argument <a> it replays ${STUB_LOG_DIR}/<a>.log
# and takes its exit code from ${STUB_LOG_DIR}/<a>.rc, defaulting to 0.
cat > "${STUB_DIR}/bin/ant" <<'STUB'
#!/bin/sh
echo "ant $*" >> "${STUB_INVOCATIONS}"
printf '%s\n' "${ANT_OPTS}" > "${STUB_ANT_OPTS}"
_rc=0
for _a in "$@"; do
  case "${_a}" in -*) continue ;; esac
  [ -f "${STUB_LOG_DIR}/${_a}.log" ] && cat "${STUB_LOG_DIR}/${_a}.log"
  [ -f "${STUB_LOG_DIR}/${_a}.rc" ] && _rc="$(cat "${STUB_LOG_DIR}/${_a}.rc")"
done
exit "${_rc}"
STUB
chmod +x "${STUB_DIR}/bin/ant"
export PATH="${STUB_DIR}/bin:${PATH}"

# the project must look built, or run-tests.sh stops early
version="$(grep 'property\s*name="base.version"' "${CASSANDRA_DIR}/build.xml" | sed -ne 's/.*value="\([^"]*\)".*/\1/p')"
touch "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar"

################################
#
# Harness
#
################################

reset_stub() {
  # remove ant.log too, or a stale one from a previous failing case reads as this one's
  rm -rf "${STUB_LOG_DIR}" "${STUB_INVOCATIONS}" "${STUB_ANT_OPTS}" "${DIST_DIR}/ant.log"
  mkdir -p "${STUB_LOG_DIR}"
  : > "${STUB_INVOCATIONS}"
}

stub_log() { printf '%s\n' "$2" > "${STUB_LOG_DIR}/$1.log"; }
stub_rc()  { printf '%s\n' "$2" > "${STUB_LOG_DIR}/$1.rc"; }

################################
#
# Cases: help and argument parsing
#
################################

reset_stub
for s in build-jars check-code run-tests; do
  run_case "${CASSANDRA_DIR}/.build/${s}.sh" -h
  expect_rc "${s}.sh -h exits 0" 0
done

run_case "${CASSANDRA_DIR}/.build/build-jars.sh" --bogus
expect_rc "build-jars.sh rejects an unknown argument" 1
expect_out "build-jars.sh names the unknown argument" "Unknown argument --bogus"

run_case "${CASSANDRA_DIR}/.build/check-code.sh" --bogus
expect_rc "check-code.sh rejects an unknown argument" 1

run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -z
expect_rc "run-tests.sh rejects an unknown option" 1

run_case "${CASSANDRA_DIR}/.build/run-tests.sh" --bogus
expect_rc "run-tests.sh rejects an unknown long option" 1
expect_out "run-tests.sh names the unknown long option" "Invalid option: --bogus"

run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a
expect_rc "run-tests.sh rejects a flag with no argument" 1
expect_out "run-tests.sh says which flag needs an argument" "Option -a requires an argument"

run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a bogus-target
expect_rc "run-tests.sh rejects an unknown target type" 1
expect_out "run-tests.sh names the unknown target type" "Invalid test target type"

run_case "${CASSANDRA_DIR}/.build/run-tests.sh" --target bogus-target
expect_rc "run-tests.sh --target reaches the same check as -a" 1
expect_out "run-tests.sh --target names the unknown target type" "Invalid test target type"

# this script runs on the java on the path, so a legacy positional java version is a
# mistake. reading it as a test name regexp would run the wrong tests on the wrong jdk
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" test 17
expect_rc "run-tests.sh rejects a legacy positional java version" 1
expect_out "run-tests.sh says it cannot set the java version" "cannot set the java version"
expect_out "run-tests.sh names the docker script that can" "docker/run-tests.sh -a test -j 17"

################################
#
# Case: error() is defined before the pre-conditions call it
#
################################

reset_stub
run_case env PATH="${STUB_DIR}/empty" "${CASSANDRA_DIR}/.build/run-tests.sh" -h
expect_rc "run-tests.sh fails when ant is absent" 1
expect_out "run-tests.sh reports the missing ant, not a missing function" "ant needs to be installed"

################################
#
# Case: the split check uses a POSIX character class, not \d
#
################################

reset_stub
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a stress-test -c 2/4
expect_rc "run-tests.sh rejects a split on stress-test" 1
expect_out "run-tests.sh explains the rejected split" "does not support splits"

reset_stub
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a test -c 2/4 -t StorageServiceServerTest
expect_rc "run-tests.sh allows a split on test" 0

################################
#
# Cases: run-tests.sh --summary
#
################################

reset_stub
stub_log generate-test-report "   [concat] [Test Summary] Run: 12, Failed: 0, Errors: 0, Skipped: 1"
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a test -t StorageServiceServerTest
expect_rc "run-tests.sh -s exits 0 on a green run" 0
expect_out "run-tests.sh -s reports the pass" "TESTS PASSED"
expect_invocation "run-tests.sh runs generate-test-report for the test target" "generate-test-report"

# -s must keep the full output, as the summary prints matched lines only
if grep -q "Test Summary" "${DIST_DIR}/run-tests.log" 2>/dev/null; then
  pass "run-tests.sh -s keeps the full output in DIST_DIR/run-tests.log"
else
  fail "run-tests.sh -s keeps the full output in DIST_DIR/run-tests.log"
fi
expect_out "run-tests.sh -s names the log file" "full output: ${DIST_DIR}/run-tests.log"

reset_stub
stub_log generate-test-report "[Test Summary] Run: 12, Failed: 0, Errors: 0, Skipped: 0"
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" --summary --target test --test StorageServiceServerTest
expect_rc "run-tests.sh long flags exit 0 on a green run" 0
expect_out "run-tests.sh --summary reports the pass" "TESTS PASSED"

reset_stub
stub_log generate-test-report "[Test Summary] Run: 12, Failed: 2, Errors: 0, Skipped: 1"
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a test -t StorageServiceServerTest
expect_rc "run-tests.sh -s exits 1 on test failures" 1
expect_out "run-tests.sh -s shows the failure counts" "Failed: 2"

reset_stub
stub_rc testclasslist 1
stub_log testclasslist "    [junit] Testcase: testFoo(org.apache.cassandra.FooTest):	FAILED"
stub_log generate-test-report "[Test Summary] Run: 12, Failed: 1, Errors: 0, Skipped: 0"
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a test -t StorageServiceServerTest
expect_rc "run-tests.sh -s exits 1 when ant returns non-zero" 1
expect_out "run-tests.sh -s names the failed target" "failed unit testclasslist"
expect_invocation "run-tests.sh still reports after a failing target" "generate-test-report"

# a setup failure must take precedence over the summary, and must name its cause.
# the summary matches test results only, so without the log tail the user reads
# "No test summary found" and nothing else
reset_stub
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a test -t NoSuchTestClassAnywhere
expect_rc "run-tests.sh -s propagates a setup failure" 1
expect_out "run-tests.sh -s shows the tail of the log on a setup failure" "run-tests.sh exited 1"
expect_out "run-tests.sh -s shows what the run last printed" "Running tests: NoSuchTestClassAnywhere"

# errexit is dynamically scoped, so -s must not disable it inside _main.
# resolver-dist-lib runs before any test, and its failure must stop the run.
reset_stub
stub_rc resolver-dist-lib 1
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a test -t StorageServiceServerTest
expect_rc "run-tests.sh -s stops when the ant setup target fails" 1
expect_no_invocation "run-tests.sh -s runs no test after a failed setup target" "testclasslist"

################################
#
# Case: generate-test-report is skipped for targets with no JUnit xml
#
################################

reset_stub
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a microbench -t AutoBoxingBench
expect_rc "run-tests.sh -s exits 0 on a green microbench run" 0
expect_out "run-tests.sh -s reports the microbench pass" "completed successfully"
expect_no_invocation "run-tests.sh skips generate-test-report for microbench" "generate-test-report"

reset_stub
stub_rc microbench 1
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -s -a microbench -t AutoBoxingBench
expect_rc "run-tests.sh -s exits 1 on a failing microbench run" 1
expect_out "run-tests.sh -s names the failed microbench" "failed microbench"

# without -s there is no summary to read the marker, so the status must propagate
reset_stub
stub_rc microbench 1
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a microbench -t AutoBoxingBench
expect_rc "run-tests.sh exits 1 on a failing microbench run without -s" 1

################################
#
# Case: stale test output is removed (the brace expansion was quoted before)
#
################################

reset_stub
mkdir -p "${DIST_DIR}/test/output" "${DIST_DIR}/test/logs"
touch "${DIST_DIR}/test/output/TEST-stale.xml" "${DIST_DIR}/test/logs/stale.log"
run_case "${CASSANDRA_DIR}/.build/run-tests.sh" -a test -t StorageServiceServerTest
if [ -e "${DIST_DIR}/test/output/TEST-stale.xml" ] || [ -e "${DIST_DIR}/test/logs/stale.log" ]; then
  fail "run-tests.sh removes stale test output"
else
  pass "run-tests.sh removes stale test output"
fi

# a DIST_DIR holding a space must not be word split, or rm -rf takes the wrong path.
# unquoted, "${STUB_DIR}/spaced dist/test/{html,…}" splits into "${STUB_DIR}/spaced"
# and "dist/test/html", so the sentinel below is what the old code deleted.
reset_stub
SPACED_DIR="${STUB_DIR}/spaced dist"
SENTINEL="${STUB_DIR}/spaced"
mkdir -p "${SPACED_DIR}/test/output" "${SPACED_DIR}/test/logs" "${SENTINEL}"
touch "${SPACED_DIR}/apache-cassandra-${version}-SNAPSHOT.jar"
touch "${SPACED_DIR}/test/output/TEST-stale.xml" "${SPACED_DIR}/test/logs/stale.log"
touch "${SENTINEL}/keep-me"
run_case env DIST_DIR="${SPACED_DIR}" "${CASSANDRA_DIR}/.build/run-tests.sh" -a test -t StorageServiceServerTest
if [ -e "${SPACED_DIR}/test/output/TEST-stale.xml" ] || [ -e "${SPACED_DIR}/test/logs/stale.log" ]; then
  fail "run-tests.sh removes stale output under a DIST_DIR with a space"
else
  pass "run-tests.sh removes stale output under a DIST_DIR with a space"
fi
if [ -e "${SENTINEL}/keep-me" ]; then
  pass "run-tests.sh does not delete the word split prefix of DIST_DIR"
else
  fail "run-tests.sh does not delete the word split prefix of DIST_DIR"
fi
rm -rf "${SPACED_DIR}" "${SENTINEL}"

################################
#
# Cases: build-jars.sh and check-code.sh
#
################################

# both scripts call run_ant, so loop over the behaviour they share.
# each entry names the script and the one ant target it runs
for _case in "build-jars.sh jar" "check-code.sh check"; do
  s="${_case% *}"
  target="${_case#* }"

  reset_stub
  run_case "${CASSANDRA_DIR}/.build/${s}" -s
  expect_rc "${s} -s exits 0 on a green run" 0
  expect_out "${s} -s reports the pass" "BUILD SUCCESSFUL"
  expect_invocation "${s} runs the ${target} target" " ${target}"

  reset_stub
  stub_rc "${target}" 1
  stub_log "${target}" "${target}:
    [javac] /src/Foo.java:12: error: cannot find symbol
BUILD FAILED"
  run_case "${CASSANDRA_DIR}/.build/${s}" -s
  expect_rc "${s} -s exits 1 on a failing ${target}" 1
  expect_out "${s} -s names the failed target" "Failed target: ${target}"
  expect_out "${s} -s shows the line that failed" "cannot find symbol"

  reset_stub
  stub_rc "${target}" 1
  run_case "${CASSANDRA_DIR}/.build/${s}"
  expect_rc "${s} without -s still exits 1 on a failing ${target}" 1

  # ant can die without printing "BUILD FAILED", which the summary alone reads as a pass
  reset_stub
  stub_rc "${target}" 1
  stub_log "${target}" "${target}:
java.lang.OutOfMemoryError: Java heap space"
  run_case "${CASSANDRA_DIR}/.build/${s}" -s
  expect_rc "${s} -s exits 1 when ant dies without BUILD FAILED" 1
  # the log must sit under DIST_DIR, which .build/docker/_docker_run.sh bind mounts.
  # a --rm container destroys /tmp, so a path there names a file the user cannot read
  expect_out "${s} -s names a log file under DIST_DIR" "full output: ${DIST_DIR}/"
  if [ -f "${DIST_DIR}/ant.log" ]; then
    pass "${s} -s keeps the failed run's log in DIST_DIR"
  else
    fail "${s} -s keeps the failed run's log in DIST_DIR"
  fi
done

# --clean belongs to build-jars.sh only
reset_stub
run_case "${CASSANDRA_DIR}/.build/build-jars.sh" -s
expect_no_invocation "build-jars.sh does not clean without --clean" " clean"

reset_stub
run_case "${CASSANDRA_DIR}/.build/build-jars.sh" -s --clean
expect_rc "build-jars.sh -s --clean exits 0 on a green build" 0
expect_out "build-jars.sh -s labels the clean summary" "=== ant clean ==="
expect_out "build-jars.sh -s labels the jar summary" "=== ant jar ==="
first="$(sed -n 1p "${STUB_INVOCATIONS}")"
second="$(sed -n 2p "${STUB_INVOCATIONS}")"
case "${first}" in
  *" clean"*) case "${second}" in
                *" jar"*) pass "build-jars.sh --clean cleans before it builds" ;;
                *)        fail "build-jars.sh --clean cleans before it builds: second call was '${second}'" ;;
              esac ;;
  *) fail "build-jars.sh --clean cleans before it builds: first call was '${first}'" ;;
esac

# _docker_run.sh puts -Dbuild.dir=${DIST_DIR} in ANT_OPTS, which check-code.sh must not drop
reset_stub
run_case env ANT_OPTS="-Dbuild.dir=/sentinel" "${CASSANDRA_DIR}/.build/check-code.sh" -s
expect_rc "check-code.sh -s exits 0 with an inherited ANT_OPTS" 0
if grep -q -- "-Dbuild.dir=/sentinel" "${STUB_DIR}/ant-opts" 2>/dev/null; then
  pass "check-code.sh keeps the ANT_OPTS it inherited"
else
  fail "check-code.sh keeps the ANT_OPTS it inherited"
fi

################################
#
# Result
#
################################

report stub-ant
