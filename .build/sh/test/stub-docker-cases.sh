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
# Exercise the .build/docker/*.sh wrapper scripts against a stub `docker`.
#
# The stub answers the host queries and records every call, so these cases test the
# argument parsing without a container.  Two pieces of parsing need the coverage:
# the optional positional java version in _docker_run.sh, and the long flags in
# .build/docker/run-tests.sh.  Both decide what the container is told to run.
#
# Usage: .build/sh/test/stub-docker-cases.sh
#

set -o nounset
set -o pipefail

export cassandra_dir="$(cd -- "$(dirname -- "$0")/../../.." && pwd)"
STUB_DIR="$(mktemp -d)"
trap 'rm -rf "${STUB_DIR}"' EXIT

export STUB_INVOCATIONS="${STUB_DIR}/invocations"
export build_dir="${STUB_DIR}/dist"
mkdir -p "${STUB_DIR}/bin" "${build_dir}"

# defines run_case(), the expect_* assertions and report()
# shellcheck source=.build/sh/test/_test-harness.sh
. "$(dirname -- "$0")/_test-harness.sh"

# the m2 repository is only mounted, so keep it out of the developer's home
export m2_dir="${STUB_DIR}/m2"

java_default="$(grep 'property\s*name="java.default"' "${cassandra_dir}/build.xml" | sed -ne 's/.*value="\([^"]*\)".*/\1/p')"

# the stub docker. It records every call, and answers the three host queries that
# .build/docker/run-tests.sh reads: the cpu count, the memory size and the cpu limit.
cat > "${STUB_DIR}/bin/docker" <<'STUB'
#!/bin/sh
echo "docker $*" >> "${STUB_INVOCATIONS}"
case "$1" in
  --version) echo "Docker version 0.0.0-stub, build stub" ;;
  info)      echo " CPUs: 8" ;;
  images)    echo "stub-image-id" ;;
  run)
    # `docker run --rm alpine … nproc --all` and `… free -b` ask the host, not the image
    for _a in "$@"; do
      case "${_a}" in
        nproc) echo 8 ; exit 0 ;;
        free)  echo "Mem: 34359738368 0 0" ; exit 0 ;;
      esac
    done
    echo "stub-container-id"
    ;;
esac
exit 0
STUB
chmod +x "${STUB_DIR}/bin/docker"
export PATH="${STUB_DIR}/bin:${PATH}"

reset_stub() {
  rm -f "${STUB_INVOCATIONS}"
  : > "${STUB_INVOCATIONS}"
}

################################
#
# Cases: _docker_run.sh, and the optional positional java version
#
################################

reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" -h
expect_rc "_docker_run.sh -h exits 0" 0
expect_out "_docker_run.sh -h documents the script arguments" "script arguments"

# with no version, the default applies and the flag still reaches the run script
reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" debian-build.docker build-jars.sh --clean
expect_rc "_docker_run.sh runs with no java version" 0
expect_out "_docker_run.sh reports the default java version" "Defaulting to java ${java_default}"
expect_invocation "_docker_run.sh sets the default java version" "_set_java.sh ${java_default}"
expect_invocation "_docker_run.sh passes --clean to build-jars.sh" "build-jars.sh --clean"

# with a version, the run script must not receive it as one of its own arguments
reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" debian-build.docker build-jars.sh 17 --clean
expect_rc "_docker_run.sh runs with a java version" 0
expect_no_out "_docker_run.sh does not default when a version is given" "Defaulting to java"
expect_invocation "_docker_run.sh sets the given java version" "_set_java.sh 17"
expect_invocation "_docker_run.sh still passes --clean" "build-jars.sh --clean"
expect_no_invocation "_docker_run.sh does not pass the version to the run script" "build-jars.sh 17"

# a leading - marks a run script argument, so -s must never be read as a java version
reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" debian-build.docker check-code.sh -s
expect_rc "_docker_run.sh reads -s as a run script argument" 0
expect_invocation "_docker_run.sh sets the default java version before -s" "_set_java.sh ${java_default}"
expect_invocation "_docker_run.sh passes -s to check-code.sh" "check-code.sh -s"

reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" debian-build.docker build-jars.sh 99
expect_rc "_docker_run.sh rejects an unsupported java version" 1
expect_out "_docker_run.sh names the supported java versions" "Java version is not in"

reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" no-such.docker build-jars.sh
expect_rc "_docker_run.sh rejects a missing dockerfile" 1
expect_out "_docker_run.sh names the missing dockerfile" "no-such.docker must exist"

# docker accepts [a-zA-Z0-9][a-zA-Z0-9_.-] in a container name only. four call sites pass a
# run_script that holds a directory, so the name must not carry the directory separator
reset_stub
run_case "${cassandra_dir}/.build/docker/_docker_run.sh" almalinux-build.docker docker/_build-redhat.sh 21
expect_rc "_docker_run.sh runs a run_script under a directory" 0
expect_invocation "_docker_run.sh names the container after the run_script alone" "\-\-name cassandra_almalinux-build_build-redhat_jdk21__"
expect_no_invocation "_docker_run.sh puts no directory separator in the container name" "\-\-name [^ ]*/"

################################
#
# Cases: docker/run-tests.sh, and its long flags
#
################################

reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" -h
expect_rc "docker/run-tests.sh -h exits 0" 0
expect_out "docker/run-tests.sh -h documents --summary" "-s, --summary"

reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" -a test -t StorageServiceServerTest -j 17 -s
expect_rc "docker/run-tests.sh runs with short flags" 0
# two assertions, as an empty split_chunk_arg leaves a double space between -a and -t
expect_invocation "docker/run-tests.sh passes the target on" "_docker_init_tests.sh \-a test"
expect_invocation "docker/run-tests.sh passes the test name and -s on" "\-t StorageServiceServerTest \-s"
expect_invocation "docker/run-tests.sh sets java from -j" "_set_java.sh 17"

# the long flags must reach the container as the same short flags
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" --target test --test StorageServiceServerTest --java 17 --summary
expect_rc "docker/run-tests.sh runs with long flags" 0
expect_invocation "docker/run-tests.sh turns --target into -a" "_docker_init_tests.sh \-a test"
expect_invocation "docker/run-tests.sh turns --test and --summary into -t and -s" "\-t StorageServiceServerTest \-s"
expect_invocation "docker/run-tests.sh sets java from --java" "_set_java.sh 17"

reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" --target bogus-target
expect_rc "docker/run-tests.sh rejects an unknown target type" 1
expect_out "docker/run-tests.sh names the unknown target type" "Invalid test target type"

reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" --target
expect_rc "docker/run-tests.sh rejects a long flag with no argument" 1
expect_out "docker/run-tests.sh says which flag needs an argument" "requires an argument"

# run-python-dtests.sh has no summary mode, so the flag must be refused before any docker work
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" --target dtest --summary
expect_rc "docker/run-tests.sh rejects --summary for a dtest target" 1
expect_out "docker/run-tests.sh explains the refused --summary" "has no summary mode"
expect_no_invocation "docker/run-tests.sh starts no container for a refused --summary" "docker run"

# with no target, that same guard must not fire on an empty test_target
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" --summary
expect_rc "docker/run-tests.sh rejects --summary with no target" 1
expect_out "docker/run-tests.sh names the missing target, not the summary mode" "resource limits unconfigured"

# an argument this script does not know reaches the downstream script
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" -a test -t StorageServiceServerTest -b 4.1
expect_rc "docker/run-tests.sh runs with an argument it does not know" 0
expect_invocation "docker/run-tests.sh passes the unknown argument downstream" "\-b 4.1"

# the sibling docker scripts take the java version positionally, so refuse that form here
# rather than let the downstream script reject the token after the container is up
java_supported="$(grep 'property\s*name="java.supported"' "${cassandra_dir}/build.xml" | sed -ne 's/.*value="\([^"]*\)".*/\1/p')"
for _v in ${java_supported//,/ } ; do
  reset_stub
  run_case "${cassandra_dir}/.build/docker/run-tests.sh" -a test "${_v}"
  expect_rc "docker/run-tests.sh rejects a positional java version ${_v}" 1
  expect_out "docker/run-tests.sh names -j for the positional ${_v}" "Use '-j ${_v}'"
  expect_no_invocation "docker/run-tests.sh starts no container for the positional ${_v}" "docker run"
done

# the guard reads whole arguments only, so a version inside a value must survive it
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" -a test -t StorageServiceServerTest -b cassandra-11
expect_rc "docker/run-tests.sh keeps an argument that merely contains a java version" 0
expect_invocation "docker/run-tests.sh passes that argument downstream" "\-b cassandra-11"

################################
#
# Cases: docker/run-tests.sh, and the deprecated positional arguments
#
################################

# the legacy form is <target> [<test regexp|chunk>] [<java version>]
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" test StorageServiceServerTest 17
expect_rc "docker/run-tests.sh runs three legacy arguments" 0
expect_out "docker/run-tests.sh shows the flags the legacy arguments became" "format: -a test -t StorageServiceServerTest -j 17"
expect_invocation "docker/run-tests.sh sets java from the third legacy argument" "_set_java.sh 17"

reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" test 1/8
expect_rc "docker/run-tests.sh reads a legacy X/Y as a chunk" 0
expect_invocation "docker/run-tests.sh turns the legacy chunk into -c" "_docker_init_tests.sh \-a test \-c 1/8"

# with two legacy arguments the second reads as the java version, not as a test name regexp
reset_stub
run_case "${cassandra_dir}/.build/docker/run-tests.sh" test 17
expect_rc "docker/run-tests.sh runs two legacy arguments" 0
expect_out "docker/run-tests.sh shows the second legacy argument as -j" "format: -a test  -j 17"
expect_invocation "docker/run-tests.sh sets java from the second legacy argument" "_set_java.sh 17"
expect_no_invocation "docker/run-tests.sh reads no test name regexp from it" "\-t 17"

report stub-docker
