#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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

# Summarize the output of .build/run-tests.sh.
#
# Unlike ant-log-summary.py (which keys off ant's BUILD FAILED/SUCCESSFUL),
# run-tests.sh deliberately continues past failing tests, as ant still prints
# BUILD SUCCESSFUL with test failures.
#
# Failures are identified from the test results instead:
#   - the "[Test Summary] Run: N, Failed: N, Errors: N, Skipped: N" line
#     emitted by ant's generate-test-report target,
#   - run-tests.sh's own "failed <prefix> <target> ..." lines,
#   - "failure rate: X/Y" lines printed for repeated runs,
#   - per-test "Testcase: <name>:\tFAILED / Caused an ERROR" markers from the
#     brief JUnit formatter,
#   - a compile/setup "BUILD FAILED" (with the [javac] errors).
#
# The exit code mirrors the outcome: 0 for a clean run, 1 if any failure signal is
# seen, and 2 if the log cannot be read.  ant-log-summary.py uses the same codes.

import argparse
import re
import sys

TEST_SUMMARY_RE = re.compile(
    r"\[Test Summary\]\s*Run:\s*(\d+),\s*Failed:\s*(\d+),\s*Errors:\s*(\d+),\s*Skipped:\s*(\d+)"
)
FAILURE_RATE_RE = re.compile(r"failure rate:\s*(\d+)/(\d+)")
# run-tests.sh: echo "failed ${_target_prefix} ${_testlist_target} ..."
# the negative lookahead skips prose such as "failed to reset/clean …"
RUN_TESTS_FAILED_RE = re.compile(r"^failed\s+(?!to\s)\S+")
# brief JUnit formatter: "Testcase: <name>:\tFAILED" / ":\tCaused an ERROR",
# optionally prefixed by an ant task tag such as "[junit] ".
TESTCASE_FAIL_RE = re.compile(r"^(?:\[[^\]]+\]\s*)?Testcase:\s.*\b(FAILED|Caused an ERROR)\b")
# run-tests.sh microbench success marker: microbench emits no JUnit xml
MICROBENCH_OK_RE = re.compile(r"^\S+\s+completed successfully")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize Apache Cassandra .build/run-tests.sh output"
    )
    parser.add_argument(
        "log_file",
        nargs="?",
        default="-",
        help='Path to the run-tests.sh log (use "-" or omit to read from stdin)',
    )
    return parser.parse_args()


ANT_TAG_RE = re.compile(r"^\[[^\]]+\]\s*")


def _strip_tag(line):
    """Drop a leading ant task tag such as '[junit] ' or '[echo] '."""
    return ANT_TAG_RE.sub("", line.strip())


def summarize(content):
    """Return (failed, lines) where failed is a bool and lines is the summary."""
    lines = content.split("\n")

    summaries = []          # [Test Summary] lines
    failed_targets = []     # run-tests.sh "failed ..." lines
    failure_rates = []      # "failure rate: X/Y" lines
    failed_testcases = []   # per-test FAILED / ERROR markers
    javac_errors = []       # compile errors, when a BUILD FAILED is present
    microbench_ok = []      # microbench success markers

    build_failed = "BUILD FAILED" in content

    for line in lines:
        stripped = line.strip()
        m = TEST_SUMMARY_RE.search(line)
        if m:
            run, failures, errors, skipped = (int(g) for g in m.groups())
            summaries.append((_strip_tag(line), failures, errors))
            continue
        if RUN_TESTS_FAILED_RE.match(stripped):
            failed_targets.append(stripped)
            continue
        m = FAILURE_RATE_RE.search(line)
        if m:
            failure_rates.append((stripped, int(m.group(1))))
            continue
        if TESTCASE_FAIL_RE.match(stripped):
            failed_testcases.append(_strip_tag(line))
            continue
        if MICROBENCH_OK_RE.match(stripped):
            microbench_ok.append(stripped)
            continue
        if build_failed and "[javac]" in line and ("error:" in line or "errors" in line):
            clean = line.replace("[javac]", "").strip()
            if clean:
                javac_errors.append(clean)

    # decide pass/fail
    failed = build_failed
    for _, failures, errors in summaries:
        if failures or errors:
            failed = True
    if failed_targets:
        failed = True
    if failed_testcases:
        # targets that emit no JUnit xml have no [Test Summary] line to read
        failed = True
    for _, n in failure_rates:
        if n:
            failed = True

    # build the summary output
    out = []
    if build_failed:
        out.append("BUILD FAILED")
        if javac_errors:
            out.append("")
            out.append("Compilation Errors:")
            out.append("-" * 20)
            out.extend(javac_errors)

    if failed_testcases:
        out.append("")
        out.append("Failed tests:")
        out.append("-" * 13)
        # de-duplicate while preserving order
        seen = set()
        for tc in failed_testcases:
            if tc not in seen:
                seen.add(tc)
                out.append(tc)

    if failed_targets:
        out.append("")
        out.extend(failed_targets)

    for line, _ in failure_rates:
        out.append(line)

    if summaries:
        out.append("")
        for line, _, _ in summaries:
            out.append(line)
    elif microbench_ok:
        # microbench emits no JUnit xml; its own success marker is the report
        out.append("")
        out.extend(microbench_ok)
    elif not build_failed and not failed:
        # nothing ran a test report and nothing failed
        out.append("No test summary found (nothing ran, or non-test target).")

    if not failed and (summaries or microbench_ok):
        out.append("")
        out.append("TESTS PASSED")

    return failed, out


def main():
    args = parse_args()

    try:
        if args.log_file == "-":
            content = sys.stdin.read()
        else:
            with open(args.log_file, "r") as f:
                content = f.read()
    except FileNotFoundError:
        print(f"Error: Log file '{args.log_file}' not found")
        sys.exit(2)
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(2)

    failed, out = summarize(content)
    print("\n".join(out))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
