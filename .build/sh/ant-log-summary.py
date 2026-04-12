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

import argparse
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse Apache Cassandra ant build logs and summarize failures"
    )
    parser.add_argument(
        "log_file",
        help='Path to the ant build log file to analyze (use "-" to read from stdin)',
    )
    return parser.parse_args()


def find_build_failures(content):
    """
    Parse the log content and extract failure information.
    Returns a tuple: (is_failed, failed_target, failure_output)
    """
    lines = content.split("\n")

    # Check if build was successful first
    if "BUILD FAILED" not in content:
        return False, None, []

    # Track what ant targets are running before the build failed
    failed_target = None
    failed_target_line = -1
    for i, line in enumerate(lines):
        if "BUILD FAILED" in line:
            break
        # Keep track of tasks so when we finally see the BUILD FAILED we know what the last task was
        line_stripped = line.strip()
        if line_stripped.endswith(":") and not line.startswith(" ") and "[" not in line:
            failed_target = line_stripped[:-1]
            failed_target_line = i

    target_start_idx = 0
    if failed_target and failed_target_line >= 0:
        target_start_idx = failed_target_line

    # Collect the output from the failed target up to BUILD FAILED
    failure_output = []
    for line in lines[target_start_idx:]:
        stripped = line.strip()
        if stripped:
            failure_output.append(line.rstrip())

    return True, failed_target, failure_output


def extract_compilation_errors(content):
    """Extract compilation error details specifically."""
    lines = content.split("\n")
    error_lines = []

    for line in lines:
        if "[javac]" in line and ("error:" in line or "errors" in line):
            # Clean up the line to show just the error
            clean_line = line.replace("[javac]", "").strip()
            if clean_line:
                error_lines.append(clean_line)

    return error_lines


def extract_test_failures(content):
    """Extract test failure details specifically."""
    lines = content.split("\n")
    test_failures = []

    for line in lines:
        if "Test " in line and "FAILED" in line:
            test_failures.append(line.strip())

    return test_failures


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
        sys.exit(1)
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(1)

    is_failed, failed_target, failure_output = find_build_failures(content)

    if not is_failed:
        print("BUILD SUCCESSFUL")
        sys.exit(0)

    print("BUILD FAILED")
    if failed_target:
        print(f"Failed target: {failed_target}")

    print("=" * 50)

    # Special handling for compilation and test failures
    if failed_target and (
        "test" in failed_target.lower() or "compile" in failed_target.lower()
    ):
        # Extract compilation errors if it's a compile-related target
        compilation_errors = extract_compilation_errors("\n".join(failure_output))
        if compilation_errors:
            print("\nCompilation Errors:")
            print("-" * 20)
            for error in compilation_errors:
                print(error)
            # Also show the full output for context
            print(f"\nFull output from failed target '{failed_target}':")
            print("-" * 40)
            for line in failure_output:
                print(line)
            sys.exit(1)

        # Extract test failures if it's a test-related target
        test_failures = extract_test_failures("\n".join(failure_output))
        if test_failures:
            print("\nTest Failures:")
            print("-" * 15)
            for failure in test_failures:
                print(failure)
            # Always show the full output for test failures - this is crucial for debugging
            print(f"\nFull output from failed target '{failed_target}':")
            print("-" * 40)
            for line in failure_output:
                print(line)
            sys.exit(1)

    # For all other targets or if no specific errors found, show the task output
    if failure_output:
        print(f"\nOutput from failed target '{failed_target}':")
        print("-" * 40)
        for line in failure_output:
            print(line)

    sys.exit(1)


if __name__ == "__main__":
    main()
