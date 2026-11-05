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

# Unit tests for .build/sh/test-log-summary.py and .build/sh/ant-log-summary.py.
#
# Run with:  python3 -m unittest discover -s .build/sh/test

import importlib.util
import pathlib
import subprocess
import sys
import unittest

SH_DIR = pathlib.Path(__file__).resolve().parents[1]
TEST_LOG_SUMMARY = SH_DIR / "test-log-summary.py"
ANT_LOG_SUMMARY = SH_DIR / "ant-log-summary.py"


def _load(name, path):
    """Import a script whose file name is not a legal module name."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


summariser = _load("cassandra_test_log_summary", TEST_LOG_SUMMARY)


def summary_line(run=10, failed=0, errors=0, skipped=0, tag=""):
    return "%s[Test Summary] Run: %d, Failed: %d, Errors: %d, Skipped: %d" % (
        tag,
        run,
        failed,
        errors,
        skipped,
    )


class SummarizeTest(unittest.TestCase):
    """Exercise summarize() directly.  Text in, (failed, lines) out."""

    def summarize(self, content):
        return summariser.summarize(content)

    def test_clean_pass(self):
        failed, out = self.summarize(summary_line(run=42))
        self.assertFalse(failed)
        self.assertIn("TESTS PASSED", out)

    def test_clean_pass_with_ant_task_tag(self):
        # ant prefixes concat output with a task tag
        failed, out = self.summarize(summary_line(run=42, tag="   [concat] "))
        self.assertFalse(failed)
        self.assertIn("TESTS PASSED", out)
        self.assertTrue(any(line.startswith("[Test Summary]") for line in out))

    def test_skipped_tests_alone_pass(self):
        failed, _ = self.summarize(summary_line(run=42, skipped=7))
        self.assertFalse(failed)

    def test_failures_fail(self):
        failed, out = self.summarize(summary_line(run=42, failed=2))
        self.assertTrue(failed)
        self.assertNotIn("TESTS PASSED", out)

    def test_errors_fail(self):
        failed, _ = self.summarize(summary_line(run=42, errors=1))
        self.assertTrue(failed)

    def test_last_summary_does_not_mask_an_earlier_failure(self):
        content = "\n".join([summary_line(failed=3), summary_line()])
        failed, _ = self.summarize(content)
        self.assertTrue(failed)

    def test_run_tests_failed_target(self):
        content = "\n".join(
            [
                "failed unit testclasslist 1/1 StorageServiceServerTest",
                summary_line(),
            ]
        )
        failed, out = self.summarize(content)
        self.assertTrue(failed)
        self.assertIn("failed unit testclasslist 1/1 StorageServiceServerTest", out)

    def test_failed_to_prose_is_not_a_failed_target(self):
        # _build_all_dtest_jars logs prose that must not read as a target failure
        content = "\n".join(
            [
                "WARNING: could not reset/clean /tmp/cassandra-dtest-jars… continuing…",
                "failed to parse something harmless",
                summary_line(),
            ]
        )
        failed, _ = self.summarize(content)
        self.assertFalse(failed)

    def test_failure_rate_non_zero_fails(self):
        failed, out = self.summarize("failure rate: 3/500")
        self.assertTrue(failed)
        self.assertTrue(any("failure rate: 3/500" in line for line in out))

    def test_failure_rate_zero_passes(self):
        content = "\n".join(["failure rate: 0/500", summary_line()])
        failed, _ = self.summarize(content)
        self.assertFalse(failed)

    def test_testcase_failure_without_a_summary_fails(self):
        # microbench, microbench-test and build_dtest_jars skip generate-test-report,
        # so there is no [Test Summary] line to read the verdict from
        content = "    [junit] Testcase: testFoo(org.apache.cassandra.FooTest):\tFAILED"
        failed, out = self.summarize(content)
        self.assertTrue(failed)
        self.assertIn("Failed tests:", out)

    def test_testcase_error_without_a_summary_fails(self):
        content = (
            "    [junit] Testcase: testFoo(org.apache.cassandra.FooTest):"
            "\tCaused an ERROR"
        )
        failed, _ = self.summarize(content)
        self.assertTrue(failed)

    def test_duplicate_testcase_lines_are_reported_once(self):
        line = "Testcase: testFoo(org.apache.cassandra.FooTest):\tFAILED"
        failed, out = self.summarize("\n".join([line, line, line]))
        self.assertTrue(failed)
        self.assertEqual(1, out.count(line))

    def test_build_failed_reports_compilation_errors(self):
        content = "\n".join(
            [
                "compile:",
                "    [javac] /src/Foo.java:12: error: cannot find symbol",
                "    [javac] 1 error",
                "BUILD FAILED",
            ]
        )
        failed, out = self.summarize(content)
        self.assertTrue(failed)
        self.assertIn("BUILD FAILED", out)
        self.assertIn("Compilation Errors:", out)
        self.assertTrue(any("cannot find symbol" in line for line in out))

    def test_microbench_marker_passes(self):
        failed, out = self.summarize("microbench completed successfully")
        self.assertFalse(failed)
        self.assertIn("TESTS PASSED", out)
        self.assertIn("microbench completed successfully", out)

    def test_microbench_failure_fails(self):
        failed, _ = self.summarize("failed microbench 1/1")
        self.assertTrue(failed)

    def test_empty_input_passes_with_a_notice(self):
        failed, out = self.summarize("")
        self.assertFalse(failed)
        self.assertTrue(any("No test summary found" in line for line in out))


class ExitCodeTest(unittest.TestCase):
    """Exercise the two scripts as processes, since the shell reads $?."""

    def run_script(self, script, stdin_text, args=("-",)):
        proc = subprocess.run(
            [sys.executable, str(script), *args],
            input=stdin_text,
            capture_output=True,
            text=True,
        )
        return proc.returncode, proc.stdout

    def test_test_log_summary_passes_with_zero(self):
        rc, out = self.run_script(TEST_LOG_SUMMARY, summary_line())
        self.assertEqual(0, rc)
        self.assertIn("TESTS PASSED", out)

    def test_test_log_summary_fails_with_one(self):
        rc, _ = self.run_script(TEST_LOG_SUMMARY, summary_line(failed=1))
        self.assertEqual(1, rc)

    def test_test_log_summary_reads_stdin_by_default(self):
        rc, out = self.run_script(TEST_LOG_SUMMARY, summary_line(), args=())
        self.assertEqual(0, rc)
        self.assertIn("TESTS PASSED", out)

    def test_test_log_summary_missing_file(self):
        # 2, as both summarizers reserve 1 for "the log reports a failure"
        rc, _ = self.run_script(TEST_LOG_SUMMARY, "", args=("/nonexistent.log",))
        self.assertEqual(2, rc)

    def test_ant_log_summary_passes_with_zero(self):
        rc, out = self.run_script(ANT_LOG_SUMMARY, "jar:\nBUILD SUCCESSFUL\n")
        self.assertEqual(0, rc)
        self.assertIn("BUILD SUCCESSFUL", out)

    def test_ant_log_summary_fails_with_one(self):
        content = "\n".join(
            [
                "checkstyle:",
                "    [checkstyle] /src/Foo.java:1: line too long",
                "BUILD FAILED",
            ]
        )
        rc, out = self.run_script(ANT_LOG_SUMMARY, content)
        self.assertEqual(1, rc)
        self.assertIn("BUILD FAILED", out)
        self.assertIn("Failed target: checkstyle", out)

    def test_ant_log_summary_missing_file(self):
        # 2, as both summarizers reserve 1 for "the log reports a failure"
        rc, _ = self.run_script(ANT_LOG_SUMMARY, "", args=("/nonexistent.log",))
        self.assertEqual(2, rc)


if __name__ == "__main__":
    unittest.main()
