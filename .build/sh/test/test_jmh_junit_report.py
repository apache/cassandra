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

# Unit tests for .build/sh/jmh-junit-report.py.
#
# Run with:  python3 -m unittest discover -s .build/sh/test

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

SH_DIR = pathlib.Path(__file__).resolve().parents[1]
JMH_JUNIT_REPORT = SH_DIR / "jmh-junit-report.py"


def _load(name, path):
    """Import a script whose file name is not a legal module name."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


report = _load("cassandra_jmh_junit_report", JMH_JUNIT_REPORT)

BENCH = "org.apache.cassandra.test.microbench.FooBench"

# what JMH 1.37 prints for the failures, copied from runs of benchmarks made to fail
EXCEPTION = [
    "Iteration   1: <failure>",
    "",
    "java.lang.IllegalStateException: boom",
    "\tat org.apache.cassandra.test.microbench.FooBench.method(FooBench.java:21)",
    "",
    "",
]
CRASH = [
    "Iteration   1: <failure: VM prematurely exited before JMH had finished with it, explicit System.exit was called?>",
    "<forked VM failed with exit code 3>",
    "<stdout last='20 lines'>",
    "</stdout>",
    "<stderr last='20 lines'>",
    "</stderr>",
    "",
]
PASS = ["Iteration   1: 369234.405 ops/ms", "", "", 'Result "%s.method":' % BENCH, "  369234.405 ops/ms", "", ""]


def section(name=BENCH + ".method", mode="Throughput, ops/time", params=None, body=PASS):
    """The lines JMH prints for one benchmark: its header, then the run."""
    lines = [
        "# JMH version: 1.37",
        "# VM version: JDK 11.0.31, OpenJDK 64-Bit Server VM, 11.0.31+11",
        "# Warmup: <none>",
        "# Measurement: 1 iterations, 1 s each",
        "# Benchmark mode: " + mode,
        "# Benchmark: " + name,
    ]
    if params:
        lines.append("# Parameters: (%s)" % params)
    return lines + ["", "# Run progress: 0.00% complete, ETA 00:00:08", "# Fork: 1 of 1"] + body


def result(name=BENCH + ".method", mode="thrpt", params=None):
    """One entry of the -rff json."""
    return {"benchmark": name, "mode": mode, "params": params or {}}


def parse(lines, clock=None):
    parser = report.Parser(clock) if clock else report.Parser()
    for line in lines:
        parser.feed(line + "\n")
    return parser.finish()


class ParserTest(unittest.TestCase):
    """Text in, the benchmarks JMH ran out."""

    def test_reads_the_header(self):
        [b] = parse(section(params="size = 1, type = x"))
        self.assertEqual((BENCH + ".method", "thrpt", "size = 1, type = x"), b.key())
        self.assertEqual(BENCH, b.class_name())

    def test_strips_the_ant_tag(self):
        [b] = parse(["     [java] " + line for line in section(body=EXCEPTION)])
        self.assertEqual((BENCH + ".method", "thrpt", ""), b.key())
        self.assertEqual("java.lang.IllegalStateException: boom", b.failure_message())

    def test_splits_benchmarks_at_each_header(self):
        lines = section(params="size = 1") + section(params="size = 2", body=EXCEPTION) + ["# Run complete. Total time: 00:00:08"]
        first, second = parse(lines)
        self.assertEqual("size = 1", first.params)
        self.assertIsNone(first.failure)
        self.assertEqual("size = 2", second.params)
        self.assertIsNotNone(second.failure)

    def test_maps_every_mode_to_the_json_label(self):
        modes = {"Throughput, ops/time": "thrpt", "Average time, time/op": "avgt", "Sampling time": "sample",
                 "Single shot invocation time": "ss"}
        for label, short in modes.items():
            [b] = parse(section(mode=label))
            self.assertEqual(short, b.mode)

    def test_a_parameter_value_may_end_in_a_bracket(self):
        [b] = parse(section(params="f = g(x)"))
        self.assertEqual("f = g(x)", b.params)

    def test_header_lines_printed_by_a_benchmark_are_not_read(self):
        [b] = parse(section(body=["# Benchmark: printed.by.the.Benchmark", "# Parameters: (a = 1)"] + PASS))
        self.assertEqual(BENCH + ".method", b.name)
        self.assertEqual("", b.params)

    def test_times_each_benchmark_until_the_next_starts(self):
        ticks = iter([10.0, 12.5, 20.0])
        lines = section() + section() + ["# Run complete. Total time: 00:00:10"]
        first, second = parse(lines, clock=lambda: next(ticks))
        self.assertEqual(2.5, first.ended - first.started)
        self.assertEqual(7.5, second.ended - second.started)

    def test_output_after_the_run_is_not_a_benchmark(self):
        lines = section() + ["# Run complete. Total time: 00:00:08", "Benchmark  Mode  Cnt  Score", "<failure>"]
        [b] = parse(lines)
        self.assertIsNone(b.failure)

    def test_a_header_cut_short_is_dropped(self):
        self.assertEqual([], parse(["# JMH version: 1.37", "# VM version: JDK 11"]))


class FailureTest(unittest.TestCase):
    """What a failed benchmark reports."""

    def test_exception(self):
        [b] = parse(section(body=EXCEPTION))
        self.assertEqual("java.lang.IllegalStateException: boom", b.failure_message())
        text = b.failure_text()
        self.assertTrue(text.startswith("<failure>"))
        self.assertIn("FooBench.java:21", text)
        self.assertFalse(text.endswith("\n"))

    def test_crashed_fork(self):
        [b] = parse(section(body=CRASH))
        self.assertEqual("failure: VM prematurely exited before JMH had finished with it, explicit System.exit was called?",
                         b.failure_message())
        self.assertIn("<forked VM failed with exit code 3>", b.failure_text())

    def test_fork_that_failed_to_start(self):
        [b] = parse(section(body=["<failed to invoke the VM, caught IOException: no java>", ""]))
        self.assertEqual("failed to invoke the VM, caught IOException: no java", b.failure_message())

    def test_no_marker_reports_the_last_lines(self):
        [b] = parse(section(body=["line %d" % i for i in range(report.TAIL_LINES + 10)]))
        self.assertEqual("no result in the jmh json", b.failure_message())
        self.assertTrue(b.failure_text().startswith("line 10\n"))

    def test_failure_text_is_capped(self):
        [b] = parse(section(body=EXCEPTION + ["\tat Frame.f(F.java:%d)" % i for i in range(2 * report.FAILURE_LINES)]))
        self.assertEqual(report.FAILURE_LINES, len(b.failure))


class SuitesTest(unittest.TestCase):
    """Benchmarks and the json in, the JUnit <testsuite>s out."""

    def suites(self, lines, results, tag="_jdk11"):
        return report.build_suites(parse(lines), report.result_keys(results), tag)

    def test_passed_is_in_the_json_and_failed_is_not(self):
        lines = section(params="size = 1") + section(params="size = 2", body=EXCEPTION)
        suites, failed = self.suites(lines, [result(params={"size": "1"})])
        suite = suites[BENCH]
        self.assertEqual(BENCH + "-_jdk11", suite.get("name"))
        self.assertEqual(("2", "1"), (suite.get("tests"), suite.get("failures")))
        passed, failing = suite.findall("testcase")
        self.assertIsNone(passed.find("failure"))
        self.assertEqual("java.lang.IllegalStateException: boom", failing.find("failure").get("message"))
        self.assertEqual([failing.get("name")], [b.test_name("_jdk11") for b in failed])

    def test_names_a_test_by_mode_and_params(self):
        lines = section(params="size = 1, type = x") + section(mode="Average time, time/op")
        suites, _ = self.suites(lines, [result(params={"size": "1", "type": "x"}), result(mode="avgt")])
        names = [case.get("name") for case in suites[BENCH].findall("testcase")]
        self.assertEqual(["method[thrpt,size=1,type=x]-_jdk11", "method[avgt]-_jdk11"], names)
        self.assertEqual({BENCH}, {case.get("classname") for case in suites[BENCH].findall("testcase")})

    def test_no_tag(self):
        suites, _ = self.suites(section(), [result()], tag="")
        self.assertEqual(BENCH, suites[BENCH].get("name"))
        self.assertEqual("method[thrpt]", suites[BENCH].find("testcase").get("name"))

    def test_one_suite_per_class(self):
        other = "org.apache.cassandra.test.microbench.BarBench"
        lines = section() + section(name=other + ".run")
        suites, _ = self.suites(lines, [result(), result(name=other + ".run")])
        self.assertEqual([BENCH, other], list(suites))

    def test_benchmark_output_cannot_break_the_xml(self):
        body = ["Iteration   1: <failure>", "", "java.lang.Error: \x1b[31mred\x1b[0m & <tag> \x00"]
        suites, _ = self.suites(section(body=body), [])
        report._indent(suites[BENCH])
        parsed = ET.fromstring(ET.tostring(suites[BENCH]))
        self.assertEqual("java.lang.Error: \ufffd[31mred\ufffd[0m & <tag> \ufffd",
                         parsed.find("testcase/failure").get("message"))


class ExitCodeTest(unittest.TestCase):
    """Exercise the script as a process: the pipe, the files, the exit code."""

    def run_script(self, lines, results):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        result_file = self.dir / "jmh-result.json"
        if results is not None:
            result_file.write_text(json.dumps(results))
        stdin = "".join("     [java] %s\n" % line for line in lines)
        proc = subprocess.run(
            [sys.executable, str(JMH_JUNIT_REPORT), "--result", str(result_file), "--output-dir", str(self.dir / "out"),
             "--tag", "_jdk11"],
            input=stdin.encode("utf-8"),
            capture_output=True,
        )
        self.assertTrue(proc.stdout.startswith(stdin.encode("utf-8")), "the output must be copied first")
        return proc.returncode, proc.stdout[len(stdin.encode("utf-8")):].decode("utf-8")

    def xml_files(self):
        return sorted(p.name for p in (self.dir / "out").glob("*.xml")) if (self.dir / "out").exists() else []

    def test_all_passed_exits_zero(self):
        rc, out = self.run_script(section(), [result()])
        self.assertEqual(0, rc)
        self.assertIn("[Test Summary] Run: 1, Failed: 0, Errors: 0, Skipped: 0", out)
        self.assertEqual(["TEST-%s.xml" % BENCH], self.xml_files())
        self.assertEqual("1", ET.parse(self.dir / "out" / ("TEST-%s.xml" % BENCH)).getroot().get("tests"))

    def test_a_failure_exits_one_and_names_it(self):
        rc, out = self.run_script(section() + section(params="size = 2", body=EXCEPTION), [result()])
        self.assertEqual(1, rc)
        # the brief JUnit formatter's line, which .build/sh/test-log-summary.py lists
        self.assertIn("Testcase: method[thrpt,size=2]-_jdk11(%s):\tFAILED" % BENCH, out)
        self.assertIn("[Test Summary] Run: 2, Failed: 1, Errors: 0, Skipped: 0", out)
        self.assertEqual(["TEST-%s.xml" % BENCH], self.xml_files())

    def test_no_json_exits_two_and_writes_nothing(self):
        rc, out = self.run_script(section(), None)
        self.assertEqual(2, rc)
        self.assertIn("no JUnit xml written", out)
        self.assertEqual([], self.xml_files())

    def test_no_benchmark_exits_zero_and_writes_nothing(self):
        rc, out = self.run_script(["BUILD FAILED"], None)
        self.assertEqual(0, rc)
        self.assertEqual("", out)
        self.assertEqual([], self.xml_files())


if __name__ == "__main__":
    unittest.main()
