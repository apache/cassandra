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

# Render a JMH run as JUnit xml, so CI lists a failed benchmark as a failed test.
#
# A filter: the JMH output (ant's, with its "[java]" tags, or JMH's own) is read on
# stdin and copied to stdout unchanged.  Reading it as it is printed is what times
# each benchmark, as JMH prints no durations.
#
# Every benchmark JMH starts prints a "# JMH version:" header.  One that ended with
# a result is in the -rff json, so any started benchmark missing from it failed; the
# output from its "<failure>" (or "<forked VM failed ...>") on is the failure text.
# This needs the run to be made with "-foe false", or JMH stops at the first failure.
#
# Once the output ends a TEST-<benchmark class>.xml is written for each class run,
# and the failures are printed in the form .build/sh/test-log-summary.py reads.
#
# The exit code: 0 when every benchmark passed, or none ran; 1 when any failed;
# 2 when the json is missing, as then the run did not complete and no xml is written.
#
# Usage: ant microbench -Djmh.foe=false 2>&1 \
#          | jmh-junit-report.py --result <dir>/jmh-result.json --output-dir <dir> --tag _jdk11

import argparse
import collections
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET

# ant's tag on the forked java's output
ANT_TAG_RE = re.compile(r"^\s*\[java\] ?")
# every way JMH reports a failed benchmark: an exception (thrown by the benchmark or a
# fixture), a forked VM that exited early or failed to start, an interrupted host VM.
# Not anchored, as JMH prints it after the "Iteration   1: " that was running.
FAILURE_RE = re.compile(r"<(?:failure|forked VM failed|failed to invoke the VM|host VM has been interrupted)")
# Mode.longLabel() to Mode.shortLabel(), the json's
MODES = {
    "Throughput, ops/time": "thrpt",
    "Average time, time/op": "avgt",
    "Sampling time": "sample",
    "Single shot invocation time": "ss",
    "All benchmark modes": "all",
}
# a failure keeps this many lines from its marker, and one with no marker its last lines
FAILURE_LINES = 500
TAIL_LINES = 50
# XML 1.0 has no escape for these, and benchmark output (colour codes) holds them
INVALID_XML_RE = re.compile("[^\u0009\u000a\u000d\u0020-\ud7ff\ue000-\ufffd\U00010000-\U0010ffff]")


class Benchmark:
    """One benchmark JMH started: a method, in one mode, with one set of params."""

    def __init__(self, started):
        self.name = None      # org.apache.cassandra.test.microbench.FooBench.method
        self.mode = None      # thrpt
        self.params = ""      # "size = 1, type = x", as JMH prints them
        self.in_header = True
        self.started = started
        self.ended = started
        self.failure = None   # the lines from the failure marker on
        self.tail = collections.deque(maxlen=TAIL_LINES)

    def key(self):
        return self.name, self.mode, self.params

    def class_name(self):
        return self.name.rpartition(".")[0]

    def test_name(self, tag):
        # [mode,params], as a parameterized JUnit test is named; the mode keeps a benchmark run
        # in several modes apart
        labels = [self.mode] + ([self.params.replace(" = ", "=").replace(", ", ",")] if self.params else [])
        return "%s[%s]%s" % (self.name.rpartition(".")[2], ",".join(labels), "-" + tag if tag else "")

    def failure_message(self):
        if not self.failure:
            return "no result in the jmh json"
        marker = self.failure[0].strip()
        if marker != "<failure>":
            # "<forked VM failed with exit code 3>", "<failure: VM prematurely exited ...>"
            return marker.strip("<>")
        # the exception, "java.lang.IllegalStateException: boom", is on the next line printed
        return next((line.strip() for line in self.failure[1:] if line.strip()), "failure")

    def failure_text(self):
        return "\n".join(self.failure if self.failure else self.tail).rstrip()


class Parser:
    """Splits JMH's output into the benchmarks it ran."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.benchmarks = []
        self.current = None

    def feed(self, line):
        line = ANT_TAG_RE.sub("", line.rstrip("\r\n"))
        if line.startswith("# JMH version: "):
            now = self.clock()
            self._end(now)
            self.current = Benchmark(now)
            self.benchmarks.append(self.current)
            return
        if line.startswith("# Run complete."):
            self._end(self.clock())
            return
        b = self.current
        if b is None:
            return
        if b.in_header:
            # the header ends at its first blank line
            if not line.strip():
                b.in_header = False
            elif line.startswith("# Benchmark mode: "):
                label = line[len("# Benchmark mode: "):]
                b.mode = MODES.get(label, label)
            elif line.startswith("# Benchmark: "):
                b.name = line[len("# Benchmark: "):].strip()
            elif line.startswith("# Parameters: (") and line.endswith(")"):
                b.params = line[len("# Parameters: ("):-1]
            return
        if b.failure is None:
            m = FAILURE_RE.search(line)
            if m:
                b.failure = [line[m.start():]]
            else:
                b.tail.append(line)
        elif len(b.failure) < FAILURE_LINES:
            b.failure.append(line)

    def finish(self):
        if self.current is not None:
            self._end(self.clock())
        # a header cut short names no benchmark, and has nothing to report
        return [b for b in self.benchmarks if b.name]

    def _end(self, now):
        if self.current is not None:
            self.current.ended = now
            self.current = None


def result_keys(results):
    """The (benchmark, mode, params) of every benchmark in the jmh json, the ones that passed."""
    return {(r["benchmark"], r["mode"], ", ".join("%s = %s" % kv for kv in (r.get("params") or {}).items()))
            for r in results}


def _xml_safe(s):
    return INVALID_XML_RE.sub("\ufffd", s)


def build_suites(benchmarks, passed, tag):
    """Map each benchmark class to its <testsuite>, and return those and the failed benchmarks."""
    by_class = collections.OrderedDict()
    for b in benchmarks:
        by_class.setdefault(b.class_name(), []).append(b)

    suites = collections.OrderedDict()
    failed = []
    for class_name, members in by_class.items():
        suite = ET.Element("testsuite")
        # named as CassandraXMLJUnitResultFormatter names a suite: class, then the tag
        suite.set("name", _xml_safe(class_name + ("-" + tag if tag else "")))
        failures = 0
        total = 0.0
        for b in members:
            seconds = b.ended - b.started
            total += seconds
            case = ET.SubElement(suite, "testcase")
            case.set("classname", _xml_safe(class_name))
            case.set("name", _xml_safe(b.test_name(tag)))
            case.set("time", "%.3f" % seconds)
            if b.key() not in passed:
                failures += 1
                failed.append(b)
                failure = ET.SubElement(case, "failure")
                failure.set("message", _xml_safe(b.failure_message()))
                failure.text = _xml_safe(b.failure_text())
        suite.set("tests", str(len(members)))
        suite.set("failures", str(failures))
        suite.set("errors", "0")
        suite.set("skipped", "0")
        suite.set("time", "%.3f" % total)
        suites[class_name] = suite
    return suites, failed


def _indent(suite):
    # one testcase a line. ElementTree.indent() is python 3.9+
    cases = list(suite)
    suite.text = "\n  "
    suite.tail = "\n"
    for i, case in enumerate(cases):
        case.tail = "\n" if i == len(cases) - 1 else "\n  "


def write_suites(suites, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for class_name, suite in suites.items():
        _indent(suite)
        ET.ElementTree(suite).write(os.path.join(output_dir, "TEST-%s.xml" % class_name),
                                    encoding="UTF-8", xml_declaration=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy JMH output from stdin to stdout, and render it as JUnit xml"
    )
    parser.add_argument("--result", required=True, help="the JMH json result file (-rf json -rff <file>)")
    parser.add_argument("--output-dir", required=True, help="directory to write the TEST-*.xml files to")
    parser.add_argument("--tag", default="", help="suffix of the suite and test names, as -Dcassandra.testtag")
    return parser.parse_args()


def main():
    args = parse_args()
    parser = Parser()
    out = sys.stdout.buffer
    for raw in sys.stdin.buffer:
        out.write(raw)
        out.flush()
        parser.feed(raw.decode("utf-8", errors="replace"))
    benchmarks = parser.finish()
    if not benchmarks:
        return 0

    try:
        with open(args.result, "r", encoding="utf-8") as f:
            passed = result_keys(json.load(f))
    except (OSError, ValueError) as e:
        print("jmh-junit-report: no JUnit xml written, as the JMH run did not complete (%s)" % e)
        return 2

    suites, failed = build_suites(benchmarks, passed, args.tag)
    write_suites(suites, args.output_dir)
    for b in failed:
        print("Testcase: %s(%s):\tFAILED" % (b.test_name(args.tag), b.class_name()))
    # the line generate-test-report prints, which microbench has no call of
    print("[Test Summary] Run: %d, Failed: %d, Errors: 0, Skipped: 0" % (len(benchmarks), len(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
