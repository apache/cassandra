#!/usr/bin/env python3
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

"""How evenly each test target's splits divided, and which of them came near their deadline.

What makes a split long: `_split_tests` in ../run-tests.sh deals an alphabetically sorted class list
round-robin with `split -n r/K/N`, which balances the count of classes and knows nothing of their duration.

Round-robin is required, so that which split holds a test is predictable and easy to find.

This report cannot tell a structurally long split from one that drew a
slow agent, so change a split count only when consecutive builds agree.

Usage:

    cell_balance.py [--input build/test] [--output build/ci_summary.html] [--budget-margin 15]

`--input` holds `cell-times/*.tsv`, one per cell from `recordCellTime` in the Jenkinsfile, and
`output/<target>/` of decompressed JUnit XML.  A cell records whether it ended ok, hit its deadline, or failed,
so a target that never finishes still gets a row.  With no records at all it says so and exits 0: this is not
the check to fail a build on.
"""

import argparse
import os
import statistics
import sys
import xml.etree.ElementTree as ElementTree

# Columns of a cell-times record, in order.  test_seconds covers every class and not only the twelve .suites
# lists, so the setup figure is exact rather than a residue.  outcome is ok, timeout, or failed.  Records from
# before either column still read: a shorter record leaves the trailing keys unset.
FIELDS = ("step", "arch", "jdk", "split", "splits", "timeout_hours", "duration_ms", "test_seconds", "outcome")

# Enough of the worst cell's longest suites to show whether one class dominates or the whole split is heavy,
# which is the distinction that decides what to do.  More when over budget, that being the one to act on.
LONGEST_SUITES = 6
LONGEST_SUITES_OVER_BUDGET = 12

# The floor keeps it quiet for cells with no setup worth reporting
UNDER_RECORDED_TEST_SHARE = 0.25
UNDER_RECORDED_MINIMUM_MINUTES = 5.0

# Name the test classes only when the worst cell's longest reaches this
NAME_CLASSES_ABOVE_MINUTES = 5.0


def read_records(cell_times_dir):
    """Every cell record, as a list of dicts.  A malformed file is skipped rather than fatal."""
    records = []
    if not os.path.isdir(cell_times_dir):
        return records
    # The .suites files sit in this directory too, so only .tsv is a record.
    for name in sorted(n for n in os.listdir(cell_times_dir) if n.endswith(".tsv")):
        path = os.path.join(cell_times_dir, name)
        try:
            with open(path, encoding="utf-8") as handle:
                fields = handle.readline().rstrip("\n").split("\t")
            if not (len(FIELDS) - 2 <= len(fields) <= len(FIELDS)):
                continue
            record = dict(zip(FIELDS, fields))
            record["minutes"] = int(record["duration_ms"]) / 60000.0
            record["timed_out"] = record.get("outcome") == "timeout"
            record["failed"] = record.get("outcome") == "failed"

            record["test_minutes"] = (float(record["test_seconds"]) / 60.0
                                      if record.get("test_seconds") else None)
            record["deadline_minutes"] = float(record["timeout_hours"]) * 60.0
            record["label"] = f"{record['step']} jdk{record['jdk']} {record['split']}/{record['splits']}"
            record["suites"] = read_suites(path[: -len(".tsv")] + ".suites")
            records.append(record)
        except (OSError, ValueError):
            continue
    return records


def read_suites(path):
    """(suite name, minutes) for one cell, longest first, or [] when the file is absent."""
    try:
        with open(path, encoding="utf-8") as handle:
            fields = [line.rstrip("\n").partition("\t") for line in handle]
        return [(name, float(seconds) / 60.0) for seconds, _, name in fields if name]
    except (OSError, ValueError):
        return []


def summarise(records, budget_margin):
    """One row per target, worst first."""
    by_step = {}
    for record in records:
        by_step.setdefault(record["step"], []).append(record)

    rows = []
    for step, cells in by_step.items():
        durations = sorted(cell["minutes"] for cell in cells)
        worst = max(cells, key=lambda cell: cell["minutes"])
        median = statistics.median(durations)
        budget = max(cell["deadline_minutes"] for cell in cells) - budget_margin
        rows.append({
            "step": step,
            "cells": len(cells),
            "min": durations[0],
            "median": median,
            "max": durations[-1],
            # Guarded, because a target whose median is zero is a target that ran nothing.
            "ratio": durations[-1] / median if median > 0 else 0.0,
            "budget": budget,
            "over_budget": durations[-1] > budget,
            # Counted because neither duration is a plain measurement: a killed cell's is its deadline, and a
            # failed cell's stops wherever the failure came.
            "timed_out_cells": sum(1 for cell in cells if cell["timed_out"]),
            "failed_cells": sum(1 for cell in cells if cell["failed"]),
            "worst_timed_out": worst["timed_out"],
            "worst_label": worst["label"],
            "worst_suites": worst["suites"],
            # The cell's own total where it recorded one, and the sum of what it listed otherwise.
            "worst_test_minutes": (worst["test_minutes"] if worst["test_minutes"] is not None
                                   else sum(minutes for _, minutes in worst["suites"])),
            "worst_test_minutes_exact": worst["test_minutes"] is not None,
        })
    rows.sort(key=lambda row: -row["max"])
    return rows


def suite_times(target_output_dir):
    """(suite name, minutes) for every JUnit suite under one target, longest first."""
    suites = []
    for root, _, names in os.walk(target_output_dir):
        for name in names:
            if not name.endswith(".xml"):
                continue
            try:
                element = ElementTree.parse(os.path.join(root, name)).getroot()
            except (OSError, ElementTree.ParseError):
                continue
            # A file's root is <testsuite> from ant and <testsuites> from pytest, so both are searched.
            for suite in [element] if element.tag == "testsuite" else element.findall(".//testsuite"):
                try:
                    suites.append((suite.get("name") or "?", float(suite.get("time") or 0) / 60.0))
                except ValueError:
                    continue
    suites.sort(key=lambda pair: -pair[1])
    return suites


def print_report(rows, output_root, budget_margin):
    if not rows:
        print("No cell-times records were found, so there is no split balance to report.")
        return

    print()
    print("Split balance, by test target.  `worst` is one cell's whole duration, setup included, against a")
    print(f"`budget` of its deadline less {budget_margin} min.  Splitting balances the count of classes and"
          " not their")
    print("duration, so a high w/med is a split that drew slow ones.  Confirm against the previous build.")
    print()
    header = (f"{'target':34s} {'cells':>5s} {'min':>7s} {'median':>7s} {'worst':>7s}"
              f" {'w/med':>6s} {'budget':>7s}  {'':4s} worst cell")
    print(header)
    print("-" * len(header))
    for row in rows:
        flag = "KILL" if row["worst_timed_out"] else "OVER" if row["over_budget"] else ""
        print(f"{row['step']:34s} {row['cells']:5d} {row['min']:7.1f} {row['median']:7.1f}"
              f" {row['max']:7.1f} {row['ratio']:6.2f} {row['budget']:7.0f}  {flag:4s}"
              f" {row['worst_label']}{cell_outcome_note(row)}")

    if not any(row["over_budget"] for row in rows):
        print()
        print("Every target's worst cell is inside its budget.")

    killed = sum(row["timed_out_cells"] for row in rows)
    failed = sum(row["failed_cells"] for row in rows)
    if killed or failed:
        print()
        print(f"Cells killed at their deadline (KILL): {killed}.  Cells ended by a failure: {failed}.  A killed"
              " cell's duration is that deadline, so read it as a lower bound; a failed cell's stops where the"
              " failure came, so it can pull min and median down.")

    print()
    print("What each worst cell ran.  One class near the cell's whole duration cannot be split further;")
    print("several of comparable size can be.  Classes are named only where the longest reaches"
          f" {NAME_CLASSES_ABOVE_MINUTES:.0f} min,")
    print("below which none of them is the reason.")
    for row in rows:
        print()
        print_worst_cell(row, output_root)


def cell_outcome_note(row):
    """What to add after a worst cell's label, when it did not simply finish."""
    if row["worst_timed_out"]:
        return " (killed at its deadline)"
    other = row["timed_out_cells"] + row["failed_cells"]
    return f" (+{other} killed or failed)" if other else ""


def print_share(minutes, total, text):
    """One breakdown line: minutes, what share of the cell they are, and what spent them."""
    print(f"    {minutes:7.1f} min  {100 * minutes / total if total else 0:4.0f}%  {text}")


def print_worst_cell(row, output_root):
    """The longest classes of one target's worst cell, and the time that was not test time."""
    flag = "  OVER BUDGET" if row["over_budget"] else ""
    print(f"{row['worst_label']}: {row['max']:.1f} min against a budget of {row['budget']:.0f}{flag}")
    if row["worst_timed_out"]:
        print("    killed at its deadline, so this duration is a lower bound and the classes below are only"
              " the ones that finished")

    suites, total, tests = row["worst_suites"], row["max"], row["worst_test_minutes"]
    if not suites:
        fallback = suite_times(os.path.join(output_root, row["step"]))[:LONGEST_SUITES]
        if not fallback:
            print("    no per-cell suite record, and no JUnit XML for this target either")
            return
        print("    no per-cell suite record, so these are the longest across the whole target:")
        for name, minutes in fallback:
            print(f"    {minutes:7.1f} min  {name}")
        return

    longest = max(minutes for _, minutes in suites)
    if longest < NAME_CLASSES_ABOVE_MINUTES:
        # No class is large enough to be the reason, so the whole test time is one line
        print_share(tests, total, f"every test class in this cell, the longest of them {longest:.1f} min")
    else:
        shown = suites[:LONGEST_SUITES_OVER_BUDGET if row["over_budget"] else LONGEST_SUITES]
        for name, minutes in shown:
            print_share(minutes, total, name)
        other = tests - sum(minutes for _, minutes in shown)
        if other > 0.05:
            print_share(other, total, "every other test class in this cell")

    setup = total - tests
    if setup <= 0:
        return
    share = tests / total if total else 1.0
    if total >= UNDER_RECORDED_MINIMUM_MINUTES and share < UNDER_RECORDED_TEST_SHARE:
        print_share(setup, total, f"setup at most: only {tests:.1f} min of tests was recorded,"
                                  f" {100 * share:.0f}% of the cell,")
        print("                        so tests are missing from the JUnit XML and this is an upper bound")
    else:
        exact = "" if row["worst_test_minutes_exact"] else ", or a test class this cell did not record"
        print_share(setup, total, "not running tests: the node, the image pulls, the compile and the"
                                  f" virtualenv{exact}")


def html_report(rows, budget_margin):
    parts = ["<h2>Split balance</h2>"]
    if not rows:
        parts.append("<p>No cell-times records were found.</p>")
        return "".join(parts)
    parts.append(
        "<p><code>worst</code> is one cell's whole duration, setup included, which is what the cell deadline"
        f" governs; <code>budget</code> is that deadline less {budget_margin} minutes of margin."
        " Round-robin splitting balances the number of classes in a split and not their duration, so a high"
        " <code>worst/median</code> is a split that drew slow classes. Confirm against the previous build"
        " before changing a split count.</p>")
    killed = sum(row["timed_out_cells"] for row in rows)
    failed = sum(row["failed_cells"] for row in rows)
    if killed or failed:
        parts.append(
            f"<p>Cells killed at their deadline: {killed}. Cells ended by a failure: {failed}. A killed cell's"
            " duration is that deadline, so read it as a lower bound; a failed cell's stops where the failure"
            " came, so it can pull <code>min</code> and <code>median</code> down.</p>")
    parts.append("<table border='1' cellpadding='4'><tr><th>target</th><th>cells</th><th>min</th>"
                 "<th>median</th><th>worst</th><th>worst/median</th><th>budget</th><th>worst cell</th></tr>")
    for row in rows:
        style = " bgcolor='#ffdddd'" if row["over_budget"] else ""
        parts.append(
            f"<tr{style}><td>{row['step']}</td><td>{row['cells']}</td><td>{row['min']:.1f}</td>"
            f"<td>{row['median']:.1f}</td><td>{row['max']:.1f}</td><td>{row['ratio']:.2f}</td>"
            f"<td>{row['budget']:.0f}</td><td>{row['worst_label']}{cell_outcome_note(row)}</td></tr>")
    parts.append("</table>")
    return "".join(parts)


def write_into_body(path, html):
    """Put the table inside the document's body, ci_parser.py having already closed it.

    Insert before the last `</body>` instead, and append only when the file has no body to insert into.
    """
    with open(path, encoding="utf-8") as handle:
        document = handle.read()
    head, tag, tail = document.rpartition("</body>")
    updated = head + html + tag + tail if tag else document + html
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(updated)


def main():
    parser = argparse.ArgumentParser(description="Report how evenly each test target's splits divided.")
    parser.add_argument("--input", default="build/test",
                       help="directory holding cell-times/ and output/, default build/test")
    parser.add_argument("--output", default=None,
                       help="existing .html file to insert the table into; omitted prints only")
    parser.add_argument("--budget-margin", type=int, default=15,
                       help="minutes of margin to keep under a cell's deadline, default 15")
    args = parser.parse_args()

    records = read_records(os.path.join(args.input, "cell-times"))
    rows = summarise(records, args.budget_margin)
    print_report(rows, os.path.join(args.input, "output"), args.budget_margin)

    if args.output and os.path.isfile(args.output):
        try:
            write_into_body(args.output, html_report(rows, args.budget_margin))
        except OSError as error:
            print(f"could not write the table into {args.output}: {error}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
