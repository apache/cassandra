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


"""
Script to take an arbitrary root directory of subdirectories of junit output format and create a summary .html file
with their results.
"""

import argparse
import cProfile
import pstats
import os
import shutil
import xml.etree.ElementTree as ET
from typing import Callable, Dict, Tuple, Type
from pathlib import Path

from junit_helpers import JUnitResultBuilder, JUnitTestCase, JUnitTestSuite, JUnitTestStatus, LOG_FILE_NAME
from logging_helper import build_logger, mute_logging, CustomLogger

try:
    from bs4 import BeautifulSoup
except ImportError:
    print('bs4 not installed; make sure you have bs4 in your active python env.')
    exit(1)


parser = argparse.ArgumentParser(description="""
Parses ci results provided ci output in input path and generates html
results in specified output file. Expects an existing .html file to insert
results into; this file will be backed up into a <file_name>.bak file in its
local directory.
""")
parser.add_argument('--input', type=str, help='path to input files (recursive directory search for *.xml)')
# TODO: Change this paradigm to a full "input dir translates into output file", where output file includes some uuid
# We'll need full support for all job types, not just junit, which will also necessitate refactoring into some kind of
# TestResultParser, of which JUnit would be one type. But there's a clear pattern here we can extract. Thinking checkstyle.
parser.add_argument('--output', type=str, help='existing .html output file to append to')
parser.add_argument('--mute', action='store_true', help='mutes stdout and only logs to log file')
parser.add_argument('--profile', '-p', action='store_true', help='Enable perf profiling on operations')
parser.add_argument('-v', '-d', '--verbose', '--debug', dest='debug', action='store_true', help='verbose log output')
args = parser.parse_args()
if args.input is None or args.output is None:
    parser.print_help()
    exit(1)

logger = build_logger(LOG_FILE_NAME, args.debug)  # type: CustomLogger
if args.mute:
    mute_logging(logger)


def main():
    check_file_condition(lambda: os.path.exists(args.input), f'Cannot find {args.input}. Aborting.')

    xml_files = [str(file) for file in Path(args.input).rglob('*.xml')]
    check_file_condition(lambda: len(xml_files) != 0, f'Found 0 .xml files in path: {args.input}. Cannot proceed with .xml extraction.')
    logger.info(f'Found {len(xml_files)} xml files under: {args.input}')

    test_suites = process_xml_files(xml_files)

    for suite in test_suites.values():
        if suite.is_empty() and suite.file_count() == 0:
            logger.warning(f'Have an empty test_suite: {suite.name()} that had no .xml files associated with it. Did the jobs run correctly and produce junit files? Check {suite.get_archive()} for test run command result details.')
        elif suite.is_empty():
            logger.warning(f'Got an unexpected empty test_suite: {suite.name()} with no .xml file parsing associated with it. Check {LOG_FILE_NAME}.log when run with -v for details.')

    create_summary_file(test_suites, xml_files, args.output)


def process_xml_files(xml_files: str) -> Dict[str, JUnitTestSuite]:
    """
    For a given input input_dir, will find all .xml files in that tree, extract files from them preserving input_dir structure
    and parse out all found junit test results into the global test result containers.
    :param xml_files: all .xml files under args.input_dir
    """

    test_suites = dict()  # type: Dict[str, JUnitTestSuite]
    test_count = 0

    for file in xml_files:
        files, tests = process_xml_file(file, test_suites)
        test_count += tests

    logger.progress(f'Total junit file count: {len(xml_files)}')
    logger.progress(f'Total suite count: {len(test_suites.keys())}')
    logger.progress(f'Total test count: {test_count}')
    passed = 0
    failed = 0
    skipped = 0

    for suite in test_suites.values():
        passed += suite.passed()
        failed += suite.failed()
        if suite.failed() != 0:
            print_errors(suite)
        skipped += suite.skipped()

    logger.progress(f'-- Passed: {passed}')
    logger.progress(f'-- Failed: {failed}')
    logger.progress(f'-- Skipped: {skipped}')
    return test_suites


def process_xml_file(xml_file, test_suites: Dict[str, JUnitTestSuite]) -> Tuple[int, int]:
    """
    Pretty straightforward here - walk through and look for tests,
    parsing them out into our global JUnitTestCase Dicts as we find them

    No thread safety on target Dict -> relying on the "one .xml per suite" rule to keep things clean

    Can be called in context of executor thread.
    :return: Tuple[file count, test count]
    """

    # TODO: In extreme cases (python upgrade dtests), this could theoretically be a HUGE file we're materializing in memory. Consider .iterparse or tag sanitization using sed first.
    with open(xml_file, "rb") as xml_input:
        try:
            suite_name = "?"
            root = ET.parse(xml_input).getroot()  # type: ignore
            suite_name = str(root.get('name'))
            logger.progress(f'Processing archive: {xml_file} for test suite: {suite_name}')

            # And make sure we're not racing
            if suite_name in test_suites:
                logger.error(f'Got a duplicate suite_name - this will lead to race conditions. Suite: {suite_name}. xml file: {xml_file}. Skipping this file.')
                return 0, 0
            else:
                test_suites[suite_name] = JUnitTestSuite(suite_name)

            active_suite = test_suites[suite_name]
            # Store this for later logging if we have a failed job; help the user know where to look next.
            active_suite.set_archive(xml_file)
            test_file_count = 0
            test_count = 0
            fc = process_test_cases(active_suite, xml_file, root)
            if fc != 0:
                test_file_count += 1
                test_count += fc
        except (EOFError, ET.ParseError) as e:
            logger.error(f'Error on {xml_file}: {e}. Skipping; will be missing results for {suite_name}')
            return 0, 0
        except Exception as e:
            logger.critical(f'Got unexpected error while parsing {xml_file}: {e}. Aborting.')
            raise e
    return test_file_count, test_count


def print_errors(suite: JUnitTestSuite) -> None:
    logger.warning(f'\n[Printing {suite.failed()} tests from suite: {suite.name()}]')
    for testcase in suite.get_tests(JUnitTestStatus.FAILURE):
        logger.warning(f'{testcase}')


def process_test_cases(suite: JUnitTestSuite, file_name: str, root) -> int:
    """
    For a given input .xml, will extract all JUnitTestCase matching objects and store them in the global registry keyed off
    suite name.

    Can be called in context of executor thread.
    :param suite: The JUnitTestSuite object we're currently working with
    :param file_name: .xml file_name to check for tests. junit format.
    :param root: etree root for file_name
    :return : count of tests extracted from this file_name
    """
    xml_exclusions = ['logback', 'checkstyle']
    if any(x in file_name for x in xml_exclusions):
        return 0

    # Search inside entire hierarchy since sometimes it's at the root and sometimes one level down.
    test_count = len(root.findall('.//testcase'))
    if test_count == 0:
        logger.warning(f'Appear to be processing an .xml file without any junit tests in it: {file_name}. Update .xml exclusions to exclude this.')
        if args.debug:
            logger.info(ET.tostring(root))
        return 0

    suite.add_file(file_name)
    found = 0
    for testcase in root.iter('testcase'):
        processed = JUnitTestCase(testcase)
        suite.add_testcase(processed)
        found = 1
    if found == 0:
        logger.error(f'file: {file_name} has test_count: {test_count} but root.iter iterated across nothing!')
        logger.error(ET.tostring(root))
    return test_count


def create_summary_file(test_suites: Dict[str, JUnitTestSuite], xml_files, output: str) -> None:
    """
    Will create a table with all failed tests in it organized by sorted suite name.
    :param test_suites: Collection of JUnitTestSuite's parsed out pass/fail data
    :param output: Path to the .html we want to append to the <body> of
    """

    # if needed create a blank ci_summary.html
    if not os.path.exists(args.output):
        with open(args.output, "w") as ci_summary_html:
            ci_summary_html.write('<html><head><body><h1>CI Summary</h1></body></head></html>')

    with open(output, 'r') as file:
        soup = BeautifulSoup(file, 'html.parser')

    failures_list_tag = soup.new_tag("div")
    failures_list_tag.string = '<br/><br/>[Test Failures]<br/><br/>'
    failures_tag = soup.new_tag("div")
    failures_tag.string = '<br/><br/>[Test Failure Details]<br/><br/>'
    suites_tag = soup.new_tag("div")
    suites_tag.string = '<br/><br/><hr/>[Test Suite Details]<br/><br/>'
    suites_builder = JUnitResultBuilder('Suites')
    suites_builder.label_columns(['Suite', 'Passed', 'Failed', 'Skipped'], ["width: 70%; text-align: left;", "width: 10%; text-align: right", "width: 10%; text-align: right", "width: 10%; text-align: right"])

    JUnitResultBuilder.add_style_tags(soup)

    # We cut off at 200 failures; if you have > than that chances are you have a bad run and there's no point in
    # just continuing to pollute the summary file with it and blow past file size. Since the inlined failures are
    # a tool to be used in the attaching / review process and not primarily workflow and fixing.
    total_passed_count = 0
    total_skipped_count = 0
    total_failure_count = 0
    for suite_name in sorted(test_suites.keys()):
        suite = test_suites[suite_name]
        passed_count = suite.passed()
        skipped_count = suite.skipped()
        failure_count = suite.failed()

        suites_builder.add_row([suite_name, str(passed_count), str(failure_count), str(skipped_count)])

        if failure_count == 0:
            # Don't append anything to results in the happy path case.
            logger.debug(f'No failed tests in suite: {suite_name}')
        elif total_failure_count < 200:
            # Else independent table per suite.
            failures_builder = JUnitResultBuilder(suite_name)
            failures_builder.label_columns(['Class', 'Method', 'Output', 'Duration'], ["width: 15%; text-align: left;", "width: 15%; text-align: left;", "width: 60%; text-align: left;", "width: 10%; text-align: right;"])
            for test in suite.get_tests(JUnitTestStatus.FAILURE):
                failures_builder.add_row(test.row_data())
            failures_list_tag.append(BeautifulSoup(failures_builder.build_list(), 'html.parser'))
            failures_tag.append(BeautifulSoup(failures_builder.build_table(), 'html.parser'))
            total_failure_count += failure_count
            if total_failure_count > 200:
                logger.critical(f'Saw {total_failure_count} failures; greater than 200 threshold. Not appending further failure details to {output}.')
        total_passed_count += passed_count
        total_skipped_count += skipped_count

    # totals, manual html
    totals_tag = soup.new_tag("div")
    totals_tag.string = f"""[Totals]<br/><br/><table style="width:100px">
        <tr><td >Passed</td><td></td><td align="right"> {total_passed_count}</td></tr>
        <tr><td >Failed</td><td></td><td align="right"> {total_failure_count}</td></tr>
        <tr><td >Skipped</td><td></td><td align="right"> {total_skipped_count}</td></tr>
        <tr><td >Total</td><td>&nbsp;&nbsp;&nbsp;</td><td align="right"> {total_passed_count + total_failure_count + total_skipped_count}</td></tr>
        <tr><td >Files</td><td></td><td align="right"> {len(xml_files)}</td></tr>
        <tr><td >Suites</td><td></td><td align="right"> {len(test_suites.keys())}</td></tr></table><hr/>
        """

    soup.body.append(totals_tag)
    soup.body.append(failures_list_tag)
    soup.body.append(failures_tag)
    suites_tag.append(BeautifulSoup(suites_builder.build_table(), 'html.parser'))
    soup.body.append(suites_tag)

    # Only backup the output file if we've gotten this far
    shutil.copyfile(output, output + '.bak')

    # We write w/formatter set to None as invalid char above our insertion in the input file we're modifying (from other
    # tests, test output, etc) can cause the parser to get very confused and do Bad Things.
    with open(output, 'w') as file:
        file.write(soup.prettify(formatter=None))
    logger.progress(f'Test failure details appended to file: {output}')


def check_file_condition(function: Callable[[], bool], msg: str) -> None:
    """
    Specifically raises a FileNotFoundError if something's wrong with the Callable
    """
    if not function():
        log_and_raise(msg, FileNotFoundError)


def log_and_raise(msg: str, error_type: Type[BaseException]) -> None:
    logger.critical(msg)
    raise error_type(msg)


if __name__ == "__main__" and args.profile:
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats('cumulative')
    stats.print_stats()
else:
    main()
