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
In-memory representations of JUnit test results and some helper methods to construct .html output
based on those results
"""

import logging
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from enum import Enum
from jinja2 import Template
from typing import Any, Dict, Iterable, List, Set, Tuple

LOG_FILE_NAME = 'junit_parsing'
logger = logging.getLogger(LOG_FILE_NAME)


class JUnitTestStatus(Enum):
    label: str

    """
    Map to the string tag expected in the child element in junit output
    """
    UNKNOWN = (0, 'unknown')
    PASSED = (1, 'passed')
    FAILURE = (2, 'failure')
    SKIPPED = (3, 'skipped')
    # Error and FAILURE are unfortunately used interchangeably by some of our suites, so we combine them on parsing
    ERROR = (4, 'error')

    def __new__(cls, value, label) -> Any:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.label = label
        return obj

    def __str__(self):
        return self.label

    @staticmethod
    def html_cell_order() -> Tuple:
        """
        We won't have UNKNOWN, and ERROR is merged into FAILURE. This is our preferred order to represent things in html.
        """
        return JUnitTestStatus.FAILURE, JUnitTestStatus.PASSED, JUnitTestStatus.SKIPPED


class JUnitResultBuilder:
    """
    Wraps up jinja templating for our junit based results. Manually doing this stuff was proving to be a total headache.
    That said, this didn't turn out to be a picnic on its own either. The particularity of .html parsing and this
    templating combined with BeautifulSoup means things are... very very particular. Bad parsing on things from the .sh
    or other sources can make bs4 replace things in weird ways.
    """
    def __init__(self, name: str, failures: int) -> None:
        self._name = name
        self._failures = failures
        self._labels = []  # type: List[str]
        self._rows = []  # type: List[List[str]]
        self._header = ['unknown', 'unknown', 'unknown', 'unknown']

        # Have to have the 4 <th> members since the stylesheet formats them based on position and it'll get all stupid
        # otherwise.
        self._template = Template('''
        <table class="table-fixed">
          <tr style = "height: 18px; background-color: black; color: white;">
            <th> {{header}} </th>
            <th></th>
            <th></th>
            <th></th>
          </tr>
          <tr>
            <th>{{ labels[0] }}</td>
            <th>{{ labels[1] }}</td>
            <th>{{ labels[2] }}</td>
            <th>{{ labels[3] }}</td>
          </tr>
          {% for row in rows %}
          <tr>
            <td>{{ row[0] }}</td>
            <td>{{ row[1] }}</td>
            <td>{{ row[2] }}</td>
            <td>{{ row[3] }}</td>
          </tr>
          {% endfor %}
        </table>
        ''')

    @staticmethod
    def add_style_tags(soup: BeautifulSoup) -> None:
        """
        We want to be opinionated about the width of our tables for our test suites; the messages dominate the output
        so we want to dedicate the largest amount of space to them and limit word-wrapping
        """
        style_tag = soup.new_tag("style")
        style_tag.string = """
        .table-fixed {
            table-layout: fixed;
            width: 100%;
        }
        .table-fixed th:nth-child(1), .table-fixed td:nth-child(1),
        .table-fixed th:nth-child(2), .table-fixed td:nth-child(2) {
            width: 15%;
            text-align: left;
            word-break: break-all;
        }
        .table-fixed th:nth-child(3), .table-fixed td:nth-child(3) {
            width: 60%;
            text-align: left;
        }
        .table-fixed th:nth-child(4), .table-fixed td:nth-child(4) {
            width: 10%;
            text-align: right;
        }
        """
        soup.head.append(style_tag)

    def label_columns(self, cols: List[str]) -> None:
        if len(cols) != 4:
            raise AssertionError(f'Got invalid number of columns on label_columns: {len(cols)}. Expected: 4.')
        self._labels = cols

    def add_row(self, row: List[str]) -> None:
        if len(row) != 4:
            raise AssertionError(f'Got invalid number of columns on add_row: {len(row)}. Expected: 4.')
        self._rows.append(row)

    def build_table(self) -> str:
        return self._template.render(header=f'{self._name} failures: {self._failures}', labels=self._labels, rows=self._rows)


class JUnitTestCase:
    """
    Pretty straightforward in-memory representation of the state of a jUnit test. Not the _most_ tolerant of bad input,
    so don't test your luck.
    """
    def __init__(self, testcase: ET.Element) -> None:
        """
        From a given xml element, constructs a junit testcase. Doesn't do any sanity checking to make sure you gave
        it something correct, so... don't screw up.

        Here's our general junit formatting:

        <testcase classname = "org.apache.cassandra.index.sai.cql.VectorUpdateDeleteTest" name = "updateTest-_jdk11" time = "0.314">
            <failure message = "Result set does not contain a row with pk = 0" type = "junit.framework.AssertionFailedError">
            # The following is stored in failure.text:
            DETAILED ERROR MESSAGE / STACK TRACE
            DETAILED ERROR MESSAGE / STACK TRACE
            ...
            </failure>
        </testcase>

        And our skipped format:
        <testcase classname="org.apache.cassandra.distributed.test.PreviewRepairCoordinatorFastTest" name="snapshotFailure[PARALLEL/true]-_jdk11" time="0.218">
          <skipped message="Parallel repair does not perform snapshots" />
        </testcase>

        Same for errors

        We conflate the <error and <failure sub-elements in our junit failures and will need to combine those on processing.
        """
        if testcase is None:
            raise AssertionError('Got an empty testcase; cannot create JUnitTestCase from nothing.')
        # No point in including the whole long o.a.c. thing. At least for now. This could bite us later if we end up with
        # other pathed test cases but /shrug
        self._class_name = testcase.get('classname', '').replace('org.apache.cassandra.', '')  # type: str
        self._test_name = testcase.get('name', '')  # type: str
        self._message = 'Passed'  # type: str
        self._status = JUnitTestStatus.PASSED  # type: JUnitTestStatus
        self._time = testcase.get('time', '')  # type: str

        # For the current set of tests, we don't have > 1 child tag indicating something went wrong. So we check to ensure
        # that remains true and will assert out if we hit something unexpected.
        saw_error = 0

        def _check_for_child_element(failure_type: JUnitTestStatus) -> None:
            """
            The presence of any failure, error, or skipped child elements indicated this test wasn't a normal 'pass'.
            We want to extract the message from the child if it has one as well as update the status of this object,
            including glomming together ERROR and FAILURE cases here. We combine those two as some legit test failures
            are reported as <error /> in the pytest cases.
            """
            nonlocal testcase
            child = testcase.find(failure_type.label)
            if child is None:
                return

            nonlocal saw_error
            if saw_error != 0:
                raise AssertionError(f'Got a test with > 1 "bad" state (error, failed, skipped). classname: {self._class_name}. test: {self._test_name}.')
            saw_error = 1

            # We don't know if we're going to have message attribute data, text attribute, or text inside our tag. So
            # we just connect all three
            final_msg = '-'.join(filter(None, (child.get('message'), child.get('text'), child.text)))

            self._message = final_msg
            if failure_type == JUnitTestStatus.ERROR or failure_type == JUnitTestStatus.FAILURE:
                self._status = JUnitTestStatus.FAILURE
            else:
                self._status = failure_type

        _check_for_child_element(JUnitTestStatus.FAILURE)
        _check_for_child_element(JUnitTestStatus.ERROR)
        _check_for_child_element(JUnitTestStatus.SKIPPED)

    @staticmethod
    def headers() -> List[str]:
        return ['Class', 'Method', 'Output', 'Duration']

    def row_data(self) -> List[str]:
        return [self._class_name, self._test_name, self._message, str(self._time)]

    def status(self) -> JUnitTestStatus:
        return self._status

    def message(self) -> str:
        return self._message

    def __hash__(self) -> int:
        """
        We want to allow overwriting of existing combinations of class + test names, since our tarballs of results will
        have us doing potentially duplicate sequential processing of files and we just want to keep the most recent one.
        Of note, sorting the tarball contents and trying to navigate to and find the oldest and only process that was
        _significantly_ slower than just brute-force overwriting this way. Like... I gave up after 10 minutes vs. < 1 second.
        """
        return hash((self._class_name, self._test_name))

    def __eq__(self, other) -> bool:
        if isinstance(other, JUnitTestCase):
            return (self._class_name, self._test_name) == (other._class_name, other._test_name)
        return NotImplemented

    def __str__(self) -> str:
        """
        We slice the message here; don't rely on this for anything where you need full reporting
        :return:
        """
        clean_msg = self._message.replace('\n', ' ')
        return (f"JUnitTestCase(class_name='{self._class_name}', "
                f"name='{self._test_name}', msg='{clean_msg[:50]}', "
                f"time={self._time}, status={self._status.name})")


class JUnitTestSuite:
    """
    Straightforward container for a set of tests.
    """

    def __init__(self, name: str):
        self._name = name  # type: str
        self._suites = dict()  # type: Dict[JUnitTestStatus, Set[JUnitTestCase]]
        self._files = set()  # type: Set[str]
        # We only allow one archive to be associated with each JUnitTestSuite
        self._archive = 'unknown'  # type: str
        for status in JUnitTestStatus:
            self._suites[status] = set()

    def add_testcase(self, newcase: JUnitTestCase) -> None:
        """
        Replaces if existing is found.
        """
        if newcase.status() == JUnitTestStatus.UNKNOWN:
            raise AssertionError(f'Attempted to add a testcase with an unknown status: {newcase}. Aborting.')
        self._suites[newcase.status()].discard(newcase)
        self._suites[newcase.status()].add(newcase)

    def get_tests(self, status: JUnitTestStatus) -> Iterable[JUnitTestCase]:
        """
        Returns sorted list of tests, class name first then test name
        """
        return sorted(self._suites[status], key=lambda x: (x._class_name, x._test_name))

    def passed(self) -> int:
        return self.count(JUnitTestStatus.PASSED)

    def failed(self) -> int:
        return self.count(JUnitTestStatus.FAILURE)

    def skipped(self) -> int:
        return self.count(JUnitTestStatus.SKIPPED)

    def count(self, status: JUnitTestStatus) -> int:
        return len(self._suites[status])

    def name(self) -> str:
        return self._name

    def is_empty(self) -> bool:
        return self.passed() == 0 and self.failed() == 0 and self.skipped() == 0

    def set_archive(self, name: str) -> None:
        if self._archive != "unknown":
            msg = f'Attempted to set archive for suite: {self._name} when archive already set: {self._archive}. This is a bug.'
            logger.critical(msg)
            raise AssertionError(msg)
        self._archive = name

    def get_archive(self) -> str:
        return self._archive

    def add_file(self, name: str) -> None:
        # Just silently noop if we already have one since dupes in tarball indicate the same thing effectively. That we have it.
        self._files.add(name)

    def file_count(self) -> int:
        """
        Returns count of _unique_ files associated with this suite, not necessarily the _absolute_ count of files, since
        we don't bother keeping count of multiple instances of a .xml file in the tarball.
        :return:
        """
        return len(self._files)

    @staticmethod
    def headers() -> List[str]:
        result = ['Suite']
        for status in JUnitTestStatus.html_cell_order():
            result.append(status.name)
        return result
