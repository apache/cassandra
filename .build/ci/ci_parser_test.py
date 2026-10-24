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

import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from bs4 import BeautifulSoup


class TestSummaryTotals(unittest.TestCase):
    def report(self, failure_counts):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            for index, failures in enumerate(failure_counts):
                suite = ET.Element('testsuite', name=f'suite-{index}')
                for number in range(failures):
                    test = ET.SubElement(suite, 'testcase', classname='ExampleTest', name=f'failure-{number}', time='1')
                    ET.SubElement(test, 'failure', message='failure detail')
                ET.SubElement(suite, 'testcase', classname='ExampleTest', name='passed', time='1')
                skipped = ET.SubElement(suite, 'testcase', classname='ExampleTest', name='skipped', time='0')
                ET.SubElement(skipped, 'skipped')
                ET.ElementTree(suite).write(directory / f'suite-{index}.xml')
            output = directory / 'ci_summary.html'
            result = subprocess.run([sys.executable, str(Path(__file__).with_name('ci_parser.py').resolve()),
                                     '--input', str(directory), '--output', str(output), '--mute'],
                                    cwd=directory, capture_output=True, text=True, check=False)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return BeautifulSoup(output.read_text(), 'html.parser')

    def test_detail_limit_does_not_cap_totals(self):
        for first_suite_failures in (200, 216):
            with self.subTest(first_suite_failures=first_suite_failures):
                report = self.report((first_suite_failures, 528 - first_suite_failures))
                totals = report.find('table')
                counts = {cells[0].get_text(strip=True): int(cells[-1].get_text(strip=True))
                          for row in totals.find_all('tr') if (cells := row.find_all('td'))}
                self.assertEqual(counts['Failed'], 528)
                self.assertEqual(counts['Total'], 532)
                self.assertEqual(counts['Passed'], 2)
                self.assertEqual(counts['Skipped'], 2)
                self.assertEqual(totals.get('data-failure-count-capped'), 'false')
                tables = report.find_all('table')[1:]
                self.assertEqual([table.find('th').get_text(strip=True) for table in tables], ['suite-0', 'Suites'])


if __name__ == '__main__':
    unittest.main()
