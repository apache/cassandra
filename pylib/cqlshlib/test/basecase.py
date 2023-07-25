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

import logging
import os
from os.path import dirname, join, normpath
import sys
import unittest

cqlshlog = logging.getLogger('test_cqlsh')

test_dir = dirname(__file__)
cassandra_dir = normpath(join(test_dir, '..', '..', '..'))
cqlsh_dir = join(cassandra_dir, 'bin')
path_to_cqlsh = join(cqlsh_dir, 'cqlsh.py')

sys.path.append(cqlsh_dir)

TEST_HOST = os.environ.get('CQL_TEST_HOST', '127.0.0.1')
TEST_PORT = int(os.environ.get('CQL_TEST_PORT', 9042))
TEST_USER = os.environ.get('CQL_TEST_USER', 'cassandra')
TEST_PWD = os.environ.get('CQL_TEST_PWD', 'cassandra')


class BaseTestCase(unittest.TestCase):
    def assertNicelyFormattedTableHeader(self, line, msg=None):
        return self.assertRegex(line, r'^ +\w+( +\| \w+)*\s*$', msg=msg)

    def assertNicelyFormattedTableRule(self, line, msg=None):
        return self.assertRegex(line, r'^-+(\+-+)*\s*$', msg=msg)

    def assertNicelyFormattedTableData(self, line, msg=None):
        return self.assertRegex(line, r'^ .* \| ', msg=msg)

    def assertRegex(self, text, regex, msg=None):
        """Call assertRegexpMatches() if in Python 2"""
        if hasattr(unittest.TestCase, 'assertRegex'):
            return super().assertRegex(text, regex, msg)
        else:
            return self.assertRegexpMatches(text, regex, msg)

    def assertNotRegex(self, text, regex, msg=None):
        """Call assertNotRegexpMatches() if in Python 2"""
        if hasattr(unittest.TestCase, 'assertNotRegex'):
            return super().assertNotRegex(text, regex, msg)
        else:
            return self.assertNotRegexpMatches(text, regex, msg)


def dedent(s):
    lines = [ln.rstrip() for ln in s.splitlines()]
    if lines[0] == '':
        lines = lines[1:]
    spaces = [len(line) - len(line.lstrip()) for line in lines if line]
    minspace = min(spaces if len(spaces) > 0 else (0,))
    return '\n'.join(line[minspace:] for line in lines)


def at_a_time(i, num):
    return zip(*([iter(i)] * num))
