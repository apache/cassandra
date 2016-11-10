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

import os
import sys
import logging
from itertools import izip
from os.path import dirname, join, normpath

cqlshlog = logging.getLogger('test_cqlsh')

try:
    # a backport of python2.7 unittest features, so we can test against older
    # pythons as necessary. python2.7 users who don't care about testing older
    # versions need not install.
    import unittest2 as unittest
except ImportError:
    import unittest

rundir = dirname(__file__)
cqlshdir = normpath(join(rundir, '..', '..', '..', 'bin'))
path_to_cqlsh = normpath(join(cqlshdir, 'cqlsh.py'))

sys.path.append(cqlshdir)

import cqlsh
cql = cqlsh.cassandra.cluster.Cluster
policy = cqlsh.cassandra.policies.RoundRobinPolicy()
quote_name = cqlsh.cassandra.metadata.maybe_escape_name

TEST_HOST = os.environ.get('CQL_TEST_HOST', '127.0.0.1')
TEST_PORT = int(os.environ.get('CQL_TEST_PORT', 9042))

class BaseTestCase(unittest.TestCase):
    def assertNicelyFormattedTableHeader(self, line, msg=None):
        return self.assertRegexpMatches(line, r'^ +\w+( +\| \w+)*\s*$', msg=msg)

    def assertNicelyFormattedTableRule(self, line, msg=None):
        return self.assertRegexpMatches(line, r'^-+(\+-+)*\s*$', msg=msg)

    def assertNicelyFormattedTableData(self, line, msg=None):
        return self.assertRegexpMatches(line, r'^ .* \| ', msg=msg)

def dedent(s):
    lines = [ln.rstrip() for ln in s.splitlines()]
    if lines[0] == '':
        lines = lines[1:]
    spaces = [len(line) - len(line.lstrip()) for line in lines if line]
    minspace = min(spaces if len(spaces) > 0 else (0,))
    return '\n'.join(line[minspace:] for line in lines)

def at_a_time(i, num):
    return izip(*([iter(i)] * num))
