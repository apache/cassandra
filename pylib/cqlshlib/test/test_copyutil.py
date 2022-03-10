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

# to configure behavior, define $CQL_TEST_HOST to the destination address
# and $CQL_TEST_PORT to the associated port.


import unittest

from cassandra.metadata import MIN_LONG, Murmur3Token
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host
from unittest.mock import Mock

from cqlshlib.copyutil import ExportTask


class CopyTaskTest(unittest.TestCase):

    def setUp(self):
        # set up default test data
        self.ks = 'testks'
        self.table = 'testtable'
        self.columns = ['a', 'b']
        self.fname = 'test_fname'
        self.opts = {}
        self.protocol_version = 0
        self.config_file = 'test_config'
        self.hosts = [
            Host('10.0.0.1', SimpleConvictionPolicy, 9000),
            Host('10.0.0.2', SimpleConvictionPolicy, 9000),
            Host('10.0.0.3', SimpleConvictionPolicy, 9000),
            Host('10.0.0.4', SimpleConvictionPolicy, 9000)
    ]

    def mock_shell(self):
        """
        Set up a mock Shell so we can unit test ExportTask internals
        """
        shell = Mock()
        shell.conn = Mock()
        shell.conn.get_control_connection_host.return_value = self.hosts[0]
        shell.get_column_names.return_value = self.columns
        shell.debug = False
        return shell


class TestExportTask(CopyTaskTest):

    def _test_get_ranges_murmur3_base(self, opts, expected_ranges):
        """
        Set up a mock shell with a simple token map to test the ExportTask get_ranges function.
        """
        shell = self.mock_shell()
        shell.conn.metadata.partitioner = 'Murmur3Partitioner'
        # token range for a cluster of 4 nodes with replication factor 3
        shell.get_ring.return_value = {
            Murmur3Token(-9223372036854775808): self.hosts[0:3],
            Murmur3Token(-4611686018427387904): self.hosts[1:4],
            Murmur3Token(0): [self.hosts[2], self.hosts[3], self.hosts[0]],
            Murmur3Token(4611686018427387904): [self.hosts[3], self.hosts[0], self.hosts[1]]
        }
        # merge override options with standard options
        overridden_opts = dict(self.opts)
        for k, v in opts.items():
            overridden_opts[k] = v
        export_task = ExportTask(shell, self.ks, self.table, self.columns, self.fname, overridden_opts, self.protocol_version, self.config_file)
        assert export_task.get_ranges() == expected_ranges
        export_task.close()

    def test_get_ranges_murmur3(self):
        """
        Test behavior of ExportTask internal get_ranges function
        """

        # return empty dict and print error if begin_token < min_token
        self._test_get_ranges_murmur3_base({'begintoken': MIN_LONG - 1}, {})

        # return empty dict and print error if begin_token < min_token
        self._test_get_ranges_murmur3_base({'begintoken': 1, 'endtoken': -1}, {})

        # simple case of a single range
        expected_ranges = {(1, 2): {'hosts': ('10.0.0.4', '10.0.0.1', '10.0.0.2'), 'attempts': 0, 'rows': 0, 'workerno': -1}}
        self._test_get_ranges_murmur3_base({'begintoken': 1, 'endtoken': 2}, expected_ranges)

        # simple case of two contiguous ranges
        expected_ranges = {
            (-4611686018427387903, 0): {'hosts': ('10.0.0.3', '10.0.0.4', '10.0.0.1'), 'attempts': 0, 'rows': 0, 'workerno': -1},
            (0, 1): {'hosts': ('10.0.0.4', '10.0.0.1', '10.0.0.2'), 'attempts': 0, 'rows': 0, 'workerno': -1}
        }
        self._test_get_ranges_murmur3_base({'begintoken': -4611686018427387903, 'endtoken': 1}, expected_ranges)

        # specify a begintoken only (endtoken defaults to None)
        expected_ranges = {
            (4611686018427387905, None): {'hosts': ('10.0.0.1', '10.0.0.2', '10.0.0.3'), 'attempts': 0, 'rows': 0, 'workerno': -1}
        }
        self._test_get_ranges_murmur3_base({'begintoken': 4611686018427387905}, expected_ranges)

        # specify an endtoken only (begintoken defaults to None)
        expected_ranges = {
            (None, MIN_LONG + 1): {'hosts': ('10.0.0.2', '10.0.0.3', '10.0.0.4'), 'attempts': 0, 'rows': 0, 'workerno': -1}
        }
        self._test_get_ranges_murmur3_base({'endtoken': MIN_LONG + 1}, expected_ranges)
