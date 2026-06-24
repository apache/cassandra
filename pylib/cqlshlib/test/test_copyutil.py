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


import csv
import io
import unittest

from cassandra.metadata import MIN_LONG, Murmur3Token
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host
from unittest.mock import Mock

from cqlshlib.copyutil import ExportProcess, ExportTask, ImportConversion
from cqlshlib.displaying import NO_COLOR_MAP
from cqlshlib.formatting import CqlType, DateTimeFormat, format_value_text


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


class TestCopyBackslashRoundtrip(unittest.TestCase):
    """
    COPY TO followed by COPY FROM must be a lossless round-trip for text values -
    including text nested in collections - that contain backslashes.

    Regression test for CASSANDRA-21131. The corruption only manifests on Python
    3.10+, where csv.writer began escaping the escapechar itself (bpo-12178);
    before 3.10 csv.writer left bare backslashes alone, so the pre-doubling done by
    formatting.format_value_text was cancelled out by csv.reader and the round-trip
    was already lossless. These tests therefore assert the observable round-trip
    property rather than any intermediate escaping, so they hold on every supported
    Python (3.6-3.11).

    The round-trip test design follows the approach proposed by Howie Zhao
    (@howiezhao) in CASSANDRA-21349 / PR #4780.
    """

    # CSV dialect produced from the default COPY ESCAPE / QUOTE / DELIMITER options
    # (copyutil.parse_options). No explicit quoting is configured, so csv defaults
    # to QUOTE_MINIMAL.
    DIALECT = dict(quotechar='"', escapechar='\\', delimiter=',', doublequote=False)

    def _format_value(self, val, typestring):
        # Build an ExportProcess without running __init__ (which starts a
        # multiprocessing.Process and opens cluster connections); set only the
        # attributes that format_value reads.
        proc = ExportProcess.__new__(ExportProcess)
        proc.formatters = {}
        proc.encoding = 'utf-8'
        proc.date_time_format = DateTimeFormat()
        proc.float_precision = 5
        proc.double_precision = 12
        proc.nullval = ''
        proc.decimal_sep = '.'
        proc.thousands_sep = ''
        proc.boolean_styles = ['True', 'False']
        return proc.format_value(val, CqlType(typestring))

    def _csv_cell(self, formatted):
        # One COPY TO write (csv.writer) followed by one COPY FROM read
        # (csv.reader), exactly as cqlsh streams values through a CSV file.
        buf = io.StringIO()
        csv.writer(buf, **self.DIALECT).writerow([formatted])
        return next(csv.reader(io.StringIO(buf.getvalue()), **self.DIALECT))[0]

    def _roundtrip_scalar(self, original, typestring):
        # Full COPY TO -> CSV -> COPY FROM for a scalar value, including the
        # import-side unprotect step (mirrors ImportConversion._get_converter).
        cell = self._csv_cell(self._format_value(original, typestring))
        return str(ImportConversion.unprotect(cell))

    def test_scalar_text_roundtrip(self):
        values = ['plain', 'a\\b', 'C:\\tmp\\f', 'https:\\/\\/apache.org',
                  '\\lead', 'trail\\', '\\\\\\', '', 'a\\,b', '\\"Marianne"\\']
        for typestring in ('text', 'varchar', 'ascii'):
            for original in values:
                self.assertEqual(
                    original, self._roundtrip_scalar(original, typestring),
                    'round-trip changed %r (%s)' % (original, typestring))

    def test_collection_text_roundtrip(self):
        # The type_name here is list/set/map/tuple, so the fix must reach the text
        # elements nested inside the collection, not just top-level scalars. After
        # the CSV layer the cell handed to COPY FROM's CQL parser must carry the
        # original (single) backslashes, never doubled ones.
        cases = [
            (['a\\b', 'c\\d'], 'list<text>', "['a\\b', 'c\\d']"),
            ({'x\\y'}, 'set<text>', "{'x\\y'}"),
            ({'k\\1': 'v\\2'}, 'map<text, text>', "{'k\\1': 'v\\2'}"),
            (('a\\b', 'c\\d'), 'tuple<text, text>', "('a\\b', 'c\\d')"),
        ]
        for val, typestring, expected_cell in cases:
            self.assertEqual(
                expected_cell, self._csv_cell(self._format_value(val, typestring)),
                'collection round-trip changed %r (%s)' % (val, typestring))

    def test_roundtrip_is_idempotent(self):
        original = 'https:\\/\\/example.com\\path'
        once = self._roundtrip_scalar(original, 'text')
        twice = self._roundtrip_scalar(once, 'text')
        self.assertEqual(original, once, 'first round-trip changed the value')
        self.assertEqual(once, twice, 'second round-trip changed the value (non-idempotent)')

    def test_display_path_still_escapes_backslashes(self):
        # The terminal-display path must keep doubling backslashes so SELECT output
        # renders them visibly; only the CSV export path (copyutil) opts out, on
        # Python 3.10+, by undoing the doubling before handing the value to csv.writer.
        self.assertEqual(
            'V\\\\S',
            format_value_text('V\\S', encoding='utf-8', colormap=NO_COLOR_MAP))
