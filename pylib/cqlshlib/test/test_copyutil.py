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
import sys
import unittest

from cassandra.metadata import MIN_LONG, Murmur3Token
from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host
from unittest.mock import Mock

from cqlshlib.copyutil import ExportProcess, ExportTask, ImportConversion


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
    Tests that COPY TO followed by COPY FROM is a lossless round-trip for
    string values that contain backslashes.
    """

    # Default COPY dialect (mirrors copyutil.py parse_options defaults)
    DEFAULT_DIALECT = dict(quotechar='"', escapechar='\\', doublequote=False)

    def _make_export_process(self, dialect=None):
        """
        Build a minimal ExportProcess without spawning a child process.
        Only the attributes consumed by write_rows_to_csv/format_value are set.
        """
        proc = object.__new__(ExportProcess)
        proc.formatters = {}
        proc.float_precision = 5
        proc.double_precision = 12
        proc.nullval = ''
        proc.encoding = 'utf-8'
        proc.date_time_format = Mock()
        proc.decimal_sep = '.'
        proc.thousands_sep = ''
        proc.boolean_styles = ('True', 'False')
        proc.options = Mock()
        proc.options.dialect = dialect or self.DEFAULT_DIALECT
        proc.report_error = Mock()
        return proc

    def _text_cqltype(self):
        """
        Minimal mock CQL text type sufficient for format_value.
        formatter=None causes get_formatter to fall back to type(val).__name__
        lookup, which resolves to format_value_text for str values.
        """
        return Mock(formatter=None, type_name='text')

    def _export(self, proc, val):
        """
        Call the real write_rows_to_csv, capture and return the CSV content string.
        self.send() normally writes to a multiprocessing pipe, we intercept it here.
        """
        captured = []
        proc.send = lambda data: captured.append(data)
        proc.write_rows_to_csv(token_range=None, rows=[[val]], cql_types=[self._text_cqltype()])
        self.assertFalse(proc.report_error.called, 'write_rows_to_csv raised an error')
        csv_content, _ = captured[0][1]
        return csv_content

    def _import(self, csv_content, dialect=None):
        """
        Read back a text column value using the real import-side functions:
        csv.reader -> ImportConversion.unprotect -> convert_text
        """
        rows = list(csv.reader(io.StringIO(csv_content), **(dialect or self.DEFAULT_DIALECT)))
        raw = rows[0][0]
        return str(ImportConversion.unprotect(raw))  # mirrors ImportConversion._get_converter

    def _roundtrip(self, original, dialect=None):
        """Full COPY TO -> CSV file -> COPY FROM pipeline using real functions."""
        proc = self._make_export_process(dialect)
        csv_content = self._export(proc, original)
        return self._import(csv_content, dialect)

    def test_no_backslash(self):
        self.assertEqual('hello world', self._roundtrip('hello world'))

    def test_single_backslash(self):
        self.assertEqual('a\\b', self._roundtrip('a\\b'))

    def test_url_with_backslashes(self):
        original = 'https:\\/\\/apache.org'
        self.assertEqual(original, self._roundtrip(original))

    def test_multiple_consecutive_backslashes(self):
        self.assertEqual('a\\\\b', self._roundtrip('a\\\\b'))

    def test_backslash_at_start(self):
        self.assertEqual('\\hello', self._roundtrip('\\hello'))

    def test_backslash_at_end(self):
        self.assertEqual('hello\\', self._roundtrip('hello\\'))

    def test_only_backslashes(self):
        self.assertEqual('\\\\\\', self._roundtrip('\\\\\\'))

    def test_empty_string(self):
        self.assertEqual('', self._roundtrip(''))

    def test_backslash_before_delimiter(self):
        """Backslash before comma: csv.writer must quote the field correctly."""
        self.assertEqual('a\\,b', self._roundtrip('a\\,b'))

    def test_backslash_before_quotechar(self):
        """Backslash immediately before the quotechar must not corrupt the field."""
        self.assertEqual('say \\"hi\\"', self._roundtrip('say \\"hi\\"'))

    def test_roundtrip_is_idempotent(self):
        """Two consecutive COPY TO/FROM cycles must produce identical results."""
        original = 'https:\\/\\/example.com\\path'
        after_first = self._roundtrip(original)
        after_second = self._roundtrip(after_first)
        self.assertEqual(original, after_first,
                         'First round-trip changed the value')
        self.assertEqual(after_first, after_second,
                         'Second round-trip changed the value (non-idempotent)')
