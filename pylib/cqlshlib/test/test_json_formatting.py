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

"""Unit tests for format_json_value — no live Cassandra connection required."""

import datetime
import io
import json
import math
import unittest
from collections import namedtuple
from decimal import Decimal
from uuid import UUID

from cassandra.util import Date, Duration, SortedSet, Time

from cqlshlib.displaying import CsvTablePrinter, FormattedValue, JsonTablePrinter
from cqlshlib.formatting import CqlType, DateTimeFormat, format_json_value


class FakeUserTypeMeta:
    def __init__(self, field_types):
        self.field_types = field_types


class FakeKeyspaceMeta:
    user_types = {'address': FakeUserTypeMeta(['text', 'int'])}


Address = namedtuple('address', ['street', 'zip'])


class TestFormatJsonValue(unittest.TestCase):
    """Tests that format_json_value returns native Python types for JSON output."""

    def setUp(self):
        self.enc = 'utf-8'
        self.dtf = DateTimeFormat()
        self.fp = 3

    def _fmt(self, val, type_str):
        cqltype = CqlType(type_str, FakeKeyspaceMeta()) if type_str else None
        return format_json_value(val, cqltype, self.enc,
                                 date_time_format=self.dtf,
                                 float_precision=self.fp)

    # ---- null ----

    def test_none_is_json_null(self):
        self.assertIsNone(self._fmt(None, 'bigint'))
        self.assertIsNone(self._fmt(None, 'text'))
        self.assertIsNone(self._fmt(None, 'int'))
        self.assertIsNone(self._fmt(None, 'boolean'))

    # ---- integer types → int ----

    def test_bigint_is_int(self):
        result = self._fmt(14413, 'bigint')
        self.assertEqual(result, 14413)
        self.assertIsInstance(result, int)

    def test_int_is_int(self):
        result = self._fmt(42, 'int')
        self.assertEqual(result, 42)
        self.assertIsInstance(result, int)

    def test_varint_is_int(self):
        big = 10000000000000000000000000
        result = self._fmt(big, 'varint')
        self.assertEqual(result, big)
        self.assertIsInstance(result, int)

    def test_smallint_is_int(self):
        result = self._fmt(32767, 'smallint')
        self.assertIsInstance(result, int)

    def test_tinyint_is_int(self):
        result = self._fmt(127, 'tinyint')
        self.assertIsInstance(result, int)

    def test_negative_int(self):
        result = self._fmt(-12, 'int')
        self.assertEqual(result, -12)

    # ---- float types → float ----

    def test_float_is_float(self):
        result = self._fmt(1.5, 'float')
        self.assertIsInstance(result, float)

    def test_double_is_float(self):
        result = self._fmt(1.0, 'double')
        self.assertIsInstance(result, float)

    def test_nan_becomes_string(self):
        self.assertEqual(self._fmt(float('nan'), 'float'), 'NaN')

    def test_positive_inf_becomes_string(self):
        self.assertEqual(self._fmt(float('inf'), 'float'), 'Infinity')

    def test_negative_inf_becomes_string(self):
        self.assertEqual(self._fmt(float('-inf'), 'float'), '-Infinity')

    # ---- boolean → bool ----

    def test_true_is_bool(self):
        result = self._fmt(True, 'boolean')
        self.assertIs(result, True)
        self.assertIsInstance(result, bool)

    def test_false_is_bool(self):
        result = self._fmt(False, 'boolean')
        self.assertIs(result, False)

    # ---- text types → str ----

    def test_text_is_str(self):
        result = self._fmt('hello', 'text')
        self.assertEqual(result, 'hello')
        self.assertIsInstance(result, str)

    def test_ascii_is_str(self):
        result = self._fmt('abc', 'ascii')
        self.assertIsInstance(result, str)

    def test_text_with_double_quotes(self):
        result = self._fmt('say "hi"', 'text')
        self.assertIn('"', result)
        self.assertIsInstance(result, str)

    def test_text_no_cql_quoting(self):
        # Raw string must not be wrapped in single-quotes (CQL literal style)
        result = self._fmt('hello', 'text')
        self.assertFalse(result.startswith("'"))

    # ---- list → list ----

    def test_list_is_list(self):
        result = self._fmt(['a', 'b', 'c'], 'list<text>')
        self.assertIsInstance(result, list)
        self.assertEqual(result, ['a', 'b', 'c'])

    def test_list_elements_are_native(self):
        result = self._fmt([1, 2, 3], 'list<int>')
        for v in result:
            self.assertIsInstance(v, int)

    def test_empty_list(self):
        result = self._fmt([], 'list<text>')
        self.assertEqual(result, [])

    # ---- set → list ----

    def test_set_is_list(self):
        result = self._fmt({'a', 'b', 'c'}, 'set<text>')
        self.assertIsInstance(result, list)
        self.assertEqual(set(result), {'a', 'b', 'c'})

    def test_empty_set(self):
        result = self._fmt(set(), 'set<text>')
        self.assertEqual(result, [])

    # ---- map → dict ----

    def test_map_is_dict(self):
        result = self._fmt({'a': 1, 'b': 2}, 'map<text, int>')
        self.assertIsInstance(result, dict)
        self.assertEqual(result, {'a': 1, 'b': 2})

    def test_map_values_are_int(self):
        result = self._fmt({'x': 42}, 'map<text, int>')
        self.assertIsInstance(result['x'], int)

    def test_map_int_keys_become_strings(self):
        result = self._fmt({4: 3, 5: 2}, 'map<int, int>')
        self.assertIsInstance(result, dict)
        self.assertIn('4', result)
        self.assertEqual(result['4'], 3)
        self.assertIn('5', result)
        self.assertEqual(result['5'], 2)

    def test_empty_map(self):
        result = self._fmt({}, 'map<text, int>')
        self.assertEqual(result, {})

    def test_map_null_value(self):
        result = self._fmt({'k': None}, 'map<text, int>')
        self.assertIsNone(result['k'])

    # ---- fallback types → str ----

    def test_uuid_is_string(self):
        u = UUID('bd1924e1-6af8-44ae-b5e1-f24131dbd460')
        result = self._fmt(u, 'uuid')
        self.assertIsInstance(result, str)
        self.assertRegex(result,
                         r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

    def test_decimal_is_string(self):
        d = Decimal('19952.11882')
        result = self._fmt(d, 'decimal')
        self.assertIsInstance(result, str)
        self.assertEqual(Decimal(result), d)

    def test_types_without_json_form_are_strings(self):
        self.assertEqual(self._fmt(Duration(1, 2, 3), 'duration'), '1mo2d3ns')
        self.assertEqual(self._fmt(Date(19000), 'date'), '2022-01-08')
        self.assertEqual(self._fmt(Time(3723000000000), 'time'), '01:02:03.000000000')
        self.assertEqual(self._fmt(datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc), 'timestamp'),
                         '2024-01-02 03:04:05.000000+0000')
        self.assertEqual(self._fmt('10.0.0.1', 'inet'), '10.0.0.1')
        self.assertEqual(self._fmt(b'\xca\xfe', 'blob'), '0xcafe')

    # ---- user types, tuples, vectors and nested collections ----

    def test_counter_is_int(self):
        self.assertEqual(self._fmt(5, 'counter'), 5)

    def test_udt_is_dict(self):
        self.assertEqual(self._fmt(Address('Main St', 12345), 'frozen<address>'),
                         {'street': 'Main St', 'zip': 12345})

    def test_udt_null_field(self):
        self.assertEqual(self._fmt(Address('Main St', None), 'frozen<address>'),
                         {'street': 'Main St', 'zip': None})

    def test_udt_in_list(self):
        self.assertEqual(self._fmt([Address('Main St', 1)], 'list<frozen<address>>'),
                         [{'street': 'Main St', 'zip': 1}])

    def test_tuple_is_list(self):
        self.assertEqual(self._fmt((1, 'a', None), 'tuple<int, text, int>'), [1, 'a', None])

    def test_vector_is_list(self):
        self.assertEqual(self._fmt([1.0, 2.5, 3.0], 'vector<float, 3>'), [1.0, 2.5, 3.0])

    def test_set_of_tuples(self):
        self.assertEqual(self._fmt(SortedSet([(1, 'a')]), 'set<frozen<tuple<int, text>>>'), [[1, 'a']])

    def test_map_of_lists(self):
        self.assertEqual(self._fmt({'k': [1, 2]}, 'map<text, frozen<list<int>>>'), {'k': [1, 2]})

    def test_nested_collections(self):
        self.assertEqual(self._fmt([{'a': SortedSet([1, 2])}], 'frozen<list<frozen<map<text, frozen<set<int>>>>>>'),
                         [{'a': [1, 2]}])

    def test_map_non_string_keys_are_json_text(self):
        self.assertEqual(self._fmt({True: 1}, 'map<boolean, int>'), {'true': 1})
        self.assertEqual(self._fmt({(1, 2): 'x'}, 'map<frozen<tuple<int, int>>, text>'), {'[1, 2]': 'x'})
        self.assertEqual(self._fmt({Address('Main St', 1): 1}, 'map<frozen<address>, int>'),
                         {'{"street": "Main St", "zip": 1}': 1})

    # ---- json.dumps round-trip ----

    def test_json_roundtrip_integers(self):
        row = {
            'bigintcol': self._fmt(14413, 'bigint'),
            'intcol': self._fmt(-12, 'int'),
            'varintcol': self._fmt(10000000000000000000000000, 'varint'),
        }
        s = json.dumps(row)
        parsed = json.loads(s)
        self.assertEqual(parsed['bigintcol'], 14413)
        self.assertEqual(parsed['intcol'], -12)
        self.assertEqual(parsed['varintcol'], 10000000000000000000000000)

    def test_json_roundtrip_collections(self):
        row = {
            'listcol': self._fmt(['a', 'b'], 'list<text>'),
            'mapcol': self._fmt({'k': 1}, 'map<text, int>'),
            'nullcol': self._fmt(None, 'bigint'),
        }
        s = json.dumps(row)
        parsed = json.loads(s)
        self.assertEqual(parsed['listcol'], ['a', 'b'])
        self.assertEqual(parsed['mapcol'], {'k': 1})
        self.assertIsNone(parsed['nullcol'])


class FakeShell:
    def __init__(self):
        self.query_out = io.StringIO()

    def writeresult(self, text, newline=True):
        self.query_out.write(text + ('\n' if newline else ''))


class TestMachineReadablePrinters(unittest.TestCase):
    """Tests for the json and csv table printers, without a live Cassandra connection."""

    names = [FormattedValue('a'), FormattedValue('b')]

    def test_json_finish_without_rows(self):
        shell = FakeShell()
        printer = JsonTablePrinter(shell)
        printer.finish()
        self.assertEqual('[\n]\n', shell.query_out.getvalue())
        self.assertEqual([], json.loads(shell.query_out.getvalue()))

    def test_json_empty_result(self):
        shell = FakeShell()
        printer = JsonTablePrinter(shell)
        printer.print_header(self.names)
        printer.print_rows(self.names, None)
        printer.finish()
        self.assertEqual('[\n]\n', shell.query_out.getvalue())

    def test_json_rows_across_pages(self):
        shell = FakeShell()
        printer = JsonTablePrinter(shell)
        printer.print_header(self.names)
        printer.print_rows(self.names, [[1, 'x']])
        printer.print_rows(self.names, [[2, None]])
        printer.finish()
        self.assertEqual('[\n  {"a": 1, "b": "x"},\n  {"a": 2, "b": null}\n]\n', shell.query_out.getvalue())

    def test_csv_empty_result_prints_header(self):
        shell = FakeShell()
        printer = CsvTablePrinter(shell)
        printer.print_header(self.names)
        printer.print_rows(self.names, None)
        printer.finish()
        self.assertEqual('a,b\r\n', shell.query_out.getvalue())

    def test_csv_header_written_once(self):
        shell = FakeShell()
        printer = CsvTablePrinter(shell)
        printer.print_header(self.names)
        printer.print_rows(self.names, [['1', 'x']])
        printer.print_rows(self.names, [['2', '']])
        printer.finish()
        self.assertEqual('a,b\r\n1,x\r\n2,\r\n', shell.query_out.getvalue())


if __name__ == '__main__':
    unittest.main()
