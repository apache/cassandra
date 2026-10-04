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

import math
import unittest
from decimal import Decimal
from uuid import UUID

from cqlshlib.formatting import CqlType, DateTimeFormat, format_json_value


class TestFormatJsonValue(unittest.TestCase):
    """Tests that format_json_value returns native Python types for JSON output."""

    def setUp(self):
        self.enc = 'utf-8'
        self.dtf = DateTimeFormat()
        self.fp = 3

    def _fmt(self, val, type_str):
        cqltype = CqlType(type_str) if type_str else None
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

    # ---- json.dumps round-trip ----

    def test_json_roundtrip_integers(self):
        import json
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
        import json
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

    def test_null_serializes_as_json_null_not_string(self):
        import json
        row = {'val': self._fmt(None, 'bigint')}
        s = json.dumps(row)
        self.assertIn('null', s)
        self.assertNotIn('"null"', s)

    def test_int_serializes_without_quotes(self):
        import json
        row = {'val': self._fmt(14413, 'bigint')}
        s = json.dumps(row)
        self.assertIn('14413', s)
        self.assertNotIn('"14413"', s)


if __name__ == '__main__':
    unittest.main()
