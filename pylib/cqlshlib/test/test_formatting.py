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

import unittest
from collections import OrderedDict

from cqlshlib.displaying import NO_COLOR_MAP
from cqlshlib.formatting import (
    format_value_text,
    format_value_list,
    format_value_set,
    format_value_tuple,
    format_value_map,
    format_value_utype,
    CqlType
)


class _MockUDT:
    """ Mimics the driver's UDT shape (exposes _asdict()) without the
        identifier restrictions Python's namedtuple imposes on field names. """
    def __init__(self, items):
        self._items = items

    def _asdict(self):
        return OrderedDict(self._items)


class TestFormatting(unittest.TestCase):

    def setUp(self):
        self.fmt_kwargs = {
            'encoding': 'utf-8',
            'colormap': NO_COLOR_MAP,
            'date_time_format': None,
            'float_precision': 3,
            'nullval': 'null',
            'decimal_sep': '.',
            'thousands_sep': ',',
            'boolean_styles': None
        }

    def test_format_value_text_control_chars(self):
        """
        Test that control chars are escaped for terminal display (default),
        but preserved when escape_control_chars=False is passed (for CSV export).
        """
        self.assertEqual(
            format_value_text("Hello World", encoding='utf-8', colormap=NO_COLOR_MAP),
            "Hello World"
        )

        test_string = "Hello\nWorld\x00\tTest\r"

        terminal_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP)
        self.assertEqual(terminal_output, "Hello\\nWorld\\x00\\tTest\\r")

        csv_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP, escape_control_chars=False)
        self.assertEqual(csv_output, test_string)

    def test_format_value_list_control_chars(self):
        """ Test control character propagation in lists """
        list_val = ["line1\nline2", "null\x00byte"]
        cql_type = CqlType('list<text>')

        terminal_output = format_value_list(list_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "['line1\\nline2', 'null\\x00byte']")

        csv_output = format_value_list(list_val, cqltype=cql_type, escape_control_chars=False, **self.fmt_kwargs)
        self.assertEqual(csv_output, "['line1\nline2', 'null\x00byte']")

    def test_format_value_map_control_chars(self):
        """ Test control character propagation in map keys and values """
        map_val = {"key\n1": "val\x001"}
        cql_type = CqlType('map<text, text>')

        terminal_output = format_value_map(map_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "{'key\\n1': 'val\\x001'}")

        csv_output = format_value_map(map_val, cqltype=cql_type, escape_control_chars=False, **self.fmt_kwargs)
        self.assertEqual(csv_output, "{'key\n1': 'val\x001'}")

    def test_udt_field_name_and_value_control_chars(self):
        """ Test control character propagation in UDT field names and values """
        # The driver exposes UDT instances via an _asdict() shape; namedtuple
        # cannot be used here because UDT field names may contain characters
        # (e.g. '\n') that are not valid Python identifiers.
        udt_val = _MockUDT([('field_a\n', 'val\n1'), ('field_b', 'val\x002')])

        cql_type = CqlType('text')
        cql_type.sub_types = [CqlType('text'), CqlType('text')]

        terminal_output = format_value_utype(udt_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "{field_a\\n: 'val\\n1', field_b: 'val\\x002'}")

        csv_output = format_value_utype(udt_val, cqltype=cql_type, escape_control_chars=False, **self.fmt_kwargs)
        self.assertEqual(csv_output, "{field_a\n: 'val\n1', field_b: 'val\x002'}")

    def test_format_value_text_empty_string(self):
        """ Empty strings pass through cleanly in both modes (no spurious
            characters introduced by the regex sub or the escape pipeline). """
        self.assertEqual(
            format_value_text("", encoding='utf-8', colormap=NO_COLOR_MAP),
            ""
        )
        self.assertEqual(
            format_value_text("", encoding='utf-8', colormap=NO_COLOR_MAP, escape_control_chars=False),
            ""
        )

    def test_format_value_text_latin1_and_del_control_chars(self):
        """ UNICODE_CONTROLCHARS_RE matches [\\x00-\\x1f\\x7f-\\xa0]: in addition
            to the common C0 controls, DEL (\\x7f), C1 controls (e.g. \\x80) and
            NBSP (\\xa0) must also be escaped on terminals and preserved for CSV. """
        test_string = "del\x7fmid\x80end\xa0nbsp"

        terminal_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP)
        self.assertEqual(terminal_output, "del\\x7fmid\\x80end\\xa0nbsp")

        csv_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP,
                                       escape_control_chars=False)
        self.assertEqual(csv_output, test_string)

    def test_format_value_text_consecutive_control_chars(self):
        """ A run of adjacent control chars must be escaped/preserved
            character-by-character, not collapsed. """
        test_string = "a\n\n\x00\x00b"

        terminal_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP)
        self.assertEqual(terminal_output, "a\\n\\n\\x00\\x00b")

        csv_output = format_value_text(test_string, encoding='utf-8', colormap=NO_COLOR_MAP,
                                       escape_control_chars=False)
        self.assertEqual(csv_output, test_string)

    def test_format_value_tuple_control_chars(self):
        """ format_value_tuple delegates to format_simple_collection; verify
            the flag propagates to its element formatters. """
        tuple_val = ("a\n", "b\x00")
        cql_type = CqlType('tuple<text, text>')

        terminal_output = format_value_tuple(tuple_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "('a\\n', 'b\\x00')")

        csv_output = format_value_tuple(tuple_val, cqltype=cql_type, escape_control_chars=False,
                                        **self.fmt_kwargs)
        self.assertEqual(csv_output, "('a\n', 'b\x00')")

    def test_format_value_set_control_chars(self):
        """ format_value_set delegates to format_simple_collection. A list is
            passed here because format_simple_collection just iterates val and
            CPython set iteration order depends on PYTHONHASHSEED. """
        set_val = ["a\n", "b\x00"]
        cql_type = CqlType('set<text>')

        terminal_output = format_value_set(set_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "{'a\\n', 'b\\x00'}")

        csv_output = format_value_set(set_val, cqltype=cql_type, escape_control_chars=False,
                                      **self.fmt_kwargs)
        self.assertEqual(csv_output, "{'a\n', 'b\x00'}")

    def test_nested_map_of_list_control_chars(self):
        """ Two-level nesting (map<text, list<text>>): the flag must propagate
            through the outer map's subformat() into the inner list's element
            formatters as well. Guards against regressions where the flag is
            forwarded at one level but dropped at the next. """
        nested_val = {"key\n1": ["v\x001", "v\n2"]}
        cql_type = CqlType('map<text, list<text>>')

        terminal_output = format_value_map(nested_val, cqltype=cql_type, **self.fmt_kwargs)
        self.assertEqual(terminal_output, "{'key\\n1': ['v\\x001', 'v\\n2']}")

        csv_output = format_value_map(nested_val, cqltype=cql_type, escape_control_chars=False,
                                      **self.fmt_kwargs)
        self.assertEqual(csv_output, "{'key\n1': ['v\x001', 'v\n2']}")