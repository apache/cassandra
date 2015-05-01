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
# for Thrift connections, and $CQL_TEST_PORT to the associated port.

from unittest import TestCase
from operator import itemgetter

from ..cql3handling import CqlRuleSet


class TestCqlParsing(TestCase):
    def test_parse_string_literals(self):
        for n in ["'eggs'", "'Sausage 1'", "'spam\nspam\n\tsausage'", "''"]:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'quotedStringLiteral')])
        self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex("'eggs'")),
                                 [("'eggs'", 'quotedStringLiteral')])

    def test_parse_numbers(self):
        for n in ['6', '398', '18018']:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'wholenumber')])

    def test_parse_uuid(self):
        uuids = ['4feeae80-e9cc-11e4-b571-0800200c9a66',
                 '7142303f-828f-4806-be9e-7a973da0c3f9',
                 'dff8d435-9ca0-487c-b5d0-b0fe5c5768a8']
        for u in uuids:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(u)),
                                     [(u, 'uuid')])

    def test_comments_in_string_literals(self):
        comment_strings = ["'sausage -- comment'",
                           "'eggs and spam // comment string'",
                           "'spam eggs sausage and spam /* still in string'"]
        for s in comment_strings:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(s)),
                                     [(s, 'quotedStringLiteral')])

    def test_colons_in_string_literals(self):
        comment_strings = ["'Movie Title: The Movie'",
                           "':a:b:c:'",
                           "'(>>=) :: (Monad m) => m a -> (a -> m b) -> m b'"]
        for s in comment_strings:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(s)),
                                     [(s, 'quotedStringLiteral')])

    def test_partial_parsing(self):
        [parsed] = CqlRuleSet.cql_parse('INSERT INTO ks.test')
        self.assertSequenceEqual(parsed.matched, [])
        self.assertSequenceEqual(tokens_with_types(parsed.remainder),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('test', 'identifier')])

    def test_parse_select(self):
        parsed = parse_cqlsh_statements('SELECT FROM ks.tab;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'K_SELECT'),
                                  ('FROM', 'K_FROM'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements('SELECT FROM "MyTable";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'K_SELECT'),
                                  ('FROM', 'K_FROM'),
                                  ('"MyTable"', 'quotedName'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE foo = 3;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'K_SELECT'),
                                  ('FROM', 'K_FROM'),
                                  ('tab', 'identifier'),
                                  ('WHERE', 'K_WHERE'),
                                  ('foo', 'identifier'),
                                  ('=', 'op'),
                                  ('3', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab ORDER BY event_id DESC LIMIT 1000')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'K_SELECT'),
                                  ('FROM', 'K_FROM'),
                                  ('tab', 'identifier'),
                                  ('ORDER', 'K_ORDER'),
                                  ('BY', 'K_BY'),
                                  ('event_id', 'identifier'),
                                  ('DESC', 'K_DESC'),
                                  ('LIMIT', 'K_LIMIT'),
                                  ('1000', 'wholenumber')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE clustering_column > 200 '
            'AND clustering_column < 400 ALLOW FILTERING')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'K_SELECT'),
                                  ('FROM', 'K_FROM'),
                                  ('tab', 'identifier'),
                                  ('WHERE', 'K_WHERE'),
                                  ('clustering_column', 'identifier'),
                                  ('>', 'cmp'),
                                  ('200', 'wholenumber'),
                                  ('AND', 'K_AND'),
                                  ('clustering_column', 'identifier'),
                                  ('<', 'cmp'),
                                  ('400', 'wholenumber'),
                                  # 'allow' and 'filtering' are not keywords
                                  ('ALLOW', 'identifier'),
                                  ('FILTERING', 'identifier')])

    def test_parse_insert(self):
        parsed = parse_cqlsh_statements('INSERT INTO mytable (x) VALUES (2);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'K_VALUES'),
                                  ('(', 'op'),
                                  ('2', 'wholenumber'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'K_VALUES'),
                                  ('(', 'op'),
                                  ('2', 'wholenumber'),
                                  (',', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'K_VALUES'),
                                  ('(', 'op'),
                                  ('2', 'wholenumber'),
                                  (',', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (ids) VALUES "
            "(7ee251da-af52-49a4-97f4-3f07e406c7a7) "
            "USING TTL 86400;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('ids', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'K_VALUES'),
                                  ('(', 'op'),
                                  ('7ee251da-af52-49a4-97f4-3f07e406c7a7', 'uuid'),
                                  (')', 'op'),
                                  ('USING', 'K_USING'),
                                  ('TTL', 'K_TTL'),
                                  ('86400', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO test_table (username) VALUES ('Albert') "
            "USING TIMESTAMP 1240003134 AND TTL 600;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'K_INSERT'),
                                  ('INTO', 'K_INTO'),
                                  ('test_table', 'identifier'),
                                  ('(', 'op'),
                                  ('username', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'K_VALUES'),
                                  ('(', 'op'),
                                  ("'Albert'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  ('USING', 'K_USING'),
                                  ('TIMESTAMP', 'K_TIMESTAMP'),
                                  ('1240003134', 'wholenumber'),
                                  ('AND', 'K_AND'),
                                  ('TTL', 'K_TTL'),
                                  ('600', 'wholenumber'),
                                  (';', 'endtoken')])

    def test_parse_update(self):
        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab USING TTL 432000 SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('USING', 'K_USING'),
                                  ('TTL', 'K_TTL'),
                                  ('432000', 'wholenumber'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15, y = 'sausage' "
            "WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 "
            "WHERE y IN ('eggs', 'sausage', 'spam');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('IN', 'K_IN'),
                                  ('(', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 "
            "WHERE y = 'spam' if z = 'sausage';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('if', 'K_IF'),
                                  ('z', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' "
            "if z = 'sausage' AND w = 'spam';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('if', 'K_IF'),
                                  ('z', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  ('AND', 'K_AND'),
                                  ('w', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' IF EXISTS")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'K_UPDATE'),
                                  ('tab', 'identifier'),
                                  ('SET', 'K_SET'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'K_WHERE'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('IF', 'K_IF'),
                                  ('EXISTS', 'identifier')])

    def test_parse_delete(self):
        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE songid = 444;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'K_DELETE'),
                                  ('FROM', 'K_FROM'),
                                  ('songs', 'identifier'),
                                  ('WHERE', 'K_WHERE'),
                                  ('songid', 'identifier'),
                                  ('=', 'op'),
                                  ('444', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE name IN "
            "('Yellow Submarine', 'Eleanor Rigby');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'K_DELETE'),
                                  ('FROM', 'K_FROM'),
                                  ('songs', 'identifier'),
                                  ('WHERE', 'K_WHERE'),
                                  ('name', 'identifier'),
                                  ('IN', 'K_IN'),
                                  ('(', 'op'),
                                  ("'Yellow Submarine'", 'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'Eleanor Rigby'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE task_map ['2014-12-25'] from tasks where user_id = 'Santa';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'K_DELETE'),
                                  ('task_map', 'identifier'),
                                  ('[', 'brackets'),
                                  ("'2014-12-25'", 'quotedStringLiteral'),
                                  (']', 'brackets'),
                                  ('from', 'K_FROM'),
                                  ('tasks', 'identifier'),
                                  ('where', 'K_WHERE'),
                                  ('user_id', 'identifier'),
                                  ('=', 'op'),
                                  ("'Santa'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE my_list[0] from lists where user_id = 'Jim';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'K_DELETE'),
                                  ('my_list', 'identifier'),
                                  ('[', 'brackets'),
                                  ('0', 'wholenumber'),
                                  (']', 'brackets'),
                                  ('from', 'K_FROM'),
                                  ('lists', 'identifier'),
                                  ('where', 'K_WHERE'),
                                  ('user_id', 'identifier'),
                                  ('=', 'op'),
                                  ("'Jim'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

    def test_parse_batch(self):
        pass

    def test_parse_create_keyspace(self):
        parsed = parse_cqlsh_statements(
            "CREATE KEYSPACE ks WITH REPLICATION = "
            "{'class': 'SimpleStrategy', 'replication_factor': 1};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('KEYSPACE', 'K_KEYSPACE'),
                                  ('ks', 'identifier'),
                                  ('WITH', 'K_WITH'),
                                  ('REPLICATION', 'identifier'),
                                  ('=', 'op'),
                                  ('{', 'brackets'),
                                  ("'class'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ("'SimpleStrategy'", 'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'replication_factor'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ('1', 'wholenumber'),
                                  ('}', 'brackets'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE KEYSPACE "Cql_test_KS" WITH REPLICATION = '
            "{'class': 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2': 2};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('KEYSPACE', 'K_KEYSPACE'),
                                  ('"Cql_test_KS"', 'quotedName'),
                                  ('WITH', 'K_WITH'),
                                  ('REPLICATION', 'identifier'),
                                  ('=', 'op'),
                                  ('{', 'brackets'),
                                  ("'class'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ("'NetworkTopologyStrategy'",
                                   'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'dc1'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ('3', 'wholenumber'),
                                  (',', 'op'),
                                  ("'dc2'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ('2', 'wholenumber'),
                                  ('}', 'brackets'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE KEYSPACE ks WITH REPLICATION = "
            "{'class': 'NetworkTopologyStrategy', 'dc1': 3} AND "
            "DURABLE_WRITES = false;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('KEYSPACE', 'K_KEYSPACE'),
                                  ('ks', 'identifier'),
                                  ('WITH', 'K_WITH'),
                                  ('REPLICATION', 'identifier'),
                                  ('=', 'op'),
                                  ('{', 'brackets'),
                                  ("'class'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ("'NetworkTopologyStrategy'",
                                   'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'dc1'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ('3', 'wholenumber'),
                                  ('}', 'brackets'),
                                  ('AND', 'K_AND'),
                                  # 'DURABLE_WRITES' is not a keyword
                                  ('DURABLE_WRITES', 'identifier'),
                                  ('=', 'op'),
                                  ('false', 'identifier'),
                                  (';', 'endtoken')])

    def test_parse_drop_keyspace(self):
        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'K_DROP'),
                                  ('KEYSPACE', 'K_KEYSPACE'),
                                  ('ks', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP SCHEMA ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'K_DROP'),
                                  ('SCHEMA', 'K_SCHEMA'),
                                  ('ks', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE IF EXISTS "My_ks";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'K_DROP'),
                                  ('KEYSPACE', 'K_KEYSPACE'),
                                  ('IF', 'K_IF'),
                                  ('EXISTS', 'identifier'),
                                  ('"My_ks"', 'quotedName'),
                                  (';', 'endtoken')])

    def test_parse_create_table(self):
        pass

    def test_parse_drop_table(self):
        pass

    def test_parse_truncate(self):
        pass

    def test_parse_alter_table(self):
        pass

    def test_parse_use(self):
        pass

    def test_parse_create_index(self):
        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab (i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab (i) IF NOT EXISTS;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('IF', 'K_IF'),
                                  ('NOT', 'K_NOT'),
                                  ('EXISTS', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON tab (KEYS(i));')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('KEYS', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab FULL(i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('FULL', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE CUSTOM INDEX idx ON ks.tab (i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('CUSTOM', 'K_CUSTOM'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE INDEX idx ON ks.tab (i) USING "
            "'org.custom.index.MyIndexClass';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('USING', 'K_USING'),
                                  ("'org.custom.index.MyIndexClass'",
                                   'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE INDEX idx ON ks.tab (i) WITH OPTIONS = "
            "{'storage': '/mnt/ssd/indexes/'};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'K_CREATE'),
                                  ('INDEX', 'K_INDEX'),
                                  ('idx', 'identifier'),
                                  ('ON', 'K_ON'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('WITH', 'K_WITH'),
                                  ('OPTIONS', 'identifier'),
                                  ('=', 'op'),
                                  ('{', 'brackets'),
                                  ("'storage'", 'quotedStringLiteral'),
                                  (':', 'colon'),
                                  ("'/mnt/ssd/indexes/'", 'quotedStringLiteral'),
                                  ('}', 'brackets'),
                                  (';', 'endtoken')])

    def test_parse_drop_index(self):
        pass

    def test_parse_select_token(self):
        pass


def parse_cqlsh_statements(text):
    '''
    Runs its argument through the sequence of parsing steps that cqlsh takes its
    input through.

    Currently does not handle batch statements.
    '''
    # based on onecmd
    statements, _ = CqlRuleSet.cql_split_statements(text)
    # stops here. For regular cql commands, onecmd just splits it and sends it
    # off to the cql engine; parsing only happens for cqlsh-specific stmts.

    return strip_final_empty_items(statements)[0]


def tokens_with_types(lexed):
    for x in lexed:
        assert len(x) > 2, lexed
    return tuple(itemgetter(1, 0)(token) for token in lexed)


def strip_final_empty_items(xs):
    '''
    Returns its a copy of argument as a list, but with any terminating
    subsequence of falsey values removed.

    >>> strip_final_empty_items([[3, 4], [5, 6, 7], [], [], [1], []])
    [[3, 4], [5, 6, 7], [], [], [1]]
    '''
    rv = list(xs)

    while rv and not rv[-1]:
        rv = rv[:-1]

    return rv
