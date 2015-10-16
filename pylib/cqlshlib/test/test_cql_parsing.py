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

        tokens = CqlRuleSet.lex("'spam\nspam\n\tsausage'")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[0][0], "quotedStringLiteral")

        tokens = CqlRuleSet.lex("'spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[0][0], "unclosedString")

        tokens = CqlRuleSet.lex("'foo bar' 'spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        self.assertEqual(tokens[1][0], "unclosedString")

    def test_parse_pgstring_literals(self):
        for n in ["$$eggs$$", "$$Sausage 1$$", "$$spam\nspam\n\tsausage$$", "$$$$"]:
            self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex(n)),
                                     [(n, 'pgStringLiteral')])
        self.assertSequenceEqual(tokens_with_types(CqlRuleSet.lex("$$eggs$$")),
                                 [("$$eggs$$", 'pgStringLiteral')])

        tokens = CqlRuleSet.lex("$$spam\nspam\n\tsausage$$")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [('pgStringLiteral', '$$spam\nspam\n\tsausage$$', (0, 22))]
        self.assertEqual(tokens[0][0], "pgStringLiteral")

        tokens = CqlRuleSet.lex("$$spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [('unclosedPgString', '$$', (0, 2)), ('identifier', 'spam', (2, 6)), ('identifier', 'spam', (7, 11))]
        self.assertEqual(tokens[0][0], "unclosedPgString")

        tokens = CqlRuleSet.lex("$$foo bar$$ $$spam\nspam\n")
        tokens = CqlRuleSet.cql_massage_tokens(tokens)
        # [('pgStringLiteral', '$$foo bar$$', (0, 11)), ('unclosedPgString', '$$', (12, 14)), ('identifier', 'spam', (14, 18)), ('identifier', 'spam', (19, 23))]
        self.assertEqual(tokens[0][0], "pgStringLiteral")
        self.assertEqual(tokens[1][0], "unclosedPgString")

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
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('test', 'identifier')])

    def test_parse_select(self):
        parsed = parse_cqlsh_statements('SELECT FROM ks.tab;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements('SELECT FROM "MyTable";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('"MyTable"', 'quotedName'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE foo = 3;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('foo', 'identifier'),
                                  ('=', 'op'),
                                  ('3', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab ORDER BY event_id DESC LIMIT 1000')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('ORDER', 'reserved_identifier'),
                                  ('BY', 'reserved_identifier'),
                                  ('event_id', 'identifier'),
                                  ('DESC', 'reserved_identifier'),
                                  ('LIMIT', 'reserved_identifier'),
                                  ('1000', 'wholenumber')])

        parsed = parse_cqlsh_statements(
            'SELECT FROM tab WHERE clustering_column > 200 '
            'AND clustering_column < 400 ALLOW FILTERING')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('SELECT', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('clustering_column', 'identifier'),
                                  ('>', 'cmp'),
                                  ('200', 'wholenumber'),
                                  ('AND', 'reserved_identifier'),
                                  ('clustering_column', 'identifier'),
                                  ('<', 'cmp'),
                                  ('400', 'wholenumber'),
                                  # 'allow' and 'filtering' are not keywords
                                  ('ALLOW', 'reserved_identifier'),
                                  ('FILTERING', 'identifier')])

    def test_parse_insert(self):
        parsed = parse_cqlsh_statements('INSERT INTO mytable (x) VALUES (2);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'identifier'),
                                  ('(', 'op'),
                                  ('2', 'wholenumber'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'identifier'),
                                  ('(', 'op'),
                                  ('2', 'wholenumber'),
                                  (',', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO mytable (x, y) VALUES (2, 'eggs');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('x', 'identifier'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'identifier'),
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
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('mytable', 'identifier'),
                                  ('(', 'op'),
                                  ('ids', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'identifier'),
                                  ('(', 'op'),
                                  ('7ee251da-af52-49a4-97f4-3f07e406c7a7', 'uuid'),
                                  (')', 'op'),
                                  ('USING', 'reserved_identifier'),
                                  ('TTL', 'identifier'),
                                  ('86400', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "INSERT INTO test_table (username) VALUES ('Albert') "
            "USING TIMESTAMP 1240003134 AND TTL 600;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('INSERT', 'reserved_identifier'),
                                  ('INTO', 'reserved_identifier'),
                                  ('test_table', 'identifier'),
                                  ('(', 'op'),
                                  ('username', 'identifier'),
                                  (')', 'op'),
                                  ('VALUES', 'identifier'),
                                  ('(', 'op'),
                                  ("'Albert'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  ('USING', 'reserved_identifier'),
                                  ('TIMESTAMP', 'identifier'),
                                  ('1240003134', 'wholenumber'),
                                  ('AND', 'reserved_identifier'),
                                  ('TTL', 'identifier'),
                                  ('600', 'wholenumber'),
                                  (';', 'endtoken')])

    def test_parse_update(self):
        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab USING TTL 432000 SET x = 15 WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('USING', 'reserved_identifier'),
                                  ('TTL', 'identifier'),
                                  ('432000', 'wholenumber'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15, y = 'sausage' "
            "WHERE y = 'eggs';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  (',', 'op'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'eggs'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 "
            "WHERE y IN ('eggs', 'sausage', 'spam');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('IN', 'reserved_identifier'),
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
            "WHERE y = 'spam' IF z = 'sausage';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('IF', 'reserved_identifier'),
                                  ('z', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' "
            "IF z = 'sausage' AND w = 'spam';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('IF', 'reserved_identifier'),
                                  ('z', 'identifier'),
                                  ('=', 'op'),
                                  ("'sausage'", 'quotedStringLiteral'),
                                  ('AND', 'reserved_identifier'),
                                  ('w', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "UPDATE tab SET x = 15 WHERE y = 'spam' IF EXISTS")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('UPDATE', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('SET', 'reserved_identifier'),
                                  ('x', 'identifier'),
                                  ('=', 'op'),
                                  ('15', 'wholenumber'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('y', 'identifier'),
                                  ('=', 'op'),
                                  ("'spam'", 'quotedStringLiteral'),
                                  ('IF', 'reserved_identifier'),
                                  ('EXISTS', 'identifier')])

    def test_parse_delete(self):
        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE songid = 444;")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('songs', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('songid', 'identifier'),
                                  ('=', 'op'),
                                  ('444', 'wholenumber'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE FROM songs WHERE name IN "
            "('Yellow Submarine', 'Eleanor Rigby');")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'reserved_identifier'),
                                  ('FROM', 'reserved_identifier'),
                                  ('songs', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('name', 'identifier'),
                                  ('IN', 'reserved_identifier'),
                                  ('(', 'op'),
                                  ("'Yellow Submarine'", 'quotedStringLiteral'),
                                  (',', 'op'),
                                  ("'Eleanor Rigby'", 'quotedStringLiteral'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE task_map ['2014-12-25'] FROM tasks WHERE user_id = 'Santa';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'reserved_identifier'),
                                  ('task_map', 'identifier'),
                                  ('[', 'brackets'),
                                  ("'2014-12-25'", 'quotedStringLiteral'),
                                  (']', 'brackets'),
                                  ('FROM', 'reserved_identifier'),
                                  ('tasks', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
                                  ('user_id', 'identifier'),
                                  ('=', 'op'),
                                  ("'Santa'", 'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "DELETE my_list[0] FROM lists WHERE user_id = 'Jim';")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DELETE', 'reserved_identifier'),
                                  ('my_list', 'identifier'),
                                  ('[', 'brackets'),
                                  ('0', 'wholenumber'),
                                  (']', 'brackets'),
                                  ('FROM', 'reserved_identifier'),
                                  ('lists', 'identifier'),
                                  ('WHERE', 'reserved_identifier'),
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
                                 [('CREATE', 'reserved_identifier'),
                                  ('KEYSPACE', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('WITH', 'reserved_identifier'),
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
                                 [('CREATE', 'reserved_identifier'),
                                  ('KEYSPACE', 'reserved_identifier'),
                                  ('"Cql_test_KS"', 'quotedName'),
                                  ('WITH', 'reserved_identifier'),
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
                                 [('CREATE', 'reserved_identifier'),
                                  ('KEYSPACE', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('WITH', 'reserved_identifier'),
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
                                  ('AND', 'reserved_identifier'),
                                  # 'DURABLE_WRITES' is not a keyword
                                  ('DURABLE_WRITES', 'identifier'),
                                  ('=', 'op'),
                                  ('false', 'identifier'),
                                  (';', 'endtoken')])

    def test_parse_drop_keyspace(self):
        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'reserved_identifier'),
                                  ('KEYSPACE', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP SCHEMA ks;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'reserved_identifier'),
                                  ('SCHEMA', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'DROP KEYSPACE IF EXISTS "My_ks";')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('DROP', 'reserved_identifier'),
                                  ('KEYSPACE', 'reserved_identifier'),
                                  ('IF', 'reserved_identifier'),
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
                                 (('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab (i) IF NOT EXISTS;')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 (('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('IF', 'reserved_identifier'),
                                  ('NOT', 'reserved_identifier'),
                                  ('EXISTS', 'identifier'),
                                  (';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON tab (KEYS(i));')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 (('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('KEYS', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (')', 'op'),
                                  (';', 'endtoken')))

        parsed = parse_cqlsh_statements(
            'CREATE INDEX idx ON ks.tab FULL(i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('FULL', 'reserved_identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            'CREATE CUSTOM INDEX idx ON ks.tab (i);')
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'reserved_identifier'),
                                  ('CUSTOM', 'identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
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
                                 [('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('USING', 'reserved_identifier'),
                                  ("'org.custom.index.MyIndexClass'",
                                   'quotedStringLiteral'),
                                  (';', 'endtoken')])

        parsed = parse_cqlsh_statements(
            "CREATE INDEX idx ON ks.tab (i) WITH OPTIONS = "
            "{'storage': '/mnt/ssd/indexes/'};")
        self.assertSequenceEqual(tokens_with_types(parsed),
                                 [('CREATE', 'reserved_identifier'),
                                  ('INDEX', 'reserved_identifier'),
                                  ('idx', 'identifier'),
                                  ('ON', 'reserved_identifier'),
                                  ('ks', 'identifier'),
                                  ('.', 'op'),
                                  ('tab', 'identifier'),
                                  ('(', 'op'),
                                  ('i', 'identifier'),
                                  (')', 'op'),
                                  ('WITH', 'reserved_identifier'),
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
