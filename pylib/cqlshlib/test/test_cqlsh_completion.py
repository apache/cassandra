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

from __future__ import with_statement

import re
from .basecase import BaseTestCase, cqlsh
from .cassconnect import testrun_cqlsh
import unittest
import sys

BEL = '\x07'  # the terminal-bell character
CTRL_C = '\x03'
TAB = '\t'

# completions not printed out in this many seconds may not be acceptable.
# tune if needed for a slow system, etc, but be aware that the test will
# need to wait this long for each completion test, to make sure more info
# isn't coming
COMPLETION_RESPONSE_TIME = 0.5

completion_separation_re = re.compile(r'\s+')


@unittest.skipIf(sys.platform == "win32", 'Tab completion tests not supported on Windows')
class CqlshCompletionCase(BaseTestCase):

    def setUp(self):
        self.cqlsh_runner = testrun_cqlsh(cqlver=None, env={'COLUMNS': '100000'})
        self.cqlsh = self.cqlsh_runner.__enter__()

    def tearDown(self):
        self.cqlsh_runner.__exit__(None, None, None)

    def _get_completions(self, inputstring, split_completed_lines=True):
        """
        Get results of tab completion in cqlsh. Returns a bare string if a
        string completes immediately. Otherwise, returns a set of all
        whitespace-separated tokens in the offered completions by default, or a
        list of the lines in the offered completions if split_completed_lines is
        False.
        """
        self.cqlsh.send(inputstring)
        self.cqlsh.send(TAB)
        immediate = self.cqlsh.read_up_to_timeout(COMPLETION_RESPONSE_TIME)
        immediate = immediate.replace(' \b', '')
        self.assertEqual(immediate[:len(inputstring)], inputstring)
        immediate = immediate[len(inputstring):]
        immediate = immediate.replace(BEL, '')

        if immediate:
            return immediate

        self.cqlsh.send(TAB)
        choice_output = self.cqlsh.read_up_to_timeout(COMPLETION_RESPONSE_TIME)
        if choice_output == BEL:
            choice_output = ''

        self.cqlsh.send(CTRL_C)  # cancel any current line
        self.cqlsh.read_to_next_prompt()

        choice_lines = choice_output.splitlines()
        if choice_lines:
            # ensure the last line of the completion is the prompt
            prompt_regex = self.cqlsh.prompt.lstrip() + re.escape(inputstring)
            msg = ('Double-tab completion '
                   'does not print prompt for input "{}"'.format(inputstring))
            self.assertRegexpMatches(choice_lines[-1], prompt_regex, msg=msg)

        choice_lines = [line.strip() for line in choice_lines[:-1]]
        choice_lines = [line for line in choice_lines if line]

        if split_completed_lines:
            completed_lines = map(set, (completion_separation_re.split(line.strip())
                                  for line in choice_lines))

            if not completed_lines:
                return set()

            completed_tokens = set.union(*completed_lines)
            return completed_tokens - {''}
        else:
            return choice_lines

        assert False

    def _trycompletions_inner(self, inputstring, immediate='', choices=(),
                              other_choices_ok=False,
                              split_completed_lines=True):
        """
        Test tab completion in cqlsh. Enters in the text in inputstring, then
        simulates a tab keypress to see what is immediately completed (this
        should only happen when there is only one completion possible). If
        there is an immediate completion, the new text is expected to match
        'immediate'. If there is no immediate completion, another tab keypress
        is simulated in order to get a list of choices, which are expected to
        match the items in 'choices' (order is not important, but case is).
        """
        completed = self._get_completions(inputstring,
                                          split_completed_lines=split_completed_lines)

        if immediate:
            msg = 'cqlsh completed %r, but we expected %r' % (completed, immediate)
            self.assertEqual(completed, immediate, msg=msg)
            return

        if other_choices_ok:
            self.assertEqual(set(choices), completed.intersection(choices))
        else:
            self.assertEqual(set(choices), set(completed))

    def trycompletions(self, inputstring, immediate='', choices=(),
                       other_choices_ok=False, split_completed_lines=True):
        try:
            self._trycompletions_inner(inputstring, immediate, choices,
                                       other_choices_ok=other_choices_ok,
                                       split_completed_lines=split_completed_lines)
        finally:
            self.cqlsh.send(CTRL_C)  # cancel any current line
            self.cqlsh.read_to_next_prompt()

    def strategies(self):
        return self.module.CqlRuleSet.replication_strategies


class TestCqlshCompletion(CqlshCompletionCase):
    cqlver = '3.1.6'
    module = cqlsh.cql3handling

    def test_complete_on_empty_string(self):
        self.trycompletions('', choices=('?', 'ALTER', 'BEGIN', 'CAPTURE', 'CONSISTENCY',
                                         'COPY', 'CREATE', 'DEBUG', 'DELETE', 'DESC', 'DESCRIBE',
                                         'DROP', 'GRANT', 'HELP', 'INSERT', 'LIST', 'LOGIN', 'PAGING', 'REVOKE',
                                         'SELECT', 'SHOW', 'SOURCE', 'TRACING', 'EXPAND', 'SERIAL', 'TRUNCATE',
                                         'UPDATE', 'USE', 'exit', 'quit', 'CLEAR', 'CLS'))

    def test_complete_command_words(self):
        self.trycompletions('alt', '\b\b\bALTER ')
        self.trycompletions('I', 'NSERT INTO ')
        self.trycompletions('exit', ' ')

    def test_complete_in_uuid(self):
        pass

    def test_complete_in_select(self):
        pass

    def test_complete_in_insert(self):
        self.trycompletions('INSERT INTO  ',
                            choices=('twenty_rows_table',
                                     'ascii_with_special_chars',
                                     'users',
                                     'has_all_types',
                                     'system.',
                                     'empty_composite_table',
                                     'empty_table',
                                     'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'system_traces.',
                                     'songs'),
                            other_choices_ok=True)
        self.trycompletions('INSERT INTO twenty_rows_composite_table',
                            immediate=' ')
        self.trycompletions('INSERT INTO twenty_rows_composite_table ',
                            choices=['(', 'JSON'])
        self.trycompletions('INSERT INTO twenty_rows_composite_table (a, b ',
                            choices=(')', ','))
        self.trycompletions('INSERT INTO twenty_rows_composite_table (a, b, ',
                            immediate='c ')
        self.trycompletions('INSERT INTO twenty_rows_composite_table (a, b, c ',
                            choices=(',', ')'))
        self.trycompletions('INSERT INTO twenty_rows_composite_table (a, b)',
                            immediate=' VALUES ( ')
        self.trycompletions('INSERT INTO twenty_rows_composite_table (a, b, c) VAL',
                            immediate='UES ( ')

        self.trycompletions(
            'INSERT INTO twenty_rows_composite_table (a, b, c) VALUES (',
            ['<value for a (text)>'],
            split_completed_lines=False)

        self.trycompletions(
            "INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('",
            ['<value for a (text)>'],
            split_completed_lines=False)

        self.trycompletions(
            "INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ( 'eggs",
            ['<value for a (text)>'],
            split_completed_lines=False)

        self.trycompletions(
            "INSERT INTO twenty_rows_composite_table (a, b, c) VALUES ('eggs'",
            immediate=', ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs',"),
            ['<value for b (text)>'],
            split_completed_lines=False)

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam')"),
            immediate=' ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') "),
            choices=[';', 'USING', 'IF'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam');"),
            choices=['?', 'ALTER', 'BEGIN', 'CAPTURE', 'CONSISTENCY', 'COPY',
                     'CREATE', 'DEBUG', 'DELETE', 'DESC', 'DESCRIBE', 'DROP',
                     'EXPAND', 'GRANT', 'HELP', 'INSERT', 'LIST', 'LOGIN', 'PAGING',
                     'REVOKE', 'SELECT', 'SHOW', 'SOURCE', 'SERIAL', 'TRACING',
                     'TRUNCATE', 'UPDATE', 'USE', 'exit', 'quit',
                     'CLEAR', 'CLS'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') US"),
            immediate='ING T')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING"),
            immediate=' T')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING T"),
            choices=['TTL', 'TIMESTAMP'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TT"),
            immediate='L ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TI"),
            immediate='MESTAMP ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TIMESTAMP "),
            choices=['<wholenumber>'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL "),
            choices=['<wholenumber>'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TIMESTAMP 0 "),
            choices=['AND', ';'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL 0 "),
            choices=['AND', ';'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TIMESTAMP 0 A"),
            immediate='ND TTL ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL 0 A"),
            immediate='ND TIMESTAMP ')

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL 0 AND TIMESTAMP "),
            choices=['<wholenumber>'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL 0 AND TIMESTAMP 0 "),
            choices=['AND', ';'])

        self.trycompletions(
            ("INSERT INTO twenty_rows_composite_table (a, b, c) "
             "VALUES ( 'eggs', 'sausage', 'spam') USING TTL 0 AND TIMESTAMP 0 AND "),
            choices=[])

    def test_complete_in_update(self):
        self.trycompletions("UPD", immediate="ATE ")
        self.trycompletions("UPDATE ",
                            choices=['twenty_rows_table',
                                     'users', 'has_all_types', 'system.',
                                     'ascii_with_special_chars',
                                     'empty_composite_table', 'empty_table',
                                     'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'system_traces.', 'songs'],
                            other_choices_ok=True)

        self.trycompletions("UPDATE empty_table ", choices=['USING', 'SET'])

        self.trycompletions("UPDATE empty_table S",
                            immediate='ET lonelycol = ')
        self.trycompletions("UPDATE empty_table SET lon",
                            immediate='elycol = ')
        self.trycompletions("UPDATE empty_table SET lonelycol",
                            immediate=' = ')

        self.trycompletions("UPDATE empty_table U", immediate='SING T')
        self.trycompletions("UPDATE empty_table USING T",
                            choices=["TTL", "TIMESTAMP"])

        self.trycompletions("UPDATE empty_table SET lonelycol = ",
                            choices=['<term (text)>'],
                            split_completed_lines=False)

        self.trycompletions("UPDATE empty_table SET lonelycol = 'eg",
                            choices=['<term (text)>'],
                            split_completed_lines=False)
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs'",
                            choices=[',', 'WHERE'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE ",
                            choices=['TOKEN(', 'lonelykey'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE ",
                            choices=['TOKEN(', 'lonelykey'])

        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE lonel",
                            immediate='ykey ')
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE lonelykey ",
                            choices=['=', '<=', '>=', '>', '<', 'CONTAINS', 'IN', '['])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE lonelykey = 0.0 ",
                            choices=['AND', 'IF', ';'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE lonelykey = 0.0 AND ",
                            choices=['TOKEN(', 'lonelykey'])

        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE TOKEN(lonelykey ",
                            choices=[',', ')'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE TOKEN(lonelykey) ",
                            choices=['=', '<=', '>=', '<', '>'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE TOKEN(lonelykey) <= TOKEN(13) ",
                            choices=[';', 'AND', 'IF'])
        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE TOKEN(lonelykey) <= TOKEN(13) IF ",
                            choices=['EXISTS', '<quotedName>', '<identifier>'])

        self.trycompletions("UPDATE empty_table SET lonelycol = 'eggs' WHERE TOKEN(lonelykey) <= TOKEN(13) IF EXISTS ",
                            choices=['>=', '!=', '<=', 'IN', '[', ';', '=', '<', '>', '.'])

    def test_complete_in_delete(self):
        self.trycompletions('DELETE F', choices=['FROM', '<identifier>', '<quotedName>'])

        self.trycompletions('DELETE a ', choices=['FROM', '[', '.', ','])
        self.trycompletions('DELETE a [',
                            choices=['<wholenumber>', 'false', '-', '<uuid>',
                                     '<pgStringLiteral>', '<float>', 'TOKEN',
                                     '<identifier>', '<quotedStringLiteral>',
                                     '{', '[', 'NULL', 'true', '<blobLiteral>'])

        self.trycompletions('DELETE a, ',
                            choices=['<identifier>', '<quotedName>'])

        self.trycompletions('DELETE a FROM ',
                            choices=['twenty_rows_table',
                                     'ascii_with_special_chars', 'users',
                                     'has_all_types', 'system.',
                                     'empty_composite_table', 'empty_table',
                                     'system_auth.', 'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'system_traces.', 'songs',
                                     '"' + self.cqlsh.keyspace + '".'],
                            other_choices_ok=True)

        self.trycompletions('DELETE FROM ',
                            choices=['twenty_rows_table',
                                     'ascii_with_special_chars', 'users',
                                     'has_all_types', 'system.',
                                     'empty_composite_table', 'empty_table',
                                     'system_auth.', 'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'system_traces.', 'songs',
                                     'system_auth.', 'system_distributed.',
                                     'system_schema.', 'system_traces.',
                                     '"' + self.cqlsh.keyspace + '".'],
                            other_choices_ok=True)
        self.trycompletions('DELETE FROM twenty_rows_composite_table ',
                            choices=['USING', 'WHERE'])

        self.trycompletions('DELETE FROM twenty_rows_composite_table U',
                            immediate='SING TIMESTAMP ')

        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP ',
                            choices=['<wholenumber>'])
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0',
                            choices=['<wholenumber>'])
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 ',
                            immediate='WHERE ')
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE ',
                            choices=['a', 'b', 'TOKEN('])

        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE a ',
                            choices=['<=', '>=', 'CONTAINS', 'IN', '[', '=', '<', '>'])

        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE TOKEN(',
                            immediate='a ')
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE TOKEN(a',
                            immediate=' ')
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE TOKEN(a ',
                            choices=[')', ','])
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE TOKEN(a) ',
                            choices=['>=', '<=', '=', '<', '>'])
        self.trycompletions('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE TOKEN(a) >= ',
                            choices=['false', 'true', '<pgStringLiteral>',
                                     'token(', '-', '<float>', 'TOKEN',
                                     '<identifier>', '<uuid>', '{', '[', 'NULL',
                                     '<quotedStringLiteral>', '<blobLiteral>',
                                     '<wholenumber>'])
        self.trycompletions(('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE '
                             'TOKEN(a) >= TOKEN(0) '),
                            choices=['AND', 'IF', ';'])
        self.trycompletions(('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE '
                             'TOKEN(a) >= TOKEN(0) IF '),
                            choices=['EXISTS', '<identifier>', '<quotedName>'])
        self.trycompletions(('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE '
                             'TOKEN(a) >= TOKEN(0) IF b '),
                            choices=['>=', '!=', '<=', 'IN', '=', '<', '>'])
        self.trycompletions(('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE '
                             'TOKEN(a) >= TOKEN(0) IF b < 0 '),
                            choices=['AND', ';'])
        self.trycompletions(('DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE '
                             'TOKEN(a) >= TOKEN(0) IF b < 0 AND '),
                            choices=['<identifier>', '<quotedName>'])
        self.trycompletions(("DELETE FROM twenty_rows_composite_table USING TIMESTAMP 0 WHERE "
                             "b = 'eggs'"),
                            choices=['AND', 'IF', ';'])

    def test_complete_in_batch(self):
        pass

    def test_complete_in_create_keyspace(self):
        self.trycompletions('create keyspace ', '', choices=('<identifier>', '<quotedName>', 'IF'))
        self.trycompletions('create keyspace moo ',
                            "WITH replication = {'class': '")
        self.trycompletions('create keyspace "12SomeName" with ',
                            "replication = {'class': '")
        self.trycompletions("create keyspace fjdkljf with foo=bar ", "",
                            choices=('AND', ';'))
        self.trycompletions("create keyspace fjdkljf with foo=bar AND ",
                            "replication = {'class': '")
        self.trycompletions("create keyspace moo with replication", " = {'class': '")
        self.trycompletions("create keyspace moo with replication=", " {'class': '")
        self.trycompletions("create keyspace moo with replication={", "'class':'")
        self.trycompletions("create keyspace moo with replication={'class'", ":'")
        self.trycompletions("create keyspace moo with replication={'class': ", "'")
        self.trycompletions("create keyspace moo with replication={'class': '", "",
                            choices=self.strategies())
        # ttl is an "unreserved keyword". should work
        self.trycompletions("create keySPACE ttl with replication ="
                            "{ 'class' : 'SimpleStrategy'", ", 'replication_factor': ")
        self.trycompletions("create   keyspace ttl with replication ="
                            "{'class':'SimpleStrategy',", " 'replication_factor': ")
        self.trycompletions("create keyspace \"ttl\" with replication ="
                            "{'class': 'SimpleStrategy', ", "'replication_factor': ")
        self.trycompletions("create keyspace \"ttl\" with replication ="
                            "{'class': 'SimpleStrategy', 'repl", "ication_factor'")
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'SimpleStrategy', 'replication_factor': ", '',
                            choices=('<term>',))
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'SimpleStrategy', 'replication_factor': 1", '',
                            choices=('<term>',))
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'SimpleStrategy', 'replication_factor': 1 ", '}')
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'SimpleStrategy', 'replication_factor': 1, ",
                            '', choices=())
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'SimpleStrategy', 'replication_factor': 1} ",
                            '', choices=('AND', ';'))
        self.trycompletions("create keyspace foo with replication ="
                            "{'class': 'NetworkTopologyStrategy', ", '',
                            choices=('<dc_name>',))
        self.trycompletions("create keyspace \"PB and J\" with replication={"
                            "'class': 'NetworkTopologyStrategy'", ', ')
        self.trycompletions("create keyspace PBJ with replication={"
                            "'class': 'NetworkTopologyStrategy'} and ",
                            "durable_writes = '")

    def test_complete_in_string_literals(self):
        # would be great if we could get a space after this sort of completion,
        # but readline really wants to make things difficult for us
        self.trycompletions("create keyspace blah with replication = {'class': 'Sim",
                            "pleStrategy'")

    def test_complete_in_drop(self):
        self.trycompletions('DR', immediate='OP ')
        self.trycompletions('DROP ',
                            choices=['AGGREGATE', 'COLUMNFAMILY', 'FUNCTION',
                                     'INDEX', 'KEYSPACE', 'ROLE', 'TABLE',
                                     'TRIGGER', 'TYPE', 'USER', 'MATERIALIZED'])

    def test_complete_in_drop_keyspace(self):
        self.trycompletions('DROP K', immediate='EYSPACE ')
        quoted_keyspace = '"' + self.cqlsh.keyspace + '"'
        self.trycompletions('DROP KEYSPACE ',
                            choices=['IF', quoted_keyspace])

        self.trycompletions('DROP KEYSPACE ' + quoted_keyspace,
                            choices=[';'])

        self.trycompletions('DROP KEYSPACE I',
                            immediate='F EXISTS ' + quoted_keyspace + ';')

    def create_columnfamily_table_template(self, name):
        """Parameterized test for CREATE COLUMNFAMILY and CREATE TABLE. Since
        they're synonyms, they should have the same completion behavior, so this
        test avoids duplication between tests for the two statements."""
        prefix = 'CREATE ' + name + ' '
        quoted_keyspace = '"' + self.cqlsh.keyspace + '"'
        self.trycompletions(prefix + '',
                            choices=['IF', quoted_keyspace, '<new_table_name>'])
        self.trycompletions(prefix + 'IF ',
                            immediate='NOT EXISTS ')
        self.trycompletions(prefix + 'IF NOT EXISTS ',
                            choices=['<new_table_name>', quoted_keyspace])
        self.trycompletions(prefix + 'IF NOT EXISTS new_table ',
                            immediate='( ')

        self.trycompletions(prefix + quoted_keyspace, choices=['.', '('])

        self.trycompletions(prefix + quoted_keyspace + '( ',
                            choices=['<new_column_name>', '<identifier>',
                                     '<quotedName>'])

        self.trycompletions(prefix + quoted_keyspace + '.',
                            choices=['<new_table_name>'])
        self.trycompletions(prefix + quoted_keyspace + '.new_table ',
                            immediate='( ')
        self.trycompletions(prefix + quoted_keyspace + '.new_table ( ',
                            choices=['<new_column_name>', '<identifier>',
                                     '<quotedName>'])

        self.trycompletions(prefix + ' new_table ( ',
                            choices=['<new_column_name>', '<identifier>',
                                     '<quotedName>'])
        self.trycompletions(prefix + ' new_table (col_a ine',
                            immediate='t ')
        self.trycompletions(prefix + ' new_table (col_a int ',
                            choices=[',', 'PRIMARY'])
        self.trycompletions(prefix + ' new_table (col_a int P',
                            immediate='RIMARY KEY ')
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY ',
                            choices=[')', ','])

        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY,',
                            choices=['<identifier>', '<quotedName>'])
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY)',
                            immediate=' ')
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) ',
                            choices=[';', 'WITH'])
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) W',
                            immediate='ITH ')
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) WITH ',
                            choices=['bloom_filter_fp_chance', 'compaction',
                                     'compression',
                                     'dclocal_read_repair_chance',
                                     'default_time_to_live', 'gc_grace_seconds',
                                     'max_index_interval',
                                     'memtable_flush_period_in_ms',
                                     'read_repair_chance', 'CLUSTERING',
                                     'COMPACT', 'caching', 'comment',
                                     'min_index_interval', 'speculative_retry', 'cdc'])
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) WITH ',
                            choices=['bloom_filter_fp_chance', 'compaction',
                                     'compression',
                                     'dclocal_read_repair_chance',
                                     'default_time_to_live', 'gc_grace_seconds',
                                     'max_index_interval',
                                     'memtable_flush_period_in_ms',
                                     'read_repair_chance', 'CLUSTERING',
                                     'COMPACT', 'caching', 'comment',
                                     'min_index_interval', 'speculative_retry', 'cdc'])
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) WITH bloom_filter_fp_chance ',
                            immediate='= ')
        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) WITH bloom_filter_fp_chance = ',
                            choices=['<float_between_0_and_1>'])

        self.trycompletions(prefix + ' new_table (col_a int PRIMARY KEY) WITH compaction ',
                            immediate="= {'class': '")
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': '",
                            choices=['SizeTieredCompactionStrategy',
                                     'LeveledCompactionStrategy',
                                     'DateTieredCompactionStrategy',
                                     'TimeWindowCompactionStrategy'])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'S",
                            immediate="izeTieredCompactionStrategy'")
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy",
                            immediate="'")
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy'",
                            choices=['}', ','])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy', ",
                            immediate="'")
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy', '",
                            choices=['bucket_high', 'bucket_low', 'class',
                                     'enabled', 'max_threshold',
                                     'min_sstable_size', 'min_threshold',
                                     'tombstone_compaction_interval',
                                     'tombstone_threshold',
                                     'unchecked_tombstone_compaction',
                                     'only_purge_repaired_tombstones'])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy'}",
                            choices=[';', 'AND'])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'SizeTieredCompactionStrategy'} AND ",
                            choices=['bloom_filter_fp_chance', 'compaction',
                                     'compression',
                                     'dclocal_read_repair_chance',
                                     'default_time_to_live', 'gc_grace_seconds',
                                     'max_index_interval',
                                     'memtable_flush_period_in_ms',
                                     'read_repair_chance', 'CLUSTERING',
                                     'COMPACT', 'caching', 'comment',
                                     'min_index_interval', 'speculative_retry', 'cdc'])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'DateTieredCompactionStrategy', '",
                            choices=['base_time_seconds', 'max_sstable_age_days',
                                     'timestamp_resolution', 'min_threshold', 'class', 'max_threshold',
                                     'tombstone_compaction_interval', 'tombstone_threshold',
                                     'enabled', 'unchecked_tombstone_compaction',
                                     'max_window_size_seconds', 'only_purge_repaired_tombstones'])
        self.trycompletions(prefix + " new_table (col_a int PRIMARY KEY) WITH compaction = "
                            + "{'class': 'TimeWindowCompactionStrategy', '",
                            choices=['compaction_window_unit', 'compaction_window_size',
                                     'timestamp_resolution', 'min_threshold', 'class', 'max_threshold',
                                     'tombstone_compaction_interval', 'tombstone_threshold',
                                     'enabled', 'unchecked_tombstone_compaction',
                                     'only_purge_repaired_tombstones'])

    def test_complete_in_create_columnfamily(self):
        self.trycompletions('CREATE C', choices=['COLUMNFAMILY', 'CUSTOM'])
        self.trycompletions('CREATE CO', immediate='LUMNFAMILY ')
        self.create_columnfamily_table_template('COLUMNFAMILY')

    def test_complete_in_create_table(self):
        self.trycompletions('CREATE T', choices=['TRIGGER', 'TABLE', 'TYPE'])
        self.trycompletions('CREATE TA', immediate='BLE ')
        self.create_columnfamily_table_template('TABLE')

    def test_complete_in_describe(self):
        """
        Tests for Cassandra-10733
        """
        self.trycompletions('DES', immediate='C')
        # quoted_keyspace = '"' + self.cqlsh.keyspace + '"'
        self.trycompletions('DESCR', immediate='IBE ')
        self.trycompletions('DESC TABLE ',
                            choices=['twenty_rows_table',
                                     'ascii_with_special_chars', 'users',
                                     'has_all_types', 'system.',
                                     'empty_composite_table', 'empty_table',
                                     'system_auth.', 'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'system_traces.', 'songs',
                                     'system_distributed.',
                                     '"' + self.cqlsh.keyspace + '".'],
                            other_choices_ok=True)

        self.trycompletions('DESC TYPE ',
                            choices=['system.',
                                     'system_auth.',
                                     'system_traces.',
                                     'system_distributed.',
                                     'address',
                                     'phone_number',
                                     'band_info_type',
                                     'tags'],
                            other_choices_ok=True)

        self.trycompletions('DESC FUNCTION ',
                            choices=['system.',
                                     'system_auth.',
                                     'system_traces.',
                                     'system_distributed.',
                                     'fbestband',
                                     'fbestsong',
                                     'fmax',
                                     'fmin',
                                     '"' + self.cqlsh.keyspace + '".'],
                            other_choices_ok=True)

        self.trycompletions('DESC AGGREGATE ',
                            choices=['system.',
                                     'system_auth.',
                                     'system_traces.',
                                     'system_distributed.',
                                     'aggmin',
                                     'aggmax',
                                     '"' + self.cqlsh.keyspace + '".'],
                            other_choices_ok=True)

        # Unfortunately these commented tests will not work. This is due to the keyspace name containing quotes;
        # cqlsh auto-completes a DESC differently when the keyspace contains quotes. I'll leave the
        # test here though in case we ever change this script to test using keyspace names without
        # quotes

        # self.trycompletions('DESC TABLE ' + '"' + self.cqlsh.keyspace + '"', immediate='.')

        self.trycompletions('DESC TABLE ' + '"' + self.cqlsh.keyspace + '".',
                            choices=['twenty_rows_table',
                                     'ascii_with_special_chars',
                                     'users',
                                     'has_all_types',
                                     'empty_composite_table',
                                     'empty_table',
                                     'undefined_values_table',
                                     'dynamic_columns',
                                     'twenty_rows_composite_table',
                                     'utf8_with_special_chars',
                                     'songs'],
                            other_choices_ok=True)

        # See comment above for DESC TABLE
        # self.trycompletions('DESC TYPE ' + '"' + self.cqlsh.keyspace + '"', immediate='.')

        self.trycompletions('DESC TYPE ' + '"' + self.cqlsh.keyspace + '".',
                            choices=['address',
                                     'phone_number',
                                     'band_info_type',
                                     'tags'],
                            other_choices_ok=True)

        # See comment above for DESC TABLE
        # self.trycompletions('DESC FUNCTION ' + '"' + self.cqlsh.keyspace + '"', immediate='.f')

        self.trycompletions('DESC FUNCTION ' + '"' + self.cqlsh.keyspace + '".', immediate='f')

        self.trycompletions('DESC FUNCTION ' + '"' + self.cqlsh.keyspace + '".f',
                            choices=['fbestband',
                                     'fbestsong',
                                     'fmax',
                                     'fmin'],
                            other_choices_ok=True)

        # See comment above for DESC TABLE
        # self.trycompletions('DESC AGGREGATE ' + '"' + self.cqlsh.keyspace + '"', immediate='.aggm')

        self.trycompletions('DESC AGGREGATE ' + '"' + self.cqlsh.keyspace + '".', immediate='aggm')

        self.trycompletions('DESC AGGREGATE ' + '"' + self.cqlsh.keyspace + '".aggm',
                            choices=['aggmin',
                                     'aggmax'],
                            other_choices_ok=True)

    def test_complete_in_drop_columnfamily(self):
        pass

    def test_complete_in_truncate(self):
        pass

    def test_complete_in_alter_columnfamily(self):
        pass

    def test_complete_in_use(self):
        pass

    def test_complete_in_create_index(self):
        pass

    def test_complete_in_drop_index(self):
        pass
