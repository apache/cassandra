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

BEL = '\x07' # the terminal-bell character
CTRL_C = '\x03'
TAB = '\t'

# completions not printed out in this many seconds may not be acceptable.
# tune if needed for a slow system, etc, but be aware that the test will
# need to wait this long for each completion test, to make sure more info
# isn't coming
COMPLETION_RESPONSE_TIME = 0.5

completion_separation_re = re.compile(r'\s+')

class CqlshCompletionCase(BaseTestCase):
    def setUp(self):
        self.cqlsh_runner = testrun_cqlsh(cqlver=cqlsh.DEFAULT_CQLVER, env={'COLUMNS': '100000'})
        self.cqlsh = self.cqlsh_runner.__enter__()

    def tearDown(self):
        self.cqlsh_runner.__exit__(None, None, None)

    def _trycompletions_inner(self, inputstring, immediate='', choices=(), other_choices_ok=False):
        """
        Test tab completion in cqlsh. Enters in the text in inputstring, then
        simulates a tab keypress to see what is immediately completed (this
        should only happen when there is only one completion possible). If
        there is an immediate completion, the new text is expected to match
        'immediate'. If there is no immediate completion, another tab keypress
        is simulated in order to get a list of choices, which are expected to
        match the items in 'choices' (order is not important, but case is).
        """
        self.cqlsh.send(inputstring)
        self.cqlsh.send(TAB)
        completed = self.cqlsh.read_up_to_timeout(COMPLETION_RESPONSE_TIME)
        completed = completed.replace(' \b', '')
        self.assertEqual(completed[:len(inputstring)], inputstring)
        completed = completed[len(inputstring):]
        completed = completed.replace(BEL, '')
        self.assertEqual(completed, immediate, 'cqlsh completed %r, but we expected %r'
                                               % (completed, immediate))
        if immediate:
            return

        self.cqlsh.send(TAB)
        choice_output = self.cqlsh.read_up_to_timeout(COMPLETION_RESPONSE_TIME)
        if choice_output == BEL:
            lines = ()
        else:
            lines = choice_output.splitlines()
            self.assertRegexpMatches(lines[-1], self.cqlsh.prompt.lstrip() + re.escape(inputstring))
        choicesseen = set()
        for line in lines[:-1]:
            choicesseen.update(completion_separation_re.split(line.strip()))
        choicesseen.discard('')
        if other_choices_ok:
            self.assertEqual(set(choices), choicesseen.intersection(choices))
        else:
            self.assertEqual(set(choices), choicesseen)

    def trycompletions(self, inputstring, immediate='', choices=(), other_choices_ok=False):
        try:
            self._trycompletions_inner(inputstring, immediate, choices, other_choices_ok)
        finally:
            self.cqlsh.send(CTRL_C) # cancel any current line
            self.cqlsh.read_to_next_prompt()

    def strategies(self):
        return self.module.CqlRuleSet.replication_strategies

class TestCqlshCompletion(CqlshCompletionCase):
    cqlver = '3.1.6'
    module = cqlsh.cql3handling

    def test_complete_on_empty_string(self):
        self.trycompletions('', choices=('?', 'ALTER', 'BEGIN', 'CAPTURE', 'CONSISTENCY',
                                         'COPY', 'CREATE', 'DEBUG', 'DELETE', 'DESC', 'DESCRIBE',
                                         'DROP', 'GRANT', 'HELP', 'INSERT', 'LIST', 'PAGING', 'REVOKE',
                                         'SELECT', 'SHOW', 'SOURCE', 'TRACING', 'EXPAND', 'TRUNCATE',
                                         'UPDATE', 'USE', 'exit', 'quit'))

    def test_complete_command_words(self):
        self.trycompletions('alt', '\b\b\bALTER ')
        self.trycompletions('I', 'NSERT INTO ')
        self.trycompletions('exit', ' ')

    def test_complete_in_uuid(self):
        pass

    def test_complete_in_select(self):
        pass

    def test_complete_in_insert(self):
        pass

    def test_complete_in_update(self):
        pass

    def test_complete_in_delete(self):
        pass

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
        self.trycompletions('insert into system."Index', 'Info"')
        self.trycompletions('USE "', choices=('system', self.cqlsh.keyspace),
                            other_choices_ok=True)
        self.trycompletions("create keyspace blah with replication = {'class': 'Sim",
                            "pleStrategy'")

    def test_complete_in_drop_keyspace(self):
        pass

    def test_complete_in_create_columnfamily(self):
        pass

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
