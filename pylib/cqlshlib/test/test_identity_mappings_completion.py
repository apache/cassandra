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

from .test_cqlsh_completion import CqlshCompletionCase


class TestIdentityMappingsCompletion(CqlshCompletionCase):

    """
        Autcomplete Tests for ADD and DROP Identity Mappings for @cql3handling.py
    """

    def add_test_user_autocomplete(self):
        self.trycompletions('ADD U', immediate='SER ')


    def add_test_exists_autocomplete(self):
        self.trycompletions('ADD USER IF ', immediate='NOT EXISTS ')
        self.trycompletions('ADD USER IF NOT ', immediate='EXISTS ')

    def test_expect_str_literal_add_autocomplete(self):
        self.trycompletions('ADD USER ',
            choices=['<pgStringLiteral>', '<quotedStringLiteral>', 'IF'])

    def add_test_to_autocomplete(self):
        self.trycompletions("ADD USER 'alice@example.com' ", immediate='TO ')
        self.trycompletions("ADD USER 'alice@example.com' T", immediate='O ')

    def add_test_role_autocomplete(self):
        self.trycompletions("ADD USER 'alice@example.com' TO ", immediate='ROLE ')
        self.trycompletions("ADD USER 'alice@example.com' TO R", immediate='OLE ')

    def add_test_rolename_autocomplete(self):
        self.trycompletions("ADD USER 'alice@example.com' TO ROLE ",
                            choices=['<identifier>', '<quotedName>'],
                            other_choices_ok=True)

    def add_test_complete_user_full_statement(self):
        self.trycompletions("ADD USER IF NOT EXISTS 'alice@example.com' TO ROLE data_engineer ",
                            choices=[';'])

    def drop_test_autocomplete(self):
        self.trycompletions('DROP ',
                            choices=['AGGREGATE', 'COLUMNFAMILY', 'FUNCTION',
                                     'INDEX', 'KEYSPACE', 'ROLE', 'TABLE',
                                     'TRIGGER', 'TYPE', 'USER', 'MATERIALIZED'])

    def drop_test_user_autcomplete(self):
        self.trycompletions('DROP USER ',
                            choices=['<pgStringLiteral>', '<quotedStringLiteral>', 'IF'])

    def drop_test_exists(self):
        self.trycompletions('DROP USER IF ', immediate='EXISTS ')
        self.trycompletions('DROP USER IF EXISTS ',
                            choices=['<pgStringLiteral>', '<quotedStringLiteral>'])

    def test_complete_drop_user_statement_end(self):
        self.trycompletions("DROP USER 'alice@example.com' ", choices=[';'])
        self.trycompletions("DROP USER IF EXISTS 'alice@example.com' ", choices=[';'])