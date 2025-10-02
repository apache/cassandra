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

    def test_identity_autocomplete_add(self):
        self.trycompletions('ADD I', immediate='DENTITY ')

    def test_exists_autocomplete_add(self):
        self.trycompletions('ADD IDENTITY IF ', immediate='NOT EXISTS ')
        self.trycompletions('ADD IDENTITY IF NOT ', immediate='EXISTS ')

    def test_expect_str_literal_autocomplete_add(self):
        self.trycompletions('ADD IDENTITY ',
            choices=['<pgStringLiteral>', '<quotedStringLiteral>', 'IF'])

    def test_TO_autocomplete_add(self):
        self.trycompletions("ADD IDENTITY 'alice@example.com' ", immediate='TO ROLE ')
        self.trycompletions("ADD IDENTITY 'alice@example.com' T", immediate='O ROLE ')

    def test_role_autocomplete_add(self):
        self.trycompletions("ADD IDENTITY 'alice@example.com' TO ", immediate='ROLE ')
        self.trycompletions("ADD IDENTITY 'alice@example.com' TO R", immediate='OLE ')

    def test_rolename_autocomplete_add(self):
        self.trycompletions("ADD IDENTITY 'alice@example.com' TO ROLE ",
                            choices=['<identifier>', '<quotedName>'],
                            other_choices_ok=True)

    def test_complete_user_full_statement_add(self):
        self.trycompletions("ADD IDENTITY IF NOT EXISTS 'alice@example.com' TO ROLE data_engineer ",
                            choices=[';'])

    def test_autocomplete_drop(self):
        self.trycompletions('DROP ',
                            choices=['AGGREGATE', 'COLUMNFAMILY', 'FUNCTION',
                                     'INDEX', 'KEYSPACE', 'ROLE', 'TABLE',
                                     'TRIGGER', 'TYPE', 'USER', 'MATERIALIZED', 'IDENTITY'])

    def test_identity_autcomplete_drop(self):
        self.trycompletions('DROP IDENTITY ',
                            choices=['<pgStringLiteral>', '<quotedStringLiteral>', 'IF'])

    def test_exists_str_literal_drop(self):
        self.trycompletions('DROP IDENTITY IF ', immediate='EXISTS ')
        self.trycompletions('DROP IDENTITY IF EXISTS ',
                            choices=['<pgStringLiteral>', '<quotedStringLiteral>'])

    def test_complete_drop_IDENTITY_statement_end(self):
        self.trycompletions("DROP IDENTITY 'alice@example.com' ", choices=[';'])
        self.trycompletions("DROP IDENTITY IF EXISTS 'alice@example.com' ", choices=[';'])