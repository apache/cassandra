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

from .basecase import BaseTestCase, cqlsh
from .cassconnect import get_test_keyspace, testrun_cqlsh, testcall_cqlsh

class TestCqlParsing(BaseTestCase):
    def setUp(self):
        self.cqlsh_runner = testrun_cqlsh(cqlver=cqlsh.DEFAULT_CQLVER, env={'COLUMNS': '100000'})
        self.cqlsh = self.cqlsh_runner.__enter__()

    def tearDown(self):
        pass

    def test_parse_string_literals(self):
        pass

    def test_parse_numbers(self):
        pass

    def test_parse_uuid(self):
        pass

    def test_comments_in_string_literals(self):
        pass

    def test_colons_in_string_literals(self):
        pass

    def test_partial_parsing(self):
        pass

    def test_parse_select(self):
        pass

    def test_parse_insert(self):
        pass

    def test_parse_update(self):
        pass

    def test_parse_delete(self):
        pass

    def test_parse_batch(self):
        pass

    def test_parse_create_keyspace(self):
        pass

    def test_parse_drop_keyspace(self):
        pass

    def test_parse_create_columnfamily(self):
        pass

    def test_parse_drop_columnfamily(self):
        pass

    def test_parse_truncate(self):
        pass

    def test_parse_alter_columnfamily(self):
        pass

    def test_parse_use(self):
        pass

    def test_parse_create_index(self):
        pass

    def test_parse_drop_index(self):
        pass

    def test_parse_select_token(self):
        self.cqlsh.cmd_and_response('INSERT INTO has_all_types (num) VALUES (1);')
        response = self.cqlsh.cmd_and_response("SELECT token(num) from has_all_types where num=1;")
        self.assertIn('-4069959284402364209', response)
