# coding=utf-8
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

from __future__ import unicode_literals, with_statement

import os
import subprocess

from .basecase import BaseTestCase
from .cassconnect import (get_cassandra_connection, create_keyspace, testrun_cqlsh)


class TestCqlshUnicode(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        s = get_cassandra_connection().connect()
        s.default_timeout = 60.0
        create_keyspace(s)
        s.execute('CREATE TABLE t (k int PRIMARY KEY, v text)')

        env = os.environ.copy()
        env['LC_CTYPE'] = 'UTF-8'
        cls.default_env = env

    def test_unicode_value_round_trip(self):
        with testrun_cqlsh(tty=True, env=self.default_env) as c:
            value = 'ϑΉӁװڜ'
            c.cmd_and_response("INSERT INTO t(k, v) VALUES (1, '%s');" % (value,))
            output = c.cmd_and_response('SELECT * FROM t;')
            self.assertIn(value, output)

    def test_unicode_identifier(self):
        col_name = 'テスト'
        with testrun_cqlsh(tty=True, env=self.default_env) as c:
            c.cmd_and_response('ALTER TABLE t ADD "%s" int;' % (col_name,))
            # describe command reproduces name
            output = c.cmd_and_response('DESC t')
            self.assertIn('"%s" int' % (col_name,), output)
            c.cmd_and_response("INSERT INTO t(k, v) VALUES (1, '値');")
            # results header reproduces name
            output = c.cmd_and_response('SELECT * FROM t;')
            self.assertIn(col_name, output)

    def test_unicode_multiline_input(self):  # CASSANDRA-16400
        with testrun_cqlsh(tty=True, env=self.default_env) as c:
            value = '値'
            c.send("INSERT INTO t(k, v) VALUES (1, \n'%s');\n" % (value,))
            c.read_to_next_prompt()
            output = c.cmd_and_response('SELECT v FROM t;')
            self.assertIn(value, output)

    def test_unicode_desc(self):  # CASSANDRA-16539
        with testrun_cqlsh(tty=True, env=self.default_env) as c:
            v1 = 'ࠑ'
            v2 = 'Ξ'
            output = c.cmd_and_response('CREATE TYPE "%s" ( "%s" int );' % (v1, v2))
            output = c.cmd_and_response('DESC TYPES;')
            self.assertIn(v1, output)
            output = c.cmd_and_response('DESC TYPE "%s";' %(v1,))
            self.assertIn(v2, output)
