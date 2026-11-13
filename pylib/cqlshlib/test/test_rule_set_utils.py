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

from .basecase import BaseTestCase
from cqlshlib.cql3handling import CqlRuleSet


class TestRuleSetUtils(BaseTestCase):
    def test_dequote_value(self):
        self.assertEqual(CqlRuleSet.dequote_value("'test'"), "test")
        self.assertEqual(CqlRuleSet.dequote_value("'test''val'"), "test'val")
        self.assertEqual(CqlRuleSet.dequote_value("unquoted"), "unquoted")
        self.assertEqual(CqlRuleSet.dequote_value(""), "")

    def test_dequote_name(self):
        self.assertEqual(CqlRuleSet.dequote_name('"TestName"'), "TestName")
        self.assertEqual(CqlRuleSet.dequote_name('"Test""Name"'), 'Test"Name')
        self.assertEqual(CqlRuleSet.dequote_name("LowercaseName"), "lowercasename")
        self.assertEqual(CqlRuleSet.dequote_name(""), "")

    def test_escape_value(self):
        self.assertEqual(CqlRuleSet.escape_value("val'with'quote"), "'val''with''quote'")
        self.assertEqual(CqlRuleSet.escape_value(True), "'true'")
        self.assertEqual(CqlRuleSet.escape_value(123), "123")
        self.assertEqual(CqlRuleSet.escape_value(1.23), "1.230000")
        self.assertEqual(CqlRuleSet.escape_value(None), "NULL")

    def test_escape_name(self):
        self.assertEqual(CqlRuleSet.escape_name('name"with"quote'), '"name""with""quote"')
        self.assertEqual(CqlRuleSet.dequote_name(CqlRuleSet.escape_name('name"with"quote')), 'name"with"quote')
        self.assertEqual(CqlRuleSet.escape_name(None), "NULL")
