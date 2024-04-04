#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import unittest
import io
import os
import sys
import pytest
from unittest.mock import patch

from cassandra.auth import PlainTextAuthProvider
import cqlshlib.authproviderhandling as auth_prov
from cqlshlib.test.test_authproviderhandling import construct_config_path, _assert_auth_provider_matches


class CustomAuthLegacyTest(unittest.TestCase):

    def setUp(self):
        self._captured_std_err = io.StringIO()
        sys.stderr = self._captured_std_err
        self.fake_is_file_secure = lambda filename: True
        self.patcher = patch('cqlshlib.util.is_file_secure', self.fake_is_file_secure)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self._captured_std_err.close()
        sys.stdout = sys.__stderr__

    def test_legacy_credentials(self):
        from cqlshlib.util import is_file_secure
        from bin.cqlsh import read_options as cqlsh_read_options

        creds_file = construct_config_path('plain_text_legacy')
        opts, _, _ = cqlsh_read_options(['--credentials='+creds_file], {})
        actual = auth_prov.load_auth_provider(cred_file=creds_file, username=opts.username, password=opts.password)
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user4',
                "password": 'pass4'})
        