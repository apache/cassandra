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

import pytest
import unittest
import os

from cassandra.auth import PlainTextAuthProvider
from cqlshlib.authproviderhandling import load_custom_auth_provider

def construct_config_path(config_file_name):
    return os.path.join(os.path.dirname(__file__), 'test_authproviderhandling_config', config_file_name)

# Simple class to help verify AuthProviders that don't need arguments.
class NoUserNamePlainTextAuthProvider(PlainTextAuthProvider):
    def __init__(self):
        super(NoUserNamePlainTextAuthProvider, self).__init__('', '')

class CustomAuthProviderTest(unittest.TestCase):
    
    def _getattr_if_exists(self, obj, attribute):
        """
        Return the value of the string attribute given
        returns None if not found.
        """
        return getattr(obj, attribute) if hasattr(obj, attribute) else None

    def _assert_plain_text_auth_provider_match(self, actual, exp_user, exp_pass):
        """
        Asserts that actual is a PlainTextAuthProvider with given username and password.
        """
        assert isinstance(actual, PlainTextAuthProvider)
        username = self._getattr_if_exists(actual, 'username')
        assert username == exp_user
        
        password = self._getattr_if_exists(actual, 'password')
        assert password == exp_pass

    def test_partial_property_example(self):
        actual = load_custom_auth_provider(construct_config_path('partial_example'))
        self._assert_plain_text_auth_provider_match(actual=actual, exp_user='', exp_pass='')

    def test_full_property_example(self):
        actual = load_custom_auth_provider(construct_config_path('full_plain_text_example'))
        self._assert_plain_text_auth_provider_match(actual, exp_user='user1', exp_pass='pass1')
    
    def test_empty_example(self):
        actual = load_custom_auth_provider(construct_config_path('empty_example'))
        assert actual is None

    def test_no_classname_example(self):
        actual = load_custom_auth_provider(construct_config_path('no_classname_example'))
        assert actual is None
    def test_improper_config_example(self):
        with pytest.raises(ModuleNotFoundError) as error:
            actual = load_custom_auth_provider(construct_config_path('illegal_example'))
            assert error is not None
