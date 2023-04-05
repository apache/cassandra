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

from cassandra.auth import PlainTextAuthProvider
from cqlshlib.authproviderhandling import load_auth_provider


def construct_config_path(config_file_name):
    return os.path.join(os.path.dirname(__file__),
                        'test_authproviderhandling_config',
                        config_file_name)


# Simple class to help verify AuthProviders that don't need arguments.
class NoUserNamePlainTextAuthProvider(PlainTextAuthProvider):
    def __init__(self):
        super(NoUserNamePlainTextAuthProvider, self).__init__('', '')


class ComplexTextAuthProvider(PlainTextAuthProvider):
    def __init__(self, username, password='default_pass', extra_flag=None):
        super(ComplexTextAuthProvider, self).__init__(username, password)
        self.extra_flag = extra_flag


def _assert_auth_provider_matches(actual, klass, expected_props):
    """
    Assert that the provider matches class and properties
    * actual ..........: Thing to compare with it
    * klass ...........: Class to ensure this matches to (ie PlainTextAuthProvider)
    * expected_props ..: Dict of var properties to match
    """
    assert isinstance(actual, klass)
    assert expected_props == vars(actual)

class CustomAuthProviderTest(unittest.TestCase):

    def setUp(self):
        self._captured_std_err = io.StringIO()
        sys.stderr = self._captured_std_err

    def tearDown(self):
        self._captured_std_err.close()
        sys.stdout = sys.__stderr__

    def test_no_warning_insecure_if_no_pass(self):
        load_auth_provider(construct_config_path('plain_text_partial_example'))
        err_msg = self._captured_std_err.getvalue()
        assert err_msg == ''

    def test_insecure_creds(self):
        load_auth_provider(construct_config_path('full_plain_text_example'))
        err_msg = self._captured_std_err.getvalue()
        assert "Notice:" in err_msg
        assert "Warning:" in err_msg

    def test_creds_not_checked_for_non_plaintext(self):
        load_auth_provider(construct_config_path('complex_auth_provider_with_pass'))
        err_msg = self._captured_std_err.getvalue()
        assert err_msg == ''

    def test_partial_property_example(self):
        actual = load_auth_provider(construct_config_path('partial_example'))
        _assert_auth_provider_matches(
                actual,
                NoUserNamePlainTextAuthProvider,
                {"username": '',
                 "password": ''})

    def test_full_property_example(self):
        actual = load_auth_provider(construct_config_path('full_plain_text_example'))
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user1',
                 "password": 'pass1'})

    def test_empty_example(self):
        actual = load_auth_provider(construct_config_path('empty_example'))
        assert actual is None

    def test_plaintextauth_when_not_defined(self):
        creds_file = construct_config_path('plain_text_full_creds')
        actual = load_auth_provider(cred_file=creds_file)
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user2',
                 "password": 'pass2'})

    def test_no_cqlshrc_file(self):
        actual = load_auth_provider()
        assert actual is None

    def test_no_classname_example(self):
        actual = load_auth_provider(construct_config_path('no_classname_example'))
        assert actual is None

    def test_improper_config_example(self):
        with pytest.raises(ModuleNotFoundError) as error:
            load_auth_provider(construct_config_path('illegal_example'))
            assert error is not None

    def test_username_password_passed_from_commandline(self):
        creds_file = construct_config_path('complex_auth_provider_creds')
        cqlshrc = construct_config_path('complex_auth_provider')

        actual = load_auth_provider(cqlshrc, creds_file, 'user-from-legacy', 'pass-from-legacy')
        _assert_auth_provider_matches(
                 actual,
                 ComplexTextAuthProvider,
                 {"username": 'user-from-legacy',
                  "password": 'pass-from-legacy',
                  "extra_flag": 'flag2'})

    def test_creds_example(self):
        creds_file = construct_config_path('complex_auth_provider_creds')
        cqlshrc = construct_config_path('complex_auth_provider')

        actual = load_auth_provider(cqlshrc, creds_file)
        _assert_auth_provider_matches(
                actual,
                ComplexTextAuthProvider,
                {"username": 'user1',
                 "password": 'pass2',
                 "extra_flag": 'flag2'})

    def test_legacy_example_use_passed_username(self):
        creds_file = construct_config_path('plain_text_partial_creds')
        cqlshrc = construct_config_path('plain_text_partial_example')

        actual = load_auth_provider(cqlshrc, creds_file, 'user3')
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user3',
                 "password": 'pass2'})

    def test_legacy_example_no_auth_provider_given(self):
        cqlshrc = construct_config_path('empty_example')
        creds_file = construct_config_path('complex_auth_provider_creds')

        actual = load_auth_provider(cqlshrc, creds_file, 'user3', 'pass3')
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user3',
                 "password": 'pass3'})

    def test_shouldnt_pass_no_password_when_alt_auth_provider(self):
        cqlshrc = construct_config_path('complex_auth_provider')
        creds_file = None

        actual = load_auth_provider(cqlshrc, creds_file, 'user3')
        _assert_auth_provider_matches(
                actual,
                ComplexTextAuthProvider,
                {"username": 'user3',
                 "password": 'default_pass',
                 "extra_flag": 'flag1'})

    def test_legacy_example_no_password(self):
        cqlshrc = construct_config_path('plain_text_partial_example')
        creds_file = None

        actual = load_auth_provider(cqlshrc, creds_file, 'user3')
        _assert_auth_provider_matches(
                actual,
                PlainTextAuthProvider,
                {"username": 'user3',
                 "password": None})
