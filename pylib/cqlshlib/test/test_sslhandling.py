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

from cassandra.policies import SimpleConvictionPolicy
from cassandra.pool import Host
from cqlshlib.sslhandling import ssl_settings
from nose.tools import assert_raises

import unittest
import os
import ssl

class SslSettingsTest(unittest.TestCase):

    def setUp(self):
        os.environ['SSL_VALIDATE'] = 'False'
        self.config_file = 'test_config'
        self.host = Host('10.0.0.1', SimpleConvictionPolicy, 9000)

    def tearDown(self):
        del os.environ['SSL_VALIDATE']
        try:
            del os.environ['SSL_VERSION']
        except KeyError:
            pass

    def _test_ssl_version_from_env(self, version):
        """
        Getting SSL version string from env variable SSL_VERSION.
        """
        os.environ['SSL_VERSION'] = version
        ssl_ret_val = ssl_settings(self.host, self.config_file)
        assert ssl_ret_val is not None
        assert ssl_ret_val.get('ssl_version') == getattr(ssl, 'PROTOCOL_%s' % version)

    def test_ssl_versions_from_env(self):
        versions = ['TLS', 'TLSv1_1', 'TLSv1_2', 'TLSv1']
        for version in versions:
            self._test_ssl_version_from_env(version)

    def test_invalid_ssl_versions_from_env(self):
        msg = "invalid_ssl is not a valid SSL protocol, please use one of TLSv1, TLSv1_1, or TLSv1_2"
        with assert_raises(SystemExit) as error:
            self._test_ssl_version_from_env('invalid_ssl')
            assert msg == error.exception.message

    def test_default_ssl_version(self):
        ssl_ret_val = ssl_settings(self.host, self.config_file)
        assert ssl_ret_val is not None
        assert ssl_ret_val.get('ssl_version') == getattr(ssl, 'PROTOCOL_TLS')

    def test_ssl_version_config(self):
        ssl_ret_val = ssl_settings(self.host, os.path.join('test', 'config', 'sslhandling.config'))
        assert ssl_ret_val is not None
        assert ssl_ret_val.get('ssl_version') == getattr(ssl, 'PROTOCOL_TLSv1')

    def test_invalid_ssl_version_config(self):
        msg = "invalid_ssl is not a valid SSL protocol, please use one of TLSv1, TLSv1_1, or TLSv1_2"
        with assert_raises(SystemExit) as error:
            ssl_settings(self.host, os.path.join('test', 'config', 'sslhandling_invalid.config'))
            assert msg in error.exception.message
            