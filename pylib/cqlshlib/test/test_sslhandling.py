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

import os
import ssl
import tempfile
import unittest

import pytest

from cqlshlib.sslhandling import ssl_settings

CERTFILE = '/path/to/ca.pem'


class SslSettingsTest(unittest.TestCase):

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._dir.cleanup()

    def cqlshrc(self, **ssl_section):
        path = os.path.join(self._dir.name, 'cqlshrc')
        with open(path, 'w') as f:
            f.write('[ssl]\n')
            for k, v in ssl_section.items():
                f.write('%s = %s\n' % (k, v))
        return path

    def settings(self, host='node1.example.com', env=None, cli=None, **ssl_section):
        """cli is the --ssl-mode value; ssl_section holds cqlshrc [ssl] options."""
        ssl_section.setdefault('certfile', CERTFILE)
        return ssl_settings(host, self.cqlshrc(**ssl_section), env=env or {}, ssl_mode=cli)

    def assert_mode(self, opts, cert_reqs, check_hostname):
        assert opts['cert_reqs'] == cert_reqs
        assert opts.get('check_hostname', False) is check_hostname

    def test_require(self):
        self.assert_mode(self.settings(cli='require'), ssl.CERT_NONE, False)

    def test_verify_ca(self):
        opts = self.settings(cli='verify-ca')
        self.assert_mode(opts, ssl.CERT_REQUIRED, False)
        assert opts['ca_certs'] == CERTFILE

    def test_verify_identity(self):
        self.assert_mode(self.settings(cli='verify-identity'), ssl.CERT_REQUIRED, True)

    def test_mode_is_case_insensitive(self):
        self.assert_mode(self.settings(env={'SSL_MODE': 'Verify-Identity'}), ssl.CERT_REQUIRED, True)

    def test_unset_mode_keeps_validate_behaviour(self):
        expected = dict(ca_certs=CERTFILE, cert_reqs=ssl.CERT_REQUIRED, ssl_version=ssl.PROTOCOL_TLS,
                        keyfile=None, certfile=None, server_hostname='node1.example.com')
        assert self.settings() == expected
        assert self.settings(validate='true') == expected
        assert self.settings(env={'SSL_VALIDATE': 'true'}, validate='false') == expected
        expected['cert_reqs'] = ssl.CERT_NONE
        assert self.settings(validate='false') == expected
        assert self.settings(env={'SSL_VALIDATE': 'false'}) == expected

    def test_mode_wins_over_validate(self):
        self.assert_mode(self.settings(validate='false', ssl_mode='verify-ca'), ssl.CERT_REQUIRED, False)
        self.assert_mode(self.settings(env={'SSL_VALIDATE': 'true'}, ssl_mode='require'), ssl.CERT_NONE, False)
        self.assert_mode(self.settings(env={'SSL_VALIDATE': 'false', 'SSL_MODE': 'verify-identity'}),
                         ssl.CERT_REQUIRED, True)
        self.assert_mode(self.settings(validate='true', cli='require'), ssl.CERT_NONE, False)

    def test_precedence_cli_over_env_over_cqlshrc(self):
        self.assert_mode(self.settings(ssl_mode='verify-identity'), ssl.CERT_REQUIRED, True)
        self.assert_mode(self.settings(env={'SSL_MODE': 'require'}, ssl_mode='verify-identity'),
                         ssl.CERT_NONE, False)
        self.assert_mode(self.settings(env={'SSL_MODE': 'require'}, cli='verify-ca', ssl_mode='verify-identity'),
                         ssl.CERT_REQUIRED, False)

    def test_sni_for_hostname_at_every_mode(self):
        for mode in ('require', 'verify-ca', 'verify-identity'):
            assert self.settings(cli=mode)['server_hostname'] == 'node1.example.com'

    def test_no_sni_for_ip_literals(self):
        for host in ('127.0.0.1', '::1', '2001:db8::1'):
            for mode in ('require', 'verify-ca', 'verify-identity'):
                assert 'server_hostname' not in self.settings(host=host, cli=mode)

    def test_disable_is_an_error(self):
        for kwargs in ({'cli': 'disable'}, {'env': {'SSL_MODE': 'disable'}}, {'ssl_mode': 'disable'}):
            with pytest.raises(SystemExit, match="ssl_mode is 'disable' but SSL is enabled"):
                self.settings(**kwargs)

    def test_invalid_mode_is_an_error(self):
        for kwargs in ({'env': {'SSL_MODE': 'bogus'}}, {'ssl_mode': 'bogus'}):
            with pytest.raises(SystemExit, match='must be one of: disable, require, verify-ca, verify-identity'):
                self.settings(**kwargs)

    def test_validation_without_certfile_is_an_error(self):
        for kwargs in ({}, {'ssl_mode': 'verify-ca'}, {'ssl_mode': 'verify-identity'}):
            with pytest.raises(SystemExit, match='Validation is enabled'):
                ssl_settings('node1.example.com', self.cqlshrc(), env={}, **kwargs)
        assert ssl_settings('node1.example.com', self.cqlshrc(), env={}, ssl_mode='require')['ca_certs'] is None
