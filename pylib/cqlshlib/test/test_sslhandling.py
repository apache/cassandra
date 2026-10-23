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

from cqlshlib.sslhandling import ssl_settings

CERTFILE = '/path/to/ca.pem'


class SslSettingsTest(unittest.TestCase):

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._dir.cleanup()

    def settings(self, host='node1.example.com', env=None, **ssl_section):
        ssl_section.setdefault('certfile', CERTFILE)
        path = os.path.join(self._dir.name, 'cqlshrc')
        with open(path, 'w') as f:
            f.write('[ssl]\n')
            for k, v in ssl_section.items():
                f.write('%s = %s\n' % (k, v))
        return ssl_settings(host, path, env=env or {})

    def test_settings_unchanged_apart_from_sni(self):
        expected = dict(ca_certs=CERTFILE, cert_reqs=ssl.CERT_REQUIRED, ssl_version=ssl.PROTOCOL_TLS,
                        keyfile=None, certfile=None, server_hostname='node1.example.com')
        assert self.settings() == expected
        expected['cert_reqs'] = ssl.CERT_NONE
        assert self.settings(validate='false') == expected

    def test_sni_with_and_without_validation(self):
        for validate in ('true', 'false'):
            assert self.settings(validate=validate)['server_hostname'] == 'node1.example.com'
            assert self.settings(env={'SSL_VALIDATE': validate})['server_hostname'] == 'node1.example.com'

    def test_no_sni_for_ip_literals(self):
        for host in ('127.0.0.1', '::1', '2001:db8::1'):
            for validate in ('true', 'false'):
                assert 'server_hostname' not in self.settings(host=host, validate=validate)
