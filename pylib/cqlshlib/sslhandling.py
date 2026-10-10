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

import ipaddress
import os
import sys
import ssl

import configparser

SSL_MODES = ('disable', 'require', 'verify-ca', 'verify-identity')


def ssl_settings(host, config_file, env=os.environ, ssl_mode=None):
    """
    Function which generates SSL setting for cassandra.Cluster

    Params:
    * host .........: hostname of Cassandra node.
    * env ..........: environment variables. SSL factory will use, if passed,
                      SSL_CERTFILE, SSL_MODE and SSL_VALIDATE variables.
    * config_file ..: path to cqlsh config file (usually ~/.cqlshrc).
                      SSL factory will use, if set, certfile, ssl_mode and
                      validate options in [ssl] section, as well as host to
                      certfile mapping in [certfiles] section.
    * ssl_mode .....: --ssl-mode command line value; overrides SSL_MODE and
                      ssl_mode in config file.

    ssl_mode is one of disable, require, verify-ca or verify-identity. When it
    is not set, validate=false maps to require and anything else to verify-ca.
    [certfiles] section is optional, 'ssl_mode' and 'validate' settings in
    [ssl] section are optional too. If validation is enabled then SSL certfile
    must be provided either in the config file or as an environment variable.
    Environment variables override any options set in cqlsh config file.
    SNI is sent with the host name unless host is an IP address.
    """
    configs = configparser.ConfigParser()
    configs.read(config_file)

    def get_option(section, option):
        try:
            return configs.get(section, option)
        except configparser.Error:
            return None

    def get_best_tls_protocol(ssl_ver_str):
        if ssl_ver_str:
            print("Warning: Explicit SSL and TLS versions in the cqlshrc file or in SSL_VERSION environment property are ignored as the protocol is auto-negotiated.\n")
        return ssl.PROTOCOL_TLS

    if ssl_mode is None:
        ssl_mode = env.get('SSL_MODE')
    if ssl_mode is None:
        ssl_mode = get_option('ssl', 'ssl_mode')
    if ssl_mode is None:
        ssl_validate = env.get('SSL_VALIDATE')
        if ssl_validate is None:
            ssl_validate = get_option('ssl', 'validate')
        ssl_validate = ssl_validate is None or ssl_validate.lower() != 'false'
        ssl_mode = 'verify-ca' if ssl_validate else 'require'
    ssl_mode = ssl_mode.lower()
    if ssl_mode not in SSL_MODES:
        sys.exit("Invalid ssl_mode '%s'; must be one of: %s." % (ssl_mode, ', '.join(SSL_MODES)))
    if ssl_mode == 'disable':
        sys.exit("ssl_mode is 'disable' but SSL is enabled (--ssl or 'ssl = true' in [connection] "
                 "section of %s). Remove one of them, or choose another ssl_mode." % (config_file,))
    ssl_validate = ssl_mode != 'require'

    ssl_version_str = env.get('SSL_VERSION')
    if ssl_version_str is None:
        ssl_version_str = get_option('ssl', 'version')

    ssl_version = get_best_tls_protocol(ssl_version_str)

    ssl_certfile = env.get('SSL_CERTFILE')
    if ssl_certfile is None:
        ssl_certfile = get_option('certfiles', host)
    if ssl_certfile is None:
        ssl_certfile = get_option('ssl', 'certfile')
    if ssl_validate and ssl_certfile is None:
        sys.exit("Validation is enabled; SSL transport factory requires a valid certfile "
                 "to be specified. Please provide path to the certfile in [ssl] section "
                 "as 'certfile' option in %s (or use [certfiles] section) or set SSL_CERTFILE "
                 "environment variable." % (config_file,))
    if ssl_certfile is not None:
        ssl_certfile = os.path.expanduser(ssl_certfile)

    userkey = get_option('ssl', 'userkey')
    if userkey:
        userkey = os.path.expanduser(userkey)
    usercert = get_option('ssl', 'usercert')
    if usercert:
        usercert = os.path.expanduser(usercert)

    ssl_options = dict(ca_certs=ssl_certfile,
                       cert_reqs=ssl.CERT_REQUIRED if ssl_validate else ssl.CERT_NONE,
                       ssl_version=ssl_version,
                       keyfile=userkey, certfile=usercert)
    if ssl_mode == 'verify-identity':
        ssl_options['check_hostname'] = True
    # RFC 6066 forbids IP literals in SNI; with check_hostname the driver then matches the IP itself
    try:
        ipaddress.ip_address(host)
    except ValueError:
        ssl_options['server_hostname'] = host
    return ssl_options
