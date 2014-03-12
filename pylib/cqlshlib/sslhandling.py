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

import os
import sys
import ConfigParser
import ssl

def ssl_settings(host, config_file, env=os.environ):
    """
    Function wcich generates SSL setting for cassandra.Cluster

    Params:
    * host .........: hostname of Cassandra node.
    * env ..........: environment variables. SSL factory will use, if passed,
                      SSL_CERTFILE and SSL_VALIDATE variables.
    * config_file ..: path to cqlsh config file (usually ~/.cqlshrc).
                      SSL factory will use, if set, certfile and validate
                      options in [ssl] section, as well as host to certfile
                      mapping in [certfiles] section.

    [certfiles] section is optional, 'validate' setting in [ssl] section is
    optional too. If validation is enabled then SSL certfile must be provided
    either in the config file or as an environment variable.
    Environment variables override any options set in cqlsh config file.
    """
    configs = ConfigParser.SafeConfigParser()
    configs.read(config_file)

    def get_option(section, option):
        try:
            return configs.get(section, option)
        except ConfigParser.Error:
            return None

    ssl_validate = env.get('SSL_VALIDATE')
    if ssl_validate is None:
        ssl_validate = get_option('ssl', 'validate')
    ssl_validate = ssl_validate is None or ssl_validate.lower() != 'false'

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
    if not ssl_certfile is None:
        ssl_certfile = os.path.expanduser(ssl_certfile)

    userkey = get_option('ssl', 'userkey')
    if userkey:
        userkey = os.path.expanduser(userkey)
    usercert = get_option('ssl', 'usercert')
    if usercert:
        usercert = os.path.expanduser(usercert)

    return dict(ca_certs=ssl_certfile,
                cert_reqs=ssl.CERT_REQUIRED if ssl_validate else ssl.CERT_NONE,
                ssl_version=ssl.PROTOCOL_TLSv1,
                keyfile=userkey, certfile=usercert)

