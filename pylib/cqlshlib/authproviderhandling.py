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
"""
Handles loading of AuthProvider for CQLSH authentication.
"""

import configparser
from importlib import import_module


def load_auth_provider(config_file=None, cred_file=None, username=None, password=None):
    """
    Function which loads an auth provider from available config.

    Params:
    * config_file ..: path to cqlsh config file (usually ~/.cassandra/cqlshrc).
    * cred_file ....: path to cqlsh credentials file (default is  ~/.cassandra/credentials).
    * username .....: override used to return PlainTextAuthProvider according to legacy case
    * password .....: override used to return PlainTextAuthProvider according to legacy case

    Will attempt to load an auth provider from available config file, using what's found in
    credentials file as an override.

    Config file is expected to list module name /class in the *auth_provider*
    section for dynamic loading (which is to be of type auth_provider)

    Additional params passed to the constructor of class should be specified
    in the *auth_provider* section and can be freely named to match
    auth provider's expectation.

    If passed username and password, function will return a PlainTextAuthProvider using the
    traditional logic.  It will try to use properties specified in [Auth_provider] section if
    the PlainTextAuthProvider is the specified class.

    None is returned if no possible auth provider is found.

    EXAMPLE  CQLSHRC:
    # .. inside cqlshrc file

    [auth_provider]
    module = cassandra.auth
    classname = PlainTextAuthProvider
    username = user1
    password = password1

    if credentials file is specified put relevant properties under the class name
    EXAMPLE
    # ... inside credentials file for above example
    [PlainTextAuthProvider]
    password = password2

    Credential attributes will override found in the cqlshrc.
    in the above example, PlainTextAuthProvider would be used with a password of 'password2',
    and username of 'user1'
    """

    def get_settings_from_config(section_name,
                                 conf_file,
                                 interpolation=configparser.BasicInterpolation()):
        """
        Returns dict from section_name, and ini based conf_file

        * section_name ..: Section to read map of properties from (ex: [auth_provider])
        * conf_file .....: Ini based config file to read.  Will return empty dict if None.
        * interpolation .: Interpolation to use.

        If section is not found, or conf_file is None, function will return an empty dictionary.
        """
        conf = configparser.ConfigParser(interpolation=interpolation)
        if conf_file is None:
            return {}

        conf.read(conf_file)
        if section_name in conf.sections():
            return dict(conf.items(section_name))
        return {}

    def get_cred_file_settings(classname, creds_file):
        # Since this is the credentials file we may be encountering raw strings
        # as these are what passwords, or security tokens may inadvertently fall into
        # we don't want interpolation to mess with them.
        return get_settings_from_config(
                section_name=classname,
                conf_file=creds_file,
                interpolation=None)

    def get_auth_provider_settings(conf_file):
        return get_settings_from_config(
                section_name='auth_provider',
                conf_file=conf_file)

    def get_legacy_settings(legacy_username, legacy_password):
        result = {}
        if legacy_username is not None:
            result['username'] = legacy_username
        if legacy_password is not None:
            result['password'] = legacy_password
        return result

    provider_settings = get_auth_provider_settings(config_file)
    module_name = provider_settings.pop('module', None)
    class_name = provider_settings.pop('classname', None)

    # if a legacy username or password is passed to us
    # regardless of what the module / class is specified, we have been overridden to using
    # PlainTextAuthProvider
    if username is not None or password is not None:
        module_name = 'cassandra.auth'
        class_name = 'PlainTextAuthProvider'

    if module_name is None or class_name is None:
        return None

    credential_settings = get_cred_file_settings(class_name, cred_file)

    if module_name == 'cassandra.auth' and class_name == 'PlainTextAuthProvider':
        # merge credential settings as overrides on top of provider settings.

        # we need to ensure that password property gets "set" in all cases.
        # this is to support the ability to give the user a prompt in other parts
        # of the code.
        ctor_args = {'password': None,
                     **provider_settings,
                     **credential_settings,
                     **get_legacy_settings(username, password)}
    else:
        # merge credential settings as overrides on top of provider settings.
        ctor_args = {**provider_settings, **credential_settings}

    # Load class definitions
    module = import_module(module_name)
    auth_provider_klass = getattr(module, class_name)

    # instantiate the class
    return auth_provider_klass(**ctor_args)
