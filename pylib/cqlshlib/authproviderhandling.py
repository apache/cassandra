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

import configparser
from importlib import import_module

def load_custom_auth_provider(config_file = None):
    """
    Function which loads a custom auth provider from available config.

    Params:
    * config_file ..: path to cqlsh config file (usually ~/.cqlshrc).

    Will attempt to load a custom auth provider from available config file.
    
    Config file is expected to list module name /class in the *auth_provider*
    section for dynamic loading (which is to be of type auth_provider)
    
    Additional params passed to the constructor of class should be specified
    in the *auth_provider_config* section and can be freely named to match 
    custom provider's expectation.

    None is returned if no custom auth provider is found.

    EXAMPLE  CQLSHRC: 
    # .. inside cqlshrc file
    
    [auth_provider]
    module = cassandra.auth
    classname = PlainTextAuthProvider
    
    [auth_provider_config]
    username = user1
    password = password1
    """

    configs = configparser.ConfigParser()
    configs.read(config_file)

    def get_setting(section, option):
        try:
            return configs.get(section, option)
        except configparser.Error:
            return None
    
    def get_auth_provider_config_settings():
        if 'auth_provider_config' in configs.sections():
            return dict(configs.items("auth_provider_config"))
        else:
            return None
    
    module_name = get_setting('auth_provider', 'module')
    class_name = get_setting('auth_provider', 'classname')
    if module_name is None:
        return None
    ctor_args = get_auth_provider_config_settings()

    # Load class definitions
    module = import_module(module_name)
    auth_provider_klass = getattr(module, class_name)

    # instantiate the class
    if ctor_args is not None:
        return auth_provider_klass(**ctor_args)
    
    return auth_provider_klass()
