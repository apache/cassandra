
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

from os.path import abspath, dirname, join
from marshal import unmarshal

class BaseDecoder(object):
    def decode_column(self, keyspace, column_family, name, value):
        raise NotImplementedError()
    
class NoopDecoder(BaseDecoder):
    def decode_column(self, keyspace, column_family, name, value):
        return (name, value)

class SchemaDecoder(BaseDecoder):
    """
    Decode binary column names/values according to schema.
    """
    def __init__(self, schema={}):
        self.schema = schema
        
    def __get_column_family_def(self, keyspace, column_family):
        if self.schema.has_key(keyspace):
            if self.schema[keyspace].has_key(column_family):
                return self.schema[keyspace][column_family]
        return None
    
    def __comparator_for(self, keyspace, column_family):
        cfam = self.__get_column_family_def(keyspace, column_family)
        if cfam and cfam.has_key("comparator"):
            return cfam["comparator"]
        return None
    
    def __validator_for(self, keyspace, column_family, name):
        cfam = self.__get_column_family_def(keyspace, column_family)
        if cfam:
            if cfam["columns"].has_key(name):
                return cfam["columns"][name]
            else:
                return cfam["default_validation_class"]
        return None

    def decode_column(self, keyspace, column_family, name, value):
        comparator = self.__comparator_for(keyspace, column_family)
        validator = self.__validator_for(keyspace, column_family, name)
        return (unmarshal(name, comparator), unmarshal(value, validator))
    
