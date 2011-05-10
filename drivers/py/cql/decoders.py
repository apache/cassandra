
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

import cql
from marshal import (unmarshallers, unmarshal_noop)

class SchemaDecoder(object):
    """
    Decode binary column names/values according to schema.
    """
    def __init__(self, schema={}):
        self.schema = schema

    def __get_column_family_def(self, keyspace, column_family):
        if keyspace in self.schema and column_family in self.schema[keyspace]:
            return self.schema[keyspace][column_family]
        return None

    def __comparator_for(self, keyspace, column_family):
        cfam = self.__get_column_family_def(keyspace, column_family)
        if "comparator" in cfam:
            return cfam["comparator"]
        return None

    def __validator_for(self, keyspace, column_family, name):
        cfam = self.__get_column_family_def(keyspace, column_family)
        if name in cfam["columns"]:
            return cfam["columns"][name]
        return cfam["default_validation_class"]

    def __keytype_for(self, keyspace, column_family):
        cfam = self.__get_column_family_def(keyspace, column_family)
        if "key_validation_class" in cfam:
            return cfam["key_validation_class"]
        return None

    def decode_description(self, keyspace, column_family, row):
        description = []
        comparator = self.__comparator_for(keyspace, column_family)
        unmarshal = unmarshallers.get(comparator, unmarshal_noop)
        for column in row.columns:
            if column.name == self.__get_column_family_def(keyspace, column_family)['key_alias']:
                description.append((column.name, 'text', None, None, None, None, True))
            else:
                description.append((unmarshal(column.name), comparator, None, None, None, None, True))
        return description

    def decode_row(self, keyspace, column_family, row):
        comparator = self.__comparator_for(keyspace, column_family)
        unmarshal = unmarshallers.get(comparator, unmarshal_noop)
        values = []
        for column in row.columns:
            if column.value is None:
                values.append(None)
                continue
            if column.name == self.__get_column_family_def(keyspace, column_family)['key_alias']:
                validator = self.__keytype_for(keyspace, column_family)
            else:
                validator = self.__validator_for(keyspace, column_family, column.name)
            values.append(unmarshallers.get(validator, unmarshal_noop)(column.value))
        return values
