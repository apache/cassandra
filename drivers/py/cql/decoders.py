
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
    def __init__(self, schema):
        self.schema = schema

    def decode_description(self, row):
        schema = self.schema
        description = []
        for column in row.columns:
            name = column.name
            comparator = schema.name_types.get(name, schema.default_name_type)
            unmarshal = unmarshallers.get(comparator, unmarshal_noop)
            description.append((unmarshal(name), comparator, None, None, None, None, True))
        return description

    def decode_row(self, row):
        schema = self.schema
        values = []
        for column in row.columns:
            if column.value is None:
                values.append(None)
                continue

            name = column.name
            validator = schema.value_types.get(name, schema.default_value_type)
            unmarshal = unmarshallers.get(validator, unmarshal_noop)
            values.append(unmarshallers.get(validator, unmarshal_noop)(column.value))

        return values
