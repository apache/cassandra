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

from cassandra.marshal import uint16_unpack
from cassandra.cqltypes import CompositeType
import collections
from formatting import formatter_for, format_value_utype

class UserType(CompositeType):
    typename = "'org.apache.cassandra.db.marshal.UserType'"

    @classmethod
    def apply_parameters(cls, subtypes, names):
        newname = subtypes[1].cassname.decode("hex")
        field_names = [encoded_name.decode("hex") for encoded_name in names[2:]]
        assert len(field_names) == len(subtypes[2:])
        formatter_for(newname)(format_value_utype)
        return type(newname, (cls,), {'subtypes': subtypes[2:],
                                      'cassname': cls.cassname, 'typename': newname, 'fieldnames': field_names})

    @classmethod
    def cql_parameterized_type(cls):
        return cls.typename

    @classmethod
    def deserialize_safe(cls, byts):
        p = 0
        Result = collections.namedtuple(cls.typename, cls.fieldnames)
        result = []
        for col_type in cls.subtypes:
            if p == len(byts):
                break
            itemlen = uint16_unpack(byts[p:p + 2])
            p += 2
            item = byts[p:p + itemlen]
            p += itemlen
            result.append(col_type.from_binary(item))
            p += 1

        if len(result) < len(cls.subtypes):
            nones = [None] * (len(cls.subtypes) - len(result))
            result = result + nones

        return Result(*result)



