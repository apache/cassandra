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

from cql.marshal import uint16_unpack
from cql.cqltypes import CompositeType
from formatting import formatter_for, format_value_utype


def get_field_names(ks_name, ut_name):
    """
    UserTypes will use this function to get its fields names from Shell's utypes_dict
    """
    raise NotImplementedError("this function shall be overloaded by Shell")


class UserType(CompositeType):
    typename = 'UserType'

    @classmethod
    def apply_parameters(cls, *subtypes):
        ksname = subtypes[0].cassname
        newname = subtypes[1].cassname.decode("hex")
        field_names = get_field_names(ksname, newname)
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
        result = []
        for col_name, col_type in zip(cls.fieldnames, cls.subtypes):
            if p == len(byts):
                break
            itemlen = uint16_unpack(byts[p:p + 2])
            p += 2
            item = byts[p:p + itemlen]
            p += itemlen
            result.append((str(col_name), col_type.from_binary(item)))
            p += 1

        return result



