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

from cassandra.marshal import int32_unpack, uint16_unpack
from cassandra.cqltypes import CompositeType
import collections
from formatting import formatter_for, format_value_utype

class UserType(CompositeType):
    typename = "'org.apache.cassandra.db.marshal.UserType'"

    FIELD_LENGTH = 4

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
            itemlen = int32_unpack(byts[p:p + cls.FIELD_LENGTH])
            p += cls.FIELD_LENGTH
            if itemlen < 0:
                result.append(None)
            else:
                item = byts[p:p + itemlen]
                p += itemlen
                result.append(col_type.from_binary(item))

        if len(result) < len(cls.subtypes):
            nones = [None] * (len(cls.subtypes) - len(result))
            result = result + nones

        return Result(*result)

def deserialize_safe_collection(cls, byts):
    """
        Temporary work around for CASSANDRA-7267
    """
    subtype, = cls.subtypes
    unpack = uint16_unpack
    length = 2
    numelements = unpack(byts[:length])
    if numelements == 0 and len(byts) > 2 :
        unpack = int32_unpack
        length = 4
        numelements = unpack(byts[:length])
    p = length
    result = []
    for n in xrange(numelements):
        itemlen = unpack(byts[p:p + length])
        p += length
        item = byts[p:p + itemlen]
        p += itemlen
        result.append(subtype.from_binary(item))
    return cls.adapter(result)

try:
    from collections import OrderedDict
except ImportError:  # Python <2.7
    from cassandra.util import OrderedDict

def deserialize_safe_map(cls, byts):
    """
        Temporary work around for CASSANDRA-7267
    """
    subkeytype, subvaltype = cls.subtypes
    unpack = uint16_unpack
    length = 2
    numelements = unpack(byts[:length])
    if numelements == 0 and len(byts) > 2:
        unpack = int32_unpack
        length = 4
        numelements = unpack(byts[:length])

    p = length
    themap = OrderedDict()
    for n in xrange(numelements):
        key_len = unpack(byts[p:p + length])
        p += length
        keybytes = byts[p:p + key_len]
        p += key_len
        val_len = unpack(byts[p:p + length])
        p += length
        valbytes = byts[p:p + val_len]
        p += val_len
        key = subkeytype.from_binary(keybytes)
        val = subvaltype.from_binary(valbytes)
        themap[key] = val
    return themap
