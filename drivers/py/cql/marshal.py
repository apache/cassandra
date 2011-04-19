
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

import re
import struct
from uuid import UUID

import cql

__all__ = ['prepare', 'marshal', 'unmarshal_noop', 'unmarshallers']

if hasattr(struct, 'Struct'): # new in Python 2.5
   _have_struct = True
   _long_packer = struct.Struct('>q')
else:
    _have_struct = False

_param_re = re.compile(r"(?<!strategy_options)(:[a-zA-Z_][a-zA-Z0-9_]*)", re.M)

BYTES_TYPE = "org.apache.cassandra.db.marshal.BytesType"
ASCII_TYPE = "org.apache.cassandra.db.marshal.AsciiType"
UTF8_TYPE = "org.apache.cassandra.db.marshal.UTF8Type"
INTEGER_TYPE = "org.apache.cassandra.db.marshal.IntegerType"
LONG_TYPE = "org.apache.cassandra.db.marshal.LongType"
UUID_TYPE = "org.apache.cassandra.db.marshal.UUIDType"
LEXICAL_UUID_TYPE = "org.apache.cassandra.db.marshal.LexicalType"
TIME_UUID_TYPE = "org.apache.cassandra.db.marshal.TimeUUIDType"

def prepare(query, params):
    # For every match of the form ":param_name", call marshal
    # on kwargs['param_name'] and replace that section of the query
    # with the result
    new, count = re.subn(_param_re, lambda m: marshal(params[m.group(1)[1:]]), query)
    if len(params) > count:
        raise cql.ProgrammingError("More keywords were provided than parameters")
    return new

def marshal(term):
    if isinstance(term, unicode):
        return "'%s'" % __escape_quotes(term.encode('utf8'))
    elif isinstance(term, str):
        return "'%s'" % __escape_quotes(term)
    else:
        return str(term)

def unmarshal_noop(bytestr):
    return bytestr

def unmarshal_utf8(bytestr):
    return bytestr.decode("utf8")

def unmarshal_int(bytestr):
    return decode_bigint(bytestr)

if _have_struct:
    def unmarshal_long(bytestr):
        return _long_packer.unpack(bytestr)[0]
else:
    def unmarshal_long(bytestr):
        return struct.unpack(">q", bytestr)[0]

def unmarshal_uuid(bytestr):
    return UUID(bytes=bytestr)

unmarshallers = {BYTES_TYPE:        unmarshal_noop,
                 ASCII_TYPE:        unmarshal_noop,
                 UTF8_TYPE:         unmarshal_utf8,
                 INTEGER_TYPE:      unmarshal_int,
                 LONG_TYPE:         unmarshal_long,
                 UUID_TYPE:         unmarshal_uuid,
                 LEXICAL_UUID_TYPE: unmarshal_uuid,
                 TIME_UUID_TYPE:    unmarshal_uuid}

def decode_bigint(term):
    val = int(term.encode('hex'), 16)
    if (ord(term[0]) & 128) != 0:
        val = val - (1 << (len(term) * 8))
    return val

def __escape_quotes(term):
    assert isinstance(term, (str, unicode))
    return term.replace("\'", "''")
