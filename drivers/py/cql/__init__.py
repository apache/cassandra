
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

import exceptions
import datetime
import time

import connection
import marshal


# dbapi Error hierarchy

class Warning(exceptions.StandardError): pass
class Error  (exceptions.StandardError): pass

class InterfaceError(Error): pass
class DatabaseError (Error): pass

class DataError        (DatabaseError): pass
class OperationalError (DatabaseError): pass
class IntegrityError   (DatabaseError): pass
class InternalError    (DatabaseError): pass
class ProgrammingError (DatabaseError): pass
class NotSupportedError(DatabaseError): pass


# Module constants

apilevel = 1.0
threadsafety = 1 # Threads may share the module, but not connections/cursors.
paramstyle = 'named'

# TODO: Pull connections out of a pool instead.
def connect(host, port=9160, keyspace='system', user=None, password=None):
    return connection.Connection(host, port, keyspace, user, password)

# Module Type Objects and Constructors

Date = datetime.date

Time = datetime.time

Timestamp = datetime.datetime

Binary = buffer

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])

class DBAPITypeObject:

    def __init__(self, *values):
        self.values = values

    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

STRING = DBAPITypeObject(marshal.BYTES_TYPE, marshal.ASCII_TYPE, marshal.UTF8_TYPE)

BINARY = DBAPITypeObject(marshal.BYTES_TYPE, marshal.UUID_TYPE, marshal.LEXICAL_UUID_TYPE)

NUMBER = DBAPITypeObject(marshal.LONG_TYPE, marshal.INTEGER_TYPE)

DATETIME = DBAPITypeObject(marshal.TIME_UUID_TYPE)

ROWID = DBAPITypeObject(marshal.BYTES_TYPE, marshal.ASCII_TYPE, marshal.UTF8_TYPE,
                        marshal.INTEGER_TYPE, marshal.LONG_TYPE, marshal.UUID_TYPE,
                        marshal.LEXICAL_UUID_TYPE, marshal.TIME_UUID_TYPE)

