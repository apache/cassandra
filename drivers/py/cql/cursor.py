
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
import zlib

import cql
from cql.marshal import prepare
from cql.decoders import SchemaDecoder
from cql.cassandra.ttypes import (
    Compression, 
    CqlResultType, 
    InvalidRequestException,
    UnavailableException,
    TimedOutException,
    TApplicationException,
    SchemaDisagreementException)

_COUNT_DESCRIPTION = (None, None, None, None, None, None, None)
_VOID_DESCRIPTION = (None)

class Cursor:
    _keyspace_re = re.compile("USE (\w+);?",
                              re.IGNORECASE | re.MULTILINE)
    _cfamily_re = re.compile("\s*SELECT\s+.+?\s+FROM\s+[\']?(\w+)",
                             re.IGNORECASE | re.MULTILINE | re.DOTALL)
    _ddl_re = re.compile("\s*(CREATE|ALTER|DROP)\s+",
                         re.IGNORECASE | re.MULTILINE)

    def __init__(self, parent_connection):
        self.open_socket = True
        self._connection = parent_connection

        self.description = None # A list of 7-tuples: 
                                #  (column_name, type_code, none, none,
                                #   none, none, nulls_ok=True)
                                # Populate on execute()

        self.arraysize = 1
        self.rowcount = -1      # Populate on execute()
        self.compression = 'GZIP'
        self.decoder = None

    ###
    # Cursor API
    ###

    def close(self):
        self.open_socket = False

    def prepare(self, query, params):
        return prepare(query, params)

    def execute(self, cql_query, params={}):
        self.__checksock()
        self.rs_idx = 0
        self.rowcount = 0
        self.description = None
        try:
            prepared_q = self.prepare(cql_query, params)
        except KeyError, e:
            raise cql.ProgrammingError("Unmatched named substitution: " +
                                       "%s not given for %s" % (e, cql_query))

        if self.compression == 'GZIP':
            compressed_q = zlib.compress(prepared_q)
        else:
            compressed_q = prepared_q
        request_compression = getattr(Compression, self.compression)

        try:
            client = self._connection.client
            response = client.execute_cql_query(compressed_q, request_compression)
        except InvalidRequestException, ire:
            raise cql.ProgrammingError("Bad Request: %s" % ire.why)
        except SchemaDisagreementException, sde:
            raise cql.IntegrityError("Schema versions disagree, (try again later).")
        except UnavailableException:
            raise cql.OperationalError("Unable to complete request: one or "
                                       "more nodes were unavailable.")
        except TimedOutException:
            raise cql.OperationalError("Request did not complete within rpc_timeout.")
        except TApplicationException, tapp:
            raise cql.InternalError("Internal application error")

        if response.type == CqlResultType.ROWS:
            self.decoder = SchemaDecoder(response.schema)
            self.result = response.rows
            self.rs_idx = 0
            self.rowcount = len(self.result)
            if self.result:
                self.description = self.decoder.decode_description(self.result[0])
        elif response.type == CqlResultType.INT:
            self.result = [(response.num,)]
            self.rs_idx = 0
            self.rowcount = 1
            # TODO: name could be the COUNT expression
            self.description = _COUNT_DESCRIPTION
        elif response.type == CqlResultType.VOID:
            self.result = []
            self.rs_idx = 0
            self.rowcount = 0
            self.description = _VOID_DESCRIPTION
        else:
            raise Exception('unknown result type ' + response.type)

        # 'Return values are not defined.'
        return True

    def executemany(self, operation_list, argslist):
        self.__checksock()
        opssize = len(operation_list)
        argsize = len(argslist)

        if opssize > argsize:
            raise cql.InterfaceError("Operations outnumber args for executemany().")
        elif opssize < argsize:
            raise cql.InterfaceError("Args outnumber operations for executemany().")

        for idx in xrange(opssize):
            self.execute(operation_list[idx], *argslist[idx])

    def fetchone(self):
        self.__checksock()
        if self.rs_idx == len(self.result):
            return None

        row = self.result[self.rs_idx]
        self.rs_idx += 1
        if self.description == _COUNT_DESCRIPTION:
            return row
        else:
            self.description = self.decoder.decode_description(row)
            return self.decoder.decode_row(row)

    def fetchmany(self, size=None):
        self.__checksock()
        if size is None:
            size = self.arraysize
        # we avoid leveraging fetchone here to avoid calling decode_description unnecessarily
        L = []
        while len(L) < size and self.rs_idx < len(self.result):
            row = self.result[self.rs_idx]
            self.rs_idx += 1
            L.append(self.decoder.decode_row(row))
        return L

    def fetchall(self):
        return self.fetchmany(len(self.result) - self.rs_idx)

    ###
    # extra, for cqlsh
    ###
    
    def _reset(self):
        self.rs_idx = 0

    ###
    # Iterator extension
    ###

    def next(self):
        if self.rs_idx >= len(self.result):
            raise StopIteration
        return self.fetchone()

    def __iter__(self):
        return self

    ###
    # Unsupported, unimplemented optionally
    ###

    def setinputsizes(self, sizes):
        pass # DO NOTHING

    def setoutputsize(self, size, *columns):
        pass # DO NOTHING

    def callproc(self, procname, *args):
        raise cql.NotSupportedError()

    def nextset(self):
        raise cql.NotSupportedError()

    ###
    # Helpers
    ###

    def __checksock(self):
        if not self.open_socket:
            raise cql.InternalError("Cursor belonging to %s has been closed." %
                                    (self._connection, ))
