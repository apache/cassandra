
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

class Cursor:

    _keyspace_re = re.compile("USE (\w+);?", re.I | re.M)
    _cfamily_re = re.compile("\s*SELECT\s+.+\s+FROM\s+[\']?(\w+)", re.I | re.M)
    _ddl_re = re.compile("\s*(CREATE|ALTER|DROP)\s+", re.I | re.M)

    def __init__(self, parent_connection):
        self.open_socket = True
        self.parent_connection = parent_connection

        self.description = None # A list of 7-tuples: 
                                #  (column_name, type_code, none, none,
                                #   none, none, nulls_ok=True)
                                # Populate on execute()

        self.arraysize = 1
        self.rowcount = -1      # Populate on execute()
        self.compression = 'GZIP'

        self._query_ks = self.parent_connection.keyspace
        self._query_cf = None
        self.decoder = SchemaDecoder(self.__get_schema())

    ###
    # Cursor API
    ###

    def close(self):
        self.open_socket = False

    def prepare(self, query, params):
        prepared_query = prepare(query, params)
        self._schema_update_needed = False

        # Snag the keyspace or column family and stash it for later use in
        # decoding columns.  These regexes don't match every query, but the
        # current column family only needs to be current for SELECTs.
        match = Cursor._cfamily_re.match(prepared_query)
        if match:
            self._query_cf = match.group(1)
            return prepared_query
        match = Cursor._keyspace_re.match(prepared_query)
        if match:
            self._query_ks = match.group(1)
            return prepared_query

        # If this is a CREATE, then refresh the schema for decoding purposes.
        match = Cursor._ddl_re.match(prepared_query)
        if match:
            self._schema_update_needed = True
        return prepared_query

    def __get_schema(self):
        def columns(metadata):
            results = {}
            for col in metadata:
                results[col.name] = col.validation_class
            return results

        def column_families(cf_defs):
            d = {}
            for cf in cf_defs:
                d[cf.name] = {'comparator': cf.comparator_type,
                              'default_validation_class': cf.default_validation_class,
                              'key_validation_class': cf.key_validation_class,
                              'columns': columns(cf.column_metadata),
                              'key_alias': cf.key_alias}
            return d

        schema = {}
        client = self.parent_connection.client
        for ksdef in client.describe_keyspaces():
            schema[ksdef.name] = column_families(ksdef.cf_defs)
        return schema

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
            client = self.parent_connection.client
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

        if self._schema_update_needed and isinstance(self.decoder, SchemaDecoder):
            self.decoder.schema = self.__get_schema()

        if response.type == CqlResultType.ROWS:
            self.result = response.rows
            self.rs_idx = 0
            self.rowcount = len(self.result)
            if self.result:
                self.description = self.decoder.decode_description(self._query_ks, self._query_cf, self.result[0])

        if response.type == CqlResultType.INT:
            self.result = [(response.num,)]
            self.rs_idx = 0
            self.rowcount = 1
            # TODO: name could be the COUNT expression
            self.description = _COUNT_DESCRIPTION

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
        row = self.result[self.rs_idx]
        self.rs_idx += 1
        if self.description != _COUNT_DESCRIPTION:
            self.description = self.decoder.decode_description(self._query_ks, self._query_cf, row)
            return self.decoder.decode_row(self._query_ks, self._query_cf, row)
        else:
            return row

    def fetchmany(self, size=None):
        self.__checksock()
        if size is None:
            size = self.arraysize
        # we avoid leveraging fetchone here to avoid calling decode_description unnecessarily
        L = []
        while len(L) < size and self.rs_idx < len(self.result):
            row = self.result[self.rs_idx]
            self.rs_idx += 1
            L.append(self.decoder.decode_row(self._query_ks, self._query_cf, row))
        return L

    def fetchall(self):
        return self.fetchmany(len(self.result) - self.rs_idx)

    ###
    # Iterator extension
    ###

    def next(self):
        raise Warning("DB-API extension cursor.next() used")

        if self.rs_idx >= len(self.result):
            raise StopIteration
        return self.fetchone()

    def __iter__(self):
        raise Warning("DB-API extension cursor.__iter__() used")
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
                                    (self.parent_connection, ))
