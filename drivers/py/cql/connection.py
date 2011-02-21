
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

from os.path import exists, abspath, dirname, join
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from errors import CQLException, InvalidCompressionScheme
from marshal import prepare
from decoders import SchemaDecoder
from results import RowsProxy
import zlib, re

try:
    from cassandra import Cassandra
    from cassandra.ttypes import Compression, InvalidRequestException, \
                                 CqlResultType, AuthenticationRequest
except ImportError:
    # Hack to run from a source tree
    import sys
    sys.path.append(join(abspath(dirname(__file__)),
                         '..',
                         '..',
                         '..',
                         'interface',
                         'thrift',
                         'gen-py'))
    from cassandra import Cassandra
    from cassandra.ttypes import Compression, InvalidRequestException, \
                          CqlResultType, AuthenticationRequest
    
COMPRESSION_SCHEMES = ['GZIP']
DEFAULT_COMPRESSION = 'GZIP'

__all__ = ['COMPRESSION_SCHEMES', 'DEFAULT_COMPRESSION', 'Connection']

class Connection(object):
    """
    CQL connection object.
    
    Example usage:
    >>> conn = Connection("localhost", keyspace="Keyspace1")
    >>> r = conn.execute('SELECT "age" FROM Users')
    >>> for row in r.rows:
    ...     for column in row.columns:
    ...         print "%s is %s years of age" % (r.key, column.age)
    """
    _keyspace_re = re.compile("USE (\w+);?", re.I | re.M)
    _cfamily_re = re.compile("SELECT\s+.+\s+FROM\s+(\w+)", re.I | re.M)
    
    def __init__(self, host, port=9160, keyspace=None, username=None,
                 password=None, decoder=None):
        """
        Params:
        * host .........: hostname of Cassandra node.
        * port .........: port number to connect to (optional).
        * keyspace .....: keyspace name (optional).
        * username .....: username used in authentication (optional).
        * password .....: password used in authentication (optional).
        * decoder ......: result decoder instance (optional, defaults to none).
        """
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Cassandra.Client(protocol)
        socket.open()
        
        # XXX: "current" is probably a misnomer.
        self._cur_keyspace = None
        self._cur_column_family = None
        
        if username and password:
            credentials = {"username": username, "password": password}
            self.client.login(AuthenticationRequest(credentials=credentials))
        
        if keyspace:
            self.execute('USE %s;' % keyspace)
            self._cur_keyspace = keyspace
        
        if not decoder:
            self.decoder = SchemaDecoder(self.__get_schema())
        else:
            self.decoder = decoder

    def __get_schema(self):
        def columns(metadata):
            results = {}
            for col in metadata:
                results[col.name] = col.validation_class
            return results
        
        def column_families(cf_defs):
            cfresults = {}
            for cf in cf_defs:
                cfresults[cf.name] = {"comparator": cf.comparator_type}
                cfresults[cf.name]["default_validation_class"] = \
                         cf.default_validation_class
                cfresults[cf.name]["columns"] = columns(cf.column_metadata)
            return cfresults
        
        schema = {}
        for ksdef in self.client.describe_keyspaces():
            schema[ksdef.name] = column_families(ksdef.cf_defs)
        return schema        
            
    def prepare(self, query, *args):
        prepared_query = prepare(query, *args)
        
        # Snag the keyspace or column family and stash it for later use in
        # decoding columns.  These regexes don't match every query, but the
        # current column family only needs to be current for SELECTs.
        match = Connection._cfamily_re.match(prepared_query)
        if match:
            self._cur_column_family = match.group(1)
        else:
            match = Connection._keyspace_re.match(prepared_query)
            if match:
                self._cur_keyspace = match.group(1)

        return prepared_query

    def execute(self, query, *args, **kwargs):
        """
        Execute a CQL query on a remote node.
        
        Params:
        * query .........: CQL query string.
        * args ..........: Query parameters.
        * compression ...: Query compression type (optional).
        """
        if kwargs.has_key("compression"):
            compress = kwargs.get("compression").upper()
        else:
            compress = DEFAULT_COMPRESSION
        
        compressed_query = Connection.compress_query(self.prepare(query, *args),
                                                     compress)
        request_compression = getattr(Compression, compress)

        try:
            response = self.client.execute_cql_query(compressed_query,
                                                     request_compression)
        except InvalidRequestException, ire:
            raise CQLException("Bad Request: %s" % ire.why)
        except TApplicationException, tapp:
            raise CQLException("Internal application error")
        except Exception, exc:
            raise CQLException(exc)

        if response.type == CqlResultType.ROWS:
            return RowsProxy(response.rows,
                             self._cur_keyspace,
                             self._cur_column_family,
                             self.decoder)
        
        if response.type == CqlResultType.INT:
            return response.num

        return None

    def close(self):
        self.transport.close()
        
    def is_open(self):
        return self.transport.isOpen()

    @classmethod
    def compress_query(cls, query, compression):
        """
        Returns a query string compressed with the specified compression type.
        
        Params:
        * query .........: The query string to compress.
        * compression ...: Type of compression to use.
        """
        if not compression in COMPRESSION_SCHEMES:
            raise InvalidCompressionScheme(compression)

        if compression == 'GZIP':
            return zlib.compress(query)
    
# vi: ai ts=4 tw=0 sw=4 et
