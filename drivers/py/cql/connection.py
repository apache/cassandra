
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
import zlib

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
    def __init__(self, host, port=9160, keyspace=None, username=None,
                 password=None):
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Cassandra.Client(protocol)
        socket.open()
        
        if username and password:
            credentials = {"username": username, "password": password}
            self.client.login(AuthenticationRequest(credentials=credentials))
        
        if keyspace:
            self.execute('USE %s;' % keyspace)

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
    
        compressed_query = Connection.compress_query(prepare(query, *args),
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
            return response.rows
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
