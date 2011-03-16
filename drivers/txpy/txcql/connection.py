
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

from os.path import abspath, dirname, join
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from twisted.internet import defer, reactor
from cassandra import Cassandra
from cassandra.ttypes import Compression, InvalidRequestException, \
			 CqlResultType, AuthenticationRequest, \
			 SchemaDisagreementException

try:
    from cql.errors import CQLException, InvalidCompressionScheme
    from cql.marshal import prepare
    from cql.decoders import SchemaDecoder
    from cql.results import RowsProxy
    from cql.connection import Connection as connection, COMPRESSION_SCHEMES, DEFAULT_COMPRESSION
except ImportError:
    # Hack to run from a source tree
    import sys
    sys.path.append(join(abspath(dirname(__file__)),
                         '..',
                         '..',
                         'py'))
    from cql.errors import CQLException, InvalidCompressionScheme
    from cql.marshal import prepare
    from cql.decoders import SchemaDecoder
    from cql.results import RowsProxy
    from cql.connection import Connection as connection, COMPRESSION_SCHEMES, DEFAULT_COMPRESSION
    

class Connection(TTwisted.ThriftClientProtocol):
    def __init__(self, iprot_factory, oprot_factory=None, keyspace=None, credentials=None, decoder=None):
        TTwisted.ThriftClientProtocol.__init__(self, Cassandra.Client, iprot_factory, oprot_factory)
        self.keyspace = keyspace
        self.column_family = None
        self.credentials = credentials
        self.decoder = decoder

    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.client.protocol = self
        d = self.setupConnection()
        d.addErrback(self.setupFailed)

    def setupConnection(self):
        d = defer.succeed(True)
        if self.credentials:
            d.addCallback(lambda _: self.client.login(AuthenticationRequest(credentials=self.credentials)))
        if self.keyspace:
            d.addCallback(lambda _: self.execute('USE %s;' % self.keyspace))
        if not self.decoder:
            d.addCallback(self.__getSchema)
        return d
    
    def __getSchema(self, res=None):
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
        
        def setSchema(ksdefs):
            schema = {}
            for ksdef in ksdefs:
                schema[ksdef.name] = column_families(ksdef.cf_defs)
            self.decoder = SchemaDecoder(schema)
            return True
        d = self.client.describe_keyspaces()
        d.addCallback(setSchema)
        return d
    
    def setupFailed(self, err):
        self.transport.loseConnection()

    def prepare(self, query, *args):
        prepared_query = prepare(query, *args)
        match = connection._cfamily_re.match(prepared_query)
        if match:
            self.column_family = match.group(1)
        else:
            match = connection._keyspace_re.match(prepared_query)
            if match:
                self.keyspace = match.group(1)
        return prepared_query
    
    def execute(self, query, *args, **kwargs):
        """
        Execute a CQL query on a remote node.
        
        Params:
        * query .........: CQL query string.
        * args ..........: Query parameters.
        * compression ...: Query compression type (optional).
        """
        def _error(err):
            if isinstance(err, InvalidRequestException):
                raise CQLException("Bad Request: %s" % err.why)
            elif isinstance(err, TApplicationException):
                raise CQLException("Internal application error")
            elif isinstance(err, SchemaDisagreementException):
                raise CQLException("schema versions disagree, (try again later).")
            else:
                raise CQLException(err)
            
        def _success(response):
            if response.type == CqlResultType.ROWS:
                return RowsProxy(response.rows,
                                 self.keyspace,
                                 self.column_family,
                                 self.decoder)
            if response.type == CqlResultType.INT:
                return response.num
            return None
            
        if kwargs.has_key("compression"):
            compress = kwargs.get("compression").upper()
        else:
            compress = DEFAULT_COMPRESSION
        
        compressed_query = connection.compress_query(self.prepare(query, *args),
                                                     compress)
        request_compression = getattr(Compression, compress)
        
        d = self.client.execute_cql_query(compressed_query,
                                                     request_compression)
        d.addCallbacks(_success, _error)
        return d
