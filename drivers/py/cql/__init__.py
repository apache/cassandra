
from os.path import exists, abspath, dirname, join
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
import zlib

try:
    from cassandra import Cassandra
    from cassandra.ttypes import Compression, InvalidRequestException, \
                                 CqlResultType
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
                          CqlResultType
    
COMPRESSION_SCHEMES = ['GZIP']
DEFAULT_COMPRESSION = 'GZIP'

class Connection(object):
    def __init__(self, keyspace, host, port=9160):
        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Cassandra.Client(protocol)
        socket.open()

        if keyspace:
            self.execute('USE %s' % keyspace)

    def execute(self, query, compression=None):
        compress = compression is None and DEFAULT_COMPRESSION \
                or compression.upper()
    
        compressed_query = Connection.compress_query(query, compress)
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

    @classmethod
    def compress_query(cls, query, compression):
        if not compression in COMPRESSION_SCHEMES:
            raise InvalidCompressionScheme(compression)

        if compression == 'GZIP':
            return zlib.compress(query)


class InvalidCompressionScheme(Exception): pass
class CQLException(Exception): pass

# vi: ai ts=4 tw=0 sw=4 et
