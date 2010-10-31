
from avro.ipc  import HTTPTransceiver, Requestor
import avro.protocol, zlib
from os.path   import exists, abspath, dirname, join

def _load_protocol():
    # By default, look for the proto schema in the same dir as this file.
    avpr = join(abspath(dirname(__file__)), 'cassandra.avpr')
    if exists(avpr):
        return avro.protocol.parse(open(avpr).read())

    # Fall back to ../../interface/avro/cassandra.avpr (dev environ).
    avpr = join(abspath(dirname(__file__)),
                '..',
                '..',
                '..',
                'interface',
                'avro',
                'cassandra.avpr')
    if exists(avpr):
        return avro.protocol.parse(open(avpr).read())

    raise Exception("Unable to locate an avro protocol schema!")

COMPRESSION_SCHEMES = ['GZIP']
DEFAULT_COMPRESSION = 'GZIP'


class Connection(object):
    def __init__(self, keyspace, host, port=9160):
        client = HTTPTransceiver(host, port)
        self.requestor = Requestor(_load_protocol(), client)
        if keyspace:
            self.execute('USE %s' % keyspace)

    def execute(self, query, compression=None):
        compress = compression is None and DEFAULT_COMPRESSION \
                or compression.upper()
        if not compress in COMPRESSION_SCHEMES:
            raise InvalidCompressionScheme(compress)
    
        compressed_query = Connection.compress_query(query, compress)
        request_params = dict(query=compressed_query, compression=compress)
        response = self.requestor.request('execute_cql_query', request_params)

        if response['type'] == 'ROWS':
            return response['rows']
        return None

    @classmethod
    def compress_query(cls, query, compression):
        if compression == 'GZIP':
            return zlib.compress(query)


class InvalidCompressionScheme(Exception): pass

if __name__ == '__main__':
    dbconn = Connection('localhost', 9160)
    query = 'USE Keyspace1;'
    dbconn.execute(query, 'GZIP') 
    query = 'UPDATE Standard2 WITH ROW("k", COL("c", "v"));'
    dbconn.execute(query, 'GZIP') 

# vi: ai ts=4 tw=0 sw=4 et
