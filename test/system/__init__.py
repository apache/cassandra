import os, sys, time, signal

__all__ = ['root', 'client']

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

# add cassandra directory to sys.path
L = os.path.abspath(__file__).split(os.path.sep)[:-3]
root = os.path.sep.join(L)
_ipath = os.path.join(root, 'interface', 'gen-py')
sys.path.append(os.path.join(_ipath, 'org', 'apache', 'cassandra'))
import Cassandra

host, port = '127.0.0.1', 9160
def get_client():
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client

client = get_client()


import tempfile
_, pid_fname = tempfile.mkstemp()
def pid():
    return int(open(pid_fname).read())


class CassandraTester(object):
    # leave this True unless you are manually starting a server and then
    # running only a single test against it; tests assume they start against an empty db.
    runserver = True

    def setUp(self):
        if self.runserver:
            # clean out old stuff
            import shutil
            # todo get directories from conf/storage-conf.xml
            for dirname in ['system', 'data', 'commitlog']:
                try:
                    shutil.rmtree('build/test/cassandra/' + dirname)
                except OSError:
                    pass
            # start the server
            import subprocess as sp
            os.chdir(root)
            os.environ['CASSANDRA_INCLUDE'] = 'test/cassandra.in.sh'
            args = ['bin/cassandra', '-p', pid_fname]
            sp.Popen(args, stderr=sp.PIPE, stdout=sp.PIPE)
            time.sleep(0.1)

            # connect to it, with a timeout in case something went wrong
            start = time.time()
            while time.time() < start + 20:
                try:
                    client.transport.open()
                except:
                    time.sleep(0.1)
                else:
                    break
            else:
                os.kill(pid(), signal.SIGKILL) # just in case
                print "Couldn't connect to server; aborting regression test"
                sys.exit()
        else:
            client.transport.open()

    def tearDown(self):
        if self.runserver:
            client.transport.close()
            open('/tmp/kill', 'w').write('killing %s\n' % pid())
            os.kill(pid(), signal.SIGTERM)
            # TODO kill server with SIGKILL if it's still alive
            time.sleep(0.5)
            # TODO assert server is Truly Dead
