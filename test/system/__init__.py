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

import os, sys, time, signal

__all__ = ['root', 'client']

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

# add cassandra directory to sys.path
L = os.path.abspath(__file__).split(os.path.sep)[:-3]
root = os.path.sep.join(L)
_ipath = os.path.join(root, 'interface', 'thrift', 'gen-py')
sys.path.append(os.path.join(_ipath, 'cassandra'))
import Cassandra

def get_client(host='127.0.0.1', port=9170):
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client

client = get_client()


pid_fname = "system_test.pid"
def pid():
    return int(open(pid_fname).read())


class CassandraTester(object):
    # leave this True unless you are manually starting a server and then
    # running only a single test against it; tests assume they start against an empty db.
    runserver = True

    def setUp(self):
        if self.runserver:
            if os.path.exists(pid_fname):
                pid_path = os.path.join(root, pid_fname)
                print "Unclean shutdown detected, (%s found)" % pid_path
                sys.exit()

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
            process = sp.Popen(args, stderr=sp.PIPE, stdout=sp.PIPE)
            time.sleep(0.1)

            # connect to it, with a timeout in case something went wrong
            start = time.time()
            while time.time() < start + 10:
                try:
                    client.transport.open()
                except:
                    time.sleep(0.1)
                else:
                    break
            else:
                print "Couldn't connect to server; aborting regression test"
                # see if process is still alive
                process.poll()
                
                if process.returncode is None:
                    os.kill(pid(), signal.SIGKILL) # just in case
                else:
                    stdout_value, stderr_value = process.communicate()
                    print "Stdout: %s" % (stdout_value)
                    print "Stderr: %s" % (stderr_value)
                sys.exit()
        else:
            try:
                client.transport.open()
            except:
                pass

    def tearDown(self):
        if self.runserver:
            client.transport.close()
            open('/tmp/kill', 'w').write('killing %s\n' % pid())
            os.kill(pid(), signal.SIGTERM)
            # TODO kill server with SIGKILL if it's still alive
            time.sleep(0.5)
            # TODO assert server is Truly Dead

# vim:ai sw=4 ts=4 tw=0 et
