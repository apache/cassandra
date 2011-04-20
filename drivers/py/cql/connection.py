
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

from cursor import Cursor
from cassandra import Cassandra
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from cql.cassandra.ttypes import AuthenticationRequest


class Connection(object):

    def __init__(self, host, port, keyspace, user=None, password=None):
        """
        Params:
        * host .........: hostname of Cassandra node.
        * port .........: port number to connect to.
        * keyspace .....: keyspace to connect to.
        * user .........: username used in authentication (optional).
        * password .....: password used in authentication (optional).
        """
        self.host = host
        self.port = port
        self.keyspace = keyspace

        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = Cassandra.Client(protocol)

        socket.open()
        self.open_socket = True

        if user and password:
            credentials = {"username": user, "password": password}
            self.client.login(AuthenticationRequest(credentials=credentials))

        if keyspace:
            c = self.cursor()
            c.execute('USE %s;' % keyspace)
            c.close()

    def __str__(self):
        return "{host: '%s:%s', keyspace: '%s'}"%(self.host,self.port,self.keyspace)

    ###
    # Connection API
    ###

    def close(self):
        if not self.open_socket:
            return

        self.transport.close()
        self.open_socket = False

    def commit(self):
        """
        'Database modules that do not support transactions should
          implement this method with void functionality.'
        """
        return

    def rollback(self):
        from cql import NotSupportedError
        raise NotSupportedError("Rollback functionality not present in Cassandra.")

    def cursor(self):
        from cql import ProgrammingError
        if not self.open_socket:
            raise ProgrammingError("Connection has been closed.")
        return Cursor(self)
