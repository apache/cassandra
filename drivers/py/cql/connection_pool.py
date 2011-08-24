
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

from Queue import Queue, Empty
from threading import Thread
from time import sleep
from connection import Connection

__all__ = ['ConnectionPool']

class ConnectionPool(object):
    """
    Simple connection-caching pool implementation.

    ConnectionPool provides the simplest possible connection pooling,
    lazily creating new connections if needed as `borrow_connection' is
    called.  Connections are re-added to the pool by `return_connection',
    unless doing so would exceed the maximum pool size.
    
    Example usage:
    >>> pool = ConnectionPool("localhost", 9160, "Keyspace1")
    >>> conn = pool.borrow_connection()
    >>> conn.execute(...)
    >>> pool.return_connection(conn)
    """
    def __init__(self, hostname, port=9160, keyspace=None, username=None,
                 password=None, decoder=None, max_conns=25, max_idle=5,
                 eviction_delay=10000):
        self.hostname = hostname
        self.port = port
        self.keyspace = keyspace
        self.username = username
        self.password = password
        self.decoder = decoder
        self.max_conns = max_conns
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay
        
        self.connections = Queue()
        self.connections.put(self.__create_connection())
        self.eviction = Eviction(self.connections,
                                 self.max_idle,
                                 self.eviction_delay)
    
    def __create_connection(self):
        return Connection(self.hostname,
                          port=self.port,
                          keyspace=self.keyspace,
                          username=self.username,
                          password=self.password,
                          decoder=self.decoder)
        
    def borrow_connection(self):
        try:
            connection = self.connections.get(block=False)
        except Empty:
            connection = self.__create_connection()
        return connection
    
    def return_connection(self, connection):
        if self.connections.qsize() > self.max_conns:
            connection.close()
            return
        if not connection.is_open():
            return
        self.connections.put(connection)

class Eviction(Thread):
    def __init__(self, connections, max_idle, eviction_delay):
        Thread.__init__(self)
        
        self.connections = connections
        self.max_idle = max_idle
        self.eviction_delay = eviction_delay
        
        self.setDaemon(True)
        self.setName("EVICTION-THREAD")
        self.start()
        
    def run(self):
        while(True):
            while(self.connections.qsize() > self.max_idle):
                connection = self.connections.get(block=False)
                if connection:
                    if connection.is_open():
                        connection.close()
            sleep(self.eviction_delay/1000)
        
        
        
