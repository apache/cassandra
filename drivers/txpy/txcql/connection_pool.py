
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

from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer, reactor
from twisted.internet.error import UserError
from connection import Connection
from cassandra.ttypes import InvalidRequestException
from sys import exc_info

class ClientBusy(Exception):
    pass
        
class ManagedConnection(Connection):
    aborted = False
    deferred = None
    
    def connectionMade(self):
        TTwisted.ThriftClientProtocol.connectionMade(self)
        self.client.protocol = self
        d = self.setupConnection()
        d.addCallbacks(
            (lambda res: self.factory.clientIdle(self, res)),
            self.setupFailed
        )
        
    def connectionLost(self, reason=None):
        if not self.aborted: # don't allow parent class to raise unhandled TTransport
                             # exceptions, the manager handled our failure
            TTwisted.ThriftClientProtocol.connectionLost(self, reason)
        self.factory.clientGone(self)
        
    def setupFailed(self, err):
        Connection.setupFailed(self, err)
        self.factory.clientSetupFailed(err)
        
    def _complete(self, res=None):
        self.deferred = None
        self.factory.clientIdle(self)
        return res
    
    def execute(self, query, *args, **kwargs):
        if not self.deferred:
            d = Connection.execute(self, query, *args, **kwargs)
            d.addBoth(self._complete)
            return d
        else:
            raise ClientBusy
    
    def abort(self):
        self.aborted = True
        self.transport.loseConnection()
        
class ConnectionPool(ReconnectingClientFactory):
    maxDelay = 5
    thriftFactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory
    protocol = ManagedConnection

    def __init__(self, keyspace=None, retries=0, credentials=None, decoder=None):
        self.deferred  = defer.Deferred()
        self.queue = defer.DeferredQueue()
        self.continueTrying = True
        self._protos = []
        self._pending = []
        self.request_retries = retries
        self.keyspace = keyspace
        self.credentials = credentials
        self.decoder = decoder

    def _errback(self, reason=None):
        if self.deferred:
            self.deferred.errback(reason)
            self.deferred = None

    def _callback(self, value=None):
        if self.deferred:
            self.deferred.callback(value)
            self.deferred = None

    def clientConnectionFailed(self, connector, reason):
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
        self._errback(reason)

    def clientSetupFailed(self, reason):
        self._errback(reason)

    def clientIdle(self, proto, result=True):
        if proto not in self._protos:
            self._protos.append(proto)
        self._submitExecution(proto)
        self._callback(result)

    def buildProtocol(self, addr):
        p = self.protocol(self.thriftFactory(),
                          keyspace=self.keyspace,
                          credentials=self.credentials,
                          decoder=self.decoder)
        p.factory = self
        self.resetDelay()
        return p

    def clientGone(self, proto):
        try:
            self._protos.remove(proto)
        except ValueError:
            pass
        
    def _submitExecution(self, proto):
        def reqError(err, req, d, r):
            if err.check(InvalidRequestException) or r < 1:
                if err.tb is None:
                    try:
                        raise err.value
                    except Exception:
                        # make new Failure object explicitly, so that the same
                        # (traceback-less) one made by Thrift won't be retained
                        # and useful tracebacks thrown away
                        t, v, tb = exc_info()
                        err = failure.Failure(v, t, tb)
                d.errback(err)
                self._pending.remove(d)
            else:
                self.queue.put((req, d, r))
        def reqSuccess(res, d):
            d.callback(res)
            self._pending.remove(d)
        def _process((request, deferred, retries)):
            if not proto in self._protos:
                # may have disconnected while we were waiting for a request
                self.queue.put((request, deferred, retries))
            else:
                try:
                    d = proto.execute(request[0], *request[1], **request[2])
                except Exception:
                    proto.abort()
                    d = defer.fail()
                retries -= 1
                d.addCallbacks(reqSuccess,
                               reqError,
                               callbackArgs=[deferred],
                               errbackArgs=[request, deferred, retries])
        return self.queue.get().addCallback(_process)
    
    def execute(self, query, *args, **kwargs):
        if 'retries' in kwargs:
            retries = kwargs['retries']
            del kwargs['retries']
        else:
            retries = self.request_retries
        d = defer.Deferred()
        self._pending.append(d)
        self.queue.put(((query, args, kwargs), d, retries))
        return d
    
    def shutdown(self):
        self.stopTrying()
        for p in self._protos:
            if p.transport:
                p.transport.loseConnection()
        for d in self._pending:
            if not d.called: d.errback(UserError(string="Shutdown requested"))
    
