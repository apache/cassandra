#!/usr/bin/python

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

from txcql.connection_pool import ConnectionPool
from twisted.internet import defer, reactor

HOST = 'localhost'
PORT = 9160
KEYSPACE = 'txcql_test'
CF = 'test'

@defer.inlineCallbacks
def testcql(client):
    yield client.execute("CREATE KEYSPACE ? with replication_factor=1 and strategy_class='SimpleStrategy'", KEYSPACE)
    yield client.execute("CREATE COLUMNFAMILY ? with comparator=ascii and default_validation=ascii", CF)
    yield client.execute("UPDATE ? set foo = bar, bar = baz WHERE key = test", CF)
    res = yield client.execute("SELECT foo, bar from ? WHERE key = test", CF)
    for r in res:
        print r.__dict__
    yield client.execute("DROP KEYSPACE ?", KEYSPACE)
    reactor.stop() 

if __name__ == '__main__':
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)

    pool = ConnectionPool(KEYSPACE)
    testcql(pool)
    reactor.connectTCP(HOST, PORT, pool)
    reactor.run()
