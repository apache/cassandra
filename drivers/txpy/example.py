#!/usr/bin/python
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
