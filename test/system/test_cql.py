
from os.path import abspath, dirname, join
import sys, uuid, time

sys.path.append(join(abspath(dirname(__file__)), '../../drivers/py'))

from cql import Connection
from cql.errors import CQLException
from . import ThriftTester
from . import thrift_client     # TODO: temporary

def assert_raises(exception, method, *args):
    try:
        method(*args)
    except exception:
        return
    raise AssertionError("failed to see expected exception")
    
def uuid1bytes_to_millis(uuidbytes):
    return (uuid.UUID(bytes=uuidbytes).get_time() / 10000) - 12219292800000L

def load_sample(dbconn):
    dbconn.execute("""
        UPDATE Standard1 SET "ca1" = "va1", "col" = "val" WHERE KEY = "ka"
    """)
    dbconn.execute("""
        UPDATE Standard1 SET "cb1" = "vb1", "col" = "val" WHERE KEY = "kb"
    """)
    dbconn.execute("""
        UPDATE Standard1 SET "cc1" = "vc1", "col" = "val" WHERE KEY = "kc"
    """)
    dbconn.execute("""
        UPDATE Standard1 SET "cd1" = "vd1", "col" = "val" WHERE KEY = "kd"
    """)
    
    dbconn.execute("""
        UPDATE Standard2 SET u"%s" = "ve1", "col" = "val" WHERE KEY = "kd"
    """ % u'\xa9'.encode('utf8'))
    dbconn.execute("""
        UPDATE Standard2 SET u"cf1" = "vf1", "col" = "val" WHERE KEY = "kd"
    """)

    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY.ONE
     UPDATE StandardLong1 SET 1L="1", 2L="2", 3L="3", 4L="4" WHERE KEY="aa";
     UPDATE StandardLong1 SET 5L="5", 6L="6", 7L="8", 9L="9" WHERE KEY="ab";
     UPDATE StandardLong1 SET 9L="9", 8L="8", 7L="7", 6L="6" WHERE KEY="ac";
     UPDATE StandardLong1 SET 5L="5", 4L="4", 3L="3", 2L="2" WHERE KEY="ad";
     UPDATE StandardLong1 SET 1L="1", 2L="2", 3L="3", 4L="4" WHERE KEY="ae";
     UPDATE StandardLong1 SET 1L="1", 2L="2", 3L="3", 4L="4" WHERE KEY="af";
     UPDATE StandardLong1 SET 5L="5", 6L="6", 7L="8", 9L="9" WHERE KEY="ag";
    APPLY BATCH
    """)
    
    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY.ONE
      UPDATE StandardInteger1 SET 10="a", 20="b", 30="c", 40="d" WHERE KEY="k1";
      UPDATE StandardInteger1 SET 10="e", 20="f", 30="g", 40="h" WHERE KEY="k2";
      UPDATE StandardInteger1 SET 10="i", 20="j", 30="k", 40="l" WHERE KEY="k3";
      UPDATE StandardInteger1 SET 10="m", 20="n", 30="o", 40="p" WHERE KEY="k4";
      UPDATE StandardInteger1 SET 10="q", 20="r", 30="s", 40="t" WHERE KEY="k5";
      UPDATE StandardInteger1 SET 10="u", 20="v", 30="w", 40="x" WHERE KEY="k6";
      UPDATE StandardInteger1 SET 10="y", 20="z", 30="A", 40="B" WHERE KEY="k7";
    APPLY BATCH
    """)

    dbconn.execute("""
    BEGIN BATCH
    UPDATE Indexed1 SET "birthdate"=100L, "unindexed"=250L WHERE KEY="asmith";
    UPDATE Indexed1 SET "birthdate"=100L, "unindexed"=200L WHERE KEY="dozer";
    UPDATE Indexed1 SET "birthdate"=175L, "unindexed"=200L WHERE KEY="morpheus";
    UPDATE Indexed1 SET "birthdate"=150L, "unindexed"=250L WHERE KEY="neo";
    UPDATE Indexed1 SET "birthdate"=125L, "unindexed"=200L WHERE KEY="trinity";
    APPLY BATCH
    """)

def init(keyspace="Keyspace1"):
    dbconn = Connection('localhost', 9170, keyspace)
    load_sample(dbconn)
    return dbconn

class TestCql(ThriftTester):
    def test_select_simple(self):
        "retrieve a column"
        conn = init()
        r = conn.execute('SELECT "ca1" FROM Standard1 WHERE KEY="ka"')
        assert r[0].key == 'ka'
        assert r[0].columns[0].name == 'ca1'
        assert r[0].columns[0].value == 'va1'

    def test_select_columns(self):
        "retrieve multiple columns"
        conn = init()
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]

    def test_select_row_range(self):
        "retrieve a range of rows with columns"
        conn = init()
        r = conn.execute('SELECT 4L FROM StandardLong1 WHERE KEY > "ad" AND KEY < "ag";')
        assert len(r) == 3
        assert r[0].key == "ad"
        assert r[1].key == "ae"
        assert r[2].key == "af"
        assert len(r[0].columns) == 1
        assert len(r[1].columns) == 1
        assert len(r[2].columns) == 1

    def test_select_row_range_with_limit(self):
        "retrieve a limited range of rows with columns"
        conn = init()
        r = conn.execute("""
            SELECT 1L,5L,9L FROM StandardLong1 WHERE KEY > "aa" AND KEY < "ag" LIMIT 3
        """)
        assert len(r) == 3
        
        r = conn.execute("""
            SELECT 20,40 FROM StandardInteger1 WHERE KEY > "k1"
                    AND KEY < "k7" LIMIT 5
        """)
        assert len(r) == 5
        r[0].key == "k1"
        r[4].key == "k5"

    def test_select_columns_slice(self):
        "range of columns (slice) by row"
        conn = init()
        r = conn.execute('SELECT 1L..3L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1
        assert r[0].columns[0].value == "1"
        assert r[0].columns[1].value == "2"
        assert r[0].columns[2].value == "3"
        
        r = conn.execute('SELECT 10..30 FROM StandardInteger1 WHERE KEY="k1"')
        assert len(r) == 1
        assert r[0].columns[0].value == "a"
        assert r[0].columns[1].value == "b"
        assert r[0].columns[2].value == "c"

    def test_select_columns_slice_with_limit(self):
        "range of columns (slice) by row with limit"
        conn = init()
        r = conn.execute('SELECT FIRST 1 1L..3L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1
        assert len(r[0].columns) == 1
        assert r[0].columns[0].value == "1"

    def test_select_columns_slice_reversed(self):
        "range of columns (slice) by row reversed"
        conn = init()
        r = conn.execute('SELECT FIRST 2 REVERSED 3L..1L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1, "%d != 1" % len(r)
        assert len(r[0].columns) == 2
        assert r[0].columns[0].value == "3"
        assert r[0].columns[1].value == "2"

    def test_error_on_multiple_key_by(self):
        "ensure multiple key-bys in where clause excepts"
        conn = init()
        query = 'SELECT "col" FROM Standard1 WHERE KEY = "ka" AND KEY = "kb";'
        assert_raises(CQLException, conn.execute, query)

    def test_index_scan_equality(self):
        "indexed scan where column equals value"
        conn = init()
        r = conn.execute('SELECT "birthdate" FROM Indexed1 WHERE "birthdate" = 100L')
        assert len(r) == 2
        assert r[0].key == "asmith"
        assert r[1].key == "dozer"
        assert len(r[0].columns) == 1
        assert len(r[1].columns) == 1

    def test_index_scan_greater_than(self):
        "indexed scan where a column is greater than a value"
        conn = init()
        r = conn.execute("""
            SELECT "birthdate" FROM Indexed1 WHERE "birthdate" = 100L AND "unindexed" > 200L
        """)
        assert len(r) == 1
        assert r[0].key == "asmith"

    def test_index_scan_with_start_key(self):
        "indexed scan with a starting key"
        conn = init()
        r = conn.execute("""
            SELECT "birthdate" FROM Indexed1 WHERE "birthdate" = 100L AND KEY > "asmithZ"
        """)
        assert len(r) == 1
        assert r[0].key == "dozer"

    def test_no_where_clause(self):
        "empty where clause (range query w/o start key)"
        conn = init()
        r = conn.execute('SELECT "col" FROM Standard1 LIMIT 3')
        assert len(r) == 3
        assert r[0].key == "ka"
        assert r[1].key == "kb"
        assert r[2].key == "kc"

    def test_column_count(self):
        "getting a result count instead of results"
        conn = init()
        r = conn.execute('SELECT COUNT(1L..4L) FROM StandardLong1 WHERE KEY = "aa";')
        assert r == 4, "expected 4 results, got %d" % (r and r or 0)

    def test_truncate_columnfamily(self):
        "truncating a column family"
        conn = init()
        conn.execute('TRUNCATE Standard1;')
        r = conn.execute('SELECT "cd1" FROM Standard1 WHERE KEY = "kd"')
        assert len(r) == 0

    def test_delete_columns(self):
        "delete columns from a row"
        conn = init()
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]
        conn.execute('DELETE "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert len(r[0].columns) == 0

    def test_delete_columns_multi_rows(self):
        "delete columns from multiple rows"
        conn = init()
        r = conn.execute('SELECT "col" FROM Standard1 WHERE KEY = "kc"')
        assert len(r[0].columns) == 1
        r = conn.execute('SELECT "col" FROM Standard1 WHERE KEY = "kd"')
        assert len(r[0].columns) == 1

        conn.execute('DELETE "col" FROM Standard1 WHERE KEY IN ("kc", "kd")')
        r = conn.execute('SELECT "col" FROM Standard1 WHERE KEY = "kc"')
        assert len(r[0].columns) == 0
        r = conn.execute('SELECT "col" FROM Standard1 WHERE KEY = "kd"')
        assert len(r[0].columns) == 0

    def test_delete_rows(self):
        "delete entire rows"
        conn = init()
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]
        conn.execute('DELETE FROM Standard1 WHERE KEY = "kd"')
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert len(r[0].columns) == 0
        
    def test_create_keyspace(self):
        "create a new keyspace"
        init().execute("""
        CREATE KEYSPACE TestKeyspace42 WITH strategy_options:DC1 = 1"
            AND strategy_class = "SimpleStrategy" AND replication_factor = 3
        """)
        
        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("TestKeyspace42")
        
        assert ksdef.replication_factor == 3
        strategy_class = "org.apache.cassandra.locator.SimpleStrategy"
        assert ksdef.strategy_class == strategy_class
        assert ksdef.strategy_options['DC1'] == "1"

    def test_time_uuid(self):
        "store and retrieve time-based (type 1) uuids"
        conn = init()
        
        # Store and retrieve a timeuuid using it's hex-formatted string
        timeuuid = uuid.uuid1()
        conn.execute("""
            UPDATE Standard2 SET timeuuid("%s") = 10 WHERE KEY = "uuidtest"
        """ % str(timeuuid))
        
        r = conn.execute("""
            SELECT timeuuid("%s") FROM Standard2 WHERE KEY = "uuidtest"
        """ % str(timeuuid))
        assert r[0].columns[0].name == timeuuid.bytes
        
        # Tests a node-side conversion from long to UUID.
        ms = uuid1bytes_to_millis(uuid.uuid1().bytes)
        conn.execute("""
            UPDATE Standard2 SET "id" = timeuuid(%d) WHERE KEY = "uuidtest"
        """ % ms)
        
        r = conn.execute('SELECT "id" FROM Standard2 WHERE KEY = "uuidtest"')
        assert uuid1bytes_to_millis(r[0].columns[0].value) == ms
        
        # Tests a node-side conversion from ISO8601 to UUID.
        conn.execute("""
            UPDATE Standard2 SET "id2" = timeuuid("2011-01-31 17:00:00-0000")
                    WHERE KEY = "uuidtest"
        """)
        
        r = conn.execute('SELECT "id2" FROM Standard2 WHERE KEY = "uuidtest"')
        # 2011-01-31 17:00:00-0000 == 1296493200000ms
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ms == 1296493200000, \
                "%d != 1296493200000 (2011-01-31 17:00:00-0000)" % ms

        # Tests node-side conversion of empty term to UUID
        conn.execute("""
            UPDATE Standard2 SET "id3" = timeuuid() WHERE KEY = "uuidtest"
        """)
        
        r = conn.execute('SELECT "id3" FROM Standard2 WHERE KEY = "uuidtest"')
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ((time.time() * 1e3) - ms) < 100, \
            "timeuuid() not within 100ms of now (UPDATE vs. SELECT)"
            
        # Tests node-side conversion of timeuuid("now") to UUID
        conn.execute("""
            UPDATE Standard2 SET "id4" = timeuuid("now") WHERE KEY = "uuidtest"
        """)
        
        r = conn.execute('SELECT "id4" FROM Standard2 WHERE KEY = "uuidtest"')
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ((time.time() * 1e3) - ms) < 100, \
            "timeuuid(\"now\") not within 100ms of now (UPDATE vs. SELECT)"
        
        # TODO: slices of timeuuids from cf w/ TimeUUIDType comparator
        
    def test_lexical_uuid(self):
        "store and retrieve lexical uuids"
        conn = init()
        uid = uuid.uuid4()
        conn.execute("""
            UPDATE Standard2 SET uuid("%s") = 10 WHERE KEY = "uuidtest"
        """ % str(uid))
        
        r = conn.execute("""
            SELECT uuid("%s") FROM Standard2 WHERE KEY = "uuidtest"
        """ % str(uid))
        assert r[0].columns[0].name == uid.bytes
        
        # TODO: slices of uuids from cf w/ LexicalUUIDType comparator

