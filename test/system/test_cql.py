
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
        CREATE COLUMNFAMILY StandardString1 WITH comparator = ascii
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardUtf82 WITH comparator = utf8
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardLongA WITH comparator = long
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardIntegerA WITH comparator = int
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardUUID WITH comparator = uuid
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardTimeUUID WITH comparator = timeuuid
            AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardTimeUUIDValues WITH comparator = ascii
            AND default_validation = timeuuid;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY IndexedA ('birthdate' long) WITH comparator = ascii
            AND default_validation = ascii;
    """)
    dbconn.execute("CREATE INDEX ON IndexedA ('birthdate');")
    
    query = "UPDATE StandardString1 SET ? = ?, ? = ? WHERE KEY = ?"
    dbconn.execute(query, "ca1", "va1", "col", "val", "ka")
    dbconn.execute(query, "cb1", "vb1", "col", "val", "kb")
    dbconn.execute(query, "cc1", "vc1", "col", "val", "kc")
    dbconn.execute(query, "cd1", "vd1", "col", "val", "kd")

    dbconn.execute("""
        UPDATE StandardUtf82 SET u'%s' = 've1', 'col' = 'val' WHERE KEY = 'kd'
    """ % u'\xa9'.encode('utf8'))
    dbconn.execute("""
        UPDATE StandardUtf82 SET u'cf1' = 'vf1', 'col' = 'val' WHERE KEY = 'kd'
    """)

    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY ONE
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='aa';
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ab';
     UPDATE StandardLongA SET 9='9', 8='8', 7='7', 6='6' WHERE KEY='ac';
     UPDATE StandardLongA SET 5='5', 4='4', 3='3', 2='2' WHERE KEY='ad';
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='ae';
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='af';
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ag';
    APPLY BATCH
    """)
    
    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY ONE
      UPDATE StandardIntegerA SET 10='a', 20='b', 30='c', 40='d' WHERE KEY='k1';
      UPDATE StandardIntegerA SET 10='e', 20='f', 30='g', 40='h' WHERE KEY='k2';
      UPDATE StandardIntegerA SET 10='i', 20='j', 30='k', 40='l' WHERE KEY='k3';
      UPDATE StandardIntegerA SET 10='m', 20='n', 30='o', 40='p' WHERE KEY='k4';
      UPDATE StandardIntegerA SET 10='q', 20='r', 30='s', 40='t' WHERE KEY='k5';
      UPDATE StandardIntegerA SET 10='u', 20='v', 30='w', 40='x' WHERE KEY='k6';
      UPDATE StandardIntegerA SET 10='y', 20='z', 30='A', 40='B' WHERE KEY='k7';
    APPLY BATCH
    """)

    dbconn.execute("""
    BEGIN BATCH
    UPDATE IndexedA SET 'birthdate'=100, 'unindexed'=250 WHERE KEY='asmith';
    UPDATE IndexedA SET 'birthdate'=100, 'unindexed'=200 WHERE KEY='dozer';
    UPDATE IndexedA SET 'birthdate'=175, 'unindexed'=200 WHERE KEY='morpheus';
    UPDATE IndexedA SET 'birthdate'=150, 'unindexed'=250 WHERE KEY='neo';
    UPDATE IndexedA SET 'birthdate'=125, 'unindexed'=200 WHERE KEY='trinity';
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
        r = conn.execute("SELECT 'ca1' FROM StandardString1 WHERE KEY='ka'")
        assert r[0].key == 'ka'
        assert r[0].columns[0].name == 'ca1'
        assert r[0].columns[0].value == 'va1'

    def test_select_columns(self):
        "retrieve multiple columns"
        conn = init()
        r = conn.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]

    def test_select_row_range(self):
        "retrieve a range of rows with columns"
        conn = init()
        r = conn.execute("""
            SELECT 4 FROM StandardLongA WHERE KEY > 'ad' AND KEY < 'ag';
        """)
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
            SELECT 1,5,9 FROM StandardLongA WHERE KEY > 'aa'
                    AND KEY < 'ag' LIMIT 3
        """)
        assert len(r) == 3
        
        r = conn.execute("""
            SELECT 20,40 FROM StandardIntegerA WHERE KEY > 'k1'
                    AND KEY < 'k7' LIMIT 5
        """)
        assert len(r) == 5
        r[0].key == "k1"
        r[4].key == "k5"

    def test_select_columns_slice(self):
        "range of columns (slice) by row"
        conn = init()
        r = conn.execute("SELECT 1..3 FROM StandardLongA WHERE KEY = 'aa';")
        assert len(r) == 1
        assert r[0].columns[0].value == "1"
        assert r[0].columns[1].value == "2"
        assert r[0].columns[2].value == "3"
        
        r = conn.execute("SELECT 10..30 FROM StandardIntegerA WHERE KEY='k1'")
        assert len(r) == 1
        assert r[0].columns[0].value == "a"
        assert r[0].columns[1].value == "b"
        assert r[0].columns[2].value == "c"

    def test_select_columns_slice_with_limit(self):
        "range of columns (slice) by row with limit"
        conn = init()
        r = conn.execute("""
            SELECT FIRST 1 1..3 FROM StandardLongA WHERE KEY = 'aa';
        """)
        assert len(r) == 1
        assert len(r[0].columns) == 1
        assert r[0].columns[0].value == "1"

    def test_select_columns_slice_reversed(self):
        "range of columns (slice) by row reversed"
        conn = init()
        r = conn.execute("""
            SELECT FIRST 2 REVERSED 3..1 FROM StandardLongA WHERE KEY = 'aa';
        """)
        assert len(r) == 1, "%d != 1" % len(r)
        assert len(r[0].columns) == 2
        assert r[0].columns[0].value == "3"
        assert r[0].columns[1].value == "2"

    def test_error_on_multiple_key_by(self):
        "ensure multiple key-bys in where clause excepts"
        conn = init()
        assert_raises(CQLException, conn.execute, """
            SELECT 'col' FROM StandardString1 WHERE KEY = 'ka' AND KEY = 'kb';
        """)

    def test_index_scan_equality(self):
        "indexed scan where column equals value"
        conn = init()
        r = conn.execute("""
            SELECT 'birthdate' FROM IndexedA WHERE 'birthdate' = 100
        """)
        assert len(r) == 2
        assert r[0].key == "asmith"
        assert r[1].key == "dozer"
        assert len(r[0].columns) == 1
        assert len(r[1].columns) == 1

    def test_index_scan_greater_than(self):
        "indexed scan where a column is greater than a value"
        conn = init()
        r = conn.execute("""
            SELECT 'birthdate' FROM IndexedA WHERE 'birthdate' = 100
                    AND 'unindexed' > 200
        """)
        assert len(r) == 1
        assert r[0].key == "asmith"

    def test_index_scan_with_start_key(self):
        "indexed scan with a starting key"
        conn = init()
        r = conn.execute("""
            SELECT 'birthdate' FROM IndexedA WHERE 'birthdate' = 100
                    AND KEY > 'asmithZ'
        """)
        assert len(r) == 1
        assert r[0].key == "dozer"

    def test_no_where_clause(self):
        "empty where clause (range query w/o start key)"
        conn = init()
        r = conn.execute("SELECT 'col' FROM StandardString1 LIMIT 3")
        assert len(r) == 3
        assert r[0].key == "ka"
        assert r[1].key == "kb"
        assert r[2].key == "kc"

    def test_column_count(self):
        "getting a result count instead of results"
        conn = init()
        r = conn.execute("""
            SELECT COUNT(1..4) FROM StandardLongA WHERE KEY = 'aa';
        """)
        assert r == 4, "expected 4 results, got %d" % (r and r or 0)

    def test_truncate_columnfamily(self):
        "truncating a column family"
        conn = init()
        conn.execute('TRUNCATE StandardString1;')
        r = conn.execute("SELECT 'cd1' FROM StandardString1 WHERE KEY = 'kd'")
        assert len(r) == 0

    def test_delete_columns(self):
        "delete columns from a row"
        conn = init()
        r = conn.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]
        conn.execute("""
            DELETE 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        r = conn.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert len(r[0].columns) == 0

    def test_delete_columns_multi_rows(self):
        "delete columns from multiple rows"
        conn = init()
        r = conn.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        assert len(r[0].columns) == 1
        r = conn.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        assert len(r[0].columns) == 1

        conn.execute("""
            DELETE 'col' FROM StandardString1 WHERE KEY IN ('kc', 'kd')
        """)
        r = conn.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        assert len(r[0].columns) == 0
        r = conn.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        assert len(r[0].columns) == 0

    def test_delete_rows(self):
        "delete entire rows"
        conn = init()
        r = conn.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert "cd1" in [i.name for i in r[0].columns]
        assert "col" in [i.name for i in r[0].columns]
        conn.execute("DELETE FROM StandardString1 WHERE KEY = 'kd'")
        r = conn.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert len(r[0].columns) == 0
        
    def test_create_keyspace(self):
        "create a new keyspace"
        init().execute("""
        CREATE KEYSPACE TestKeyspace42 WITH strategy_options:DC1 = '1'
            AND strategy_class = 'SimpleStrategy' AND replication_factor = 3
        """)
        
        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("TestKeyspace42")
        
        assert ksdef.replication_factor == 3
        strategy_class = "org.apache.cassandra.locator.SimpleStrategy"
        assert ksdef.strategy_class == strategy_class
        assert ksdef.strategy_options['DC1'] == "1"
        
    def test_drop_keyspace(self):
        "removing a keyspace"
        conn = init()
        conn.execute("""
        CREATE KEYSPACE Keyspace4Drop
            WITH strategy_class = 'SimpleStrategy' AND replication_factor = 1
        """)
        
        # TODO: temporary (until this can be done with CQL).
        thrift_client.describe_keyspace("Keyspace4Drop")
        
        conn.execute('DROP KEYSPACE Keyspace4Drop;')
        
        # Technically this should throw a ttypes.NotFound(), but this is
        # temporary and so not worth requiring it on PYTHONPATH.
        assert_raises(Exception,
                      thrift_client.describe_keyspace,
                      "Keyspace4Drop")
        
    def test_create_column_family(self):
        "create a new column family"
        conn = init()
        conn.execute("""
            CREATE KEYSPACE CreateCFKeyspace WITH replication_factor = 1
                AND strategy_class = 'SimpleStrategy';
        """)
        conn.execute("USE CreateCFKeyspace;")
        
        conn.execute("""
            CREATE COLUMNFAMILY NewCf1 (
                'username' utf8,
                'age' int,
                'birthdate' long,
                'id' uuid
            ) WITH comparator = utf8 AND comment = 'shiny, new, cf' AND
                    default_validation = ascii;
        """)
        
        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 1, \
            "expected 1 column family total, found %d" % len(ksdef.cf_defs)
        cfam= ksdef.cf_defs[0]
        assert len(cfam.column_metadata) == 4, \
            "expected 4 columns, found %d" % len(cfam.column_metadata)
        assert cfam.comment == "shiny, new, cf"
        assert cfam.default_validation_class == "org.apache.cassandra.db.marshal.AsciiType"
        assert cfam.comparator_type == "org.apache.cassandra.db.marshal.UTF8Type"
        
        # No column defs, defaults all-around
        conn.execute("CREATE COLUMNFAMILY NewCf2")
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 2, \
            "expected 2 column families total, found %d" % len(ksdef.cf_defs)
        
        # No column defs
        conn.execute("CREATE COLUMNFAMILY NewCf3 WITH comparator = long")
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 3, \
            "expected 3 column families total, found %d" % len(ksdef.cf_defs)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf3"][0]
        assert cfam.comparator_type == "org.apache.cassandra.db.marshal.LongType"
        
        # Column defs, defaults otherwise
        conn.execute("CREATE COLUMNFAMILY NewCf4 ('a' int, 'b' int);")
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 4, \
            "expected 4 column families total, found %d" % len(ksdef.cf_defs)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf4"][0]
        assert len(cfam.column_metadata) == 2, \
            "expected 2 columns, found %d" % len(cfam.column_metadata)
        for coldef in cfam.column_metadata:
            assert coldef.name in ("a", "b"), "Unknown column name"
            assert coldef.validation_class.endswith("marshal.IntegerType")
            
    def test_drop_columnfamily(self):
        "removing a column family"
        conn = init()
        conn.execute("""
            CREATE KEYSPACE Keyspace4CFDrop WITH replication_factor = 1
                AND strategy_class = 'SimpleStrategy';
        """)
        conn.execute('USE Keyspace4CFDrop;')
        conn.execute('CREATE COLUMNFAMILY CF4Drop;')
        
        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("Keyspace4CFDrop")
        assert len(ksdef.cf_defs), "Column family not created!"
        
        conn.execute('DROP COLUMNFAMILY CF4Drop;')
        
        ksdef = thrift_client.describe_keyspace("Keyspace4CFDrop")
        assert not len(ksdef.cf_defs), "Column family not deleted!"
            
    def test_create_indexs(self):
        "creating column indexes"
        conn = init()
        conn.execute("USE Keyspace1")
        conn.execute("CREATE COLUMNFAMILY CreateIndex1")
        conn.execute("CREATE INDEX namedIndex ON CreateIndex1 ('items')")
        conn.execute("CREATE INDEX ON CreateIndex1 ('stuff')")
        
        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("Keyspace1")
        cfam = [i for i in ksdef.cf_defs if i.name == "CreateIndex1"][0]
        items = [i for i in cfam.column_metadata if i.name == "items"][0]
        stuff = [i for i in cfam.column_metadata if i.name == "stuff"][0]
        assert items.index_name == "namedIndex", "missing index (or name)"
        assert items.index_type == 0, "missing index"
        assert not stuff.index_name, \
            "index_name should be unset, not %s" % stuff.index_name
        assert stuff.index_type == 0, "missing index"

        assert_raises(CQLException,
                      conn.execute,
                      "CREATE INDEX ON CreateIndex1 (\"stuff\")")

    def test_time_uuid(self):
        "store and retrieve time-based (type 1) uuids"
        conn = init()
        
        # Store and retrieve a timeuuid using it's hex-formatted string
        timeuuid = uuid.uuid1()
        conn.execute("""
            UPDATE StandardTimeUUID SET timeuuid('%s') = 10
                    WHERE KEY = 'uuidtest'
        """ % str(timeuuid))
        
        r = conn.execute("""
            SELECT timeuuid('%s') FROM StandardTimeUUID WHERE KEY = 'uuidtest'
        """ % str(timeuuid))
        assert r[0].columns[0].name == timeuuid.bytes
        
        # Tests a node-side conversion from long to UUID.
        ms = uuid1bytes_to_millis(uuid.uuid1().bytes)
        conn.execute("""
            UPDATE StandardTimeUUIDValues SET 'id' = timeuuid(%d)
                WHERE KEY = 'uuidtest'
        """ % ms)
        
        r = conn.execute("""
            SELECT 'id' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        assert uuid1bytes_to_millis(r[0].columns[0].value) == ms
        
        # Tests a node-side conversion from ISO8601 to UUID.
        conn.execute("""
            UPDATE StandardTimeUUIDValues
            SET 'id2' = timeuuid('2011-01-31 17:00:00-0000') 
            WHERE KEY = 'uuidtest'
        """)
        
        r = conn.execute("""
            SELECT 'id2' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        # 2011-01-31 17:00:00-0000 == 1296493200000ms
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ms == 1296493200000, \
                "%d != 1296493200000 (2011-01-31 17:00:00-0000)" % ms

        # Tests node-side conversion of empty term to UUID
        conn.execute("""
            UPDATE StandardTimeUUIDValues SET 'id3' = timeuuid()
                    WHERE KEY = 'uuidtest'
        """)
        
        r = conn.execute("""
            SELECT 'id3' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ((time.time() * 1e3) - ms) < 100, \
            "timeuuid() not within 100ms of now (UPDATE vs. SELECT)"
            
        # Tests node-side conversion of timeuuid("now") to UUID
        conn.execute("""
            UPDATE StandardTimeUUIDValues SET 'id4' = timeuuid('now')
                    WHERE KEY = 'uuidtest'
        """)
        
        r = conn.execute("""
            SELECT 'id4' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        ms = uuid1bytes_to_millis(r[0].columns[0].value)
        assert ((time.time() * 1e3) - ms) < 100, \
            "timeuuid(\"now\") not within 100ms of now (UPDATE vs. SELECT)"
        
        # TODO: slices of timeuuids from cf w/ TimeUUIDType comparator
        
    def test_lexical_uuid(self):
        "store and retrieve lexical uuids"
        conn = init()
        uid = uuid.uuid4()
        conn.execute("UPDATE StandardUUID SET ? = 10 WHERE KEY = 'uuidtest'",
                     uid)
        
        r = conn.execute("SELECT ? FROM StandardUUID WHERE KEY = 'uuidtest'",
                         uid)
        assert r[0].columns[0].name == uid.bytes
        
        # TODO: slices of uuids from cf w/ LexicalUUIDType comparator

