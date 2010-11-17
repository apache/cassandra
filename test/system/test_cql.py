
from os.path import abspath, dirname, join
import sys

sys.path.append(join(abspath(dirname(__file__)), '../../drivers/py'))

from cql import Connection, CQLException
from . import AvroTester
from avro_utils import assert_raises

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
    BEGIN BATCH
    UPDATE Indexed1 SET "birthdate"=100L, "unindexed"=250L WHERE KEY="asmith";
    UPDATE Indexed1 SET "birthdate"=100L, "unindexed"=200L WHERE KEY="dozer";
    UPDATE Indexed1 SET "birthdate"=175L, "unindexed"=200L WHERE KEY="morpheus";
    UPDATE Indexed1 SET "birthdate"=150L, "unindexed"=250L WHERE KEY="neo";
    UPDATE Indexed1 SET "birthdate"=125L, "unindexed"=200L WHERE KEY="trinity";
    APPLY BATCH
    """)

def init(keyspace="Keyspace1"):
    dbconn = Connection(keyspace, 'localhost', 9170)
    load_sample(dbconn)
    return dbconn

class TestCql(AvroTester):
    def test_select_simple(self):
        "retrieve a column"
        conn = init()
        r = conn.execute('SELECT "ca1" FROM Standard1 WHERE KEY="ka"')
        assert r[0]['key'] == 'ka'
        assert r[0]['columns'][0]['name'] == 'ca1'
        assert r[0]['columns'][0]['value'] == 'va1'

    def test_select_columns(self):
        "retrieve multiple columns"
        conn = init()
        r = conn.execute('SELECT "cd1", "col" FROM Standard1 WHERE KEY = "kd"')
        assert "cd1" in [i['name'] for i in r[0]['columns']]
        assert "col" in [i['name'] for i in r[0]['columns']]

    def test_select_row_range(self):
        "retrieve a range of rows with columns"
        conn = init()
        r = conn.execute('SELECT 4L FROM StandardLong1 WHERE KEY > "ad" AND KEY < "ag";')
        assert len(r) == 3
        assert r[0]['key'] == "ad"
        assert r[1]['key'] == "ae"
        assert r[2]['key'] == "af"
        assert len(r[0]['columns']) == 1
        assert len(r[1]['columns']) == 1
        assert len(r[2]['columns']) == 1

    def test_select_row_range_with_limit(self):
        "retrieve a limited range of rows with columns"
        conn = init()
        r = conn.execute("""
            SELECT 1L,5L,9L FROM StandardLong1 WHERE KEY > "aa" AND KEY < "ag" LIMIT 3
        """)
        assert len(r) == 3

    def test_select_columns_slice(self):
        "range of columns (slice) by row"
        conn = init()
        r = conn.execute('SELECT 1L..3L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1
        assert r[0]['columns'][0]['value'] == "1"
        assert r[0]['columns'][1]['value'] == "2"
        assert r[0]['columns'][2]['value'] == "3"

    def test_select_columns_slice_with_limit(self):
        "range of columns (slice) by row with limit"
        conn = init()
        r = conn.execute('SELECT FIRST 1 1L..3L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1
        assert len(r[0]['columns']) == 1
        assert r[0]['columns'][0]['value'] == "1"

    def test_select_columns_slice_reversed(self):
        "range of columns (slice) by row reversed"
        conn = init()
        r = conn.execute('SELECT FIRST 2 REVERSED 3L..1L FROM StandardLong1 WHERE KEY = "aa";')
        assert len(r) == 1, "%d != 1" % len(r)
        assert len(r[0]['columns']) == 2
        assert r[0]['columns'][0]['value'] == "3"
        assert r[0]['columns'][1]['value'] == "2"

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
        assert r[0]['key'] == "asmith"
        assert r[1]['key'] == "dozer"
        assert len(r[0]['columns']) == 1
        assert len(r[1]['columns']) == 1

    def test_index_scan_greater_than(self):
        "indexed scan where a column is greater than a value"
        conn = init()
        r = conn.execute("""
            SELECT "birthdate" FROM Indexed1 WHERE "birthdate" = 100L AND "unindexed" > 200L
        """)
        assert len(r) == 1
        assert r[0]['key'] == "asmith"

    def test_index_scan_with_start_key(self):
        "indexed scan with a starting key"
        conn = init()
        r = conn.execute("""
            SELECT "birthdate" FROM Indexed1 WHERE "birthdate" = 100L AND KEY > "asmithZ"
        """)
        assert len(r) == 1
        assert r[0]['key'] == "dozer"

    def test_no_where_clause(self):
        "empty where clause (range query w/o start key)"
        conn = init()
        r = conn.execute('SELECT "col" FROM Standard1 LIMIT 3')
        assert len(r) == 3
        assert r[0]['key'] == "ka"
        assert r[1]['key'] == "kb"
        assert r[2]['key'] == "kc"

    def test_column_count(self):
        "getting a result count instead of results"
        conn = init()
        r = conn.execute('SELECT COUNT(1L..4L) FROM StandardLong1 WHERE KEY = "aa";')
        assert r == 4

