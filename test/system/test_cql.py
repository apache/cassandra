
from os.path import abspath, dirname, join
import sys

sys.path.append(join(abspath(dirname(__file__)), '../../drivers/py'))

from cql import Connection, CQLException
from . import AvroTester
from avro_utils import assert_raises

def load_sample(dbconn):
    dbconn.execute("""
        UPDATE
            Standard1
        WITH
            ROW("ka", COL("ca1", "va1"), COL("col", "val")) AND
            ROW("kb", COL("cb1", "vb1"), COL("col", "val")) AND
            ROW("kc", COL("cc1", "vc1"), COL("col", "val")) AND
            ROW("kd", COL("cd1", "vd1"), COL("col", "val"));
    """)
    dbconn.execute("""
      UPDATE
          StandardLong1
      WITH
        ROW("aa", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
        ROW("ab", COL(5L, "5"), COL(6L, "6"), COL(7L, "8"), COL(9L, "9")) AND
        ROW("ac", COL(9L, "9"), COL(8L, "8"), COL(7L, "7"), COL(6L, "6")) AND
        ROW("ad", COL(5L, "5"), COL(4L, "4"), COL(3L, "3"), COL(2L, "2")) AND
        ROW("ae", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
        ROW("af", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
        ROW("ag", COL(5L, "5"), COL(6L, "6"), COL(7L, "8"), COL(9L, "9"));
    """)
    dbconn.execute("""
        UPDATE
            Indexed1
        WITH
            ROW("asmith",   COL("birthdate", 100L), COL("unindexed", 250L)) AND
            ROW("dozer",    COL("birthdate", 100L), COL("unindexed", 200L)) AND
            ROW("morpheus", COL("birthdate", 175L), COL("unindexed", 200L)) AND
            ROW("neo",      COL("birthdate", 150L), COL("unindexed", 250L)) AND
            ROW("trinity",  COL("birthdate", 125L), COL("unindexed", 200L));
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
