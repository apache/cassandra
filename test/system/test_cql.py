
from os.path import abspath, dirname, join
import sys

sys.path.append(join(abspath(dirname(__file__)), '../../drivers/py'))

from cql import Connection
from . import AvroTester

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
    #dbconn.execute("""
    #  UPDATE
    #      StandardLong1
    #  WITH
    #    ROW("aa", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
    #    ROW("ab", COL(5L, "5"), COL(6L, "6"), COL(7L, "8"), COL(9L, "9")) AND
    #    ROW("ac", COL(9L, "9"), COL(8L, "8"), COL(7L, "7"), COL(6L, "6")) AND
    #    ROW("ad", COL(5L, "5"), COL(4L, "4"), COL(3L, "3"), COL(2L, "2")) AND
    #    ROW("ae", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
    #    ROW("af", COL(1L, "1"), COL(2L, "2"), COL(3L, "3"), COL(4L, "4")) AND
    #    ROW("ag", COL(5L, "5"), COL(6L, "6"), COL(7L, "8"), COL(9L, "9")));
    #""")

def init(keyspace="Keyspace1"):
    dbconn = Connection(keyspace, 'localhost', 9170)
    load_sample(dbconn)
    return dbconn

class TestCql(AvroTester):
    def test_select_simple(self):
        "retrieve a column"
        conn = init()
        r = conn.execute('SELECT FROM Standard1 WHERE KEY="ka" AND COL="ca1"')
        assert r[0]['key'] == 'ka'
        assert r[0]['columns'][0]['name'] == 'ca1'
        assert r[0]['columns'][0]['value'] == 'va1'

    def test_select_columns(self):
        "retrieve multiple columns"
        conn = init()
        r = conn.execute("""
            SELECT FROM Standard1 WHERE KEY = "kd" AND COLUMN = "cd1"
                    AND COLUMN = "col"
        """)
        assert "cd1" in [i['name'] for i in r[0]['columns']]
        assert "col" in [i['name'] for i in r[0]['columns']]

    def test_select_rows_columns(self):
        "fetch multiple rows and columns"
        conn = init()
        r = conn.execute("""
            SELECT FROM
                Standard1
            WHERE
                KEY = "ka" AND KEY = "kd" AND COLUMN = "col";
        """)
        for result in r:
            assert result['key'] in ("ka", "kd")
            assert result['columns'][0]['name'] == "col"
            assert result['columns'][0]['value'] == "val"

    def test_select_rows(self):
        "fetch multiple rows, all columns"
        conn = init()
        r = conn.execute("""
            SELECT FROM
                Standard1
            WHERE
                KEY = "ka" AND KEY = "kd" AND KEY = "kb"
        """)
        for result in r:
            assert result['key'] in ("ka", "kd", "kb")
            assert len(result['columns']) == 2

    #def test_select_row_range(self):
    #    "retrieve a range of rows with columns"
    #    conn = init()
    #    r = conn.execute("""
    #        SELECT FROM Standard1Long WHERE KEY > "ad" AND KEY < "ag";
    #    """)
