# -*- coding: utf-8 -*-
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

# to run a single test, run from trunk/:
# PYTHONPATH=test nosetests --tests=system.test_cql:TestCql.test_column_count

from os.path import abspath, dirname, join
import sys, uuid, time

sys.path.append(join(abspath(dirname(__file__)), '../../drivers/py'))

import cql
from cql.connection import Connection
from __init__ import ThriftTester
from __init__ import thrift_client     # TODO: temporary

def assert_raises(exception, method, *args, **kwargs):
    try:
        method(*args, **kwargs)
    except exception:
        return
    raise AssertionError("failed to see expected exception")

def uuid1bytes_to_millis(uuidbytes):
    return (uuid.UUID(bytes=uuidbytes).get_time() / 10000) - 12219292800000L

def load_sample(dbconn):
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardString1 (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardString2 (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardUtf82 (KEY text PRIMARY KEY)
            WITH comparator = text AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardLongA (KEY text PRIMARY KEY)
            WITH comparator = bigint AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardIntegerA (KEY text PRIMARY KEY)
            WITH comparator = varint AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardUUID (KEY text PRIMARY KEY)
            WITH comparator = uuid AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardTimeUUID (KEY text PRIMARY KEY)
            WITH comparator = uuid AND default_validation = ascii;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY StandardTimeUUIDValues (KEY text PRIMARY KEY)
            WITH comparator = ascii AND default_validation = uuid;
    """)
    dbconn.execute("""
        CREATE COLUMNFAMILY IndexedA (KEY text PRIMARY KEY, birthdate int)
            WITH comparator = ascii AND default_validation = ascii;
    """)
    dbconn.execute("CREATE INDEX ON IndexedA (birthdate)")

    query = "UPDATE StandardString1 SET :c1 = :v1, :c2 = :v2 WHERE KEY = :key"
    dbconn.execute(query, dict(c1="ca1", v1="va1", c2="col", v2="val", key="ka"))
    dbconn.execute(query, dict(c1="cb1", v1="vb1", c2="col", v2="val", key="kb"))
    dbconn.execute(query, dict(c1="cc1", v1="vc1", c2="col", v2="val", key="kc"))
    dbconn.execute(query, dict(c1="cd1", v1="vd1", c2="col", v2="val", key="kd"))

    dbconn.execute("""
    BEGIN BATCH USING CONSISTENCY ONE
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='aa'
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ab'
     UPDATE StandardLongA SET 9='9', 8='8', 7='7', 6='6' WHERE KEY='ac'
     UPDATE StandardLongA SET 5='5', 4='4', 3='3', 2='2' WHERE KEY='ad'
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='ae'
     UPDATE StandardLongA SET 1='1', 2='2', 3='3', 4='4' WHERE KEY='af'
     UPDATE StandardLongA SET 5='5', 6='6', 7='8', 9='9' WHERE KEY='ag'
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
    dbconn = cql.connect('localhost', 9170, keyspace)
    cursor = dbconn.cursor()
    load_sample(cursor)
    return cursor

class TestCql(ThriftTester):
    def test_select_simple(self):
        "single-row named column queries"
        cursor = init()
        cursor.execute("SELECT KEY, ca1 FROM StandardString1 WHERE KEY='ka'")
        r = cursor.fetchone()
        d = cursor.description

        assert d[0][0] == 'KEY'
        assert r[0] == 'ka'

        assert d[1][0] == 'ca1'
        assert r[1] == 'va1'

        # retrieve multiple columns
        # (we deliberately request columns in non-comparator order)
        cursor.execute("""
            SELECT ca1, col, cd1 FROM StandardString1 WHERE KEY = 'kd'
        """)

        d = cursor.description
        assert ['ca1', 'col', 'cd1'] == [col_dscptn[0] for col_dscptn in d], d
        row = cursor.fetchone()
        # check that the column that didn't exist in the row comes back as null
        assert [None, 'val', 'vd1'] == row, row

    def test_select_row_range(self):
        "retrieve a range of rows with columns"
        cursor = init()

        # everything
        cursor.execute("SELECT * FROM StandardLongA")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'] == keys, keys

        # [start, end], mid-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'ad' AND KEY <= 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['ad', 'ae', 'af', 'ag'] == keys, keys

        # (start, end), mid-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'ad' AND KEY < 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['ae', 'af'] == keys, keys

        # [start, end], full-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'aa' AND KEY <= 'ag'")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'] == keys, keys

        # (start, end), full-row
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' AND KEY < 'g'")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'] == keys, keys

        # LIMIT tests

        # no WHERE
        cursor.execute("SELECT * FROM StandardLongA LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa'] == keys, keys

        # with >=, non-existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'a' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa'] == keys, keys

        # with >=, existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY >= 'aa' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa'] == keys, keys

        # with >, non-existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa'] == keys, keys

        # with >, existing key
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'aa' LIMIT 1")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['ab'] == keys, keys

        # with both > and <, existing keys
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'aa' and KEY < 'ag' LIMIT 5")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['ab', 'ac', 'ad', 'ae', 'af'] == keys, keys

        # with both > and <, non-existing keys
        cursor.execute("SELECT * FROM StandardLongA WHERE KEY > 'a' and KEY < 'b' LIMIT 5")
        keys = [row[0] for row in cursor.fetchall()]
        assert ['aa', 'ab', 'ac', 'ad', 'ae'] == keys, keys

    def test_select_columns_slice(self):
        "column slice tests"
        cursor = init()

        # * includes row key, explicit slice does not
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = 'ka';")
        row = cursor.fetchone()
        assert ['ka', 'va1', 'val'] == row, row

        cursor.execute("SELECT ''..'' FROM StandardString1 WHERE KEY = 'ka';")
        row = cursor.fetchone()
        assert ['va1', 'val'] == row, row

        # column subsets
        cursor.execute("SELECT 1..3 FROM StandardLongA WHERE KEY = 'aa';")
        assert cursor.rowcount == 1
        row = cursor.fetchone()
        assert ['1', '2', '3'] == row, row
        
        cursor.execute("""
            SELECT key,20,40 FROM StandardIntegerA
            WHERE KEY > 'k1' AND KEY < 'k7' LIMIT 5
        """)
        row = cursor.fetchone()
        assert ['k2', 'f', 'h'] == row, row

        # range of columns (slice) by row with FIRST
        cursor.execute("SELECT FIRST 1 1..3 FROM StandardLongA WHERE KEY = 'aa'")
        assert cursor.rowcount == 1
        row = cursor.fetchone()
        assert ['1'] == row, row

        # range of columns (slice) by row reversed
        cursor.execute("SELECT FIRST 2 REVERSED 3..1 FROM StandardLongA WHERE KEY = 'aa'")
        assert cursor.rowcount == 1, "%d != 1" % cursor.rowcount
        row = cursor.fetchone()
        assert ['3', '2'] == row, row

    def test_select_range_with_single_column_results(self):
        "range should not fail when keys were not set"
        cursor = init()
        cursor.execute("""
          BEGIN BATCH
            UPDATE StandardString2 SET name='1',password='pass1' WHERE KEY = 'user1'
            UPDATE StandardString2 SET name='2',password='pass2' WHERE KEY = 'user2'
            UPDATE StandardString2 SET password='pass3' WHERE KEY = 'user3'
          APPLY BATCH
        """)

        cursor.execute("""
          SELECT KEY, name FROM StandardString2
        """)

        assert cursor.rowcount == 3, "expected 3 results, got %d" % cursor.rowcount

        # two of three results should contain one column "name", third should be empty
        for i in range(1, 3):
            r = cursor.fetchone()
            assert len(r) == 2
            assert r[0] == "user%d" % i
            assert r[1] == "%s" % i

        r = cursor.fetchone()
        assert len(r) == 2
        assert r[0] == "user3"
        assert r[1] == None

    def test_error_on_multiple_key_by(self):
        "ensure multiple key-bys in where clause excepts"
        cursor = init()
        assert_raises(cql.ProgrammingError, cursor.execute, """
            SELECT 'col' FROM StandardString1 WHERE KEY = 'ka' AND KEY = 'kb';
        """)

    def test_index_scan_equality(self):
        "indexed scan where column equals value"
        cursor = init()
        cursor.execute("""
            SELECT KEY, birthdate FROM IndexedA WHERE birthdate = 100
        """)
        assert cursor.rowcount == 2

        r = cursor.fetchone()
        assert r[0] == "asmith"
        assert len(r) == 2

        r = cursor.fetchone()
        assert r[0] == "dozer"
        assert len(r) == 2

    def test_index_scan_greater_than(self):
        "indexed scan where a column is greater than a value"
        cursor = init()
        cursor.execute("""
            SELECT KEY, 'birthdate' FROM IndexedA 
            WHERE 'birthdate' = 100 AND 'unindexed' > 200
        """)
        assert cursor.rowcount == 1
        row = cursor.fetchone()
        assert row[0] == "asmith", row

    def test_index_scan_with_start_key(self):
        "indexed scan with a starting key"
        cursor = init()
        cursor.execute("""
            SELECT KEY, 'birthdate' FROM IndexedA 
            WHERE 'birthdate' = 100 AND KEY >= 'asmithZ'
        """)
        assert cursor.rowcount == 1
        r = cursor.fetchone()
        assert r[0] == "dozer"

    def test_no_where_clause(self):
        "empty where clause (range query w/o start key)"
        cursor = init()
        cursor.execute("SELECT KEY, 'col' FROM StandardString1 LIMIT 3")
        assert cursor.rowcount == 3
        rows = cursor.fetchmany(3)
        assert rows[0][0] == "ka"
        assert rows[1][0] == "kb"
        assert rows[2][0] == "kc"

    def test_column_count(self):
        "getting a result count instead of results"
        cursor = init()
        cursor.execute("""
            SELECT COUNT(1..4) FROM StandardLongA WHERE KEY = 'aa';
        """)
        r = cursor.fetchone()
        assert r[0] == 4, "expected 4 results, got %d" % (r and r or 0)

    def test_truncate_columnfamily(self):
        "truncating a column family"
        cursor = init()
        cursor.execute('TRUNCATE StandardString1;')
        cursor.execute("SELECT 'cd1' FROM StandardString1 WHERE KEY = 'kd'")
        assert cursor.rowcount == 0

        # truncate against non-existing CF
        assert_raises(cql.ProgrammingError,
                      cursor.execute,
                      "TRUNCATE notExistingCFAAAABB")

    def test_delete_columns(self):
        "delete columns from a row"
        cursor = init()
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        desc = [col_d[0] for col_d in cursor.description]
        assert ['cd1', 'col'] == desc, desc

        cursor.execute("""
            DELETE 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        row = cursor.fetchone()
        assert [None, None] == row, row

    def test_delete_columns_multi_rows(self):
        "delete columns from multiple rows"
        cursor = init()

        # verify rows exist initially
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        row = cursor.fetchone()
        assert ['val'] == row, row
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        row = cursor.fetchone()
        assert ['val'] == row, row

        # delete and verify data is gone
        cursor.execute("""
            DELETE 'col' FROM StandardString1 WHERE KEY IN ('kc', 'kd')
        """)
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kc'")
        row = cursor.fetchone()
        assert [None] == row, row
        cursor.execute("SELECT 'col' FROM StandardString1 WHERE KEY = 'kd'")
        r = cursor.fetchone()
        assert [None] == r, r

    def test_delete_rows(self):
        "delete entire rows"
        cursor = init()
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        assert ['cd1', 'col'] == [col_d[0] for col_d in cursor.description]
        cursor.execute("DELETE FROM StandardString1 WHERE KEY = 'kd'")
        cursor.execute("""
            SELECT 'cd1', 'col' FROM StandardString1 WHERE KEY = 'kd'
        """)
        row = cursor.fetchone()
        assert [None, None] == row, row

    def test_create_keyspace(self):
        "create a new keyspace"
        cursor = init()
        cursor.execute("""
        CREATE KEYSPACE TestKeyspace42 WITH strategy_options:DC1 = '1'
            AND strategy_class = 'NetworkTopologyStrategy'
        """)

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("TestKeyspace42")

        strategy_class = "org.apache.cassandra.locator.NetworkTopologyStrategy"
        assert ksdef.strategy_class == strategy_class
        assert ksdef.strategy_options['DC1'] == "1"

    def test_drop_keyspace(self):
        "removing a keyspace"
        cursor = init()
        cursor.execute("""
               CREATE KEYSPACE Keyspace4Drop WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy'
        """)

        # TODO: temporary (until this can be done with CQL).
        thrift_client.describe_keyspace("Keyspace4Drop")

        cursor.execute('DROP KEYSPACE Keyspace4Drop;')

        # Technically this should throw a ttypes.NotFound(), but this is
        # temporary and so not worth requiring it on PYTHONPATH.
        assert_raises(Exception,
                      thrift_client.describe_keyspace,
                      "Keyspace4Drop")

    def test_create_column_family(self):
        "create a new column family"
        cursor = init()
        cursor.execute("""
               CREATE KEYSPACE CreateCFKeyspace WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """)
        cursor.execute("USE CreateCFKeyspace;")

        cursor.execute("""
            CREATE COLUMNFAMILY NewCf1 (
                KEY varint PRIMARY KEY,
                'username' text,
                'age' varint,
                'birthdate' bigint,
                'id' uuid
            ) WITH comment = 'shiny, new, cf' AND default_validation = ascii;
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
        assert cfam.key_validation_class == "org.apache.cassandra.db.marshal.IntegerType"

        # Missing primary key
        assert_raises(cql.ProgrammingError, cursor.execute, "CREATE COLUMNFAMILY NewCf2")

        # Too many primary keys
        assert_raises(cql.ProgrammingError,
                      cursor.execute,
                      """CREATE COLUMNFAMILY NewCf2
                             (KEY varint PRIMARY KEY, KEY text PRIMARY KEY)""")

        # No column defs
        cursor.execute("""CREATE COLUMNFAMILY NewCf3
                            (KEY varint PRIMARY KEY) WITH comparator = bigint""")
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 2, \
            "expected 3 column families total, found %d" % len(ksdef.cf_defs)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf3"][0]
        assert cfam.comparator_type == "org.apache.cassandra.db.marshal.LongType"

        # Column defs, defaults otherwise
        cursor.execute("""CREATE COLUMNFAMILY NewCf4
                            (KEY varint PRIMARY KEY, 'a' varint, 'b' varint)
                            WITH comparator = text;""")
        ksdef = thrift_client.describe_keyspace("CreateCFKeyspace")
        assert len(ksdef.cf_defs) == 3, \
            "expected 4 column families total, found %d" % len(ksdef.cf_defs)
        cfam = [i for i in ksdef.cf_defs if i.name == "NewCf4"][0]
        assert len(cfam.column_metadata) == 2, \
            "expected 2 columns, found %d" % len(cfam.column_metadata)
        for coldef in cfam.column_metadata:
            assert coldef.name in ("a", "b"), "Unknown column name " + coldef.name
            assert coldef.validation_class.endswith("marshal.IntegerType")

    def test_drop_columnfamily(self):
        "removing a column family"
        cursor = init()
        cursor.execute("""
               CREATE KEYSPACE Keyspace4CFDrop WITH strategy_options:replication_factor = '1'
                   AND strategy_class = 'SimpleStrategy';
        """)
        cursor.execute('USE Keyspace4CFDrop;')
        cursor.execute('CREATE COLUMNFAMILY CF4Drop (KEY varint PRIMARY KEY);')

        # TODO: temporary (until this can be done with CQL).
        ksdef = thrift_client.describe_keyspace("Keyspace4CFDrop")
        assert len(ksdef.cf_defs), "Column family not created!"

        cursor.execute('DROP COLUMNFAMILY CF4Drop;')

        ksdef = thrift_client.describe_keyspace("Keyspace4CFDrop")
        assert not len(ksdef.cf_defs), "Column family not deleted!"

    def test_create_indexs(self):
        "creating column indexes"
        cursor = init()
        cursor.execute("USE Keyspace1")
        cursor.execute("CREATE COLUMNFAMILY CreateIndex1 (KEY text PRIMARY KEY)")
        cursor.execute("CREATE INDEX namedIndex ON CreateIndex1 (items)")
        cursor.execute("CREATE INDEX ON CreateIndex1 (stuff)")

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

        # already indexed
        assert_raises(cql.ProgrammingError,
                      cursor.execute,
                      "CREATE INDEX ON CreateIndex1 (stuff)")

    def test_time_uuid(self):
        "store and retrieve time-based (type 1) uuids"
        cursor = init()

        # Store and retrieve a timeuuid using it's hex-formatted string
        timeuuid = uuid.uuid1()
        cursor.execute("""
            UPDATE StandardTimeUUID SET '%s' = 10 WHERE KEY = 'uuidtest'
        """ % str(timeuuid))

        cursor.execute("""
            SELECT '%s' FROM StandardTimeUUID WHERE KEY = 'uuidtest'
        """ % str(timeuuid))
        d = cursor.description
        assert d[0][0] == timeuuid, "%s, %s" % (str(d[1][0]), str(timeuuid))

        # Tests a node-side conversion from bigint to UUID.
        ms = uuid1bytes_to_millis(uuid.uuid1().bytes)
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id' = %d WHERE KEY = 'uuidtest'
        """ % ms)

        cursor.execute("""
            SELECT 'id' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        r = cursor.fetchone()
        assert uuid1bytes_to_millis(r[0].bytes) == ms

        # Tests a node-side conversion from ISO8601 to UUID.
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id2' = '2011-01-31 17:00:00-0000'
            WHERE KEY = 'uuidtest'
        """)

        cursor.execute("""
            SELECT 'id2' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        # 2011-01-31 17:00:00-0000 == 1296493200000ms
        r = cursor.fetchone()
        ms = uuid1bytes_to_millis(r[0].bytes)
        assert ms == 1296493200000, \
                "%d != 1296493200000 (2011-01-31 17:00:00-0000)" % ms

        # Tests node-side conversion of timeuuid("now") to UUID
        cursor.execute("""
            UPDATE StandardTimeUUIDValues SET 'id3' = 'now'
                    WHERE KEY = 'uuidtest'
        """)

        cursor.execute("""
            SELECT 'id3' FROM StandardTimeUUIDValues WHERE KEY = 'uuidtest'
        """)
        r = cursor.fetchone()
        ms = uuid1bytes_to_millis(r[0].bytes)
        assert ((time.time() * 1e3) - ms) < 100, \
            "new timeuuid not within 100ms of now (UPDATE vs. SELECT)"

        uuid_range = []
        update = "UPDATE StandardTimeUUID SET :name = :val WHERE KEY = slicetest"
        for i in range(5):
            uuid_range.append(uuid.uuid1())
            cursor.execute(update, dict(name=uuid_range[i], val=i))

        cursor.execute("""
            SELECT :start..:finish FROM StandardTimeUUID WHERE KEY = slicetest
            """, dict(start=uuid_range[0], finish=uuid_range[len(uuid_range)-1]))
        d = cursor.description
        for (i, col_d) in enumerate(d):
            assert uuid_range[i] == col_d[0]


    def test_lexical_uuid(self):
        "store and retrieve lexical uuids"
        cursor = init()
        uid = uuid.uuid4()
        cursor.execute("UPDATE StandardUUID SET :name = 10 WHERE KEY = 'uuidtest'",
                       dict(name=uid))

        cursor.execute("SELECT :name FROM StandardUUID WHERE KEY = 'uuidtest'",
                       dict(name=uid))
        d = cursor.description
        assert d[0][0] == uid, "expected %s, got %s (%s)" % \
                (uid.bytes.encode('hex'), str(d[1][0]).encode('hex'), d[1][1])

        # TODO: slices of uuids from cf w/ LexicalUUIDType comparator

    def test_utf8_read_write(self):
        "reading and writing utf8 values"
        cursor = init()
        # Sorting: ¢ (u00a2) < © (u00a9) < ® (u00ae) < ¿ (u00bf)
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="¿"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="©"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="®"))
        cursor.execute("UPDATE StandardUtf82 SET :name = v1 WHERE KEY = k1", dict(name="¢"))

        cursor.execute("SELECT * FROM StandardUtf82 WHERE KEY = k1")
        d = cursor.description
        assert d[0][0] == 'KEY', d[0][0]
        assert d[1][0] == u"¢", d[1][0]
        assert d[2][0] == u"©", d[2][0]
        assert d[3][0] == u"®", d[3][0]
        assert d[4][0] == u"¿", d[4][0]

        cursor.execute("SELECT :start..'' FROM StandardUtf82 WHERE KEY = k1", dict(start="©"))
        row = cursor.fetchone()
        assert len(row) == 3, row
        d = cursor.description
        assert d[0][0] == u"©"
        assert d[1][0] == u"®"
        assert d[2][0] == u"¿"

    def test_read_write_negative_numerics(self):
        "reading and writing negative numeric values"
        cursor = init()
        for cf in ("StandardIntegerA", "StandardLongA"):
            for i in range(10):
                cursor.execute("UPDATE :cf SET :name = :val WHERE KEY = negatives;",
                               dict(cf=cf, name=-(i + 1), val=i))

            cursor.execute("SELECT :start..:finish FROM :cf WHERE KEY = negatives;",
                           dict(start=-10, finish=-1, cf=cf))
            r = cursor.fetchone()
            assert len(r) == 10, \
                "returned %d columns, expected %d" % (len(r) - 1, 10)
            d = cursor.description
            assert d[0][0] == -10
            assert d[9][0] == -1

    def test_escaped_quotes(self):
        "reading and writing strings w/ escaped quotes"
        cursor = init()

        cursor.execute("""
                       UPDATE StandardString1 SET 'x''and''y' = z WHERE KEY = :key
                       """, dict(key="test_escaped_quotes"))

        cursor.execute("""
                       SELECT 'x''and''y' FROM StandardString1 WHERE KEY = :key
                       """, dict(key="test_escaped_quotes"))
        assert cursor.rowcount == 1
        r = cursor.fetchone()
        assert len(r) == 1, "wrong number of results"
        d = cursor.description
        assert d[0][0] == "x'and'y"
        
    def test_typed_keys(self):
        "using typed keys"
        cursor = init()
        cursor.execute("SELECT * FROM StandardString1 WHERE KEY = :key", dict(key="ka"))
        row = cursor.fetchone()
        assert isinstance(row[0], unicode), \
            "wrong key-type returned, expected unicode, got %s" % type(row[0])

        # FIXME: The above is woefully inadequate, but the test config uses
        # CollatingOrderPreservingPartitioner which only supports UTF8.

    def test_write_using_insert(self):
        "peforming writes using \"insert\""
        cursor = init()
        cursor.execute("INSERT INTO StandardUtf82 (KEY, :c1, :c2) VALUES (:key, :v1, :v2)", 
                       dict(c1="pork", c2="beef", key="meat", v1="bacon", v2="brisket"))

        cursor.execute("SELECT * FROM StandardUtf82 WHERE KEY = :key", dict(key="meat"))
        r = cursor.fetchone()
        d = cursor.description
        assert d[1][0] == "beef"
        assert r[1] == "brisket"

        assert d[2][0] == "pork"
        assert r[2] == "bacon"

        # Bad writes.

        # Too many column values
        assert_raises(cql.ProgrammingError,
                      cursor.execute,
                      "INSERT INTO StandardUtf82 (KEY, :c1) VALUES (:key, :v1, :v2)",
                      dict(c1="name1", key="key0", v1="value1", v2="value2"))

        # Too many column names, (not enough column values)
        assert_raises(cql.ProgrammingError,
                      cursor.execute,
                      "INSERT INTO StandardUtf82 (KEY, :c1, :c2) VALUES (:key, :v1)",
                      dict(c1="name1", c2="name2", key="key0", v1="value1"))

    def test_compression_disabled(self):
        "reading and writing w/ compression disabled"
        cursor = init()
        cursor.compression = 'NONE'
        cursor.execute("UPDATE StandardString1 SET :name = :val WHERE KEY = :key",
                        dict(name="some_name", val="some_value", key="compression_test"))

        cursor.execute("SELECT :name FROM StandardString1 WHERE KEY = :key",
                       dict(name="some_name", key="compression_test"))

        assert cursor.rowcount == 1, "expected 1 result, got %d" % cursor.rowcount
        colnames = [col_d[0] for col_d in cursor.description]
        assert ['some_name'] == colnames, colnames
        row = cursor.fetchone()
        assert ['some_value'] == row, row
