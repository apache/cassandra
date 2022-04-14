/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

/* ViewFilteringTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes
 * - ViewFilteringPKTest
 * - ViewFilteringClustering1Test
 * - ViewFilteringClustering2Test
 * - ViewFilteringTest
 * - ...
 * - ViewFiltering*Test
 */
public class ViewFiltering2Test extends ViewAbstractParameterizedTest
{
    @BeforeClass
    public static void startup()
    {
        ViewFiltering1Test.startup();
    }

    @AfterClass
    public static void tearDown()
    {
        ViewFiltering1Test.tearDown();
    }

    @Test
    public void testAllTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");
        String columnNames = "asciival, " +
                             "bigintval, " +
                             "blobval, " +
                             "booleanval, " +
                             "dateval, " +
                             "decimalval, " +
                             "doubleval, " +
                             "floatval, " +
                             "inetval, " +
                             "intval, " +
                             "textval, " +
                             "timeval, " +
                             "timestampval, " +
                             "timeuuidval, " +
                             "uuidval," +
                             "varcharval, " +
                             "varintval, " +
                             "frozenlistval, " +
                             "frozensetval, " +
                             "frozenmapval, " +
                             "tupleval, " +
                             "udtval";

        createTable(
        "CREATE TABLE %s (" +
        "asciival ascii, " +
        "bigintval bigint, " +
        "blobval blob, " +
        "booleanval boolean, " +
        "dateval date, " +
        "decimalval decimal, " +
        "doubleval double, " +
        "floatval float, " +
        "inetval inet, " +
        "intval int, " +
        "textval text, " +
        "timeval time, " +
        "timestampval timestamp, " +
        "timeuuidval timeuuid, " +
        "uuidval uuid," +
        "varcharval varchar, " +
        "varintval varint, " +
        "frozenlistval frozen<list<int>>, " +
        "frozensetval frozen<set<uuid>>, " +
        "frozenmapval frozen<map<ascii, int>>," +
        "tupleval frozen<tuple<int, ascii, uuid>>," +
        "udtval frozen<" + myType + ">, " +
        "PRIMARY KEY (" + columnNames + "))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " +
                   "asciival = 'abc' AND " +
                   "bigintval = 123 AND " +
                   "blobval = 0xfeed AND " +
                   "booleanval = true AND " +
                   "dateval = '1987-03-23' AND " +
                   "decimalval = 123.123 AND " +
                   "doubleval = 123.123 AND " +
                   "floatval = 123.123 AND " +
                   "inetval = '127.0.0.1' AND " +
                   "intval = 123 AND " +
                   "textval = 'abc' AND " +
                   "timeval = '07:35:07.000111222' AND " +
                   "timestampval = 123123123 AND " +
                   "timeuuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND " +
                   "uuidval = 6BDDC89A-5644-11E4-97FC-56847AFE9799 AND " +
                   "varcharval = 'abc' AND " +
                   "varintval = 123123123 AND " +
                   "frozenlistval = [1, 2, 3] AND " +
                   "frozensetval = {6BDDC89A-5644-11E4-97FC-56847AFE9799} AND " +
                   "frozenmapval = {'a': 1, 'b': 2} AND " +
                   "tupleval = (1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799) AND " +
                   "udtval = {a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}} " +
                   "PRIMARY KEY (" + columnNames + ")");

        execute("INSERT INTO %s (" + columnNames + ") VALUES (" +
                "'abc'," +
                "123," +
                "0xfeed," +
                "true," +
                "'1987-03-23'," +
                "123.123," +
                "123.123," +
                "123.123," +
                "'127.0.0.1'," +
                "123," +
                "'abc'," +
                "'07:35:07.000111222'," +
                "123123123," +
                "6BDDC89A-5644-11E4-97FC-56847AFE9799," +
                "6BDDC89A-5644-11E4-97FC-56847AFE9799," +
                "'abc'," +
                "123123123," +
                "[1, 2, 3]," +
                "{6BDDC89A-5644-11E4-97FC-56847AFE9799}," +
                "{'a': 1, 'b': 2}," +
                "(1, 'foobar', 6BDDC89A-5644-11E4-97FC-56847AFE9799)," +
                "{a: 1, b: 6BDDC89A-5644-11E4-97FC-56847AFE9799, c: {'foo', 'bar'}})");

        assert !executeView("SELECT * FROM %s").isEmpty();

        executeNet("ALTER TABLE %s RENAME inetval TO foo");
        assert !executeView("SELECT * FROM %s").isEmpty();
    }

    @Test
    public void testMVCreationWithNonPrimaryRestrictions()
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");

        try
        {
            String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                   "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d = 1 " +
                                   "PRIMARY KEY (a, b, c)");
            dropView(mv);
        }
        catch (Exception e)
        {
            throw new RuntimeException("MV creation with non primary column restrictions failed.", e);
        }

        dropTable("DROP TABLE %s");
    }

    @Test
    public void testNonPrimaryRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 1, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 0, 1, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 0);

        // only accept rows where c = 1
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                   "PRIMARY KEY (a, b, c)");

        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 0),
                                row(1, 1, 1, 0)
        );

        // insert new rows that do not match the filter
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 2, 1, 2, 0);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 0),
                                row(1, 1, 1, 0)
        );

        // insert new row that does match the filter
        execute("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 1, 0);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 0),
                                row(1, 1, 1, 0),
                                row(1, 2, 1, 0)
        );

        // update rows that don't match the filter
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 2, 2, 0);
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 1, 2, 1);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 0),
                                row(1, 1, 1, 0),
                                row(1, 2, 1, 0)
        );

        // update a row that does match the filter
        execute("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 1, 1, 0);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 1),
                                row(1, 1, 1, 0),
                                row(1, 2, 1, 0)
        );

        // delete rows that don't match the filter
        execute("DELETE FROM %s WHERE a = ? AND b = ?", 2, 0);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 1),
                                row(1, 1, 1, 0),
                                row(1, 2, 1, 0)
        );

        // delete a row that does match the filter
        execute("DELETE FROM %s WHERE a = ? AND b = ?", 1, 2);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0),
                                row(1, 0, 1, 1),
                                row(1, 1, 1, 0)
        );

        // delete a partition that matches the filter
        execute("DELETE FROM %s WHERE a = ?", 1);
        assertRowsIgnoringOrder(executeView("SELECT a, b, c, d FROM %s"),
                                row(0, 0, 1, 0),
                                row(0, 1, 1, 0)
        );

        dropView();
        dropTable("DROP TABLE %s");
    }

    @Test
    public void complexRestrictedTimestampUpdateTestWithFlush() throws Throwable
    {
        complexRestrictedTimestampUpdateTest(true);
    }

    @Test
    public void complexRestrictedTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexRestrictedTimestampUpdateTest(false);
    }

    public void complexRestrictedTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");
        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                               "WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                               "PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        //Set initial values TS=0, matching the restriction and verify view
        executeNet("INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            Util.flush(ks);

        //update c's timestamp TS=2
        executeNet("UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            Util.flush(ks);

        //change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 0, 0, 0));

        if (flush)
        {
            ks.getColumnFamilyStore(mv).forceMajorCompaction();
            Util.flush(ks);
        }

        //change c's value back to 1 with TS=4, check we can see d
        executeNet("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore(mv).forceMajorCompaction();
            Util.flush(ks);
        }

        assertRows(executeView("SELECT d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));

        //Add e value @ TS=1
        executeNet("UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(executeView("SELECT d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            Util.flush(ks);

        //Change d value @ TS=2
        executeNet("UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            Util.flush(ks);

        //Change d value @ TS=3
        executeNet("UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));

        //Tombstone c
        executeNet("DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRowsIgnoringOrder(executeView("SELECT d FROM %s"));
        assertRows(executeView("SELECT d FROM %s"));

        //Add back without D
        executeNet("INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(executeView("SELECT d FROM %s WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));

        //New partition
        // insert a row with timestamp 0
        executeNet("INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 1, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet("INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 1, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet("DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(executeView("SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                   row(1, 0, 1, null, 0)
        );

        executeNet("UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 1);
        executeNet("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        assertRows(executeView("SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                   row(1, 0, 1, null, 0)
        );

        executeNet("UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(executeView("SELECT a, b, c, d, e FROM %s WHERE c = ? and a = ? and b = ?", 1, 1, 0),
                   row(1, 0, 1, 0, 0)
        );
    }

    @Test
    public void testRestrictedRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE k IS NOT NULL AND c IS NOT NULL AND c = 1 " +
                   "PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(executeView("SELECT c, k, val FROM %s"), row(1, 0, 1));

        updateView("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);
        assertRows(executeView("SELECT c, k, val FROM %s"), row(1, 0, 2));
    }

    @Test
    public void testOldTimestampsWithRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " + "" +
                    "PRIMARY KEY(k, c))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL AND val = 'baz' " +
                   "PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 1);
        assertRows(execute("SELECT val from %s where k = 0 and c = 1"), row("baz"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 1);
        assertRows(execute("SELECT val from %s where k = 0 and c = 1"), row("bar"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "bar"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(0));
    }
}
