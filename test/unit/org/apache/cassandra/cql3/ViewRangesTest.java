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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

/*
 * This test class was too large and used to timeout CASSANDRA-16777. We're splitting it into:
 * - ViewTest
 * - ViewPKTest
 * - ViewRangesTest
 * - ViewTimesTest
 */
public class ViewRangesTest extends ViewAbstractTest
{
    @Test
    public void testExistingRangeTombstoneWithFlush() throws Throwable
    {
        testExistingRangeTombstone(true);
    }

    @Test
    public void testExistingRangeTombstoneWithoutFlush() throws Throwable
    {
        testExistingRangeTombstone(false);
    }

    public void testExistingRangeTombstone(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k1, c1, c2))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL " +
                   "PRIMARY KEY (k1, c2, c1)");

        updateView("DELETE FROM %s USING TIMESTAMP 10 WHERE k1 = 1 and c1=1");

        if (flush)
            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 0, 0, 0) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 0, 1, 0, 1) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 0, 1, 0) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 1, 1, 1) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 2, 1, 2) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 1, 3, 1, 3) USING TIMESTAMP 5; " +
                "INSERT INTO " + table + " (k1, c1, c2, v1, v2) VALUES (1, 2, 0, 2, 0) USING TIMESTAMP 5; " +
                "APPLY BATCH");

        assertRowsIgnoringOrder(execute("select * from %s"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
        assertRowsIgnoringOrder(executeView("select k1,c1,c2,v1,v2 from %s"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
    }

    @Test
    public void testRangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "textval2 text, " +
                    "PRIMARY KEY((k, asciival), bigintval, textval1)" +
                    ")");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                   "PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1,textval2) VALUES (?,?,?,?,?)",
                       0, "foo", (long) i % 2, "bar" + i, "baz");

        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());

        Assert.assertEquals(100, executeView("select * from %s").size());

        //Check the builder works
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                   "PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        Assert.assertEquals(100, executeView("select * from %s").size());

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL " +
                   "PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)");

        Assert.assertEquals(100, executeView("select * from %s").size());
        Assert.assertEquals(100, executeView("select asciival from %s where textval2 = ? and k = ?", "baz", 0).size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(50, executeView("select asciival from %s where textval2 = ? and k = ?", "baz", 0).size());
    }


    @Test
    public void testRangeTombstone2() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL " +
                   "PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, executeView("select * from %s").size());
    }

    @Test
    public void testRangeTombstone3() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "textval1 text, " +
                    "PRIMARY KEY((k, asciival), bigintval)" +
                    ")");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL " +
                   "PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval >= ?", 0, "foo", 0L);

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, executeView("select * from %s").size());
    }
}
