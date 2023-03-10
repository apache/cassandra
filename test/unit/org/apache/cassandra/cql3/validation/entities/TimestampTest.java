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
package org.apache.cassandra.cql3.validation.entities;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TimestampTest extends CQLTester
{
    @Test
    public void testNegativeTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 1, 1, -42L);
        assertRows(execute("SELECT writetime(v) FROM %s WHERE k = ?", 1),
            row(-42L)
        );

        assertInvalid("INSERT INTO %s (k, v) VALUES (?, ?) USING TIMESTAMP ?", 2, 2, Long.MIN_VALUE);
    }

    /**
     * Test timestmp and ttl
     * migrated from cql_tests.py:TestCQL.timestamp_and_ttl_test()
     */
    @Test
    public void testTimestampTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c text, d text)");

        execute("INSERT INTO %s (k, c) VALUES (1, 'test')");
        execute("INSERT INTO %s (k, c) VALUES (2, 'test') USING TTL 400");

        Object[][] res = getRows(execute("SELECT k, c, writetime(c), ttl(c) FROM %s"));
        Assert.assertEquals(2, res.length);

        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }


        // wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
        res = getRows(execute("SELECT k, c, blob_as_bigint(bigint_as_blob(writetime(c))), ttl(c) FROM %s"));
        Assert.assertEquals(2, res.length);

        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }

        res = getRows(execute("SELECT k, c, writetime(c), blob_as_int(int_as_blob(ttl(c))) FROM %s"));
        Assert.assertEquals(2, res.length);


        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }

        assertInvalid("SELECT k, c, writetime(k) FROM %s");

        assertRows(execute("SELECT k, d, writetime(d) FROM %s WHERE k = 1"),
                   row(1, null, null));
    }

    private void setupSchemaForMaxTimestamp()
    {
        String myType = createType("CREATE TYPE %s (a int, b int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a text, " +
                    "l list<int>, fl frozen<list<int>>," +
                    "s set<int>, fs frozen<set<int>>," +
                    "m map<int, text>, fm frozen<map<int, text>>," +
                    "t " + myType + ", ft frozen<" + myType + ">)");
    }

    @Test
    public void testCallMaxTimestampOnEmptyCollectionReturnsNull() throws Throwable
    {
        setupSchemaForMaxTimestamp();

        execute("INSERT INTO %s (k) VALUES (1)");
        Object[][] res = getRows(execute("SELECT maxwritetime(a), maxwritetime(l), maxwritetime(fl)," +
                                         "maxwritetime(s), maxwritetime(fs), maxwritetime(m), maxwritetime(fm)," +
                                         "maxwritetime(t), maxwritetime(ft) FROM %s WHERE k=1"));

        assertEquals(1, res.length);
        for (Object v : res[0])
        {
            assertNull("All the multi-cell data are empty (we did not insert), calling maxwritetime should return null",
                       v);
        }
    }

    @Test
    public void testMaxTimestamp() throws Throwable
    {
        setupSchemaForMaxTimestamp();

        execute("INSERT INTO %s (k, a, l, fl, s, fs, m, fm, t, ft) VALUES " +
                "(1, 'test', [1], [2], {1}, {2}, {1 : 'a'}, {2 : 'b'}, {a : 1, b : 1 }, {a : 2, b : 2}) USING TIMESTAMP 1");

        // enumerate through all multi-cell types and make sure maxwritetime reflects the expected result
        testMaxTimestampWithColumnUpdate(Arrays.asList(
           Pair.create(1, "UPDATE %s USING TIMESTAMP 10 SET l = l + [10] WHERE k = 1"),
           Pair.create(3, "UPDATE %s USING TIMESTAMP 11 SET s = s + {10} WHERE k = 1"),
           Pair.create(5, "UPDATE %s USING TIMESTAMP 12 SET m = m + {10 : 'c'} WHERE k = 1"),
           Pair.create(7, "UPDATE %s USING TIMESTAMP 13 SET t.a = 10 WHERE k = 1")
        ));
    }

    private void testMaxTimestampWithColumnUpdate(List<Pair<Integer, String>> updateStatements) throws Throwable
    {
        for (Pair<Integer, String> update : updateStatements)
        {
            int fieldPos = update.left();
            String statement = update.right();

            // run the update statement and update the timestamp of the column
            execute(statement);

            Object[][] res = getRows(execute("SELECT maxwritetime(a), maxwritetime(l), maxwritetime(fl)," +
                                             "maxwritetime(s), maxwritetime(fs), maxwritetime(m), maxwritetime(fm)," +
                                             "maxwritetime(t), maxwritetime(ft) FROM %s WHERE k=1"));
            Assert.assertEquals(1, res.length);
            Assert.assertEquals("maxwritetime should work on both single cell and complex columns",
                                9, res[0].length);
            for (Object ts : res[0])
            {
                assertTrue(ts instanceof Long); // all the result fields are timestamps
            }

            long updatedTs = (long) res[0][fieldPos]; // maxwritetime the updated column

            for (int i = 0; i < res[0].length; i++)
            {
                long ts = (long) res[0][i];
                if (i != fieldPos)
                    assertTrue("The updated column should have a large maxwritetime since it is updated later",
                               ts < updatedTs);
            }
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.invalid_custom_timestamp_test()
     */
    @Test
    public void testInvalidCustomTimestamp() throws Throwable
    {
        // Conditional updates
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v))");

        execute("BEGIN BATCH " +
                "INSERT INTO %1$s (k, v) VALUES(0, 0) IF NOT EXISTS; " +
                "INSERT INTO %1$s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                "APPLY BATCH");

        assertInvalid("BEGIN BATCH " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 2) IF NOT EXISTS USING TIMESTAMP 1; " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 3) IF NOT EXISTS; " +
                      "APPLY BATCH");
        assertInvalid("BEGIN BATCH " +
                      "USING TIMESTAMP 1 INSERT INTO %1$s (k, v) VALUES(0, 4) IF NOT EXISTS; " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                      "APPLY BATCH");

        execute("INSERT INTO %s (k, v) VALUES(1, 0) IF NOT EXISTS");
        assertInvalid("INSERT INTO %s (k, v) VALUES(1, 1) IF NOT EXISTS USING TIMESTAMP 5");

        // Counters
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c counter)");

        execute("UPDATE %s SET c = c + 1 WHERE k = 0");
        assertInvalid("UPDATE %s USING TIMESTAMP 10 SET c = c + 1 WHERE k = 0");

        execute("BEGIN COUNTER BATCH " +
                "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                "APPLY BATCH");

        assertInvalid("BEGIN COUNTER BATCH " +
                      "UPDATE %1$s USING TIMESTAMP 3 SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH");

        assertInvalid("BEGIN COUNTER BATCH " +
                      "USING TIMESTAMP 3 UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH");
    }

    @Test
    public void testInsertTimestampWithUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TIMESTAMP ?", unset()); // treat as 'now'
    }

    @Test
    public void testTimestampsOnUnsetColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (1, 1) USING TIMESTAMP 1;");
        execute("INSERT INTO %s (k) VALUES (2) USING TIMESTAMP 2;");
        execute("INSERT INTO %s (k, i) VALUES (3, 3) USING TIMESTAMP 1;");
        assertRows(execute("SELECT k, i, writetime(i) FROM %s "),
                   row(1, 1, 1L),
                   row(2, null, null),
                   row(3, 3, 1L));
    }

    @Test
    public void testTimestampsOnUnsetColumnsWide() throws Throwable
    {
        createTable("CREATE TABLE %s (k int , c int, i int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TIMESTAMP 1;");
        execute("INSERT INTO %s (k, c) VALUES (1, 2) USING TIMESTAMP 1;");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 3, 1) USING TIMESTAMP 1;");
        execute("INSERT INTO %s (k, c) VALUES (2, 2) USING TIMESTAMP 2;");
        execute("INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TIMESTAMP 1;");
        assertRows(execute("SELECT k, c, i, writetime(i) FROM %s "),
                   row(1, 1, 1, 1L),
                   row(1, 2, null, null),
                   row(1, 3, 1, 1L),
                   row(2, 2, null, null),
                   row(3, 3, 3, 1L));
    }

    @Test
    public void testTimestampAndTTLPrepared() throws Throwable
    {

        createTable("CREATE TABLE %s (k int , c int, i int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, i) VALUES (1, 1, 1) USING TIMESTAMP ? AND TTL ?;", 1L,5);
        execute("INSERT INTO %s (k, c) VALUES (1, 2) USING TIMESTAMP ? AND TTL ? ;", 1L, 5);
        execute("INSERT INTO %s (k, c, i) VALUES (1, 3, 1) USING TIMESTAMP ? AND TTL ?;", 1L, 5);
        execute("INSERT INTO %s (k, c) VALUES (2, 2) USING TIMESTAMP ? AND TTL ?;", 2L, 5);
        execute("INSERT INTO %s (k, c, i) VALUES (3, 3, 3) USING TIMESTAMP ? AND TTL ?;", 1L, 5);
        assertRows(execute("SELECT k, c, i, writetime(i) FROM %s "),
                row(1, 1, 1, 1L),
                row(1, 2, null, null),
                row(1, 3, 1, 1L),
                row(2, 2, null, null),
                row(3, 3, 3, 1L));
        Thread.sleep(6*1000);
        assertEmpty(execute("SELECT k, c, i, writetime(i) FROM %s "));
    }

    @Test
    public void testTimestampAndTTLUpdatePrepared() throws Throwable
    {

        createTable("CREATE TABLE %s (k int , c int, i int, PRIMARY KEY (k, c))");
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=1 AND c = 1 ;", 1L, 5);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=1 AND c = 3 ;", 1L, 5);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=1 WHERE k=2 AND c = 2 ;", 2L, 5);
        execute("UPDATE %s USING TIMESTAMP ? AND TTL ? SET i=3 WHERE k=3 AND c = 3 ;", 1L, 5);
        assertRows(execute("SELECT k, c, i, writetime(i) FROM %s "),
                row(1, 1, 1, 1L),
                row(1, 3, 1, 1L),
                row(2, 2, 1, 2L),
                row(3, 3, 3, 1L));
        Thread.sleep(6*1000);
        assertEmpty(execute("SELECT k, c, i, writetime(i) FROM %s "));
    }
}
