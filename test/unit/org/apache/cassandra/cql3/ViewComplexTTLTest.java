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

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertTrue;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670, CASSANDRA-17167)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 * - ...
 * - ViewComplex*Test
 */
public class ViewComplexTTLTest extends ViewComplexTester
{
    @Test
    public void testUpdateColumnInViewPKWithTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(true);
    }

    @Test
    public void testUpdateColumnInViewPKWithTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13657
        testUpdateColumnInViewPKWithTTL(false);
    }

    private void testUpdateColumnInViewPKWithTTL(boolean flush) throws Throwable
    {
        // CASSANDRA-13657 if base column used in view pk is ttled, then view row is considered dead
        createTable("create table %s (k int primary key, a int, b int)");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                               "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        updateView("UPDATE %s SET a = 1 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"), row(1, 1, null));
        assertRows(execute("SELECT * from " + mv), row(1, 1, null));

        updateView("DELETE a FROM %s WHERE k = 1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from " + mv));

        updateView("INSERT INTO %s (k) VALUES (1);");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from " + mv));

        updateView("UPDATE %s USING TTL 5 SET a = 10 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"), row(1, 10, null));
        assertRows(execute("SELECT * from " + mv), row(10, 1, null));

        updateView("UPDATE %s SET b = 100 WHERE k = 1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"), row(1, 10, 100));
        assertRows(execute("SELECT * from " + mv), row(10, 1, 100));

        Thread.sleep(5000);

        // 'a' is TTL of 5 and removed.
        assertRows(execute("SELECT * from %s"), row(1, null, 100));
        assertEmpty(execute("SELECT * from " + mv));
        assertEmpty(execute("SELECT * from " + mv + " WHERE k = ? AND a = ?", 1, 10));

        updateView("DELETE b FROM %s WHERE k=1");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertRows(execute("SELECT * from %s"), row(1, null, null));
        assertEmpty(execute("SELECT * from " + mv));

        updateView("DELETE FROM %s WHERE k=1;");

        if (flush)
            FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        assertEmpty(execute("SELECT * from %s"));
        assertEmpty(execute("SELECT * from " + mv));
    }
    @Test
    public void testUnselectedColumnsTTLWithFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUnselectedColumnsTTL(true);
    }

    @Test
    public void testUnselectedColumnsTTLWithoutFlush() throws Throwable
    {
        // CASSANDRA-13127
        testUnselectedColumnsTTL(false);
    }

    private void testUnselectedColumnsTTL(boolean flush) throws Throwable
    {
        // CASSANDRA-13127 not ttled unselected column in base should keep view row alive
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT p, c FROM %%s " +
                               "WHERE p IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, p)");
        ks.getColumnFamilyStore(mv).disableAutoCompaction();

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (0, 0) USING TTL 3;", flush);

        updateViewWithFlush("UPDATE %s USING TTL 1000 SET v = 0 WHERE p = 0 and c = 0;", flush);

        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        Thread.sleep(3000);

        UntypedResultSet.Row row = execute("SELECT v, ttl(v) from %s WHERE c = ? AND p = ?", 0, 0).one();
        assertEquals("row should have value of 0", 0, row.getInt("v"));
        assertTrue("row should have ttl less than 1000", row.getInt("ttl(v)") < 1000);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateViewWithFlush("DELETE FROM %s WHERE p = 0 and c = 0;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0));

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (0, 0) ", flush);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        // already have a live row, no need to apply the unselected cell ttl
        updateViewWithFlush("UPDATE %s USING TTL 3 SET v = 0 WHERE p = 0 and c = 0;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0), row(0, 0));

        updateViewWithFlush("INSERT INTO %s (p, c) VALUES (1, 1) USING TTL 3", flush);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 1, 1), row(1, 1));

        Thread.sleep(4000);

        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 0, 0), row(0, 0));
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 1, 1));

        // unselected should keep view row alive
        updateViewWithFlush("UPDATE %s SET v = 0 WHERE p = 1 and c = 1;", flush);
        assertRowsIgnoringOrder(execute("SELECT * from " + mv + " WHERE c = ? AND p = ?", 1, 1), row(1, 1));

    } 

    @Test
    public void testBaseTTLWithSameTimestampTest() throws Throwable
    {
        // CASSANDRA-13127 when liveness timestamp tie, greater localDeletionTime should win if both are expiring.
        createTable("create table %s (p int, c int, v int, primary key(p, c))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) using timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));

        // reversed order
        execute("truncate %s;");

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING TTL 3 and timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        updateView("INSERT INTO %s (p, c, v) VALUES (0, 0, 0) USING timestamp 1;");

        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));

        Thread.sleep(4000);

        assertEmpty(execute("SELECT * from %s WHERE c = ? AND p = ?", 0, 0));
    }    
}
