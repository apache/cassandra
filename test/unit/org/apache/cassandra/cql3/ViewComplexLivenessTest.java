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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 */
public class ViewComplexLivenessTest extends ViewAbstractParameterizedTest
{
    @Test
    public void testUnselectedColumnWithExpiredLivenessInfoWithFlush() throws Throwable
    {
        testUnselectedColumnWithExpiredLivenessInfo(true);
    }

    @Test
    public void testUnselectedColumnWithExpiredLivenessInfoWithoutFlush() throws Throwable
    {
        testUnselectedColumnWithExpiredLivenessInfo(false);
    }

    private void testUnselectedColumnWithExpiredLivenessInfo(boolean flush) throws Throwable
    {
        createTable("create table %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select k,c,b from %s " +
                   "where c is not null and k is not null primary key (c, k)");
        ks.getColumnFamilyStore(currentView()).disableAutoCompaction();

        // sstable-1, Set initial values TS=1
        updateViewWithFlush("UPDATE %s SET a = 1 WHERE k = 1 AND c = 1;", flush);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(executeView("SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        // sstable-2
        updateViewWithFlush("INSERT INTO %s(k,c) VALUES(1,1) USING TTL 5", flush);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(executeView("SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        Thread.sleep(5001);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(executeView("SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        // sstable-3
        updateViewWithFlush("Update %s set a = null where k = 1 AND c = 1;", flush);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"));
        assertRowsIgnoringOrder(executeView("SELECT k,c,b from %s WHERE k = 1 AND c = 1;"));

        // sstable-4
        updateViewWithFlush("Update %s USING TIMESTAMP 1 set b = 1 where k = 1 AND c = 1;", flush);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, null, 1));
        assertRowsIgnoringOrder(executeView("SELECT k,c,b from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1));
    }

    @Test
    public void testExpiredLivenessLimitWithFlush() throws Throwable
    {
        // CASSANDRA-13883
        testExpiredLivenessLimit(true);
    }

    @Test
    public void testExpiredLivenessLimitWithoutFlush() throws Throwable
    {
        // CASSANDRA-13883
        testExpiredLivenessLimit(false);
    }

    private void testExpiredLivenessLimit(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int);");

        Keyspace ks = Keyspace.open(keyspace());

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k)");
        ks.getColumnFamilyStore(mv1).disableAutoCompaction();
        ks.getColumnFamilyStore(mv2).disableAutoCompaction();

        for (int i = 1; i <= 100; i++)
            updateView("INSERT INTO %s(k, a, b) VALUES (?, ?, ?);", i, i, i);
        for (int i = 1; i <= 100; i++)
        {
            if (i % 50 == 0)
                continue;
            // create expired liveness
            updateView("DELETE a FROM %s WHERE k = ?;", i);
        }

        if (flush)
        {
            ks.getColumnFamilyStore(mv1).forceBlockingFlush();
            ks.getColumnFamilyStore(mv2).forceBlockingFlush();
        }

        for (String view : Arrays.asList(mv1, mv2))
        {
            // paging
            assertEquals(1, executeNetWithPaging(String.format("SELECT k,a,b FROM %s limit 1", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(String.format("SELECT k,a,b FROM %s limit 2", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(String.format("SELECT k,a,b FROM %s", view), 1).all().size());
            assertRowsNet(executeNetWithPaging(String.format("SELECT k,a,b FROM %s ", view), 1),
                          row(50, 50, 50),
                          row(100, 100, 100));
            // limit
            assertEquals(1, execute(String.format("SELECT k,a,b FROM %s limit 1", view)).size());
            assertRowsIgnoringOrder(execute(String.format("SELECT k,a,b FROM %s limit 2", view)),
                                    row(50, 50, 50),
                                    row(100, 100, 100));
        }
    }

    @Test
    public void testStrictLivenessTombstone() throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        Keyspace ks = Keyspace.open(keyspace());

        createView("create materialized view %s as select * from %s " +
                   "where p is not null and v1 is not null primary key (v1, p) " +
                   "with gc_grace_seconds=5");
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(currentView());
        cfs.disableAutoCompaction();

        updateView("Insert into %s (p, v1, v2) values (1, 1, 1)");
        assertRowsIgnoringOrder(executeView("SELECT p, v1, v2 from %s"), row(1, 1, 1));

        updateView("Update %s set v1 = null WHERE p = 1");
        FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(executeView("SELECT p, v1, v2 from %s"));

        cfs.forceMajorCompaction(); // before gc grace second, strict-liveness tombstoned dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());

        Thread.sleep(6000);
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());

        updateView("Update %s using ttl 5 set v1 = 1 WHERE p = 1");
        FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(executeView("SELECT p, v1, v2 from %s"), row(1, 1, 1));

        cfs.forceMajorCompaction(); // before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(executeView("SELECT p, v1, v2 from %s"), row(1, 1, 1));

        Thread.sleep(5500); // after expired, before gc_grace_second
        cfs.forceMajorCompaction();// before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(executeView("SELECT p, v1, v2 from %s"));

        Thread.sleep(5500); // after expired + gc_grace_second
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());
    }
}
