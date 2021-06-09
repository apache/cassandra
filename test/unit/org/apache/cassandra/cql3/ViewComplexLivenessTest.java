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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.ViewComplexTest.createView;
import static org.apache.cassandra.cql3.ViewComplexTest.updateView;
import static org.apache.cassandra.cql3.ViewComplexTest.updateViewWithFlush;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.junit.Assert.assertEquals;

/* ViewComplexTest class has been split into multiple ones because of timeout issues (CASSANDRA-16670)
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 * - ViewComplexLivenessTest
 */
@RunWith(Parameterized.class)
public class ViewComplexLivenessTest extends CQLTester
{
    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ViewComplexTest.versions();
    }

    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        ViewComplexTest.startup();
    }

    @Before
    public void begin()
    {
        ViewComplexTest.beginSetup(views);
    }

    @After
    public void end() throws Throwable
    {
        ViewComplexTest.endTearDown(views, version, this);
    }

    @Test
    public void testUnselectedColumnWithExpiredLivenessInfo() throws Throwable
    {
        boolean flush = true;
        createTable("create table %s (k int, c int, a int, b int, PRIMARY KEY(k, c))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select k,c,b from %%s where c is not null and k is not null primary key (c, k);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable-1, Set initial values TS=1
        updateViewWithFlush("UPDATE %s SET a = 1 WHERE k = 1 AND c = 1;", flush, version, this);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT k,c,b from mv WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        // sstable-2
        updateViewWithFlush("INSERT INTO %s(k,c) VALUES(1,1) USING TTL 5", flush, version, this);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT k,c,b from mv WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        Thread.sleep(5001);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT k,c,b from mv WHERE k = 1 AND c = 1;"),
                                row(1, 1, null));

        // sstable-3
        updateViewWithFlush("Update %s set a = null where k = 1 AND c = 1;", flush, version, this);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"));
        assertRowsIgnoringOrder(execute("SELECT k,c,b from mv WHERE k = 1 AND c = 1;"));

        // sstable-4
        updateViewWithFlush("Update %s USING TIMESTAMP 1 set b = 1 where k = 1 AND c = 1;", flush, version, this);

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE k = 1 AND c = 1;"),
                                row(1, 1, null, 1));
        assertRowsIgnoringOrder(execute("SELECT k,c,b from mv WHERE k = 1 AND c = 1;"),
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

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv1",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (k, a);",
                   version,
                   this,
                   views);
        createView("mv2",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND a IS NOT NULL PRIMARY KEY (a, k);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv1").disableAutoCompaction();
        ks.getColumnFamilyStore("mv2").disableAutoCompaction();

        for (int i = 1; i <= 100; i++)
            updateView("INSERT INTO %s(k, a, b) VALUES (?, ?, ?);", version, this, i, i, i);
        for (int i = 1; i <= 100; i++)
        {
            if (i % 50 == 0)
                continue;
            // create expired liveness
            updateView("DELETE a FROM %s WHERE k = ?;", version, this, i);
        }
        if (flush)
        {
            ks.getColumnFamilyStore("mv1").forceBlockingFlush(UNIT_TESTS);
            ks.getColumnFamilyStore("mv2").forceBlockingFlush(UNIT_TESTS);
        }

        for (String view : Arrays.asList("mv1", "mv2"))
        {
            // paging
            assertEquals(1, executeNetWithPaging(version, String.format("SELECT k,a,b FROM %s limit 1", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(version, String.format("SELECT k,a,b FROM %s limit 2", view), 1).all().size());
            assertEquals(2, executeNetWithPaging(version, String.format("SELECT k,a,b FROM %s", view), 1).all().size());
            assertRowsNet(version, executeNetWithPaging(version, String.format("SELECT k,a,b FROM %s ", view), 1),
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

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p)"
                           + " with gc_grace_seconds=5;",
                   version,
                   this,
                   views);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
        cfs.disableAutoCompaction();

        updateView("Insert into %s (p, v1, v2) values (1, 1, 1) ;", version, this);
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        updateView("Update %s set v1 = null WHERE p = 1", version, this);
        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"));

        cfs.forceMajorCompaction(); // before gc grace second, strict-liveness tombstoned dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());

        Thread.sleep(6000);
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());

        updateView("Update %s using ttl 5 set v1 = 1 WHERE p = 1", version, this);
        FBUtilities.waitOnFutures(ks.flush(UNIT_TESTS));
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        cfs.forceMajorCompaction(); // before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"), row(1, 1, 1));

        Thread.sleep(5500); // after expired, before gc_grace_second
        cfs.forceMajorCompaction();// before ttl+gc_grace_second, strict-liveness ttled dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"));

        Thread.sleep(5500); // after expired + gc_grace_second
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());
    }
}
