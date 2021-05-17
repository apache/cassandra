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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Objects;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/* This class been split into multiple ones bc of timeout issues CASSANDRA-16670
 * Any changes here check if they apply to the other classes:
 * - ViewComplexUpdatesTest
 * - ViewComplexDeletionsTest
 * - ViewComplexTTLTest
 * - ViewComplexTest
 */
@RunWith(Parameterized.class)
public class ViewComplexTest extends CQLTester
{
    @Parameterized.Parameter
    public ProtocolVersion version;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return ProtocolVersion.SUPPORTED.stream()
                                        .map(v -> new Object[]{v})
                                        .collect(Collectors.toList());
    }

    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Before
    public void begin()
    {
        beginSetup(views);
    }

    public static void beginSetup(List<String> views)
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        endTearDown(views, version, this);
    }

    public static void endTearDown(List<String> views, ProtocolVersion version, CQLTester cqlTester) throws Throwable
    {
        for (String viewName : views)
            cqlTester.executeNet(version, "DROP MATERIALIZED VIEW " + viewName);
    }

    public static void createView(String name, String query, ProtocolVersion version, CQLTester cqlTester, List<String> views) throws Throwable
    {
        try
        {
            cqlTester.executeNet(version, String.format(query, name));
            // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
            // the desired behavior
            views.add(name);
        }
        catch (OperationTimedOutException ex)
        {
            // ... except for timeout, when we actually do not know whether the view was created or not
            views.add(name);
            throw ex;
        }
    }

    public static void updateView(String query, ProtocolVersion version, CQLTester cqlTester, Object... params) throws Throwable
    {
        updateViewWithFlush(query, false, version, cqlTester, params);
    }

    public static void updateViewWithFlush(String query, boolean flush, ProtocolVersion version, CQLTester cqlTester, Object... params) throws Throwable
    {
        cqlTester.executeNet(version, query, params);
        while (!(((SEPExecutor) Stage.VIEW_MUTATION.executor()).getPendingTaskCount() == 0
                && ((SEPExecutor) Stage.VIEW_MUTATION.executor()).getActiveTaskCount() == 0))
        {
            Thread.sleep(1);
        }
        if (flush)
            Keyspace.open(cqlTester.keyspace()).flush();
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
            ks.getColumnFamilyStore("mv1").forceBlockingFlush();
            ks.getColumnFamilyStore("mv2").forceBlockingFlush();
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
    public void testNonBaseColumnInViewPkWithFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(true);
    }

    @Test
    public void testNonBaseColumnInViewPkWithoutFlush() throws Throwable
    {
        testNonBaseColumnInViewPk(true);
    }

    public void testNonBaseColumnInViewPk(boolean flush) throws Throwable
    {
        createTable("create table %s (p1 int, p2 int, v1 int, v2 int, primary key (p1,p2))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p1 is not null and p2 is not null primary key (p2, p1)"
                           + " with gc_grace_seconds=5;",
                   version,
                   this,
                   views);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
        cfs.disableAutoCompaction();

        updateView("UPDATE %s USING TIMESTAMP 1 set v1 =1 where p1 = 1 AND p2 = 1;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, 1, null));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, 1, null));

        updateView("UPDATE %s USING TIMESTAMP 2 set v1 = null, v2 = 1 where p1 = 1 AND p2 = 1;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, 1));

        updateView("UPDATE %s USING TIMESTAMP 2 set v2 = null where p1 = 1 AND p2 = 1;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"));

        updateView("INSERT INTO %s (p1,p2) VALUES(1,1) USING TIMESTAMP 3;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, null));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, null));

        updateView("DELETE FROM %s USING TIMESTAMP 4 WHERE p1 =1 AND p2 = 1;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"));

        updateView("UPDATE %s USING TIMESTAMP 5 set v2 = 1 where p1 = 1 AND p2 = 1;", version, this);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from %s"), row(1, 1, null, 1));
        assertRowsIgnoringOrder(execute("SELECT p1, p2, v1, v2 from mv"), row(1, 1, null, 1));
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
        FBUtilities.waitOnFutures(ks.flush());
        assertRowsIgnoringOrder(execute("SELECT p, v1, v2 from mv"));

        cfs.forceMajorCompaction(); // before gc grace second, strict-liveness tombstoned dead row remains
        assertEquals(1, cfs.getLiveSSTables().size());

        Thread.sleep(6000);
        assertEquals(1, cfs.getLiveSSTables().size()); // no auto compaction.

        cfs.forceMajorCompaction(); // after gc grace second, no data left
        assertEquals(0, cfs.getLiveSSTables().size());

        updateView("Update %s using ttl 5 set v1 = 1 WHERE p = 1", version, this);
        FBUtilities.waitOnFutures(ks.flush());
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

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(true);
    }

    @Test
    public void testCellTombstoneAndShadowableTombstonesWithoutFlush() throws Throwable
    {
        testCellTombstoneAndShadowableTombstones(false);
    }

    private void testCellTombstoneAndShadowableTombstones(boolean flush) throws Throwable
    {
        createTable("create table %s (p int primary key, v1 int, v2 int)");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv",
                   "create materialized view %s as select * from %%s where p is not null and v1 is not null primary key (v1, p);",
                   version,
                   this,
                   views);
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        // sstable 1, Set initial values TS=1
        updateView("Insert into %s (p, v1, v2) values (3, 1, 3) using timestamp 1;", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3), row(3, 1L));
        // sstable 2
        updateView("UPdate %s using timestamp 2 set v2 = null where p = 3", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v2, WRITETIME(v2) from mv WHERE v1 = ? AND p = ?", 1, 3),
                                row(null, null));
        // sstable 3
        updateView("UPdate %s using timestamp 3 set v1 = 2 where p = 3", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(2, 3, null, null));
        // sstable 4
        updateView("UPdate %s using timestamp 4 set v1 = 1 where p = 3", version, this);

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));

        if (flush)
        {
            // compact sstable 2 and 3;
            ColumnFamilyStore cfs = ks.getColumnFamilyStore("mv");
            List<String> sstables = cfs.getLiveSSTables()
                                       .stream()
                                       .sorted(Comparator.comparingInt(s -> s.descriptor.generation))
                                       .map(s -> s.getFilename())
                                       .collect(Collectors.toList());
            String dataFiles = String.join(",", Arrays.asList(sstables.get(1), sstables.get(2)));
            CompactionManager.instance.forceUserDefinedCompaction(dataFiles);
        }
        // cell-tombstone in sstable 4 is not compacted away, because the shadowable tombstone is shadowed by new row.
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv"), row(1, 3, null, null));
        assertRowsIgnoringOrder(execute("SELECT v1, p, v2, WRITETIME(v2) from mv limit 1"), row(1, 3, null, null));
    }

    @Test
    public void testMVWithDifferentColumnsWithFlush() throws Throwable
    {
        testMVWithDifferentColumns(true);
    }

    @Test
    public void testMVWithDifferentColumnsWithoutFlush() throws Throwable
    {
        testMVWithDifferentColumns(false);
    }

    private void testMVWithDifferentColumns(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY(a, b))");

        execute("USE " + keyspace());
        executeNet(version, "USE " + keyspace());
        List<String> viewNames = new ArrayList<>();
        List<String> mvStatements = Arrays.asList(
                                                  // all selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // unselected e,f
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // no selected
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a,b)",
                                                  // all selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // unselected e,f, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b,c,d FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)",
                                                  // no selected, re-order keys
                                                  "CREATE MATERIALIZED VIEW %s AS SELECT a,b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b,a)");

        Keyspace ks = Keyspace.open(keyspace());

        for (int i = 0; i < mvStatements.size(); i++)
        {
            String name = "mv" + i;
            viewNames.add(name);
            createView(name, mvStatements.get(i), version, this, views);
            ks.getColumnFamilyStore(name).disableAutoCompaction();
        }

        // insert
        updateViewWithFlush("INSERT INTO %s (a,b,c,d,e,f) VALUES(1,1,1,1,1,1) using timestamp 1", flush, version, this);
        assertBaseViews(row(1, 1, 1, 1, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET c=0, d=0 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, 0, 0, 1, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 2 SET e=0, f=0 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, 0, 0, 0, 0), viewNames);

        updateViewWithFlush("DELETE FROM %s using timestamp 2 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(null, viewNames);

        // partial update unselected, selected
        updateViewWithFlush("UPDATE %s using timestamp 3 SET f=1 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = 1, f=null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, null, null, 1, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 4 SET e = null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = 1 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 5 SET c = null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET d = 1 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, null, 1, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 7 SET d = null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(null, viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = 1 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, null, null, null, 1), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = 1 WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, 1, null, null, 1), viewNames);

        // view row still alive due to c=1@6
        updateViewWithFlush("UPDATE %s using timestamp 8 SET f = null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(row(1, 1, 1, null, null, null), viewNames);

        updateViewWithFlush("UPDATE %s using timestamp 6 SET c = null WHERE a=1 AND b=1", flush, version, this);
        assertBaseViews(null, viewNames);
    }

    private void assertBaseViews(Object[] row, List<String> viewNames) throws Throwable
    {
        UntypedResultSet result = execute("SELECT * FROM %s");
        if (row == null)
            assertRowsIgnoringOrder(result);
        else
            assertRowsIgnoringOrder(result, row);
        for (int i = 0; i < viewNames.size(); i++)
            assertBaseView(result, execute(String.format("SELECT * FROM %s", viewNames.get(i))), viewNames.get(i));
    }

    private void assertBaseView(UntypedResultSet base, UntypedResultSet view, String mv)
    {
        List<ColumnSpecification> baseMeta = base.metadata();
        List<ColumnSpecification> viewMeta = view.metadata();

        Iterator<UntypedResultSet.Row> iter = base.iterator();
        Iterator<UntypedResultSet.Row> viewIter = view.iterator();

        List<UntypedResultSet.Row> baseData = com.google.common.collect.Lists.newArrayList(iter);
        List<UntypedResultSet.Row> viewData = com.google.common.collect.Lists.newArrayList(viewIter);

        if (baseData.size() != viewData.size())
            fail(String.format("Mismatch number of rows in view %s: <%s>, in base <%s>",
                               mv,
                               makeRowStrings(view),
                               makeRowStrings(base)));
        if (baseData.size() == 0)
            return;
        if (viewData.size() != 1)
            fail(String.format("Expect only one row in view %s, but got <%s>",
                               mv,
                               makeRowStrings(view)));

        UntypedResultSet.Row row = baseData.get(0);
        UntypedResultSet.Row viewRow = viewData.get(0);

        Map<String, ByteBuffer> baseValues = new HashMap<>();
        for (int j = 0; j < baseMeta.size(); j++)
        {
            ColumnSpecification column = baseMeta.get(j);
            ByteBuffer actualValue = row.getBytes(column.name.toString());
            baseValues.put(column.name.toString(), actualValue);
        }
        for (int j = 0; j < viewMeta.size(); j++)
        {
            ColumnSpecification column = viewMeta.get(j);
            String name = column.name.toString();
            ByteBuffer viewValue = viewRow.getBytes(name);
            if (!baseValues.containsKey(name))
            {
                fail(String.format("Extra column: %s with value %s in view", name, column.type.compose(viewValue)));
            }
            else if (!Objects.equal(baseValues.get(name), viewValue))
            {
                fail(String.format("Non equal column: %s, expected <%s> but got <%s>",
                                   name,
                                   column.type.compose(baseValues.get(name)),
                                   column.type.compose(viewValue)));
            }
        }
    }
}
