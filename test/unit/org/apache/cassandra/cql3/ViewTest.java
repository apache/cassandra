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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
 * This test class was too large and used to timeout CASSANDRA-16777. We're splitting it into:
 * - ViewTest
 * - ViewPKTest
 * - ViewRangesTest
 * - ViewTimesTest
 */
@RunWith(BMUnitRunner.class)
public class ViewTest extends ViewAbstractTest
{
    /** Latch used by {@link #testTruncateWhileBuilding()} Byteman injections. */
    @SuppressWarnings("unused")
    private static final CountDownLatch blockViewBuild = new CountDownLatch(1);

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage(String.format("Materialized view '%s.view_does_not_exist' doesn't exist", KEYSPACE), "DROP MATERIALIZED VIEW " + KEYSPACE + ".view_does_not_exist");
        assertInvalidMessage("Materialized view 'keyspace_does_not_exist.view_does_not_exist' doesn't exist", "DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist");

        execute("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".view_does_not_exist");
        execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist");
    }

    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "sval text static, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)");
            Assert.fail("Use of static column in a MV primary key should fail");
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getCause() instanceof InvalidRequestException);
        }

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)");
            Assert.fail("Explicit select of static column in MV should fail");
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getCause() instanceof InvalidRequestException);
        }

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");
            Assert.fail("Implicit select of static column in MV should fail");
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getCause() instanceof InvalidRequestException);
        }

        createView("CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

        Assert.assertEquals(2, execute("select * from %s").size());

        assertRows(execute("SELECT sval from %s"), row("bar99"), row("bar99"));

        Assert.assertEquals(2, executeView("select * from %s").size());

        assertInvalid("SELECT sval from " + currentView());
    }


    @Test
    public void testOldTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, executeView("select * from %s").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "bar"), row(0));
        assertRows(executeView("SELECT c from %s where k = 0 and val = ?", "baz"), row(1));
    }

    @Test
    public void testCountersTable()
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "count counter)");

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)");
            Assert.fail("MV on counter should fail");
        }
        catch (Exception e)
        {
            Assert.assertTrue(e.getCause() instanceof InvalidRequestException);
        }
    }

    @Test
    public void testDurationsTable()
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "result duration)");

        try
        {
            createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)");
            Assert.fail("MV on duration should fail");
        }
        catch (Exception e)
        {
            Throwable cause = e.getCause();
            Assert.assertEquals("duration type is not supported for PRIMARY KEY column 'result'", cause.getMessage());
        }
    }

    @Test
    public void testBuilderWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "PRIMARY KEY (k, c))");

        for (int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval) VALUES (?, ?, ?)", 0, i, 0);

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");

        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));
        assertRows(executeView("SELECT count(*) from %s WHERE intval = ?", 0), row(1024L));
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "listval list<int>, " +
                    "PRIMARY KEY (k))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, from_json(?))", 0, 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(executeView("SELECT k, listval from %s WHERE intval = ?", 0), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 1, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 1), row(1, list(1, 2, 3)));
        assertRows(executeView("SELECT k, listval from %s WHERE intval = ?", 1), row(1, list(1, 2, 3)));
    }

    @Test
    public void testFrozenCollectionsWithComplicatedInnerType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, intval int,  listval frozen<list<tuple<text,text>>>, PRIMARY KEY (k))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, from_json(?))",
                   0,
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");

        // verify input
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(executeView("SELECT k, listval from %s"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));

        // update listval with the same value and it will be compared in view generator
        updateView("INSERT INTO %s (k, listval) VALUES (?, from_json(?))",
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        // verify result
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(executeView("SELECT k, listval from %s"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
    }

    @Test
    public void testUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(executeView("SELECT k, intval from %s WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(executeView("SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testIgnoreUpdate() throws Throwable
    {
        // regression test for CASSANDRA-10614

        createTable("CREATE TABLE %s (" +
                    "a int, " +
                    "b int, " +
                    "c int, " +
                    "d int, " +
                    "PRIMARY KEY (a, b))");

        createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        assertRows(executeView("SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0));

        updateView("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 0, 0, 0);
        assertRows(executeView("SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0));

        // Note: errors here may result in the test hanging when the memtables are flushed as part of the table drop,
        // because empty rows in the memtable will cause the flush to fail.  This will result in a test timeout that
        // should not be ignored.
        String table = KEYSPACE + "." + currentTable();
        updateView("BEGIN BATCH " +
                   "INSERT INTO " + table + " (a, b, c, d) VALUES (?, ?, ?, ?); " + // should be accepted
                   "UPDATE " + table + " SET d = ? WHERE a = ? AND b = ?; " +  // should be accepted
                   "APPLY BATCH",
                   0, 0, 0, 0,
                   1, 0, 1);
        assertRows(executeView("SELECT a, b, c from %s WHERE b = ?", 0), row(0, 0, 0));
        assertRows(executeView("SELECT a, b, c from %s WHERE b = ?", 1), row(0, 1, null));

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentView());
        Util.flush(cfs);
        Set<SSTableReader> tables = cfs.getLiveSSTables();
        // cf may have flushed due to the commit log being dirty, plus our explicit flush above
        Assert.assertTrue(String.format("Expected one or two sstables, got %s", tables), tables.size() > 0 && tables.size() <= 2);
    }

    @Test
    public void rowDeletionTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        String table = keyspace() + "." + currentTable();
        updateView("DELETE FROM " + table + " USING TIMESTAMP 6 WHERE a = 1 AND b = 1;");
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP 3", 1, 1, 1, 1);
        Assert.assertEquals(0, executeViewNet("SELECT * FROM %s WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void testMultipleDeletes() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet("USE " + keyspace());

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 2);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 3);

        ResultSet mvRows = executeViewNet("SELECT a, b FROM %s");
        assertRowsNet(mvRows, row(1, 1), row(1, 2), row(1, 3));

        updateView(String.format("BEGIN UNLOGGED BATCH " +
                                 "DELETE FROM %s WHERE a = 1 AND b > 1 AND b < 3;" +
                                 "DELETE FROM %s WHERE a = 1;" +
                                 "APPLY BATCH", currentTable(), currentTable()));

        mvRows = executeViewNet("SELECT a, b FROM %s");
        assertRowsNet(mvRows);
    }

    @Test
    public void testCollectionInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c map<int, text>," +
                    "PRIMARY KEY (a))");

        executeNet("USE " + keyspace());
        createView("CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        ResultSet mvRows = executeViewNet("SELECT a, b FROM %s WHERE b = ?", 0);
        assertRowsNet(mvRows, row(0, 0));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, map(1, "1"));
        mvRows = executeViewNet("SELECT a, b FROM %s WHERE b = ?", 1);
        assertRowsNet(mvRows, row(1, 1));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, map(0, "0"));
        mvRows = executeViewNet("SELECT a, b FROM %s WHERE b = ?", 0);
        assertRowsNet(mvRows, row(0, 0));
    }

    @Test
    public void testReservedKeywordsInMV() throws Throwable
    {
        createTable("CREATE TABLE %s (\"token\" int PRIMARY KEY, \"keyspace\" int)");

        executeNet("USE " + keyspace());

        createView("CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT \"keyspace\", \"token\"" +
                   "  FROM %s" +
                   "  WHERE \"keyspace\" IS NOT NULL AND \"token\" IS NOT NULL" +
                   "  PRIMARY KEY (\"keyspace\", \"token\")");

        execute("INSERT INTO %s (\"token\", \"keyspace\") VALUES (?, ?)", 0, 1);

        assertRowsNet(executeNet("SELECT * FROM %s"), row(0, 1));
        assertRowsNet(executeViewNet("SELECT * FROM %s"), row(1, 0));
    }

    private void testViewBuilderResume(int concurrentViewBuilders) throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        CompactionManager.instance.setConcurrentViewBuilders(concurrentViewBuilders);
        CompactionManager.instance.setCoreCompactorThreads(1);
        CompactionManager.instance.setMaximumCompactorThreads(1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, String.valueOf(i));

        Util.flush(cfs);

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, String.valueOf(i));

        Util.flush(cfs);

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, String.valueOf(i));

        Util.flush(cfs);

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, String.valueOf(i));

        Util.flush(cfs);

        String mv1 = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                     "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        cfs.enableAutoCompaction();
        List<? extends Future<?>> futures = CompactionManager.instance.submitBackground(cfs);

        //Force a second MV on the same base table, which will restart the first MV builder...
        createView("CREATE MATERIALIZED VIEW %s AS SELECT val, k, c FROM %s " +
                   "WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        //Compact the base table
        FBUtilities.waitOnFutures(futures);

        waitForViewBuild(mv1);

        assertRows(executeView("SELECT count(*) FROM %s"), row(1024L));
    }

    @Test
    public void testViewBuilderResume() throws Throwable
    {
        for (int i = 1; i <= 8; i *= 2)
        {
            testViewBuilderResume(i);
        }
    }

    /**
     * Tests that a client warning is issued on materialized view creation.
     */
    @Test
    public void testClientWarningOnCreate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ClientWarn.instance.captureWarnings();
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        List<String> warnings = ClientWarn.instance.getWarnings();

        Assert.assertNotNull(warnings);
        Assert.assertEquals(1, warnings.size());
        Assert.assertEquals(View.USAGE_WARNING, warnings.get(0));
    }

    /**
     * Tests the configuration flag to disable materialized views.
     */
    @Test
    public void testDisableMaterializedViews() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        executeNet("USE " + keyspace());

        boolean enableMaterializedViews = DatabaseDescriptor.getMaterializedViewsEnabled();
        try
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(false);
            createView("CREATE MATERIALIZED VIEW %s AS SELECT v FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
            Assert.fail("Should not be able to create a materialized view if they are disabled");
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause();
            Assertions.assertThat(cause).isInstanceOf(InvalidRequestException.class);
            Assertions.assertThat(cause.getMessage()).contains("Materialized views are disabled");
        }
        finally
        {
            DatabaseDescriptor.setMaterializedViewsEnabled(enableMaterializedViews);
        }
    }

    @Test
    public void testQuotedIdentifiersInWhereClause() throws Throwable
    {
        createTable("CREATE TABLE %s (\"theKey\" int, \"theClustering_1\" int, \"theClustering_2\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\"))");

        executeNet("USE " + keyspace());

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE \"theKey\" IS NOT NULL AND \"theClustering_1\" IS NOT NULL AND \"theClustering_2\" IS NOT NULL AND \"theValue\" IS NOT NULL  PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\");");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE \"theKey\" IS NOT NULL AND (\"theClustering_1\", \"theClustering_2\") = (1, 2) AND \"theValue\" IS NOT NULL  PRIMARY KEY (\"theKey\", \"theClustering_1\", \"theClustering_2\");");

        assertRows(execute("SELECT where_clause FROM system_schema.views"),
                   row("\"theKey\" IS NOT NULL AND \"theClustering_1\" IS NOT NULL AND \"theClustering_2\" IS NOT NULL AND \"theValue\" IS NOT NULL"),
                   row("\"theKey\" IS NOT NULL AND (\"theClustering_1\", \"theClustering_2\") = (1, 2) AND \"theValue\" IS NOT NULL"));
    }

    @Test(expected = SyntaxException.class)
    public void emptyViewNameTest() throws Throwable
    {
        execute("CREATE MATERIALIZED VIEW \"\" AS SELECT a, b FROM tbl WHERE b IS NOT NULL PRIMARY KEY (b, a)");
    }

    @Test(expected = SyntaxException.class)
    public void emptyBaseTableNameTest() throws Throwable
    {
        execute("CREATE MATERIALIZED VIEW myview AS SELECT a, b FROM \"\" WHERE b IS NOT NULL PRIMARY KEY (b, a)");
    }

    @Test
    public void testFunctionInWhereClause() throws Throwable
    {
        // Native token function with lowercase, should be unquoted in the schema where clause
        assertEmpty(testFunctionInWhereClause("CREATE TABLE %s (k bigint PRIMARY KEY, v int)",
                                              null,
                                              "CREATE MATERIALIZED VIEW %s AS" +
                                              "   SELECT * FROM %s WHERE k = token(1) AND v IS NOT NULL " +
                                              "   PRIMARY KEY (v, k)",
                                              "k = token(1) AND v IS NOT NULL",
                                              "INSERT INTO %s(k, v) VALUES (0, 1)",
                                              "INSERT INTO %s(k, v) VALUES (2, 3)"));

        // Native token function with uppercase, should be unquoted and lowercased in the schema where clause
        assertEmpty(testFunctionInWhereClause("CREATE TABLE %s (k bigint PRIMARY KEY, v int)",
                                              null,
                                              "CREATE MATERIALIZED VIEW %s AS" +
                                              "   SELECT * FROM %s WHERE k = TOKEN(1) AND v IS NOT NULL" +
                                              "   PRIMARY KEY (v, k)",
                                              "k = token(1) AND v IS NOT NULL",
                                              "INSERT INTO %s(k, v) VALUES (0, 1)",
                                              "INSERT INTO %s(k, v) VALUES (2, 3)"));

        // UDF with lowercase name, shouldn't be quoted in the schema where clause
        assertRows(testFunctionInWhereClause("CREATE TABLE %s (k int PRIMARY KEY, v int)",
                                             "CREATE FUNCTION fun()" +
                                             "   CALLED ON NULL INPUT" +
                                             "   RETURNS int LANGUAGE java" +
                                             "   AS 'return 2;'",
                                             "CREATE MATERIALIZED VIEW %s AS " +
                                             "   SELECT * FROM %s WHERE k = fun() AND v IS NOT NULL" +
                                             "   PRIMARY KEY (v, k)",
                                             "k = fun() AND v IS NOT NULL",
                                             "INSERT INTO %s(k, v) VALUES (0, 1)",
                                             "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2));

        // UDF with uppercase name, should be quoted in the schema where clause
        assertRows(testFunctionInWhereClause("CREATE TABLE %s (k int PRIMARY KEY, v int)",
                                             "CREATE FUNCTION \"FUN\"()" +
                                             "   CALLED ON NULL INPUT" +
                                             "   RETURNS int" +
                                             "   LANGUAGE java" +
                                             "   AS 'return 2;'",
                                             "CREATE MATERIALIZED VIEW %s AS " +
                                             "   SELECT * FROM %s WHERE k = \"FUN\"() AND v IS NOT NULL" +
                                             "   PRIMARY KEY (v, k)",
                                             "k = \"FUN\"() AND v IS NOT NULL",
                                             "INSERT INTO %s(k, v) VALUES (0, 1)",
                                             "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2));

        // UDF with uppercase name conflicting with TOKEN keyword but not with native token function name,
        // should be quoted in the schema where clause
        assertRows(testFunctionInWhereClause("CREATE TABLE %s (k int PRIMARY KEY, v int)",
                                             "CREATE FUNCTION \"TOKEN\"(x int)" +
                                             "   CALLED ON NULL INPUT" +
                                             "   RETURNS int" +
                                             "   LANGUAGE java" +
                                             "   AS 'return x;'",
                                             "CREATE MATERIALIZED VIEW %s AS" +
                                             "   SELECT * FROM %s WHERE k = \"TOKEN\"(2) AND v IS NOT NULL" +
                                             "   PRIMARY KEY (v, k)",
                                             "k = \"TOKEN\"(2) AND v IS NOT NULL",
                                             "INSERT INTO %s(k, v) VALUES (0, 1)",
                                             "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2));

        // UDF with lowercase name conflicting with both TOKEN keyword and native token function name,
        // requires specifying the keyspace and should be quoted in the schema where clause
        assertRows(testFunctionInWhereClause("CREATE TABLE %s (k int PRIMARY KEY, v int)",
                                             "CREATE FUNCTION \"token\"(x int)" +
                                             "   CALLED ON NULL INPUT" +
                                             "   RETURNS int" +
                                             "   LANGUAGE java" +
                                             "   AS 'return x;'",
                                             "CREATE MATERIALIZED VIEW %s AS" +
                                             "   SELECT * FROM %s " +
                                             "   WHERE k = " + keyspace() + ".\"token\"(2) AND v IS NOT NULL" +
                                             "   PRIMARY KEY (v, k)",
                                             "k = " + keyspace() + ".\"token\"(2) AND v IS NOT NULL",
                                             "INSERT INTO %s(k, v) VALUES (0, 1)",
                                             "INSERT INTO %s(k, v) VALUES (2, 3)"), row(3, 2));
    }

    /**
     * Tests that truncating a table stops the ongoing builds of its materialized views,
     * so they don't write into the MV data that has been truncated in the base table.
     * <p>
     * See CASSANDRA-16567 for further details.
     */
    @Test
    @BMRules(rules = {
    @BMRule(name = "Block view builder tasks",
    targetClass = "ViewBuilderTask",
    targetMethod = "buildKey",
    action = "com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly" +
             "(org.apache.cassandra.cql3.ViewTest.blockViewBuild);"),
    @BMRule(name = "Unblock view builder tasks",
    targetClass = "ColumnFamilyStore",
    targetMethod = "truncateBlocking",
    action = "org.apache.cassandra.cql3.ViewTest.blockViewBuild.countDown();")
    })
    public void testTruncateWhileBuilding() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 0, 0, 0);
        createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                        "WHERE k IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL " +
                        "PRIMARY KEY (v, c, k)");

        // check that the delayed view builder tasks are either running or pending,
        // and that they haven't written anything yet
        assertThat(runningViewBuilds()).isPositive();
        assertFalse(SystemKeyspace.isViewBuilt(KEYSPACE, currentView()));
        waitForViewMutations();
        assertRows(executeView("SELECT * FROM %s"));

        // truncate the view, this should unblock the view builders, wait for their cancellation,
        // drop the sstables and, finally, start a new view build
        updateView("TRUNCATE %s");

        // check that there aren't any rows after truncating
        assertRows(executeView("SELECT * FROM %s"));

        // check that the view builder tasks finish and that the view is still empty after that
        Awaitility.await().untilAsserted(() -> assertEquals(0, runningViewBuilds()));
        assertTrue(SystemKeyspace.isViewBuilt(KEYSPACE, currentView()));
        waitForViewMutations();
        assertRows(executeView("SELECT * FROM %s"));
    }

    private static int runningViewBuilds()
    {
        return Metrics.getThreadPoolMetrics("ViewBuildExecutor")
                      .map(p -> p.activeTasks.getValue() + p.pendingTasks.getValue())
                      .orElse(0);
    }

    private UntypedResultSet testFunctionInWhereClause(String createTableQuery,
                                                       String createFunctionQuery,
                                                       String createViewQuery,
                                                       String expectedSchemaWhereClause,
                                                       String... insertQueries) throws Throwable
    {
        createTable(createTableQuery);

        if (createFunctionQuery != null)
        {
            execute(createFunctionQuery);
        }

        createView(createViewQuery);

        // Test the where clause stored in system_schema.views
        String schemaQuery = String.format("SELECT where_clause FROM %s.%s WHERE keyspace_name = ? AND view_name = ?",
                                           SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                           SchemaKeyspaceTables.VIEWS);
        assertRows(execute(schemaQuery, keyspace(), currentView()), row(expectedSchemaWhereClause));

        for (String insert : insertQueries)
        {
            execute(insert);
        }

        return executeView("SELECT * FROM %s");
    }
}
