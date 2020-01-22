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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.Uninterruptibles;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ViewTest extends CQLTester
{
    ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private final List<String> views = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        views.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : views)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        views.add(name);
    }

    private void updateView(String query, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getPendingTasks() == 0
                && ((SEPExecutor) StageManager.getStage(Stage.VIEW_MUTATION)).getActiveCount() == 0))
        {
            Thread.sleep(1);
        }
    }

    @Test
    public void testNonExistingOnes() throws Throwable
    {
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW " + KEYSPACE + ".view_does_not_exist");
        assertInvalidMessage("Cannot drop non existing materialized view", "DROP MATERIALIZED VIEW keyspace_does_not_exist.view_does_not_exist");

        execute("DROP MATERIALIZED VIEW IF EXISTS " + KEYSPACE + ".view_does_not_exist");
        execute("DROP MATERIALIZED VIEW IF EXISTS keyspace_does_not_exist.view_does_not_exist");
    }

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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1",
                   "CREATE MATERIALIZED VIEW view1 AS SELECT * FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL PRIMARY KEY (k1, c2, c1)");

        updateView("DELETE FROM %s USING TIMESTAMP 10 WHERE k1 = 1 and c1=1");

        if (flush)
            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

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
        assertRowsIgnoringOrder(execute("select k1,c1,c2,v1,v2 from view1"),
                                row(1, 0, 0, 0, 0),
                                row(1, 0, 1, 0, 1),
                                row(1, 2, 0, 2, 0));
    }

    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from view1").size());
    }


    @Test
    public void createMvWithUnrestrictedPKParts() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateView("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateView("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from view1").size());
    }

    @Test
    public void testPrimaryKeyIsNotNull() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must include both when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            fail("Should fail if compound primary is not completely filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        dropTable("DROP TABLE %s");

        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY(k, asciival))");
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Can omit "k IS NOT NULL" because we have a sinlge partition key
        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT * FROM %s WHERE sval IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (sval,k,c)");
            fail("Use of static column in a MV primary key should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %%s AS SELECT val, sval FROM %s WHERE val IS NOT NULL AND  k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val, k, c)");
            fail("Explicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        try
        {
            createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");
            fail("Implicit select of static column in MV should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT val,k,c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

        Assert.assertEquals(2, execute("select * from %s").size());

        assertRows(execute("SELECT sval from %s"), row("bar99"), row("bar99"));

        Assert.assertEquals(2, execute("select * from mv_static").size());

        assertInvalid("SELECT sval from mv_static");
    }


    @Test
    public void testOldTimestamps() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_tstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv_tstest").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateView("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"));

        //Latest TS
        updateView("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"), row(0));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(1));
    }

    @Test
    public void testRegularColumnTimestampUpdates() throws Throwable
    {
        // Regression test for CASSANDRA-10910

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_rctstest", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        updateView("UPDATE %s SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s SET c = ? WHERE k = ?", 1, 0);
        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(1, 0, 1));

        updateView("TRUNCATE %s");

        updateView("UPDATE %s USING TIMESTAMP 1 SET c = ?, val = ? WHERE k = ?", 0, 0, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 2 SET val = ? WHERE k = ?", 1, 0);
        updateView("UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE k = ?", 2, 0);
        updateView("UPDATE %s USING TIMESTAMP 3 SET val = ? WHERE k = ?", 2, 0);

        assertRows(execute("SELECT c, k, val FROM mv_rctstest"), row(2, 0, 2));
        assertRows(execute("SELECT c, k, val FROM mv_rctstest limit 1"), row(2, 0, 2));
    }

    @Test
    public void testCountersTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "count counter)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_counter", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE count IS NOT NULL AND k IS NOT NULL PRIMARY KEY (count,k)");
            fail("MV on counter should fail");
        }
        catch (InvalidQueryException e)
        {
        }
    }

    @Test
    public void testSuperCoumn() throws Throwable
    {
        String keyspace = createKeyspaceName();
        String table = createTableName();
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.superCFMD(keyspace, table, AsciiType.instance, AsciiType.instance));

        execute("USE " + keyspace);
        executeNet(protocolVersion, "USE " + keyspace);

        try
        {
            createView("mv_super_column", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + keyspace + "." + table + " WHERE key IS NOT NULL AND column1 IS NOT NULL PRIMARY KEY (key,column1)");
            fail("MV on SuperColumn table should fail");
        }
        catch (InvalidQueryException e)
        {
            assertEquals("Materialized views are not supported on SuperColumn tables", e.getMessage());
        }
    }

    @Test
    public void testDurationsTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "result duration)");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        try
        {
            createView("mv_duration", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE result IS NOT NULL AND k IS NOT NULL PRIMARY KEY (result,k)");
            fail("MV on duration should fail");
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("Cannot use Duration column 'result' in PRIMARY KEY of materialized view", e.getMessage());
        }
    }

    @Test
    public void complexTimestampUpdateTestWithFlush() throws Throwable
    {
        complexTimestampUpdateTest(true);
    }

    @Test
    public void complexTimestampUpdateTestWithoutFlush() throws Throwable
    {
        complexTimestampUpdateTest(false);
    }

    public void complexTimestampUpdateTest(boolean flush) throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, PRIMARY KEY (a, b))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());
        Keyspace ks = Keyspace.open(keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, a, b)");
        ks.getColumnFamilyStore("mv").disableAutoCompaction();

        //Set initial values TS=0, leaving e null and verify view
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d) VALUES (0, 0, 1, 0) USING TIMESTAMP 0");
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        //update c's timestamp TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());

        // change c's value and TS=3, tombstones c=1 and adds c=0 record
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? and b = ? ", 0, 0, 0);
        if (flush)
            FBUtilities.waitOnFutures(ks.flush());
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0));

        if(flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }


        //change c's value back to 1 with TS=4, check we can see d
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 4 SET c = ? WHERE a = ? and b = ? ", 1, 0, 0);
        if (flush)
        {
            ks.getColumnFamilyStore("mv").forceMajorCompaction();
            FBUtilities.waitOnFutures(ks.flush());
        }

        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, null));


        //Add e value @ TS=1
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 1 SET e = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d,e from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(0, 1));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=2
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET d = ? WHERE a = ? and b = ? ", 2, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(2));

        if (flush)
            FBUtilities.waitOnFutures(ks.flush());


        //Change d value @ TS=3
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? and b = ? ", 1, 0, 0);
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row(1));


        //Tombstone c
        executeNet(protocolVersion, "DELETE FROM %s WHERE a = ? and b = ?", 0, 0);
        assertRows(execute("SELECT d from mv"));

        //Add back without D
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c) VALUES (0, 0, 1)");

        //Make sure D doesn't pop back in.
        assertRows(execute("SELECT d from mv WHERE c = ? and a = ? and b = ?", 1, 0, 0), row((Object) null));


        //New partition
        // insert a row with timestamp 0
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, d, e) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP 0", 1, 0, 0, 0, 0);

        // overwrite pk and e with timestamp 1, but don't overwrite d
        executeNet(protocolVersion, "INSERT INTO %s (a, b, c, e) VALUES (?, ?, ?, ?) USING TIMESTAMP 1", 1, 0, 0, 0);

        // delete with timestamp 0 (which should only delete d)
        executeNet(protocolVersion, "DELETE FROM %s USING TIMESTAMP 0 WHERE a = ? AND b = ?", 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 2 SET c = ? WHERE a = ? AND b = ?", 1, 1, 0);
        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET c = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, null, 0)
        );

        executeNet(protocolVersion, "UPDATE %s USING TIMESTAMP 3 SET d = ? WHERE a = ? AND b = ?", 0, 1, 0);
        assertRows(execute("SELECT a, b, c, d, e from mv WHERE c = ? and a = ? and b = ?", 0, 1, 0),
                   row(1, 0, 0, 0, 0)
        );


    }

    @Test
    public void testBuilderWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "intval int, " +
                    "PRIMARY KEY (k, c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());


        for(int i = 0; i < 1024; i++)
            execute("INSERT INTO %s (k, c, intval) VALUES (?, ?, ?)", 0, i, 0);

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");


        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv"))
            Thread.sleep(1000);

        assertRows(execute("SELECT count(*) from %s WHERE k = ?", 0), row(1024L));
        assertRows(execute("SELECT count(*) from mv WHERE intval = ?", 0), row(1024L));
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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv_test1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1,textval2)VALUES(?,?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i, "baz");

        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());

        Assert.assertEquals(100, execute("select * from mv_test1").size());

        //Check the builder works
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test2").size());

        createView("mv_test3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test3"))
            Thread.sleep(10);

        Assert.assertEquals(100, execute("select * from mv_test3").size());
        Assert.assertEquals(100, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(50, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());
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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from mv").size());
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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval1 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL PRIMARY KEY ((textval1, k), asciival, bigintval)");

        for (int i = 0; i < 100; i++)
            updateView("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateView("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval >= ?", 0, "foo", 0L);

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from mv").size());
    }

    @Test
    public void testCompoundPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        CFMetaData metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnDefinition def : new HashSet<>(metadata.allColumns()))
        {
            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ("
                               + def.name + ", k" + (def.name.toString().equals("asciival") ? "" : ", asciival") + ")";
                createView("mv1_" + def.name, query);

                if (def.type.isMultiCell())
                    fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + " PRIMARY KEY ("
                               + def.name + ", asciival" + (def.name.toString().equals("k") ? "" : ", k") + ")";
                createView("mv2_" + def.name, query);

                if (def.type.isMultiCell())
                    fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                if (def.type.isMultiCell())
                    fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                fail("Should fail on duplicate name");
            }
            catch (Exception e)
            {
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), nonexistentcolumn)";
                createView("mv3_" + def.name, query);
                fail("Should fail with unknown base column");
            }
            catch (InvalidQueryException e)
            {
            }
        }

        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "123123123123");
        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 123123123123L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L), row(0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0), row("ascii text"));


        //UPDATE BASE
        updateView("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "1");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 1L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 1L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0), row("ascii text"));


        //test truncate also truncates all MV
        updateView("TRUNCATE %s");

        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0));
    }

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "listval list<int>, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))", 0, 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 0), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 1, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 1), row(1, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 1), row(1, list(1, 2, 3)));
    }

    @Test
    public void testFrozenCollectionsWithComplicatedInnerType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, intval int,  listval frozen<list<tuple<text,text>>>, PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND listval IS NOT NULL PRIMARY KEY (k, listval)");

        updateView("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))",
                   0,
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");

        // verify input
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));

        // update listval with the same value and it will be compared in view generator
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))",
                   0,
                   "[[\"a\",\"1\"], [\"b\",\"2\"], [\"c\",\"3\"]]");
        // verify result
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
        assertRows(execute("SELECT k, listval from mv"),
                   row(0, list(tuple("a", "1"), tuple("b", "2"), tuple("c", "3"))));
    }

    @Test
    public void testUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
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

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT a, b, c FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

        updateView("UPDATE %s SET d = ? WHERE a = ? AND b = ?", 0, 0, 0);
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));

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
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 0), row(0, 0, 0));
        assertRows(execute("SELECT a, b, c from mv WHERE b = ?", 1), row(0, 1, null));

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore("mv");
        cfs.forceBlockingFlush();
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void ttlTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        List<Row> results = executeNet(protocolVersion, "SELECT d FROM mv WHERE c = 2 AND a = 1 AND b = 1").all();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue("There should be a null result given back due to ttl expiry", results.get(0).isNull(0));
    }

    @Test
    public void ttlExpirationTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 3", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
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

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        String table = keyspace() + "." + currentTable();
        updateView("DELETE FROM " + table + " USING TIMESTAMP 6 WHERE a = 1 AND b = 1;");
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TIMESTAMP 3", 1, 1, 1, 1);
        Assert.assertEquals(0, executeNet(protocolVersion, "SELECT * FROM mv WHERE c = 1 AND a = 1 AND b = 1").all().size());
    }

    @Test
    public void conflictingTimestampTest() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE c IS NOT NULL AND a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (c, a, b)");

        for (int i = 0; i < 50; i++)
        {
            updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i);
        }

        ResultSet mvRows = executeNet(protocolVersion, "SELECT c FROM mv");
        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM %s").all();
        Assert.assertEquals("There should be exactly one row in base", 1, rows.size());
        int expected = rows.get(0).getInt("c");
        assertRowsNet(protocolVersion, mvRows, row(expected));
    }

    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b, c))" +
                    "WITH CLUSTERING ORDER BY (b ASC, c DESC)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c) WITH CLUSTERING ORDER BY (b DESC)");
        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c ASC)");
        createView("mv3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");
        createView("mv4", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, c, b) WITH CLUSTERING ORDER BY (c DESC)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 1, 1, 1);
        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 1, 2, 2, 2);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv2");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT b FROM mv3");
        assertRowsNet(protocolVersion, mvRows,
                      row(1),
                      row(2));

        mvRows = executeNet(protocolVersion, "SELECT c FROM mv4");
        assertRowsNet(protocolVersion, mvRows,
                      row(2),
                      row(1));
    }

    @Test
    public void testMultipleDeletes() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 2);
        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 3);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows,
                      row(1, 1),
                      row(1, 2),
                      row(1, 3));

        updateView(String.format("BEGIN UNLOGGED BATCH " +
                                 "DELETE FROM %s WHERE a = 1 AND b > 1 AND b < 3;" +
                                 "DELETE FROM %s WHERE a = 1;" +
                                 "APPLY BATCH", currentTable(), currentTable()));

        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows);
    }

    @Test
    public void testPrimaryKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }

    @Test
    public void testPartitionKeyOnlyTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY ((a, b)))");

        executeNet(protocolVersion, "USE " + keyspace());

        // Cannot use SELECT *, as those are always handled by the includeAll shortcut in View.updateAffectsView
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 1, 1);

        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(1, 1));
    }

    @Test
    public void testDeleteSingleColumnInViewClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (a, d, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testDeleteSingleColumnInViewPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND d IS NOT NULL PRIMARY KEY (d, a, b)");

        updateView("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?)", 0, 0, 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, 0));

        updateView("DELETE c FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b, c FROM mv1");
        assertRowsNet(protocolVersion, mvRows, row(0, 0, 0, null));

        updateView("DELETE d FROM %s WHERE a = ? AND b = ?", 0, 0);
        mvRows = executeNet(protocolVersion, "SELECT a, d, b FROM mv1");
        assertTrue(mvRows.isExhausted());
    }

    @Test
    public void testCollectionInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c map<int, text>," +
                    "PRIMARY KEY (a))");

        executeNet(protocolVersion, "USE " + keyspace());
        createView("mvmap", "CREATE MATERIALIZED VIEW %s AS SELECT a, b FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");

        updateView("INSERT INTO %s (a, b) VALUES (?, ?)", 0, 0);
        ResultSet mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, map(1, "1"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 1);
        assertRowsNet(protocolVersion, mvRows, row(1, 1));

        updateView("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, map(0, "0"));
        mvRows = executeNet(protocolVersion, "SELECT a, b FROM mvmap WHERE b = ?", 0);
        assertRowsNet(protocolVersion, mvRows, row(0, 0));
    }

    @Test
    public void testMultipleNonPrimaryKeysInView() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "c int," +
                    "d int," +
                    "e int," +
                    "PRIMARY KEY ((a, b), c))");

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((d, a), b, e, c)");
            fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }

        try
        {
            createView("mv_de", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL AND d IS NOT NULL AND e IS NOT NULL PRIMARY KEY ((a, b), c, d, e)");
            fail("Should have rejected a query including multiple non-primary key base columns");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testNullInClusteringColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, v1 text, v2 text, PRIMARY KEY (id1, id2))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT id1, v1, id2, v2" +
                   "  FROM %%s" +
                   "  WHERE id1 IS NOT NULL AND v1 IS NOT NULL AND id2 IS NOT NULL" +
                   "  PRIMARY KEY (id1, v1, id2)" +
                   "  WITH CLUSTERING ORDER BY (v1 DESC, id2 ASC)");

        execute("INSERT INTO %s (id1, id2, v1, v2) VALUES (?, ?, ?, ?)", 0, 1, "foo", "bar");

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, "foo", "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(0, "foo", 1, "bar"));

        executeNet(protocolVersion, "UPDATE %s SET v1=? WHERE id1=? AND id2=?", null, 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "bar"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));

        executeNet(protocolVersion, "UPDATE %s SET v2=? WHERE id1=? AND id2=?", "rab", 0, 1);
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1, null, "rab"));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"));
    }

    @Test
    public void testReservedKeywordsInMV() throws Throwable
    {
        createTable("CREATE TABLE %s (\"token\" int PRIMARY KEY, \"keyspace\" int)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv",
                   "CREATE MATERIALIZED VIEW %s AS" +
                   "  SELECT \"keyspace\", \"token\"" +
                   "  FROM %%s" +
                   "  WHERE \"keyspace\" IS NOT NULL AND \"token\" IS NOT NULL" +
                   "  PRIMARY KEY (\"keyspace\", \"token\")");

        execute("INSERT INTO %s (\"token\", \"keyspace\") VALUES (?, ?)", 0, 1);

        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM %s"), row(0, 1));
        assertRowsNet(protocolVersion, executeNet(protocolVersion, "SELECT * FROM mv"), row(1, 0));
    }

    public void testCreateMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "c int, " +
                "val int) WITH default_time_to_live = 60");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        // Must NOT include "default_time_to_live" for Materialized View creation
        try
        {
            createView("mv_ttl1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c) WITH default_time_to_live = 30");
            fail("Should fail if TTL is provided for materialized view");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testAlterMvWithTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
                    "c int, " +
                    "val int) WITH default_time_to_live = 60");

        createView("mv_ttl2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (k,c)");

        // Must NOT include "default_time_to_live" on alter Materialized View
        try
        {
            executeNet(protocolVersion, "ALTER MATERIALIZED VIEW %s WITH default_time_to_live = 30");
            fail("Should fail if TTL is provided while altering materialized view");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testViewBuilderResume() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "c int, " +
                    "val text, " +
                    "PRIMARY KEY(k,c))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        CompactionManager.instance.setCoreCompactorThreads(1);
        CompactionManager.instance.setMaximumCompactorThreads(1);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        for (int i = 0; i < 1024; i++)
            execute("INSERT into %s (k,c,val)VALUES(?,?,?)", i, i, ""+i);

        cfs.forceBlockingFlush();

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        cfs.enableAutoCompaction();
        List<Future<?>> futures = CompactionManager.instance.submitBackground(cfs);

        //Force a second MV on the same base table, which will restart the first MV builder...
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT val, k, c FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");


        //Compact the base table
        FBUtilities.waitOnFutures(futures);

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        assertRows(execute("SELECT count(*) FROM mv_test"), row(1024L));
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

    /**
     * Tests that a client warning is issued on materialized view creation.
     */
    @Test
    public void testClientWarningOnCreate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ClientWarn.instance.captureWarnings();
        String viewName = keyspace() + ".warning_view";
        execute("CREATE MATERIALIZED VIEW " + viewName +
                " AS SELECT v FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        views.add(viewName);
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

        executeNet(protocolVersion, "USE " + keyspace());

        boolean enableMaterializedViews = DatabaseDescriptor.getEnableMaterializedViews();
        try
        {
            DatabaseDescriptor.setEnableMaterializedViews(false);
            createView("view1", "CREATE MATERIALIZED VIEW %s AS SELECT v FROM %%s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
            fail("Should not be able to create a materialized view if they are disabled");
        }
        catch (Throwable e)
        {
            Assert.assertTrue(e instanceof InvalidQueryException);
            Assert.assertTrue(e.getMessage().contains("Materialized views are disabled"));
        }
        finally
        {
            DatabaseDescriptor.setEnableMaterializedViews(enableMaterializedViews);
        }
    }

    @Test
    public void viewOnCompactTableTest() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, v int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        executeNet(protocolVersion, "USE " + keyspace());
        try
        {
            createView("mv",
                       "CREATE MATERIALIZED VIEW %s AS SELECT a, b, value FROM %%s WHERE b IS NOT NULL PRIMARY KEY (b, a)");
            fail("Should have thrown an exception");
        }
        catch (Throwable t)
        {
            Assert.assertEquals("Undefined column name value",
                                t.getMessage());
        }
    }
}