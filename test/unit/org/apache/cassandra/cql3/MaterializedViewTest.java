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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import junit.framework.Assert;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaterializedViewTest extends CQLTester
{
    int protocolVersion = 4;
    private final List<String> materializedViews = new ArrayList<>();

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }
    @Before
    public void begin()
    {
        materializedViews.clear();
    }

    @After
    public void end() throws Throwable
    {
        for (String viewName : materializedViews)
            executeNet(protocolVersion, "DROP MATERIALIZED VIEW " + viewName);
    }

    private void createView(String name, String query) throws Throwable
    {
        executeNet(protocolVersion, String.format(query, name));
        // If exception is thrown, the view will not be added to the list; since it shouldn't have been created, this is
        // the desired behavior
        materializedViews.add(name);
    }

    private void updateMV(String query, Object... params) throws Throwable
    {
        executeNet(protocolVersion, query, params);
        while (!(((SEPExecutor) StageManager.getStage(Stage.MATERIALIZED_VIEW_MUTATION)).getPendingTasks() == 0
                 && ((SEPExecutor) StageManager.getStage(Stage.MATERIALIZED_VIEW_MUTATION)).getActiveCount() == 0))
        {
            Thread.sleep(1);
        }
    }

    @Test
    public void testPartitionTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateMV("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateMV("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from view1").size());

        updateMV("DELETE FROM %s WHERE k1 = 1");

        Assert.assertEquals(0, execute("select * from %s").size());
        Assert.assertEquals(0, execute("select * from view1").size());
    }

    @Test
    public void testClusteringKeyTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, c1 int , val int, PRIMARY KEY (k1, c1))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("view1", "CREATE MATERIALIZED VIEW view1 AS SELECT k1 FROM %%s WHERE k1 IS NOT NULL AND c1 IS NOT NULL AND val IS NOT NULL PRIMARY KEY (val, k1, c1)");

        updateMV("INSERT INTO %s (k1, c1, val) VALUES (1, 2, 200)");
        updateMV("INSERT INTO %s (k1, c1, val) VALUES (1, 3, 300)");

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from view1").size());

        updateMV("DELETE FROM %s WHERE k1 = 1 and c1 = 3");

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

        // Must include "IS NOT NULL" for primary keys
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s");
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Must include both when the partition key is composite
        try
        {
            createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
            Assert.fail("Should fail if compound primary is not completely filtered as NOT NULL");
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
            Assert.fail("Should fail if no primary key is filtered as NOT NULL");
        }
        catch (Exception e)
        {
        }

        // Can omit "k IS NOT NULL" because we have a sinlge partition key
        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
    }

    @Test
    public void testAccessAndSchema() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "asciival ascii, " +
                    "bigintval bigint, " +
                    "PRIMARY KEY((k, asciival)))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE bigintval IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL PRIMARY KEY (bigintval, k, asciival)");
        updateMV("INSERT INTO %s(k,asciival,bigintval)VALUES(?,?,?)", 0, "foo", 1L);

        try
        {
            updateMV("INSERT INTO mv1_test(k,asciival,bigintval) VALUES(?,?,?)", 1, "foo", 2L);
            Assert.fail("Shouldn't be able to modify a MV directly");
        }
        catch (Exception e)
        {
        }

        try
        {
            executeNet(protocolVersion, "ALTER TABLE mv1_test ADD foo text");
            Assert.fail("Should not be able to use alter table with MV");
        }
        catch (Exception e)
        {
        }

        try
        {
            executeNet(protocolVersion, "ALTER TABLE mv1_test WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
            Assert.fail("Should not be able to use alter table with MV");
        }
        catch (Exception e)
        {
        }

        executeNet(protocolVersion, "ALTER MATERIALIZED VIEW mv1_test WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");

        //Test alter add
        executeNet(protocolVersion, "ALTER TABLE %s ADD foo text");
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace(), "mv1_test");
        Assert.assertNotNull(metadata.getColumnDefinition(ByteBufferUtil.bytes("foo")));

        updateMV("INSERT INTO %s(k,asciival,bigintval,foo)VALUES(?,?,?,?)", 0, "foo", 1L, "bar");
        assertRows(execute("SELECT foo from %s"), row("bar"));

        //Test alter rename
        executeNet(protocolVersion, "ALTER TABLE %s RENAME asciival TO bar");

        assertRows(execute("SELECT bar from %s"), row("foo"));
        metadata = Schema.instance.getCFMetaData(keyspace(), "mv1_test");
        Assert.assertNotNull(metadata.getColumnDefinition(ByteBufferUtil.bytes("bar")));
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
            Assert.fail("MV on static should fail");
        }
        catch (InvalidQueryException e)
        {
        }

        createView("mv_static", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE val IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (val,k,c)");

        for (int i = 0; i < 100; i++)
            updateMV("INSERT into %s (k,c,sval,val)VALUES(?,?,?,?)", 0, i % 2, "bar" + i, "baz");

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
            updateMV("INSERT into %s (k,c,val)VALUES(?,?,?)", 0, i % 2, "baz");

        Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();

        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv_tstest").size());

        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));

        //Make sure an old TS does nothing
        updateMV("UPDATE %s USING TIMESTAMP 100 SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("baz"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(0), row(1));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"));

        //Latest TS
        updateMV("UPDATE %s SET val = ? where k = ? AND c = ?", "bar", 0, 0);
        assertRows(execute("SELECT val from %s where k = 0 and c = 0"), row("bar"));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "bar"), row(0));
        assertRows(execute("SELECT c from mv_tstest where k = 0 and val = ?", "baz"), row(1));
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
            Assert.fail("MV on counter should fail");
        }
        catch (InvalidQueryException e)
        {
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
            updateMV("INSERT into %s (k,asciival,bigintval,textval1,textval2)VALUES(?,?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i, "baz");

        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(50, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());

        Assert.assertEquals(100, execute("select * from mv_test1").size());

        //Check the builder works
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), asciival, bigintval, textval1)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(1000);

        Assert.assertEquals(100, execute("select * from mv_test2").size());

        createView("mv_test3", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE textval2 IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL AND bigintval IS NOT NULL AND textval1 IS NOT NULL PRIMARY KEY ((textval2, k), bigintval, textval1, asciival)");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test3"))
            Thread.sleep(1000);

        Assert.assertEquals(100, execute("select * from mv_test3").size());
        Assert.assertEquals(100, execute("select asciival from mv_test3 where textval2 = ? and k = ?", "baz", 0).size());

        //Write a RT and verify the data is removed from index
        updateMV("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

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
            updateMV("INSERT into %s (k,asciival,bigintval,textval1)VALUES(?,?,?,?)", 0, "foo", (long) i % 2, "bar" + i);

        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 0").size());
        Assert.assertEquals(1, execute("select * from %s where k = 0 and asciival = 'foo' and bigintval = 1").size());


        Assert.assertEquals(2, execute("select * from %s").size());
        Assert.assertEquals(2, execute("select * from mv").size());

        //Write a RT and verify the data is removed from index
        updateMV("DELETE FROM %s WHERE k = ? AND asciival = ? and bigintval = ?", 0, "foo", 0L);

        Assert.assertEquals(1, execute("select * from %s").size());
        Assert.assertEquals(1, execute("select * from mv").size());
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
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + " PRIMARY KEY ("
                               + def.name + ", asciival" + (def.name.toString().equals("k") ? "" : ", k") + ")";
                createView("mv2_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }


            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), asciival)";
                createView("mv3_" + def.name, query);

                Assert.fail("Should fail on duplicate name");
            }
            catch (Exception e)
            {
            }

            try
            {
                String query = "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL "
                               + (def.name.toString().equals("asciival") ? "" : "AND asciival IS NOT NULL ") + "PRIMARY KEY ((" + def.name + ", k), nonexistentcolumn)";
                createView("mv3_" + def.name, query);
                Assert.fail("Should fail with unknown base column");
            }
            catch (InvalidQueryException e)
            {
            }
        }

        updateMV("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "123123123123");
        updateMV("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(123123123123L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 123123123123L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L), row(0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0), row("ascii text"));


        //UPDATE BASE
        updateMV("INSERT INTO %s (k, asciival, bigintval) VALUES (?, ?, fromJson(?))", 0, "ascii text", "1");
        assertRows(execute("SELECT bigintval FROM %s WHERE k = ? and asciival = ?", 0, "ascii text"), row(1L));

        //Check the MV
        assertRows(execute("SELECT k, bigintval from mv1_asciival WHERE asciival = ?", "ascii text"), row(0, 1L));
        assertRows(execute("SELECT k, bigintval from mv2_k WHERE asciival = ? and k = ?", "ascii text", 0), row(0, 1L));
        assertRows(execute("SELECT k from mv1_bigintval WHERE bigintval = ?", 123123123123L));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 123123123123L, 0));
        assertRows(execute("SELECT asciival from mv3_bigintval where bigintval = ? AND k = ?", 1L, 0), row("ascii text"));


        //test truncate also truncates all MV
        updateMV("TRUNCATE %s");

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

        updateMV("INSERT INTO %s (k, intval, listval) VALUES (?, ?, fromJson(?))", 0, 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 0), row(0, list(1, 2, 3)));

        updateMV("INSERT INTO %s (k, intval) VALUES (?, ?)", 1, 1);
        updateMV("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 1, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 1), row(1, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv WHERE intval = ?", 1), row(1, list(1, 2, 3)));
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

        updateMV("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 0), row(0, 0));

        updateMV("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testDecimalUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "decimalval decimal, " +
                    "asciival ascii, " +
                    "PRIMARY KEY (k))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND decimalval IS NOT NULL PRIMARY KEY (decimalval, k)");

        updateMV("INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "ascii text");
        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));
        assertRows(execute("SELECT k, asciival from mv WHERE decimalval = fromJson(?)", "123123.123123"));
        assertRows(execute("SELECT k, asciival from mv WHERE decimalval = fromJson(?)", "123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as doubles
        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval, asciival FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123"), "ascii text"));
        assertRows(execute("SELECT k, asciival from mv WHERE decimalval = fromJson(?)", "\"123123.123123\""), row(0, "ascii text"));
    }

    @Test
    public void testAllTypes() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");

        createTable("CREATE TABLE %s (" +
                    "k int PRIMARY KEY, " +
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
                    "listval list<int>, " +
                    "frozenlistval frozen<list<int>>, " +
                    "setval set<uuid>, " +
                    "frozensetval frozen<set<uuid>>, " +
                    "mapval map<ascii, int>," +
                    "frozenmapval frozen<map<ascii, int>>," +
                    "tupleval frozen<tuple<int, ascii, uuid>>," +
                    "udtval frozen<" + myType + ">)");

        CFMetaData metadata = currentTableMetadata();

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        for (ColumnDefinition def : new HashSet<>(metadata.allColumns()))
        {
            try
            {
                createView("mv_" + def.name, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL PRIMARY KEY (" + def.name + ",k)");

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);

                if (def.isPartitionKey())
                    Assert.fail("MV on partition key should fail " + def);
            }
            catch (InvalidQueryException e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }
        }

        // fromJson() can only be used when the receiver type is known
        assertInvalidMessage("fromJson() cannot be used in the selection clause", "SELECT fromJson(asciival) FROM %s", 0, 0);

        String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$");
        createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$");

        // ================ ascii ================
        updateMV("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));

        //Check the MV
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateMV("UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateMV("DELETE FROM %s WHERE k = fromJson(?)", "0");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"));

        updateMV("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"), row(0, null));

        // ================ bigint ================
        updateMV("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k, asciival from mv_bigintval WHERE bigintval = ?", 123123123123L), row(0, "ascii text"));

        // ================ blob ================
        updateMV("INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));
        assertRows(execute("SELECT k, asciival from mv_blobval WHERE blobval = ?", ByteBufferUtil.bytes(1)), row(0, "ascii text"));

        // ================ boolean ================
        updateMV("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", false), row(0, "ascii text"));

        // ================ date ================
        updateMV("INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));
        assertRows(execute("SELECT k, asciival from mv_dateval WHERE dateval = fromJson(?)", "\"1987-03-23\""), row(0, "ascii text"));

        // ================ decimal ================
        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as doubles
        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        updateMV("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "\"-1.23E-12\""), row(0, "ascii text"));

        // ================ double ================
        updateMV("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ float ================
        updateMV("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ inet ================
        updateMV("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"::1\""), row(0, "ascii text"));

        // ================ int ================
        updateMV("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));
        assertRows(execute("SELECT k, asciival from mv_intval WHERE intval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ text (varchar) ================
        updateMV("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        updateMV("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"\\u2013\""), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, "ascii text"));

        // ================ time ================
        updateMV("INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));
        assertRows(execute("SELECT k, asciival from mv_timeval WHERE timeval = fromJson(?)", "\"07:35:07.000111222\""), row(0, "ascii text"));

        // ================ timestamp ================
        updateMV("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "123123123123"), row(0, "ascii text"));

        updateMV("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "\"2014-01-01\""), row(0, "ascii text"));

        // ================ timeuuid ================
        updateMV("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateMV("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_timeuuidval WHERE timeuuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ uuidval ================
        updateMV("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateMV("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_uuidval WHERE uuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ varint ================
        updateMV("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "123123123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as longs
        updateMV("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "\"1234567890123456789012345678901234567890\""), row(0, "ascii text"));

        // ================ lists ================
        updateMV("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));

        updateMV("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1)));

        updateMV("UPDATE %s SET listval = listval + fromJson(?) WHERE k = ?", "[2]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2)));

        updateMV("UPDATE %s SET listval = fromJson(?) + listval WHERE k = ?", "[0]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 1, 2)));

        updateMV("UPDATE %s SET listval[1] = fromJson(?) WHERE k = ?", "10", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 10, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 10, 2)));

        updateMV("DELETE listval[1] FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 2)));

        updateMV("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, null));

        // frozen
        updateMV("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"), row(0, "abcd"));

        updateMV("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[3, 2, 1]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(3, 2, 1)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[3, 2, 1]"), row(0, "abcd"));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(3, 2, 1)));

        updateMV("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list()));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list()));

        // ================ sets ================
        updateMV("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                 0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        // duplicates are okay, just like in CQL
        updateMV("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                 0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateMV("UPDATE %s SET setval = setval + fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateMV("UPDATE %s SET setval = setval - fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateMV("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));


        // frozen
        updateMV("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                 0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateMV("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                 0, "[\"6bddc89a-0000-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798")))));

        // ================ maps ================
        updateMV("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, map("a", 1, "b", 2)));

        updateMV("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "c", 3, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 2, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 2, "c", 3)));

        updateMV("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "b", 10, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 10, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 10, "c", 3)));

        updateMV("DELETE mapval[?] FROM %s WHERE k = ?", "b", 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "c", 3)));

        updateMV("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));

        // frozen
        updateMV("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, textval FROM mv_frozenmapval WHERE frozenmapval = fromJson(?)", "{\"a\": 1, \"b\": 2}"), row(0, "abcd"));

        updateMV("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 3}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));

        // ================ tuples ================
        updateMV("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        updateMV("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        // ================ UDTs ================
        updateMV("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // order of fields shouldn't matter
        updateMV("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test nulls
        updateMV("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test missing fields
        updateMV("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}"),
                   row(0, "abcd"));
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

        updateMV("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, ?) USING TTL 5", 1, 1, 1, 1);

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        updateMV("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 1, 1, 2);

        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        List<Row> results = executeNet(protocolVersion, "SELECT d FROM mv WHERE c = 2 AND a = 1 AND b = 1").all();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue("There should be a null result given back due to ttl expiry", results.get(0).isNull(0));
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
            updateMV("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP 1", 1, 1, i);
        }

        ResultSet mvRows = executeNet(protocolVersion, "SELECT c FROM mv");
        List<Row> rows = executeNet(protocolVersion, "SELECT c FROM %s").all();
        Assert.assertEquals("There should be exactly one row in base", 1, rows.size());
        int expected = rows.get(0).getInt("c");
        assertRowsNet(protocolVersion, mvRows, row(expected));
    }
}
