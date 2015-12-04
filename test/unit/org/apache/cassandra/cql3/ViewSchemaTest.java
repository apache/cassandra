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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;


public class ViewSchemaTest extends CQLTester
{
    int protocolVersion = 4;
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
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (\"theKey\" int, \"theClustering\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))");

        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 0, 0, 0);

        createView("mv_test", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s " +
                              "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL AND \"theValue\" IS NOT NULL " +
                              "PRIMARY KEY (\"theKey\", \"theClustering\")");

        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test"))
            Thread.sleep(10);
        createView("mv_test2", "CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"theValue\" FROM %%s " +
                               "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL AND \"theValue\" IS NOT NULL " +
                               "PRIMARY KEY (\"theKey\", \"theClustering\")");
        while (!SystemKeyspace.isViewBuilt(keyspace(), "mv_test2"))
            Thread.sleep(10);

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRows(execute("SELECT \"theKey\", \"theClustering\", \"theValue\" FROM " + mvname),
               row(0, 0, 0)
            );
        }

        executeNet(protocolVersion, "ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"");

        for (String mvname : Arrays.asList("mv_test", "mv_test2"))
        {
            assertRows(execute("SELECT \"theKey\", \"Col\", \"theValue\" FROM " + mvname),
                       row(0, 0, 0)
            );
        }
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
        updateView("INSERT INTO %s(k,asciival,bigintval)VALUES(?,?,?)", 0, "foo", 1L);

        try
        {
            updateView("INSERT INTO mv1_test(k,asciival,bigintval) VALUES(?,?,?)", 1, "foo", 2L);
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

        updateView("INSERT INTO %s(k,asciival,bigintval,foo)VALUES(?,?,?,?)", 0, "foo", 1L, "bar");
        assertRows(execute("SELECT foo from %s"), row("bar"));

        //Test alter rename
        executeNet(protocolVersion, "ALTER TABLE %s RENAME asciival TO bar");

        assertRows(execute("SELECT bar from %s"), row("foo"));
        metadata = Schema.instance.getCFMetaData(keyspace(), "mv1_test");
        Assert.assertNotNull(metadata.getColumnDefinition(ByteBufferUtil.bytes("bar")));
    }


    @Test
    public void testTwoTablesOneView() throws Throwable
    {
        execute("USE " + keyspace());
        executeNet(protocolVersion, "USE " + keyspace());

        createTable("CREATE TABLE " + keyspace() + ".dummy_table (" +
                "j int, " +
                "intval int, " +
                "PRIMARY KEY (j))");

        createTable("CREATE TABLE " + keyspace() + ".real_base (" +
                "k int, " +
                "intval int, " +
                "PRIMARY KEY (k))");

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + keyspace() + ".real_base WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");
        createView("mv2", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + keyspace() + ".dummy_table WHERE j IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, j)");

        updateView("INSERT INTO " + keyspace() + ".real_base (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO " + keyspace() + ".real_base (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));

        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));

        updateView("INSERT INTO " + keyspace() +".dummy_table (j, intval) VALUES(?, ?)", 0, 1);
        assertRows(execute("SELECT j, intval FROM " + keyspace() + ".dummy_table WHERE j = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testReuseName() throws Throwable
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

        executeNet(protocolVersion, "DROP MATERIALIZED VIEW mv");
        views.remove("mv");

        createView("mv", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from mv WHERE intval = ?", 1), row(0, 1));
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
        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));

        //Check the MV
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateView("UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateView("DELETE FROM %s WHERE k = fromJson(?)", "0");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"));

        updateView("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"), row(0, null));

        // ================ bigint ================
        updateView("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k, asciival from mv_bigintval WHERE bigintval = ?", 123123123123L), row(0, "ascii text"));

        // ================ blob ================
        updateView("INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));
        assertRows(execute("SELECT k, asciival from mv_blobval WHERE blobval = ?", ByteBufferUtil.bytes(1)), row(0, "ascii text"));

        // ================ boolean ================
        updateView("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", false), row(0, "ascii text"));

        // ================ date ================
        updateView("INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));
        assertRows(execute("SELECT k, asciival from mv_dateval WHERE dateval = fromJson(?)", "\"1987-03-23\""), row(0, "ascii text"));

        // ================ decimal ================
        updateView("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123.123123"));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as doubles
        updateView("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        updateView("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = fromJson(?)", "\"-1.23E-12\""), row(0, "ascii text"));

        // ================ double ================
        updateView("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ float ================
        updateView("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ inet ================
        updateView("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"127.0.0.1\""));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = fromJson(?)", "\"::1\""), row(0, "ascii text"));

        // ================ int ================
        updateView("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));
        assertRows(execute("SELECT k, asciival from mv_intval WHERE intval = fromJson(?)", "123123"), row(0, "ascii text"));

        // ================ text (varchar) ================
        updateView("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        updateView("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"\\u2013\""), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, "ascii text"));

        // ================ time ================
        updateView("INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));
        assertRows(execute("SELECT k, asciival from mv_timeval WHERE timeval = fromJson(?)", "\"07:35:07.000111222\""), row(0, "ascii text"));

        // ================ timestamp ================
        updateView("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "123123123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = fromJson(?)", "\"2014-01-01\""), row(0, "ascii text"));

        // ================ timeuuid ================
        updateView("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateView("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_timeuuidval WHERE timeuuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ uuidval ================
        updateView("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateView("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_uuidval WHERE uuidval = fromJson(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ varint ================
        updateView("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "123123123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as longs
        updateView("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = fromJson(?)", "\"1234567890123456789012345678901234567890\""), row(0, "ascii text"));

        // ================ lists ================
        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1)));

        updateView("UPDATE %s SET listval = listval + fromJson(?) WHERE k = ?", "[2]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2)));

        updateView("UPDATE %s SET listval = fromJson(?) + listval WHERE k = ?", "[0]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 1, 2)));

        updateView("UPDATE %s SET listval[1] = fromJson(?) WHERE k = ?", "10", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 10, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 10, 2)));

        updateView("DELETE listval[1] FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(0, 2)));

        updateView("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, null));

        // frozen
        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"), row(0, "abcd"));

        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[3, 2, 1]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(3, 2, 1)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[1, 2, 3]"));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = fromJson(?)", "[3, 2, 1]"), row(0, "abcd"));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list(3, 2, 1)));

        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list()));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, list()));

        // ================ sets ================
        updateView("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        // duplicates are okay, just like in CQL
        updateView("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("UPDATE %s SET setval = setval + fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("UPDATE %s SET setval = setval - fromJson(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));


        // frozen
        updateView("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                   0, "[\"6bddc89a-0000-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798")))));

        // ================ maps ================
        updateView("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""), row(0, map("a", 1, "b", 2)));

        updateView("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "c", 3, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 2, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 2, "c", 3)));

        updateView("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "b", 10, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 10, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 10, "c", 3)));

        updateView("DELETE mapval[?] FROM %s WHERE k = ?", "b", 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, map("a", 1, "c", 3)));

        updateView("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = fromJson(?)", "\"abcd\""),
                   row(0, null));

        // frozen
        updateView("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, textval FROM mv_frozenmapval WHERE frozenmapval = fromJson(?)", "{\"a\": 1, \"b\": 2}"), row(0, "abcd"));

        updateView("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 3}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));

        // ================ tuples ================
        updateView("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        updateView("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        // ================ UDTs ================
        updateView("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // order of fields shouldn't matter
        updateView("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test nulls
        updateView("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test missing fields
        updateView("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = fromJson(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}"),
                   row(0, "abcd"));
    }

    @Test
    public void testDropTableWithMV() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "a int," +
                "b int," +
                "c int," +
                "d int," +
                "PRIMARY KEY (a, b, c))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView(keyspace() + ".mv1",
                   "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");

        try
        {
            executeNet(protocolVersion, "DROP TABLE " + keyspace() + ".mv1");
            Assert.fail();
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("Cannot use DROP TABLE on Materialized View", e.getMessage());
        }
    }

    @Test
    public void testAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b text," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (b, a)");

        alterTable("ALTER TABLE %s ALTER b TYPE blob");
    }

    @Test
    public void testAlterReversedTypeBaseTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b text," +
                    "PRIMARY KEY (a, b))" +
                    "WITH CLUSTERING ORDER BY (b DESC)");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b) WITH CLUSTERING ORDER BY (b ASC)");

        alterTable("ALTER TABLE %s ALTER b TYPE blob");
    }

    @Test
    public void testAlterReversedTypeViewTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b text," +
                    "PRIMARY KEY (a, b))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b) WITH CLUSTERING ORDER BY (b DESC)");

        alterTable("ALTER TABLE %s ALTER b TYPE blob");
    }

    @Test
    public void testAlterClusteringViewTable() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b text," +
                    "PRIMARY KEY (a))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b) WITH CLUSTERING ORDER BY (b DESC)");

        alterTable("ALTER TABLE %s ALTER b TYPE blob");
    }

    @Test
    public void testAlterViewTableValue() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "a int," +
                    "b int," +
                    "PRIMARY KEY (a))");

        executeNet(protocolVersion, "USE " + keyspace());

        createView("mv1", "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %%s WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b) WITH CLUSTERING ORDER BY (b DESC)");

        assertInvalid("ALTER TABLE %s ALTER b TYPE blob");
    }
}
