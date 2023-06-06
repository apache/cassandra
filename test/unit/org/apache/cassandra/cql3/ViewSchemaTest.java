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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertTrue;

public class ViewSchemaTest extends ViewAbstractTest
{
    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (\"theKey\" int, \"theClustering\" int, \"theValue\" int, PRIMARY KEY (\"theKey\", \"theClustering\"))");

        execute("INSERT INTO %s (\"theKey\", \"theClustering\", \"theValue\") VALUES (?, ?, ?)", 0, 0, 0);

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL AND \"theValue\" IS NOT NULL " +
                                "PRIMARY KEY (\"theKey\", \"theClustering\")");

        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT \"theKey\", \"theClustering\", \"theValue\" FROM %s " +
                                "WHERE \"theKey\" IS NOT NULL AND \"theClustering\" IS NOT NULL AND \"theValue\" IS NOT NULL " +
                                "PRIMARY KEY (\"theKey\", \"theClustering\")");

        for (String mvname : Arrays.asList(mv1, mv2))
        {
            assertRows(execute("SELECT \"theKey\", \"theClustering\", \"theValue\" FROM " + mvname),
                       row(0, 0, 0));
        }

        executeNet("ALTER TABLE %s RENAME \"theClustering\" TO \"Col\"");

        for (String mvname : Arrays.asList(mv1, mv2))
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

        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                   "WHERE bigintval IS NOT NULL AND k IS NOT NULL AND asciival IS NOT NULL " +
                   "PRIMARY KEY (bigintval, k, asciival)");
        updateView("INSERT INTO %s(k,asciival,bigintval)VALUES(?,?,?)", 0, "foo", 1L);

        try
        {
            updateView("INSERT INTO " + currentView() + "(k,asciival,bigintval) VALUES(?,?,?)", 1, "foo", 2L);
            Assert.fail("Shouldn't be able to modify a MV directly");
        }
        catch (InvalidQueryException e)
        {
            Assertions.assertThat(e.getMessage()).contains("Cannot directly modify a materialized view");
        }

        try
        {
            executeViewNet("ALTER TABLE %s ADD foo text");
            Assert.fail("Should not be able to use alter table with MV");
        }
        catch (InvalidQueryException e)
        {
            Assertions.assertThat(e.getMessage()).contains("Cannot use ALTER TABLE on a materialized view");
        }

        try
        {
            executeViewNet("ALTER TABLE %s WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
            Assert.fail("Should not be able to use alter table with MV");
        }
        catch (InvalidQueryException e)
        {
            Assertions.assertThat(e.getMessage()).contains("Cannot use ALTER TABLE on a materialized view");
        }

        executeViewNet("ALTER MATERIALIZED VIEW %s WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");

        //Test alter add
        executeNet("ALTER TABLE %s ADD foo text");
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace(), currentView());
        Assert.assertNotNull(metadata.getColumn(ByteBufferUtil.bytes("foo")));

        updateView("INSERT INTO %s(k,asciival,bigintval,foo)VALUES(?,?,?,?)", 0, "foo", 1L, "bar");
        assertRows(execute("SELECT foo from %s"), row("bar"));

        //Test alter rename
        executeNet("ALTER TABLE %s RENAME asciival TO bar");

        assertRows(execute("SELECT bar from %s"), row("foo"));
        metadata = Schema.instance.getTableMetadata(keyspace(), currentView());
        Assert.assertNotNull(metadata.getColumn(ByteBufferUtil.bytes("bar")));
    }


    @Test
    public void testTwoTablesOneView() throws Throwable
    {
        createTable("CREATE TABLE " + keyspace() + ".dummy_table (" +
                    "j int, " +
                    "intval int, " +
                    "PRIMARY KEY (j))");

        createTable("CREATE TABLE " + keyspace() + ".real_base (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + keyspace() + ".real_base WHERE k IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, k)");
        createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM " + keyspace() + ".dummy_table WHERE j IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, j)");

        updateView("INSERT INTO " + keyspace() + ".real_base (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 0));
        assertRows(execute("SELECT k, intval from " + mv + " WHERE intval = ?", 0), row(0, 0));

        updateView("INSERT INTO " + keyspace() + ".real_base (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from " + mv + " WHERE intval = ?", 1), row(0, 1));

        assertRows(execute("SELECT k, intval FROM " + keyspace() + ".real_base WHERE k = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from " + mv + " WHERE intval = ?", 1), row(0, 1));

        updateView("INSERT INTO " + keyspace() + ".dummy_table (j, intval) VALUES(?, ?)", 0, 1);
        assertRows(execute("SELECT j, intval FROM " + keyspace() + ".dummy_table WHERE j = ?", 0), row(0, 1));
        assertRows(execute("SELECT k, intval from " + mv + " WHERE intval = ?", 1), row(0, 1));
    }

    @Test
    public void testReuseName() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "k int, " +
                    "intval int, " +
                    "PRIMARY KEY (k))");

        String view = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                 "WHERE k IS NOT NULL AND intval IS NOT NULL " +
                                 "PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 0));
        assertRows(executeView("SELECT k, intval from %s WHERE intval = ?", 0), row(0, 0));

        executeNet("DROP MATERIALIZED VIEW " + view);

        createView(view, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                         "WHERE k IS NOT NULL AND intval IS NOT NULL " +
                         "PRIMARY KEY (intval, k)");

        updateView("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 1);
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 1));
        assertRows(executeView("SELECT k, intval from %s WHERE intval = ?", 1), row(0, 1));
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

        TableMetadata metadata = currentTableMetadata();

        for (ColumnMetadata def : new HashSet<>(metadata.columns()))
        {
            try
            {
                createView("mv_" + def.name, "CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE " + def.name + " IS NOT NULL AND k IS NOT NULL PRIMARY KEY (" + def.name + ",k)");

                if (def.type.isMultiCell())
                    Assert.fail("MV on a multicell should fail " + def);

                if (def.isPartitionKey())
                    Assert.fail("MV on partition key should fail " + def);
            }
            catch (Exception e)
            {
                if (!def.type.isMultiCell() && !def.isPartitionKey())
                    Assert.fail("MV creation failed on " + def);
            }
        }

        // from_json() can only be used when the receiver type is known
        assertInvalidMessage("from_json() cannot be used in the selection clause", "SELECT from_json(asciival) FROM %s", 0, 0);

        String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$");
        createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$");

        // ================ ascii ================
        updateView("INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        // test that we can use from_json() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = from_json(?)", "0"), row("ascii \" text"));

        //Check the MV
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateView("UPDATE %s SET asciival = from_json(?) WHERE k = from_json(?)", "\"ascii \\\" text\"", "0");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"), row(0, null));

        updateView("DELETE FROM %s WHERE k = from_json(?)", "0");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0));
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii \" text"));

        updateView("INSERT INTO %s (k, asciival) VALUES (?, from_json(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, udtval from mv_asciival WHERE asciival = ?", "ascii text"), row(0, null));

        // ================ bigint ================
        updateView("INSERT INTO %s (k, bigintval) VALUES (?, from_json(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));
        assertRows(execute("SELECT k, asciival from mv_bigintval WHERE bigintval = ?", 123123123123L), row(0, "ascii text"));

        // ================ blob ================
        updateView("INSERT INTO %s (k, blobval) VALUES (?, from_json(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));
        assertRows(execute("SELECT k, asciival from mv_blobval WHERE blobval = ?", ByteBufferUtil.bytes(1)), row(0, "ascii text"));

        // ================ boolean ================
        updateView("INSERT INTO %s (k, booleanval) VALUES (?, from_json(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, booleanval) VALUES (?, from_json(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", true));
        assertRows(execute("SELECT k, asciival from mv_booleanval WHERE booleanval = ?", false), row(0, "ascii text"));

        // ================ date ================
        updateView("INSERT INTO %s (k, dateval) VALUES (?, from_json(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));
        assertRows(execute("SELECT k, asciival from mv_dateval WHERE dateval = from_json(?)", "\"1987-03-23\""), row(0, "ascii text"));

        // ================ decimal ================
        updateView("INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = from_json(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = from_json(?)", "123123.123123"));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = from_json(?)", "123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as doubles
        updateView("INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        updateView("INSERT INTO %s (k, decimalval) VALUES (?, from_json(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));
        assertRows(execute("SELECT k, asciival from mv_decimalval WHERE decimalval = from_json(?)", "\"-1.23E-12\""), row(0, "ascii text"));

        // ================ double ================
        updateView("INSERT INTO %s (k, doubleval) VALUES (?, from_json(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = from_json(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, doubleval) VALUES (?, from_json(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));
        assertRows(execute("SELECT k, asciival from mv_doubleval WHERE doubleval = from_json(?)", "123123"), row(0, "ascii text"));

        // ================ float ================
        updateView("INSERT INTO %s (k, floatval) VALUES (?, from_json(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = from_json(?)", "123123.123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, floatval) VALUES (?, from_json(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));
        assertRows(execute("SELECT k, asciival from mv_floatval WHERE floatval = from_json(?)", "123123"), row(0, "ascii text"));

        // ================ inet ================
        updateView("INSERT INTO %s (k, inetval) VALUES (?, from_json(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = from_json(?)", "\"127.0.0.1\""), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, inetval) VALUES (?, from_json(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = from_json(?)", "\"127.0.0.1\""));
        assertRows(execute("SELECT k, asciival from mv_inetval WHERE inetval = from_json(?)", "\"::1\""), row(0, "ascii text"));

        // ================ int ================
        updateView("INSERT INTO %s (k, intval) VALUES (?, from_json(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));
        assertRows(execute("SELECT k, asciival from mv_intval WHERE intval = from_json(?)", "123123"), row(0, "ascii text"));

        // ================ text (varchar) ================
        updateView("INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        updateView("INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = from_json(?)", "\"\\u2013\""), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, textval) VALUES (?, from_json(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));
        assertRows(execute("SELECT k, asciival from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, "ascii text"));

        // ================ time ================
        updateView("INSERT INTO %s (k, timeval) VALUES (?, from_json(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));
        assertRows(execute("SELECT k, asciival from mv_timeval WHERE timeval = from_json(?)", "\"07:35:07.000111222\""), row(0, "ascii text"));

        // ================ timestamp ================
        updateView("INSERT INTO %s (k, timestampval) VALUES (?, from_json(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = from_json(?)", "123123123123"), row(0, "ascii text"));

        updateView("INSERT INTO %s (k, timestampval) VALUES (?, from_json(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));
        assertRows(execute("SELECT k, asciival from mv_timestampval WHERE timestampval = from_json(?)", "\"2014-01-01\""), row(0, "ascii text"));

        // ================ timeuuid ================
        updateView("INSERT INTO %s (k, timeuuidval) VALUES (?, from_json(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateView("INSERT INTO %s (k, timeuuidval) VALUES (?, from_json(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_timeuuidval WHERE timeuuidval = from_json(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ uuidval ================
        updateView("INSERT INTO %s (k, uuidval) VALUES (?, from_json(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        updateView("INSERT INTO %s (k, uuidval) VALUES (?, from_json(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, asciival from mv_uuidval WHERE uuidval = from_json(?)", "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\""), row(0, "ascii text"));

        // ================ varint ================
        updateView("INSERT INTO %s (k, varintval) VALUES (?, from_json(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = from_json(?)", "123123123123"), row(0, "ascii text"));

        // accept strings for numbers that cannot be represented as longs
        updateView("INSERT INTO %s (k, varintval) VALUES (?, from_json(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));
        assertRows(execute("SELECT k, asciival from mv_varintval WHERE varintval = from_json(?)", "\"1234567890123456789012345678901234567890\""), row(0, "ascii text"));

        // ================ lists ================
        updateView("INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(1, 2, 3)));

        updateView("INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[1]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(1)));

        updateView("UPDATE %s SET listval = listval + from_json(?) WHERE k = ?", "[2]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(1, 2)));

        updateView("UPDATE %s SET listval = from_json(?) + listval WHERE k = ?", "[0]", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 1, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(0, 1, 2)));

        updateView("UPDATE %s SET listval[1] = from_json(?) WHERE k = ?", "10", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 10, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(0, 10, 2)));

        updateView("DELETE listval[1] FROM %s WHERE k = ?", 0);
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(0, 2)));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(0, 2)));

        updateView("INSERT INTO %s (k, listval) VALUES (?, from_json(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, listval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, null));

        // frozen
        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(1, 2, 3)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = from_json(?)", "[1, 2, 3]"), row(0, "abcd"));

        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[3, 2, 1]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(3, 2, 1)));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = from_json(?)", "[1, 2, 3]"));
        assertRows(execute("SELECT k, textval from mv_frozenlistval where frozenlistval = from_json(?)", "[3, 2, 1]"), row(0, "abcd"));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list(3, 2, 1)));

        updateView("INSERT INTO %s (k, frozenlistval) VALUES (?, from_json(?))", 0, "[]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list()));
        assertRows(execute("SELECT k, frozenlistval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, list()));

        // ================ sets ================
        updateView("INSERT INTO %s (k, setval) VALUES (?, from_json(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        // duplicates are okay, just like in CQL
        updateView("INSERT INTO %s (k, setval) VALUES (?, from_json(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("UPDATE %s SET setval = setval + from_json(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-0000-97fc-56847afe9799"), UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("UPDATE %s SET setval = setval - from_json(?) WHERE k = ?", "[\"6bddc89a-5644-0000-97fc-56847afe9799\"]", 0);
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("INSERT INTO %s (k, setval) VALUES (?, from_json(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, setval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, null));


        // frozen
        updateView("INSERT INTO %s (k, frozensetval) VALUES (?, from_json(?))",
                   0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))));

        updateView("INSERT INTO %s (k, frozensetval) VALUES (?, from_json(?))",
                   0, "[\"6bddc89a-0000-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"))))
        );
        assertRows(execute("SELECT k, frozensetval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, set(UUID.fromString("6bddc89a-0000-11e4-97fc-56847afe9799"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798")))));

        // ================ maps ================
        updateView("INSERT INTO %s (k, mapval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = from_json(?)", "\"abcd\""), row(0, map("a", 1, "b", 2)));

        updateView("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "c", 3, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 2, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 2, "c", 3)));

        updateView("UPDATE %s SET mapval[?] = ?  WHERE k = ?", "b", 10, 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "b", 10, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, map("a", 1, "b", 10, "c", 3)));

        updateView("DELETE mapval[?] FROM %s WHERE k = ?", "b", 0);
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0),
                   row(0, map("a", 1, "c", 3))
        );
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, map("a", 1, "c", 3)));

        updateView("INSERT INTO %s (k, mapval) VALUES (?, from_json(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));
        assertRows(execute("SELECT k, mapval from mv_textval WHERE textval = from_json(?)", "\"abcd\""),
                   row(0, null));

        // frozen
        updateView("INSERT INTO %s (k, frozenmapval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));
        assertRows(execute("SELECT k, textval FROM mv_frozenmapval WHERE frozenmapval = from_json(?)", "{\"a\": 1, \"b\": 2}"), row(0, "abcd"));

        updateView("INSERT INTO %s (k, frozenmapval) VALUES (?, from_json(?))", 0, "{\"b\": 2, \"a\": 3}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 3, "b", 2)));

        // ================ tuples ================
        updateView("INSERT INTO %s (k, tupleval) VALUES (?, from_json(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        updateView("INSERT INTO %s (k, tupleval) VALUES (?, from_json(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                   row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, textval FROM mv_tupleval WHERE tupleval = ?", tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))),
                   row(0, "abcd"));

        // ================ UDTs ================
        updateView("INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // order of fields shouldn't matter
        updateView("INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test nulls
        updateView("INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"),
                   row(0, "abcd"));

        // test missing fields
        updateView("INSERT INTO %s (k, udtval) VALUES (?, from_json(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                   row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}"));
        assertRows(execute("SELECT k, textval FROM mv_udtval WHERE udtval = from_json(?)", "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}"),
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

        executeNet("USE " + keyspace());

        String mv = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL PRIMARY KEY (a, b, c)");

        try
        {
            executeNet("DROP TABLE " + keyspace() + '.' + mv);
            Assert.fail();
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals("Cannot use DROP TABLE on a materialized view. Please use DROP MATERIALIZED VIEW instead.", e.getMessage());
        }
    }

    @Test
    public void testCreateMVWithFilteringOnNonPkColumn() throws Throwable
    {
        // SEE CASSANDRA-13798, we cannot properly support non-pk base column filtering for mv without huge storage
        // format changes.
        createTable("CREATE TABLE %s ( a int, b int, c int, d int, PRIMARY KEY (a, b, c))");

        executeNet("USE " + keyspace());

        assertInvalidMessage("Non-primary key columns can only be restricted with 'IS NOT NULL'",
                             "CREATE MATERIALIZED VIEW " + keyspace() + ".mv AS SELECT * FROM %s "
                             + "WHERE b IS NOT NULL AND c IS NOT NULL AND a IS NOT NULL "
                             + "AND d = 1 PRIMARY KEY (c, b, a)");
    }

    @Test
    public void testViewTokenRestrictions() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY(a))");

        execute("INSERT into %s (a,b,c,d) VALUES (?,?,?,?)", 1, 2, 3, 4);

        assertInvalidThrowMessage("Cannot use token relation when defining a materialized view", InvalidRequestException.class,
                                  "CREATE MATERIALIZED VIEW mv_test AS SELECT a,b,c FROM %s WHERE a IS NOT NULL and b IS NOT NULL and token(a) = token(1) PRIMARY KEY(b,a)");
    }

    @Test
    public void testCreateViewWithClusteringOrderOnMvOnly() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "pk int, " +
                    "c1 int," +
                    "c2 int," +
                    "c3 int," +
                    "v int, " +
                    "PRIMARY KEY (pk, c1, c2, c3))");

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 DESC, c1 ASC, c3 ASC)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC, c3 DESC)");

        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 1);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 2, 2);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 3);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 4);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 2, 5);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 6);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 2, 1, 7);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 2, 1, 1, 8);

        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 0),
                   row(0, 0, 0, 0, 0),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 2, 2),
                   row(0, 0, 1, 0, 3),
                   row(0, 0, 1, 1, 4),
                   row(0, 0, 1, 2, 5),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 2, 1, 7),
                   row(0, 2, 1, 1, 8));

        assertRows(execute("SELECT * FROM " + mv1 + " WHERE pk = ?", 0),
                   row(0, 2, 1, 1, 7),
                   row(0, 1, 0, 0, 3),
                   row(0, 1, 0, 1, 4),
                   row(0, 1, 0, 2, 5),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 2, 1, 8),
                   row(0, 0, 0, 0, 0),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 2, 2));

        assertRows(execute("SELECT * FROM " + mv2 + " WHERE pk = ?", 0),
                   row(0, 0, 0, 2, 2),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 2, 1, 8),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 0, 2, 5),
                   row(0, 1, 0, 1, 4),
                   row(0, 1, 0, 0, 3),
                   row(0, 2, 1, 1, 7));
    }

    @Test
    public void testCreateViewWithClusteringOrderOnBaseTableAndMv() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "pk int, " +
                    "c1 int," +
                    "c2 int," +
                    "c3 int," +
                    "v int, " +
                    "PRIMARY KEY (pk, c1, c2, c3)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC, c3 DESC)");

        String mv1 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3)");
        String mv2 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 DESC, c1 ASC, c3 ASC)");
        String mv3 = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE pk IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL and c3 IS NOT NULL PRIMARY KEY (pk, c2, c1, c3) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC, c3 DESC)");

        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 1, 1);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 2, 2);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 0, 3);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 4);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 2, 5);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 1, 1, 6);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 1, 2, 1, 7);
        updateView("INSERT INTO %s (pk, c1, c2, c3, v) VALUES (?, ?, ?, ?, ?)", 0, 2, 1, 1, 8);

        assertRows(execute("SELECT * FROM %s WHERE pk = ?", 0),
                   row(0, 2, 1, 1, 8),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 2, 1, 7),
                   row(0, 0, 0, 2, 2),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 0, 0),
                   row(0, 0, 1, 2, 5),
                   row(0, 0, 1, 1, 4),
                   row(0, 0, 1, 0, 3));

        assertRows(execute("SELECT * FROM " + mv1 + " WHERE pk = ?", 0),
                   row(0, 0, 0, 2, 2),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 2, 1, 8),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 0, 2, 5),
                   row(0, 1, 0, 1, 4),
                   row(0, 1, 0, 0, 3),
                   row(0, 2, 1, 1, 7));

        assertRows(execute("SELECT * FROM " + mv2 + " WHERE pk = ?", 0),
                   row(0, 2, 1, 1, 7),
                   row(0, 1, 0, 0, 3),
                   row(0, 1, 0, 1, 4),
                   row(0, 1, 0, 2, 5),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 2, 1, 8),
                   row(0, 0, 0, 0, 0),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 2, 2));

        assertRows(execute("SELECT * FROM " + mv3 + " WHERE pk = ?", 0),
                   row(0, 0, 0, 2, 2),
                   row(0, 0, 0, 1, 1),
                   row(0, 0, 0, 0, 0),
                   row(0, 1, 2, 1, 8),
                   row(0, 1, 1, 1, 6),
                   row(0, 1, 0, 2, 5),
                   row(0, 1, 0, 1, 4),
                   row(0, 1, 0, 0, 3),
                   row(0, 2, 1, 1, 7));
    }

    @Test
    public void testViewMetadataCQLNotIncludeAllColumn()
    {
        String createBase = "CREATE TABLE IF NOT EXISTS %s (" +
                            "pk1 int," +
                            "pk2 int," +
                            "ck1 int," +
                            "ck2 int," +
                            "reg1 int," +
                            "reg2 list<int>," +
                            "reg3 int," +
                            "PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH " +
                            "CLUSTERING ORDER BY (ck1 ASC, ck2 ASC);";

        String createView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT pk1, pk2, ck1, ck2, reg1, reg2 FROM %s "
                            + "WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL PRIMARY KEY((pk2, pk1), ck2, ck1)";

        String expectedViewSnapshot = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS\n" +
                                      "    SELECT pk2, pk1, ck2, ck1, reg1, reg2\n" +
                                      "    FROM %s.%s\n" +
                                      "    WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL\n" +
                                      "    PRIMARY KEY ((pk2, pk1), ck2, ck1)\n" +
                                      " WITH ID = %s\n" +
                                      "    AND CLUSTERING ORDER BY (ck2 ASC, ck1 ASC)";

        testViewMetadataCQL(createBase,
                            createView,
                            expectedViewSnapshot);
    }

    @Test
    public void testViewMetadataCQLIncludeAllColumn()
    {
        String createBase = "CREATE TABLE IF NOT EXISTS %s (" +
                            "pk1 int," +
                            "pk2 int," +
                            "ck1 int," +
                            "ck2 int," +
                            "reg1 int," +
                            "reg2 list<int>," +
                            "reg3 int," +
                            "PRIMARY KEY ((pk1, pk2), ck1, ck2)) WITH " +
                            "CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);";

        String createView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT * FROM %s "
                            + "WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL PRIMARY KEY((pk2, pk1), ck2, ck1)";

        String expectedViewSnapshot = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS\n" +
                                      "    SELECT *\n" +
                                      "    FROM %s.%s\n" +
                                      "    WHERE pk2 IS NOT NULL AND pk1 IS NOT NULL AND ck2 IS NOT NULL AND ck1 IS NOT NULL\n" +
                                      "    PRIMARY KEY ((pk2, pk1), ck2, ck1)\n" +
                                      " WITH ID = %s\n" +
                                      "    AND CLUSTERING ORDER BY (ck2 DESC, ck1 ASC)";

        testViewMetadataCQL(createBase,
                            createView,
                            expectedViewSnapshot);
    }

    @Test
    public void testAlterViewIfExists() throws Throwable
    {
        executeNet("USE " + keyspace());
        executeNet("ALTER MATERIALIZED VIEW IF EXISTS mv1_test WITH compaction = { 'class' : 'LeveledCompactionStrategy' }");
    }

    private void testViewMetadataCQL(String createBase, String createView, String viewSnapshotSchema)
    {
        String base = createTable(createBase);

        String view = createView(createView);

        Keyspace keyspace = Keyspace.open(keyspace());
        ColumnFamilyStore mv = keyspace.getColumnFamilyStore(view);
        assertTrue(SchemaCQLHelper.getTableMetadataAsCQL(mv.metadata(), keyspace.getMetadata())
                                  .startsWith(String.format(viewSnapshotSchema,
                                                            keyspace(),
                                                            view,
                                                            keyspace(),
                                                            base,
                                                            mv.metadata().id)));
    }
}
