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

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        StorageService.instance.setPartitionerUnsafe(ByteOrderedPartitioner.instance);
    }

    @Test
    public void testSelectJsonWithPagingWithFrozenTuple() throws Throwable
    {
        final UUID uuid = UUID.fromString("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc");
        final Object partitionKey = tuple(uuid, 2);

        createTable("CREATE TABLE %s (k1 FROZEN<TUPLE<uuid, int>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))");

        // prepare data
        for (int i = 1; i < 5; i++)
            execute("INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, tuple(uuid, i), i);

        for (int pageSize = 1; pageSize < 6; pageSize++)
        {
            // SELECT JSON
            assertRowsNet(executeNetWithPaging("SELECT JSON * FROM %s", pageSize),
                           row("{\"k1\": [\"" + uuid + "\", 2], \"c1\": [\"" + uuid + "\", 1], \"value\": 1}"),
                           row("{\"k1\": [\"" + uuid + "\", 2], \"c1\": [\"" + uuid + "\", 2], \"value\": 2}"),
                           row("{\"k1\": [\"" + uuid + "\", 2], \"c1\": [\"" + uuid + "\", 3], \"value\": 3}"),
                           row("{\"k1\": [\"" + uuid + "\", 2], \"c1\": [\"" + uuid + "\", 4], \"value\": 4}"));

            // SELECT toJson(column)
            assertRowsNet(executeNetWithPaging("SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          row("[\"" + uuid + "\", 2]", "[\"" + uuid + "\", 1]", "1"),
                          row("[\"" + uuid + "\", 2]", "[\"" + uuid + "\", 2]", "2"),
                          row("[\"" + uuid + "\", 2]", "[\"" + uuid + "\", 3]", "3"),
                          row("[\"" + uuid + "\", 2]", "[\"" + uuid + "\", 4]", "4"));
        }
    }

    @Test
    public void testSelectJsonWithPagingWithFrozenMap() throws Throwable
    {
        final UUID uuid = UUID.fromString("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc");
        final Object partitionKey = map(1, tuple(uuid, 1), 2, tuple(uuid, 2));

        createTable("CREATE TABLE %s (k1 FROZEN<map<int, tuple<uuid, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))");

        // prepare data
        for (int i = 1; i < 5; i++)
            execute("INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, tuple(uuid, i), i);

        for (int pageSize = 1; pageSize < 6; pageSize++)
        {
            // SELECT JSON
            assertRowsNet(executeNetWithPaging("SELECT JSON * FROM %s", pageSize),
                          row("{\"k1\": {\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}, \"c1\": [\"" + uuid + "\", 1], \"value\": 1}"),
                          row("{\"k1\": {\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}, \"c1\": [\"" + uuid + "\", 2], \"value\": 2}"),
                          row("{\"k1\": {\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}, \"c1\": [\"" + uuid + "\", 3], \"value\": 3}"),
                          row("{\"k1\": {\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}, \"c1\": [\"" + uuid + "\", 4], \"value\": 4}"));

            // SELECT toJson(column)
            assertRowsNet(executeNetWithPaging("SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          row("{\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}", "[\"" + uuid + "\", 1]", "1"),
                          row("{\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}", "[\"" + uuid + "\", 2]", "2"),
                          row("{\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}", "[\"" + uuid + "\", 3]", "3"),
                          row("{\"1\": [\"" + uuid + "\", 1], \"2\": [\"" + uuid + "\", 2]}", "[\"" + uuid + "\", 4]", "4"));
        }
    }

    @Test
    public void testSelectJsonWithPagingWithFrozenSet() throws Throwable
    {
        final UUID uuid = UUID.fromString("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc");
        final Object partitionKey = set(tuple(list(1, 2), 1), tuple(list(2, 3), 2));

        createTable("CREATE TABLE %s (k1 frozen<set<tuple<list<int>, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))");

        // prepare data
        for (int i = 1; i < 5; i++)
            execute("INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, tuple(uuid, i), i);

        for (int pageSize = 1; pageSize < 6; pageSize++)
        {
            // SELECT JSON
            assertRowsNet(executeNetWithPaging("SELECT JSON * FROM %s", pageSize),
                          row("{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + uuid + "\", 1], \"value\": 1}"),
                          row("{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + uuid + "\", 2], \"value\": 2}"),
                          row("{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + uuid + "\", 3], \"value\": 3}"),
                          row("{\"k1\": [[[1, 2], 1], [[2, 3], 2]], \"c1\": [\"" + uuid + "\", 4], \"value\": 4}"));

            // SELECT toJson(column)
            assertRowsNet(executeNetWithPaging("SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          row("[[[1, 2], 1], [[2, 3], 2]]", "[\"" + uuid + "\", 1]", "1"),
                          row("[[[1, 2], 1], [[2, 3], 2]]", "[\"" + uuid + "\", 2]", "2"),
                          row("[[[1, 2], 1], [[2, 3], 2]]", "[\"" + uuid + "\", 3]", "3"),
                          row("[[[1, 2], 1], [[2, 3], 2]]", "[\"" + uuid + "\", 4]", "4"));
        }
    }

    @Test
    public void testSelectJsonWithPagingWithFrozenList() throws Throwable
    {
        final UUID uuid = UUID.fromString("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc");
        final Object partitionKey = list(tuple(uuid, 2), tuple(uuid, 3));

        createTable("CREATE TABLE %s (k1 frozen<list<tuple<uuid, int>>>, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))");

        // prepare data
        for (int i = 1; i < 5; i++)
            execute("INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, tuple(uuid, i), i);

        for (int pageSize = 1; pageSize < 6; pageSize++)
        {
        // SELECT JSON
        assertRowsNet(executeNetWithPaging("SELECT JSON * FROM %s", pageSize),
                      row("{\"k1\": [[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]], \"c1\": [\"" + uuid + "\", 1], \"value\": 1}"),
                      row("{\"k1\": [[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]], \"c1\": [\"" + uuid + "\", 2], \"value\": 2}"),
                      row("{\"k1\": [[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]], \"c1\": [\"" + uuid + "\", 3], \"value\": 3}"),
                      row("{\"k1\": [[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]], \"c1\": [\"" + uuid + "\", 4], \"value\": 4}"));

        // SELECT toJson(column)
        assertRowsNet(executeNetWithPaging("SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                      row("[[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]]", "[\"" + uuid + "\", 1]", "1"),
                      row("[[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]]", "[\"" + uuid + "\", 2]", "2"),
                      row("[[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]]", "[\"" + uuid + "\", 3]", "3"),
                      row("[[\"" + uuid + "\", 2], [\"" + uuid + "\", 3]]", "[\"" + uuid + "\", 4]", "4"));
        }
    }

    @Test
    public void testSelectJsonWithPagingWithFrozenUDT() throws Throwable
    {
        final UUID uuid = UUID.fromString("2dd2cd62-6af3-4cf6-96fc-91b9ab62eedc");

        String typeName = createType("CREATE TYPE %s (a int, b int, c list<text>)");
        createTable("CREATE TABLE %s (k1 frozen<" + typeName + ">, c1 frozen<tuple<uuid, int>>, value int, PRIMARY KEY (k1, c1))");

        final Object partitionKey = userType("a", 1, "b", 2, "c", list("1", "2"));

        // prepare data
        for (int i = 1; i < 5; i++)
        execute("INSERT INTO %s (k1, c1, value) VALUES (?, ?, ?)", partitionKey, tuple(uuid, i), i);

        for (int pageSize = 1; pageSize < 6; pageSize++)
        {
            // SELECT JSON
            assertRowsNet(executeNetWithPaging("SELECT JSON * FROM %s", pageSize),
                          row("{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + uuid + "\", 1], \"value\": 1}"),
                          row("{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + uuid + "\", 2], \"value\": 2}"),
                          row("{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + uuid + "\", 3], \"value\": 3}"),
                          row("{\"k1\": {\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}, \"c1\": [\"" + uuid + "\", 4], \"value\": 4}"));

            // SELECT toJson(column)
            assertRowsNet(executeNetWithPaging("SELECT toJson(k1), toJson(c1), toJson(value) FROM %s", pageSize),
                          row("{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + uuid + "\", 1]", "1"),
                          row("{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + uuid + "\", 2]", "2"),
                          row("{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + uuid + "\", 3]", "3"),
                          row("{\"a\": 1, \"b\": 2, \"c\": [\"1\", \"2\"]}", "[\"" + uuid + "\", 4]", "4"));
        }
    }

    @Test
    public void testFromJsonFct() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");
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
                "smallintval smallint, " +
                "textval text, " +
                "timeval time, " +
                "timestampval timestamp, " +
                "timeuuidval timeuuid, " +
                "tinyintval tinyint, " +
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
                "udtval frozen<" + typeName + ">," +
                "durationval duration)");

        // fromJson() can only be used when the receiver type is known
        assertInvalidMessage("fromJson() cannot be used in the selection clause", "SELECT fromJson(asciival) FROM %s", 0, 0);

        String func1 = createFunction(KEYSPACE, "int", "CREATE FUNCTION %s (a int) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return a.toString(); $$");
        createFunctionOverload(func1, "int", "CREATE FUNCTION %s (a text) CALLED ON NULL INPUT RETURNS text LANGUAGE java AS $$ return new String(a); $$");

        assertInvalidMessage("Ambiguous call to function",
                "INSERT INTO %s (k, textval) VALUES (?, " + func1 + "(fromJson(?)))", 0, "123");

        // fails JSON parsing
        assertInvalidMessage("Could not decode JSON string '\u038E\u0394\u03B4\u03E0'",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\u038E\u0394\u03B4\u03E0");

        // handle nulls
        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, null);
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, null));

        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, null);
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, null));

        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, null);
        assertRows(execute("SELECT k, udtval FROM %s WHERE k = ?", 0), row(0, null));

        // ================ ascii ================
        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii text"));

        execute("INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"ascii \\\" text\"");
        assertRows(execute("SELECT k, asciival FROM %s WHERE k = ?", 0), row(0, "ascii \" text"));

        assertInvalidMessage("Invalid ASCII character in string literal",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "\"\\u1fff\\u2013\\u33B4\\u2014\"");

        assertInvalidMessage("Expected an ascii string, but got a Integer",
                "INSERT INTO %s (k, asciival) VALUES (?, fromJson(?))", 0, "123");

        // test that we can use fromJson() in other valid places in queries
        assertRows(execute("SELECT asciival FROM %s WHERE k = fromJson(?)", "0"), row("ascii \" text"));
        execute("UPDATE %s SET asciival = fromJson(?) WHERE k = fromJson(?)", "\"ascii \\\" text\"", "0");
        execute("DELETE FROM %s WHERE k = fromJson(?)", "0");

        // ================ bigint ================
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        // strings are also accepted
        execute("INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"123123123123\"");
        assertRows(execute("SELECT k, bigintval FROM %s WHERE k = ?", 0), row(0, 123123123123L));

        // overflow (Long.MAX_VALUE + 1)
        assertInvalidMessage("Expected a bigint value, but got a",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "9223372036854775808");

        assertInvalidMessage("Expected a bigint value, but got a",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to make long from",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "\"abc\"");

        assertInvalidMessage("Expected a bigint value, but got a",
                "INSERT INTO %s (k, bigintval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        // ================ blob ================
        execute("INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x00000001\"");
        assertRows(execute("SELECT k, blobval FROM %s WHERE k = ?", 0), row(0, ByteBufferUtil.bytes(1)));

        assertInvalidMessage("Value 'xyzz' is not a valid blob representation",
            "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("String representation of blob is missing 0x prefix: 123",
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"123\"");

        assertInvalidMessage("Value '0x123' is not a valid blob representation",
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "\"0x123\"");

        assertInvalidMessage("Value '123' is not a valid blob representation",
                "INSERT INTO %s (k, blobval) VALUES (?, fromJson(?))", 0, "123");

        // ================ boolean ================
        execute("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "true");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, true));

        execute("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "false");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));

        // strings are also accepted
        execute("INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "\"false\"");
        assertRows(execute("SELECT k, booleanval FROM %s WHERE k = ?", 0), row(0, false));

        assertInvalidMessage("Unable to make boolean from",
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "\"abc\"");

        assertInvalidMessage("Expected a boolean value, but got a Integer",
                "INSERT INTO %s (k, booleanval) VALUES (?, fromJson(?))", 0, "123");

        // ================ date ================
        execute("INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"1987-03-23\"");
        assertRows(execute("SELECT k, dateval FROM %s WHERE k = ?", 0), row(0, SimpleDateSerializer.dateStringToDays("1987-03-23")));

        assertInvalidMessage("Expected a string representation of a date",
                "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Unable to coerce 'xyz' to a formatted date",
                "INSERT INTO %s (k, dateval) VALUES (?, fromJson(?))", 0, "\"xyz\"");

        // ================ decimal ================
        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123")));

        // accept strings for numbers that cannot be represented as doubles
        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"123123.123123\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("123123.123123")));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"-1.23E-12\"");
        assertRows(execute("SELECT k, decimalval FROM %s WHERE k = ?", 0), row(0, new BigDecimal("-1.23E-12")));

        assertInvalidMessage("Value 'xyzz' is not a valid representation of a decimal value",
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Value 'true' is not a valid representation of a decimal value",
                "INSERT INTO %s (k, decimalval) VALUES (?, fromJson(?))", 0, "true");

        // ================ double ================
        execute("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.123123d));

        execute("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));

        // strings are also accepted
        execute("INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"123123\"");
        assertRows(execute("SELECT k, doubleval FROM %s WHERE k = ?", 0), row(0, 123123.0d));

        assertInvalidMessage("Unable to make double from",
                "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a double value, but got",
                "INSERT INTO %s (k, doubleval) VALUES (?, fromJson(?))", 0, "true");

        // ================ float ================
        execute("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123.123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.123123f));

        execute("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));

        // strings are also accepted
        execute("INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"123123.0\"");
        assertRows(execute("SELECT k, floatval FROM %s WHERE k = ?", 0), row(0, 123123.0f));

        assertInvalidMessage("Unable to make float from",
                "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a float value, but got a",
                "INSERT INTO %s (k, floatval) VALUES (?, fromJson(?))", 0, "true");

        // ================ inet ================
        execute("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"127.0.0.1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("127.0.0.1")));

        execute("INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"::1\"");
        assertRows(execute("SELECT k, inetval FROM %s WHERE k = ?", 0), row(0, InetAddress.getByName("::1")));

        assertInvalidMessage("Unable to make inet address from 'xyzz'",
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a string representation of an inet value, but got a Integer",
                "INSERT INTO %s (k, inetval) VALUES (?, fromJson(?))", 0, "123");

        // ================ int ================
        execute("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123123");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        // strings are also accepted
        execute("INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"123123\"");
        assertRows(execute("SELECT k, intval FROM %s WHERE k = ?", 0), row(0, 123123));

        // int overflow (2 ^ 32, or Integer.MAX_INT + 1)
        assertInvalidMessage("Expected an int value, but got a",
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "2147483648");

        assertInvalidMessage("Expected an int value, but got a",
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected an int value, but got a",
                "INSERT INTO %s (k, intval) VALUES (?, fromJson(?))", 0, "true");

        // ================ smallint ================
        execute("INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "32767");
        assertRows(execute("SELECT k, smallintval FROM %s WHERE k = ?", 0), row(0, (short) 32767));

        // strings are also accepted
        execute("INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "\"32767\"");
        assertRows(execute("SELECT k, smallintval FROM %s WHERE k = ?", 0), row(0, (short) 32767));

        // smallint overflow (Short.MAX_VALUE + 1)
        assertInvalidMessage("Unable to make short from",
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "32768");

        assertInvalidMessage("Unable to make short from",
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to make short from",
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a short value, but got a Boolean",
                "INSERT INTO %s (k, smallintval) VALUES (?, fromJson(?))", 0, "true");

        // ================ tinyint ================
        execute("INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "127");
        assertRows(execute("SELECT k, tinyintval FROM %s WHERE k = ?", 0), row(0, (byte) 127));

        // strings are also accepted
        execute("INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "\"127\"");
        assertRows(execute("SELECT k, tinyintval FROM %s WHERE k = ?", 0), row(0, (byte) 127));

        // tinyint overflow (Byte.MAX_VALUE + 1)
        assertInvalidMessage("Unable to make byte from",
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "128");

        assertInvalidMessage("Unable to make byte from",
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to make byte from",
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Expected a byte value, but got a Boolean",
                "INSERT INTO %s (k, tinyintval) VALUES (?, fromJson(?))", 0, "true");

        // ================ text (varchar) ================
        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, ""));

        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"abcd\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "abcd"));

        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"some \\\" text\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "some \" text"));

        execute("INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "\"\\u2013\"");
        assertRows(execute("SELECT k, textval FROM %s WHERE k = ?", 0), row(0, "\u2013"));

        assertInvalidMessage("Expected a UTF-8 string, but got a Integer",
                "INSERT INTO %s (k, textval) VALUES (?, fromJson(?))", 0, "123");

        // ================ time ================
        execute("INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, timeval FROM %s WHERE k = ?", 0), row(0, TimeSerializer.timeStringToLong("07:35:07.000111222")));

        assertInvalidMessage("Expected a string representation of a time value",
                "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "123456");

        assertInvalidMessage("Unable to coerce 'xyz' to a formatted time",
                "INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"xyz\"");

        // ================ timestamp ================
        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new Date(123123123123L)));

        execute("INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"2014-01-01\"");
        assertRows(execute("SELECT k, timestampval FROM %s WHERE k = ?", 0), row(0, new SimpleDateFormat("y-M-d").parse("2014-01-01")));

        assertInvalidMessage("Expected a long or a datestring representation of a timestamp value, but got a Double",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "123.456");

        assertInvalidMessage("Unable to coerce 'abcd' to a formatted date",
                "INSERT INTO %s (k, timestampval) VALUES (?, fromJson(?))", 0, "\"abcd\"");

        // ================ timeuuid ================
        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, timeuuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("TimeUUID supports only version 1 UUIDs",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-0000-000000000000\"");

        assertInvalidMessage("Expected a string representation of a timeuuid, but got a Integer",
                "INSERT INTO %s (k, timeuuidval) VALUES (?, fromJson(?))", 0, "123");

         // ================ uuidval ================
        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        execute("INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"6BDDC89A-5644-11E4-97FC-56847AFE9799\"");
        assertRows(execute("SELECT k, uuidval FROM %s WHERE k = ?", 0), row(0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));

        assertInvalidMessage("Unable to make UUID from",
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "\"00000000-0000-0000-zzzz-000000000000\"");

        assertInvalidMessage("Expected a string representation of a uuid, but got a Integer",
                "INSERT INTO %s (k, uuidval) VALUES (?, fromJson(?))", 0, "123");

        // ================ varint ================
        execute("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123123123");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("123123123123")));

        // accept strings for numbers that cannot be represented as longs
        execute("INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"1234567890123456789012345678901234567890\"");
        assertRows(execute("SELECT k, varintval FROM %s WHERE k = ?", 0), row(0, new BigInteger("1234567890123456789012345678901234567890")));

        assertInvalidMessage("Value '123123.123' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "123123.123");

        assertInvalidMessage("Value 'xyzz' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"xyzz\"");

        assertInvalidMessage("Value '' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "\"\"");

        assertInvalidMessage("Value 'true' is not a valid representation of a varint value",
                "INSERT INTO %s (k, varintval) VALUES (?, fromJson(?))", 0, "true");

        // ================ lists ================
        execute("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        execute("INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, listval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a list, but got a Integer",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        assertInvalidMessage("Invalid null element in list",
                "INSERT INTO %s (k, listval) VALUES (?, fromJson(?))", 0, "[null]");

        // frozen
        execute("INSERT INTO %s (k, frozenlistval) VALUES (?, fromJson(?))", 0, "[1, 2, 3]");
        assertRows(execute("SELECT k, frozenlistval FROM %s WHERE k = ?", 0), row(0, list(1, 2, 3)));

        // ================ sets ================
        execute("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        // duplicates are okay, just like in CQL
        execute("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        execute("INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[]");
        assertRows(execute("SELECT k, setval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a list (representing a set), but got a Integer",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Unable to make UUID from",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[\"abc\"]");

        assertInvalidMessage("Invalid null element in set",
                "INSERT INTO %s (k, setval) VALUES (?, fromJson(?))", 0, "[null]");

        // frozen
        execute("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        execute("INSERT INTO %s (k, frozensetval) VALUES (?, fromJson(?))",
                0, "[\"6bddc89a-5644-11e4-97fc-56847afe9799\", \"6bddc89a-5644-11e4-97fc-56847afe9798\"]");
        assertRows(execute("SELECT k, frozensetval FROM %s WHERE k = ?", 0),
                row(0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))))
        );

        // ================ maps ================
        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        execute("INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{}");
        assertRows(execute("SELECT k, mapval FROM %s WHERE k = ?", 0), row(0, null));

        assertInvalidMessage("Expected a map, but got a Integer",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "123");

        assertInvalidMessage("Invalid ASCII character in string literal",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"\\u1fff\\u2013\\u33B4\\u2014\": 1}");

        assertInvalidMessage("Invalid null value in map",
                "INSERT INTO %s (k, mapval) VALUES (?, fromJson(?))", 0, "{\"a\": null}");

        // frozen
        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": 2}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, fromJson(?))", 0, "{\"b\": 2, \"a\": 1}");
        assertRows(execute("SELECT k, frozenmapval FROM %s WHERE k = ?", 0), row(0, map("a", 1, "b", 2)));

        // ================ tuples ================
        execute("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
            row(0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        execute("INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))", 0, "[1, null, \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");
        assertRows(execute("SELECT k, tupleval FROM %s WHERE k = ?", 0),
                row(0, tuple(1, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")))
        );

        assertInvalidMessage("Tuple contains extra items",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\", 1, 2, 3]");

        assertInvalidMessage("Tuple is missing items",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[1, \"foobar\"]");

        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s (k, tupleval) VALUES (?, fromJson(?))",
                0, "[\"not an int\", \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]");

        // ================ UDTs ================
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // ================ duration ================
        execute("INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"53us\"");
        assertRows(execute("SELECT k, durationval FROM %s WHERE k = ?", 0), row(0, Duration.newInstance(0, 0, 53000L)));

        execute("INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"P2W\"");
        assertRows(execute("SELECT k, durationval FROM %s WHERE k = ?", 0), row(0, Duration.newInstance(0, 14, 0)));

        assertInvalidMessage("Unable to convert 'xyz' to a duration",
                             "INSERT INTO %s (k, durationval) VALUES (?, fromJson(?))", 0, "\"xyz\"");

        // order of fields shouldn't matter
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"a\": 1, \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test nulls
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": null, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"foo\", \"bar\"]}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, null, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("bar", "foo"))
        );

        // test missing fields
        execute("INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\"}");
        assertRows(execute("SELECT k, udtval.a, udtval.b, udtval.c FROM %s WHERE k = ?", 0),
                row(0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), null)
        );

        assertInvalidMessage("Unknown field", "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"xxx\": 1}");
        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s (k, udtval) VALUES (?, fromJson(?))", 0, "{\"a\": \"foobar\"}");
    }

    @Test
    public void testToJsonFct() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b uuid, c set<text>)");
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
                "smallintval smallint, " +
                "textval text, " +
                "timeval time, " +
                "timestampval timestamp, " +
                "timeuuidval timeuuid, " +
                "tinyintval tinyint, " +
                "uuidval uuid," +
                "varcharval varchar, " +
                "varintval varint, " +
                "listval list<int>, " +
                "frozenlistval frozen<list<int>>, " +
                "setval set<uuid>, " +
                "frozensetval frozen<set<uuid>>, " +
                "mapval map<ascii, int>, " +
                "frozenmapval frozen<map<ascii, int>>, " +
                "tupleval frozen<tuple<int, ascii, uuid>>," +
                "udtval frozen<" + typeName + ">," +
                "durationval duration)");

        // toJson() can only be used in selections
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "INSERT INTO %s (k, asciival) VALUES (?, toJson(?))", 0, 0);
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "UPDATE %s SET asciival = toJson(?) WHERE k = ?", 0, 0);
        assertInvalidMessage("toJson() may only be used within the selection clause",
                "DELETE FROM %s WHERE k = fromJson(toJson(?))", 0);

        // ================ ascii ================
        execute("INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "ascii text");
        assertRows(execute("SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), row(0, "\"ascii text\""));

        execute("INSERT INTO %s (k, asciival) VALUES (?, ?)", 0, "");
        assertRows(execute("SELECT k, toJson(asciival) FROM %s WHERE k = ?", 0), row(0, "\"\""));

        // ================ bigint ================
        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 123123123123L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "123123123123"));

        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, 0L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, bigintval) VALUES (?, ?)", 0, -123123123123L);
        assertRows(execute("SELECT k, toJson(bigintval) FROM %s WHERE k = ?", 0), row(0, "-123123123123"));

        // ================ blob ================
        execute("INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, ByteBufferUtil.bytes(1));
        assertRows(execute("SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), row(0, "\"0x00000001\""));

        execute("INSERT INTO %s (k, blobval) VALUES (?, ?)", 0, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        assertRows(execute("SELECT k, toJson(blobval) FROM %s WHERE k = ?", 0), row(0, "\"0x\""));

        // ================ boolean ================
        execute("INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, true);
        assertRows(execute("SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), row(0, "true"));

        execute("INSERT INTO %s (k, booleanval) VALUES (?, ?)", 0, false);
        assertRows(execute("SELECT k, toJson(booleanval) FROM %s WHERE k = ?", 0), row(0, "false"));

        // ================ date ================
        execute("INSERT INTO %s (k, dateval) VALUES (?, ?)", 0, SimpleDateSerializer.dateStringToDays("1987-03-23"));
        assertRows(execute("SELECT k, toJson(dateval) FROM %s WHERE k = ?", 0), row(0, "\"1987-03-23\""));

        // ================ decimal ================
        execute("INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, new BigDecimal("123123.123123"));
        assertRows(execute("SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), row(0, "123123.123123"));

        execute("INSERT INTO %s (k, decimalval) VALUES (?, ?)", 0, new BigDecimal("-1.23E-12"));
        assertRows(execute("SELECT k, toJson(decimalval) FROM %s WHERE k = ?", 0), row(0, "-1.23E-12"));

        // ================ double ================
        execute("INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123.123123d);
        assertRows(execute("SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), row(0, "123123.123123"));

        execute("INSERT INTO %s (k, doubleval) VALUES (?, ?)", 0, 123123d);
        assertRows(execute("SELECT k, toJson(doubleval) FROM %s WHERE k = ?", 0), row(0, "123123.0"));

        // ================ float ================
        execute("INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123.123f);
        assertRows(execute("SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), row(0, "123.123"));

        execute("INSERT INTO %s (k, floatval) VALUES (?, ?)", 0, 123123f);
        assertRows(execute("SELECT k, toJson(floatval) FROM %s WHERE k = ?", 0), row(0, "123123.0"));

        // ================ inet ================
        execute("INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, InetAddress.getByName("127.0.0.1"));
        assertRows(execute("SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), row(0, "\"127.0.0.1\""));

        execute("INSERT INTO %s (k, inetval) VALUES (?, ?)", 0, InetAddress.getByName("::1"));
        assertRows(execute("SELECT k, toJson(inetval) FROM %s WHERE k = ?", 0), row(0, "\"0:0:0:0:0:0:0:1\""));

        // ================ int ================
        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 123123);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "123123"));

        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, 0);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, intval) VALUES (?, ?)", 0, -123123);
        assertRows(execute("SELECT k, toJson(intval) FROM %s WHERE k = ?", 0), row(0, "-123123"));

        // ================ smallint ================
        execute("INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, (short) 32767);
        assertRows(execute("SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), row(0, "32767"));

        execute("INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, (short) 0);
        assertRows(execute("SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, smallintval) VALUES (?, ?)", 0, (short) -32768);
        assertRows(execute("SELECT k, toJson(smallintval) FROM %s WHERE k = ?", 0), row(0, "-32768"));

        // ================ tinyint ================
        execute("INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, (byte) 127);
        assertRows(execute("SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), row(0, "127"));

        execute("INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, (byte) 0);
        assertRows(execute("SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), row(0, "0"));

        execute("INSERT INTO %s (k, tinyintval) VALUES (?, ?)", 0, (byte) -128);
        assertRows(execute("SELECT k, toJson(tinyintval) FROM %s WHERE k = ?", 0), row(0, "-128"));

        // ================ text (varchar) ================
        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"\""));

        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "abcd");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"abcd\""));

        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "\u8422");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"\u8422\""));

        execute("INSERT INTO %s (k, textval) VALUES (?, ?)", 0, "\u0000");
        assertRows(execute("SELECT k, toJson(textval) FROM %s WHERE k = ?", 0), row(0, "\"\\u0000\""));

        // ================ time ================
        execute("INSERT INTO %s (k, timeval) VALUES (?, ?)", 0, 123L);
        assertRows(execute("SELECT k, toJson(timeval) FROM %s WHERE k = ?", 0), row(0, "\"00:00:00.000000123\""));

        execute("INSERT INTO %s (k, timeval) VALUES (?, fromJson(?))", 0, "\"07:35:07.000111222\"");
        assertRows(execute("SELECT k, toJson(timeval) FROM %s WHERE k = ?", 0), row(0, "\"07:35:07.000111222\""));

        // ================ timestamp ================
        SimpleDateFormat sdf = new SimpleDateFormat("y-M-d");
        sdf.setTimeZone(TimeZone.getTimeZone("UDT"));
        execute("INSERT INTO %s (k, timestampval) VALUES (?, ?)", 0, sdf.parse("2014-01-01"));
        assertRows(execute("SELECT k, toJson(timestampval) FROM %s WHERE k = ?", 0), row(0, "\"2014-01-01 00:00:00.000Z\""));

        // ================ timeuuid ================
        execute("INSERT INTO %s (k, timeuuidval) VALUES (?, ?)", 0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(timeuuidval) FROM %s WHERE k = ?", 0), row(0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""));

         // ================ uuidval ================
        execute("INSERT INTO %s (k, uuidval) VALUES (?, ?)", 0, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(uuidval) FROM %s WHERE k = ?", 0), row(0, "\"6bddc89a-5644-11e4-97fc-56847afe9799\""));

        // ================ varint ================
        execute("INSERT INTO %s (k, varintval) VALUES (?, ?)", 0, new BigInteger("123123123123123123123"));
        assertRows(execute("SELECT k, toJson(varintval) FROM %s WHERE k = ?", 0), row(0, "123123123123123123123"));

        // ================ lists ================
        execute("INSERT INTO %s (k, listval) VALUES (?, ?)", 0, list(1, 2, 3));
        assertRows(execute("SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), row(0, "[1, 2, 3]"));

        execute("INSERT INTO %s (k, listval) VALUES (?, ?)", 0, list());
        assertRows(execute("SELECT k, toJson(listval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozenlistval) VALUES (?, ?)", 0, list(1, 2, 3));
        assertRows(execute("SELECT k, toJson(frozenlistval) FROM %s WHERE k = ?", 0), row(0, "[1, 2, 3]"));

        // ================ sets ================
        execute("INSERT INTO %s (k, setval) VALUES (?, ?)",
                0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, toJson(setval) FROM %s WHERE k = ?", 0),
                row(0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        execute("INSERT INTO %s (k, setval) VALUES (?, ?)", 0, set());
        assertRows(execute("SELECT k, toJson(setval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozensetval) VALUES (?, ?)",
                0, set(UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9798"), (UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"))));
        assertRows(execute("SELECT k, toJson(frozensetval) FROM %s WHERE k = ?", 0),
                row(0, "[\"6bddc89a-5644-11e4-97fc-56847afe9798\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        // ================ maps ================
        execute("INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, map("a", 1, "b", 2));
        assertRows(execute("SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), row(0, "{\"a\": 1, \"b\": 2}"));

        execute("INSERT INTO %s (k, mapval) VALUES (?, ?)", 0, map());
        assertRows(execute("SELECT k, toJson(mapval) FROM %s WHERE k = ?", 0), row(0, "null"));

        // frozen
        execute("INSERT INTO %s (k, frozenmapval) VALUES (?, ?)", 0, map("a", 1, "b", 2));
        assertRows(execute("SELECT k, toJson(frozenmapval) FROM %s WHERE k = ?", 0), row(0, "{\"a\": 1, \"b\": 2}"));

        // ================ tuples ================
        execute("INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, tuple(1, "foobar", UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799")));
        assertRows(execute("SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
            row(0, "[1, \"foobar\", \"6bddc89a-5644-11e4-97fc-56847afe9799\"]")
        );

        execute("INSERT INTO %s (k, tupleval) VALUES (?, ?)", 0, tuple(1, "foobar", null));
        assertRows(execute("SELECT k, toJson(tupleval) FROM %s WHERE k = ?", 0),
                row(0, "[1, \"foobar\", null]")
        );

        // ================ UDTs ================
        execute("INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?, c: ?})", 0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"), set("foo", "bar"));
        assertRows(execute("SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                row(0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": [\"bar\", \"foo\"]}")
        );

        execute("INSERT INTO %s (k, udtval) VALUES (?, {a: ?, b: ?})", 0, 1, UUID.fromString("6bddc89a-5644-11e4-97fc-56847afe9799"));
        assertRows(execute("SELECT k, toJson(udtval) FROM %s WHERE k = ?", 0),
                row(0, "{\"a\": 1, \"b\": \"6bddc89a-5644-11e4-97fc-56847afe9799\", \"c\": null}")
        );

        // ================ duration ================
        execute("INSERT INTO %s (k, durationval) VALUES (?, 12µs)", 0);
        assertRows(execute("SELECT k, toJson(durationval) FROM %s WHERE k = ?", 0), row(0, "\"12us\""));

        execute("INSERT INTO %s (k, durationval) VALUES (?, P1Y1M2DT10H5M)", 0);
        assertRows(execute("SELECT k, toJson(durationval) FROM %s WHERE k = ?", 0), row(0, "\"1y1mo2d10h5m\""));
    }

    @Test
    public void testJsonWithGroupBy() throws Throwable
    {
        // tests SELECT JSON statements
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 0, 1)");

        assertRows(execute("SELECT JSON * FROM %s GROUP BY k"),
                   row("{\"k\": 0, \"c\": 0, \"v\": 0}"),
                   row("{\"k\": 1, \"c\": 0, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON k, c, v FROM %s GROUP BY k"),
                   row("{\"k\": 0, \"c\": 0, \"v\": 0}"),
                   row("{\"k\": 1, \"c\": 0, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON count(*) FROM %s GROUP BY k"),
                row("{\"count\": 2}"),
                row("{\"count\": 1}")
        );
    }

    @Test
    public void testSelectJsonSyntax() throws Throwable
    {
        // tests SELECT JSON statements
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");

        assertRows(execute("SELECT JSON * FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON k, v FROM %s"),
                row("{\"k\": 0, \"v\": 0}"),
                row("{\"k\": 1, \"v\": 1}")
        );

        assertRows(execute("SELECT JSON v, k FROM %s"),
                row("{\"v\": 0, \"k\": 0}"),
                row("{\"v\": 1, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON v as foo, k as bar FROM %s"),
                row("{\"foo\": 0, \"bar\": 0}"),
                row("{\"foo\": 1, \"bar\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v), k FROM %s"),
                row("{\"ttl(v)\": null, \"k\": 0}"),
                row("{\"ttl(v)\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON ttl(v) as foo, k FROM %s"),
                row("{\"foo\": null, \"k\": 0}"),
                row("{\"foo\": null, \"k\": 1}")
        );

        assertRows(execute("SELECT JSON count(*) FROM %s"),
                row("{\"count\": 2}")
        );

        assertRows(execute("SELECT JSON count(*) as foo FROM %s"),
                row("{\"foo\": 2}")
        );

        assertRows(execute("SELECT JSON toJson(blobAsInt(intAsBlob(v))) FROM %s LIMIT 1"),
                row("{\"system.tojson(system.blobasint(system.intasblob(v)))\": \"0\"}")
        );
    }

    @Test
    public void testInsertJsonSyntax() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0)
        );

        // without specifying column names
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0)
        );

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": null}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        execute("INSERT INTO %s JSON ?", "{\"v\": 1, \"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 1)
        );

        execute("INSERT INTO %s JSON ?", "{\"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null)
        );

        if (USE_PREPARED_VALUES)
            assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s JSON ?", new Object[]{null});

        assertInvalidMessage("Got null for INSERT JSON values", "INSERT INTO %s JSON ?", "null");
        assertInvalidMessage("Could not decode JSON string as a map", "INSERT INTO %s JSON ?", "\"notamap\"");
        assertInvalidMessage("Could not decode JSON string as a map", "INSERT INTO %s JSON ?", "12.34");
        assertInvalidMessage("JSON values map contains unrecognized column",
                "INSERT INTO %s JSON ?",
                "{\"k\": 0, \"v\": 0, \"zzz\": 0}");

        assertInvalidMessage("Unable to make int from",
                "INSERT INTO %s JSON ?",
                "{\"k\": 0, \"v\": \"notanint\"}");
    }

    @Test
    public void testInsertJsonSyntaxDefaultUnset() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v1\": 0, \"v2\": 0}");

        // leave v1 unset
        execute("INSERT INTO %s JSON ? DEFAULT UNSET", "{\"k\": 0, \"v2\": 2}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, 0, 2)
        );

        // explicit specification DEFAULT NULL
        execute("INSERT INTO %s JSON ? DEFAULT NULL", "{\"k\": 0, \"v2\": 2}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null, 2)
        );

        // implicitly setting v2 to null
        execute("INSERT INTO %s JSON ? DEFAULT NULL", "{\"k\": 0}");
        assertRows(execute("SELECT * FROM %s"),
                row(0, null, null)
        );

        // mix setting null explicitly with default unset:
        // set values for all fields
        execute("INSERT INTO %s JSON ?", "{\"k\": 1, \"v1\": 1, \"v2\": 1}");
        // explicitly set v1 to null while leaving v2 unset which retains its value
        execute("INSERT INTO %s JSON ? DEFAULT UNSET", "{\"k\": 1, \"v1\": null}");
        assertRows(execute("SELECT * FROM %s WHERE k=1"),
                row(1, null, 1)
        );

        // test string literal instead of bind marker
        execute("INSERT INTO %s JSON '{\"k\": 2, \"v1\": 2, \"v2\": 2}'");
        // explicitly set v1 to null while leaving v2 unset which retains its value
        execute("INSERT INTO %s JSON '{\"k\": 2, \"v1\": null}' DEFAULT UNSET");
        assertRows(execute("SELECT * FROM %s WHERE k=2"),
                row(2, null, 2)
        );
        execute("INSERT INTO %s JSON '{\"k\": 2}' DEFAULT NULL");
        assertRows(execute("SELECT * FROM %s WHERE k=2"),
                row(2, null, null)
        );
    }

    @Test
    public void testCaseSensitivity() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, \"Foo\" int)");
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"\\\"Foo\\\"\": 0}");
        execute("INSERT INTO %s JSON ?", "{\"K\": 0, \"\\\"Foo\\\"\": 0}");
        execute("INSERT INTO %s JSON ?", "{\"\\\"k\\\"\": 0, \"\\\"Foo\\\"\": 0}");

        // results should preserve and quote case-sensitive identifiers
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"\\\"Foo\\\"\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as foo FROM %s"), row("{\"k\": 0, \"foo\": 0}"));
        assertRows(execute("SELECT JSON k, \"Foo\" as \"Bar\" FROM %s"), row("{\"k\": 0, \"\\\"Bar\\\"\": 0}"));

        assertInvalid("INSERT INTO %s JSON ?", "{\"k\": 0, \"foo\": 0}");
        assertInvalid("INSERT INTO %s JSON ?", "{\"k\": 0, \"\\\"foo\\\"\": 0}");

        // user-defined types also need to handle case-sensitivity
        String typeName = createType("CREATE TYPE %s (a int, \"Foo\" int)");
        createTable("CREATE TABLE %s (k int primary key, v frozen<" + typeName + ">)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}");
        assertRows(execute("SELECT JSON k, v FROM %s"), row("{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}"));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"v\": {\"A\": 0, \"\\\"Foo\\\"\": 0}}");
        assertRows(execute("SELECT JSON k, v FROM %s"), row("{\"k\": 0, \"v\": {\"a\": 0, \"\\\"Foo\\\"\": 0}}"));
    }

    @Test
    public void testInsertJsonSyntaxWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "m map<text, boolean>, " +
                "mf frozen<map<text, boolean>>, " +
                "s set<int>, " +
                "sf frozen<set<int>>, " +
                "l list<int>, " +
                "lf frozen<list<int>>)");

        // map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"m\": {\"a\": true, \"b\": false}}");
        assertRows(execute("SELECT k, m FROM %s"), row(0, map("a", true, "b", false)));

        // frozen map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"mf\": {\"a\": true, \"b\": false}}");
        assertRows(execute("SELECT k, mf FROM %s"), row(0, map("a", true, "b", false)));

        // set
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"s\": [3, 1, 2]}");
        assertRows(execute("SELECT k, s FROM %s"), row(0, set(1, 2, 3)));

        // frozen set
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"sf\": [3, 1, 2]}");
        assertRows(execute("SELECT k, sf FROM %s"), row(0, set(1, 2, 3)));

        // list
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"l\": [1, 2, 3]}");
        assertRows(execute("SELECT k, l FROM %s"), row(0, list(1, 2, 3)));

        // frozen list
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"lf\": [1, 2, 3]}");
        assertRows(execute("SELECT k, lf FROM %s"), row(0, list(1, 2, 3)));
    }

    @Test
    public void testInsertJsonSyntaxWithNonNativeMapKeys() throws Throwable
    {
        // JSON doesn't allow non-string keys, so we accept string representations of any type as map keys and
        // return maps with string keys when necessary.

        String typeName = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "intmap map<int, boolean>, " +
                "bigintmap map<bigint, boolean>, " +
                "varintmap map<varint, boolean>, " +
                "smallintmap map<smallint, boolean>, " +
                "tinyintmap map<tinyint, boolean>, " +
                "booleanmap map<boolean, boolean>, " +
                "floatmap map<float, boolean>, " +
                "doublemap map<double, boolean>, " +
                "decimalmap map<decimal, boolean>, " +
                "tuplemap map<frozen<tuple<int, text>>, boolean>, " +
                "udtmap map<frozen<" + typeName + ">, boolean>, " +
                "setmap map<frozen<set<int>>, boolean>, " +
                "listmap map<frozen<list<int>>, boolean>, " +
                "textsetmap map<frozen<set<text>>, boolean>, " +
                "nestedsetmap map<frozen<map<set<text>, text>>, boolean>, " +
                "frozensetmap frozen<map<set<int>, boolean>>)");

        // int keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"intmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, intmap FROM %s"), row("{\"k\": 0, \"intmap\": {\"0\": true, \"1\": false}}"));

        // bigint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"bigintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, bigintmap FROM %s"), row("{\"k\": 0, \"bigintmap\": {\"0\": true, \"1\": false}}"));

        // varint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"varintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, varintmap FROM %s"), row("{\"k\": 0, \"varintmap\": {\"0\": true, \"1\": false}}"));

        // smallint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"smallintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, smallintmap FROM %s"), row("{\"k\": 0, \"smallintmap\": {\"0\": true, \"1\": false}}"));

        // tinyint keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"tinyintmap\": {\"0\": true, \"1\": false}}");
        assertRows(execute("SELECT JSON k, tinyintmap FROM %s"), row("{\"k\": 0, \"tinyintmap\": {\"0\": true, \"1\": false}}"));

        // boolean keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"booleanmap\": {\"true\": true, \"false\": false}}");
        assertRows(execute("SELECT JSON k, booleanmap FROM %s"), row("{\"k\": 0, \"booleanmap\": {\"false\": false, \"true\": true}}"));

        // float keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"floatmap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, floatmap FROM %s"), row("{\"k\": 0, \"floatmap\": {\"1.23\": true, \"4.56\": false}}"));

        // double keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"doublemap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, doublemap FROM %s"), row("{\"k\": 0, \"doublemap\": {\"1.23\": true, \"4.56\": false}}"));

        // decimal keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"decimalmap\": {\"1.23\": true, \"4.56\": false}}");
        assertRows(execute("SELECT JSON k, decimalmap FROM %s"), row("{\"k\": 0, \"decimalmap\": {\"1.23\": true, \"4.56\": false}}"));

        // tuple<int, text> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"tuplemap\": {\"[0, \\\"a\\\"]\": true, \"[1, \\\"b\\\"]\": false}}");
        assertRows(execute("SELECT JSON k, tuplemap FROM %s"), row("{\"k\": 0, \"tuplemap\": {\"[0, \\\"a\\\"]\": true, \"[1, \\\"b\\\"]\": false}}"));

        // UDT keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"udtmap\": {\"{\\\"a\\\": 0}\": true, \"{\\\"a\\\": 1}\": false}}");
        assertRows(execute("SELECT JSON k, udtmap FROM %s"), row("{\"k\": 0, \"udtmap\": {\"{\\\"a\\\": 0}\": true, \"{\\\"a\\\": 1}\": false}}"));

        // set<int> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"setmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, setmap FROM %s"), row("{\"k\": 0, \"setmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));

        // list<int> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"listmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, listmap FROM %s"), row("{\"k\": 0, \"listmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));

        // set<text> keys
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"textsetmap\": {\"[\\\"0\\\", \\\"1\\\"]\": true, \"[\\\"3\\\", \\\"4\\\"]\": false}}");
        assertRows(execute("SELECT JSON k, textsetmap FROM %s"), row("{\"k\": 0, \"textsetmap\": {\"[\\\"0\\\", \\\"1\\\"]\": true, \"[\\\"3\\\", \\\"4\\\"]\": false}}"));

        // map<set<text>, text> keys
        String innerKey1 = "[\"0\", \"1\"]";
        String fullKey1 = String.format("{\"%s\": \"%s\"}", Json.quoteAsJsonString(innerKey1), "a");
        String stringKey1 = Json.quoteAsJsonString(fullKey1);
        String innerKey2 = "[\"3\", \"4\"]";
        String fullKey2 = String.format("{\"%s\": \"%s\"}", Json.quoteAsJsonString(innerKey2), "b");
        String stringKey2 = Json.quoteAsJsonString(fullKey2);
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"nestedsetmap\": {\"" + stringKey1 + "\": true, \"" + stringKey2 + "\": false}}");
        assertRows(execute("SELECT JSON k, nestedsetmap FROM %s"), row("{\"k\": 0, \"nestedsetmap\": {\"" + stringKey1 + "\": true, \"" + stringKey2 + "\": false}}"));

        // set<int> keys in a frozen map
        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"frozensetmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}");
        assertRows(execute("SELECT JSON k, frozensetmap FROM %s"), row("{\"k\": 0, \"frozensetmap\": {\"[0, 1, 2]\": true, \"[3, 4, 5]\": false}}"));
    }

    @Test
    public void testInsertJsonSyntaxWithTuplesAndUDTs() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int, b frozen<set<int>>, c tuple<int, int>)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "a frozen<" + typeName + ">, " +
                "b tuple<int, boolean>)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": [1, 2, 3], \"c\": [0, 1]}, \"b\": [0, true]}");
        assertRows(execute("SELECT k, a.a, a.b, a.c, b FROM %s"), row(0, 0, set(1, 2, 3), tuple(0, 1), tuple(0, true)));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": [1, 2, 3], \"c\": null}, \"b\": null}");
        assertRows(execute("SELECT k, a.a, a.b, a.c, b FROM %s"), row(0, 0, set(1, 2, 3), null, null));
    }

    // done for CASSANDRA-11146
    @Test
    public void testAlterUDT() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (" +
                "k int PRIMARY KEY, " +
                "a frozen<" + typeName + ">)");

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0}}");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0}}"));

        schemaChange("ALTER TYPE " + KEYSPACE + "." + typeName + " ADD b boolean");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0, \"b\": null}}"));

        execute("INSERT INTO %s JSON ?", "{\"k\": 0, \"a\": {\"a\": 0, \"b\": true}}");
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"k\": 0, \"a\": {\"a\": 0, \"b\": true}}"));
    }

    // done for CASSANDRA-11048
    @Test
    public void testJsonThreadSafety() throws Throwable
    {
        int numThreads = 10;
        final int numRows = 5000;

        createTable("CREATE TABLE %s (" +
                "k text PRIMARY KEY, " +
                "v text)");

        for (int i = 0; i < numRows; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", "" + i, "" + i);

        long seed = System.nanoTime();
        System.out.println("Seed " + seed);
        final Random rand = new Random(seed);

        final Runnable worker = new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    for (int i = 0; i < numRows; i++)
                    {
                        String key = "" + rand.nextInt(numRows);
                        assertRows(execute("SELECT JSON * FROM %s WHERE k = ?", key),
                                row(String.format("{\"k\": \"%s\", \"v\": \"%s\"}", key, key)));
                    }
                }
                catch (Throwable exc)
                {
                    exc.printStackTrace();
                    fail(exc.getMessage());
                }
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++)
            futures.add(executor.submit(worker));

        for (Future future : futures)
            future.get(30, TimeUnit.SECONDS);

        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }

   @Test
    public void emptyStringJsonSerializationTest() throws Throwable
    {
        createTable("create table %s(id INT, name TEXT, PRIMARY KEY(id));");
        execute("insert into %s(id, name) VALUES (0, 'Foo');");
        execute("insert into %s(id, name) VALUES (2, '');");
        execute("insert into %s(id, name) VALUES (3, null);");

        assertRows(execute("SELECT JSON * FROM %s"),
                   row("{\"id\": 0, \"name\": \"Foo\"}"),
                   row("{\"id\": 2, \"name\": \"\"}"),
                   row("{\"id\": 3, \"name\": null}"));
    }

    // CASSANDRA-14286
    @Test
    public void testJsonOrdering() throws Throwable
    {
        createTable("CREATE TABLE %s( PRIMARY KEY (a, b), a INT, b INT);");
        execute("INSERT INTO %s(a, b) VALUES (20, 30);");
        execute("INSERT INTO %s(a, b) VALUES (100, 200);");

        assertRows(execute("SELECT JSON a, b FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"b\": 30}"),
                   row("{\"a\": 100, \"b\": 200}"));

        assertRows(execute("SELECT JSON a, b FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"b\": 200}"),
                   row("{\"a\": 20, \"b\": 30}"));

        assertRows(execute("SELECT JSON a FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100}"),
                   row("{\"a\": 20}"));

        // Check ordering with alias 
        assertRows(execute("SELECT JSON a, b as c FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"c\": 30}"),
                   row("{\"a\": 100, \"c\": 200}"));

        assertRows(execute("SELECT JSON a, b as c FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"c\": 200}"),
                   row("{\"a\": 20, \"c\": 30}"));

        // Check ordering with CAST 
        assertRows(execute("SELECT JSON a, CAST(b AS FLOAT) FROM %s WHERE a IN (20, 100) ORDER BY b"),
                   row("{\"a\": 20, \"cast(b as float)\": 30.0}"),
                   row("{\"a\": 100, \"cast(b as float)\": 200.0}"));

        assertRows(execute("SELECT JSON a, CAST(b AS FLOAT) FROM %s WHERE a IN (20, 100) ORDER BY b DESC"),
                   row("{\"a\": 100, \"cast(b as float)\": 200.0}"),
                   row("{\"a\": 20, \"cast(b as float)\": 30.0}"));
    }

    @Test
     public void testInsertAndSelectJsonSyntaxWithEmptyAndNullValues() throws Throwable
     {
         createTable("create table %s(id INT, name TEXT, name_asc ASCII, bytes BLOB, PRIMARY KEY(id));");

         // Test with empty values

         execute("INSERT INTO %s JSON ?", "{\"id\": 0, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}");
         assertRows(execute("SELECT * FROM %s WHERE id=0"), row(0, ByteBufferUtil.EMPTY_BYTE_BUFFER, "", ""));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 0"),
                    row("{\"id\": 0, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}"));

         execute("INSERT INTO %s(id, name, name_asc, bytes) VALUES (1, ?, ?, ?);", "", "", ByteBufferUtil.EMPTY_BYTE_BUFFER);
         assertRows(execute("SELECT * FROM %s WHERE id=1"), row(1, ByteBufferUtil.EMPTY_BYTE_BUFFER, "", ""));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 1"),
                    row("{\"id\": 1, \"bytes\": \"0x\", \"name\": \"\", \"name_asc\": \"\"}"));

         // Test with null values

         execute("INSERT INTO %s JSON ?", "{\"id\": 2, \"bytes\": null, \"name\": null, \"name_asc\": null}");
         assertRows(execute("SELECT * FROM %s WHERE id=2"), row(2, null, null, null));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 2"),
                    row("{\"id\": 2, \"bytes\": null, \"name\": null, \"name_asc\": null}"));

         execute("INSERT INTO %s(id, name, name_asc, bytes) VALUES (3, ?, ?, ?);", null, null, null);
         assertRows(execute("SELECT * FROM %s WHERE id=3"), row(3, null, null, null));
         assertRows(execute("SELECT JSON * FROM %s WHERE id = 3"),
                 row("{\"id\": 3, \"bytes\": null, \"name\": null, \"name_asc\": null}"));
     }

    @Test
    public void testJsonWithNaNAndInfinity() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, f1 float, f2 float, f3 float, d1 double, d2 double, d3 double)");
        execute("INSERT INTO %s (pk, f1, f2, f3, d1, d2, d3) VALUES (?, ?, ?, ?, ?, ?, ?)",
                1, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        // JSON does not support NaN, Infinity and -Infinity values. Most of the parser convert them into null.
        assertRows(execute("SELECT JSON * FROM %s"), row("{\"pk\": 1, \"d1\": null, \"d2\": null, \"d3\": null, \"f1\": null, \"f2\": null, \"f3\": null}"));
    }

    @Test
    public void testDurationJsonRoundtrip() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, d duration)");
        execute("INSERT INTO %s (pk, d) VALUES (1, 6h40m)");
        UntypedResultSet res = execute("SELECT JSON * FROM %s WHERE pk = 1");
        UntypedResultSet.Row r = res.one();
        String json = r.getString("[json]");
        execute("DELETE FROM %s WHERE pk = 1");
        execute("INSERT INTO %s JSON '"+json+"'");
        res = execute("SELECT JSON * FROM %s WHERE pk = 1");
        assertEquals(json, res.one().getString("[json]"));

    }
}
