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

package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

public class FunctionFactoryTest extends CQLTester
{
    /**
     * A function that just returns its only argument without any changes.
     * Calls to this function will try to infer the type of its argument, if missing, from the function's receiver.
     */
    private static final FunctionFactory IDENTITY = new FunctionFactory("identity", FunctionParameter.anyType(true))
    {
        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            return new NativeScalarFunction(name.name, argTypes.get(0), argTypes.get(0))
            {
                @Override
                public Arguments newArguments(ProtocolVersion version)
                {
                    return FunctionArguments.newNoopInstance(version, 1);
                }

                @Override
                public ByteBuffer execute(Arguments arguments)
                {
                    return arguments.containsNulls() ? null : arguments.get(0);
                }
            };
        }
    };

    /**
     * A function that returns the string representation of its only argument.
     * Calls to this function won't try to infer the type of its argument, if missing, from the function's receiver.
     */
    private static final FunctionFactory TO_STRING = new FunctionFactory("tostring", FunctionParameter.anyType(false))
    {
        @Override
        protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
        {
            return new NativeScalarFunction(name.name, UTF8Type.instance, argTypes.get(0))
            {
                @Override
                public ByteBuffer execute(Arguments arguments)
                {
                    if (arguments.containsNulls())
                        return null;

                    Object value = arguments.get(0);
                    return UTF8Type.instance.decompose(value.toString());
                }
            };
        }
    };

    private static final UUID uuid = UUID.fromString("62c3e96f-55cd-493b-8c8e-5a18883a1698");
    private static final TimeUUID timeUUID = TimeUUID.fromString("00346642-2d2f-11ed-a261-0242ac120002");
    private static final BigInteger bigint = new BigInteger("12345678901234567890");
    private static final BigDecimal bigdecimal = new BigDecimal("1234567890.1234567890");
    private static final Date date = new Date();
    private static final Duration duration = Duration.newInstance(1, 2, 3);
    private static final InetAddress inet = new InetSocketAddress(0).getAddress();
    private static final ByteBuffer blob = ByteBufferUtil.hexToBytes("ABCDEF");

    @BeforeClass
    public static void beforeClass()
    {
        NativeFunctions.instance.add(IDENTITY);
        NativeFunctions.instance.add(TO_STRING);
    }

    @Test
    public void testInvalidNumberOfArguments() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        String msg = "Invalid number of arguments for function system.identity(any)";
        assertInvalidMessage(msg, "SELECT identity() FROM %s");
        assertInvalidMessage(msg, "SELECT identity(1, 2) FROM %s");
        assertInvalidMessage(msg, "SELECT identity('1', '2', '3') FROM %s");
    }

    @Test
    public void testUnknownFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY)");
        assertInvalidMessage("Unknown function 'unknown'", "SELECT unknown() FROM %s");
    }

    @Test
    public void testSimpleTypes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, " +
                    "tinyint tinyint, " +
                    "smallint smallint, " +
                    "int int, " +
                    "bigint bigint, " +
                    "float float, " +
                    "double double, " +
                    "varint varint, " +
                    "decimal decimal, " +
                    "text text, " +
                    "ascii ascii, " +
                    "boolean boolean, " +
                    "date date, " +
                    "timestamp timestamp, " +
                    "duration duration, " +
                    "uuid uuid, " +
                    "timeuuid timeuuid," +
                    "inet inet," +
                    "blob blob)");

        // Test with empty table
        String select = "SELECT " +
                        "identity(tinyint),  identity(smallint), identity(int), " +
                        "identity(bigint), identity(float), identity(double), " +
                        "identity(varint), identity(decimal), identity(text), " +
                        "identity(ascii), identity(boolean), identity(date), " +
                        "identity(timestamp), identity(duration), identity(uuid), " +
                        "identity(timeuuid), identity(inet), identity(blob) " +
                        "FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs,
                          "system.identity(tinyint)", "system.identity(smallint)", "system.identity(int)",
                          "system.identity(bigint)", "system.identity(float)", "system.identity(double)",
                          "system.identity(varint)", "system.identity(decimal)", "system.identity(text)",
                          "system.identity(ascii)", "system.identity(boolean)", "system.identity(date)",
                          "system.identity(timestamp)", "system.identity(duration)", "system.identity(uuid)",
                          "system.identity(timeuuid)", "system.identity(inet)", "system.identity(blob)");
        assertEmpty(rs);

        // Test with not-empty table
        Object[] row = row((byte) 1, (short) 1, 123, 123L, 1.23f, 1.23d, bigint, bigdecimal,
                           "ábc", "abc", true, 1, date, duration, uuid, timeUUID, inet, blob);
        execute("INSERT INTO %s (k, tinyint, smallint, int, bigint, float, double, varint, decimal, " +
                "text, ascii, boolean, date, timestamp, duration, uuid, timeuuid, inet, blob) " +
                "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row);
        assertRows(execute(select), row);

        // Test with bind markers
        execute("INSERT INTO %s (k, tinyint, smallint, int, bigint, float, double, varint, decimal, " +
                "text, ascii, boolean, date, timestamp, duration, uuid, timeuuid, inet, blob) " +
                "VALUES (1, " +
                "identity(?), identity(?), identity(?), identity(?), identity(?), identity(?), " +
                "identity(?), identity(?), identity(?), identity(?), identity(?), identity(?), " +
                "identity(?), identity(?), identity(?), identity(?), identity(?), identity(?))", row);
        assertRows(execute(select), row);

        // Test literals
        testLiteral("(tinyint) 1", (byte) 1);
        testLiteral("(smallint) 1", (short) 1);
        testLiteral(123);
        testLiteral(1234567890123L);
        testLiteral(1.23);
        testLiteral(1234567.1234567D);
        testLiteral(bigint);
        testLiteral(bigdecimal);
        testLiteral("'abc'", "abc");
        testLiteral("'ábc'", "ábc");
        testLiteral(true);
        testLiteral(false);
        testLiteral("(timestamp) '1970-01-01 00:00:00.000+0000'", new Date(0));
        testLiteral("(time) '00:00:00.000000'", 0L);
        testLiteral(duration);
        testLiteral(uuid);
        testLiteral(timeUUID);
        testLiteral("(inet) '0.0.0.0'", inet);
        testLiteral("0x" + ByteBufferUtil.bytesToHex(blob), blob);
    }

    @Test
    public void testSets() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>, fs frozen<set<int>>)");

        // Test with empty table
        String select = "SELECT identity(s), identity(fs) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.identity(s)", "system.identity(fs)");
        assertEmpty(rs);

        // Test with not-empty table
        execute("INSERT INTO %s (k, s, fs) VALUES (1, {1, 2}, {1, 2})");
        execute("INSERT INTO %s (k, s, fs) VALUES (2, {1, 2, 3}, {1, 2, 3})");
        execute("INSERT INTO %s (k, s, fs) VALUES (3, {2, 1}, {2, 1})");
        assertRows(execute(select),
                   row(set(1, 2), set(1, 2)),
                   row(set(1, 2, 3), set(1, 2, 3)),
                   row(set(2, 1), set(2, 1)));

        // Test with bind markers
        Object[] row = row(set(1, 2, 3), set(4, 5, 6));
        execute("INSERT INTO %s (k, s, fs) VALUES (4, identity(?), identity(?))", row);
        assertRows(execute("SELECT s, fs FROM %s WHERE k = 4"), row);

        // Test literals
        testLiteralFails("[]");
        testLiteral("{1, 1234567890}", set(1, 1234567890));
        testLiteral(String.format("{1, %s}", bigint), set(BigInteger.ONE, bigint));
        testLiteral("{'abc'}", set("abc"));
        testLiteral("{'ábc'}", set("ábc"));
        testLiteral("{'abc', 'ábc'}", set("abc", "ábc"));
        testLiteral("{'ábc', 'abc'}", set("ábc", "abc"));
        testLiteral("{true}", set(true));
        testLiteral("{false}", set(false));
        testLiteral(String.format("{%s}", uuid), set(uuid));
        testLiteral(String.format("{%s}", timeUUID), set(timeUUID));
        testLiteral(String.format("{%s, %s}", uuid, timeUUID), set(uuid, timeUUID.asUUID()));
    }

    @Test
    public void testLists() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>, fl frozen<list<int>>)");

        // Test with empty table
        String select = "SELECT identity(l), identity(fl) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.identity(l)", "system.identity(fl)");
        assertEmpty(rs);

        // Test with not-empty table
        execute("INSERT INTO %s (k, l, fl) VALUES (1, [1, 2], [1, 2])");
        execute("INSERT INTO %s (k, l, fl) VALUES (2, [1, 2, 3], [1, 2, 3])");
        execute("INSERT INTO %s (k, l, fl) VALUES (3, [2, 1], [2, 1])");
        assertRows(execute(select),
                   row(list(1, 2), list(1, 2)),
                   row(list(1, 2, 3), list(1, 2, 3)),
                   row(list(2, 1), list(2, 1)));

        // Test with bind markers
        Object[] row = row(list(1, 2, 3), list(4, 5, 6));
        execute("INSERT INTO %s (k, l, fl) VALUES (4, identity(?), identity(?))", row);
        assertRows(execute("SELECT l, fl FROM %s WHERE k = 4"), row);

        // Test literals
        testLiteralFails("[]");
        testLiteral("[1, 1234567890]", list(1, 1234567890));
        testLiteral(String.format("[1, %s]", bigint), list(BigInteger.ONE, bigint));
        testLiteral("['abc']", list("abc"));
        testLiteral("['ábc']", list("ábc"));
        testLiteral("['abc', 'ábc']", list("abc", "ábc"));
        testLiteral("['ábc', 'abc']", list("ábc", "abc"));
        testLiteral("[true]", list(true));
        testLiteral("[false]", list(false));
        testLiteral(String.format("[%s]", uuid), list(uuid));
        testLiteral(String.format("[%s]", timeUUID), list(timeUUID));
        testLiteral(String.format("[%s, %s]", uuid, timeUUID), list(uuid, timeUUID.asUUID()));
    }

    @Test
    public void testMaps() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<int, int>, fm frozen<map<int, int>>)");

        // Test with empty table
        String select = "SELECT identity(m), identity(fm) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.identity(m)", "system.identity(fm)");
        assertEmpty(rs);

        // Test with not-empty table
        execute("INSERT INTO %s (k, m, fm) VALUES (1, {1:10, 2:20}, {1:10, 2:20})");
        execute("INSERT INTO %s (k, m, fm) VALUES (2, {1:10, 2:20, 3:30}, {1:10, 2:20, 3:30})");
        execute("INSERT INTO %s (k, m, fm) VALUES (3, {2:20, 1:10}, {2:20, 1:10})");
        assertRows(execute(select),
                   row(map(1, 10, 2, 20), map(1, 10, 2, 20)),
                   row(map(1, 10, 2, 20, 3, 30), map(1, 10, 2, 20, 3, 30)),
                   row(map(1, 10, 2, 20), map(1, 10, 2, 20)));

        // Test with bind markers
        Object[] row = row(map(1, 10, 2, 20), map(3, 30, 4, 40));
        execute("INSERT INTO %s (k, m, fm) VALUES (4, identity(?), identity(?))", row);
        assertRows(execute("SELECT m, fm FROM %s WHERE k = 4"), row);

        // Test literals
        testLiteralFails("{}");
        testLiteralFails("{1: 10, 2: 20}");
        testLiteral("(map<int, int>) {1: 10, 2: 20}", map(1, 10, 2, 20));
        testLiteral("(map<int, text>) {1: 'abc', 2: 'ábc'}", map(1, "abc", 2, "ábc"));
        testLiteral("(map<text, int>) {'abc': 1, 'ábc': 2}", map("abc", 1, "ábc", 2));
    }

    @Test
    public void testTuples() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<int, text, boolean>)");

        // Test with empty table
        String select = "SELECT identity(t) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.identity(t)");
        assertEmpty(rs);

        // Test with not-empty table
        execute("INSERT INTO %s (k, t) VALUES (1, (1, 'a', false))");
        execute("INSERT INTO %s (k, t) VALUES (2, (2, 'b', true))");
        execute("INSERT INTO %s (k, t) VALUES (3, (3, null, true))");
        assertRows(execute(select),
                   row(tuple(1, "a", false)),
                   row(tuple(2, "b", true)),
                   row(tuple(3, null, true)));

        // Test with bind markers
        Object[] row = row(tuple(4, "d", false));
        execute("INSERT INTO %s (k, t) VALUES (4, identity(?))", row);
        assertRows(execute("SELECT t FROM %s WHERE k = 4"), row);

        // Test literals
        testLiteralFails("(1)");
        testLiteral("(tuple<int>) (1)", tuple(1));
        testLiteral("(1, 'a')", tuple(1, "a"));
        testLiteral("(1, 'a', false)", tuple(1, "a", false));
    }

    @Test
    public void testUDTs() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, u frozen<" + udt + ">, fu frozen<" + udt + ">)");

        // Test with empty table
        String select = "SELECT identity(u), identity(fu) FROM %s";
        UntypedResultSet rs = execute(select);
        assertColumnNames(rs, "system.identity(u)", "system.identity(fu)");
        assertEmpty(rs);

        // Test with not-empty table
        execute("INSERT INTO %s (k, u, fu) VALUES (1, {x: 2}, null)");
        execute("INSERT INTO %s (k, u, fu) VALUES (2, {x: 4}, {x: 6})");
        execute("INSERT INTO %s (k, u, fu) VALUES (4, null, {x: 8})");
        assertRows(execute(select),
                   row(userType("x", 2), null),
                   row(userType("x", 4), userType("x", 6)),
                   row(null, userType("x", 8)));

        // Test with bind markers
        Object[] row = row(userType("x", 4), userType("x", 5));
        execute("INSERT INTO %s (k, u, fu) VALUES (4, identity(?), identity(?))", row);
        assertRows(execute("SELECT u, fu FROM %s WHERE k = 4"), row);

        // Test literals
        testLiteralFails("{}");
        testLiteralFails("{x: 10}");
        testLiteral('(' + udt + "){x: 10}", tuple(10));
    }

    @Test
    public void testNestedCalls() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int, t text)");

        // Test function that infers parameter type from receiver type
        execute("INSERT INTO %s (k, v) VALUES (1, identity(identity(2)))");
        assertRows(execute("SELECT v FROM %s WHERE k = 1"), row(2));
        execute("INSERT INTO %s (k, v) VALUES (1, identity(identity((int) ?)))", 3);
        assertRows(execute("SELECT v FROM %s WHERE k = 1"), row(3));
        assertRows(execute("SELECT identity(identity(v)) FROM %s WHERE k = 1"), row(3));

        // Test function that does not infer parameter type from receiver type
        execute("INSERT INTO %s (k, t) VALUES (1, tostring(tostring(4)))");
        assertRows(execute("SELECT t FROM %s WHERE k = 1"), row("4"));
        execute("INSERT INTO %s (k, t) VALUES (1, tostring(tostring((int) ?)))", 5);
        assertRows(execute("SELECT tostring(tostring(t)) FROM %s WHERE k = 1"), row("5"));
    }

    private void testLiteral(Object literal) throws Throwable
    {
        testLiteral(literal, literal);
    }

    private void testLiteral(Object functionArgs, Object expectedResult) throws Throwable
    {
        assertRows(execute(String.format("SELECT %s(%s) FROM %%s LIMIT 1", IDENTITY.name(), functionArgs)),
                   row(expectedResult));
    }

    private void testLiteralFails(Object functionArgs) throws Throwable
    {
        assertInvalidMessage("Cannot infer type of argument " + functionArgs,
                             String.format("SELECT %s(%s) FROM %%s", IDENTITY.name(), functionArgs));
    }
}
