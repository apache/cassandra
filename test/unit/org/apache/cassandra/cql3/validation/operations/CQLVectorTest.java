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

package org.apache.cassandra.cql3.validation.operations;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.functions.Arguments;
import org.apache.cassandra.cql3.functions.FunctionArguments;
import org.apache.cassandra.cql3.functions.NativeFunctions;
import org.apache.cassandra.cql3.functions.NativeScalarFunction;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

public class CQLVectorTest extends CQLTester.InMemory
{
    @Test
    public void select()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");

        Vector<Integer> vector = vector(1, 2);
        Object[] row = row(vector);

        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 2]"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = ?", vector), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + 1]"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, ?]", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, (int) ?]", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk = [1, 1 + (int) ?]", 1), row);

        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 2], [1, 2])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN (?)", vector), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + 1])"), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, ?])", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, (int) ?])", 2), row);
        assertRows(execute("SELECT * FROM %s WHERE pk IN ([1, 1 + (int) ?])", 1), row);

        assertRows(execute("SELECT * FROM %s WHERE pk > [0, 0] AND pk < [1, 3] ALLOW FILTERING"), row);
        assertRows(execute("SELECT * FROM %s WHERE token(pk) = token([1, 2])"), row);

        assertRows(execute("SELECT * FROM %s"), row);
        Assertions.assertThat(execute("SELECT * FROM %s").one().getVector("pk", Int32Type.instance, 2))
                  .isEqualTo(vector);
    }

    @Test
    public void selectNonPk()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        assertRows(execute("SELECT * FROM %s WHERE value=[1, 2] ALLOW FILTERING"), row(0, list(1, 2)));
    }

    @Test
    public void insert()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");

        execute("INSERT INTO %s (pk) VALUES ([1, 2])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES (?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk) VALUES ([1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void insertNonPK()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + 1])");
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, (int) ?])", 2);
        test.run();

        execute("INSERT INTO %s (pk, value) VALUES (0, [1, 1 + (int) ?])", 1);
        test.run();
    }

    @Test
    public void invalidNumberOfDimensionsFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2, 3])");
        assertInvalidThrowMessage("Unexpected 4 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2, 3));
    }

    @Test
    public void invalidNumberOfDimensionsVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<text, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a"));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b', 'c'])");
        assertInvalidThrowMessage("Unexpected 2 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b", "c"));
    }

    @Test
    public void invalidElementTypeFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fixed-length bigint instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (bigint)1 is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(bigint) 1, (bigint) 2])");
        assertInvalidThrowMessage("Unexpected 8 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1L, Long.MAX_VALUE));

        // variable-length text instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 'a' is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b"));
    }

    @Test
    public void invalidElementTypeVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fixed-length int instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        assertInvalidThrowMessage("Unexpected 6 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));

        // variable-length varint instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (varint)1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(varint) 1, (varint) 2])");
        assertInvalidThrowMessage("String didn't validate.",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)",
                                  vector(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), BigInteger.ONE));
    }

    @Test
    public void update()
    {
        Runnable test = () -> {
            assertRows(execute("SELECT * FROM %s"), row(0, list(1, 2)));
            execute("TRUNCATE %s");
            assertRows(execute("SELECT * FROM %s"));
        };

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        execute("UPDATE %s set VALUE = [1, 2] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = ? WHERE pk = 0", vector(1, 2));
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + 1] WHERE pk = 0");
        test.run();

        execute("UPDATE %s set VALUE = [1, ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, (int) ?] WHERE pk = 0", 2);
        test.run();

        execute("UPDATE %s set VALUE = [1, 1 + (int) ?] WHERE pk = 0", 1);
        test.run();
    }

    @Test
    public void nullValues()
    {
        assertAcceptsNullValues("int"); // fixed length
        assertAcceptsNullValues("float"); // fixed length with special/optimized treatment
        assertAcceptsNullValues("text"); // variable length
    }

    private void assertAcceptsNullValues(String type)
    {
        createTable(format("CREATE TABLE %%s (k int primary key, v vector<%s, 2>)", type));

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        assertRows(execute("SELECT * FROM %s"), row(0, null));

        execute("INSERT INTO %s (k, v) VALUES (0, ?)", (List<Integer>) null);
        assertRows(execute("SELECT * FROM %s"), row(0, null));
    }

    @Test
    public void emptyValues() throws Throwable
    {
        assertRejectsEmptyValues("int"); // fixed length
        assertRejectsEmptyValues("float"); // fixed length with special/optimized treatment
        assertRejectsEmptyValues("text"); // variable length
    }

    private void assertRejectsEmptyValues(String type) throws Throwable
    {
        createTable(format("CREATE TABLE %%s (k int primary key, v vector<%s, 2>)", type));

        assertInvalidThrowMessage(format("Invalid HEX constant (0x) for \"v\" of type vector<%s, 2>", type),
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (k, v) VALUES (0, 0x)");

        assertInvalidThrowMessage("Invalid empty vector value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (k, v) VALUES (0, ?)",
                                  ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Test
    public void functions()
    {
        VectorType<Integer> type = VectorType.getInstance(Int32Type.instance, 2);
        Vector<Integer> vector = vector(1, 2);

        NativeFunctions.instance.add(new NativeScalarFunction("f", type, type)
        {
            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                return arguments.get(0);
            }

            @Override
            public Arguments newArguments(ProtocolVersion version)
            {
                return FunctionArguments.newNoopInstance(version, 1);
            }
        });

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

        assertRows(execute("SELECT f(value) FROM %s WHERE pk=0"), row(vector));
        assertRows(execute("SELECT f([1, 2]) FROM %s WHERE pk=0"), row(vector));
    }

    @Test
    public void specializedFunctions()
    {
        VectorType<Float> type = VectorType.getInstance(FloatType.instance, 2);
        Vector<Float> vector = vector(1.0f, 2.0f);

        NativeFunctions.instance.add(new NativeScalarFunction("f", type, type, type)
        {
            @Override
            public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
            {
                float[] left = arguments.get(0);
                float[] right = arguments.get(1);
                int size = Math.min(left.length, right.length);
                float[] sum = new float[size];
                for (int i = 0; i < size; i++)
                    sum[i] = left[i] + right[i];
                return type.decomposeAsFloat(sum);
            }

            @Override
            public Arguments newArguments(ProtocolVersion version)
            {
                return new FunctionArguments(version,
                                             (v, b) -> type.composeAsFloat(b),
                                             (v, b) -> type.composeAsFloat(b));
            }
        });

        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<float, 2>)");
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);
        execute("INSERT INTO %s (pk, value) VALUES (1, ?)", vector);

        Object[][] expected = { row(vector(2f, 4f)), row(vector(2f, 4f)) };
        assertRows(execute("SELECT f(value, [1.0, 2.0]) FROM %s"), expected);
        assertRows(execute("SELECT f([1.0, 2.0], value) FROM %s"), expected);
    }

    @Test
    public void token()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk vector<int, 2> primary key)");
        execute("INSERT INTO %s (pk) VALUES (?)", vector(1, 2));
        long tokenColumn = execute("SELECT token(pk) as t FROM %s").one().getLong("t");
        long tokenTerminal = execute("SELECT token([1, 2]) as t FROM %s").one().getLong("t");
        Assert.assertEquals(tokenColumn, tokenTerminal);
    }

    @Test
    public void udf() throws Throwable
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
        Vector<Integer> vector = vector(1, 2);
        execute("INSERT INTO %s (pk, value) VALUES (0, ?)", vector);

        // identitiy function
        String f = createFunction(KEYSPACE,
                                  "",
                                  "CREATE FUNCTION %s (x vector<int, 2>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 2> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");
        assertRows(execute(format("SELECT %s(value) FROM %%s", f)), row(vector));
        assertRows(execute(format("SELECT %s([2, 3]) FROM %%s", f)), row(vector(2, 3)));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // identitiy function with nested type
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x list<vector<int, 2>>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS list<vector<int, 2>> " +
                           "LANGUAGE java " +
                           "AS 'return x;'");
        assertRows(execute(format("SELECT %s([value]) FROM %%s", f)), row(list(vector)));
        assertRows(execute(format("SELECT %s([[2, 3]]) FROM %%s", f)), row(list(vector(2, 3))));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // identitiy function with elements of variable length
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x vector<text, 2>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS vector<text, 2> " +
                           "LANGUAGE java " +
                           "AS 'return x;'");
        assertRows(execute(format("SELECT %s(['abc', 'defghij']) FROM %%s", f)), row(vector("abc", "defghij")));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Vector<Integer>) null));

        // function accessing vector argument elements
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x vector<int, 2>, i int) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS int " +
                           "LANGUAGE java " +
                           "AS 'return x == null ? null : x.get(i);'");
        assertRows(execute(format("SELECT %s(value, 0), %<s(value, 1) FROM %%s", f)), row(1, 2));
        assertRows(execute(format("SELECT %s([2, 3], 0), %<s([2, 3], 1) FROM %%s", f)), row(2, 3));
        assertRows(execute(format("SELECT %s(null, 0) FROM %%s", f)), row((Integer) null));

        // function accessing vector argument dimensions
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x vector<int, 2>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS int " +
                           "LANGUAGE java " +
                           "AS 'return x == null ? 0 : x.size();'");
        assertRows(execute(format("SELECT %s(value) FROM %%s", f)), row(2));
        assertRows(execute(format("SELECT %s([2, 3]) FROM %%s", f)), row(2));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row(0));

        // build vector with elements of fixed length
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s () " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS vector<double, 3> " +
                           "LANGUAGE java " +
                           "AS 'return Arrays.asList(1.3, 2.2, 3.1);'");
        assertRows(execute(format("SELECT %s() FROM %%s", f)), row(vector(1.3, 2.2, 3.1)));

        // build vector with elements of variable length
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s () " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS vector<text, 3> " +
                           "LANGUAGE java " +
                           "AS 'return Arrays.asList(\"a\", \"bc\", \"def\");'");
        assertRows(execute(format("SELECT %s() FROM %%s", f)), row(vector("a", "bc", "def")));

        // concat vectors, just to put it all together
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (x vector<int, 2>, y vector<int, 2>) " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS vector<int, 4> " +
                           "LANGUAGE java " +
                           "AS '" +
                           "if (x == null || y == null) return null;" +
                           "List<Integer> l = new ArrayList<Integer>(x); " +
                           "l.addAll(y); " +
                           "return l;'");
        assertRows(execute(format("SELECT %s(value, [3, 4]) FROM %%s", f)), row(vector(1, 2, 3, 4)));
        assertRows(execute(format("SELECT %s([2, 3], value) FROM %%s", f)), row(vector(2, 3, 1, 2)));
        assertRows(execute(format("SELECT %s(null, null) FROM %%s", f)), row((Vector<Integer>) null));

        // Test wrong arguments on function call
        assertInvalidThrowMessage("cannot be passed as argument 0 of function " + f,
                                  InvalidRequestException.class,
                                  format("SELECT %s((int) 0, [3, 4]) FROM %%s", f));
        assertInvalidThrowMessage("cannot be passed as argument 1 of function " + f,
                                  InvalidRequestException.class,
                                  format("SELECT %s([1, 2], (int) 0) FROM %%s", f));
        assertInvalidThrowMessage("Invalid number of arguments in call to function " + f,
                                  InvalidRequestException.class,
                                  format("SELECT %s([1, 2]) FROM %%s", f));
        assertInvalidThrowMessage("Invalid number of arguments in call to function " + f,
                                  InvalidRequestException.class,
                                  format("SELECT %s([1, 2], [3, 4], [5, 6]) FROM %%s", f));
        assertInvalidThrowMessage("Unable to create a vector selector of type vector<int, 2> from 3 elements",
                                  InvalidRequestException.class,
                                  format("SELECT %s([1, 2, 3], [4, 5, 6]) FROM %%s", f));

        // Test wrong types on function creation
        assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
                                  InvalidRequestException.class,
                                  "CREATE FUNCTION %s (x vector<int, 0>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 2> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");
        assertInvalidThrowMessage("vectors may only have positive dimensions; given 0",
                                  InvalidRequestException.class,
                                  "CREATE FUNCTION %s (x vector<int, 2>) " +
                                  "CALLED ON NULL INPUT " +
                                  "RETURNS vector<int, 0> " +
                                  "LANGUAGE java " +
                                  "AS 'return x;'");

        // function reading and writing a udt vector field
        String udt = createType("CREATE TYPE %s (v vector<int,2>)");
        alterTable("ALTER TABLE %s ADD udt " + udt);
        execute("INSERT INTO %s (pk, udt) VALUES (0, ?)", userType("v", vector));
        f = createFunction(KEYSPACE,
                           "",
                           "CREATE FUNCTION %s (udt " + udt + ") " +
                           "CALLED ON NULL INPUT " +
                           "RETURNS " + udt + ' ' +
                           "LANGUAGE java " +
                           "AS '" +
                           "if (udt == null) return null;" +
                           "List<Integer> v = new ArrayList<Integer>(udt.getVector(\"v\", Integer.class));" +
                           "v.set(0, 7);" +
                           "return udt.setVector(\"v\", v);'");
        assertRows(execute(format("SELECT %s(udt) FROM %%s", f)), row(userType("v", vector(7, 2))));
        assertRows(execute(format("SELECT %s({v: [10, 20]}) FROM %%s", f)), row(userType("v", vector(7, 20))));
        assertRows(execute(format("SELECT %s(null) FROM %%s", f)), row((Object) null));

        // make sure the function referencing the UDT is dropped before dropping the UDT at cleanup
        execute("DROP FUNCTION " + f);
    }
}
