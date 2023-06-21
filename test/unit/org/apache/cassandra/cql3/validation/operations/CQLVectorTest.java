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

import java.nio.ByteBuffer;

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
import org.assertj.core.api.Assertions;

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
        // For future authors, if this test starts to fail as vectors become supported in UDFs, please update this test
        // to test the integration and remove the requirement that we reject UDFs all together
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, value vector<int, 2>)");
        Assertions.assertThatThrownBy(() -> createFunction(KEYSPACE,
                                                           "",
                                                           "CREATE FUNCTION %s (x vector<int, 2>) " +
                                                           "CALLED ON NULL INPUT " +
                                                           "RETURNS vector<int, 2> " +
                                                           "LANGUAGE java " +
                                                           "AS 'return x;'"))
                  .hasRootCauseMessage("Vectors are not supported on UDFs; given vector<int, 2>");

        Assertions.assertThatThrownBy(() -> createFunction(KEYSPACE,
                                                           "",
                                                           "CREATE FUNCTION %s (x list<vector<int, 2>>) " +
                                                           "CALLED ON NULL INPUT " +
                                                           "RETURNS list<vector<int, 2>> " +
                                                           "LANGUAGE java " +
                                                           "AS 'return x;'"))
                  .hasRootCauseMessage("Vectors are not supported on UDFs; given list<vector<int, 2>>");
    }
}
