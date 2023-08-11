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

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeSupport;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.db.SchemaCQLHelper.toCqlType;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.tupleTypeGen;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.apache.cassandra.utils.Generators.filter;
import static org.quicktheories.QuickTheory.qt;

public class TupleTypeTest extends CQLTester
{
    @Test
    public void testTuplePutAndGet() throws Throwable
    {
        String[] valueTypes = {"frozen<tuple<int, text, double>>", "tuple<int, text, double>"};
        for (String valueType : valueTypes)
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, t " + valueType + ")");

            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, "foo", 3.4));
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 1, tuple(8, "bar", 0.2));
            assertAllRows(row(1, tuple(8, "bar", 0.2)),
                          row(0, tuple(3, "foo", 3.4))
            );

            // nulls
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 2, tuple(5, null, 3.4));
            assertRows(execute("SELECT * FROM %s WHERE k=?", 2),
                       row(2, tuple(5, null, 3.4))
            );

            // incomplete tuple
            execute("INSERT INTO %s (k, t) VALUES (?, ?)", 3, tuple(5, "bar"));
            assertRows(execute("SELECT * FROM %s WHERE k=?", 3),
                       row(3, tuple(5, "bar"))
            );
        }
    }

    @Test
    public void testNestedTuple() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, tuple<text, double>>>)");

        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, tuple("foo", 3.4)));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 1, tuple(8, tuple("bar", 0.2)));
        assertAllRows(
            row(1, tuple(8, tuple("bar", 0.2))),
            row(0, tuple(3, tuple("foo", 3.4)))
        );
    }

    @Test
    public void testTupleInPartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (t frozen<tuple<int, text>> PRIMARY KEY)");

        execute("INSERT INTO %s (t) VALUES (?)", tuple(3, "foo"));
        assertAllRows(row(tuple(3, "foo")));
    }

    @Test
    public void testTupleInClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, t frozen<tuple<int, text>>, PRIMARY KEY (k, t))");

        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(5, "bar"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(3, "foo"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(6, "bar"));
        execute("INSERT INTO %s (k, t) VALUES (?, ?)", 0, tuple(5, "foo"));

        assertAllRows(
            row(0, tuple(3, "foo")),
            row(0, tuple(5, "bar")),
            row(0, tuple(5, "foo")),
            row(0, tuple(6, "bar"))
        );
    }

    @Test
    public void testTupleFromString() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, t frozen<tuple<int, text>>, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, t) VALUES (0, 0, '0:0')");
        execute("INSERT INTO %s (k, c, t) VALUES (0, 1, '0:1')");
        execute("INSERT INTO %s (k, c, t) VALUES (0, 2, '1')");
        execute("INSERT INTO %s (k, c, t) VALUES (0, 3, '1:1\\:1')");
        execute("INSERT INTO %s (k, c, t) VALUES (0, 4, '@:1')");

        assertAllRows(
            row(0, 0, tuple(0, "0")),
            row(0, 1, tuple(0, "1")),
            row(0, 2, tuple(1)),
            row(0, 3, tuple(1, "1:1")),
            row(0, 4, tuple(null, "1"))
        );

        assertInvalidMessage("Invalid tuple literal: too many elements. Type frozen<tuple<int, text>> expects 2 but got 3",
                             "INSERT INTO %s(k, t) VALUES (1,'1:2:3')");
    }

    @Test
    public void testInvalidQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, text, double>>)");

        assertInvalidSyntax("INSERT INTO %s (k, t) VALUES (0, ())");

        assertInvalidMessage("Invalid tuple literal for t: too many elements. Type frozen<tuple<int, text, double>> expects 3 but got 4",
                             "INSERT INTO %s (k, t) VALUES (0, (2, 'foo', 3.1, 'bar'))");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, t frozen<tuple<int, tuple<int, text, double>>>)");
        assertInvalidMessage("Invalid remaining data after end of tuple value",
                             "INSERT INTO %s (k, t) VALUES (0, ?)",
                             tuple(1, tuple(1, "1", 1.0, 1)));

        assertInvalidMessage("Invalid tuple literal for t: component 1 is not of type frozen<tuple<int, text, double>>",
                             "INSERT INTO %s (k, t) VALUES (0, (1, (1, '1', 1.0, 1)))");
    }

    @Test
    public void testTupleWithUnsetValues() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<int, text, double>)");
        // invalid positional field substitution
        assertInvalidMessage("Invalid unset value for tuple field number 1",
                             "INSERT INTO %s (k, t) VALUES(0, (3, ?, 2.1))", unset());

        createIndex("CREATE INDEX tuple_index ON %s (t)");
        // select using unset
        assertInvalidMessage("Invalid unset value for tuple field number 0", "SELECT * FROM %s WHERE k = ? and t = (?,?,?)", unset(), unset(), unset(), unset());
    }

    /**
     * Test the syntax introduced by #4851,
     * migrated from cql_tests.py:TestCQL.tuple_notation_test()
     */
    @Test
    public void testTupleNotation() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))");
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                for (int k = 0; k < 2; k++)
                    execute("INSERT INTO %s (k, v1, v2, v3) VALUES (0, ?, ?, ?)", i, j, k);

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0"),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1),
                   row(1, 0, 0),
                   row(1, 0, 1),
                   row(1, 1, 0),
                   row(1, 1, 1));

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)"),
                   row(1, 0, 1),
                   row(1, 1, 0),
                   row(1, 1, 1));
        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) >= (1, 1)"),
                   row(1, 1, 0),
                   row(1, 1, 1));

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)"),
                   row(1, 0, 0),
                   row(1, 0, 1),
                   row(1, 1, 0));

        assertInvalid("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v3) > (1, 0)");
    }

    /**
     * Test for CASSANDRA-8062,
     * migrated from cql_tests.py:TestCQL.test_v2_protocol_IN_with_tuples()
     */
    @Test
    public void testSelectInStatementWithTuples() throws Throwable
    {   // TODO - the dtest was using v2 protocol
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, PRIMARY KEY (k, c1, c2))");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'a')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'b')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'c')");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND (c1, c2) IN ((0, 'b'), (0, 'c'))"),
                   row(0, 0, "b"),
                   row(0, 0, "c"));
    }

    @Test
    public void testInvalidInputForTuple() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, t tuple<text, text>)");
        assertInvalidMessage("Not enough bytes to read 0th component",
                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, "test");
        assertInvalidMessage("Not enough bytes to read 0th component",
                             "INSERT INTO %s (pk, t) VALUES (?, ?)", 1, Long.MAX_VALUE);
    }

    @Test
    public void testTupleModification() throws Throwable
    {
        createTable("CREATE TABLE %s(pk int PRIMARY KEY, value tuple<int, int>)");
        assertInvalidMessage("Invalid operation (value = value + (1, 1)) for tuple column value",
                             "UPDATE %s SET value += (1, 1) WHERE k=0;");
    }

    @Test
    public void testReversedTypeTuple() throws Throwable
    {
        // CASSANDRA-13717
        createTable("CREATE TABLE %s (id int, tdemo frozen<tuple<timestamp, varchar>>, primary key (id, tdemo)) with clustering order by (tdemo desc)");
        execute("INSERT INTO %s (id, tdemo) VALUES (1, ('2017-02-03 03:05+0000','Europe'))");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mmX", Locale.ENGLISH);
        assertRows(execute("SELECT tdemo FROM %s"), row(tuple( df.parse("2017-02-03 03:05+0000"), "Europe")));
    }

    @Test
    public void tuplePartitionReadWrite()
    {
        qt().withExamples(100).withShrinkCycles(0).forAll(typesAndRowsGen()).checkAssert(orFail(testcase -> {
            TupleType tupleType = testcase.type;
            createTable("CREATE TABLE %s (id " + toCqlType(tupleType) + ", value int, PRIMARY KEY(id))");
            SortedMap<ByteBuffer, Integer> map = new TreeMap<>(Comparator.comparing(currentTableMetadata().partitioner::decorateKey));
            int count = 0;
            for (ByteBuffer value : testcase.uniqueRows)
            {
                map.put(value, count);
                ByteBuffer[] tupleBuffers = tupleType.split(ByteBufferAccessor.instance, value);

                // use cast to avoid warning
                execute("INSERT INTO %s (id, value) VALUES (?, ?)", tuple((Object[]) tupleBuffers), count);

                assertRows(execute("SELECT * FROM %s WHERE id = ?", tuple((Object[]) tupleBuffers)),
                           row(tuple((Object[]) tupleBuffers), count));
                count++;
            }
            assertRows(execute("SELECT * FROM %s LIMIT 100"),
                       map.entrySet().stream().map(e -> row(e.getKey(), e.getValue())).toArray(Object[][]::new));
        }));
    }

    @Test
    public void tupleCkReadWriteAsc()
    {
        tupleCkReadWrite(Order.ASC);
    }

    @Test
    public void tupleCkReadWriteDesc()
    {
        tupleCkReadWrite(Order.DESC);
    }

    private void tupleCkReadWrite(Order order)
    {
        // for some reason this test is much slower than the partition key test: with 100 examples partition key is 6s and these tests were 20-30s
        qt().withExamples(50).withShrinkCycles(0).forAll(typesAndRowsGen()).checkAssert(orFail(testcase -> {
            TupleType tupleType = testcase.type;
            createTable("CREATE TABLE %s (pk int, ck " + toCqlType(tupleType) + ", value int, PRIMARY KEY(pk, ck))" +
                        " WITH CLUSTERING ORDER BY (ck "+order.name()+")");
            SortedMap<ByteBuffer, Integer> map = new TreeMap<>(order.apply(tupleType));
            int count = 0;
            for (ByteBuffer value : testcase.uniqueRows)
            {
                map.put(value, count);
                ByteBuffer[] tupleBuffers = tupleType.split(ByteBufferAccessor.instance, value);

                // use cast to avoid warning
                execute("INSERT INTO %s (pk, ck, value) VALUES (?, ?, ?)", 1, tuple((Object[]) tupleBuffers), count);

                assertRows(execute("SELECT * FROM %s WHERE pk = ? AND ck = ?", 1, tuple((Object[]) tupleBuffers)),
                           row(1, tuple((Object[]) tupleBuffers), count));
                count++;
            }
            UntypedResultSet results = execute("SELECT * FROM %s LIMIT 100");
            assertRows(results,
                       map.entrySet().stream().map(e -> row(1, e.getKey(), e.getValue())).toArray(Object[][]::new));
        }));
    }

    private static final class TypeAndRows
    {
        TupleType type;
        List<ByteBuffer> uniqueRows;
    }

    private static Gen<TypeAndRows> typesAndRowsGen()
    {
        return typesAndRowsGen(10);
    }

    private static Gen<TypeAndRows> typesAndRowsGen(int numRows)
    {
        Gen<AbstractType<?>> subTypeGen = AbstractTypeGenerators.builder()
                                                                .withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE)
                                                                // ordering doesn't make sense for duration
                                                                .withoutPrimitive(DurationType.instance)
                                                                // data is "normalized" causing equality matches to fail
                                                                .withoutPrimitive(DecimalType.instance)
                                                                .build();
        Gen<TupleType> typeGen = tupleTypeGen(subTypeGen, SourceDSL.integers().between(1, 10));
        Set<ByteBuffer> distinctRows = new HashSet<>(numRows); // reuse the memory
        Gen<TypeAndRows> gen = rnd -> {
            TypeAndRows c = new TypeAndRows();
            c.type = typeGen.generate(rnd);
            TypeSupport<ByteBuffer> support = getTypeSupport(c.type);
            Gen<ByteBuffer> valueGen = filter(support.valueGen, b -> b.remaining() <= Short.MAX_VALUE);
            valueGen = filter(valueGen, 20, v -> !distinctRows.contains(v));

            distinctRows.clear();
            for (int i = 0; i < numRows; i++)
            {
                try
                {
                    assert distinctRows.add(valueGen.generate(rnd)) : "unable to add distinct row";
                }
                catch (IllegalStateException e)
                {
                    // gave up trying to find values... so just try with how ever many rows we could
                    logger.warn("Unable to generate enough distinct rows; using {} rows", distinctRows.size());
                    break;
                }
            }
            c.uniqueRows = new ArrayList<>(distinctRows);
            return c;
        };
        gen = gen.describedAs(c -> c.type.asCQL3Type().toString());
        return gen;
    }

    private enum Order {
        ASC
        {
            <T> Comparator<T> apply(Comparator<T> c)
            {
                return c;
            }
        },
        DESC
        {
            <T> Comparator<T> apply(Comparator<T> c)
            {
                return c.reversed();
            }
        };

        abstract <T> Comparator<T> apply(Comparator<T> c);
    }

    private static List<Object[]> toObjects(UntypedResultSet results)
    {
        List<Object[]> rows = new ArrayList<>(results.size());
        for (UntypedResultSet.Row row : results)
            rows.add(results.metadata().stream().map(c -> c.type.compose(row.getBlob(c.name.toString()))).toArray());
        return rows;
    }
}

