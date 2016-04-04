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
package org.apache.cassandra.index.sasi.plan;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.*;

public class OperationTest extends SchemaLoader
{
    private static final String KS_NAME = "sasi";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME = "clustering_test_cf";
    private static final String STATIC_CF_NAME = "static_sasi_test_cf";

    private static ColumnFamilyStore BACKEND;
    private static ColumnFamilyStore CLUSTERING_BACKEND;
    private static ColumnFamilyStore STATIC_BACKEND;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        SchemaLoader.loadSchema();
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(KS_NAME,
                                                                     KeyspaceParams.simpleTransient(1),
                                                                     Tables.of(SchemaLoader.sasiCFMD(KS_NAME, CF_NAME),
                                                                               SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME),
                                                                               SchemaLoader.staticSASICFMD(KS_NAME, STATIC_CF_NAME))));

        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        CLUSTERING_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME);
        STATIC_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(STATIC_CF_NAME);
    }

    private QueryController controller;

    @Before
    public void beforeTest()
    {
        controller = new QueryController(BACKEND,
                                         PartitionRangeReadCommand.allDataRead(BACKEND.metadata, FBUtilities.nowInSeconds()),
                                         TimeUnit.SECONDS.toMillis(10));
    }

    @After
    public void afterTest()
    {
        controller.finish();
    }

    @Test
    public void testAnalyze() throws Exception
    {
        final ColumnDefinition firstName = getColumn(UTF8Type.instance.decompose("first_name"));
        final ColumnDefinition age = getColumn(UTF8Type.instance.decompose("age"));
        final ColumnDefinition comment = getColumn(UTF8Type.instance.decompose("comment"));

        // age != 5 AND age > 1 AND age != 6 AND age <= 10
        Map<Expression.Op, Expression> expressions = convert(Operation.analyzeGroup(controller, OperationType.AND,
                                                                                Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                                              new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                                                                              new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(6)),
                                                                                              new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10)))));

        Expression expected = new Expression("age", Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(1), false);
            upper = new Bound(Int32Type.instance.decompose(10), true);

            exclusions.add(Int32Type.instance.decompose(5));
            exclusions.add(Int32Type.instance.decompose(6));
        }};

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(expected, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age >= 7
        expressions = convert(Operation.analyzeGroup(controller, OperationType.OR,
                                                    Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                  new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(7)))));
        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression("age", Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        Assert.assertEquals(new Expression("age", Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(7), true);
                            }}, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age < 7
        expressions = convert(Operation.analyzeGroup(controller, OperationType.OR,
                                                    Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                  new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression("age", Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));
        Assert.assertEquals(new Expression("age", Int32Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        // age > 1 AND age < 7
        expressions = convert(Operation.analyzeGroup(controller, OperationType.AND,
                                                    Arrays.asList(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                                                  new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(new Expression("age", Int32Type.instance)
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(1), false);
                                    upper = new Bound(Int32Type.instance.decompose(7), false);
                            }}, expressions.get(Expression.Op.RANGE));

        // first_name = 'a' OR first_name != 'b'
        expressions = convert(Operation.analyzeGroup(controller, OperationType.OR,
                                                    Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                                  new SimpleExpression(firstName, Operator.NEQ, UTF8Type.instance.decompose("b")))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression("first_name", UTF8Type.instance)
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(UTF8Type.instance.decompose("b"), true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));
        Assert.assertEquals(new Expression("first_name", UTF8Type.instance)
                            {{
                                    operation = Op.EQ;
                                    lower = upper = new Bound(UTF8Type.instance.decompose("a"), true);
                            }}, expressions.get(Expression.Op.EQ));

        // comment = 'soft eng' and comment != 'likes do'
        ListMultimap<ColumnDefinition, Expression> e = Operation.analyzeGroup(controller, OperationType.OR,
                                                    Arrays.asList(new SimpleExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("soft eng")),
                                                                  new SimpleExpression(comment, Operator.NEQ, UTF8Type.instance.decompose("likes do"))));

        List<Expression> expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.MATCH;
                        lower = new Bound(UTF8Type.instance.decompose("soft"), true);
                        upper = lower;
                }});

                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.MATCH;
                        lower = new Bound(UTF8Type.instance.decompose("eng"), true);
                        upper = lower;
                }});

                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes"), true);
                        upper = lower;
                }});

                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("do"), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // first_name = 'j' and comment != 'likes do'
        e = Operation.analyzeGroup(controller, OperationType.OR,
                        Arrays.asList(new SimpleExpression(comment, Operator.NEQ, UTF8Type.instance.decompose("likes do")),
                                      new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("j"))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes"), true);
                        upper = lower;
                }});

                add(new Expression("comment", UTF8Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("do"), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // age != 27 first_name = 'j' and age != 25
        e = Operation.analyzeGroup(controller, OperationType.OR,
                        Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(27)),
                                      new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("j")),
                                      new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(25))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression("age", Int32Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(27), true);
                        upper = lower;
                }});

                add(new Expression("age", Int32Type.instance)
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(25), true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(age));
    }

    @Test
    public void testSatisfiedBy() throws Exception
    {
        final ColumnDefinition timestamp = getColumn(UTF8Type.instance.decompose("timestamp"));
        final ColumnDefinition age = getColumn(UTF8Type.instance.decompose("age"));

        Operation.Builder builder = new Operation.Builder(OperationType.AND, controller, new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)));
        Operation op = builder.complete();

        Unfiltered row = buildRow(buildCell(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(5), System.currentTimeMillis()));

        // and reject incorrect value
        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        // range with exclusions - age != 5 AND age > 1 AND age != 6 AND age <= 10
        builder = new Operation.Builder(OperationType.AND, controller,
                                        new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                        new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                        new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(6)),
                                        new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10)));
        op = builder.complete();

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 5, 6, 11);
        for (int i = 0; i <= 11; i++)
        {
            row = buildRow(buildCell(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(row, staticRow, false);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // now let's do something more complex - age = 5 OR age = 6
        builder = new Operation.Builder(OperationType.OR, controller,
                                        new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5)),
                                        new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(6)));

        op = builder.complete();

        exclusions = Sets.newHashSet(0, 1, 2, 3, 4, 7, 8, 9, 10);
        for (int i = 0; i <= 10; i++)
        {
            row = buildRow(buildCell(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(row, staticRow, false);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // now let's test aggregated AND commands
        builder = new Operation.Builder(OperationType.AND, controller);

        // logical should be ignored by analyzer, but we still what to make sure that it is
        //IndexExpression logical = new IndexExpression(ByteBufferUtil.EMPTY_BYTE_BUFFER, IndexOperator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        //logical.setLogicalOp(LogicalIndexOperator.AND);

        //builder.add(logical);
        builder.add(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(0)));
        builder.add(new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10)));
        builder.add(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(7)));

        op = builder.complete();

        exclusions = Sets.newHashSet(7);
        for (int i = 0; i < 10; i++)
        {
            row = buildRow(buildCell(age, Int32Type.instance.decompose(i), System.currentTimeMillis()));

            boolean result = op.satisfiedBy(row, staticRow, false);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(timestamp, Operator.GTE, LongType.instance.decompose(10L)));
        builder.add(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5)));

        op = builder.complete();

        row = buildRow(buildCell(age, Int32Type.instance.decompose(6), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(22L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        // operation with internal expressions and right child
        builder = new Operation.Builder(OperationType.OR, controller,
                                        new SimpleExpression(timestamp, Operator.GT, LongType.instance.decompose(10L)));
        builder.setRight(new Operation.Builder(OperationType.AND, controller,
                                               new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0)),
                                               new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10))));
        op = builder.complete();

        row = buildRow(buildCell(age, Int32Type.instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(20), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        row = buildRow(buildCell(age, Int32Type.instance.decompose(0), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        // and for desert let's try out null and deleted rows etc.
        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(30)));
        op = builder.complete();

        Assert.assertFalse(op.satisfiedBy(null, staticRow, false));
        Assert.assertFalse(op.satisfiedBy(row, null, false));
        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        long now = System.currentTimeMillis();

        row = OperationTest.buildRow(
                Row.Deletion.regular(new DeletionTime(now - 10, (int) (now / 1000))),
                          buildCell(age, Int32Type.instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        row = buildRow(deletedCell(age, System.currentTimeMillis(), FBUtilities.nowInSeconds()));

        Assert.assertFalse(op.satisfiedBy(row, staticRow, true));

        try
        {
            Assert.assertFalse(op.satisfiedBy(buildRow(), staticRow, false));
        }
        catch (IllegalStateException e)
        {
            // expected
        }

        try
        {
            Assert.assertFalse(op.satisfiedBy(buildRow(), staticRow, true));
        }
        catch (IllegalStateException e)
        {
            Assert.fail("IllegalStateException should not be thrown when missing column and allowMissingColumns=true");
        }
    }

    @Test
    public void testAnalyzeNotIndexedButDefinedColumn() throws Exception
    {
        final ColumnDefinition firstName = getColumn(UTF8Type.instance.decompose("first_name"));
        final ColumnDefinition height = getColumn(UTF8Type.instance.decompose("height"));

        // first_name = 'a' AND height != 10
        Map<Expression.Op, Expression> expressions;
        expressions = convert(Operation.analyzeGroup(controller, OperationType.AND,
                Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression("height", Int32Type.instance)
        {{
                operation = Op.NOT_EQ;
                lower = new Bound(Int32Type.instance.decompose(5), true);
                upper = lower;
        }}, expressions.get(Expression.Op.NOT_EQ));

        expressions = convert(Operation.analyzeGroup(controller, OperationType.AND,
                Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.GT, Int32Type.instance.decompose(0)),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression("height", Int32Type.instance)
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(0), false);
            exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));

        expressions = convert(Operation.analyzeGroup(controller, OperationType.AND,
                Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)),
                              new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(0)),
                              new SimpleExpression(height, Operator.LT, Int32Type.instance.decompose(10)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression("height", Int32Type.instance)
        {{
                operation = Op.RANGE;
                lower = new Bound(Int32Type.instance.decompose(0), true);
                upper = new Bound(Int32Type.instance.decompose(10), false);
                exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));
    }

    @Test
    public void testSatisfiedByWithMultipleTerms()
    {
        final ColumnDefinition comment = getColumn(UTF8Type.instance.decompose("comment"));

        Unfiltered row = buildRow(buildCell(comment,UTF8Type.instance.decompose("software engineer is working on a project"),System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Operation.Builder builder = new Operation.Builder(OperationType.AND, controller,
                                            new SimpleExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("eng is a work")));
        Operation op = builder.complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller,
                                            new SimpleExpression(comment, Operator.LIKE_CONTAINS, UTF8Type.instance.decompose("soft works fine")));
        op = builder.complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));
    }

    @Test
    public void testSatisfiedByWithClustering()
    {
        ColumnDefinition location = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("location"));
        ColumnDefinition age = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnDefinition height = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("height"));
        ColumnDefinition score = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("score"));

        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("US"), Int32Type.instance.decompose(27)),
                                  buildCell(height, Int32Type.instance.decompose(182), System.currentTimeMillis()),
                                  buildCell(score, DoubleType.instance.decompose(1.0d), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Operation.Builder builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(27)));
        builder.add(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182)));

        Assert.assertTrue(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);

        builder.add(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(28)));
        builder.add(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182)));

        Assert.assertFalse(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US")));
        builder.add(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(27)));

        Assert.assertTrue(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("BY")));
        builder.add(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(28)));

        Assert.assertFalse(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US")));
        builder.add(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(27)));
        builder.add(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182)));

        Assert.assertTrue(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US")));
        builder.add(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182)));
        builder.add(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d)));

        Assert.assertTrue(builder.complete().satisfiedBy(row, staticRow, false));

        builder = new Operation.Builder(OperationType.AND, controller);
        builder.add(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182)));
        builder.add(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d)));

        Assert.assertTrue(builder.complete().satisfiedBy(row, staticRow, false));
    }

    private Map<Expression.Op, Expression> convert(Multimap<ColumnDefinition, Expression> expressions)
    {
        Map<Expression.Op, Expression> converted = new HashMap<>();
        for (Expression expression : expressions.values())
        {
            Expression column = converted.get(expression.getOp());
            assert column == null; // sanity check
            converted.put(expression.getOp(), expression);
        }

        return converted;
    }

    @Test
    public void testSatisfiedByWithStatic()
    {
        final ColumnDefinition sensorType = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("sensor_type"));
        final ColumnDefinition value = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("value"));

        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("date"), LongType.instance.decompose(20160401L)),
                          buildCell(value, DoubleType.instance.decompose(24.56), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING,
                         buildCell(sensorType, UTF8Type.instance.decompose("TEMPERATURE"), System.currentTimeMillis()));

        // sensor_type ='TEMPERATURE' AND value = 24.56
        Operation op = new Operation.Builder(OperationType.AND, controller,
                                        new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE")),
                                        new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))).complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        // sensor_type ='TEMPERATURE' AND value = 30
        op = new Operation.Builder(OperationType.AND, controller,
                                             new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE")),
                                             new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))).complete();

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        // sensor_type ='PRESSURE' OR value = 24.56
        op = new Operation.Builder(OperationType.OR, controller,
                                             new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE")),
                                             new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))).complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        // sensor_type ='PRESSURE' OR value = 30
        op = new Operation.Builder(OperationType.AND, controller,
                                   new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("PRESSURE")),
                                   new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))).complete();

        Assert.assertFalse(op.satisfiedBy(row, staticRow, false));

        // (sensor_type = 'TEMPERATURE' OR sensor_type = 'PRESSURE') AND value = 24.56
        op = new Operation.Builder(OperationType.OR, controller,
                                   new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE")),
                                   new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("PRESSURE")))
             .setRight(new Operation.Builder(OperationType.AND, controller,
                                             new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56)))).complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));

        // sensor_type = LIKE 'TEMP%'  AND value = 24.56
        op = new Operation.Builder(OperationType.AND, controller,
                                   new SimpleExpression(sensorType, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("TEMP")),
                                   new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))).complete();

        Assert.assertTrue(op.satisfiedBy(row, staticRow, false));
    }

    private static class SimpleExpression extends RowFilter.Expression
    {
        SimpleExpression(ColumnDefinition column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        protected Kind kind()
        {
            return Kind.SIMPLE;
        }

        @Override
        public boolean isSatisfiedBy(CFMetaData metadata, DecoratedKey partitionKey, Row row)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static Unfiltered buildRow(Cell... cells)
    {
        return buildRow(Clustering.EMPTY, null, cells);
    }

    private static Row buildRow(Row.Deletion deletion, Cell... cells)
    {
        return buildRow(Clustering.EMPTY, deletion, cells);
    }

    private static Row buildRow(Clustering clustering, Cell... cells)
    {
        return buildRow(clustering, null, cells);
    }

    private static Row buildRow(Clustering clustering, Row.Deletion deletion, Cell... cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(clustering);
        for (Cell c : cells)
            rowBuilder.addCell(c);

        if (deletion != null)
            rowBuilder.addRowDeletion(deletion);

        return rowBuilder.build();
    }

    private static Cell buildCell(ColumnDefinition column, ByteBuffer value, long timestamp)
    {
        return BufferCell.live(column, timestamp, value);
    }

    private static Cell deletedCell(ColumnDefinition column, long timestamp, int nowInSeconds)
    {
        return BufferCell.tombstone(column, timestamp, nowInSeconds);
    }

    private static ColumnDefinition getColumn(ByteBuffer name)
    {
        return getColumn(BACKEND, name);
    }

    private static ColumnDefinition getColumn(ColumnFamilyStore cfs, ByteBuffer name)
    {
        return cfs.metadata.getColumnDefinition(name);
    }
}
