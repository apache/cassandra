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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.IndexingSchemaLoader;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.marshal.Int32Type.instance;

public class OperationTest extends IndexingSchemaLoader
{
    private static final String KS_NAME = "sai";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME = "clustering_test_cf";
    private static final String STATIC_CF_NAME = "static_ndi_test_cf";

    private static ColumnFamilyStore BACKEND;
    private static ColumnFamilyStore CLUSTERING_BACKEND;
    private static ColumnFamilyStore STATIC_BACKEND;


    private QueryController controller;
    private QueryController controllerClustering;
    private QueryController controllerStatic;


    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");

        IndexingSchemaLoader.loadSchema();

        IndexingSchemaLoader.createKeyspace(KS_NAME,
                                            KeyspaceParams.simpleTransient(1),
                                            IndexingSchemaLoader.ndiCFMD(KS_NAME, CF_NAME),
                                            IndexingSchemaLoader.clusteringNDICFMD(KS_NAME, CLUSTERING_CF_NAME),
                                            IndexingSchemaLoader.staticNDICFMD(KS_NAME, STATIC_CF_NAME));

        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        CLUSTERING_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME);
        STATIC_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(STATIC_CF_NAME);
    }


    @Before
    public void beforeTest()
    {
        ReadCommand command = PartitionRangeReadCommand.allDataRead(BACKEND.metadata(), FBUtilities.nowInSeconds());
        controller = new QueryController(BACKEND, command, null, new QueryContext(), null);

        command = PartitionRangeReadCommand.allDataRead(CLUSTERING_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerClustering = new QueryController(CLUSTERING_BACKEND, command, null, new QueryContext(), null);

        command = PartitionRangeReadCommand.allDataRead(STATIC_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerStatic = new QueryController(STATIC_BACKEND, command, null, new QueryContext(), null);
    }

    @After
    public void afterTest()
    {
    }

    @Test
    public void testAnalyze()
    {
        final ColumnMetadata firstName = getColumn(UTF8Type.instance.decompose("first_name"));
        final ColumnMetadata age = getColumn(UTF8Type.instance.decompose("age"));
        final ColumnMetadata comment = getColumn(UTF8Type.instance.decompose("comment"));

        // age != 5 AND age > 1 AND age != 6 AND age <= 10
        Map<Expression.Op, Expression> expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.AND,
                                                                                    Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                                              new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                                                                              new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(6)),
                                                                                              new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10)))));

        Expression expected = new Expression(SAITester.createColumnContext("age", Int32Type.instance))
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(1), Int32Type.instance, false);
            upper = new Bound(Int32Type.instance.decompose(10), Int32Type.instance, true);

            exclusions.add(Int32Type.instance.decompose(5));
            exclusions.add(Int32Type.instance.decompose(6));
        }};

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(expected, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age >= 7
        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                                     Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                  new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(7)))));
        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), Int32Type.instance, true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        Assert.assertEquals(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(7), Int32Type.instance, true);
                            }}, expressions.get(Expression.Op.RANGE));

        // age != 5 OR age < 7
        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                                     Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)),
                                                                  new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                            {{
                                    operation = Op.RANGE;
                                    upper = new Bound(Int32Type.instance.decompose(7), Int32Type.instance, false);
                            }}, expressions.get(Expression.Op.RANGE));
        Assert.assertEquals(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(Int32Type.instance.decompose(5), Int32Type.instance, true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));

        // age > 1 AND age < 7
        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.AND,
                                                     Arrays.asList(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                                                  new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(7)))));

        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                            {{
                                    operation = Op.RANGE;
                                    lower = new Bound(Int32Type.instance.decompose(1), Int32Type.instance, false);
                                    upper = new Bound(Int32Type.instance.decompose(7), Int32Type.instance, false);
                            }}, expressions.get(Expression.Op.RANGE));

        // first_name = 'a' OR first_name != 'b'
        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                                     Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                                  new SimpleExpression(firstName, Operator.NEQ, UTF8Type.instance.decompose("b")))));

        Assert.assertEquals(2, expressions.size());
        Assert.assertEquals(new Expression(SAITester.createColumnContext("first_name", UTF8Type.instance))
                            {{
                                    operation = Op.NOT_EQ;
                                    lower = new Bound(UTF8Type.instance.decompose("b"), UTF8Type.instance, true);
                                    upper = lower;
                            }}, expressions.get(Expression.Op.NOT_EQ));
        Assert.assertEquals(new Expression(SAITester.createColumnContext("first_name", UTF8Type.instance))
                            {{
                                    operation = Op.EQ;
                                    lower = upper = new Bound(UTF8Type.instance.decompose("a"), UTF8Type.instance, true);
                            }}, expressions.get(Expression.Op.EQ));

        // comment = 'soft eng' and comment != 'likes do'
        ListMultimap<ColumnMetadata, Expression> e = Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                                                            Arrays.asList(new SimpleExpression(comment, Operator.LIKE_MATCHES, UTF8Type.instance.decompose("soft eng")),
                                                                  new SimpleExpression(comment, Operator.NEQ, UTF8Type.instance.decompose("likes do"))));

        List<Expression> expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(SAITester.createColumnContext("comment", UTF8Type.instance))
                {{
                        operation = Op.MATCH;
                        lower = new Bound(UTF8Type.instance.decompose("soft eng"), UTF8Type.instance, true);
                        upper = lower;
                }});

                add(new Expression(SAITester.createColumnContext("comment", UTF8Type.instance))
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes do"), UTF8Type.instance, true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // first_name = 'j' and comment != 'likes do'
        e = Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                   Arrays.asList(new SimpleExpression(comment, Operator.NEQ, UTF8Type.instance.decompose("likes do")),
                                      new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("j"))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(SAITester.createColumnContext("comment", UTF8Type.instance))
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(UTF8Type.instance.decompose("likes do"), UTF8Type.instance, true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(comment));

        // age != 27 first_name = 'j' and age != 25
        e = Operation.analyzeGroup(controller, Operation.OperationType.OR,
                                   Arrays.asList(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(27)),
                                      new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("j")),
                                      new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(25))));

        expectedExpressions = new ArrayList<Expression>(2)
        {{
                add(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(27), Int32Type.instance, true);
                        upper = lower;
                }});

                add(new Expression(SAITester.createColumnContext("age", Int32Type.instance))
                {{
                        operation = Op.NOT_EQ;
                        lower = new Bound(Int32Type.instance.decompose(25), Int32Type.instance, true);
                        upper = lower;
                }});
        }};

        Assert.assertEquals(expectedExpressions, e.get(age));
    }

    @Test
    public void testSatisfiedBy()
    {
        final ColumnMetadata timestamp = getColumn(UTF8Type.instance.decompose("timestamp"));
        final ColumnMetadata age = getColumn(UTF8Type.instance.decompose("age"));

        Operation.Node node = new Operation.ExpressionNode(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5)));
        FilterTree filterTree = node.buildFilter(controller);

        DecoratedKey key = buildKey("0");
        Unfiltered row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Assert.assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()));

        // and reject incorrect value
        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));

        Assert.assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        // range with exclusions - age != 5 AND age > 1 AND age != 6 AND age <= 10
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(5))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(6))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10))));
        filterTree = node.buildFilter(controller);

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 5, 6, 11);
        for (int i = 0; i <= 11; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, row, staticRow);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // now let's do something more complex - age = 5 OR age = 6
        node = new Operation.OrNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(6))));
        filterTree = node.buildFilter(controller);

        exclusions = Sets.newHashSet(0, 1, 2, 3, 4, 7, 8, 9, 10);
        for (int i = 0; i <= 10; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, row, staticRow);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // now let's test aggregated AND commands
        node = new Operation.AndNode();

        // logical should be ignored by analyzer, but we still what to make sure that it is
        //IndexExpression logical = new IndexExpression(ByteBufferUtil.EMPTY_BYTE_BUFFER, IndexOperator.EQ, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        //logical.setLogicalOp(LogicalIndexOperator.AND);

        //builder.add(logical);
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(0))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.NEQ, Int32Type.instance.decompose(7))));

        filterTree = node.buildFilter(controller);

        exclusions = Sets.newHashSet(7);
        for (int i = 0; i < 10; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, row, staticRow);
            Assert.assertTrue(exclusions.contains(i) != result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(timestamp, Operator.GTE, LongType.instance.decompose(10L))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5))));

        filterTree = node.buildFilter(controller);

        row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(22L), System.currentTimeMillis()));

        Assert.assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        // operation with internal expressions and right child
        node = new Operation.OrNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(timestamp, Operator.GT, LongType.instance.decompose(10L))));
        Operation.Node child = new Operation.AndNode();
        child.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0))));
        child.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10))));
        node.add(child);
        filterTree = node.buildFilter(controller);

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(20), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        Assert.assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(0), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        // and for desert let's try out null and deleted rows etc.
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(30))));
        filterTree = node.buildFilter(controller);

        Assert.assertFalse(filterTree.isSatisfiedBy(key, null, staticRow));
        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, null));
        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        long now = System.currentTimeMillis();

        row = OperationTest.buildRow(
        Row.Deletion.regular(new DeletionTime(now - 10, (int) (now / 1000))),
        buildCell(age, instance.decompose(6), System.currentTimeMillis()));

        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(deletedCell(age, System.currentTimeMillis(), FBUtilities.nowInSeconds()));

        Assert.assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        try
        {
            Assert.assertFalse(filterTree.isSatisfiedBy(key, buildRow(), staticRow));
        }
        catch (IllegalStateException e)
        {
            Assert.fail("IllegalStateException should not be thrown when missing column");
        }
    }

    @Test
    public void testAnalyzeNotIndexedButDefinedColumn()
    {
        final ColumnMetadata firstName = getColumn(UTF8Type.instance.decompose("first_name"));
        final ColumnMetadata height = getColumn(UTF8Type.instance.decompose("height"));

        // first_name = 'a' AND height != 10
        Map<Expression.Op, Expression> expressions;
        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.AND,
                                                     Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(SAITester.createColumnContext("height", Int32Type.instance))
        {{
                operation = Op.NOT_EQ;
                lower = new Bound(Int32Type.instance.decompose(5), Int32Type.instance, true);
                upper = lower;
        }}, expressions.get(Expression.Op.NOT_EQ));

        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.AND,
                                                     Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.GT, Int32Type.instance.decompose(0)),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(SAITester.createColumnContext("height", Int32Type.instance))
        {{
            operation = Op.RANGE;
            lower = new Bound(Int32Type.instance.decompose(0), Int32Type.instance, false);
            exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));

        expressions = convert(Operation.analyzeGroup(controller, Operation.OperationType.AND,
                                                     Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                              new SimpleExpression(height, Operator.NEQ, Int32Type.instance.decompose(5)),
                              new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(0)),
                              new SimpleExpression(height, Operator.LT, Int32Type.instance.decompose(10)))));

        Assert.assertEquals(2, expressions.size());

        Assert.assertEquals(new Expression(SAITester.createColumnContext("height", Int32Type.instance))
        {{
                operation = Op.RANGE;
                lower = new Bound(Int32Type.instance.decompose(0), Int32Type.instance, true);
                upper = new Bound(Int32Type.instance.decompose(10), Int32Type.instance, false);
                exclusions.add(Int32Type.instance.decompose(5));
        }}, expressions.get(Expression.Op.RANGE));
    }

    @Test
    public void testSatisfiedByWithClustering()
    {
        ColumnMetadata location = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("location"));
        ColumnMetadata age = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata height = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("height"));
        ColumnMetadata score = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("score"));

        DecoratedKey key = buildKey(CLUSTERING_BACKEND, "0");
        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("US"), Int32Type.instance.decompose(27)),
                                  buildCell(height, instance.decompose(182), System.currentTimeMillis()),
                                  buildCell(score, DoubleType.instance.decompose(1.0d), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        Assert.assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();

        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(28))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        Assert.assertFalse(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(27))));

        Assert.assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("BY"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(28))));

        Assert.assertFalse(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));

        Assert.assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        Assert.assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        Assert.assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));
    }

    private Map<Expression.Op, Expression> convert(Multimap<ColumnMetadata, Expression> expressions)
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
        final ColumnMetadata sensorType = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("sensor_type"));
        final ColumnMetadata value = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("value"));

        DecoratedKey key = buildKey(STATIC_BACKEND, 0);
        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("date"), LongType.instance.decompose(20160401L)),
                                  buildCell(value, DoubleType.instance.decompose(24.56), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING,
                                 buildCell(sensorType, UTF8Type.instance.decompose("TEMPERATURE"), System.currentTimeMillis()));

        // sensor_type ='TEMPERATURE' AND value = 24.56
        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        Assert.assertTrue(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // sensor_type ='TEMPERATURE' AND value = 30
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))));

        Assert.assertFalse(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // sensor_type ='PRESSURE' OR value = 24.56
        node = new Operation.OrNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("PRESSURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        Assert.assertTrue(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // sensor_type ='PRESSURE' OR value = 30
        node = new Operation.OrNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("PRESSURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))));

        Assert.assertFalse(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // (sensor_type = 'TEMPERATURE' OR sensor_type = 'PRESSURE') AND value = 24.56
        node = new Operation.AndNode();
        Operation.Node child = new Operation.OrNode();
        child.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        child.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("PRESSURE"))));
        node.add(child);
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        Assert.assertTrue(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // sensor_type = LIKE 'TEMP%'  AND value = 24.56
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.LIKE_PREFIX, UTF8Type.instance.decompose("TEMP"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        Assert.assertTrue(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));
    }

    private static class SimpleExpression extends RowFilter.Expression
    {
        SimpleExpression(ColumnMetadata column, Operator operator, ByteBuffer value)
        {
            super(column, operator, value);
        }

        @Override
        public Kind kind()
        {
            return Kind.SIMPLE;
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static DecoratedKey buildKey(Object... key) {
        return buildKey(BACKEND, key);
    }

    private static DecoratedKey buildKey(ColumnFamilyStore cfs, Object... key) {
        AbstractType<?> type = cfs.metadata().partitionKeyType;
        ByteBuffer decomposed;
        if(type instanceof CompositeType)
        {
            Preconditions.checkArgument(key.length == type.subTypes().size());
            decomposed = ((CompositeType) type).decompose(key);
        }
        else
        {
            Preconditions.checkArgument(key.length == 1);
            decomposed = ((AbstractType) type).decompose(key[0]);
        }
        return Murmur3Partitioner.instance.decorateKey(decomposed);
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

    private static Cell buildCell(ColumnMetadata column, ByteBuffer value, long timestamp)
    {
        return BufferCell.live(column, timestamp, value);
    }

    private static Cell deletedCell(ColumnMetadata column, long timestamp, int nowInSeconds)
    {
        return BufferCell.tombstone(column, timestamp, nowInSeconds);
    }

    private static ColumnMetadata getColumn(ByteBuffer name)
    {
        return getColumn(BACKEND, name);
    }

    private static ColumnMetadata getColumn(ColumnFamilyStore cfs, ByteBuffer name)
    {
        return cfs.metadata().getColumn(name);
    }
}
