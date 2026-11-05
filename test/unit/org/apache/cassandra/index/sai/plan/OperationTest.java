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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.db.marshal.Int32Type.instance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OperationTest
{
    private static final String KS_NAME = "sai";
    private static final String CF_NAME = "test_cf";
    private static final String CLUSTERING_CF_NAME = "clustering_test_cf";
    private static final String STATIC_CF_NAME = "static_sai_test_cf";
    private static final String NON_STRICT_CF_NAME = "non_strict_test_cf";
    private static final String NON_STRICT_CLUSTERING_CF_NAME = "non_strict_clustering_test_cf";
    private static final String MEMTABLE_ONLY_CF_NAME = "memtable_only_test_cf";

    private static ColumnFamilyStore BACKEND;
    private static ColumnFamilyStore CLUSTERING_BACKEND;
    private static ColumnFamilyStore STATIC_BACKEND;
    private static ColumnFamilyStore NON_STRICT_BACKEND;
    private static ColumnFamilyStore NON_STRICT_CLUSTERING_BACKEND;
    private static ColumnFamilyStore MEMTABLE_ONLY_BACKEND;

    private QueryController controller;
    private QueryController controllerClustering;
    private QueryController controllerStatic;

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        CASSANDRA_CONFIG.setString("cassandra-murmur.yaml");

        SchemaLoader.loadSchema();

        SchemaLoader.createKeyspace(KS_NAME,
                                    KeyspaceParams.simpleTransient(1),
                                    skinnySAITableMetadata(KS_NAME, CF_NAME),
                                    clusteringSAITableMetadata(KS_NAME, CLUSTERING_CF_NAME),
                                    staticSAITableMetadata(KS_NAME, STATIC_CF_NAME),
                                    nonStrictSAITableMetadata(KS_NAME, NON_STRICT_CF_NAME),
                                    nonStrictClusteringSAITableMetadata(KS_NAME, NON_STRICT_CLUSTERING_CF_NAME),
                                    nonStrictSAITableMetadata(KS_NAME, MEMTABLE_ONLY_CF_NAME));

        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        CLUSTERING_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME);
        STATIC_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(STATIC_CF_NAME);
        NON_STRICT_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(NON_STRICT_CF_NAME);
        NON_STRICT_CLUSTERING_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(NON_STRICT_CLUSTERING_CF_NAME);
        MEMTABLE_ONLY_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(MEMTABLE_ONLY_CF_NAME);

        // A single flushed row gives each indexed column a known, single-point on-disk range (min == max),
        // so a predicate covers the whole range iff its bound sits on the far side of that single value.
        new RowUpdateBuilder(NON_STRICT_BACKEND.metadata(), FBUtilities.timestampMicros(), "0").add("tag", "a")
                                                                                               .add("age", 5)
                                                                                               .add("score", 5.0d)
                                                                                               .add("weight", 5.0d)
                                                                                               .build()
                                                                                               .apply();
        NON_STRICT_BACKEND.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        new RowUpdateBuilder(NON_STRICT_CLUSTERING_BACKEND.metadata(), FBUtilities.timestampMicros(), "0").clustering(5)
                                                                                                          .add("weight", 5.0d)
                                                                                                          .add("score", 5.0d)
                                                                                                          .build()
                                                                                                          .apply();
        NON_STRICT_CLUSTERING_BACKEND.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // This row only ever exists in the Memtable:
        new RowUpdateBuilder(MEMTABLE_ONLY_BACKEND.metadata(), FBUtilities.timestampMicros(), "0").add("tag", "a")
                                                                                                  .add("age", 5)
                                                                                                  .add("score", 5.0d)
                                                                                                  .add("weight", 5.0d)
                                                                                                  .build()
                                                                                                  .apply();
    }

    @Before
    public void beforeTest()
    {
        ReadCommand command = PartitionRangeReadCommand.allDataRead(BACKEND.metadata(), FBUtilities.nowInSeconds());
        controller = new QueryController(BACKEND, command, null, contextWithUnrepairedMatches(command));

        command = PartitionRangeReadCommand.allDataRead(CLUSTERING_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerClustering = new QueryController(CLUSTERING_BACKEND, command, null, contextWithUnrepairedMatches(command));

        command = PartitionRangeReadCommand.allDataRead(STATIC_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerStatic = new QueryController(STATIC_BACKEND, command, null, contextWithUnrepairedMatches(command));
    }

    private static QueryContext contextWithUnrepairedMatches(ReadCommand command)
    {
        QueryContext context = new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
        context.hasUnrepairedMatches = true;
        return context;
    }

    private static QueryController controllerFor(ColumnFamilyStore cfs, boolean needsReconciliation, RowFilter.Expression... expressions)
    {
        RowFilter rowFilter = RowFilter.create(needsReconciliation);
        for (RowFilter.Expression expression : expressions)
            rowFilter.add(expression.column(), expression.operator(), expression.getIndexValue());

        ReadCommand command = PartitionRangeReadCommand.create(cfs.metadata(),
                                                                FBUtilities.nowInSeconds(),
                                                                ColumnFilter.all(cfs.metadata()),
                                                                rowFilter,
                                                                DataLimits.NONE,
                                                                DataRange.allData(cfs.metadata().partitioner));
        return new QueryController(cfs, command, null, contextWithUnrepairedMatches(command));
    }

    @Test
    public void testAnalyze()
    {
        final ColumnMetadata age = getColumn(UTF8Type.instance.decompose("age"));

        // age > 1 AND age < 7
        Map<Expression.IndexOperator, Expression> expressions = convert(Operation.buildIndexExpressions(controller,
                                                                                                        Arrays.asList(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1)),
                                                                                                                      new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(7)))));

        assertEquals(1, expressions.size());

        Expression rangeExpression = expressions.get(Expression.IndexOperator.RANGE);

        assertExpression(rangeExpression, Expression.IndexOperator.RANGE, Int32Type.instance.decompose(1), false, Int32Type.instance.decompose(7), false);
    }

    @Test
    public void testSatisfiedBy()
    {
        final ColumnMetadata timestamp = getColumn(UTF8Type.instance.decompose("timestamp"));
        final ColumnMetadata age = getColumn(UTF8Type.instance.decompose("age"));

        Operation.Node node = new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5)));
        FilterTree filterTree = node.buildFilter(controller, true);

        DecoratedKey key = buildKey("0");
        Unfiltered row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        assertFalse(filterTree.isSatisfiedBy(key, (Row) row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()));

        assertTrue(filterTree.isSatisfiedBy(key, (Row) row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));

        assertFalse(filterTree.isSatisfiedBy(key, (Row) row, staticRow));

        // range with exclusions - age > 1 AND age <= 10
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10))));
        filterTree = node.buildFilter(controller, true);

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 11);
        for (int i = 0; i <= 11; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, (Row) row, staticRow);
            assertTrue(exclusions.contains(i) != result);
        }

        // now let's test aggregated AND commands
        node = new Operation.AndNode();

        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(0))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10))));

        filterTree = node.buildFilter(controller, true);

        for (int i = 0; i < 10; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, (Row) row, staticRow);
            assertTrue(result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(timestamp, Operator.GTE, LongType.instance.decompose(10L))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5))));

        FilterTree filterTreeStrict = node.buildFilter(controller, true);
        FilterTree filterTreeNonStrict = node.buildFilter(controller, false);

        long startTime = System.currentTimeMillis();
        row = buildRow(buildCell(age, instance.decompose(6), startTime),
                       buildCell(timestamp, LongType.instance.decompose(11L), startTime + 1));

        assertFalse(filterTreeStrict.isSatisfiedBy(key, (Row) row, staticRow));
        assertTrue(filterTreeNonStrict.isSatisfiedBy(key, (Row) row, staticRow)); // matches on timestamp >= 10

        row = buildRow(buildCell(age, instance.decompose(5), startTime + 2),
                       buildCell(timestamp, LongType.instance.decompose(22L), startTime + 3));

        assertTrue(filterTreeStrict.isSatisfiedBy(key, (Row) row, staticRow));
        assertTrue(filterTreeNonStrict.isSatisfiedBy(key, (Row) row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), startTime + 4),
                       buildCell(timestamp, LongType.instance.decompose(9L), startTime + 5));

        assertFalse(filterTreeStrict.isSatisfiedBy(key, (Row) row, staticRow));
        assertTrue(filterTreeNonStrict.isSatisfiedBy(key, (Row) row, staticRow)); // matches on age = 5
    }

    @Test
    public void testAnalyzeNotIndexedButDefinedColumn()
    {
        final ColumnMetadata firstName = getColumn(UTF8Type.instance.decompose("first_name"));
        final ColumnMetadata height = getColumn(UTF8Type.instance.decompose("height"));

        // first_name = 'a' AND height > 5
        Map<Expression.IndexOperator, Expression> expressions;
        expressions = convert(Operation.buildIndexExpressions(controller,
                                                              Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                                   new SimpleExpression(height, Operator.GT, Int32Type.instance.decompose(5)))));

        assertEquals(2, expressions.size());

        expressions = convert(Operation.buildIndexExpressions(controller,
                                                              Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                                   new SimpleExpression(height, Operator.GT, Int32Type.instance.decompose(0)),
                                                                   new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(5)))));

        assertEquals(2, expressions.size());

        Expression rangeExpression = expressions.get(Expression.IndexOperator.RANGE);

        assertExpression(rangeExpression, Expression.IndexOperator.RANGE, Int32Type.instance.decompose(0), false, Int32Type.instance.decompose(5), true);

        expressions = convert(Operation.buildIndexExpressions(controller,
                                                              Arrays.asList(new SimpleExpression(firstName, Operator.EQ, UTF8Type.instance.decompose("a")),
                                                                            new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(0)),
                                                                            new SimpleExpression(height, Operator.LT, Int32Type.instance.decompose(10)))));

        assertEquals(2, expressions.size());

        rangeExpression = expressions.get(Expression.IndexOperator.RANGE);

        assertExpression(rangeExpression, Expression.IndexOperator.RANGE, Int32Type.instance.decompose(0), true, Int32Type.instance.decompose(10), false);
    }

    @Test
    public void testSatisfiedByWithClustering()
    {
        ColumnMetadata location = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("location"));
        ColumnMetadata age = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata height = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("height"));
        ColumnMetadata score = getColumn(CLUSTERING_BACKEND, UTF8Type.instance.decompose("score"));

        DecoratedKey key = buildKey(CLUSTERING_BACKEND, "0");
        Row row = buildRow(Clustering.make(UTF8Type.instance.fromString("US"), Int32Type.instance.decompose(27)),
                                  buildCell(height, instance.decompose(182), System.currentTimeMillis()),
                                  buildCell(score, DoubleType.instance.decompose(1.0d), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        assertTrue(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();

        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(28))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        assertFalse(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(27))));

        assertTrue(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("BY"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(28))));

        assertFalse(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));

        assertTrue(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        assertTrue(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        assertTrue(node.buildFilter(controllerClustering, true).isSatisfiedBy(key, row, staticRow));
    }

    private Map<Expression.IndexOperator, Expression> convert(Operation.Expressions expressions)
    {
        Map<Expression.IndexOperator, Expression> converted = new EnumMap<>(Expression.IndexOperator.class);
        for (Expression expression : expressions.all())
        {
            Expression column = converted.get(expression.getIndexOperator());
            assert column == null; // sanity check
            converted.put(expression.getIndexOperator(), expression);
        }

        return converted;
    }

    @Test
    public void testSatisfiedByWithStatic()
    {
        final ColumnMetadata sensorType = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("sensor_type"));
        final ColumnMetadata value = getColumn(STATIC_BACKEND, UTF8Type.instance.decompose("value"));

        DecoratedKey key = buildKey(STATIC_BACKEND, 0);
        Row row = buildRow(Clustering.make(UTF8Type.instance.fromString("date"), LongType.instance.decompose(20160401L)),
                                  buildCell(value, DoubleType.instance.decompose(24.56), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING,
                                 buildCell(sensorType, UTF8Type.instance.decompose("TEMPERATURE"), System.currentTimeMillis()));

        // sensor_type ='TEMPERATURE' AND value = 24.56
        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        assertTrue(node.buildFilter(controllerStatic, true).isSatisfiedBy(key, row, staticRow));

        // sensor_type ='TEMPERATURE' AND value = 30
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))));

        assertFalse(node.buildFilter(controllerStatic, true).isSatisfiedBy(key, row, staticRow));
    }

    @Test
    public void testFullRangePredicateDemotedToPostFilterWhenNonStrict()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertTrue(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testFullRangePredicateNotDemotedWhenOnlyInMemtable()
    {
        // MEMTABLE_ONLY_BACKEND's row is never flushed. The demotion check only ever looks at on-disk data, so a 
        // predicate that's unselective only against live Memtable data stays indexed until the Memtable flushes.
        ColumnMetadata age = getColumn(MEMTABLE_ONLY_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(MEMTABLE_ONLY_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(MEMTABLE_ONLY_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertFalse(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testFullRangePredicateNotDemotedWithOnlyOneIndexedColumn()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr), Arrays.asList(ageExpr));
        assertFalse(expressions.isUnindexed(age));
    }

    @Test
    public void testFullRangePredicateNotDemotedUnderStrictFiltering()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, false, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertFalse(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testEqualityPredicateNeverDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        // age = 5 matches the only on-disk value, but equality predicates are never demoted
        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(0.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertFalse(expressions.isUnindexed(age));
        assertTrue(expressions.isUnindexed(score));
    }

    @Test
    public void testAtLeastOneIndexedColumnSurvivesDemotion()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(0.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertTrue(expressions.isUnindexed(age) != expressions.isUnindexed(score));
    }

    @Test
    public void testMultipleCoveringColumnsAllButOneDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));
        ColumnMetadata weight = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("weight"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(0.0));
        RowFilter.Expression weightExpr = new SimpleExpression(weight, Operator.GT, DoubleType.instance.decompose(0.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr, weightExpr), Arrays.asList(ageExpr, scoreExpr, weightExpr));

        int demoted = 0;
        if (expressions.isUnindexed(age)) demoted++;
        if (expressions.isUnindexed(score)) demoted++;
        if (expressions.isUnindexed(weight)) demoted++;

        // Exactly one of the three fully-covering columns must stay indexed to drive the search.
        assertEquals(2, demoted);
    }

    @Test
    public void testTwoSidedRangeFullyCoveringBothBoundsDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageLower = new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(0));
        RowFilter.Expression ageUpper = new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageLower, ageUpper, scoreExpr), Arrays.asList(ageLower, ageUpper, scoreExpr));

        assertTrue(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testUpperOnlyBoundCoveringDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(1000));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertTrue(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testExactBoundaryRangeMatchDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        // age is pinned to exactly the single on-disk value via range operators, not equality
        RowFilter.Expression ageLower = new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(5));
        RowFilter.Expression ageUpper = new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(5));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageLower, ageUpper, scoreExpr), Arrays.asList(ageLower, ageUpper, scoreExpr));

        assertTrue(expressions.isUnindexed(age));
        assertFalse(expressions.isUnindexed(score));
    }

    @Test
    public void testNarrowlyMissingLowerBoundNotDemoted()
    {
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));
        ColumnMetadata score = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("score"));

        // age's only on-disk value is 5, so >= 6 excludes it by exactly one
        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(6));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(0.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, ageExpr, scoreExpr), Arrays.asList(ageExpr, scoreExpr));

        assertFalse(expressions.isUnindexed(age));
        assertTrue(expressions.isUnindexed(score));
    }

    @Test
    public void testLiteralRangePredicateIsNeverIndexed()
    {
        ColumnMetadata tag = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("tag"));
        ColumnMetadata age = getColumn(NON_STRICT_BACKEND, UTF8Type.instance.decompose("age"));

        // SAI only supports range operators on non-literal types, so this never even reaches the demotion
        // logic as an indexed expression in the first place; it's unindexed from the start, same as a
        // predicate on a column with no index at all.
        RowFilter.Expression tagExpr = new SimpleExpression(tag, Operator.GTE, UTF8Type.instance.decompose("a"));
        RowFilter.Expression ageExpr = new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_BACKEND, true, tagExpr, ageExpr), Arrays.asList(tagExpr, ageExpr));

        assertTrue(expressions.isUnindexed(tag));
        assertFalse(expressions.isUnindexed(age));
    }

    @Test
    public void testIndexedClusteringColumnNeverDemoted()
    {
        ColumnMetadata rank = getColumn(NON_STRICT_CLUSTERING_BACKEND, UTF8Type.instance.decompose("rank"));
        ColumnMetadata weight = getColumn(NON_STRICT_CLUSTERING_BACKEND, UTF8Type.instance.decompose("weight"));
        ColumnMetadata score = getColumn(NON_STRICT_CLUSTERING_BACKEND, UTF8Type.instance.decompose("score"));

        // isMutableIntersection() only counts non-primary-key columns, so a second regular column (score) is
        // needed alongside weight to make this a non-strict read in the first place.
        RowFilter.Expression rankExpr = new SimpleExpression(rank, Operator.GT, Int32Type.instance.decompose(0));
        RowFilter.Expression weightExpr = new SimpleExpression(weight, Operator.GT, DoubleType.instance.decompose(0.0));
        RowFilter.Expression scoreExpr = new SimpleExpression(score, Operator.GT, DoubleType.instance.decompose(100.0));

        Operation.Expressions expressions = Operation.buildIndexExpressions(controllerFor(NON_STRICT_CLUSTERING_BACKEND, true, rankExpr, weightExpr, scoreExpr), Arrays.asList(rankExpr, weightExpr, scoreExpr));

        assertFalse(expressions.isUnindexed(rank));
        assertTrue(expressions.isUnindexed(weight));
        assertFalse(expressions.isUnindexed(score));
    }

    public static TableMetadata.Builder skinnySAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table)
                                                     .addPartitionKeyColumn("id", UTF8Type.instance)
                                                     .addRegularColumn("first_name", UTF8Type.instance)
                                                     .addRegularColumn("last_name", UTF8Type.instance)
                                                     .addRegularColumn("age", Int32Type.instance)
                                                     .addRegularColumn("height", Int32Type.instance)
                                                     .addRegularColumn("timestamp", LongType.instance)
                                                     .addRegularColumn("address", UTF8Type.instance)
                                                     .addRegularColumn("score", DoubleType.instance);

        Indexes.Builder indexes = Indexes.builder();
        addIndex(indexes, table, "first_name");
        addIndex(indexes, table, "last_name");
        addIndex(indexes, table, "age");
        addIndex(indexes, table, "timestamp");
        addIndex(indexes, table, "address");
        addIndex(indexes, table, "score");

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder clusteringSAITableMetadata(String keyspace, String table)
    {
        return clusteringSAITableMetadata(keyspace, table, "location", "age", "height", "score");
    }

    public static TableMetadata.Builder clusteringSAITableMetadata(String keyspace, String table, String...indexedColumns)
    {
        Indexes.Builder indexes = Indexes.builder();
        for (String indexedColumn : indexedColumns)
        {
            addIndex(indexes, table, indexedColumn);
        }

        return TableMetadata.builder(keyspace, table)
                            .addPartitionKeyColumn("name", UTF8Type.instance)
                            .addClusteringColumn("location", UTF8Type.instance)
                            .addClusteringColumn("age", Int32Type.instance)
                            .addRegularColumn("height", Int32Type.instance)
                            .addRegularColumn("score", DoubleType.instance)
                            .indexes(indexes.build());
    }

    public static TableMetadata.Builder nonStrictSAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table)
                                                     .addPartitionKeyColumn("id", UTF8Type.instance)
                                                     .addRegularColumn("tag", UTF8Type.instance)
                                                     .addRegularColumn("age", Int32Type.instance)
                                                     .addRegularColumn("score", DoubleType.instance)
                                                     .addRegularColumn("weight", DoubleType.instance);

        Indexes.Builder indexes = Indexes.builder();
        addIndex(indexes, table, "tag");
        addIndex(indexes, table, "age");
        addIndex(indexes, table, "score");
        addIndex(indexes, table, "weight");

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder nonStrictClusteringSAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table)
                                                     .addPartitionKeyColumn("id", UTF8Type.instance)
                                                     .addClusteringColumn("rank", Int32Type.instance)
                                                     .addRegularColumn("weight", DoubleType.instance)
                                                     .addRegularColumn("score", DoubleType.instance);

        Indexes.Builder indexes = Indexes.builder();
        addIndex(indexes, table, "rank");
        addIndex(indexes, table, "weight");
        addIndex(indexes, table, "score");

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder staticSAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder = TableMetadata.builder(keyspace, table)
                                                     .addPartitionKeyColumn("sensor_id", Int32Type.instance)
                                                     .addStaticColumn("sensor_type", UTF8Type.instance)
                                                     .addClusteringColumn("date", LongType.instance)
                                                     .addRegularColumn("value", DoubleType.instance)
                                                     .addRegularColumn("variance", Int32Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        addIndex(indexes, table, "sensor_type");
        addIndex(indexes, table, "value");
        addIndex(indexes, table, "variance");

        return builder.indexes(indexes.build());
    }

    private void assertExpression(Expression expression, Expression.IndexOperator indexOperator, ByteBuffer lower,
                                  boolean lowerInclusive, ByteBuffer upper, boolean upperInclusive)
    {
        assertEquals(indexOperator, expression.getIndexOperator());
        assertEquals(lower, expression.lower().value.raw);
        assertEquals(lowerInclusive, expression.lower().inclusive);
        assertEquals(upper, expression.upper().value.raw);
        assertEquals(upperInclusive, expression.upper().inclusive);
    }

    private static void addIndex(Indexes.Builder indexes, String table, String column)
    {
        String indexName = table + '_' + column;
        indexes.add(IndexMetadata.fromSchemaMetadata(indexName, IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StorageAttachedIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, column);
        }}));
    }

    private static DecoratedKey buildKey(Object... key)
    {
        return buildKey(BACKEND, key);
    }

    private static DecoratedKey buildKey(ColumnFamilyStore cfs, Object... key)
    {
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

    private static Unfiltered buildRow(Cell<?>... cells)
    {
        return buildRow(Clustering.EMPTY, cells);
    }

    private static Row buildRow(Clustering<?> clustering, Cell<?>... cells)
    {
        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
        rowBuilder.newRow(clustering);
        for (Cell<?> c : cells)
            rowBuilder.addCell(c);

        return rowBuilder.build();
    }

    private static Cell<?> buildCell(ColumnMetadata column, ByteBuffer value, long timestamp)
    {
        return BufferCell.live(column, timestamp, value);
    }

    private static ColumnMetadata getColumn(ByteBuffer name)
    {
        return getColumn(BACKEND, name);
    }

    private static ColumnMetadata getColumn(ColumnFamilyStore cfs, ByteBuffer name)
    {
        return cfs.metadata().getColumn(name);
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
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String toString(boolean cql)
        {
            AbstractType<?> type = column.type;
            switch (operator)
            {
                case CONTAINS:
                    assert type instanceof CollectionType;
                    CollectionType<?> ct = (CollectionType<?>)type;
                    type = ct.kind == CollectionType.Kind.SET ? ct.nameComparator() : ct.valueComparator();
                    break;
                case CONTAINS_KEY:
                    assert type instanceof MapType;
                    type = ((MapType<?, ?>)type).nameComparator();
                    break;
                case IN:
                    type = ListType.getInstance(type, false);
                    break;
                default:
                    break;
            }
            return cql
                   ? String.format("%s %s %s", column.name.toCQLString(), operator, type.toCQLString(value) )
                   : String.format("%s %s %s", column.name.toString(), operator, type.getString(value));
        }
    }
}
