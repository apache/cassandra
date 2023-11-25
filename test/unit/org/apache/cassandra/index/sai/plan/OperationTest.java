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
import com.google.common.collect.Multimap;
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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
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

    private static ColumnFamilyStore BACKEND;
    private static ColumnFamilyStore CLUSTERING_BACKEND;
    private static ColumnFamilyStore STATIC_BACKEND;

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
                                    staticSAITableMetadata(KS_NAME, STATIC_CF_NAME));

        BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
        CLUSTERING_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME);
        STATIC_BACKEND = Keyspace.open(KS_NAME).getColumnFamilyStore(STATIC_CF_NAME);
    }

    @Before
    public void beforeTest()
    {
        ReadCommand command = PartitionRangeReadCommand.allDataRead(BACKEND.metadata(), FBUtilities.nowInSeconds());
        controller = new QueryController(BACKEND,
                                         command,
                                         null,
                                         new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)),
                                         null);

        command = PartitionRangeReadCommand.allDataRead(CLUSTERING_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerClustering = new QueryController(CLUSTERING_BACKEND,
                                                   command,
                                                   null,
                                                   new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)),
                                                   null);

        command = PartitionRangeReadCommand.allDataRead(STATIC_BACKEND.metadata(), FBUtilities.nowInSeconds());
        controllerStatic = new QueryController(STATIC_BACKEND,
                                               command,
                                               null,
                                               new QueryContext(command, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS)),
                                               null);
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
        FilterTree filterTree = node.buildFilter(controller);

        DecoratedKey key = buildKey("0");
        Unfiltered row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()));

        assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()));

        assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        // range with exclusions - age > 1 AND age <= 10
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GT, Int32Type.instance.decompose(1))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(10))));
        filterTree = node.buildFilter(controller);

        Set<Integer> exclusions = Sets.newHashSet(0, 1, 11);
        for (int i = 0; i <= 11; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, row, staticRow);
            assertTrue(exclusions.contains(i) != result);
        }

        // now let's test aggregated AND commands
        node = new Operation.AndNode();

        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(0))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LT, Int32Type.instance.decompose(10))));

        filterTree = node.buildFilter(controller);

        for (int i = 0; i < 10; i++)
        {
            row = buildRow(buildCell(age, instance.decompose(i), System.currentTimeMillis()));

            boolean result = filterTree.isSatisfiedBy(key, row, staticRow);
            assertTrue(result);
        }

        // multiple analyzed expressions in the Operation timestamp >= 10 AND age = 5
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(timestamp, Operator.GTE, LongType.instance.decompose(10L))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(5))));

        filterTree = node.buildFilter(controller);

        row = buildRow(buildCell(age, instance.decompose(6), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(11L), System.currentTimeMillis()));

        assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(22L), System.currentTimeMillis()));

        assertTrue(filterTree.isSatisfiedBy(key, row, staticRow));

        row = buildRow(buildCell(age, instance.decompose(5), System.currentTimeMillis()),
                                  buildCell(timestamp, LongType.instance.decompose(9L), System.currentTimeMillis()));

        assertFalse(filterTree.isSatisfiedBy(key, row, staticRow));

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
        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("US"), Int32Type.instance.decompose(27)),
                                  buildCell(height, instance.decompose(182), System.currentTimeMillis()),
                                  buildCell(score, DoubleType.instance.decompose(1.0d), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING);

        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();

        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.EQ, Int32Type.instance.decompose(28))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.EQ, Int32Type.instance.decompose(182))));

        assertFalse(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(27))));

        assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("BY"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.GTE, Int32Type.instance.decompose(28))));

        assertFalse(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(age, Operator.LTE, Int32Type.instance.decompose(27))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));

        assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(location, Operator.EQ, UTF8Type.instance.decompose("US"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));

        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(height, Operator.GTE, Int32Type.instance.decompose(182))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(score, Operator.EQ, DoubleType.instance.decompose(1.0d))));

        assertTrue(node.buildFilter(controllerClustering).isSatisfiedBy(key, row, staticRow));
    }

    private Map<Expression.IndexOperator, Expression> convert(Multimap<ColumnMetadata, Expression> expressions)
    {
        Map<Expression.IndexOperator, Expression> converted = new EnumMap<>(Expression.IndexOperator.class);
        for (Expression expression : expressions.values())
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
        Unfiltered row = buildRow(Clustering.make(UTF8Type.instance.fromString("date"), LongType.instance.decompose(20160401L)),
                                  buildCell(value, DoubleType.instance.decompose(24.56), System.currentTimeMillis()));
        Row staticRow = buildRow(Clustering.STATIC_CLUSTERING,
                                 buildCell(sensorType, UTF8Type.instance.decompose("TEMPERATURE"), System.currentTimeMillis()));

        // sensor_type ='TEMPERATURE' AND value = 24.56
        Operation.Node node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(24.56))));

        assertTrue(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));

        // sensor_type ='TEMPERATURE' AND value = 30
        node = new Operation.AndNode();
        node.add(new Operation.ExpressionNode(new SimpleExpression(sensorType, Operator.EQ, UTF8Type.instance.decompose("TEMPERATURE"))));
        node.add(new Operation.ExpressionNode(new SimpleExpression(value, Operator.EQ, DoubleType.instance.decompose(30.00))));

        assertFalse(node.buildFilter(controllerStatic).isSatisfiedBy(key, row, staticRow));
    }

    public static TableMetadata.Builder skinnySAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder =
        TableMetadata.builder(keyspace, table)
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

    public static TableMetadata.Builder staticSAITableMetadata(String keyspace, String table)
    {
        TableMetadata.Builder builder =
        TableMetadata.builder(keyspace, table)
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
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row)
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
