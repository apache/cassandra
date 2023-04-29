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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.NavigableMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.virtual.AbstractMutableVirtualTable;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.triggers.ITrigger;


import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class VirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "test_virtual_ks";
    private static final String VT1_NAME = "vt1";
    private static final String VT2_NAME = "vt2";
    private static final String VT3_NAME = "vt3";
    private static final String VT4_NAME = "vt4";
    private static final String VT5_NAME = "vt5";

    // As long as we execute test queries using execute (and not executeNet) the virtual tables implementation
    // do not need to be thread-safe. We choose to do it to avoid issues if the test framework was changed or somebody
    // decided to use the class with executeNet. It also provide a better example in case somebody is looking
    // at the test for learning how to create mutable virtual tables
    private static class MutableVirtualTable extends AbstractMutableVirtualTable
    {
        // <pk1, pk2> -> c1 -> c2 -> <v1, v2>
        private final Map<Pair<String, String>, NavigableMap<String, NavigableMap<String, Pair<Number, Number>>>> backingMap = new ConcurrentHashMap<>();

        MutableVirtualTable(String keyspaceName, String tableName)
        {
            super(TableMetadata.builder(keyspaceName, tableName)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("pk1", UTF8Type.instance)
                               .addPartitionKeyColumn("pk2", UTF8Type.instance)
                               .addClusteringColumn("c1", UTF8Type.instance)
                               .addClusteringColumn("c2", UTF8Type.instance)
                               .addRegularColumn("v1", Int32Type.instance)
                               .addRegularColumn("v2", LongType.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet data = new SimpleDataSet(metadata());
            backingMap.forEach((pkPair, c1Map) ->
                    c1Map.forEach((c1, c2Map) ->
                    c2Map.forEach((c2, valuePair) -> data.row(pkPair.getLeft(), pkPair.getRight(), c1, c2)
                                                         .column("v1", valuePair.getLeft())
                                                         .column("v2", valuePair.getRight()))));
            return data;
        }

        @Override
        protected void applyPartitionDeletion(ColumnValues partitionKeyColumns)
        {
            backingMap.remove(toPartitionKey(partitionKeyColumns));
        }

        @Override
        protected void applyRangeTombstone(ColumnValues partitionKeyColumns, Range<ColumnValues> range)
        {
            Optional<NavigableMap<String, NavigableMap<String, Pair<Number, Number>>>> mayBePartition = getPartition(partitionKeyColumns);

            if (!mayBePartition.isPresent())
                return;

            NavigableMap<String, NavigableMap<String, Pair<Number, Number>>> selection = mayBePartition.get();

            for (String c1 : ImmutableList.copyOf(selection.keySet()))
            {
                NavigableMap<String, Pair<Number, Number>> rows = selection.get(c1);

                for (String c2 : ImmutableList.copyOf(selection.get(c1).keySet()))
                {
                    if (range.contains(new ColumnValues(metadata().clusteringColumns(), c1, c2)))
                        rows.remove(c2);
                }

                if (rows.isEmpty())
                    selection.remove(c1);
            }
        }

        @Override
        protected void applyRowDeletion(ColumnValues partitionKeyColumns, ColumnValues clusteringColumns)
        {
            getRows(partitionKeyColumns, clusteringColumns.value(0)).ifPresent(rows -> rows.remove(clusteringColumns.value(1)));
        }

        @Override
        protected void applyColumnDeletion(ColumnValues partitionKeyColumns, ColumnValues clusteringColumns, String columnName)
        {
            getRows(partitionKeyColumns, clusteringColumns.value(0)).ifPresent(rows -> rows.computeIfPresent(clusteringColumns.value(1),
                                                                                                             (c, p) -> updateColumn(p, columnName, null)));
        }

        @Override
        protected void applyColumnUpdate(ColumnValues partitionKeyColumns,
                                         ColumnValues clusteringColumns,
                                         Optional<ColumnValue> mayBeColumnValue)
        {
            Pair<String, String> pkPair = toPartitionKey(partitionKeyColumns);
            backingMap.computeIfAbsent(pkPair, ignored -> new ConcurrentSkipListMap<>())
                      .computeIfAbsent(clusteringColumns.value(0), ignored -> new ConcurrentSkipListMap<>())
                      .compute(clusteringColumns.value(1), (ignored, p) -> updateColumn(p, mayBeColumnValue));
        }

        @Override
        public void truncate()
        {
            backingMap.clear();
        }

        private Optional<NavigableMap<String, Pair<Number, Number>>> getRows(ColumnValues partitionKeyColumns, Comparable<?> firstClusteringColumn)
        {
            return getPartition(partitionKeyColumns).map(p -> p.get(firstClusteringColumn));
        }

        private Optional<NavigableMap<String, NavigableMap<String, Pair<Number, Number>>>> getPartition(ColumnValues partitionKeyColumns)
        {
            Pair<String, String> pk = toPartitionKey(partitionKeyColumns);
            return Optional.ofNullable(backingMap.get(pk));
        }

        private Pair<String, String> toPartitionKey(ColumnValues partitionKey)
        {
            return Pair.of(partitionKey.value(0), partitionKey.value(1));
        }

        private static Pair<Number, Number> updateColumn(@Nonnull Pair<Number, Number> row,
                                                         String columnName,
                                                         Number newValue)
        {
            return "v1".equals(columnName) ? Pair.of(newValue, row.getRight())
                                           : Pair.of(row.getLeft(), newValue);
        }

        private static Pair<Number, Number> updateColumn(Pair<Number, Number> row,
                                                         Optional<ColumnValue> mayBeColumnValue)
        {
            Pair<Number, Number> r = row != null ? row : Pair.of(null, null);

            if (mayBeColumnValue.isPresent())
            {
                ColumnValue newValue = mayBeColumnValue.get();
                return updateColumn(r, newValue.name(), newValue.value());
            }

            return r;
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        TableMetadata vt1Metadata = TableMetadata.builder(KS_NAME, VT1_NAME)
                .kind(TableMetadata.Kind.VIRTUAL)
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addClusteringColumn("c", UTF8Type.instance)
                .addRegularColumn("v1", Int32Type.instance)
                .addRegularColumn("v2", LongType.instance)
                .build();

        SimpleDataSet vt1data = new SimpleDataSet(vt1Metadata);

        vt1data.row("pk1", "c1").column("v1", 11).column("v2", 11L)
               .row("pk2", "c1").column("v1", 21).column("v2", 21L)
               .row("pk1", "c2").column("v1", 12).column("v2", 12L)
               .row("pk2", "c2").column("v1", 22).column("v2", 22L)
               .row("pk1", "c3").column("v1", 13).column("v2", 13L)
               .row("pk2", "c3").column("v1", 23).column("v2", 23L);

        VirtualTable vt1 = new AbstractVirtualTable(vt1Metadata)
        {
            public DataSet data()
            {
                return vt1data;
            }
        };
        VirtualTable vt2 = new MutableVirtualTable(KS_NAME, VT2_NAME);

        TableMetadata vt3Metadata = TableMetadata.builder(KS_NAME, VT3_NAME)
                .kind(TableMetadata.Kind.VIRTUAL)
                .addPartitionKeyColumn("pk1", UTF8Type.instance)
                .addPartitionKeyColumn("pk2", UTF8Type.instance)
                .addClusteringColumn("ck1", UTF8Type.instance)
                .addClusteringColumn("ck2", UTF8Type.instance)
                .addRegularColumn("v1", Int32Type.instance)
                .addRegularColumn("v2", LongType.instance)
                .build();

        SimpleDataSet vt3data = new SimpleDataSet(vt3Metadata);

        vt3data.row("pk11", "pk11", "ck11", "ck11").column("v1", 1111).column("v2", 1111L)
               .row("pk11", "pk11", "ck22", "ck22").column("v1", 1122).column("v2", 1122L);

        VirtualTable vt3 = new AbstractVirtualTable(vt3Metadata)
        {
            public DataSet data()
            {
                return vt3data;
            }
        };

        TableMetadata vt4Metadata = TableMetadata.builder(KS_NAME, VT4_NAME)
                .kind(TableMetadata.Kind.VIRTUAL)
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addRegularColumn("v", LongType.instance)
                .build();

        // As long as we execute test queries using execute (and not executeNet) the virtual tables implementation
        // do not need to be thread-safe. We choose to do it to avoid issues if the test framework was changed or somebody
        // decided to use the class with executeNet. It also provide a better example in case somebody is looking
        // at the test for learning how to create mutable virtual tables
        VirtualTable vt4 = new AbstractMutableVirtualTable(vt4Metadata)
        {
            // CHM cannot be used here as they do not accept null values
            private final AtomicReference<Map<String, Long>> table = new AtomicReference<Map<String, Long>>(Collections.emptyMap());

            @Override
            public DataSet data()
            {
                SimpleDataSet data = new SimpleDataSet(metadata());
                table.get().forEach((pk, v) -> data.row(pk).column("v", v));
                return data;
            }

            @Override
            protected void applyPartitionDeletion(ColumnValues partitionKey)
            {
                Map<String, Long> oldMap;
                Map<String, Long> newMap;
                do
                {
                    oldMap = table.get();
                    newMap = new HashMap<>(oldMap);
                    newMap.remove(partitionKey.value(0));
                }
                while(!table.compareAndSet(oldMap, newMap));
            }

            @Override
            protected void applyColumnDeletion(ColumnValues partitionKey,
                                               ColumnValues clusteringColumns,
                                               String columnName)
            {
                Map<String, Long> oldMap;
                Map<String, Long> newMap;
                do
                {
                    oldMap = table.get();

                    if (!oldMap.containsKey(partitionKey.value(0)))
                        break;

                    newMap = new HashMap<>(oldMap);
                    newMap.put(partitionKey.value(0), null);
                }
                while(!table.compareAndSet(oldMap, newMap));
            }

            @Override
            protected void applyColumnUpdate(ColumnValues partitionKey,
                                             ColumnValues clusteringColumns,
                                             Optional<ColumnValue> columnValue)
            {
                Map<String, Long> oldMap;
                Map<String, Long> newMap;
                do
                {
                    oldMap = table.get();
                    if (oldMap.containsKey(partitionKey.value(0)) && !columnValue.isPresent())
                        break;
                    newMap = new HashMap<>(oldMap);
                    newMap.put(partitionKey.value(0), columnValue.isPresent() ? columnValue.get().value() : null);
                }
                while(!table.compareAndSet(oldMap, newMap));
            }

            @Override
            public void truncate()
            {
                Map<String, Long> oldMap;
                do
                {
                    oldMap = table.get();
                    if (oldMap.isEmpty())
                        break;
                }
                while(!table.compareAndSet(oldMap, Collections.emptyMap()));
            }

        };

        VirtualTable vt5 = new AbstractVirtualTable(TableMetadata.builder(KS_NAME, VT5_NAME)
                                                                 .kind(TableMetadata.Kind.VIRTUAL)
                                                                 .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                                 .addClusteringColumn("c", UTF8Type.instance)
                                                                 .addRegularColumn("v1", Int32Type.instance)
                                                                 .addRegularColumn("v2", LongType.instance)
                                                                 .build())
        {
            public DataSet data()
            {
                return new SimpleDataSet(metadata());
            }

            @Override
            public boolean allowFilteringImplicitly()
            {
                return false;
            }
        };

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(vt1, vt2, vt3, vt4, vt5)));

        CQLTester.setUpClass();
    }

    @Test
    public void testReadOperationsOnReadOnlyTable() throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt1 WHERE pk = 'UNKNOWN'"));

        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c = 'UNKNOWN'"));

        // Test DISTINCT query
        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks.vt1"),
                      row("pk1"),
                      row("pk2"));

        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks.vt1 WHERE token(pk) > token('pk1')"),
                      row("pk2"));

        // Test single partition queries
        assertRowsNet(executeNet("SELECT v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c = 'c1'"),
                      row(11, 11L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c IN ('c1', 'c2')"),
                      row("c1", 11, 11L),
                      row("c2", 12, 12L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c IN ('c2', 'c1') ORDER BY c DESC"),
                      row("c2", 12, 12L),
                      row("c1", 11, 11L));

        // Test multi-partition queries
        assertRows(execute("SELECT * FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row("pk1", "c1", 11, 11L),
                   row("pk1", "c2", 12, 12L),
                   row("pk2", "c1", 21, 21L),
                   row("pk2", "c2", 22, 22L));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC"),
                   row("pk1", "c2", 12),
                   row("pk2", "c2", 22),
                   row("pk1", "c1", 11),
                   row("pk2", "c1", 21));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC LIMIT 1"),
                   row("pk1", "c2", 12));

        assertRows(execute("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1' , 'c3') ORDER BY c DESC PER PARTITION LIMIT 1"),
                   row("c3", 13, 13L),
                   row("c3", 23, 23L));

        assertRows(execute("SELECT count(*) FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row(4L));

        for (int pageSize = 1; pageSize < 5; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT pk, c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c1", 21, 21L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') LIMIT 2", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row(4L));
        }

        // Test range queries
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) <= token('pk2') AND c > 'c1' PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks.vt1 WHERE token(pk) = token('pk2') AND c < 'c3' ALLOW FILTERING", pageSize),
                          row(2L));
        }
    }

    @Test
    public void testReadOperationsOnReadOnlyTableWithMultiplePks() throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt3 WHERE pk1 = 'UNKNOWN' AND pk2 = 'UNKNOWN'"));

        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt3 WHERE pk1 = 'pk11' AND pk2 = 'pk22' AND ck1 = 'UNKNOWN'"));

        // Test DISTINCT query
        assertRowsNet(executeNet("SELECT DISTINCT pk1, pk2 FROM test_virtual_ks.vt3"),
                      row("pk11", "pk11"));

        // Test single partition queries
        assertRowsNet(executeNet("SELECT v1, v2 FROM test_virtual_ks.vt3 WHERE pk1 = 'pk11' AND pk2 = 'pk11'"),
                      row(1111, 1111L),
                      row(1122, 1122L));
    }

    @Test
    public void testDMLOperationsOnMutableCompositeTable() throws Throwable
    {
        // check for a clean state
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET v1 =  1, v2 =  1 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  2, v2 =  2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  3, v2 =  3 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  4, v2 =  4 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_3';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  5, v2 =  5 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_5';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  6, v2 =  6 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_6';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  7, v2 =  7 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  8, v2 =  8 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_2' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  9, v2 =  9 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 10, v2 = 10 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_2';" +
                "APPLY BATCH");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // update a single column with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET v1 = 11 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1'"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 11, 1L));

        // update multiple columns with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET v1 = 111, v2 = 111 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1'"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 111, 111L));

        // update a single columns with INSERT
        execute("INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2, v2) VALUES ('pk1_1', 'pk2_1', 'c1_1', 'c2_2', 22)");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_2'"),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 22L));

        // update multiple columns with INSERT
        execute("INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2, v1, v2) VALUES ('pk1_1', 'pk2_1', 'c1_1', 'c2_2', 222, 222)");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_2'"),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 222, 222L));

        // delete a single partition
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // delete a first-level range (one-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 <= 'c1_1'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // delete a first-level range (two-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 > 'c1_1' AND c1 < 'c1_3'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // delete multiple rows
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L));

        // delete a second-level range (one-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 > 'c2_5'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L));

        // delete a second-level range (two-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 >= 'c2_3' AND c2 < 'c2_5'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L));

        // delete a single row
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_5'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L));

        // delete a single column
        execute("DELETE v1 FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", null, 3L));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));
    }

    @Test
    public void testRangeDeletionWithMulticolumnRestrictionsOnMutableTable() throws Throwable
    {
        // check for a clean state
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET v1 =  1, v2 =  1 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  2, v2 =  2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  3, v2 =  3 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  4, v2 =  4 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_3';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  5, v2 =  5 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_5';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  6, v2 =  6 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_6';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  7, v2 =  7 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  8, v2 =  8 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_2' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  9, v2 =  9 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 10, v2 = 10 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 11, v2 = 11 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_3';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 12, v2 = 12 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_4';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 13, v2 = 13 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_5';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 14, v2 = 14 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_3' AND c2 = 'c2_1';" +
                "APPLY BATCH");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_2", "c2_4", 12, 12L),
                row("pk1_1", "pk2_3", "c1_2", "c2_5", 13, 13L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns equality
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND (c1, c2) = ('c1_1', 'c2_5')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_1", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_2", "c2_4", 12, 12L),
                row("pk1_1", "pk2_3", "c1_2", "c2_5", 13, 13L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns with slice on both side of different length
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 >= 'c1_1' AND (c1, c2) <= ('c1_1', 'c2_5')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_2", "c2_4", 12, 12L),
                row("pk1_1", "pk2_3", "c1_2", "c2_5", 13, 13L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND (c1, c2) > ('c1_2', 'c2_3') AND (c1) < ('c1_3')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns with slice on both side of different length
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 >= 'c1_1' AND (c1, c2) <= ('c1_1', 'c2_5')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns with only top slice
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND (c1, c2) < ('c1_2', 'c2_2')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns with only bottom slice
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND (c1, c2) > ('c1_1', 'c2_1')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L),
                row("pk1_1", "pk2_3", "c1_3", "c2_1", 14, 14L));

        // Test deletion with multiple columns IN
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND (c1, c2) IN (('c1_2', 'c2_2'), ('c1_3', 'c2_1'), ('c1_4', 'c2_1'))");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_2", "pk2_1", "c1_1", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_2", "c2_1", 8, 8L),
                row("pk1_1", "pk2_3", "c1_2", "c2_3", 11, 11L));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));
    }

    @Test
    public void testDMLOperationsOnMutableNonCompositeTable() throws Throwable
    {
        // check for a clean state
        execute("TRUNCATE test_virtual_ks.vt4");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt4"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "INSERT INTO test_virtual_ks.vt4 (pk, v) VALUES ('pk1', 1);" +
                "INSERT INTO test_virtual_ks.vt4 (pk, v) VALUES ('pk2', 2);" +
                "INSERT INTO test_virtual_ks.vt4 (pk, v) VALUES ('pk3', 3);" +
                "INSERT INTO test_virtual_ks.vt4 (pk, v) VALUES ('pk4', 4);" +
                "APPLY BATCH");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                row("pk1", 1L),
                row("pk2", 2L),
                row("pk3", 3L),
                row("pk4", 4L));

         execute("UPDATE test_virtual_ks.vt4 SET v = 3 WHERE pk = 'pk1'");
         assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                 row("pk1", 3L),
                 row("pk2", 2L),
                 row("pk3", 3L),
                 row("pk4", 4L));

        // update a single columns with INSERT
         execute("INSERT INTO test_virtual_ks.vt4 (pk, v) VALUES ('pk1', 1);");
         assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                 row("pk1", 1L),
                 row("pk2", 2L),
                 row("pk3", 3L),
                 row("pk4", 4L));

         // update no column via INSERT
         execute("INSERT INTO test_virtual_ks.vt4 (pk) VALUES ('pk1');");
         assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                 row("pk1", 1L),
                 row("pk2", 2L),
                 row("pk3", 3L),
                 row("pk4", 4L));

         // insert new primary key only
         execute("INSERT INTO test_virtual_ks.vt4 (pk) VALUES ('pk5');");
         assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                 row("pk1", 1L),
                 row("pk2", 2L),
                 row("pk3", 3L),
                 row("pk4", 4L),
                 row("pk5", null));

        // delete a single partition
        execute("DELETE FROM test_virtual_ks.vt4 WHERE pk = 'pk2'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                row("pk1", 1L),
                row("pk3", 3L),
                row("pk4", 4L),
                row("pk5", null));

        // delete a single column
        execute("DELETE v FROM test_virtual_ks.vt4 WHERE pk = 'pk4'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt4"),
                row("pk1", 1L),
                row("pk3", 3L),
                row("pk4", null),
                row("pk5", null));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt4");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt4"));
    }

    @Test
    public void testInsertRowWithoutRegularColumnsOperationOnMutableTable() throws Throwable
    {
        // check for a clean state
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));

        // insert a primary key without columns
        execute("INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2) VALUES ('pk1_1', 'pk2_1', 'c1_1', 'c2_2')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_2'"),
                row("pk1_1", "pk2_1", "c1_1", "c2_2", null, null));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));
    }

    @Test
    public void testDeleteWithInOperationsOnMutableTable() throws Throwable
    {
        // check for a clean state
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET v1 =  1, v2 =  1 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  2, v2 =  2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 = 'c2_2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  3, v2 =  3 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  4, v2 =  4 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_2' AND c2 = 'c2_3';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  5, v2 =  5 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_1' AND c2 = 'c2_5';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  6, v2 =  6 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 = 'c1_2' AND c2 = 'c2_6';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  7, v2 =  7 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  8, v2 =  8 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 = 'c2_2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 =  9, v2 =  9 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_1' AND c2 = 'c2_1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 10, v2 = 10 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 = 'c1_2' AND c2 = 'c2_2';" +
                "APPLY BATCH");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_1", "pk2_1", "c1_1", "c2_1", 1, 1L),
                row("pk1_1", "pk2_2", "c1_1", "c2_2", 2, 2L),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_2", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_1", "c2_2", 8, 8L),
                row("pk1_1", "pk2_3", "c1_1", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // delete multiple partitions with IN
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 IN('pk2_1', 'pk2_2')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_2", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_1", "c2_2", 8, 8L),
                row("pk1_1", "pk2_3", "c1_1", "c2_1", 9, 9L),
                row("pk1_1", "pk2_3", "c1_2", "c2_2", 10, 10L));

        // delete multiple rows via first-level IN
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2_3' AND c1 IN('c1_1', 'c1_2')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_2", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", 6, 6L),
                row("pk1_2", "pk2_2", "c1_1", "c2_1", 7, 7L),
                row("pk1_2", "pk2_2", "c1_1", "c2_2", 8, 8L));

        // delete multiple rows via second-level IN
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_2' AND c1 = 'c1_1' AND c2 IN('c2_1', 'c2_2')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_1", 3, 3L),
                row("pk1_2", "pk2_1", "c1_2", "c2_3", 4, 4L),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", 6, 6L));

        // delete multiple rows with first-level IN and second-level range (one-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 IN('c1_1', 'c1_2') AND c2 <= 'c2_3'");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", 5, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", 6, 6L));

        // delete multiple rows via first-level and second-level IN
        execute("DELETE v1 FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2_1' AND c1 IN('c1_1', 'c1_2') AND c2 IN('c2_5', 'c2_6')");
        assertRowsIgnoringOrder(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk1_2", "pk2_1", "c1_1", "c2_5", null, 5L),
                row("pk1_2", "pk2_1", "c1_2", "c2_6", null, 6L));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));
    }

    @Test
    public void testInvalidDMLOperationsOnMutableTable() throws Throwable
    {
        // test that LOGGED batch doesn't allow virtual table updates
        assertInvalidMessage("Cannot include a virtual table statement in a logged batch",
                "BEGIN BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 1 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk1 = 'pk1_3' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "APPLY BATCH");

        // test that UNLOGGED batch doesn't allow mixing updates for regular and virtual tables
        createTable("CREATE TABLE %s (pk1 text, pk2 text, c1 text, c2 text, v1 int, v2 bigint, PRIMARY KEY ((pk1, pk2), c1, c2))");
        assertInvalidMessage("Mutations for virtual and regular tables cannot exist in the same batch",
                "BEGIN UNLOGGED BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 1 WHERE pk1 = 'pk1_1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "UPDATE %s                  SET v1 = 2 WHERE pk1 = 'pk1_2' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk1 = 'pk1_3' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2';" +
                        "APPLY BATCH");

        // test that TIMESTAMP is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Custom timestamp is not supported by virtual tables",
                "INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2, v1, v2) VALUES ('pk1', 'pk2', 'c1', 'c2', 1, 11) USING TIMESTAMP 123456789");
        assertInvalidMessage("Custom timestamp is not supported by virtual tables",
                "UPDATE test_virtual_ks.vt2 USING TIMESTAMP 123456789 SET v1 = 1, v2 = 11 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2'");

        // test that TTL is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                "INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2, v1, v2) VALUES ('pk1', 'pk2', 'c1', 'c2', 1, 11) USING TTL 86400");
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                "UPDATE test_virtual_ks.vt2 USING TTL 86400 SET v1 = 1, v2 = 11 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2'");

        // test that LWT is (currently) rejected with BATCH
        assertInvalidMessage("Conditional BATCH statements cannot include mutations for virtual tables",
                "BEGIN UNLOGGED BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2' IF v1 = 2;" +
                        "APPLY BATCH");

        // test that LWT is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                "INSERT INTO test_virtual_ks.vt2 (pk1, pk2, c1, c2, v1) VALUES ('pk1', 'pk2', 'c1', 'c2', 2) IF NOT EXISTS");
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 = 'c2' IF v1 = 2");

        // test that row DELETE without full primary key with equality relation is (currently) rejected
        assertInvalidMessage("Some partition key parts are missing: pk2",
                "DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND c1 = 'c1' AND c2 > 'c2'");
        assertInvalidMessage("Only EQ and IN relation are supported on the partition key (unless you use the token() function) for DELETE statements",
                "DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 > 'pk2' AND c1 = 'c1' AND c2 > 'c2'");
        assertInvalidMessage("KEY column \"c2\" cannot be restricted as preceding column \"c1\" is not restricted",
                "DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c2 > 'c2'");
        assertInvalidMessage("Clustering column \"c2\" cannot be restricted (preceding column \"c1\" is restricted by a non-EQ relation)",
                "DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 > 'c1' AND c2 > 'c2'");
        assertInvalidMessage("DELETE statements must restrict all PRIMARY KEY columns with equality relations",
                "DELETE FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 > 'c2' IF v1 = 2");

        // test that column DELETE without full primary key with equality relation is (currently) rejected
        assertInvalidMessage("Range deletions are not supported for specific columns",
                "DELETE v1 FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1'");
        assertInvalidMessage("Range deletions are not supported for specific columns",
                "DELETE v1 FROM test_virtual_ks.vt2 WHERE pk1 = 'pk1' AND pk2 = 'pk2' AND c1 = 'c1' AND c2 > 'c2'");
    }

    @Test
    public void testInvalidDMLOperationsOnReadOnlyTable() throws Throwable
    {
        assertInvalidMessage("Modification is not supported by table test_virtual_ks.vt1",
                "INSERT INTO test_virtual_ks.vt1 (pk, c, v1, v2) VALUES ('pk1_1', 'ck1_1', 11, 11)");

        assertInvalidMessage("Modification is not supported by table test_virtual_ks.vt1",
                "UPDATE test_virtual_ks.vt1 SET v1 = 11, v2 = 11 WHERE pk = 'pk1_1' AND c = 'ck1_1'");

        assertInvalidMessage("Modification is not supported by table test_virtual_ks.vt1",
                "DELETE FROM test_virtual_ks.vt1 WHERE pk = 'pk1_1' AND c = 'ck1_1'");

        assertInvalidMessage("Error during truncate: Truncation is not supported by table test_virtual_ks.vt1",
                "TRUNCATE TABLE test_virtual_ks.vt1");
    }

    @Test
    public void testInvalidDDLOperationsOnVirtualKeyspaceAndReadOnlyTable() throws Throwable
    {
        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "DROP KEYSPACE test_virtual_ks");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "ALTER KEYSPACE test_virtual_ks WITH durable_writes = false");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TABLE test_virtual_ks.test (id int PRIMARY KEY)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TYPE test_virtual_ks.type (id int)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "DROP TABLE test_virtual_ks.vt1");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "ALTER TABLE test_virtual_ks.vt1 DROP v1");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE INDEX ON test_virtual_ks.vt1 (v1)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE MATERIALIZED VIEW test_virtual_ks.mvt1 AS SELECT c, v1 FROM test_virtual_ks.vt1 WHERE c IS NOT NULL PRIMARY KEY(c)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TRIGGER test_trigger ON test_virtual_ks.vt1 USING '" + TestTrigger.class.getName() + '\'');
    }

    /**
     * Noop trigger for audit log testing
     */
    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition update)
        {
            return null;
        }
    }

    @Test
    public void testMBeansMethods() throws Throwable
    {
        StorageServiceMBean mbean = StorageService.instance;

        assertJMXFails(() -> mbean.forceKeyspaceCompaction(false, KS_NAME));
        assertJMXFails(() -> mbean.forceKeyspaceCompaction(false, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.scrub(true, true, true, true, 1, KS_NAME));
        assertJMXFails(() -> mbean.scrub(true, true, true, true, 1, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.verify(true, KS_NAME));
        assertJMXFails(() -> mbean.verify(true, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.upgradeSSTables(KS_NAME, false, 1));
        assertJMXFails(() -> mbean.upgradeSSTables(KS_NAME, false, 1, VT1_NAME));

        assertJMXFails(() -> mbean.garbageCollect("ROW", 1, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.forceKeyspaceFlush(KS_NAME));
        assertJMXFails(() -> mbean.forceKeyspaceFlush(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.truncate(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.loadNewSSTables(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.getAutoCompactionStatus(KS_NAME));
        assertJMXFails(() -> mbean.getAutoCompactionStatus(KS_NAME, VT1_NAME));
    }

    @Test
    public void testDisallowedFilteringOnRegularColumn() throws Throwable
    {
        try
        {
            executeNet(format("SELECT * FROM %s.%s WHERE v2 = 5", KS_NAME, VT5_NAME));
            fail(format("should fail as %s.%s is not allowed to be filtered on implicitly.", KS_NAME, VT5_NAME));
        }
        catch (InvalidQueryException ex)
        {
            assertTrue(ex.getMessage().contains("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance"));
        }
    }

    @Test
    public void testDisallowedFilteringOnClusteringColumn() throws Throwable
    {
        try
        {
            executeNet(format("SELECT * FROM %s.%s WHERE c = 'abc'", KS_NAME, VT5_NAME));
            fail(format("should fail as %s.%s is not allowed to be filtered on implicitly.", KS_NAME, VT5_NAME));
        }
        catch (InvalidQueryException ex)
        {
            assertTrue(ex.getMessage().contains("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance"));
        }
    }

    @Test
    public void testAllowedFilteringOnRegularColumn() throws Throwable
    {
        executeNet(format("SELECT * FROM %s.%s WHERE v2 = 5", KS_NAME, VT1_NAME));
    }

    @Test
    public void testAllowedFilteringOnClusteringColumn() throws Throwable
    {
        executeNet(format("SELECT * FROM %s.%s WHERE c = 'abc'", KS_NAME, VT1_NAME));
    }

    @FunctionalInterface
    private static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }

    private void assertJMXFails(ThrowingRunnable r) throws Throwable
    {
        try
        {
            r.run();
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot perform any operations against virtual keyspace " + KS_NAME, e.getMessage());
        }
    }
}
