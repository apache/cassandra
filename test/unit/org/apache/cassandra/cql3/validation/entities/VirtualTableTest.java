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
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.junit.BeforeClass;
import org.junit.Test;

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
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class VirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "test_virtual_ks";
    private static final String VT1_NAME = "vt1";
    private static final String VT2_NAME = "vt2";
    private static final String VT3_NAME = "vt3";

    private static class MutableVirtualTable extends AbstractMutableVirtualTable
    {
        // <pk1, pk2> -> c1 -> c2 -> <v1, v2>
        private final Map<Pair<String, String>, SortedMap<String, SortedMap<String, Pair<Integer, Long>>>> backingMap = new ConcurrentHashMap<>();

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
                    c2Map.forEach((c2, valuePair) -> data.row(pkPair.left, pkPair.right, c1, c2)
                            .column("v1", valuePair.left)
                            .column("v2", valuePair.right))));
            return data;
        }

        @Override
        protected void applyPartitionDeletion(Object[] partitionKeyColumnValues)
        {
            String pk1 = (String) partitionKeyColumnValues[0];
            String pk2 = (String) partitionKeyColumnValues[1];
            backingMap.remove(Pair.create(pk1, pk2));
        }

        @Override
        protected void applyRangeTombstone(Object[] partitionKeyColumnValues,
                                           Comparable<?>[] clusteringColumnValuesPrefix,
                                           Range<Comparable<?>> range)
        {
            Pair<String, String> pkPair = Pair.create((String) partitionKeyColumnValues[0], (String) partitionKeyColumnValues[1]);

            if (clusteringColumnValuesPrefix.length > 0)
            {
                SortedMap<String, Pair<Integer, Long>> clusteringColumnsMap = backingMap
                        .computeIfAbsent(pkPair, ignored -> new TreeMap<>())
                        .computeIfAbsent((String) clusteringColumnValuesPrefix[0], ignored -> new TreeMap<>());

                Maps.filterKeys(clusteringColumnsMap, range::contains).clear();
            }
            else
            {
                SortedMap<String, SortedMap<String, Pair<Integer, Long>>> clusteringColumnsMap = backingMap
                        .computeIfAbsent(pkPair, ignored -> new TreeMap<>());

                Maps.filterKeys(clusteringColumnsMap, range::contains).clear();
            }
        }

        @Override
        protected void applyRowWithoutRegularColumnsInsertion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues) {
            Pair<String, String> pkPair = Pair.create((String) partitionKeyColumnValues[0], (String) partitionKeyColumnValues[1]);
            String c1 = (String) clusteringColumnValues[0];
            String c2 = (String) clusteringColumnValues[1];

            backingMap.computeIfAbsent(pkPair, ignored -> new TreeMap<>())
                    .computeIfAbsent(c1, ignored -> new TreeMap<>())
                    .put(c2, Pair.create(null, null));
        }

        @Override
        protected void applyRowDeletion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues)
        {
            Pair<String, String> pkPair = Pair.create((String) partitionKeyColumnValues[0], (String) partitionKeyColumnValues[1]);
            String c1 = (String) clusteringColumnValues[0];
            String c2 = (String) clusteringColumnValues[1];

            backingMap.computeIfAbsent(pkPair, ignored -> new TreeMap<>())
                    .computeIfAbsent(c1, ignored -> new TreeMap<>())
                    .remove(c2);
        }

        @Override
        protected void applyColumnDeletion(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues, String columnName)
        {
            Pair<String, String> pkPair = Pair.create((String) partitionKeyColumnValues[0], (String) partitionKeyColumnValues[1]);
            String c1 = (String) clusteringColumnValues[0];
            String c2 = (String) clusteringColumnValues[1];
            Pair<Integer, Long> p = backingMap.computeIfAbsent(pkPair, ignored -> new TreeMap<>())
                    .computeIfAbsent(c1, ignored -> new TreeMap<>())
                    .get(c2);

            if (p != null)
            {
                @SuppressWarnings("SuspiciousNameCombination")
                Pair<Integer, Long> valuePair = columnName.equals("v1")
                        ? Pair.create(null, p.right) : Pair.create(p.left, null);
                backingMap.get(pkPair).get(c1).put(c2, valuePair);
            }
        }

        @Override
        protected void applyColumnUpdate(Object[] partitionKeyColumnValues, Comparable<?>[] clusteringColumnValues,
                                         String columnName, Object columnValue)
        {
            Pair<String, String> pkPair = Pair.create((String) partitionKeyColumnValues[0], (String) partitionKeyColumnValues[1]);
            String c1 = (String) clusteringColumnValues[0];
            String c2 = (String) clusteringColumnValues[1];
            backingMap.computeIfAbsent(pkPair, ignored -> new TreeMap<>())
                    .computeIfAbsent(c1, ignored -> new TreeMap<>())
                    .compute(c2, (ignored, p) -> "v1".equals(columnName)
                            ? Pair.create((Integer) columnValue, p != null ? p.right : null)
                            : Pair.create(p != null ? p.left : null, (Long) columnValue));
        }

        @Override
        public void truncate()
        {
            backingMap.clear();
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
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

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(vt1, vt2, vt3)));

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
    public void testDMLOperationsOnMutableTable() throws Throwable
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
