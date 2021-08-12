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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.AbstractWritableVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class VirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "test_virtual_ks";
    private static final String VT1_NAME = "vt1";
    private static final String VT2_NAME = "vt2";
    private static final String VT3_NAME = "vt3";

    private static class WritableVirtualTable extends AbstractWritableVirtualTable
    {
        private final Map<String, SortedMap<String,Pair<Integer, Long>>> backingMap = new ConcurrentHashMap<>();

        WritableVirtualTable(String keyspaceName, String tableName)
        {
            super(TableMetadata.builder(keyspaceName, tableName)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("pk", UTF8Type.instance)
                               .addClusteringColumn("c", UTF8Type.instance)
                               .addRegularColumn("v1", Int32Type.instance)
                               .addRegularColumn("v2", LongType.instance)
                               .build());
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet data = new SimpleDataSet(metadata());
            backingMap.forEach((pk, clusteringMap) ->
                    clusteringMap.forEach((c, p) -> data.row(pk, c)
                            .column("v1", p.left)
                            .column("v2", p.right)));
            return data;
        }

        @Override
        protected void applyPartitionDeletion(DecoratedKey partitionKey, DeletionTime partitionDeletion) {
            String key = (String) metadata.partitionKeyType.compose(partitionKey.getKey());
            backingMap.remove(key);
        }

        @Override
        protected void applyRangeTombstone(DecoratedKey partitionKey, RangeTombstone rt) {
            String key = (String) metadata.partitionKeyType.compose(partitionKey.getKey());

            ClusteringBound<?> start = rt.deletedSlice().start();
            String startClusteringColumn;
            if (Arrays.isNullOrEmpty(start.getBufferArray()))
                startClusteringColumn = null;
            else
                startClusteringColumn = (String) metadata.clusteringColumns().get(0).type.compose(start.getBufferArray()[0]);

            ClusteringBound<?> end = rt.deletedSlice().end();
            String endClusteringColumn;
            if (Arrays.isNullOrEmpty(end.getBufferArray()))
                endClusteringColumn = null;
            else
                endClusteringColumn = (String) metadata.clusteringColumns().get(0).type.compose(end.getBufferArray()[0]);

            SortedMap<String, Pair<Integer, Long>> clusteringColumnsMap = backingMap.computeIfAbsent(key, k -> new TreeMap<>());
            if (startClusteringColumn != null && endClusteringColumn != null)
                if (start.isInclusive() && end.isInclusive())
                    Maps.filterKeys(clusteringColumnsMap, Range.closed(startClusteringColumn, endClusteringColumn)).clear();
                else if (start.isExclusive() && end.isInclusive())
                    Maps.filterKeys(clusteringColumnsMap, Range.openClosed(startClusteringColumn, endClusteringColumn)).clear();
                else if (start.isInclusive() && end.isExclusive())
                    Maps.filterKeys(clusteringColumnsMap, Range.closedOpen(startClusteringColumn, endClusteringColumn)).clear();
                else
                    Maps.filterKeys(clusteringColumnsMap, Range.open(startClusteringColumn, endClusteringColumn)).clear();
            else if (startClusteringColumn == null && endClusteringColumn != null)
                if (end.isInclusive())
                    Maps.filterKeys(clusteringColumnsMap, Range.atMost(endClusteringColumn)).clear();
                else
                    Maps.filterKeys(clusteringColumnsMap, Range.lessThan(endClusteringColumn)).clear();
            else if (startClusteringColumn != null && endClusteringColumn == null)
                if (start.isInclusive())
                    Maps.filterKeys(clusteringColumnsMap, Range.atLeast(startClusteringColumn)).clear();
                else
                    Maps.filterKeys(clusteringColumnsMap, Range.greaterThan(startClusteringColumn)).clear();
            else if (startClusteringColumn == null && endClusteringColumn == null)
                throw new IllegalStateException("Both start and end range tombstone values cannot be empty");
        }

        @Override
        protected void applyRowDeletion(DecoratedKey partitionKey, Clustering<?> clustering, Row.Deletion rowDeletion) {
            String key = (String) metadata.partitionKeyType.compose(partitionKey.getKey());
            String clusteringKey = (String) metadata.clusteringColumns().get(0).type.compose(clustering.bufferAt(0));

            SortedMap<String, Pair<Integer, Long>> clusteringColumnsMap = backingMap.computeIfAbsent(key, k -> new TreeMap<>());
            clusteringColumnsMap.remove(clusteringKey);
        }

        @Override
        protected void applyColumnDeletion(DecoratedKey partitionKey, Clustering<?> clustering, Cell<?> cell) {
            String key = (String) metadata.partitionKeyType.compose(partitionKey.getKey());
            String clusteringKey = (String) metadata.clusteringColumns().get(0).type.compose(clustering.bufferAt(0));
            String columnName = cell.column().name.toCQLString();
            Pair<Integer, Long> p = backingMap.getOrDefault(key, Collections.emptySortedMap()).get(clusteringKey);

            if (p != null) {
                if (columnName.equals("v1"))
                    backingMap.get(key).put(clusteringKey, Pair.create(null, p.right));
                else
                    backingMap.get(key).put(clusteringKey, Pair.create(p.left, null));
            }
        }

        @Override
        protected void applyUpdate(DecoratedKey partitionKey, Clustering<?> clustering, Cell<?> cell) {
            String key = (String) metadata.partitionKeyType.compose(partitionKey.getKey());
            String clusteringKey = (String) metadata.clusteringColumns().get(0).type.compose(clustering.bufferAt(0));
            String columnName = cell.column().name.toCQLString();
            SortedMap<String, Pair<Integer, Long>> clusteringColumnsMap = backingMap.computeIfAbsent(key, k -> new TreeMap<>());
            clusteringColumnsMap.compute(clusteringKey, (k, p) -> {
                if ("v1".equals(columnName))
                {
                    Integer cellValue = (Integer) metadata.getColumn(cell.column().name).cellValueType().compose(cell.buffer());
                    return Pair.create(cellValue, p != null ? p.right : null);
                }
                else
                {
                    Long cellValue = (Long) metadata.getColumn(cell.column().name).cellValueType().compose(cell.buffer());
                    return Pair.create(p != null ? p.left : null, cellValue);
                }
            });
        }

        @Override
        public void truncate() {
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
        VirtualTable vt2 = new WritableVirtualTable(KS_NAME, VT2_NAME);

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
    public void testQueries() throws Throwable
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
    public void testQueriesOnTableWithMultiplePks() throws Throwable
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
    public void testModificationsOnWritableTable() throws Throwable
    {
        // check for clean state
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET v1 = 1, v2 = 1 WHERE pk ='pk1' AND c = 'c1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 2, v2 = 2 WHERE pk ='pk1' AND c = 'c2';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 3, v2 = 3 WHERE pk ='pk2' AND c = 'c1';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 4, v2 = 4 WHERE pk ='pk2' AND c = 'c3';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 5, v2 = 5 WHERE pk ='pk2' AND c = 'c5';" +
                "UPDATE test_virtual_ks.vt2 SET v1 = 6, v2 = 6 WHERE pk ='pk2' AND c = 'c6';" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                   row("pk1", "c1", 1, 1L),
                   row("pk1", "c2", 2, 2L),
                   row("pk2", "c1", 3, 3L),
                   row("pk2", "c3", 4, 4L),
                   row("pk2", "c5", 5, 5L),
                   row("pk2", "c6", 6, 6L));

        // update a single value with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET v1 = 11 WHERE pk ='pk1' AND c = 'c1'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk = 'pk1' AND c = 'c1'"),
                   row("pk1", "c1", 11, 1L));

        // update multiple values with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET v1 = 111, v2 = 111 WHERE pk ='pk1' AND c = 'c1'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk = 'pk1' AND c = 'c1'"),
                row("pk1", "c1", 111, 111L));

        // update a single value with INSERT
        execute("INSERT INTO test_virtual_ks.vt2 (pk, c, v2) VALUES ('pk1', 'c2', 22)");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk = 'pk1' AND c = 'c2'"),
                   row("pk1", "c2", 2, 22L));

        // update multiple values with INSERT
        execute("INSERT INTO test_virtual_ks.vt2 (pk, c, v1, v2) VALUES ('pk1', 'c2', 222, 222)");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE pk = 'pk1' AND c = 'c2'"),
                row("pk1", "c2", 222, 222L));

        // delete a single partition
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk ='pk1'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk2", "c1", 3, 3L),
                row("pk2", "c3", 4, 4L),
                row("pk2", "c5", 5, 5L),
                row("pk2", "c6", 6, 6L));

        // delete a range (two-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk ='pk2' AND c > 'c1' AND c < 'c5'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk2", "c1", 3, 3L),
                row("pk2", "c5", 5, 5L),
                row("pk2", "c6", 6, 6L));

        // delete a range (one-sided limit)
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk ='pk2' AND c < 'c5'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk2", "c5", 5, 5L),
                row("pk2", "c6", 6, 6L));

        // delete a single row
        execute("DELETE FROM test_virtual_ks.vt2 WHERE pk ='pk2' AND c = 'c5'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk2", "c6", 6, 6L));

        // delete a single column
        execute("DELETE v1 FROM test_virtual_ks.vt2 WHERE pk ='pk2' AND c = 'c6'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                row("pk2", "c6", null, 6L));

        // truncate
        execute("TRUNCATE test_virtual_ks.vt2");
        assertEmpty(execute("SELECT * FROM test_virtual_ks.vt2"));
    }

    @Test
    public void testInvalidDMLOperationsOnWritableTable() throws Throwable
    {
        // test that LOGGED batch doesn't allow virtual table updates
        assertInvalidMessage("Cannot include a virtual table statement in a logged batch",
                "BEGIN BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 1 WHERE pk ='pk1' AND c = 'c1';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 2 WHERE pk ='pk2' AND c = 'c2';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk ='pk3' AND c = 'c1';" +
                        "APPLY BATCH");

        // test that UNLOGGED batch doesn't allow mixing updates for regular and virtual tables
        createTable("CREATE TABLE %s (pk text, c text, v1 int, v2 bigint, PRIMARY KEY ((pk), c))");
        assertInvalidMessage("Mutations for virtual and regular tables cannot exist in the same batch",
                "BEGIN UNLOGGED BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 1 WHERE pk ='pk1' AND c = 'c1';" +
                        "UPDATE %s                  SET v1 = 2 WHERE pk ='pk2' AND c = 'c2';" +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk ='pk3' AND c = 'c1';" +
                        "APPLY BATCH");

        // test that TTL is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                "INSERT INTO test_virtual_ks.vt2 (pk, c, v1, v2) VALUES ('pk1', 'c1', 1, 11) USING TTL 86400");
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                "UPDATE test_virtual_ks.vt2 USING TTL 86400 SET v1 = 1, v2 = 11 WHERE pk ='pk1' AND c = 'c1'");

        // test that LWT is (currently) rejected with virtual tables in batches
        assertInvalidMessage("Conditional BATCH statements cannot include mutations for virtual tables",
                "BEGIN UNLOGGED BATCH " +
                        "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk = 'pk1' AND c = 'c1' IF v1 = 2;" +
                        "APPLY BATCH");

        // test that LWT is (currently) rejected with virtual tables in UPDATEs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                "UPDATE test_virtual_ks.vt2 SET v1 = 3 WHERE pk = 'pk1' AND c = 'c1' IF v1 = 2");

        // test that LWT is (currently) rejected with virtual tables in INSERTs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                "INSERT INTO test_virtual_ks.vt2 (pk, c, v1) VALUES ('pk1', 'c1', 2) IF NOT EXISTS");
    }

    @Test
    public void testInvalidDMLOperationsOnReadOnlyTable() throws Throwable
    {
        assertInvalidMessage("Modification is not supported by table test_virtual_ks.vt1",
                "INSERT INTO test_virtual_ks.vt1 (pk, c, v1, v2) " +
                        "VALUES ('pk11', 'ck11', 11, 11)");

        assertInvalidMessage("Modification is not supported by table test_virtual_ks.vt1",
                "DELETE FROM test_virtual_ks.vt1 WHERE pk ='pk11' AND c = 'ck11'");

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
