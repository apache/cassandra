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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.AbstractIteratingTable;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.triggers.ITrigger;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class VirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "test_virtual_ks";
    private static final String VT1_NAME = "vt1";
    private static final String VT2_NAME = "vt2";

    private static class WritableVirtualTable extends AbstractVirtualTable
    {
        private final ColumnMetadata valueColumn;
        private final Map<String, Integer> backingMap = new HashMap<>();

        WritableVirtualTable(String keyspaceName, String tableName)
        {
            super(TableMetadata.builder(keyspaceName, tableName)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("key", UTF8Type.instance)
                               .addRegularColumn("value", Int32Type.instance)
                               .build());
            valueColumn = metadata().regularColumns().getSimple(0);
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet data = new SimpleDataSet(metadata());
            backingMap.forEach((key, value) -> data.row(key).column("value", value));
            return data;
        }

        @Override
        public void apply(PartitionUpdate update)
        {
            String key = (String) metadata().partitionKeyType.compose(update.partitionKey().getKey());
            update.forEach(row ->
            {
                Integer value = Int32Type.instance.compose(row.getCell(valueColumn).buffer());
                backingMap.put(key, value);
            });
        }
    }

    private static DecoratedKey makeKey(TableMetadata metadata, String... partitionKeyValues)
    {
        ByteBuffer partitionKey = partitionKeyValues.length == 1
                                ?  UTF8Type.instance.decompose(partitionKeyValues[0])
                                : ((CompositeType) metadata.partitionKeyType).decompose(partitionKeyValues);
        return metadata.partitioner.decorateKey(partitionKey);
    }

    @BeforeClass
    public static void setUpClass()
    {
        TableMetadata vt1Metadata =
            TableMetadata.builder(KS_NAME, VT1_NAME)
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
        TableMetadata vt3metadata = TableMetadata.builder(KS_NAME, "vt3")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn("pk", UTF8Type.instance)
                .addClusteringColumn("c", UTF8Type.instance)
                .addRegularColumn("v1", Int32Type.instance)
                .addRegularColumn("v2", LongType.instance)
                .build();
        final List<DecoratedKey> vt3keys = Lists.newArrayList(
                makeKey(vt3metadata, "pk1"),
                makeKey(vt3metadata, "pk2"));

        VirtualTable vt3 = new AbstractIteratingTable(vt3metadata)
        {
            protected Iterator<DecoratedKey> getPartitionKeys(DataRange dataRange)
            {
                return vt3keys.iterator();
            }

            protected Iterator<Row> getRows(DecoratedKey key, ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
            {
                RegularAndStaticColumns columns = columnFilter.queriedColumns();
                String value = metadata.partitionKeyType.getString(key.getKey());
                int pk = Integer.parseInt(value.substring(2));
                List<Row> rows = Lists.newArrayList(
                row("c1").add("v1", 10 * pk + 1).add("v2", (long) (10 * pk + 1)).build(columns),
                row("c2").add("v1", 10 * pk + 2).add("v2", (long) (10 * pk + 2)).build(columns),
                row("c3").add("v1", 10 * pk + 3).add("v2", (long) (10 * pk + 3)).build(columns));
                if (clusteringFilter.isReversed())
                {
                    Collections.reverse(rows);
                }
                return rows.iterator();
            }

            protected boolean hasKey(DecoratedKey partitionKey)
            {
                return vt3keys.contains(partitionKey);
            }
        };

        /*
         * vt4 partition key is set of all integers, with rowsPerVT4Partition rows in each value is
         * primary key + clustering key. Reverse order not supported for these tests as its just to test large
         * sets
         */
        TableMetadata vt4metadata = TableMetadata.builder(KS_NAME, "vt4")
                                                 .kind(TableMetadata.Kind.VIRTUAL)
                                                 .partitioner(new LocalPartitioner(Int32Type.instance))
                                                 .addPartitionKeyColumn("pk", Int32Type.instance)
                                                 .addClusteringColumn("ck", Int32Type.instance)
                                                 .addRegularColumn("pk_plus_ck", LongType.instance)
                                                 .build();

        VirtualTable vt4 = new AbstractIteratingTable(vt4metadata)
        {
            protected Iterator<DecoratedKey> getPartitionKeys(DataRange dataRange)
            {
                AtomicInteger current = new AtomicInteger();
                if (dataRange.startKey() instanceof DecoratedKey)
                {
                    int start = Int32Type.instance.compose(((DecoratedKey) dataRange.startKey()).getKey());
                    current.set(start - 1);
                }
                return new AbstractIterator<DecoratedKey>()
                {
                    protected DecoratedKey computeNext()
                    {
                        int next = current.incrementAndGet();
                        DecoratedKey key = vt4metadata.partitioner.decorateKey(Int32Type.instance.decompose(next));
                        return key;
                    }
                };
            }

            protected Iterator<Row> getRows(DecoratedKey key, Slice slice, ColumnFilter columnFilter)
            {
                int startInclusive = 1;
                int endExclusive = rowsPerVT4Partition.get() + 1;
                if (!slice.equals(Slice.ALL)) {
                    ClusteringBound start = slice.start();
                    if (start.clustering().size() == 1)
                    {
                        startInclusive = Int32Type.instance.compose(start.clustering().get(0));
                        if (!start.isInclusive()) startInclusive ++;
                    }
                    ClusteringBound end = slice.end();
                    if (end.clustering().size() == 1)
                    {
                        endExclusive = Int32Type.instance.compose(end.clustering().get(0));
                        if (!end.isInclusive()) endExclusive ++;
                    }
                }
                int pk = (Integer) metadata.partitionKeyType.compose(key.getKey());
                // Note: only goes ascending
                return IntStream.range(startInclusive, endExclusive).mapToObj(ck ->
                     row(ck)
                           .add("pk_plus_ck", (long) pk + ck)
                           .build(columnFilter.queriedColumns())
                ).iterator();
            }

            protected Iterator<Row> getRows(DecoratedKey key, ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
            {
                Iterator<Slice> slices = clusteringFilter.getSlices(vt4metadata).iterator();
                if (clusteringFilter.isReversed()) throw new IllegalArgumentException("Cannot ORDER BY on vt4");
                return new AbstractIterator<Row>()
                {
                    Iterator<Row> current = Collections.emptyIterator();
                    protected Row computeNext()
                    {
                        while(slices.hasNext() && !current.hasNext())
                            current = getRows(key, slices.next(), columnFilter);
                        if (current.hasNext())
                            return current.next();
                        return endOfData();
                    }
                };
            }

            protected boolean hasKey(DecoratedKey key)
            {
                int pk = (Integer) metadata.partitionKeyType.compose(key.getKey());
                return pk >= 1;
            }
        };
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(vt1, vt2, vt3, vt4)));

        CQLTester.setUpClass();
    }

    private static final AtomicInteger rowsPerVT4Partition = new AtomicInteger(1);

    /**
     * 2 million rows in single partition fits on heap
     */
    @Test
    public void testHugePartition() throws Throwable
    {
        rowsPerVT4Partition.set(2_000_000);
        ResultSet res = executeNetWithPaging("SELECT * FROM test_virtual_ks.vt4 WHERE pk = 1", 100);
        Iterator<com.datastax.driver.core.Row> iter = res.iterator();
        int total = 0;
        while(iter.hasNext()) {
            total ++;
            com.datastax.driver.core.Row next = iter.next();
            Assert.assertEquals(1, next.getInt("pk"));
            Assert.assertEquals(total, next.getInt("ck"));
            Assert.assertEquals((long) total + 1, next.getLong("pk_plus_ck"));
        }
        Assert.assertEquals(2_000_000, total);
    }

    /**
     * Should be able to read from vt4 indefinitely, we will cap with 2 seconds though. Should page through without
     * OOMing.
     */
    @Test
    public void testLotsOfPartition() throws Throwable
    {
        rowsPerVT4Partition.set(1);
        ResultSet res = executeNetWithPaging("SELECT * FROM test_virtual_ks.vt4", 100);
        Iterator<com.datastax.driver.core.Row> iter = res.iterator();
        int total = 0;
        long start = System.currentTimeMillis();
        while(iter.hasNext() && (System.currentTimeMillis() - start) < 2000) {
            total ++;
            com.datastax.driver.core.Row next = iter.next();
            Assert.assertEquals(total, next.getInt("pk"));
            Assert.assertEquals(1, next.getInt("ck"));
            Assert.assertEquals((long) total + 1, next.getLong("pk_plus_ck"));
        }
        logger.info("Read *" +total+ "* partitions");
        Assert.assertTrue(total > 1);
    }

    @Test
    public void testLargeEverything() throws Throwable
    {
        rowsPerVT4Partition.set(13_217);
        ResultSet res = executeNetWithPaging("SELECT * FROM test_virtual_ks.vt4", 137);
        Iterator<com.datastax.driver.core.Row> iter = res.iterator();
        int pk = 1;
        int ck = 1;
        while(iter.hasNext() && pk < 20) {
            com.datastax.driver.core.Row next = iter.next();
            Assert.assertEquals(pk, next.getInt("pk"));
            Assert.assertEquals(ck, next.getInt("ck"));
            Assert.assertEquals((long) ck + pk, next.getLong("pk_plus_ck"));
            if (ck == rowsPerVT4Partition.get())
            {
                ck = 1;
                pk ++;
            } else
            {
                ck++;
            }
        }
        logger.info("Read *" +pk+ " * " + ck + " * rows");
    }

    public void testQueries(String table) throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks."+table+" WHERE pk = 'UNKNOWN'"));

        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks."+table+" WHERE pk = 'pk1' AND c = 'UNKNOWN'"));

        // Test DISTINCT query
        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks."+table),
                      row("pk1"),
                      row("pk2"));

        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks."+table+" WHERE token(pk) > token('pk1')"),
                      row("pk2"));

        // Test single partition queries
        assertRowsNet(executeNet("SELECT v1, v2 FROM test_virtual_ks."+table+" WHERE pk = 'pk1' AND c = 'c1'"),
                      row(11, 11L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks."+table+" WHERE pk = 'pk1' AND c IN ('c1', 'c2')"),
                      row("c1", 11, 11L),
                      row("c2", 12, 12L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks."+table+" WHERE pk = 'pk1' AND c IN ('c2', 'c1') ORDER BY c DESC"),
                      row("c2", 12, 12L),
                      row("c1", 11, 11L));

        // Test multi-partition queries
        assertRows(execute("SELECT * FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row("pk1", "c1", 11, 11L),
                   row("pk1", "c2", 12, 12L),
                   row("pk2", "c1", 21, 21L),
                   row("pk2", "c2", 22, 22L));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC"),
                   row("pk1", "c2", 12),
                   row("pk2", "c2", 22),
                   row("pk1", "c1", 11),
                   row("pk2", "c1", 21));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC LIMIT 1"),
                   row("pk1", "c2", 12));

        assertRows(execute("SELECT c, v1, v2 FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1' , 'c3') ORDER BY c DESC PER PARTITION LIMIT 1"),
                   row("c3", 13, 13L),
                   row("c3", 23, 23L));

        assertRows(execute("SELECT count(*) FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row(4L));

        for (int pageSize = 1; pageSize < 5; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT pk, c, v1, v2 FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c1", 21, 21L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') LIMIT 2", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks."+table+" WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row(4L));
        }

        // Test range queries
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks."+table+" WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks."+table+" WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks."+table+" WHERE token(pk) <= token('pk2') AND c > 'c1' PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks."+table+" WHERE token(pk) = token('pk2') AND c < 'c3' ALLOW FILTERING", pageSize),
                          row(2L));
        }
    }

    @Test
    public void testInMemoryQueries() throws Throwable
    {
        testQueries("vt1");
    }

    @Test
    public void testIteratorTable() throws Throwable
    {
        testQueries("vt3");
    }

    @Test
    public void testModifications() throws Throwable
    {
        // check for clean state
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1';" +
                "UPDATE test_virtual_ks.vt2 SET value = 2 WHERE key ='pk2';" +
                "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3';" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                   row("pk1", 1),
                   row("pk2", 2),
                   row("pk3", 3));

        // test that LOGGED batches don't allow virtual table updates
        assertInvalidMessage("Cannot include a virtual table statement in a logged batch",
                             "BEGIN BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1';" +
                             "UPDATE test_virtual_ks.vt2 SET value = 2 WHERE key ='pk2';" +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3';" +
                             "APPLY BATCH");

        // test that UNLOGGED batch doesn't allow mixing updates for regular and virtual tables
        createTable("CREATE TABLE %s (key text PRIMARY KEY, value int)");
        assertInvalidMessage("Mutations for virtual and regular tables cannot exist in the same batch",
                             "BEGIN UNLOGGED BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1'" +
                             "UPDATE %s                  SET value = 2 WHERE key ='pk2'" +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3'" +
                             "APPLY BATCH");

        // update a single value with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET value = 11 WHERE key ='pk1'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE key = 'pk1'"),
                   row("pk1", 11));

        // update a single value with INSERT
        executeNet("INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk2', 22)");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE key = 'pk2'"),
                   row("pk2", 22));

        // test that deletions are (currently) rejected
        assertInvalidMessage("Virtual tables don't support DELETE statements",
                             "DELETE FROM test_virtual_ks.vt2 WHERE key ='pk1'");

        // test that TTL is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                             "INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk1', 11) USING TTL 86400");
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                             "UPDATE test_virtual_ks.vt2 USING TTL 86400 SET value = 11 WHERE key ='pk1'");

        // test that LWT is (currently) rejected with virtual tables in batches
        assertInvalidMessage("Conditional BATCH statements cannot include mutations for virtual tables",
                             "BEGIN UNLOGGED BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3' IF value = 2;" +
                             "APPLY BATCH");

        // test that LWT is (currently) rejected with virtual tables in UPDATEs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3' IF value = 2");

        // test that LWT is (currently) rejected with virtual tables in INSERTs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                             "INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk2', 22) IF NOT EXISTS");
    }

    @Test
    public void testInvalidDDLOperations() throws Throwable
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

        assertInvalidMessage("Error during truncate: Cannot truncate virtual tables",
                             "TRUNCATE TABLE test_virtual_ks.vt1");

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
