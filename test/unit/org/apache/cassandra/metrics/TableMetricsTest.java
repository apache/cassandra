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

package org.apache.cassandra.metrics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.apache.cassandra.io.sstable.Downsampling.BASE_SAMPLING_LEVEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TableMetricsTest
{
    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "tablemetricstest";
    private static final String COUNTER_TABLE = "tablemetricscountertest";
    private static final String BASE_TABLE = "basetablemetricstest";
    private static final String MV1 = "mv1metricstest";

    private static EmbeddedCassandraService cassandra;
    private static Cluster cluster;

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", KEYSPACE));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id_c counter, id int, val text, PRIMARY KEY(id, val));", KEYSPACE, COUNTER_TABLE));
    }

    private ColumnFamilyStore recreateTable()
    {
        return recreateTable(TABLE);
    }

    private ColumnFamilyStore recreateTable(String table)
    {
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1));", KEYSPACE, table));
        return ColumnFamilyStore.getIfExists(KEYSPACE, table);
    }

    private ColumnFamilyStore recreateMVAndBaseTable()
    {
        session.execute(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEYSPACE, MV1));
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, BASE_TABLE));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1));", KEYSPACE, BASE_TABLE));
        session.execute(String.format("CREATE MATERIAlIZED VIEW %s.%s AS SELECT * FROM %s.%s WHERE id IS NOT NULL AND val1 IS NOT NULL AND val2 IS NOT NULL PRIMARY KEY(val2, id, val1);", KEYSPACE, MV1, KEYSPACE, BASE_TABLE));
        return ColumnFamilyStore.getIfExists(KEYSPACE, BASE_TABLE);
    }

    private void executeBatch(boolean isLogged, int distinctPartitions, int statementsPerPartition, String... tables)
    {
        if (tables == null || tables.length == 0)
        {
            tables = new String[] { TABLE };
        }
        BatchStatement.Type batchType;

        if (isLogged)
        {
            batchType = BatchStatement.Type.LOGGED;
        }
        else
        {
            batchType = BatchStatement.Type.UNLOGGED;
        }

        BatchStatement batch = new BatchStatement(batchType);

        for (String table : tables)
            populateBatch(batch, table, distinctPartitions, statementsPerPartition);

        session.execute(batch);
    }

    private static void populateBatch(BatchStatement batch, String table, int distinctPartitions, int statementsPerPartition)
    {
        PreparedStatement ps = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?);", KEYSPACE, table));

        for (int i=0; i<distinctPartitions; i++)
        {
            for (int j=0; j<statementsPerPartition; j++)
            {
                batch.add(ps.bind(i, j + "a", "b"));
            }
        }
    }

    @Test
    public void testRegularStatementsExecuted()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        for (int i = 0; i < 10; i++)
        {
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i, "val" + i, "val" + i));
        }

        assertEquals(10, cfs.metric.coordinatorWriteLatency.getCount());
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testAllPrimaryKeyColumnRestrictionProvidedForMVBaseTable()
    {
        Boolean originalConfig = DatabaseDescriptor.getMaterializedViewsBasetableMetricCollectionEnabled();
        DatabaseDescriptor.setMaterializedViewsBasetableMetricCollectionEnabled(true);
        ColumnFamilyStore base = recreateMVAndBaseTable();
        ColumnFamilyStore withoutMV = recreateTable();
        // delete statement not providing all primary key columns
        // test all PK columns are provided
        // for MV base table and normal table without MV, counter will not change
        String fullParimarykeyDelete = "DELETE FROM %s.%s WHERE id = %d AND val1 = '%s'";
        session.execute(String.format(fullParimarykeyDelete, KEYSPACE, BASE_TABLE, 1, "2"));
        assertEquals(0, base.metric.viewBaseTableDeleteStatementWithoutFullPrimaryKey.getCount());
        session.execute(String.format(fullParimarykeyDelete, KEYSPACE, TABLE, 1, "2"));
        assertEquals(0, withoutMV.metric.viewBaseTableDeleteStatementWithoutFullPrimaryKey.getCount());
        // test not all PK columns are provided
        // for MV base table, the counter should increase, for normal table without MV, counter will not change
        String partitionDelete = "DELETE FROM %s.%s WHERE id = %d";
        session.execute(String.format(partitionDelete, KEYSPACE, BASE_TABLE, 1));
        assertEquals(1, base.metric.viewBaseTableDeleteStatementWithoutFullPrimaryKey.getCount());
        session.execute(String.format(partitionDelete, KEYSPACE, TABLE, 1));
        assertEquals(0, withoutMV.metric.viewBaseTableDeleteStatementWithoutFullPrimaryKey.getCount());

        // batch statement
        // test base table involved in batch statement
        String batch = "BEGIN BATCH\n" +
                       "INSERT INTO %s.%s (id, val1, val2) VALUES (1, '1', '11');\n" +
                       "INSERT INTO %s.%s (id, val1, val2) VALUES (2, '1', '11');\n" +
                       "APPLY BATCH";
        session.execute(String.format(batch, KEYSPACE, BASE_TABLE, KEYSPACE, BASE_TABLE));
        assertEquals(2, base.metric.viewBaseTableUsedInBatchStatement.getCount());
        session.execute(String.format(batch, KEYSPACE, TABLE, KEYSPACE, TABLE));
        assertEquals(0, withoutMV.metric.viewBaseTableUsedInBatchStatement.getCount());

        // insert with timestamp
        String insert = "INSERT INTO %s.%s (id, val1, val2) VALUES (1, '1', '11') USING TIMESTAMP 12345;";
        session.execute(String.format(insert, KEYSPACE, BASE_TABLE));
        assertEquals(1, base.metric.viewBaseTableModificationWithTimestamp.getCount());
        session.execute(String.format(insert, KEYSPACE, TABLE));
        assertEquals(0, withoutMV.metric.viewBaseTableModificationWithTimestamp.getCount());
        // update with timestamp
        String update = "UPDATE %s.%s USING TIMESTAMP 12345 SET val2 = '2' WHERE id=1 AND val1='1';";
        session.execute(String.format(update, KEYSPACE, BASE_TABLE));
        assertEquals(2, base.metric.viewBaseTableModificationWithTimestamp.getCount());
        session.execute(String.format(update, KEYSPACE, TABLE));
        assertEquals(0, withoutMV.metric.viewBaseTableModificationWithTimestamp.getCount());

        DatabaseDescriptor.setMaterializedViewsBasetableMetricCollectionEnabled(originalConfig);
    }

    @Test
    public void testPreparedStatementsExecuted()
    {
        ColumnFamilyStore cfs = recreateTable();
        PreparedStatement metricsStatement = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?)", KEYSPACE, TABLE));

        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        for (int i = 0; i < 10; i++)
        {
            session.execute(metricsStatement.bind(i, "val" + i, "val" + i));
        }

        assertEquals(10, cfs.metric.coordinatorWriteLatency.getCount());
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testIndexSummaryNotFullySampled() throws IOException
    {
        // On Circle CI we normally don't have enough off-heap memory for this test so ignore it
        Assume.assumeTrue(System.getenv("CIRCLECI") == null);

        ColumnFamilyStore cfs = recreateTable();

        assertEquals(0, cfs.metric.indexSummaryNotFullySampled.getCount());


        final int numKeys = 1000000;
        final int keySize = 3000;
        final int minIndexInterval = 1;
        final Random random = new Random();
        IPartitioner partitioner = Util.testPartitioner();

        try (IndexSummaryBuilder builder = new IndexSummaryBuilder(numKeys, minIndexInterval, BASE_SAMPLING_LEVEL))
        {
            for (int i = 0; i < numKeys; i++)
            {
                byte[] randomBytes = new byte[keySize];
                random.nextBytes(randomBytes);
                DecoratedKey key = partitioner.decorateKey(ByteBuffer.wrap(randomBytes));
                builder.maybeAddEntry(key, i, cfs.metadata);
            }

            try (IndexSummary indexSummary = builder.build(partitioner))
            {
                assertNotNull(indexSummary);
                assertEquals(numKeys, indexSummary.getMaxNumberOfEntries());
                assertEquals(numKeys + 1, indexSummary.getEstimatedKeyCount());
            }
        }

        assertEquals(1, cfs.metric.indexSummaryNotFullySampled.getCount());
    }

    @Test
    public void testLoggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        executeBatch(true, 10, 2);
        assertEquals(1, cfs.metric.coordinatorWriteLatency.getCount());

        executeBatch(true, 20, 2);
        assertEquals(2, cfs.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testLoggedPartitionsPerBatchMultiTable()
    {
        ColumnFamilyStore first = recreateTable();
        assertEquals(0, first.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, first.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        ColumnFamilyStore second = recreateTable(TABLE + "_second");
        assertEquals(0, second.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, second.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        executeBatch(true, 10, 2, TABLE, TABLE + "_second");
        assertEquals(1, first.metric.coordinatorWriteLatency.getCount());
        assertEquals(1, second.metric.coordinatorWriteLatency.getCount());

        executeBatch(true, 20, 2, TABLE, TABLE + "_second");
        assertEquals(2, first.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertEquals(2, second.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertGreaterThan(first.metric.coordinatorWriteLatency.getMeanRate(), 0);
        assertGreaterThan(second.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testUnloggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        executeBatch(false, 5, 3);
        assertEquals(1, cfs.metric.coordinatorWriteLatency.getCount());

        executeBatch(false, 25, 2);
        assertEquals(2, cfs.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testUnloggedPartitionsPerBatchMultiTable()
    {
        ColumnFamilyStore first = recreateTable();
        assertEquals(0, first.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, first.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        ColumnFamilyStore second = recreateTable(TABLE + "_second");
        assertEquals(0, second.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, second.metric.coordinatorWriteLatency.getMeanRate(), 0.0);

        executeBatch(false, 5, 3, TABLE, TABLE + "_second");
        assertEquals(1, first.metric.coordinatorWriteLatency.getCount());

        executeBatch(false, 25, 2, TABLE, TABLE + "_second");
        assertEquals(2, first.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertEquals(2, second.metric.coordinatorWriteLatency.getCount()); // 2 for previous batch and this batch
        assertGreaterThan(first.metric.coordinatorWriteLatency.getMeanRate(), 0);
        assertGreaterThan(second.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    @Test
    public void testCounterStatement()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, COUNTER_TABLE);
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.getMeanRate(), 0.0);
        session.execute(String.format("UPDATE %s.%s SET id_c = id_c + 1 WHERE id = 1 AND val = 'val1'", KEYSPACE, COUNTER_TABLE));
        assertEquals(1, cfs.metric.coordinatorWriteLatency.getCount());
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.getMeanRate(), 0);
    }

    private static void assertGreaterThan(double actual, double expectedLessThan) {
        assertTrue("Expected " + actual + " > " + expectedLessThan, actual > expectedLessThan);
    }

    @Test
    public void testMetricsCleanupOnDrop()
    {
        String tableName = TABLE + "_metrics_cleanup";
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(tableName));

        // no metrics before creating
        assertEquals(0, metrics.get().count());

        recreateTable(tableName);
        // some metrics
        assertTrue(metrics.get().count() > 0);

        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tableName));
        // no metrics after drop
        assertEquals(metrics.get().collect(Collectors.joining(",")), 0, metrics.get().count());
    }

    @Test
    public void testViewMetricsCleanupOnDrop()
    {
        String tableName = TABLE + "_2_metrics_cleanup";
        String viewName = TABLE + "_materialized_view_cleanup";
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(viewName));

        // no metrics before creating
        assertEquals(0, metrics.get().count());

        recreateTable(tableName);
        session.execute(String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT id,val1 FROM %s.%s WHERE id IS NOT NULL AND val1 IS NOT NULL PRIMARY KEY (id,val1);", KEYSPACE, viewName, KEYSPACE, tableName));
        // some metrics
        assertTrue(metrics.get().count() > 0);

        session.execute(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s;", KEYSPACE, viewName));
        // no metrics after drop
        assertEquals(metrics.get().collect(Collectors.joining(",")), 0, metrics.get().count());
    }


    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }
}
