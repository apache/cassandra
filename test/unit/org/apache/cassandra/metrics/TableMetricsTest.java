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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableMetricsTest
{
    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "tablemetricstest";
    private static final String TABLE_WITH_HISTOS_AGGR = "tablemetricstest_histo_aggr";
    private static final String COUNTER_TABLE = "tablemetricscountertest";

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

    private void recreateExtensionTables(String keyspace)
    {
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace));

        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", keyspace, TABLE));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH extensions = {'HISTOGRAM_METRICS': '%s'};",
                                      keyspace, TABLE, TableMetrics.MetricsAggregation.INDIVIDUAL.asCQLString())); // this is also the default

        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", keyspace, TABLE_WITH_HISTOS_AGGR));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH extensions = {'HISTOGRAM_METRICS': '%s'};",
                                      keyspace, TABLE_WITH_HISTOS_AGGR, TableMetrics.MetricsAggregation.AGGREGATED.asCQLString()));
    }

    private void executeBatch(boolean isLogged, int distinctPartitions, int statementsPerPartition, String... tables)
    {
        if (tables == null || tables.length == 0)
        {
            tables = new String[]{ TABLE };
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

        for (int i = 0; i < distinctPartitions; i++)
        {
            for (int j = 0; j < statementsPerPartition; j++)
            {
                batch.add(ps.bind(i, j + "a", "b"));
            }
        }
    }

    private void checkWriteCounts(Keyspace keyspace, String table, long ksCount, long tableCount)
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);

        // The table with its non-aggregated histograms will have individual table counts
        assertEquals(tableCount, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(tableCount, cfs.metric.writeLatency.tableOrKeyspaceMetric().latency.getCount());

        assertEquals(ksCount, keyspace.metric.coordinatorWriteLatency.getCount());
        assertEquals(ksCount, keyspace.metric.writeLatency.latency.getCount());
    }

    @Test
    public void testRegularStatementsExecuted()
    {
        String keyspaceName = "regularstatementsks";
        recreateExtensionTables(keyspaceName);

        Keyspace keyspace = Keyspace.open(keyspaceName);
        assertEquals(keyspace.metric.coordinatorWriteLatency.getCount(), 0);
        assertEquals(keyspace.metric.writeLatency.latency.getCount(), 0);

        int numRows = 10;

        checkWriteCounts(keyspace, TABLE, 0, 0);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, 0, 0);

        for (int i = 0; i < numRows; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", keyspaceName, TABLE, i, "val" + i, "val" + i));

        checkWriteCounts(keyspace, TABLE, numRows, numRows);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, numRows, numRows);

        for (int i = 0; i < numRows; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", keyspaceName, TABLE_WITH_HISTOS_AGGR, i, "val" + i, "val" + i));

        checkWriteCounts(keyspace, TABLE, numRows * 2, numRows);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, numRows * 2, numRows * 2);
    }

    @Test
    public void testPreparedStatementsExecuted()
    {
        String keyspaceName = "preparedstatementsks";
        recreateExtensionTables(keyspaceName);

        Keyspace keyspace = Keyspace.open(keyspaceName);
        assertEquals(keyspace.metric.coordinatorWriteLatency.getCount(), 0);
        assertEquals(keyspace.metric.writeLatency.latency.getCount(), 0);

        int numRows = 10;

        checkWriteCounts(keyspace, TABLE, 0, 0);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, 0, 0);

        PreparedStatement metricsStatement = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?)", keyspaceName, TABLE));
        for (int i = 0; i < numRows; i++)
            session.execute(metricsStatement.bind(i, "val" + i, "val" + i));

        checkWriteCounts(keyspace, TABLE, numRows, numRows);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, numRows, numRows);

        metricsStatement = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?)", keyspaceName, TABLE_WITH_HISTOS_AGGR));
        for (int i = 0; i < numRows; i++)
            session.execute(metricsStatement.bind(i, "val" + i, "val" + i));

        checkWriteCounts(keyspace, TABLE, numRows * 2, numRows);
        checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, numRows * 2, numRows * 2);
    }

    @Test
    public void testAlterTable()
    {
        // First create a table with aggregated histograms, then alter it to individual histograms, check the counts
        String keyspaceName = "altertableks";
        recreateExtensionTables(keyspaceName);
        Keyspace keyspace = Keyspace.open(keyspaceName);

        try
        {
            int numRows = 10;
            long ksCount = keyspace.metric.coordinatorWriteLatency.getCount();

            checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, ksCount, ksCount);

            for (int i = 0; i < numRows; i++)
                session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", keyspace.getName(), TABLE_WITH_HISTOS_AGGR, i, "val" + i, "val" + i));

            // counts are the same as the keyspace since it is aggregated
            checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, ksCount + numRows, ksCount + numRows);

            session.execute(String.format("ALTER TABLE %s.%s WITH extensions = {'HISTOGRAM_METRICS': '%s'};", keyspace.getName(), TABLE_WITH_HISTOS_AGGR, TableMetrics.MetricsAggregation.INDIVIDUAL.asCQLString()));

            for (int i = 0; i < numRows; i++)
                session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", keyspace.getName(), TABLE_WITH_HISTOS_AGGR, i, "val" + i, "val" + i));

            // Now we should find individual histograms counts for the table
            checkWriteCounts(keyspace, TABLE_WITH_HISTOS_AGGR, ksCount + numRows * 2, numRows);
        }
        finally
        {
            session.execute(String.format("DROP TABLE %s.%s", keyspace.getName(), TABLE_WITH_HISTOS_AGGR));
        }
    }


    @Test
    public void testLoggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        executeBatch(true, 10, 2);
        assertEquals(1, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());

        executeBatch(true, 20, 2);
        assertEquals(2, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
    }

    @Test
    public void testLoggedPartitionsPerBatchMultiTable()
    {
        ColumnFamilyStore first = recreateTable();
        assertEquals(0, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        ColumnFamilyStore second = recreateTable(TABLE + "_second");
        assertEquals(0, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        executeBatch(true, 10, 2, TABLE, TABLE + "_second");
        assertEquals(1, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(1, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());

        executeBatch(true, 20, 2, TABLE, TABLE + "_second");
        assertEquals(2, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertEquals(2, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertGreaterThan(first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
        assertGreaterThan(second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
    }

    @Test
    public void testUnloggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        executeBatch(false, 5, 3);
        assertEquals(1, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());

        executeBatch(false, 25, 2);
        assertEquals(2, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
    }

    @Test
    public void testUnloggedPartitionsPerBatchMultiTable()
    {
        ColumnFamilyStore first = recreateTable();
        assertEquals(0, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        ColumnFamilyStore second = recreateTable(TABLE + "_second");
        assertEquals(0, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);

        executeBatch(false, 5, 3, TABLE, TABLE + "_second");
        assertEquals(1, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());

        executeBatch(false, 25, 2, TABLE, TABLE + "_second");
        assertEquals(2, first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertEquals(2, second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount()); // 2 for previous batch and this batch
        assertGreaterThan(first.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
        assertGreaterThan(second.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
    }

    @Test
    public void testCounterStatement()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, COUNTER_TABLE);
        assertEquals(0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertEquals(0.0, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0.0);
        session.execute(String.format("UPDATE %s.%s SET id_c = id_c + 1 WHERE id = 1 AND val = 'val1'", KEYSPACE, COUNTER_TABLE));
        assertEquals(1, cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getCount());
        assertGreaterThan(cfs.metric.coordinatorWriteLatency.tableOrKeyspaceTimer().getMeanRate(), 0);
    }

    private static void assertGreaterThan(double actual, double expectedLessThan)
    {
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

    @Test
    public void testEstimatedPartitionCount() throws InterruptedException
    {
        long prevPeriod = TableMetrics.ESTIMATED_PARTITION_COUNT_CACHE_PERIOD_SECONDS;
        try
        {
            TableMetrics.ESTIMATED_PARTITION_COUNT_CACHE_PERIOD_SECONDS = 1;
            doTestEstimatedPartitionCount();
        }
        finally
        {
            TableMetrics.ESTIMATED_PARTITION_COUNT_CACHE_PERIOD_SECONDS = prevPeriod;
        }
    }

    private void doTestEstimatedPartitionCount()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0L, cfs.metric.estimatedPartitionCount.getValue().longValue());
        assertEquals(0L, cfs.metric.estimatedPartitionCountInSSTablesCached.getValue().longValue());
        long startTime = System.currentTimeMillis();

        int partitionCount = 10;
        int numRows = 100;

        for (int i = 0; i < numRows; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i % partitionCount, "val" + i, "val" + i));

        assertEquals(partitionCount, cfs.metric.estimatedPartitionCount.getValue().longValue());
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertEquals(partitionCount, cfs.metric.estimatedPartitionCount.getValue().longValue());

        long estimatedPartitionCountInSSTables = cfs.metric.estimatedPartitionCountInSSTablesCached.getValue().longValue();
        long elapsedTime = System.currentTimeMillis() - startTime;
        // the caching time is one second; avoid flakiness by only checking if a long time has not passed
        // (Because we take the time after calling the method, elapsedTime < 1000 should also be stable, but let's also
        // accommodate the possibility that the cache uses a different timer with different tick times.)
        if (elapsedTime < 980)
            assertEquals(0, estimatedPartitionCountInSSTables);

        for (int i = 0; i < numRows; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i % partitionCount, "val" + i, "val" + i));

        estimatedPartitionCountInSSTables = cfs.metric.estimatedPartitionCountInSSTablesCached.getValue().longValue();
        elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime < 980)
            assertEquals(0, estimatedPartitionCountInSSTables);
        else if (elapsedTime >= 1020)
            assertEquals(partitionCount, estimatedPartitionCountInSSTables);

        // The answer below is incorrect but what the metric currently returns.
        assertEquals(partitionCount * 2, cfs.metric.estimatedPartitionCount.getValue().longValue());
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        // Recalculation for the new sstable set will correct it.
        assertEquals(partitionCount, cfs.metric.estimatedPartitionCount.getValue().longValue());

        // The cached estimatedPartitionCountInSSTables lags one second, check that.
        // Assert that the metric will return a correct value after at least a second passes
        Awaitility.await()
                  .atMost(2, TimeUnit.SECONDS)
                  .untilAsserted(() -> assertEquals(partitionCount, (long) cfs.metric.estimatedPartitionCountInSSTablesCached.getValue()));
    }


    @Test
    public void testWriteRequestsCounter()
    {
        ColumnFamilyStore cfs = recreateTable();

        // Verify initial state
        assertEquals(0, cfs.metric.writeRequests.getCount());
        assertEquals(0, cfs.metric.readRequests.getCount());

        int numOperations = 5;

        // Execute INSERT operations
        for (int i = 0; i < numOperations; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')",
                                          KEYSPACE, TABLE, i, "val" + i, "val" + i));

        // Verify writeRequests counter is incremented for inserts
        assertEquals(numOperations, cfs.metric.writeRequests.getCount());
        assertEquals(0, cfs.metric.readRequests.getCount());

        // Execute UPDATE operations
        for (int i = 0; i < numOperations; i++)
            session.execute(String.format("UPDATE %s.%s SET val2 = '%s' WHERE id = %d AND val1 = '%s'",
                                          KEYSPACE, TABLE, "updated" + i, i, "val" + i));

        // Verify counter increments for updates
        assertEquals(numOperations * 2, cfs.metric.writeRequests.getCount());

        // Execute DELETE operations
        for (int i = 0; i < numOperations; i++)
            session.execute(String.format("DELETE FROM %s.%s WHERE id = %d AND val1 = '%s'",
                                          KEYSPACE, TABLE, i, "val" + i));

        // Verify counter increments for deletes
        assertEquals(numOperations * 3, cfs.metric.writeRequests.getCount());

        // Execute a read to verify writeRequests doesn't increment on reads
        session.execute(String.format("SELECT * FROM %s.%s WHERE id = 0 AND val1 = 'val0'", KEYSPACE, TABLE));

        // writeRequests should remain the same, readRequests should increment
        assertEquals(numOperations * 3, cfs.metric.writeRequests.getCount());
        assertEquals(1, cfs.metric.readRequests.getCount());
    }

    @Test
    public void testDeleteRequestsMetric()
    {
        ColumnFamilyStore cfs = recreateTable();

        // Initially, deleteRequests should be 0
        assertEquals(0, cfs.metric.deleteRequests.getCount());
        assertEquals(0, cfs.keyspace.metric.deleteRequests.getCount());

        // Insert some data first
        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i, "val" + i, "val" + i));

        // Verify deleteRequests is still 0 after inserts
        assertEquals(0, cfs.metric.deleteRequests.getCount());
        assertEquals(0, cfs.keyspace.metric.deleteRequests.getCount());

        // Execute DELETE statements
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 0 AND val1 = 'val0'", KEYSPACE, TABLE));
        assertEquals(1, cfs.metric.deleteRequests.getCount());
        assertEquals(1, cfs.keyspace.metric.deleteRequests.getCount());

        session.execute(String.format("DELETE FROM %s.%s WHERE id = 1 AND val1 = 'val1'", KEYSPACE, TABLE));
        assertEquals(2, cfs.metric.deleteRequests.getCount());
        assertEquals(2, cfs.keyspace.metric.deleteRequests.getCount());

        // Execute a partition deletion
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 2", KEYSPACE, TABLE));
        assertEquals(3, cfs.metric.deleteRequests.getCount());
        assertEquals(3, cfs.keyspace.metric.deleteRequests.getCount());

        // Execute multiple deletes
        for (int i = 3; i < 7; i++)
            session.execute(String.format("DELETE FROM %s.%s WHERE id = %d AND val1 = 'val%d'", KEYSPACE, TABLE, i, i));

        assertEquals(7, cfs.metric.deleteRequests.getCount());
        assertEquals(7, cfs.keyspace.metric.deleteRequests.getCount());

        // Verify that UPDATE statements don't increment deleteRequests
        session.execute(String.format("UPDATE %s.%s SET val2 = 'updated' WHERE id = 7 AND val1 = 'val7'", KEYSPACE, TABLE));
        assertEquals(7, cfs.metric.deleteRequests.getCount());
        assertEquals(7, cfs.keyspace.metric.deleteRequests.getCount());

        // Verify that INSERT with NULL values don't increment deleteRequests
        session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (8, 'val8', null)", KEYSPACE, TABLE));
        assertEquals(7, cfs.metric.deleteRequests.getCount());
        assertEquals(7, cfs.keyspace.metric.deleteRequests.getCount());

        // Test BATCH with multiple DELETE statements
        String batchQuery = String.format(
        "BEGIN BATCH " +
        "DELETE FROM %s.%s WHERE id = 8 AND val1 = 'val8'; " +
        "DELETE FROM %s.%s WHERE id = 9 AND val1 = 'val9'; " +
        "APPLY BATCH",
        KEYSPACE, TABLE, KEYSPACE, TABLE);

        session.execute(batchQuery);

        // Each DELETE in the batch should increment the counter
        assertEquals(9, cfs.metric.deleteRequests.getCount());
        assertEquals(9, cfs.keyspace.metric.deleteRequests.getCount());

        // Test mixed BATCH with DELETE, UPDATE, and INSERT
        session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (10, 'val10', 'val10')", KEYSPACE, TABLE));
        session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (11, 'val11', 'val11')", KEYSPACE, TABLE));

        String mixedBatchQuery = String.format(
        "BEGIN BATCH " +
        "DELETE FROM %s.%s WHERE id = 10; " +
        "UPDATE %s.%s SET val2 = 'updated' WHERE id = 11 AND val1 = 'val11'; " +
        "INSERT INTO %s.%s (id, val1, val2) VALUES (12, 'val12', 'val12'); " +
        "APPLY BATCH",
        KEYSPACE, TABLE, KEYSPACE, TABLE, KEYSPACE, TABLE);

        session.execute(mixedBatchQuery);

        // Only the DELETE statement in the mixed batch should increment the counter
        assertEquals(10, cfs.metric.deleteRequests.getCount());
        assertEquals(10, cfs.keyspace.metric.deleteRequests.getCount());

        // Test range deletion
        // Insert data with multiple clustering columns
        for (int i = 13; i < 20; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (13, 'val%d', 'data%d')", KEYSPACE, TABLE, i, i));

        // Execute a range deletion - deletes multiple rows with same partition key but different clustering keys
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 13 AND val1 > 'val15' AND val1 < 'val19'", KEYSPACE, TABLE));

        // Range deletion should increment the counter once
        assertEquals(11, cfs.metric.deleteRequests.getCount());
        assertEquals(11, cfs.keyspace.metric.deleteRequests.getCount());

        //Test delete of multiple partition keys
        for (int i = 20; i < 25; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, 'val%d', 'data%d')", KEYSPACE, TABLE, i, i, i));

        // Execute multiple partition deletes
        session.execute(String.format("DELETE FROM %s.%s WHERE id in (20,21,22,24,25)", KEYSPACE, TABLE));

        assertEquals(12, cfs.metric.deleteRequests.getCount());
        assertEquals(12, cfs.keyspace.metric.deleteRequests.getCount());

        //Test delete of multiple clustering keys
        for (int i = 25; i < 30; i++)
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (25, 'val%d', 'data%d')", KEYSPACE, TABLE, i, i));

        // Execute multiple clustering keys deletes
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 25 and val1 in ('val25', 'val26', 'val27')", KEYSPACE, TABLE));

        assertEquals(13, cfs.metric.deleteRequests.getCount());
        assertEquals(13, cfs.keyspace.metric.deleteRequests.getCount());

        // Test multiple slices scenario - create a table with composite clustering key
        String multiSliceTable = "multi_slice_test";
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, multiSliceTable));
        session.execute(String.format("CREATE TABLE %s.%s (id int, ck1 text, ck2 int, val text, PRIMARY KEY(id, ck1, ck2))", KEYSPACE, multiSliceTable));

        ColumnFamilyStore multiSliceCfs = ColumnFamilyStore.getIfExists(KEYSPACE, multiSliceTable);

        // Insert test data
        for (int i = 0; i < 5; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                session.execute(String.format("INSERT INTO %s.%s (id, ck1, ck2, val) VALUES (100, 'ck%d', %d, 'data')", KEYSPACE, multiSliceTable, i, j));
            }
        }

        // Test 1: Execute a delete with IN on first clustering column and range on second clustering column
        // This creates multiple slices: one slice per value in the IN clause
        // DELETE WHERE id = 100 AND ck1 IN ('ck0', 'ck1', 'ck2') AND ck2 > 3 AND ck2 < 7
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 100 AND ck1 IN ('ck0', 'ck1', 'ck2') AND ck2 > 3 AND ck2 < 7", KEYSPACE, multiSliceTable));

        // Should increment by 1
        assertEquals(1, multiSliceCfs.metric.deleteRequests.getCount());
        assertEquals(14, multiSliceCfs.keyspace.metric.deleteRequests.getCount());

        // Test 2: Execute a delete with IN on both clustering columns (specific clustering keys, not slices)
        // This creates multiple specific deletes: one per combination of clustering key values
        // DELETE WHERE id = 100 AND ck1 IN ('ck3', 'ck4') AND ck2 IN (5, 6, 7)
        session.execute(String.format("DELETE FROM %s.%s WHERE id = 100 AND ck1 IN ('ck3', 'ck4') AND ck2 IN (5, 6, 7)", KEYSPACE, multiSliceTable));

        // Should increment by 1 (just once for the DELETE statement regardless of the number of rows that get deleted)
        assertEquals(2, multiSliceCfs.metric.deleteRequests.getCount());
        assertEquals(15, multiSliceCfs.keyspace.metric.deleteRequests.getCount());

        // Clean up
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, multiSliceTable));
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
