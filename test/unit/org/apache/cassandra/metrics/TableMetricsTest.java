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


    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }
}
