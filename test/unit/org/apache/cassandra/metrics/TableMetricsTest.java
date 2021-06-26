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
import org.junit.runner.RunWith;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TableMetricsTest extends SchemaLoader
{
    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "tablemetricstest";
    private static final String COUNTER_TABLE = "tablemetricscountertest";

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
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
        String tableName = TABLE + "_metrics_cleanup";
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
    public static void teardown()
    {
        session.close();
    }
}
