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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class TableMetricsTest extends SchemaLoader
{

    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "tablemetricstest";
    private static final String COUNTER_TABLE = "tablemetricscountertest";

    @BeforeClass()
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
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, TABLE));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1));", KEYSPACE, TABLE));
        return ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
    }

    private void executeBatch(boolean isLogged, int distinctPartitions, int statementsPerPartition)
    {
        BatchStatement.Type batchType;
        PreparedStatement ps = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?);", KEYSPACE, TABLE));

        if (isLogged)
        {
            batchType = BatchStatement.Type.LOGGED;
        }
        else
        {
            batchType = BatchStatement.Type.UNLOGGED;
        }

        BatchStatement batch = new BatchStatement(batchType);

        for (int i=0; i<distinctPartitions; i++)
        {
            for (int j=0; j<statementsPerPartition; j++)
            {
                batch.add(ps.bind(i, j + "a", "b"));
            }
        }

        session.execute(batch);
    }

    @Test
    public void testRegularStatementsExecuted()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() == 0);

        for (int i = 0; i < 10; i++)
        {
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i, "val" + i, "val" + i));
        }

        assertEquals(10, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() > 0);
    }

    @Test
    public void testPreparedStatementsExecuted()
    {
        ColumnFamilyStore cfs = recreateTable();
        PreparedStatement metricsStatement = session.prepare(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (?, ?, ?)", KEYSPACE, TABLE));

        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() == 0);

        for (int i = 0; i < 10; i++)
        {
            session.execute(metricsStatement.bind(i, "val" + i, "val" + i));
        }

        assertEquals(10, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() > 0);
    }

    @Test
    public void testLoggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() == 0);

        executeBatch(true, 10, 2);
        assertEquals(10, cfs.metric.coordinatorWriteLatency.getCount());

        executeBatch(true, 20, 2);
        assertEquals(30, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() > 0);
    }

    @Test
    public void testUnloggedPartitionsPerBatch()
    {
        ColumnFamilyStore cfs = recreateTable();
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() == 0);

        executeBatch(false, 5, 3);
        assertEquals(5, cfs.metric.coordinatorWriteLatency.getCount());

        executeBatch(false, 25, 2);
        assertEquals(30, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() > 0);

    }

    @Test
    public void testCounterStatement()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, COUNTER_TABLE);
        assertEquals(0, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() == 0);
        session.execute(String.format("UPDATE %s.%s SET id_c = id_c + 1 WHERE id = 1 AND val = 'val1'", KEYSPACE, COUNTER_TABLE));
        assertEquals(1, cfs.metric.coordinatorWriteLatency.getCount());
        assertTrue(cfs.metric.coordinatorWriteLatency.getMeanRate() > 0);
    }
}