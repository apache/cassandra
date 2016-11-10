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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.cql3.statements.BatchStatement.metrics;

@RunWith(OrderedJUnit4ClassRunner.class)
public class BatchMetricsTest extends SchemaLoader
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static String KEYSPACE = "junit";
    private static final String TABLE = "batchmetricstest";

    private static PreparedStatement ps;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (id int PRIMARY KEY, val text);");

        ps = session.prepare("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, val) VALUES (?, ?);");
    }

    private void executeBatch(boolean isLogged, int distinctPartitions, int statementsPerPartition)
    {
        BatchStatement.Type batchType;

        if (isLogged) {
            batchType = BatchStatement.Type.LOGGED;
        } else {
            batchType = BatchStatement.Type.UNLOGGED;
        }

        BatchStatement batch = new BatchStatement(batchType);

        for (int i=0; i<distinctPartitions; i++) {
            for (int j=0; j<statementsPerPartition; j++) {
                batch.add(ps.bind(i, "aaaaaaaa"));
            }
        }

        session.execute(batch);
    }

    @Test
    public void testLoggedPartitionsPerBatch() {
        int partitionsPerBatchCountPre = (int) metrics.partitionsPerLoggedBatch.getCount();
        executeBatch(true, 10, 2);
        assertEquals(partitionsPerBatchCountPre+1, metrics.partitionsPerLoggedBatch.getCount());
        assertTrue(partitionsPerBatchCountPre <= metrics.partitionsPerLoggedBatch.getSnapshot().getMax()); // decayingBuckets may not have exact value

        partitionsPerBatchCountPre = (int) metrics.partitionsPerLoggedBatch.getCount();
        executeBatch(true, 21, 2);
        assertEquals(partitionsPerBatchCountPre+1, metrics.partitionsPerLoggedBatch.getCount());
        assertTrue(partitionsPerBatchCountPre <= metrics.partitionsPerLoggedBatch.getSnapshot().getMax());
    }

    @Test
    public void testUnloggedPartitionsPerBatch() {
        int partitionsPerBatchCountPre = (int) metrics.partitionsPerUnloggedBatch.getCount();
        executeBatch(false, 7, 2);
        assertEquals(partitionsPerBatchCountPre+1, metrics.partitionsPerUnloggedBatch.getCount());
        assertTrue(partitionsPerBatchCountPre <= metrics.partitionsPerUnloggedBatch.getSnapshot().getMax());

        partitionsPerBatchCountPre = (int) metrics.partitionsPerUnloggedBatch.getCount();
        executeBatch(false, 25, 2);
        assertEquals(partitionsPerBatchCountPre+1, metrics.partitionsPerUnloggedBatch.getCount());
        assertTrue(partitionsPerBatchCountPre <= metrics.partitionsPerUnloggedBatch.getSnapshot().getMax());
    }
}
