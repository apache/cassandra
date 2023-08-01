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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Histogram;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.reads.range.RangeCommandIterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import static com.datastax.driver.core.Cluster.builder;

public class ClientRequestMetricsTest
{
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "junit";
    private static final String TABLE = "clientrequestsmetricstest";

    private static PreparedStatement writePS;
    private static PreparedStatement paxosPS;
    private static PreparedStatement readPS;
    private static PreparedStatement readRangePS;

    private static final ClientRequestMetrics readMetrics = ClientRequestsMetricsHolder.readMetrics;
    private static final ClientWriteRequestMetrics writeMetrics = ClientRequestsMetricsHolder.writeMetrics;

    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (id int, ord int, val text, PRIMARY KEY (id, ord));");

        writePS = session.prepare("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, ord, val) VALUES (?, ?, ?);");
        paxosPS = session.prepare("INSERT INTO " + KEYSPACE + '.' + TABLE + " (id, ord, val) VALUES (?, ?, ?) IF NOT EXISTS;");
        readPS = session.prepare("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE id=?;");
        readRangePS = session.prepare("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE id=? AND ord>=? AND ord <= ?;");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    @Test
    public void testWriteStatement()
    {
        ClientRequestMetricsContainer writeMetricsContainer = new ClientRequestMetricsContainer(writeMetrics);
        ClientRequestMetricsContainer readMetricsContainer = new ClientRequestMetricsContainer(readMetrics);

        executeWrite(1, 1, "aaaa");

        assertEquals(1, writeMetricsContainer.compareLocalRequest());
        assertEquals(0, writeMetricsContainer.compareRemoteRequest());

        assertEquals(0, readMetricsContainer.compareLocalRequest());
        assertEquals(0, readMetricsContainer.compareRemoteRequest());
    }

    @Test
    public void testPaxosStatement()
    {
        ClientRequestMetricsContainer writeMetricsContainer = new ClientRequestMetricsContainer(writeMetrics);
        ClientRequestMetricsContainer readMetricsContainer = new ClientRequestMetricsContainer(readMetrics);

        executePAXOS(2, 2, "aaaa");

        assertEquals(1, readMetricsContainer.compareLocalRequest());
        assertEquals(0, readMetricsContainer.compareRemoteRequest());

        assertEquals(1, writeMetricsContainer.compareLocalRequest());
        assertEquals(0, writeMetricsContainer.compareRemoteRequest());
    }

    @Test
    public void testBatchStatement()
    {
        ClientRequestMetricsContainer writeMetricsContainer = new ClientRequestMetricsContainer(writeMetrics);
        ClientRequestMetricsContainer readMetricsContainer = new ClientRequestMetricsContainer(readMetrics);

        executeBatch(10, 10);

        assertEquals(0, readMetricsContainer.compareLocalRequest());
        assertEquals(0, readMetricsContainer.compareRemoteRequest());

        assertEquals(10, writeMetricsContainer.compareLocalRequest());
        assertEquals(0, writeMetricsContainer.compareRemoteRequest());
    }

    @Test
    public void testReadStatement()
    {
        executeWrite(1, 1, "aaaa");

        ClientRequestMetricsContainer writeMetricsContainer = new ClientRequestMetricsContainer(writeMetrics);
        ClientRequestMetricsContainer readMetricsContainer = new ClientRequestMetricsContainer(readMetrics);

        executeRead(1);

        assertEquals(1, readMetricsContainer.compareLocalRequest());
        assertEquals(0, readMetricsContainer.compareRemoteRequest());

        assertEquals(0, writeMetricsContainer.compareLocalRequest());
        assertEquals(0, writeMetricsContainer.compareRemoteRequest());
    }

    @Test
    public void testRangeStatement()
    {
        executeBatch(1, 100);

        ClientRequestMetricsContainer writeMetricsContainer = new ClientRequestMetricsContainer(writeMetrics);
        ClientRequestMetricsContainer readMetricsContainer = new ClientRequestMetricsContainer(readMetrics);

        executeSlice(1, 0, 99);

        assertEquals(1, readMetricsContainer.compareLocalRequest());
        assertEquals(0, readMetricsContainer.compareRemoteRequest());

        assertEquals(0, writeMetricsContainer.compareLocalRequest());
        assertEquals(0, writeMetricsContainer.compareRemoteRequest());
    }

    @Test
    public void testRangeRead() throws Throwable
    {
        clearHistogram(RangeCommandIterator.rangeMetrics.roundTrips);
        long latencyCount = RangeCommandIterator.rangeMetrics.latency.getCount();

        session.execute("SELECT * FROM system.peers");

        assertThat(RangeCommandIterator.rangeMetrics.roundTrips.getCount()).isGreaterThan(0);
        assertThat(RangeCommandIterator.rangeMetrics.roundTrips.getSnapshot().getMax()).isEqualTo(1);
        assertThat(RangeCommandIterator.rangeMetrics.latency.getCount()).isEqualTo(latencyCount + 1);
    }

    private void clearHistogram(Histogram histogram)
    {
        ((ClearableHistogram) histogram).clear();
    }

    private static class ClientRequestMetricsContainer
    {
        private final ClientRequestMetrics metrics;

        private final long localRequests;
        private final long remoteRequests;

        public ClientRequestMetricsContainer(ClientRequestMetrics clientRequestMetrics)
        {
            metrics = clientRequestMetrics;
            localRequests = metrics.localRequests.getCount();
            remoteRequests = metrics.remoteRequests.getCount();
        }

        public long compareLocalRequest()
        {
            return metrics.localRequests.getCount() - localRequests;
        }

        public long compareRemoteRequest()
        {
            return metrics.remoteRequests.getCount() - remoteRequests;
        }
    }

    private void executeWrite(int id, int ord, String val)
    {
        BoundStatement bs = writePS.bind(id, ord, val);
        session.execute(bs);
    }

    private void executePAXOS(int id, int ord, String val)
    {
        BoundStatement bs = paxosPS.bind(id, ord, val);
        session.execute(bs);
    }

    private void executeBatch(int distinctPartitions, int numClusteringKeys)
    {
        BatchStatement batch = new BatchStatement();

        for (int i = 0; i < distinctPartitions; i++)
        {
            for (int y = 0; y < numClusteringKeys; y++)
            {
                batch.add(writePS.bind(i, y, "aaaaaaaa"));
            }
        }
        session.execute(batch);
    }

    private void executeRead(int id)
    {
        BoundStatement bs = readPS.bind(id);
        session.execute(bs);
    }

    private void executeSlice(int id, int start_range, int end_range)
    {
        BoundStatement bs = readRangePS.bind(id, start_range, end_range);
        session.execute(bs);
    }
}
