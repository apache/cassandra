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

import com.codahale.metrics.Metric;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyspaceMetricsTest
{
    private static Session session;
    private static Cluster cluster;
    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();
    }

    @Test
    public void testMetricsCleanupOnDrop()
    {
        String keyspace = "keyspacemetricstest_metrics_cleanup";
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(keyspace));

        // no metrics before creating
        assertEquals(0, metrics.get().count());

        session.execute(String.format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", keyspace));
        // some metrics
        assertTrue(metrics.get().count() > 0);

        session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        // no metrics after drop
        assertEquals(metrics.get().collect(Collectors.joining(",")), 0, metrics.get().count());
    }

    @Test
    public void testKeyspaceVirtualTable()
    {
        String keyspace = "uniquemetricskeyspace1";
        session.execute(String.format(
            "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
            keyspace));

        assertTrue(CassandraMetricsRegistry.Metrics.getNames().stream().anyMatch(m -> m.endsWith(keyspace)));
        ResultSet resultSet = session.execute(
            "SELECT * FROM system_metrics.keyspace_group WHERE scope = '" + keyspace + "';");

        int count = 0;
        for (Row row : resultSet)
        {
            String metricName = row.getString("name");
            if (!metricName.endsWith(keyspace))
                continue;

            Metric metric = CassandraMetricsRegistry.Metrics.getMetrics().get(metricName);
            assertEquals(CassandraMetricsRegistry.getValueAsString(metric), row.getString("value"));
            count++;
        }

        assertTrue("Keyspace " + keyspace + " metrics was not found", count > 0);

        session.execute(String.format("DROP KEYSPACE %s;", keyspace));
        assertFalse(CassandraMetricsRegistry.Metrics.getNames().stream().anyMatch(m -> m.endsWith(keyspace)));
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
