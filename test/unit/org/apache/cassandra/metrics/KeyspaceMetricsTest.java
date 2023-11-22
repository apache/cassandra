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

import com.datastax.driver.core.PreparedStatement;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;
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
    public void testResultSetSizeCount() {
        session.execute("CREATE KEYSPACE junit WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS junit.resultset (id int PRIMARY KEY, content text);");

        KeyspaceMetrics junitMetrics = Keyspace.open("junit").metric;
        // Upon receiving schema change events, clients scan the system schema to confirm the schema version,
        // find out existing peers in the ring, etc.
        // These activities shouldn't be reflected on the user keyspace metrics.
        assertEquals(0, junitMetrics.resultsetSize.getCount());

        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO junit.resultset (id, content) VALUES (%d, '%s')", i, "val" + i));

        // These LWT won't increase result set size
        session.execute("INSERT INTO junit.resultset (id, content) VALUES (4, 'new') IF NOT EXISTS;");
        session.execute("UPDATE junit.resultset SET content='new' WHERE id=1 IF content='old'");
        session.execute("UPDATE junit.resultset SET content='new' WHERE id=1 IF content='1'");
        assertEquals(0, junitMetrics.resultsetSize.getCount());


        session.execute("SELECT * FROM junit.resultset");
        assertEquals(10, junitMetrics.resultsetSize.getCount());
        session.execute("SELECT * FROM junit.resultset WHERE id < 5 ALLOW FILTERING;");
        assertEquals(15, junitMetrics.resultsetSize.getCount());

        // prepared statements should also increase the resultset size count
        PreparedStatement prepared = session.prepare("SELECT * FROM junit.resultset WHERE id = ?");
        session.execute(prepared.bind(2));
        assertEquals(16, junitMetrics.resultsetSize.getCount());
        session.execute(prepared.bind(11));
        assertEquals(16, junitMetrics.resultsetSize.getCount());

        // unqualified
        session.execute("USE junit;");
        PreparedStatement unqualifedPrepared = session.prepare("SELECT * FROM resultset WHERE id = ?");
        session.execute(unqualifedPrepared.bind(8));
        assertEquals(17, junitMetrics.resultsetSize.getCount());
        session.execute(unqualifedPrepared.bind(11));
        assertEquals(17, junitMetrics.resultsetSize.getCount());
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
