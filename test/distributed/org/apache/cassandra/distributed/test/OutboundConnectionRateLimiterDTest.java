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

package org.apache.cassandra.distributed.test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

public class OutboundConnectionRateLimiterDTest extends TestBaseImpl
{

    @Test
    public void testClusterBootAndTrafficUnderRateLimiter() throws Exception
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(2)
                                           .withConfig(c -> c.with(Feature.NETWORK)
                                                             .with(Feature.GOSSIP)
                                                             .set("internode_max_outbound_connections_per_second", 1)
                                                             .set("block_for_peers_timeout_in_secs", 60))
                                           .start()))
        {
            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).runOnInstance(() -> {
                    Assert.assertEquals(1, DatabaseDescriptor.getInternodeMaxOutboundConnectionsPerSecond());
                    Assert.assertEquals(1, MessagingService.instance().socketFactory.outboundConnectionRateLimiter().getRate());
                });
            }
            // Don't wait for a rate-limited connection here: the limiter allows a one-second burst, so whether
            // boot alone delays one of node 2's connections depends on timing. testNodeRestartUnderRateLimiter
            // checks the delay after a restart, when every connection type reconnects at once.
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int PRIMARY KEY, v text)");
            cluster.coordinator(1).execute("INSERT INTO ks.tbl (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 1, "rate_limiter");

            Object[][] rows = cluster.coordinator(2).execute("SELECT v FROM ks.tbl WHERE k = ?", ConsistencyLevel.ALL, 1);
            Assert.assertEquals(1, rows.length);
            Assert.assertEquals("rate_limiter", rows[0][0]);
        }
    }

    @Test
    public void testNodeRestartUnderRateLimiter() throws Exception
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(2)
                                           .withConfig(c -> c.with(Feature.NETWORK)
                                                             .with(Feature.GOSSIP)
                                                             .set("internode_max_outbound_connections_per_second", 1)
                                                             .set("block_for_peers_timeout_in_secs", 60))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int PRIMARY KEY, v text)");
            cluster.coordinator(1).execute("INSERT INTO ks.tbl (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 1, "before_restart");

            long mark = cluster.get(2).logs().mark();

            cluster.get(2).shutdown().get(30, TimeUnit.SECONDS);
            cluster.get(2).startup();

            cluster.coordinator(2).execute("INSERT INTO ks.tbl (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 2, "after_restart");
            String largePayload = new String(new char[70000]);
            cluster.coordinator(2).execute("INSERT INTO ks.tbl (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, 3, largePayload);

            cluster.get(2).logs().watchFor(mark, Duration.ofSeconds(60), "delayed .* by internode connection rate limiter");

            cluster.get(2).runOnInstance(() ->
                                         Assert.assertTrue("rateLimitedConnections metric must be > 0 after restart with rate=1",
                                                           InternodeOutboundMetrics.rateLimitedConnections.getCount() > 0));

            Object[][] r1 = cluster.coordinator(2).execute("SELECT v FROM ks.tbl WHERE k = ?", ConsistencyLevel.ALL, 1);
            Assert.assertEquals("before_restart", r1[0][0]);

            Object[][] r2 = cluster.coordinator(1).execute("SELECT v FROM ks.tbl WHERE k = ?", ConsistencyLevel.ALL, 2);
            Assert.assertEquals("after_restart", r2[0][0]);
        }
    }

    @Test
    public void testLiveReconfigurationViaJMX() throws Exception
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(2)
                                           .withConfig(c -> c.with(Feature.NETWORK)
                                                             .with(Feature.GOSSIP)
                                                             .set("internode_max_outbound_connections_per_second", 5))
                                           .start()))
        {
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.setInternodeMaxOutboundConnectionsPerSecond(20);
                Assert.assertEquals(20, StorageService.instance.getInternodeMaxOutboundConnectionsPerSecond());
                Assert.assertEquals(20, DatabaseDescriptor.getInternodeMaxOutboundConnectionsPerSecond());
                Assert.assertEquals(20, MessagingService.instance().socketFactory.outboundConnectionRateLimiter().getRate());

                StorageService.instance.setInternodeMaxOutboundConnectionsPerSecond(0);
                Assert.assertEquals(0, StorageService.instance.getInternodeMaxOutboundConnectionsPerSecond());
                Assert.assertEquals(0, DatabaseDescriptor.getInternodeMaxOutboundConnectionsPerSecond());
            });

            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int PRIMARY KEY, v text)");
            cluster.coordinator(1).execute("INSERT INTO ks.tbl (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, 100, "live_tune");

            Object[][] rows = cluster.coordinator(2).execute("SELECT v FROM ks.tbl WHERE k = ?", ConsistencyLevel.ONE, 100);
            Assert.assertEquals(1, rows.length);
            Assert.assertEquals("live_tune", rows[0][0]);
        }
    }
}
