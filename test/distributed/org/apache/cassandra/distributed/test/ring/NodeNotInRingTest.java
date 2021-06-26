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

package org.apache.cassandra.distributed.test.ring;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.action.GossipHelper;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class NodeNotInRingTest extends TestBaseImpl
{
    @Test
    public void nodeNotInRingTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};");
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.filters().verbs(Verb.GOSSIP_DIGEST_ACK.id,
                                    Verb.GOSSIP_DIGEST_SYN.id)
                   .from(3)
                   .outbound()
                   .drop()
                   .on();
            cluster.run(GossipHelper.removeFromRing(cluster.get(3)), 1, 2);
            cluster.run(inst -> inst.runsOnInstance(() -> {
                Assert.assertEquals("There should be 2 remaining nodes in ring",
                                    2, StorageService.instance.effectiveOwnershipWithPort(KEYSPACE).size());
            }), 1, 2);

            populate(cluster, 0, 50, 1, ConsistencyLevel.ALL);
            populate(cluster, 50, 100, 2, ConsistencyLevel.ALL);

            Map<Integer, Long> counts = BootstrapTest.count(cluster);
            Assert.assertEquals(0L, counts.get(3).longValue());
            Assert.assertEquals(100L, counts.get(2).longValue());
            Assert.assertEquals(100L, counts.get(1).longValue());
        }
    }

    public static void populate(ICluster cluster, int from, int to, int coord, ConsistencyLevel cl)
    {
        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }
}
