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

import java.util.*;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.*;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getDataPlacementDebugInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getPeerDirectoryDebugStrings;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getTokenMapDebugStrings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompatibilityTest extends TestBaseImpl
{
    @Test
    public void joinWithoutBootstrapTest() throws Throwable
    {
        try (Cluster cluster = builder().withDC("DC1", 2)
                                        .withDC("DC2", 2)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            // Instances should receive a Register event for each instance, including themselves.
            // This should cause them to add each other to their local directory of peers.
            // NodeIDs (used to derive host IDs) should be consistent across all instances.
            List<String> nodeOnePeers = getPeerDirectoryDebugStrings(cluster.get(1));
            nodeOnePeers.forEach(System.out::println);

            assertThat(nodeOnePeers).hasSize(4);
            String pattern = "Node\\{id=NodeId\\{id=00000000-0000-0000-0000-00000000000\\d}" +
                             ", addresses=NodeAddresses\\{broadcastAddress=/127.0.0.\\d:7012, localAddress=/127.0.0.\\d:7012, nativeAddress=/127.0.0.\\d:9042}" +
                             ", location=DC\\d/rack\\d" +
                             ", state=JOINED" +
                             ", version=NodeVersion\\{cassandraVersion=\\d.0.0-SNAPSHOT, serializationVersion=V\\d}}";

            for (int i = 1; i <= 4; i++)
                assertThat(nodeOnePeers.get(i - 1)).as("Checking content of node one peer directory").matches(pattern);

            for (int i = 2; i <= 4; i++)
                assertThat(getPeerDirectoryDebugStrings(cluster.get(i))).as("Checking peer directory on %s", i).isEqualTo(nodeOnePeers);

            List<String> nodeOneTokens = getTokenMapDebugStrings(cluster.get(1));
            for (int i = 2; i <= 4; i++)
                assertThat(getTokenMapDebugStrings(cluster.get(i))).as("Checking token map on %s", i).isEqualTo(nodeOneTokens);

            cluster.coordinator(1).execute("CREATE KEYSPACE " + KEYSPACE +
                                           " WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 2, 'DC2' : 2};",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("CREATE TABLE " + KEYSPACE + ".t1 (k int PRIMARY KEY);", ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            List<Range<Token>> expectedRanges = new ArrayList<>(5);
            expectedRanges.add(range(Long.MIN_VALUE,-4611686018427387905L));
            expectedRanges.add(range(-4611686018427387905L,-3L));
            expectedRanges.add(range(-3L,4611686018427387899L));
            expectedRanges.add(range(4611686018427387899L,9223372036854775801L));
            expectedRanges.add(range(9223372036854775801L,Long.MIN_VALUE));

            for (int i = 1; i <= 4; i++)
            {
                // check both read and write placements for the test keyspace according to each node
                List[] placements = getDataPlacementDebugInfo(cluster.get(i)).get(KEYSPACE);
                for (Range<Token> range : expectedRanges)
                {
                    String replica = fullReplica(cluster.get(i), range);
                    assertTrue(String.format("%s does not contain %s",
                                             placements[0], replica),
                               placements[0].contains(replica));  // read placements
                    assertTrue(String.format("%s does not contain %s",
                                             placements[1], replica),
                               placements[1].contains(replica));  // write placements
                }
                assertEquals(20, placements[0].size());
                assertEquals(20, placements[1].size());
            }

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t1 (k) VALUES (0);", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("SELECT k FROM " + KEYSPACE + ".t1 WHERE k=0;", ConsistencyLevel.ALL);

            for (int i = 1; i <= 4; i++)
                assertRows(cluster.get(i).executeInternal("SELECT k FROM " + KEYSPACE + ".t1 WHERE k=0;"), row(0));

            cluster.coordinator(1).execute("SELECT k FROM " + KEYSPACE + ".t1;", ConsistencyLevel.ALL);
        }
    }

    private Range<Token> range(long start, long end)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(start), new Murmur3Partitioner.LongToken(end));
    }

    private String fullReplica(IInstance inst, Range<Token> range)
    {
        return Replica.fullReplica(InetAddressAndPort.getByAddress(inst.broadcastAddress()), range).toString();
    }

    // copied from BootstrapTest
    public static void populate(Cluster cluster, int from, int to)
    {
        populate(cluster, from, to, 1, 3, ConsistencyLevel.QUORUM);
    }

    public static void populate(Cluster cluster, int from, int to, int coord, int rf, ConsistencyLevel cl)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(coord));

        for (int i = from; i < to; i++)
        {
            cluster.coordinator(coord).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                               cl,
                                               i, i, i);
        }
    }
}
