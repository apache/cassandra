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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end coverage of {@code nodetool placement peers} on a multi-datacenter cluster, including the two properties
 * which motivate the interface: it can answer about a node it cannot reach, and it does not depend on the native
 * protocol. Also covers {@code system_views.replica_peers}.
 */
public class PlacementPeersTest extends TestBaseImpl
{
    private static final String KEYSPACE = "placement_peers_test";
    private static final String DC1 = "datacenter1";
    private static final String DC2 = "datacenter2";

    @Test
    public void testNodetoolReportsEveryNodeWithPeersByDc() throws Throwable
    {
        // deliberately no NATIVE_PROTOCOL: the interface must not depend on it
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);

            NodeToolResult result = cluster.get(1).nodetoolResult("placement", "peers", "-k", KEYSPACE);
            result.asserts().success();

            String output = result.getStdout();
            assertThat(output).contains("epoch:", "endpoints:", "peers:", "read:", "write:");
            // every node is reported on, and each has peers in both datacenters
            assertThat(occurrences(output, "read:")).isEqualTo(4);
            assertThat(output).contains(DC1).contains(DC2);
        }
    }

    @Test
    public void testNodetoolReportsASingleNodeByAddress() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);

            String node3Address = cluster.get(3).broadcastAddress().getAddress().getHostAddress();
            NodeToolResult result = cluster.get(1).nodetoolResult("placement", "peers", "-n", node3Address, "-k", KEYSPACE);
            result.asserts().success();
            assertThat(occurrences(result.getStdout(), "read:")).isEqualTo(1);
        }
    }

    @Test
    public void testNodetoolRestrictsNodesReportedToGivenDc() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);

            NodeToolResult result = cluster.get(1).nodetoolResult("placement", "peers", "-k", KEYSPACE, "-dc", DC1);
            result.asserts().success();

            String output = result.getStdout();
            // only the two nodes of the requested datacenter are reported on, but with their peers in both
            assertThat(occurrences(output, "read:")).isEqualTo(2);
            assertThat(output).contains(DC1, DC2);
        }
    }

    @Test
    public void testNodetoolAnswersAboutANodeItCannotReach() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);
            String node2Address = cluster.get(2).broadcastAddress().getAddress().getHostAddress();
            cluster.get(2).shutdown().get();

            // automation orchestrates from one entry point, so it must be able to ask about a node which is down
            NodeToolResult result = cluster.get(1).nodetoolResult("placement", "peers", "-n", node2Address, "-k", KEYSPACE);
            result.asserts().success();
            assertThat(occurrences(result.getStdout(), "read:")).isEqualTo(1);
            assertThat(result.getStdout()).contains(DC1, DC2);
        }
    }

    @Test
    public void testNodetoolSupportsJsonFormat() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);

            NodeToolResult result = cluster.get(1).nodetoolResult("placement", "peers", "-k", KEYSPACE,
                                                                  "-f", "minified-json");
            result.asserts().success();
            assertThat(result.getStdout()).contains("\"peers\"").contains("\"endpoints\"");
        }
    }

    @Test
    public void testNodetoolRejectsAnUnknownNode() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);
            cluster.get(1).nodetoolResult("placement", "peers", "-n", "127.99.99.99").asserts().failure();
        }
    }

    @Test
    public void testNodetoolCatchUpReportsTheLatestEpoch() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);
            // Precondition: node 1 is the CMS, so its epoch is the latest one.
            assertThat(cluster.get(1).callOnInstance(() -> ClusterMetadata.current().isCMSMember())).isTrue();

            // Keep node 2 behind: it receives no replicated log entries and cannot fetch them from anyone.
            cluster.filters().verbs(Verb.TCM_REPLICATION.id).to(2).drop();
            cluster.filters().verbs(Verb.TCM_FETCH_PEER_LOG_REQ.id).from(2).drop();
            IMessageFilters.Filter cmsFetch = cluster.filters().verbs(Verb.TCM_FETCH_CMS_LOG_REQ.id).from(2).drop();

            cluster.get(1).schemaChangeInternal("CREATE TABLE " + KEYSPACE + ".catch_up (k int PRIMARY KEY)");
            long latest = epochOf(cluster.get(1));
            // Precondition: node 2 has not seen the latest epoch.
            assertThat(epochOf(cluster.get(2))).isLessThan(latest);

            // Let node 2 reach the CMS again, so that the catch-up can succeed.
            cmsFetch.off();
            NodeToolResult result = cluster.get(2).nodetoolResult("placement", "peers", "-k", KEYSPACE,
                                                                  "--catch-up", "-f", "minified-json");
            result.asserts().success();

            Matcher epochInResponse = Pattern.compile("\"epoch\"\\s*:\\s*(\\d+)").matcher(result.getStdout());
            assertThat(epochInResponse.find()).isTrue();
            assertThat(Long.parseLong(epochInResponse.group(1))).isEqualTo(latest);
            assertThat(epochOf(cluster.get(2))).isEqualTo(latest);
        }
    }

    @Test
    public void testReplicaPeersTableReportsEachNodesReplicaPeers() throws Throwable
    {
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                        .start())
        {
            createKeyspace(cluster);

            // id, datacenter and address of every node, as each node itself sees them
            List<List<Object>> nodes = new ArrayList<>();
            for (int i = 1; i <= cluster.size(); i++)
                nodes.add(cluster.get(i).callOnInstance(() -> {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    NodeId id = metadata.myNodeId();
                    return List.of(id.id(),
                                   metadata.directory.location(id).datacenter,
                                   FBUtilities.getBroadcastAddressAndPort().getHostAddress(true));
                }));

            for (int i = 1; i <= cluster.size(); i++)
            {
                IInvokableInstance instance = cluster.get(i);
                int self = (Integer) nodes.get(i - 1).get(0);
                Object[][] rows = instance.executeInternal("SELECT node_id, peer_dc, peer_id, peer_address, read_peer, write_peer, epoch " +
                                                           "FROM system_views.replica_peers");
                long epochAfter = instance.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());

                // two nodes per datacenter at RF 2 each, so every node shares data with all the others, ordered by
                // datacenter and then node id
                List<List<Object>> expected = new ArrayList<>();
                for (List<Object> node : nodes)
                {
                    if ((Integer) node.get(0) != self)
                        expected.add(List.of(self, node.get(1), node.get(0), node.get(2), true, true));
                }
                expected.sort(Comparator.comparing((List<Object> row) -> (String) row.get(1))
                                        .thenComparing(row -> (Integer) row.get(2)));

                // a node only ever answers for itself
                List<List<Object>> actual = new ArrayList<>();
                for (Object[] row : rows)
                {
                    actual.add(List.of(row[0], row[1], row[2], row[3], row[4], row[5]));
                    assertThat((Long) row[6]).isPositive().isLessThanOrEqualTo(epochAfter);
                }
                assertThat(actual).isEqualTo(expected);
            }
        }
    }

    private static long epochOf(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
    }

    private static void createKeyspace(Cluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                             "{'class': 'NetworkTopologyStrategy', '" + DC1 + "': 2, '" + DC2 + "': 2}");
    }

    private static int occurrences(String haystack, String needle)
    {
        int count = 0;
        for (int idx = haystack.indexOf(needle); idx >= 0; idx = haystack.indexOf(needle, idx + needle.length()))
            count++;
        return count;
    }
}
