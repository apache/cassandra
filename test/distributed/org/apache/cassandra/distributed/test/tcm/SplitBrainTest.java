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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.log.LogState;
import org.awaitility.Awaitility;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SplitBrainTest extends TestBaseImpl
{
    @Test
    public void testSplitBrainStartup() throws IOException, TimeoutException
    {
        try (Setup setup = setupSplitBrainCluster())
        {
            Cluster cluster = setup.cluster;
            // Perform a schema change on one of the clusters resulting from the split brain during initialisation
            // before dropping message filters. When comms can be reestablished, we fake the replication of metdata
            // state from on cluster to the other.
            cluster.coordinator(1).execute(withKeyspace("create keyspace %s with replication = {'class':'SimpleStrategy', 'replication_factor':1}"), ConsistencyLevel.ALL);
            long clusterOneEpoch = ClusterUtils.getCurrentEpoch(cluster.get(1)).getEpoch();
            long clusterTwoEpoch = ClusterUtils.getCurrentEpoch(cluster.get(3)).getEpoch();
            assertTrue(clusterOneEpoch > clusterTwoEpoch);

            // Artificially induce node1 to replicate to node3. This should be rejected by node3 as the two technically
            // belong to different clusters.
            long mark = cluster.get(3).logs().mark();
            // Turn off the initial filters
            setup.reenableCommunication();

            cluster.get(1).runOnInstance(() -> {
                LogState state = LogState.getForRecovery(ClusterMetadata.current().epoch);
                MessagingService.instance().send(Message.out(Verb.TCM_REPLICATION, state),
                                                 InetAddressAndPort.getByNameUnchecked("127.0.0.3"));
            });
            cluster.get(3).logs().watchFor(mark, Duration.ofSeconds(10), "Cluster Metadata Identifier mismatch");
            assertEquals(clusterOneEpoch, ClusterUtils.getCurrentEpoch(cluster.get(1)).getEpoch());
            assertEquals(clusterTwoEpoch, ClusterUtils.getCurrentEpoch(cluster.get(3)).getEpoch());
        }
    }


    @Test
    public void testFilterGossipStatesWithMismatchingMetadataId() throws IOException, TimeoutException
    {
        try (Setup setup = setupSplitBrainCluster())
        {
            Cluster cluster = setup.cluster;
            // Allow nodes from the two clusters to communicate again. Because each node's seed list contains an
            // instance from the other cluster, they will attempt to perform gossip exchange with that instance.
            // Verify that when this happens, gossip state isn't updated with instances from the other cluster.
            AtomicInteger node1Received = new AtomicInteger(0);
            AtomicInteger node3Received = new AtomicInteger(0);

            cluster.filters().inbound().from(1,2,3,4).to(1,2,3,4).messagesMatching((from, to, msg) -> {
                if (msg.verb() == Verb.GOSSIP_DIGEST_SYN.id ||
                    msg.verb() == Verb.GOSSIP_DIGEST_ACK.id ||
                    msg.verb() == Verb.GOSSIP_DIGEST_ACK2.id)
                {
                    if (to == 1 && (from == 3 || from == 4))
                        node1Received.incrementAndGet();
                    if (to == 3 && (from == 1 || from == 2))
                        node3Received.incrementAndGet();
                }
                return false;
            }).drop().on();

            // Turn off the initial filters
            setup.reenableCommunication();

            // Wait for cross-cluster gossip communication
            Awaitility.await()
                      .atMost(Duration.ofSeconds(30))
                      .until(() -> node1Received.get() > 5 && node3Received.get() > 5);

            // Verify that gossip states for nodes which are not a member of the same cluster were disregarded.
            // Each node should have gossip state only for itself and the one other member of its cluster.
            cluster.forEach(inst -> {
                int id = inst.config().num();
                boolean gossipStateValid = inst.callOnInstance((() -> {
                    Map<InetAddressAndPort, EndpointState> eps = Gossiper.instance.endpointStateMap;
                    if (eps.size() != 2)
                        return false;
                    Collection<InetAddressAndPort> expectedEps = (id <= 2)
                                                                 ? Arrays.asList(InetAddressAndPort.getByNameUnchecked("127.0.0.1"),
                                                                                 InetAddressAndPort.getByNameUnchecked("127.0.0.2"))
                                                                 : Arrays.asList(InetAddressAndPort.getByNameUnchecked("127.0.0.3"),
                                                                                 InetAddressAndPort.getByNameUnchecked("127.0.0.4"));
                    return eps.keySet().containsAll(expectedEps);
                }));
                Assert.assertTrue(String.format("Unexpected gossip state on node %s", id), gossipStateValid);
            });
        }
    }

    private Setup setupSplitBrainCluster() throws IOException
    {
        // partition the cluster in 2 parts on startup, node1, node2 in one, node3, node4 in the other
        Cluster cluster = builder().withNodes(4)
                                   .withConfig(config -> config.with(GOSSIP).with(NETWORK)
                                                               .set("seed_provider", new IInstanceConfig.ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                                                                            Collections.singletonMap("seeds", "127.0.0.1,127.0.0.3")))
                                                               .set("discovery_timeout", "1s"))
                                   .createWithoutStarting();
        IMessageFilters.Filter drop1 = cluster.filters().allVerbs().from(1, 2).to(3, 4).drop();
        IMessageFilters.Filter drop2 = cluster.filters().allVerbs().from(3, 4).to(1, 2).drop();
        List<Thread> startupThreads = new ArrayList<>(4);
        for (int i = 0; i < 4; i++)
        {
            int threadNr = i + 1;
            startupThreads.add(new Thread(() -> cluster.get(threadNr).startup()));
        }
        startupThreads.forEach(Thread::start);
        startupThreads.forEach(SplitBrainTest::join);
        return new Setup(cluster, drop1, drop2);
    }

    private final class Setup implements AutoCloseable
    {
        final Cluster cluster;
        final IMessageFilters.Filter[] filters;

        Setup(Cluster cluster, IMessageFilters.Filter ... filters)
        {
            this.cluster = cluster;
            this.filters = filters;
        }

        void reenableCommunication()
        {
            for (IMessageFilters.Filter filter : filters)
                filter.off();
        }

        @Override
        public void close()
        {
           cluster.close();
        }
    }

    private static void join(Thread t)
    {
        try
        {
            t.join();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

}
