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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.PrepareMove;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.impl.TestEndpointCache.toCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GossipTest extends TestBaseImpl
{
    @Test
    public void testPreventStoppingGossipDuringBootstrap() throws Exception
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP)
                                                                    .with(NETWORK)
                                                                    .set("auto_bootstrap", true))
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            Callable<?> pending = ClusterUtils.pauseBeforeCommit(cluster.get(1), (e) -> e.kind() == Transformation.Kind.MID_JOIN);
            Thread startup = new Thread(() -> cluster.get(2).startup());
            startup.start();
            pending.call();

            cluster.get(2).runOnInstance(() -> {
                try
                {
                    StorageService.instance.stopGossiping();
                    fail("stopGossiping did not fail!");
                }
                catch (Exception ex)
                {
                    Assert.assertSame(ex.getClass(), IllegalStateException.class);
                    Assert.assertEquals(ex.getMessage(), "Unable to stop gossip because the node is not in the normal state. Try to stop the node instead.");
                }
            });
            ClusterUtils.unpauseCommits(cluster.get(1));
            startup.join();
        }
    }

    @Test
    public void testPreventEnablingGossipDuringMove() throws Exception
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(config -> config.with(GOSSIP)
                                                                    .with(NETWORK))
                                        .start())
        {
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.stopGossiping();
                ClusterMetadata metadata = ClusterMetadata.current();
                ClusterMetadataService.instance().commit(new PrepareMove(metadata.myNodeId(),
                                                                         Collections.singleton(metadata.partitioner.getRandomToken()),
                                                                         new UniformRangePlacement(),
                                                                         false));
                try
                {
                    StorageService.instance.startGossiping();

                    fail("startGossiping did not fail!");
                }
                catch (Exception ex)
                {
                    Assert.assertSame(ex.getClass(), IllegalStateException.class);
                    Assert.assertEquals(ex.getMessage(), "Unable to start gossip because the node is not in the normal state.");
                }
            });
        }
    }

    @Test
    public void gossipShutdownUpdatesTokenMetadata() throws Exception
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .withoutVNodes()
                                      .start())
        {
            init(cluster, 2);
            populate(cluster);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            // initiate a move for node2, which will not complete due to the
            // ByteBuddy interceptor we injected. Wait for the other two nodes
            // to mark node2 as moving before proceeding.
            long t2 = Long.parseLong(getLocalToken(node2));
            long t3 = Long.parseLong(getLocalToken(node3));
            long moveTo = t2 + ((t3 - t2)/2);
            Callable<?> pending = pauseBeforeCommit(node1, (e) -> e.kind() == Transformation.Kind.MID_MOVE);
            new Thread(() -> node2.nodetoolResult("move", "--", Long.toString(moveTo)).asserts().failure()).start();
            pending.call();

            InetSocketAddress movingAddress = node2.broadcastAddress();
            ClusterUtils.waitForCMSToQuiesce(cluster, node1);
            // node1 & node3 should now consider some ranges pending for node2
            assertPendingRangesForPeer(true, movingAddress, cluster);

            ClusterUtils.unpauseCommits(node1);
            node2.shutdown();

            node1.acceptsOnInstance((InetSocketAddress addr) -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                StorageService.instance.cancelInProgressSequences(metadata.directory.peerId(InetAddressAndPort.getByAddress(addr)));
            }).accept(node2.broadcastAddress());

            ClusterUtils.waitForCMSToQuiesce(cluster, node1);
            // node1 & node3 should not consider any ranges as still pending for node2
            assertPendingRangesForPeer(false, movingAddress, cluster);
        }
    }

    @Test
    public void restartGossipOnGossippingOnlyMember() throws Throwable
    {
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance gossippingOnlyMember = cluster.bootstrap(config);
            withProperty(JOIN_RING, false, () -> gossippingOnlyMember.startup(cluster));

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isGossipRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopGossiping());

            assertFalse(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                            () -> StorageService.instance.isGossipRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.startGossiping());

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isGossipRunning()));
        }
    }

    private static String getLocalToken(IInvokableInstance node)
    {
        Collection<String> tokens = ClusterUtils.getLocalTokens(node);
        Assertions.assertThat(tokens).hasSize(1);
        return tokens.stream().findFirst().get();
    }

    void assertPendingRangesForPeer(final boolean expectPending, final InetSocketAddress movingAddress, final Cluster cluster)
    {
        for (IInvokableInstance inst : new IInvokableInstance[]{ cluster.get(1), cluster.get(3)})
        {
            boolean hasPending = inst.appliesOnInstance((InetSocketAddress address) -> {
                InetAddressAndPort peer = toCassandraInetAddressAndPort(address);

                boolean isMoving = StorageService.instance.endpointsWithState(NodeState.MOVING)
                                                          .stream()
                                                          .anyMatch(peer::equals);

                return isMoving && ClusterMetadata.current().hasPendingRangesFor(Keyspace.open(KEYSPACE).getMetadata(), peer);
            }).apply(movingAddress);
            assertEquals(String.format("%s should %shave PENDING RANGES for %s",
                                       inst.broadcastAddress().getHostString(),
                                       expectPending ? "" : "not ",
                                       movingAddress),
                         hasPending, expectPending);
        }
    }

    static void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)",
                                           ConsistencyLevel.ALL,
                                           i);
        }
    }

    @Test
    public void testQuarantine() throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(4, 1);
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set("progress_barrier_default_consistency_level", "ONE")
                                                        .set("progress_barrier_min_consistency_level", "ONE"))
                                      .withTokenSupplier(node -> even.token(node == 5 ? 4 : node))
                                      .start())
        {
            // 4 nodes, stop node3 from catching up
            cluster.filters().verbs(Verb.TCM_REPLICATION.id).to(3).drop();
            IInvokableInstance toRemove = cluster.get(4);
            String node4 = toRemove.config().broadcastAddress().getAddress().getHostAddress();
            stopUnchecked(toRemove);
            replaceHostAndStart(cluster, toRemove);
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS); // wait a few gossip rounds
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1), 4);
            cluster.get(2).runOnInstance(() -> assertFalse(Gossiper.instance.endpointStateMap.containsKey(InetAddressAndPort.getByNameUnchecked(node4))));
            cluster.get(3).runOnInstance(() -> assertFalse(Gossiper.instance.endpointStateMap.containsKey(InetAddressAndPort.getByNameUnchecked(node4))));
            cluster.get(3).nodetoolResult("disablegossip").asserts().success();
            cluster.get(3).nodetoolResult("enablegossip").asserts().success();
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            cluster.get(2).runOnInstance(() -> assertFalse(Gossiper.instance.endpointStateMap.containsKey(InetAddressAndPort.getByNameUnchecked(node4))));
            cluster.filters().reset();
            long epoch = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(epoch)));
            boolean removed = false;
            for (int i = 0; i < 20; i++)
            {
                if (!cluster.get(3).callOnInstance(() -> Gossiper.instance.endpointStateMap.containsKey(InetAddressAndPort.getByNameUnchecked(node4))))
                {
                    removed = true;
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            assertTrue(removed);
        }
    }
}
