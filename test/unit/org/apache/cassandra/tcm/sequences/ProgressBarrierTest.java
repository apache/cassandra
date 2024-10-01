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

package org.apache.cassandra.tcm.sequences;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.rng.PCGFastPure;
import org.apache.cassandra.harry.gen.rng.PcgRSUFast;
import org.apache.cassandra.harry.gen.rng.RngUtils;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.utils.concurrent.Future;

public class ProgressBarrierTest extends CMSTestBase
{
    static
    {
        DatabaseDescriptor.setRpcTimeout(10);
        DatabaseDescriptor.setProgressBarrierTimeout(1000);
        DatabaseDescriptor.setProgressBarrierBackoff(5);
    }

    @Test
    public void testProgressBarrier() throws Throwable
    {
        EntropySource rng = new PcgRSUFast(1L, 1l);
        Supplier<Boolean> respond = bools().toGenerator().bind(rng);
        Supplier<TokenPlacementModel.ReplicationFactor> rfs = combine(ints(0, 3),
                                                                      ints(1, 5),
                                                                      bools(),
                                                                      ints(1, 5),
                                                                      (Integer dcs, Integer nodesPerDc, Boolean addAlternate, Integer nodesPerDcAlt) -> {
                                                                         if (dcs == 0)
                                                                             return new TokenPlacementModel.SimpleReplicationFactor(nodesPerDc);
                                                                         else if (addAlternate && nodesPerDcAlt.intValue() != nodesPerDc.intValue())
                                                                         {
                                                                             int[] perDc = new int[dcs + 1];
                                                                             Arrays.fill(perDc, nodesPerDc);
                                                                             perDc[perDc.length - 1] = nodesPerDcAlt;
                                                                             return new TokenPlacementModel.NtsReplicationFactor(perDc);
                                                                         }
                                                                         else
                                                                         {
                                                                             return new TokenPlacementModel.NtsReplicationFactor(dcs, nodesPerDc);
                                                                         }
                                                                     })
                                                             .toGenerator()
                                                             .bind(rng);
        Supplier<Integer> nodes = ints(15, 20).toGenerator().bind(rng);
        Supplier<ConsistencyLevel> cls = Surjections.pick(
                                                    ConsistencyLevel.ALL,
                                                    ConsistencyLevel.QUORUM,
                                                    ConsistencyLevel.LOCAL_QUORUM,
                                                    ConsistencyLevel.EACH_QUORUM,
                                                    ConsistencyLevel.ONE).toGenerator()
                                                    .bind(rng);

        TokenPlacementModel.NodeFactory nodeFactory = TokenPlacementModel.nodeFactory();
        for (int run = 0; run < 100; run++)
        {
            TokenPlacementModel.ReplicationFactor rf = rfs.get();
            try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
            {
                List<TokenPlacementModel.Node> allNodes = new ArrayList<>();
                TokenPlacementModel.Node node = null;
                // + 1 since one of the nodes will not be joined yet by the time we create progress barrier, which will fail
                // a check with ALL.
                int nodesInCluster = Math.max(rf.total(), nodes.get()) + 1;
                for (int i = 1; i <= nodesInCluster; i++)
                {
                    node = nodeFactory.make(i, (i % rf.dcs()) + 1, 1);
                    allNodes.add(node);
                    sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
                    if (i < nodesInCluster)
                        sut.service.commit(new UnsafeJoin(node.nodeId(), Collections.singleton(node.longToken()), ClusterMetadataService.instance().placementProvider()));
                }

                sut.service.commit(new PrepareJoin(node.nodeId(), Collections.singleton(node.longToken()), ClusterMetadataService.instance().placementProvider(), true, false));

                for (int check = 0; check < 10; check++)
                {
                    ClusterMetadata metadata = ClusterMetadata.current();
                    ConsistencyLevel cl = cls.get();

                    Set<InetAddressAndPort> responded = new ConcurrentSkipListSet<>();
                    MessageDelivery delivery = new MessageDelivery()
                    {
                        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
                        {
                            // assert that it is a replcia
                            if (respond.get())
                            {
                                responded.add(to);
                                cb.onResponse((Message<RSP>) message.responseWith(message.epoch()));
                            }
                            else
                            {
                                cb.onFailure(message.from(), RequestFailureReason.TIMEOUT);
                            }
                        }

                        public <REQ> void send(Message<REQ> message, InetAddressAndPort to) {}
                        public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection) {}
                        public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to) { return null; }
                        public <V> void respond(V response, Message<?> message) {}
                    };
                    ProgressBarrier progressBarrier = ((MultiStepOperation<Epoch>)metadata.inProgressSequences.get(node.nodeId()))
                                                      .advance(metadata.epoch)
                                                      .barrier()
                                                      .withMessagingService(delivery);

                    progressBarrier.await(cl, metadata);

                    String dc = metadata.directory.location(node.nodeId()).datacenter;
                    switch (cl)
                    {
                        case ALL:
                        {
                            Set<InetAddressAndPort> replicas = metadata.lockedRanges.locked.get(LockedRanges.keyFor(metadata.epoch))
                                                                                           .toPeers(rf.asKeyspaceParams().replication, metadata.placements, metadata.directory)
                                                                                           .stream()
                                                                                           .map(n -> metadata.directory.getNodeAddresses(n).broadcastAddress)
                                                                                           .collect(Collectors.toSet());

                            Set<InetAddressAndPort> collected = responded.stream().filter(replicas::contains).collect(Collectors.toSet());
                            int expected = rf.total();
                            Assert.assertTrue(String.format("Should have collected at least %d nodes but got %d." +
                                                            "\nRF: %s" +
                                                            "\nReplicas: %s" +
                                                            "\nNodes:    %s", expected, collected.size(), rf, replicas, collected),
                                              collected.size() >= expected);

                            break;
                        }
                        case QUORUM:
                        {
                            Set<InetAddressAndPort> replicas = metadata.lockedRanges.locked.get(LockedRanges.keyFor(metadata.epoch))
                                                                                           .toPeers(rf.asKeyspaceParams().replication, metadata.placements, metadata.directory)
                                                                                           .stream()
                                                                                           .map(n -> metadata.directory.getNodeAddresses(n).broadcastAddress)
                                                                                           .collect(Collectors.toSet());

                            Set<InetAddressAndPort> collected = responded.stream().filter(replicas::contains).collect(Collectors.toSet());
                            int expected = rf.total() / 2 + 1;
                            Assert.assertTrue(String.format("Should have collected at least %d nodes but got %d." +
                                                            "\nRF: %s" +
                                                            "\nReplicas: %s" +
                                                            "\nNodes: %s", expected, collected.size(), rf, replicas, collected),
                                              collected.size() >= expected);

                            break;
                        }
                        case LOCAL_QUORUM:
                        {
                            List<InetAddressAndPort> replicas = new ArrayList<>(metadata.lockedRanges.locked.get(LockedRanges.keyFor(metadata.epoch))
                                                                                                            .toPeers(rf.asKeyspaceParams().replication, metadata.placements, metadata.directory)
                                                                                                            .stream()
                                                                                                            .filter((n) -> metadata.directory.location(n).datacenter.equals(dc))
                                                                                                            .map(n -> metadata.directory.getNodeAddresses(n).broadcastAddress)
                                                                                                            .collect(Collectors.toSet()));
                            replicas.sort(InetAddressAndPort::compareTo);
                            Set<InetAddressAndPort> collected = responded.stream().filter(replicas::contains).collect(Collectors.toSet());
                            int expected;
                            if (rf instanceof TokenPlacementModel.SimpleReplicationFactor)
                                expected = rf.total() / 2 + 1;
                            else
                                expected = rf.asMap().get(dc).totalCount / 2 + 1;
                            Assert.assertTrue(String.format("Should have collected at least %d nodes but got %d." +
                                                            "\nRF: %s" +
                                                            "\nReplicas: %s" +
                                                            "\nCollected: %s. Responded: %s", expected, collected.size(), rf, replicas, collected, responded),
                                              collected.size() >= expected);

                            break;
                        }
                        case EACH_QUORUM:
                        {
                            Map<String, Integer> byDc = new HashMap<>();
                            metadata.lockedRanges.locked.get(LockedRanges.keyFor(metadata.epoch))
                                                        .toPeers(rf.asKeyspaceParams().replication, metadata.placements, metadata.directory)
                                                        .forEach(n -> byDc.compute(metadata.directory.location(n).datacenter,
                                                                                   (k, v) -> v == null ? 1 : v + 1));

                            if (rf instanceof TokenPlacementModel.SimpleReplicationFactor)
                            {
                                int actual = byDc.get(dc);
                                int expected = rf.asMap().get(dc).totalCount / 2 + 1;
                                Assert.assertTrue(String.format("Shuold have collected at least %d nodes, but got %d." +
                                                                "\nRF: %s" +
                                                                "\nNodes: %s", expected, byDc.size(), rf, byDc),
                                                  actual >= expected);
                            }
                            else
                            {
                                for (Map.Entry<String, Integer> e : byDc.entrySet())
                                {
                                    int actual = e.getValue();
                                    int expected = rf.asMap().get(e.getKey()).totalCount / 2 + 1;
                                    Assert.assertTrue(String.format("Shuold have collected at least %d nodes, but got %d." +
                                                                    "\nRF: %s" +
                                                                    "\nNodes: %s", expected, byDc.size(), rf, byDc),
                                                      actual >= expected);
                                }
                            }
                            break;
                        }
                        case ONE:
                            Set<InetAddressAndPort> replicas = metadata.lockedRanges.locked.get(LockedRanges.keyFor(metadata.epoch))
                                                                                           .toPeers(rf.asKeyspaceParams().replication, metadata.placements, metadata.directory)
                                                                                           .stream()
                                                                                           .map(n -> metadata.directory.getNodeAddresses(n).broadcastAddress)
                                                                                           .collect(Collectors.toSet());

                            Assert.assertTrue(String.format("Should have collected at least one of the replicas %s, but got %s." +
                                                            "\nRF: %s.\nNodes: %s.",
                                                            replicas, responded, rf, allNodes),
                                              responded.stream().anyMatch(replicas::contains));
                            break;
                    }
                }

            }
        }
    }

    @Test
    public void testProgressBarrierDegradingConsistency() throws Throwable
    {
        TokenPlacementModel.ReplicationFactor rf = new TokenPlacementModel.NtsReplicationFactor(5, 5);
        TokenPlacementModel.NodeFactory nodeFactory = TokenPlacementModel.nodeFactory();

        DatabaseDescriptor.setProgressBarrierMinConsistencyLevel(ConsistencyLevel.ONE);
        try (CMSTestBase.CMSSut sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, rf))
        {
            TokenPlacementModel.Node node = null;
            for (int i = 1; i <= 4; i++)
            {
                node = nodeFactory.make(i, (i % rf.dcs()) + 1, 1);
                sut.service.commit(new Register(new NodeAddresses(node.addr()), new Location(node.dc(), node.rack()), NodeVersion.CURRENT));
                if (i < 4)
                    sut.service.commit(new UnsafeJoin(node.nodeId(), Collections.singleton(node.longToken()), ClusterMetadataService.instance().placementProvider()));
            }

            sut.service.commit(new PrepareJoin(node.nodeId(), Collections.singleton(node.longToken()), ClusterMetadataService.instance().placementProvider(), true, false));

            Set<InetAddressAndPort> responded = new ConcurrentSkipListSet<>();
            MessageDelivery delivery = new MessageDelivery()
            {
                AtomicInteger counter = new AtomicInteger();
                public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
                {
                    if (counter.getAndIncrement() == 0)
                    {
                        responded.add(to);
                        cb.onResponse((Message<RSP>) message.responseWith(message.epoch()));
                    }
                }

                public <REQ> void send(Message<REQ> message, InetAddressAndPort to) {}
                public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection) {}
                public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to) { return null; }

                @Override
                public <V> void respond(V response, Message<?> message) {}
            };

            ClusterMetadata metadata = ClusterMetadata.current();
            ProgressBarrier progressBarrier = ((MultiStepOperation<Epoch>) metadata.inProgressSequences.get(node.nodeId()))
                                              .advance(metadata.epoch)
                                              .barrier()
                                              .withMessagingService(delivery);
            long before = TCMMetrics.instance.progressBarrierRetries.getCount();
            progressBarrier.await();
            Assert.assertTrue(TCMMetrics.instance.progressBarrierRetries.getCount() - before > 0);
            Assert.assertTrue(responded.size() == 1);
        }
    }

    // TODO: move to a common lib? Is this a good idea?
    public static Surjections.Surjection<Integer> ints(int minInclusive, int maxInclusive)
    {
        return l -> RngUtils.asInt(l, minInclusive, maxInclusive);
    }

    public static Surjections.Surjection<Long> longs()
    {
        return l -> PCGFastPure.next(l, l);
    }

    public static Surjections.Surjection<Boolean> bools()
    {
        return RngUtils::asBoolean;
    }

    public static <T1, T2, T3, T4, RES> Surjections.Surjection<RES> combine(Surjections.Surjection<T1> gen1, Surjections.Surjection<T2> gen2,
                                                                            Surjections.Surjection<T3> gen3, Surjections.Surjection<T4> gen4,
                                                                            IIsolatedExecutor.QuadFunction<T1, T2, T3, T4, RES> res)
    {
        return (long l) -> {
            return res.apply(gen1.inflate(PCGFastPure.next(l, 1)),
                             gen2.inflate(PCGFastPure.next(l, 2)),
                             gen3.inflate(PCGFastPure.next(l, 2)),
                             gen4.inflate(PCGFastPure.next(l, 2)));
        };
    }
}