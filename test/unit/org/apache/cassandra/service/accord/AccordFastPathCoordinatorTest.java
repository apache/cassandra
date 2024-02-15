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

package org.apache.cassandra.service.accord;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.accord.AccordFastPath.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.service.accord.AccordTestUtils.*;

public class AccordFastPathCoordinatorTest 
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static ClusterMetadata EMPTY;


    public static final TableId TABLE_1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        EMPTY = new ClusterMetadata(partitioner);
    }

    private static class CapturedUpdate
    {
        final Node.Id node;
        final Status status;

        public CapturedUpdate(Node.Id node, Status status)
        {
            this.node = node;
            this.status = status;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedUpdate that = (CapturedUpdate) o;
            return Objects.equals(node, that.node) && status == that.status;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node, status);
        }

        @Override
        public String toString()
        {
            return "CapturedUpdate{" +
                    "node=" + node +
                    ", status=" + status +
                    '}';
        }
    }

    private static CapturedUpdate update(Node.Id node, Status status)
    {
        return new CapturedUpdate(node, status);
    }

    private static class InstrumentedFastPathCoordinator extends AccordFastPathCoordinator
    {
        private ClusterMetadata currentMetadata = EMPTY;
        private List<CapturedUpdate> capturedUpdates = new ArrayList<>();

        public InstrumentedFastPathCoordinator(Node.Id localId)
        {
            super(localId);
        }

        public InstrumentedFastPathCoordinator currentMetadata(ClusterMetadata currentMetadata)
        {
            this.currentMetadata = currentMetadata;
            return this;
        }

        @Override
        ClusterMetadata currentMetadata()
        {
            return currentMetadata;
        }

        @Override
        void registerAsListener()
        {

        }

        @Override
        void updateFastPath(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis)
        {
            capturedUpdates.add(new CapturedUpdate(node, status));

        }

        @Override
        long getAccordFastPathUpdateDelayMillis()
        {
            return TimeUnit.SECONDS.toMillis(5);
        }
    }

    @Test
    public void simpleAlive()
    {
        Topology topology = new Topology(1,
                new Shard(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                new Shard(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));

        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        coordinator.updatePeers(topology);

        // setup existing fast path state
        coordinator.currentMetadata(EMPTY.transformer()
                                         .withFastPathStatusSince(id(1), Status.UNAVAILABLE, 1, 1)
                                         .withFastPathStatusSince(id(3), Status.UNAVAILABLE, 1, 1).build().metadata);

        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());


        // peer isn't marked unavailable, shouldn't update
        coordinator.onAlive(id(2));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // node isn't a peer, shouldn't update
        coordinator.onAlive(id(3));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // node is a peer, should issue update
        coordinator.onAlive(id(1));
        Assert.assertEquals(update(id(1), Status.NORMAL), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    @Test
    public void simpleDead()
    {
        Topology topology = new Topology(1,
                new Shard(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                new Shard(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));
        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        coordinator.updatePeers(topology);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // not a peer, shouldn't update
        coordinator.onDead(id(3));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // is a peer, should update
        coordinator.onDead(id(1));
        Assert.assertEquals(update(id(1), Status.UNAVAILABLE), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    /**
     * We shouldn't be scheduling updates if there aren't any accord tables
     */
    @Test
    public void noTableTest()
    {
        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        coordinator.start();
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        coordinator.onDead(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
    }

    /**
     * node should mark itself as shutdown on shutdown
     */
    @Test
    public void selfShutdownTest()
    {
        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        coordinator.onShutdown();
        Assert.assertEquals(update(id(0), Status.SHUTDOWN), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    /**
     * If a node finds itself marked shutdown on startup, it should mark itself normal
     */
    @Test
    public void startupTest()
    {
        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        coordinator.currentMetadata(EMPTY.transformer().withFastPathStatusSince(id(0), Status.SHUTDOWN, 1, 1).build().metadata);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.start();
        Assert.assertEquals(update(id(0), Status.NORMAL), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    /**
     * if a peer is marked as shutdown, other nodes should ignore FD signals until it marks itself alive again
     */
    @Test
    public void peerShutdownTest()
    {
        Topology topology = new Topology(1,
                new Shard(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                new Shard(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));
        InstrumentedFastPathCoordinator coordinator = new InstrumentedFastPathCoordinator(id(0));
        coordinator.currentMetadata(EMPTY.transformer().withFastPathStatusSince(id(1), Status.SHUTDOWN, 1, 1).build().metadata);
        coordinator.updatePeers(topology);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.start();

        Assert.assertTrue(coordinator.isPeer(id(1)));
        coordinator.onAlive(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.onDead(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
    }
}
