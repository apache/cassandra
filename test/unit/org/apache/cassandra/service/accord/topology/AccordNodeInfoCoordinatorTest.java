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

package org.apache.cassandra.service.accord.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.transformations.AccordChangeDownStatus;
import org.apache.cassandra.tcm.transformations.AccordChangeNodeInfo;
import org.apache.cassandra.tcm.transformations.AccordMarkHardRemoved;
import org.apache.cassandra.tcm.transformations.AccordMarkStale;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.accord.AccordTestUtils.id;
import static org.apache.cassandra.service.accord.AccordTestUtils.idList;
import static org.apache.cassandra.service.accord.AccordTestUtils.idSet;
import static org.apache.cassandra.service.accord.AccordTestUtils.token;

public class AccordNodeInfoCoordinatorTest
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

    private static class CapturedDeclaration
    {
        final Node.Id node;
        final AccordNodeInfo.Delta delta;

        CapturedDeclaration(Node.Id node, AccordNodeInfo.Delta delta)
        {
            this.node = node;
            this.delta = delta;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CapturedDeclaration that = (CapturedDeclaration) o;
            return Objects.equals(node, that.node) && Objects.equals(delta, that.delta);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(node, delta);
        }

        @Override
        public String toString()
        {
            return "CapturedDeclaration{node=" + node + ", delta=" + delta + '}';
        }
    }

    private static CapturedDeclaration declaration(Node.Id node, AccordNodeInfo.Delta delta)
    {
        return new CapturedDeclaration(node, delta);
    }

    /** what start() declares: nothing may be sent to us until our command stores can receive it */
    private static AccordNodeInfo.Delta startupDeclaration(ClusterMetadata cm)
    {
        return AccordNodeInfo.Delta.status(Status.SHUTDOWN);
    }

    /** what acceptingRequests() declares: send to us, but do not read from us or wait for us */
    private static AccordNodeInfo.Delta acceptingDeclaration(ClusterMetadata cm)
    {
        return AccordNodeInfos.supportsExtendedNodeInfo(cm)
               ? AccordNodeInfo.Delta.status(Status.NORMAL).combine(AccordNodeInfo.Delta.unreadable(true))
               : AccordNodeInfo.Delta.status(Status.NORMAL);
    }

    private static class InstrumentedNodeInfoCoordinator extends AccordNodeInfoCoordinator
    {
        private ClusterMetadata currentMetadata = EMPTY;
        private List<CapturedUpdate> capturedUpdates = new ArrayList<>();
        private boolean applyUpdates = true;
        private boolean isMember = true;

        public InstrumentedNodeInfoCoordinator(Node.Id localId)
        {
            super(localId, AccordEndpointInfos.EMPTY);
        }

        public InstrumentedNodeInfoCoordinator currentMetadata(ClusterMetadata currentMetadata)
        {
            this.currentMetadata = currentMetadata;
            return this;
        }

        @Override
        void registerListeners()
        {
        }

        @Override
        boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        ClusterMetadata currentMetadata()
        {
            return currentMetadata;
        }

        @Override
        void updateDownStatusAsync(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis)
        {
            updateDownStatus(node, status, updateTimeMillis, updateDelayMillis, Long.MAX_VALUE);
        }

        @Override
        void updateDownStatus(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis, long deadlineLimitNanos)
        {
            capturedUpdates.add(new CapturedUpdate(node, status));
            if (applyUpdates)
                currentMetadata = currentMetadata.transformer().withAccordDownStatusSince(node, status, updateTimeMillis, updateDelayMillis).build().metadata;
        }

        private final List<CapturedDeclaration> capturedDeclarations = new ArrayList<>();

        @Override
        void declareSelf(AccordNodeInfo.Delta delta, long updatedTimeMillis, long deadlineLimitNanos)
        {
            capturedDeclarations.add(new CapturedDeclaration(localId, delta));
            if (applyUpdates)
                currentMetadata = currentMetadata.transformer().withAccordNodeInfo(localId, delta, updatedTimeMillis).build().metadata;
        }

        @Override
        boolean isMember(ClusterMetadata metadata)
        {
            return isMember;
        }

        @Override
        long getAccordFastPathUpdateDelayMillis()
        {
            return TimeUnit.SECONDS.toMillis(5);
        }

        @Override
        Boolean isAlive(Node.Id node)
        {
            return observed.getOrDefault(node, Status.NORMAL) == Status.NORMAL;
        }

        private final Map<Node.Id, Status> observed = new HashMap<>();

        InstrumentedNodeInfoCoordinator observe(Node.Id node, Status status)
        {
            observed.put(node, status);
            return this;
        }
    }

    @Test
    public void simpleAlive()
    {
        Topology topology = new Topology(1,
                Shard.create(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                Shard.create(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));

        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0));
        coordinator.updatePeers(topology);

        // setup existing fast path state: every member has an entry, as the directory gives them one on registration
        coordinator.currentMetadata(EMPTY.transformer()
                                         .withAccordDownStatusSince(id(1), Status.MAYBE_DOWN, 1, 1)
                                         .withAccordDownStatusSince(id(2), Status.NORMAL, 1, 1)
                                         .withAccordDownStatusSince(id(3), Status.MAYBE_DOWN, 1, 1).build().metadata);

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
                Shard.create(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                Shard.create(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0));
        coordinator.updatePeers(topology);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // not a peer, shouldn't update
        coordinator.onDead(id(3));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // is a peer, should update
        coordinator.onDead(id(1));
        Assert.assertEquals(update(id(1), Status.MAYBE_DOWN), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    /**
     * We shouldn't be scheduling updates if there aren't any accord tables
     */
    @Test
    public void noTableTest()
    {
        ClusterMetadata metadata = metadataSupportingDeltas();
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(metadata);
        coordinator.start();
        // startup always declares something for us, whether or not there is anything to coordinate for
        Assert.assertEquals(declaration(id(0), startupDeclaration(metadata)), Iterables.getOnlyElement(coordinator.capturedDeclarations));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.capturedDeclarations.clear();

        coordinator.onDead(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
    }

    /**
     * node should mark itself as shutdown on shutdown
     */
    @Test
    public void selfShutdownTest()
    {
        ClusterMetadata metadata = metadataWithMinVersion(AccordNodeInfos.MIN_VERSION.toString());
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(metadata);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        coordinator.onShutdown();
        // only we can say we are shutting down, so it is a declaration rather than an inferred down status
        Assert.assertEquals(declaration(id(0), AccordNodeInfo.Delta.status(Status.SHUTDOWN)),
                            Iterables.getOnlyElement(coordinator.capturedDeclarations));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
    }

    /**
     * On startup a node declares itself recovering - it can receive transactions but must not be read from - and only
     * marks itself normal once Accord is ready to serve
     */
    @Test
    public void startupTest()
    {
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0));
        // nothing recorded for us yet, so each declaration below is a change and is issued (see
        // declarationAlreadyRecordedIsSkipped for the converse)
        ClusterMetadata metadata = metadataSupportingDeltas();
        coordinator.currentMetadata(metadata);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.start();
        Assert.assertEquals(declaration(id(0), startupDeclaration(metadata)), Iterables.getOnlyElement(coordinator.capturedDeclarations));

        coordinator.capturedDeclarations.clear();
        coordinator.declareStartedUnreadable();
        Assert.assertEquals(declaration(id(0), acceptingDeclaration(metadata)), Iterables.getOnlyElement(coordinator.capturedDeclarations));

        coordinator.capturedDeclarations.clear();
        coordinator.declareReady();
        // ready() clears the unreadable bit, and says nothing about any other field
        Assert.assertEquals(declaration(id(0), AccordNodeInfo.Delta.status(Status.NORMAL).combine(AccordNodeInfo.Delta.unreadable(false))),
                            Iterables.getOnlyElement(coordinator.capturedDeclarations));
    }

    /**
     * UNREADABLE is only safe to declare once every node can interpret it; before that we must say NORMAL, or a peer
     * that does not know the status would keep reading from us
     */
    @Test
    public void nodeInfoDeltasAreVersionGated()
    {
        Assert.assertFalse(AccordNodeInfos.supportsExtendedNodeInfo(metadataWithMinVersion("6.0-alpha2")));
        Assert.assertTrue(AccordNodeInfos.supportsExtendedNodeInfo(metadataWithVersion(NodeVersion.CURRENT)));
        Assert.assertTrue(AccordNodeInfos.supportsExtendedNodeInfo(metadataWithMinVersion("6.1")));
    }

    /**
     * A node info below MIN_VERSION serializes byte-identically to the status-only form an older peer expects, and that
     * peer's deserializer rejects any status byte it does not know - so no transformation that would record a status
     * above MAYBE_DOWN, or either of the extra bits, may be committed until the whole cluster is above it.
     */
    @Test
    public void extendedNodeInfoIsVersionGated()
    {
        ClusterMetadata old = metadataWithMinVersion("6.0-alpha2");
        ClusterMetadata current = metadataSupportingDeltas();
        Assert.assertFalse(AccordNodeInfos.supportsExtendedNodeInfo(old.directory));
        Assert.assertTrue(AccordNodeInfos.supportsExtendedNodeInfo(current.directory));

        // the STALE bit and the HARD_REMOVED status are each unreadable to a peer on the older version
        Assert.assertFalse(new AccordMarkStale(Collections.singleton(new NodeId(1))).eligibleToCommit(old));
        Assert.assertFalse(new AccordMarkHardRemoved(Collections.singleton(new NodeId(1)), true).eligibleToCommit(old));
        Assert.assertFalse(new AccordChangeNodeInfo(id(1), AccordNodeInfo.Delta.unreadable(true), 1).eligibleToCommit(old));

        Assert.assertTrue(new AccordMarkStale(Collections.singleton(new NodeId(1))).eligibleToCommit(current));
        Assert.assertTrue(new AccordMarkHardRemoved(Collections.singleton(new NodeId(1)), true).eligibleToCommit(current));
        Assert.assertTrue(new AccordChangeNodeInfo(id(1), AccordNodeInfo.Delta.unreadable(true), 1).eligibleToCommit(current));

        // and the statuses a peer on the older version does understand are still recorded as it goes
        Assert.assertEquals(Status.MAYBE_DOWN,
                            old.transformer().withAccordDownStatusSince(id(1), Status.MAYBE_DOWN, 1, 1).build()
                               .metadata.accordNodeInfos.status(id(1)));
    }

    /**
     * A node still running an older version cannot deserialize the delta transformation at all, so below the min version
     * we must send nothing but the older status-only message
     */
    @Test
    public void sendsNoDeltaBelowMinVersion()
    {
        ClusterMetadata old = metadataWithMinVersion("6.0-alpha2");
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(old);

        coordinator.start();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        Assert.assertEquals(update(id(0), Status.SHUTDOWN), Iterables.getOnlyElement(coordinator.capturedUpdates));
        coordinator.capturedUpdates.clear();

        coordinator.declareStartedUnreadable();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        Assert.assertEquals(update(id(0), Status.NORMAL), Iterables.getOnlyElement(coordinator.capturedUpdates));
        coordinator.capturedUpdates.clear();

        coordinator.declareReady();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        // the status-only message cannot express the unreadable bit, and the status it can express is unchanged, so
        // there is nothing left to say
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        coordinator.onShutdown();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        Assert.assertEquals(update(id(0), Status.SHUTDOWN), Iterables.getOnlyElement(coordinator.capturedUpdates));
    }

    /**
     * A declaration is verified against the metadata we can see, not against the epoch our transformation produced - a
     * commit only tells us the log has reached at least that epoch - so a peer inferring that we are down in between must
     * neither stop us declaring what is ours to declare nor fail our startup.
     */
    @Test
    public void declarationToleratesAPeerInferringWeAreDown()
    {
        ClusterMetadata start = metadataSupportingDeltas();
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(start);
        coordinator.start();
        coordinator.capturedDeclarations.clear();

        // our declaration took, but by the time we look a peer's failure detector has recorded us MAYBE_DOWN; the field
        // that is ours alone - the unreadable bit - is as we declared it
        coordinator.applyUpdates = false;
        coordinator.currentMetadata(coordinator.currentMetadata()
                                               .transformer()
                                               .withAccordNodeInfo(id(0), AccordNodeInfo.Delta.status(Status.MAYBE_DOWN).combine(AccordNodeInfo.Delta.unreadable(true)), 5)
                                               .build().metadata);
        coordinator.declareStartedUnreadable();
        Assert.assertEquals(declaration(id(0), acceptingDeclaration(start)), Iterables.getOnlyElement(coordinator.capturedDeclarations));
        coordinator.capturedDeclarations.clear();

        // and a field only we may set, recorded as something else, is still declared - not contested, not fatal
        coordinator.declareReady();
        Assert.assertEquals(declaration(id(0), AccordNodeInfo.Delta.status(Status.NORMAL).combine(AccordNodeInfo.Delta.unreadable(false))),
                            Iterables.getOnlyElement(coordinator.capturedDeclarations));
    }

    /**
     * A declaration is best effort: the commit is bounded and its outcome only logged, so a declaration the cluster does
     * not end up recording must not fail startup, and must not be retried in a loop either.
     */
    @Test
    public void declarationThatDoesNotTakeIsNotFatal()
    {
        ClusterMetadata start = metadataSupportingDeltas();
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(start);
        coordinator.applyUpdates = false;

        coordinator.start();
        // issued exactly once, whether or not it took
        Assert.assertEquals(declaration(id(0), startupDeclaration(start)), Iterables.getOnlyElement(coordinator.capturedDeclarations));
        coordinator.capturedDeclarations.clear();
        coordinator.declareReady();
        Assert.assertEquals(declaration(id(0), AccordNodeInfo.Delta.status(Status.NORMAL).combine(AccordNodeInfo.Delta.unreadable(false))),
                            Iterables.getOnlyElement(coordinator.capturedDeclarations));

        // below the min version the same is true of the status-only message
        InstrumentedNodeInfoCoordinator old = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(metadataWithMinVersion("6.0-alpha2"));
        old.applyUpdates = false;
        old.start();
        Assert.assertEquals(update(id(0), Status.SHUTDOWN), Iterables.getOnlyElement(old.capturedUpdates));
    }

    /**
     * A node that has left the cluster has nothing to declare about itself: it is out of the directory, its node info is
     * removed, and on a decommissioned node the commit cannot even be delivered, as messaging is already shut down - so a
     * declaration must be skipped rather than issued and waited on.
     */
    @Test
    public void declarationSkippedWhenNoLongerAMember()
    {
        ClusterMetadata start = metadataSupportingDeltas();
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(start);
        coordinator.isMember = false;

        coordinator.start();
        coordinator.declareStartedUnreadable();
        coordinator.declareReady();
        coordinator.onShutdown();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // and the same below the min version, where the status-only message is used
        InstrumentedNodeInfoCoordinator old = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(metadataWithMinVersion("6.0-alpha2"));
        old.isMember = false;
        old.start();
        old.onShutdown();
        Assert.assertTrue(old.capturedDeclarations.isEmpty());
        Assert.assertTrue(old.capturedUpdates.isEmpty());
    }

    /**
     * A declaration the metadata already records is not issued at all: committing it would be refused as a no-op, and an
     * epoch costs more than the check.
     */
    @Test
    public void declarationAlreadyRecordedIsSkipped()
    {
        ClusterMetadata start = metadataSupportingDeltas().transformer()
                                                          .withAccordNodeInfo(id(0), AccordNodeInfo.Delta.status(Status.SHUTDOWN), 1)
                                                          .build().metadata;
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(start);

        coordinator.start();
        Assert.assertTrue(coordinator.capturedDeclarations.isEmpty());
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());

        // and the same below the min version, where only the status can be expressed
        ClusterMetadata old = metadataWithMinVersion("6.0-alpha2").transformer()
                                                                 .withAccordDownStatusSince(id(0), Status.SHUTDOWN, 1, 0)
                                                                 .build().metadata;
        InstrumentedNodeInfoCoordinator oldCoordinator = new InstrumentedNodeInfoCoordinator(id(0)).currentMetadata(old);
        oldCoordinator.start();
        Assert.assertTrue(oldCoordinator.capturedDeclarations.isEmpty());
        Assert.assertTrue(oldCoordinator.capturedUpdates.isEmpty());
    }

    /**
     * A successful transformation bumps an epoch whether or not it changed anything, and these are submitted for every
     * failure detector event, so a change that has already taken effect must be refused rather than committed.
     */
    @Test
    public void noOpUpdatesAreRejected()
    {
        ClusterMetadata metadata = metadataSupportingDeltas().transformer()
                                                            .withAccordNodeInfo(id(1), AccordNodeInfo.Delta.status(Status.MAYBE_DOWN), 10)
                                                            .build().metadata;

        // the status is already what this would set
        Assert.assertTrue(new AccordChangeDownStatus(id(1), Status.MAYBE_DOWN, 100, 0).execute(metadata) instanceof Transformation.Rejected);
        Assert.assertTrue(new AccordChangeNodeInfo(id(1), AccordNodeInfo.Delta.status(Status.MAYBE_DOWN), 100).execute(metadata) instanceof Transformation.Rejected);
        // and a change that would take effect is not refused
        Assert.assertTrue(new AccordChangeDownStatus(id(1), Status.NORMAL, 100, 0).execute(metadata) instanceof Transformation.Success);
        Assert.assertTrue(new AccordChangeNodeInfo(id(1), AccordNodeInfo.Delta.unreadable(true), 100).execute(metadata) instanceof Transformation.Success);
    }

    /** metadata whose cluster min version is new enough for the delta transformation: a cluster of nodes running this build */
    private static ClusterMetadata metadataSupportingDeltas()
    {
        ClusterMetadata metadata = metadataWithVersion(NodeVersion.CURRENT);
        Assert.assertTrue("this build must satisfy its own gate, snapshot or not",
                          AccordNodeInfos.supportsExtendedNodeInfo(metadata));
        return metadata;
    }

    private static ClusterMetadata metadataWithMinVersion(String version)
    {
        return metadataWithVersion(new NodeVersion(new CassandraVersion(version), NodeVersion.CURRENT_METADATA_VERSION));
    }

    private static ClusterMetadata metadataWithVersion(NodeVersion nodeVersion)
    {
        Directory directory = Directory.EMPTY.with(new NodeAddresses(FBUtilities.getBroadcastAddressAndPort()),
                                                   new Location("dc1", "rack1"),
                                                   nodeVersion);
        return EMPTY.transformer().with(directory).build().metadata;
    }

    /**
     * if a peer is marked as shutdown, other nodes should ignore FD signals until it marks itself alive again
     */
    @Test
    public void peerShutdownTest()
    {
        Topology topology = new Topology(1,
                Shard.create(AccordTopology.minRange(TABLE_1, token(0)), idList(0, 1, 2), idSet(0, 1, 2)),
                Shard.create(AccordTopology.maxRange(TABLE_1, token(0)), idList(3, 4, 5), idSet(3, 4, 5)));
        InstrumentedNodeInfoCoordinator coordinator = new InstrumentedNodeInfoCoordinator(id(0));
        ClusterMetadata metadata = metadataSupportingDeltas().transformer().withAccordNodeInfo(id(1), AccordNodeInfo.Delta.status(Status.SHUTDOWN), 1).build().metadata;
        coordinator.currentMetadata(metadata);
        coordinator.updatePeers(topology);
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.start();
        // our own startup declaration; the peer's status is what this test is about
        Assert.assertEquals(declaration(id(0), startupDeclaration(metadata)), Iterables.getOnlyElement(coordinator.capturedDeclarations));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.capturedDeclarations.clear();

        Assert.assertTrue(coordinator.isPeer(id(1)));
        coordinator.onAlive(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
        coordinator.onDead(id(1));
        Assert.assertTrue(coordinator.capturedUpdates.isEmpty());
    }
}
