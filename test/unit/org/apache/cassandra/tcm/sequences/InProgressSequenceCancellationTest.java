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

import java.util.Collections;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementForRange;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;

import static org.apache.cassandra.tcm.membership.MembershipUtils.nodeAddresses;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.deltas;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.placements;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.ranges;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.affectedRanges;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.epoch;
import static org.apache.cassandra.tcm.sequences.SequencesUtils.lockedRanges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InProgressSequenceCancellationTest
{

    private static final Logger logger = LoggerFactory.getLogger(InProgressSequenceCancellationTest.class);
    private static final int ITERATIONS = 100;

    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        // disable the sorting of replica lists as it assumes all endpoints are present in
        // the token map and this test uses randomly generated placements, so this is not true
        CassandraRelevantProperties.TCM_SORT_REPLICA_GROUPS.setBoolean(false);
    }

    @Test
    public void revertBootstrapAndJoinEffects()
    {
        for (int i = 0; i < ITERATIONS; i++)
            testRevertingBootstrap(System.nanoTime());
    }

    private void testRevertingBootstrap(long seed)
    {
        Random random = new Random(seed);
        logger.info("SEED: {}", seed);
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication,
                                                             KeyspaceParams.simple(5).replication,
                                                             KeyspaceParams.simple(10).replication);
        NodeAddresses addresses = nodeAddresses(random);
        Directory directory = new Directory().with(addresses, new Location("dc", "rack"));
        NodeId nodeId = directory.peerId(addresses.broadcastAddress);
        LockedRanges.Key key = LockedRanges.keyFor(epoch(random));
        // Initial placements, i.e. state before the sequence was initiated - what we want to return to
        DataPlacements placements = placements(ranges(random), replication, random);
        // Ranges locked by other operations
        LockedRanges locked = lockedRanges(placements, random);

        // state of metadata before starting the sequence
        ClusterMetadata before = metadata(directory).transformer()
                                                    .with(placements)
                                                    .withNodeState(nodeId, NodeState.REGISTERED)
                                                    .with(locked)
                                                    .build().metadata;

        // Placements after PREPARE_JOIN
        DataPlacements afterPrepare = placements(ranges(random), replication, random);
        PlacementDeltas prepareDeltas = deltas(placements, afterPrepare);
        // Placements after START_JOIN
        DataPlacements afterStart = placements(ranges(random), replication, random);
        PlacementDeltas startDeltas = deltas(afterPrepare, afterStart);
        // Placements after MID_JOIN
        DataPlacements afterMid = placements(ranges(random), replication, random);
        PlacementDeltas midDeltas = deltas(afterStart, afterMid);
        // No need to create a deltas or placements for after FINISH_JOIN because it's too late to cancel by then
        PlacementDeltas finishDeltas = PlacementDeltas.empty();

        Set<Token> tokens = Collections.singleton(token(random.nextLong()));

        BootstrapAndJoin plan = new BootstrapAndJoin(ProgressBarrier.NONE,
                                                     key,
                                                     Transformation.Kind.FINISH_JOIN,
                                                     prepareDeltas,
                                                     new PrepareJoin.StartJoin(nodeId, startDeltas, key),
                                                     new PrepareJoin.MidJoin(nodeId, midDeltas, key),
                                                     new PrepareJoin.FinishJoin(nodeId, tokens, finishDeltas, key),
                                                     false,
                                                     false);

        // Ranges locked by this sequence
        locked = locked.lock(key, affectedRanges(placements, random));
        // State of metadata after executing up to the FINISH step
        ClusterMetadata during = before.transformer()
                                       .with(afterMid)
                                       .with(locked)
                                       .withNodeState(nodeId, NodeState.BOOTSTRAPPING)
                                       .with(before.inProgressSequences.with(nodeId, plan))
                                       .build().metadata;

        ClusterMetadata after = plan.cancel(during).build().metadata;

        assertRelevantMetadata(before, after);
        // cancelling the sequence doesn't remove it from metadata, that's the job of the CancelInProgressSequence event
        assertNull(before.inProgressSequences.get(nodeId));
        assertEquals(plan, after.inProgressSequences.get(nodeId));
    }

    @Test
    public void revertUnbootstrapAndLeaveEffects()
    {
        for (int i = 0; i < ITERATIONS; i++)
            testRevertingLeave(System.nanoTime());
    }

    private void testRevertingLeave(long seed)
    {
        Random random = new Random(seed);
        logger.info("SEED: {}", seed);
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication,
                                                             KeyspaceParams.simple(5).replication,
                                                             KeyspaceParams.simple(10).replication);

        NodeAddresses addresses = nodeAddresses(random);
        Directory directory = new Directory().with(addresses, new Location("dc", "rack"));
        NodeId nodeId = directory.peerId(addresses.broadcastAddress);
        LockedRanges.Key key = LockedRanges.keyFor(epoch(random));
        // Initial placements, i.e. state before the sequence was initiated - what we want to return to
        DataPlacements placements = placements(ranges(random), replication, random);
        // Ranges locked by other operations
        LockedRanges locked = lockedRanges(placements, random);
        // state of metadata before starting the sequence
        ClusterMetadata before = metadata(directory).transformer()
                                                    .with(placements)
                                                    .withNodeState(nodeId, NodeState.JOINED)
                                                    .with(locked)
                                                    .build().metadata;


        // PREPARE_LEAVE does not modify placements, so first transformation is START_LEAVE
        DataPlacements afterStart = placements(ranges(random), replication, random);
        PlacementDeltas startDeltas = deltas(placements, afterStart);
        // Placements after MID_LEAVE
        DataPlacements afterMid = placements(ranges(random), replication, random);
        PlacementDeltas midDeltas = deltas(afterStart, afterMid);

        UnbootstrapAndLeave plan = new UnbootstrapAndLeave(ProgressBarrier.NONE,
                                                           key,
                                                           Transformation.Kind.FINISH_LEAVE,
                                                           new PrepareLeave.StartLeave(nodeId, startDeltas, key),
                                                           new PrepareLeave.MidLeave(nodeId, midDeltas, key),
                                                           new PrepareLeave.FinishLeave(nodeId, PlacementDeltas.empty(), key));

        // Ranges locked by this sequence (just random, not accurate, as we're only asserting that they get unlocked)
        locked = locked.lock(key, affectedRanges(placements, random));
        // State of metadata after executing up to the FINISH step
        ClusterMetadata during = before.transformer()
                                       .with(afterMid)
                                       .with(locked)
                                       .withNodeState(nodeId, NodeState.LEAVING)
                                       .with(before.inProgressSequences.with(nodeId, plan))
                                       .build().metadata;

        ClusterMetadata after = plan.cancel(during).build().metadata;

        assertRelevantMetadata(before, after);
        // cancelling the sequence doesn't remove it from metadata, that's the job of the CancelInProgressSequence event
        assertNull(before.inProgressSequences.get(nodeId));
        assertEquals(plan, after.inProgressSequences.get(nodeId));
    }

    @Test
    public void revertBootstrapAndReplaceEffects()
    {
        for (int i = 0; i < ITERATIONS; i++)
            testRevertingReplace(System.nanoTime());
    }

    private void testRevertingReplace(long seed)
    {
        Random random = new Random(seed);
        logger.info("SEED: {}", seed);
        Set<ReplicationParams> replication = ImmutableSet.of(KeyspaceParams.simple(1).replication,
                                                             KeyspaceParams.simple(2).replication,
                                                             KeyspaceParams.simple(3).replication,
                                                             KeyspaceParams.simple(5).replication,
                                                             KeyspaceParams.simple(10).replication);

        NodeAddresses addresses = nodeAddresses(random);
        Directory directory = new Directory().with(addresses, new Location("dc", "rack"));
        NodeId nodeId = directory.peerId(addresses.broadcastAddress);
        LockedRanges.Key key = LockedRanges.keyFor(epoch(random));
        // Initial placements, i.e. state before the sequence was initiated - what we want to return to
        DataPlacements placements = placements(ranges(random), replication, random);
        // Ranges locked by other operations
        LockedRanges locked = lockedRanges(placements, random);
        // State of metadata before starting the sequence
        ClusterMetadata before = metadata(directory).transformer()
                                                    .with(placements)
                                                    .withNodeState(nodeId, NodeState.REGISTERED)
                                                    .with(locked)
                                                    .build().metadata;

        // nodeId is the id of the replacement node. Add the node being replaced to metadata
        NodeAddresses replacedAddresses = nodeAddresses(random);
        // Make sure we don't try to replace with the same address
        while (replacedAddresses.broadcastAddress.equals(addresses.broadcastAddress))
            replacedAddresses = nodeAddresses(random);
        before = before.transformer()
                       .register(replacedAddresses, new Location("dc", "rack"), NodeVersion.CURRENT)
                       .build().metadata;
        NodeId replacedId = before.directory.peerId(replacedAddresses.broadcastAddress);
        Set<Token> tokens = Collections.singleton(token(random.nextLong()));
        // bit of a hack to jump the old node directly to joined
        before = before.transformer().proposeToken(replacedId, tokens).build().metadata;
        before = before.transformer().join(replacedId).build().metadata;

        // PREPARE_REPLACE does not modify placements, so first transformation is START_REPLACE
        DataPlacements afterStart = placements(ranges(random), replication, random);
        PlacementDeltas startDeltas = deltas(placements, afterStart);
        // Placements after MID_REPLACE
        DataPlacements afterMid = placements(ranges(random), replication, random);
        PlacementDeltas midDeltas = deltas(afterStart, afterMid);

        BootstrapAndReplace plan = new BootstrapAndReplace(ProgressBarrier.NONE,
                                                           key,
                                                           Transformation.Kind.FINISH_REPLACE,
                                                           tokens,
                                                           new PrepareReplace.StartReplace(replacedId, nodeId, startDeltas, key),
                                                           new PrepareReplace.MidReplace(replacedId, nodeId, midDeltas, key),
                                                           new PrepareReplace.FinishReplace(replacedId, nodeId, PlacementDeltas.empty(), key),
                                                           false,
                                                           false);

        // Ranges locked by this sequence (just random, not accurate, as we're only asserting that they get unlocked)
        locked = locked.lock(key, affectedRanges(placements, random));
        // State of metadata after executing up to the FINISH step
        ClusterMetadata during = before.transformer()
                                       .with(afterMid)
                                       .with(locked)
                                       .with(before.inProgressSequences.with(nodeId, plan))
                                       .build().metadata;

        ClusterMetadata after = plan.cancel(during).build().metadata;

        assertRelevantMetadata(before, after);
        // cancelling the sequence doesn't remove it from metadata, that's the job of the CancelInProgressSequence event
        assertNull(before.inProgressSequences.get(nodeId));
        assertEquals(plan, after.inProgressSequences.get(nodeId));
    }

    private void assertRelevantMetadata(ClusterMetadata first, ClusterMetadata second)
    {
        assertPlacementsEquivalent(first.placements, second.placements);
        assertTrue(first.directory.isEquivalent(second.directory));
        assertTrue(first.tokenMap.isEquivalent(second.tokenMap));
        assertEquals(first.lockedRanges.locked.keySet(), second.lockedRanges.locked.keySet());
    }

    private static ClusterMetadata metadata(Directory directory)
    {
        return new ClusterMetadata(Murmur3Partitioner.instance, directory);
    }

    private void assertPlacementsEquivalent(DataPlacements first, DataPlacements second)
    {
        assertEquals(first.keys(), second.keys());

        first.asMap().forEach((params, placement) -> {
            DataPlacement otherPlacement = second.get(params);
            PlacementForRange r1 = placement.reads;
            PlacementForRange r2 = otherPlacement.reads;
            assertEquals(r1.replicaGroups().keySet(), r2.replicaGroups().keySet());
            r1.replicaGroups().forEach((range, e1) -> {
                EndpointsForRange e2 = r2.forRange(range);
                assertEquals(e1.size(),e2.size());
                assertTrue(e1.stream().allMatch(e2::contains));
            });

            PlacementForRange w1 = placement.reads;
            PlacementForRange w2 = otherPlacement.reads;
            assertEquals(w1.replicaGroups().keySet(), w2.replicaGroups().keySet());
            w1.replicaGroups().forEach((range, e1) -> {
                EndpointsForRange e2 = w2.forRange(range);
                assertEquals(e1.size(),e2.size());
                assertTrue(e1.stream().allMatch(e2::contains));
            });

        });
    }
}
