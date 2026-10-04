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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import accord.local.Node;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo.Delta;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.StampedNodeInfo;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

// this class was written by Claude
public class AccordNodeInfosTest
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void serde()
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            Gen<Set<Node.Id>> nodesGen = Gens.lists(AccordGens.nodes()).unique().ofSizeBetween(0, 9).map(l -> new HashSet<>(l));
            Gen<Epoch> epochGen = AccordGens.epochs().map(Epoch::create);
            Gen<Status> statusGen = Gens.pick(Status.NORMAL, Status.SHUTDOWN, Status.MAYBE_DOWN, Status.REMOVED, Status.HARD_REMOVED);

            qt().check(rs -> {
                Epoch epoch = epochGen.next(rs);
                AccordNodeInfos infos = AccordNodeInfos.EMPTY;
                for (Node.Id node : nodesGen.next(rs))
                {
                    Delta delta = Delta.status(statusGen.next(rs))
                                       .combine(Delta.unreadable(rs.nextBoolean()))
                                       .combine(Delta.stale(rs.nextBoolean()));
                    infos = infos.withNodeInfo(node, delta, 1 + rs.nextLong(0, 1000));
                }
                AsymmetricMetadataSerializers.testSerde(buffer, AccordNodeInfos.serializer, infos.withLastModified(epoch), Version.MIN_ACCORD_VERSION);
            });
        }
    }

    @Test
    public void deltaSerde()
    {
        try (DataOutputBuffer buffer = new DataOutputBuffer())
        {
            Gen<Status> statusGen = Gens.pick(Status.values());
            qt().check(rs -> {
                Delta delta = Delta.status(statusGen.next(rs))
                                   .combine(Delta.unreadable(rs.nextBoolean()))
                                   .combine(Delta.stale(rs.nextBoolean()));
                AsymmetricMetadataSerializers.testSerde(buffer, Delta.serializer, delta, Version.MIN_ACCORD_VERSION);
            });
        }
    }

    /**
     * Each field of a NodeInfo has its own writer - peers infer the status, the node itself declares the unreadable bit,
     * an operator marks it stale - so a writer must change only what it names, or it would silently revert the others.
     */
    @Test
    public void deltaLeavesOtherFieldsAlone()
    {
        Node.Id node = new Node.Id(1);
        AccordNodeInfos infos = AccordNodeInfos.EMPTY
                                .withNodeInfo(node, Delta.status(Status.NORMAL).combine(Delta.unreadable(true)), 10)
                                .withStale(SortedArrayList.ofSorted(node));
        assertEquals(Status.NORMAL, infos.status(node));
        assertTrue(infos.get(node).isUnreadable());
        assertTrue(infos.get(node).isStale());

        // a node declaring itself readable must not clear the stale mark an operator made
        AccordNodeInfos ready = infos.withNodeInfo(node, Delta.status(Status.NORMAL).combine(Delta.unreadable(false)), 20);
        assertFalse(ready.get(node).isUnreadable());
        assertTrue("declaring must not clear a stale mark", ready.get(node).isStale());
        assertEquals(SortedArrayList.ofSorted(node), ready.stale());

        // nor must a peer's inferred status change
        AccordNodeInfos down = ready.maybeChangeDownStatus(node, Status.MAYBE_DOWN, 30, 0);
        assertEquals(Status.MAYBE_DOWN, down.status(node));
        assertTrue("an inferred status must not clear a stale mark", down.get(node).isStale());
        assertFalse(down.get(node).isUnreadable());

        // and unmarking stale must not disturb the status
        AccordNodeInfos rejoined = down.withoutStale(SortedArrayList.ofSorted(node));
        assertFalse(rejoined.get(node).isStale());
        assertEquals(Status.MAYBE_DOWN, rejoined.status(node));
    }

    /**
     * The derived views are what the topology is built from, so they must agree with the per-node fields they are
     * computed from.
     */
    @Test
    public void derivedViewsMatchStatuses()
    {
        Node.Id normal = new Node.Id(1), maybeDown = new Node.Id(2), unreadable = new Node.Id(3), hardRemoved = new Node.Id(4);
        AccordNodeInfos infos = AccordNodeInfos.EMPTY
                                .withNodeInfo(normal, Delta.status(Status.NORMAL), 1)
                                .withNodeInfo(maybeDown, Delta.status(Status.MAYBE_DOWN), 1)
                                .withNodeInfo(unreadable, Delta.status(Status.NORMAL).combine(Delta.unreadable(true)), 1)
                                .withNodeInfo(hardRemoved, Delta.status(Status.HARD_REMOVED), 1)
                                .withStale(SortedArrayList.ofSorted(unreadable));

        // only a NORMAL node counts for a fast quorum; being unreadable or stale does not exclude it
        assertEquals(SortedArrayList.ofSorted(maybeDown, hardRemoved), infos.excludedFromFastQuorum());
        assertEquals(SortedArrayList.ofSorted(hardRemoved), infos.hardRemoved());
        assertEquals(SortedArrayList.ofSorted(unreadable), infos.stale());

        assertTrue(infos.shouldUpdateDownStatus(maybeDown, Status.NORMAL, 1000, 0));
        assertTrue(infos.shouldUpdateDownStatus(normal, Status.MAYBE_DOWN, 1000, 0));
        // a node with nothing recorded simply gets an entry (members are given one as they join, so this is only the
        // pre-sweep case; the coordinator decides separately whether it is a peer we report on at all)
        assertTrue(infos.shouldUpdateDownStatus(new Node.Id(5), Status.MAYBE_DOWN, 1000, 0));
        assertTrue(infos.shouldUpdateDownStatus(new Node.Id(5), Status.NORMAL, 1000, 0));
        // and a removed node is beyond our inferences
        assertFalse(infos.shouldUpdateDownStatus(hardRemoved, Status.MAYBE_DOWN, 1000, 0));
    }

    /**
     * The update delay damps a status a peer <i>infers</i>, so that two failure detectors cannot flip one between them.
     * A node announcing its own shutdown is not an inference and nobody else can tell us, so it is not damped; leaving
     * SHUTDOWN is damped like any other transition, and not contesting a peer's SHUTDOWN at all is the coordinator's
     * rule rather than this one (see AccordNodeStatusCoordinatorTest.peerShutdownTest).
     */
    @Test
    public void shutdownIsUndamped()
    {
        Node.Id node = new Node.Id(1);
        AccordNodeInfos normal = AccordNodeInfos.EMPTY.withNodeInfo(node, Delta.status(Status.NORMAL), 1000);

        // a node announces its own shutdown, well inside the delay that damps an inferred status
        assertTrue(normal.shouldUpdateDownStatus(node, Status.SHUTDOWN, 1001, 5000));
        assertEquals(Status.SHUTDOWN, normal.maybeChangeDownStatus(node, Status.SHUTDOWN, 1001, 5000).status(node));
        // whereas an inferred MAYBE_DOWN waits out the delay
        assertFalse(normal.shouldUpdateDownStatus(node, Status.MAYBE_DOWN, 1001, 5000));
        assertTrue(normal.shouldUpdateDownStatus(node, Status.MAYBE_DOWN, 6000, 5000));

        AccordNodeInfos shutdown = AccordNodeInfos.EMPTY.withNodeInfo(node, Delta.status(Status.SHUTDOWN), 10);

        // the node itself declares NORMAL as it comes back up, undamped
        assertTrue(shutdown.shouldUpdateDownStatus(node, Status.NORMAL, 20, 0));
        assertEquals(Status.NORMAL, shutdown.maybeChangeDownStatus(node, Status.NORMAL, 20, 0).status(node));
        // and on a cluster new enough for deltas it does so through the delta, which needs no such convention
        assertEquals(Status.NORMAL, shutdown.withNodeInfo(node, Delta.status(Status.NORMAL), 20).status(node));

        // a removed node is beyond this path entirely, however it is addressed
        AccordNodeInfos removed = AccordNodeInfos.EMPTY.withNodeInfo(node, Delta.status(Status.REMOVED), 10);
        assertFalse(removed.shouldUpdateDownStatus(node, Status.SHUTDOWN, 100000, 0));
        assertFalse(removed.shouldUpdateDownStatus(node, Status.NORMAL, 100000, 0));
        assertSame(removed, removed.maybeChangeDownStatus(node, Status.NORMAL, 100000, 0));
    }

    /**
     * A member should always have an entry: they are inserted as the directory grows, marked removed as it shrinks, and
     * swept for on startup, so that no reader has to interpret an absent one. Until a cluster has been swept, an absent
     * entry reads as the default.
     */
    @Test
    public void membersAreGivenEntries()
    {
        NodeId one = new NodeId(1), two = new NodeId(2);
        AccordNodeInfos infos = AccordNodeInfos.EMPTY.withNodes(Arrays.asList(one, two));
        assertEquals(StampedNodeInfo.DEFAULT, infos.get(one));
        assertEquals(Status.NORMAL, infos.status(new Node.Id(one.id())));
        assertEquals(SortedArrayList.ofSorted(), infos.excludedFromFastQuorum());

        // a sweep that finds nothing missing must not touch the value, or every startup would mark it modified
        assertSame(infos, infos.withNodes(Arrays.asList(one, two)));
        // and it must not overwrite what is already recorded
        AccordNodeInfos down = infos.maybeChangeDownStatus(new Node.Id(one.id()), Status.MAYBE_DOWN, 100, 0);
        assertSame(down, down.withNodes(Arrays.asList(one, two)));
        assertEquals(Status.MAYBE_DOWN, down.status(new Node.Id(one.id())));

        // a departed member keeps its entry, marked removed on departure (and back-filled by the startup sweep for a
        // cluster that removed it before MIN_VERSION), so that no reader has to consult the directory
        AccordNodeInfos removed = down.withRemoved(Arrays.asList(one));
        assertEquals(Status.REMOVED, removed.status(new Node.Id(one.id())));
        assertEquals(Status.NORMAL, removed.status(new Node.Id(two.id())));
        assertSame(removed, removed.withRemoved(Arrays.asList(one)));
        // and a hard removed mark is not weakened by a later departure
        AccordNodeInfos hardRemoved = down.withHardRemoved(SortedArrayList.ofSorted(new Node.Id(two.id())));
        assertSame(hardRemoved, hardRemoved.withRemoved(Arrays.asList(two)));
        assertEquals(SortedArrayList.ofSorted(new Node.Id(two.id())), hardRemoved.hardRemoved());

        // an absent entry reads as the default
        assertEquals(StampedNodeInfo.DEFAULT, AccordNodeInfos.EMPTY.getOrDefault(new Node.Id(9)));
    }

    @Test
    public void removedNodesRejectUpdates()
    {
        Node.Id node = new Node.Id(1);
        AccordNodeInfos removed = AccordNodeInfos.EMPTY.withNodeInfo(node, Delta.status(Status.REMOVED), 1);
        assertThatThrownBy(() -> removed.withNodeInfo(node, Delta.status(Status.NORMAL), 2))
            .isInstanceOf(InvalidRequestException.class);
        // except to hard remove it
        assertEquals(Status.HARD_REMOVED, removed.withNodeInfo(node, Delta.status(Status.HARD_REMOVED), 2).status(node));

        AccordNodeInfos hardRemoved = removed.withNodeInfo(node, Delta.status(Status.HARD_REMOVED), 2);
        assertThatThrownBy(() -> hardRemoved.withNodeInfo(node, Delta.status(Status.NORMAL), 3))
            .isInstanceOf(InvalidRequestException.class);
    }
}
