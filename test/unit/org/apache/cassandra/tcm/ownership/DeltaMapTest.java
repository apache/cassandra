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

package org.apache.cassandra.tcm.ownership;

import java.util.Collection;

import com.google.common.collect.ImmutableMultimap;

import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeltaMapTest
{
    private static final ReplicationParams key = ReplicationParams.simple(1);
    private static final NodeId N1 = new NodeId(1);
    private static final NodeId N2 = new NodeId(2);
    private static final NodeId N3 = new NodeId(3);
    private static final NodeId N4 = new NodeId(4);
    private static final Range<Token> R1 = new Range<>(token(0), token(100));
    private static final Range<Token> R2 = new Range<>(token(100), token(200));
    private static final Range<Token> R_INT = new Range<>(token(50), token(150));

    @Test
    public void mergeDisjointDeltas()
    {
        // Combine 2 Deltas with disjoint removals (and no additions), for the same ReplicationParams.
        // Verify that the resulting merged Delta contains the removals/additions from both.
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N2, R2);

        Delta d1 = new NodeIdDelta(group1, emptyNodes());
        Delta d2 = new NodeIdDelta(group2, emptyNodes());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);
        for (NodeIdDelta delta : new NodeIdDelta[]{ (NodeIdDelta) merged.reads, (NodeIdDelta) merged.writes })
        {
            assertTrue(delta.additions.isEmpty());
            assertEquals(group1.get(N1), delta.removals.get(N1));
            assertEquals(group2.get(N2), delta.removals.get(N2));
        }
    }

    private static ImmutableMultimap<NodeId, ReplicaNode> fullNodes(NodeId nodeId, Range<Token> range)
    {
        return ImmutableMultimap.of(nodeId, new ReplicaNode(nodeId, range, true));
    }

    private static ImmutableMultimap<NodeId, ReplicaNode> transientNodes(NodeId nodeId, Range<Token> range)
    {
        return ImmutableMultimap.of(nodeId, new ReplicaNode(nodeId, range, false));
    }

    private static ImmutableMultimap<NodeId, ReplicaNode> emptyNodes()
    {
        return ImmutableMultimap.of();
    }

    @Test
    public void mergeDisjointReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain removals for the same endpoint, but for disjoint ranges.
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N1, R2);

        NodeIdDelta d1 = new NodeIdDelta(group1, emptyNodes());
        NodeIdDelta d2 = new NodeIdDelta(group2, emptyNodes());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);

        for (NodeIdDelta delta : new NodeIdDelta[]{ (NodeIdDelta) merged.reads, (NodeIdDelta) merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            Collection<ReplicaNode> mergedGroup = delta.removals.get(N1);

            assertEquals(2, mergedGroup.size());
            group1.values().forEach(r -> assertTrue(mergedGroup.contains(r)));
            group2.values().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void mergeIdenticalReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain identical removals for the same endpoint.
        // Effectively a noop.
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);

        Delta d1 = new NodeIdDelta(group1, emptyNodes());
        Delta d2 = new NodeIdDelta(group1, emptyNodes());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);

        for (NodeIdDelta delta : new NodeIdDelta[]{ (NodeIdDelta)merged.reads, (NodeIdDelta)merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            Collection<ReplicaNode> mergedGroup = delta.removals.get(N1);
            assertEquals(1, mergedGroup.size());
            group1.values().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void mergeIntersectingReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain replicas for a common endpoint, but with intersecting ranges.
        // TODO there isn't an obvious reason to support this, so perhaps we should be conservative and
        //      explicitly reject it
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N1, R_INT);

        Delta d1 = new NodeIdDelta(group1, emptyNodes());
        Delta d2 = new NodeIdDelta(group2, emptyNodes());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build().get(key);

        for (NodeIdDelta delta : new NodeIdDelta[]{ (NodeIdDelta)merged.reads, (NodeIdDelta)merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            Collection<ReplicaNode> mergedGroup = delta.removals.get(N1);
            assertEquals(2, mergedGroup.size());
            group1.values().forEach(r -> assertTrue(mergedGroup.contains(r)));
            group2.values().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void invertSingleDelta()
    {
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N2, R2);

        Delta d1 = new NodeIdDelta(group1, group2);
        Delta d2 = new NodeIdDelta(group2, group1);

        assertEquals(d1, d2.invert());
        assertEquals(d2, d2.invert().invert());
    }

    @Test
    public void invertEmptyDelta()
    {
        Delta d = NodeIdDelta.empty();
        assertEquals(d, d.invert());
    }

    @Test
    public void invertPartiallyEmptyDelta()
    {
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N2, R2);

        NodeIdDelta additions = new NodeIdDelta(emptyNodes(), group1);
        NodeIdDelta inverted = (NodeIdDelta)additions.invert();
        assertEquals(ImmutableMultimap.of(), inverted.additions);
        assertEquals(additions.additions, inverted.removals);

        NodeIdDelta removals = new NodeIdDelta(group2, emptyNodes());
        inverted = (NodeIdDelta)removals.invert();
        assertEquals(ImmutableMultimap.of(), inverted.removals);
        assertEquals(removals.removals, inverted.additions);
    }

    @Test
    public void invertPlacementDelta()
    {
        ImmutableMultimap<NodeId, ReplicaNode> group1 = fullNodes(N1, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group2 = fullNodes(N2, R1);
        Delta d1 = new NodeIdDelta(group1, group2);

        ImmutableMultimap<NodeId, ReplicaNode> group3 = fullNodes(N3, R1);
        ImmutableMultimap<NodeId, ReplicaNode> group4 = fullNodes(N4, R2);
        Delta d2 = new NodeIdDelta(group3, group4);

        PlacementDeltas.PlacementDelta pd1 = new PlacementDeltas.PlacementDelta(d1,d2);
        PlacementDeltas.PlacementDelta pd2 = new PlacementDeltas.PlacementDelta(d1.invert(), d2.invert());
        assertEquals(pd2, pd1.invert());
    }

    @Test
    public void testMerge()
    {
        // delta to remove transient replica and add trivial replica
        NodeIdDelta toFinal = new NodeIdDelta(transientNodes(N1, R1), fullNodes(N1, R1));
        // delta to remove trivial replica
        NodeIdDelta toMerge = new NodeIdDelta(fullNodes(N1, R1), emptyNodes());
        // merged should contain only the transient replica removal
        NodeIdDelta merged = (NodeIdDelta) toMerge.merge(toFinal);
        assertEquals(0, merged.additions.get(N1).size());
        assertEquals(1, merged.removals.get(N1).size());
        assertTrue(merged.removals.get(N1).contains(new ReplicaNode(N1, R1, false)));
    }
}
