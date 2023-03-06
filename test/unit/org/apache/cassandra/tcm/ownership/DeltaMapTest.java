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

import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.schema.ReplicationParams;

import static org.apache.cassandra.tcm.membership.MembershipUtils.endpoint;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.emptyReplicas;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.trivialReplicas;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeltaMapTest
{
    private static final ReplicationParams key = ReplicationParams.simple(1);
    private static final InetAddressAndPort P1 = endpoint(1);
    private static final InetAddressAndPort P2 = endpoint(2);
    private static final InetAddressAndPort P3 = endpoint(3);
    private static final InetAddressAndPort P4 = endpoint(4);
    private static final Range<Token> R1 = new Range<>(token(0), token(100));
    private static final Range<Token> R2 = new Range<>(token(100), token(200));
    private static final Range<Token> R_INT = new Range<>(token(50), token(150));


    @Test
    public void mergeDisjointDeltas()
    {
        // Combine 2 Deltas with disjoint removals (and no additions), for the same ReplicationParams.
        // Verify that the resulting merged Delta contains the removals/additions from both.
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P2, R2);

        Delta d1 = new Delta(group1, emptyReplicas());
        Delta d2 = new Delta(group2, emptyReplicas());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);

        for (Delta delta : new Delta[]{ merged.reads, merged.writes })
        {
            assertTrue(delta.additions.isEmpty());
            assertEquals(group1.get(P1), delta.removals.get(P1));
            assertEquals(group2.get(P2), delta.removals.get(P2));
        }
    }

    @Test
    public void mergeDisjointReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain removals for the same endpoint, but for disjoint ranges.
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P1, R2);

        Delta d1 = new Delta(group1, emptyReplicas());
        Delta d2 = new Delta(group2, emptyReplicas());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);

        for (Delta delta : new Delta[]{ merged.reads, merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            RangesAtEndpoint mergedGroup = delta.removals.get(P1);

            assertEquals(2, mergedGroup.size());
            group1.flattenValues().forEach(r -> assertTrue(mergedGroup.contains(r)));
            group2.flattenValues().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void mergeIdenticalReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain identical removals for the same endpoint.
        // Effectively a noop.
        RangesByEndpoint group1 = trivialReplicas(P1, R1);

        Delta d1 = new Delta(group1, emptyReplicas());
        Delta d2 = new Delta(group1, emptyReplicas());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build()
                                                               .get(key);

        for (Delta delta : new Delta[]{ merged.reads, merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            RangesAtEndpoint mergedGroup = delta.removals.get(P1);
            assertEquals(1, mergedGroup.size());
            group1.flattenValues().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void mergeIntersectingReplicasForSameEndpoint()
    {
        // Combine 2 Deltas which both contain replicas for a common endpoint, but with intersecting ranges.
        // TODO there isn't an obvious reason to support this, so perhaps we should be conservative and
        //      explicitly reject it
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P1, R_INT);

        Delta d1 = new Delta(group1, emptyReplicas());
        Delta d2 = new Delta(group2, emptyReplicas());
        PlacementDeltas.PlacementDelta merged = PlacementDeltas.builder(1)
                                                               .put(key, new PlacementDeltas.PlacementDelta(d1, d1))
                                                               .put(key, new PlacementDeltas.PlacementDelta(d2, d2))
                                                               .build().get(key);

        for (Delta delta : new Delta[]{ merged.reads, merged.writes })
        {
            assertEquals(1, delta.removals.keySet().size());
            RangesAtEndpoint mergedGroup = delta.removals.get(P1);
            assertEquals(2, mergedGroup.size());
            group1.flattenValues().forEach(r -> assertTrue(mergedGroup.contains(r)));
            group2.flattenValues().forEach(r -> assertTrue(mergedGroup.contains(r)));
        }
    }

    @Test
    public void invertSingleDelta()
    {
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P2, R2);

        Delta d1 = new Delta(group1, group2);
        Delta d2 = new Delta(group2, group1);

        assertEquals(d1, d2.invert());
        assertEquals(d2, d2.invert().invert());
    }

    @Test
    public void invertEmptyDelta()
    {
        Delta d = Delta.empty();
        assertEquals(d, d.invert());
    }

    @Test
    public void invertPartiallyEmptyDelta()
    {
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P2, R1);

        Delta additions = new Delta(emptyReplicas(), group1);
        Delta inverted = additions.invert();
        assertEquals(RangesByEndpoint.EMPTY, inverted.additions);
        assertEquals(additions.additions, inverted.removals);

        Delta removals = new Delta(group2, emptyReplicas());
        inverted = removals.invert();
        assertEquals(RangesByEndpoint.EMPTY, inverted.removals);
        assertEquals(removals.removals, inverted.additions);
    }

    @Test
    public void invertPlacementDelta()
    {
        RangesByEndpoint group1 = trivialReplicas(P1, R1);
        RangesByEndpoint group2 = trivialReplicas(P2, R1);
        Delta d1 = new Delta(group1, group2);

        RangesByEndpoint group3 = trivialReplicas(P3, R1);
        RangesByEndpoint group4 = trivialReplicas(P4, R2);
        Delta d2 = new Delta(group3, group4);

        PlacementDeltas.PlacementDelta pd1 = new PlacementDeltas.PlacementDelta(d1,d2);
        PlacementDeltas.PlacementDelta pd2 = new PlacementDeltas.PlacementDelta(d1.invert(), d2.invert());
        assertEquals(pd2, pd1.invert());
    }
}
