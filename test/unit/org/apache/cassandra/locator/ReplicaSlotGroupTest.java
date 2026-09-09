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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ReplicaSlotGroup and SlotGroupMaps classes.
 */
public class ReplicaSlotGroupTest
{
    private static InetAddressAndPort ep(String addr) throws UnknownHostException
    {
        return InetAddressAndPort.getByName(addr);
    }

    private static Token token(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    // ==================== ReplicaSlotGroup Tests ====================

    @Test
    public void testStableSlot() throws UnknownHostException
    {
        InetAddressAndPort natural = ep("127.0.0.1");
        ReplicaSlotGroup slot = ReplicaSlotGroup.stableSlot(natural);

        assertFalse("Stable slot should not be transitioning", slot.isTransitioning());
        assertEquals(natural, slot.naturalEndpoint());
        assertNull("Stable slot should have null pending endpoint", slot.pendingEndpoint());
        assertEquals(1, slot.requiredAcks());
        assertEquals(1, slot.members().size());
        assertTrue(slot.members().contains(natural));
    }

    @Test
    public void testTransitioningSlot() throws UnknownHostException
    {
        InetAddressAndPort natural = ep("127.0.0.1");
        InetAddressAndPort pending = ep("127.0.0.2");
        ReplicaSlotGroup slot = ReplicaSlotGroup.transitioningSlot(natural, pending);

        assertTrue("Transitioning slot should be transitioning", slot.isTransitioning());
        assertEquals(natural, slot.naturalEndpoint());
        assertEquals(pending, slot.pendingEndpoint());
        assertEquals(2, slot.requiredAcks());
        assertEquals(2, slot.members().size());
        assertTrue(slot.members().contains(natural));
        assertTrue(slot.members().contains(pending));
    }

    @Test
    public void testStableSlotSatisfaction() throws UnknownHostException
    {
        InetAddressAndPort natural = ep("127.0.0.1");
        InetAddressAndPort other = ep("127.0.0.2");
        ReplicaSlotGroup slot = ReplicaSlotGroup.stableSlot(natural);

        // Empty set should not satisfy
        assertFalse(slot.isSatisfied(Collections.emptySet()));

        // Wrong endpoint should not satisfy
        assertFalse(slot.isSatisfied(Collections.singleton(other)));

        // Correct endpoint should satisfy
        assertTrue(slot.isSatisfied(Collections.singleton(natural)));

        // Multiple endpoints including natural should satisfy
        Set<InetAddressAndPort> both = new HashSet<>(Arrays.asList(natural, other));
        assertTrue(slot.isSatisfied(both));
    }

    @Test
    public void testTransitioningSlotSatisfaction() throws UnknownHostException
    {
        InetAddressAndPort natural = ep("127.0.0.1");
        InetAddressAndPort pending = ep("127.0.0.2");
        InetAddressAndPort other = ep("127.0.0.3");
        ReplicaSlotGroup slot = ReplicaSlotGroup.transitioningSlot(natural, pending);

        // Empty set should not satisfy
        assertFalse(slot.isSatisfied(Collections.emptySet()));

        // Only natural should not satisfy
        assertFalse(slot.isSatisfied(Collections.singleton(natural)));

        // Only pending should not satisfy
        assertFalse(slot.isSatisfied(Collections.singleton(pending)));

        // Both natural and pending should satisfy
        Set<InetAddressAndPort> both = new HashSet<>(Arrays.asList(natural, pending));
        assertTrue(slot.isSatisfied(both));

        // Natural, pending, and others should satisfy
        Set<InetAddressAndPort> all = new HashSet<>(Arrays.asList(natural, pending, other));
        assertTrue(slot.isSatisfied(all));
    }

    @Test
    public void testEquality() throws UnknownHostException
    {
        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");

        ReplicaSlotGroup stable1 = ReplicaSlotGroup.stableSlot(ep1);
        ReplicaSlotGroup stable2 = ReplicaSlotGroup.stableSlot(ep1);
        ReplicaSlotGroup stable3 = ReplicaSlotGroup.stableSlot(ep2);

        assertEquals(stable1, stable2);
        assertNotEquals(stable1, stable3);

        ReplicaSlotGroup trans1 = ReplicaSlotGroup.transitioningSlot(ep1, ep2);
        ReplicaSlotGroup trans2 = ReplicaSlotGroup.transitioningSlot(ep1, ep2);
        ReplicaSlotGroup trans3 = ReplicaSlotGroup.transitioningSlot(ep2, ep1);

        assertEquals(trans1, trans2);
        assertNotEquals(trans1, trans3);
        assertNotEquals(stable1, trans1);
    }

    // ==================== SlotGroupMaps Tests ====================

    @Test
    public void testSlotGroupMapsEmpty()
    {
        SlotGroupMaps maps = new SlotGroupMaps();
        assertTrue(maps.isEmpty());
        assertEquals(0, maps.size());
        assertNull(maps.getSlotInfoForToken(token(100)));
    }

    @Test
    public void testSlotGroupMapsLookup() throws UnknownHostException
    {
        SlotGroupMaps maps = new SlotGroupMaps();

        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");
        InetAddressAndPort ep3 = ep("127.0.0.3");

        // Add slot groups for token 100
        List<ReplicaSlotGroup> slots100 = Arrays.asList(
            ReplicaSlotGroup.stableSlot(ep1),
            ReplicaSlotGroup.stableSlot(ep2),
            ReplicaSlotGroup.stableSlot(ep3)
        );
        maps.addSlotGroups(token(100), slots100);

        // Add slot groups for token 200
        List<ReplicaSlotGroup> slots200 = Arrays.asList(
            ReplicaSlotGroup.stableSlot(ep2),
            ReplicaSlotGroup.stableSlot(ep3),
            ReplicaSlotGroup.stableSlot(ep1)
        );
        maps.addSlotGroups(token(200), slots200);

        assertFalse(maps.isEmpty());
        assertEquals(2, maps.size());

        // Lookup should find the ceiling entry
        SlotGroupMaps.SlotGroupInfo info50 = maps.getSlotInfoForToken(token(50));
        assertNotNull(info50);
        assertEquals(3, info50.endpointToSlot.size());  // 3 stable slots = 3 endpoints

        // Lookup exact token
        SlotGroupMaps.SlotGroupInfo info100 = maps.getSlotInfoForToken(token(100));
        assertNotNull(info100);
        assertEquals(3, info100.endpointToSlot.size());

        // Lookup between tokens
        SlotGroupMaps.SlotGroupInfo info150 = maps.getSlotInfoForToken(token(150));
        assertNotNull(info150);
        assertEquals(3, info150.endpointToSlot.size());
    }

    @Test
    public void testSlotGroupInfoPrecomputed() throws UnknownHostException
    {
        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");
        InetAddressAndPort ep3 = ep("127.0.0.3");
        InetAddressAndPort ep4 = ep("127.0.0.4");

        // Create slots: one stable, one transitioning
        List<ReplicaSlotGroup> slots = Arrays.asList(
            ReplicaSlotGroup.stableSlot(ep1),                    // Stable: needs 1 ack
            ReplicaSlotGroup.transitioningSlot(ep2, ep3),        // Transitioning: needs 2 acks
            ReplicaSlotGroup.stableSlot(ep4)                     // Stable: needs 1 ack
        );

        SlotGroupMaps.SlotGroupInfo info = new SlotGroupMaps.SlotGroupInfo(slots);

        // 4 endpoints mapped: ep1, ep2, ep3 (both in transitioning slot with ep2), ep4
        assertEquals(4, info.endpointToSlot.size());

        // Check endpoint-to-slot mapping
        ReplicaSlotGroup ep1Slot = info.endpointToSlot.get(ep1);
        assertNotNull(ep1Slot);
        assertFalse(ep1Slot.isTransitioning());
        assertEquals(ep1, ep1Slot.naturalEndpoint());
        assertEquals(1, ep1Slot.requiredAcks());

        // ep2 and ep3 should map to the same transitioning slot
        ReplicaSlotGroup ep2Slot = info.endpointToSlot.get(ep2);
        ReplicaSlotGroup ep3Slot = info.endpointToSlot.get(ep3);
        assertNotNull(ep2Slot);
        assertTrue(ep2Slot.isTransitioning());
        assertSame(ep2Slot, ep3Slot);  // Same slot object
        assertEquals(ep2, ep2Slot.naturalEndpoint());
        assertEquals(ep3, ep2Slot.pendingEndpoint());
        assertEquals(2, ep2Slot.requiredAcks());

        ReplicaSlotGroup ep4Slot = info.endpointToSlot.get(ep4);
        assertNotNull(ep4Slot);
        assertFalse(ep4Slot.isTransitioning());
        assertEquals(ep4, ep4Slot.naturalEndpoint());
        assertEquals(1, ep4Slot.requiredAcks());
    }

    @Test
    public void testSlotGroupInfoNoTransitioning() throws UnknownHostException
    {
        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");

        // All stable slots
        List<ReplicaSlotGroup> slots = Arrays.asList(
            ReplicaSlotGroup.stableSlot(ep1),
            ReplicaSlotGroup.stableSlot(ep2)
        );

        SlotGroupMaps.SlotGroupInfo info = new SlotGroupMaps.SlotGroupInfo(slots);

        // 2 stable slots = 2 endpoints
        assertEquals(2, info.endpointToSlot.size());

        // All slots should be stable (no transitioning)
        for (ReplicaSlotGroup slot : info.endpointToSlot.values())
        {
            assertFalse(slot.isTransitioning());
            assertEquals(1, slot.requiredAcks());
        }
    }

    @Test
    public void testSlotGroupMapsWrapAround() throws UnknownHostException
    {
        SlotGroupMaps maps = new SlotGroupMaps();

        InetAddressAndPort ep1 = ep("127.0.0.1");

        // Add only one slot group at token 100
        maps.addSlotGroups(token(100), Collections.singletonList(ReplicaSlotGroup.stableSlot(ep1)));

        // Lookup for token > 100 should wrap around to first entry
        SlotGroupMaps.SlotGroupInfo info500 = maps.getSlotInfoForToken(token(500));
        assertNotNull(info500);
        assertEquals(1, info500.endpointToSlot.size());
    }

    // ==================== SlotGroupMaps.Builder Tests ====================

    @Test
    public void testSlotGroupMapsBuilderAllStable() throws UnknownHostException
    {
        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");
        InetAddressAndPort ep3 = ep("127.0.0.3");

        Set<Token> allTokens = new TreeSet<>(Arrays.asList(token(100), token(200), token(300)));
        Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot = new HashMap<>();

        // Simulated natural replicas: same 3 endpoints for all tokens
        EndpointsForRange stableReplicas = EndpointsForRange.of(
            new Replica(ep1, token(0), token(100), true),
            new Replica(ep2, token(0), token(100), true),
            new Replica(ep3, token(0), token(100), true)
        );

        SlotGroupMaps result = SlotGroupMaps.Builder.build(allTokens, tokenToPendingSlot, t -> stableReplicas);

        assertFalse(result.isEmpty());
        assertEquals(3, result.size());

        // All slots should be stable
        for (Token t : allTokens)
        {
            SlotGroupMaps.SlotGroupInfo info = result.getSlotInfoForToken(t);
            assertNotNull(info);
            assertEquals(3, info.endpointToSlot.size());
            for (ReplicaSlotGroup slot : info.endpointToSlot.values())
            {
                assertFalse(slot.isTransitioning());
            }
        }
    }

    @Test
    public void testSlotGroupMapsBuilderWithTransitioning() throws UnknownHostException
    {
        InetAddressAndPort ep1 = ep("127.0.0.1");
        InetAddressAndPort ep2 = ep("127.0.0.2");
        InetAddressAndPort ep3 = ep("127.0.0.3");
        InetAddressAndPort ep4 = ep("127.0.0.4");

        Set<Token> allTokens = new TreeSet<>(Arrays.asList(token(100), token(200)));

        // Token 100 has a pending: ep4 is replacing ep3
        Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot = new HashMap<>();
        tokenToPendingSlot.put(token(100), Pair.create(ep4, ep3));

        // Natural replicas for all tokens: ep1, ep2, ep3
        EndpointsForRange naturalReplicas = EndpointsForRange.of(
            new Replica(ep1, token(0), token(100), true),
            new Replica(ep2, token(0), token(100), true),
            new Replica(ep3, token(0), token(100), true)
        );

        SlotGroupMaps result = SlotGroupMaps.Builder.build(allTokens, tokenToPendingSlot, t -> naturalReplicas);

        // Token 100: ep1 stable, ep2 stable, ep3->ep4 transitioning
        SlotGroupMaps.SlotGroupInfo info100 = result.getSlotInfoForToken(token(100));
        assertNotNull(info100);
        // 4 endpoints mapped: ep1, ep2, ep3 (natural of transitioning), ep4 (pending of transitioning)
        assertEquals(4, info100.endpointToSlot.size());

        ReplicaSlotGroup ep3Slot = info100.endpointToSlot.get(ep3);
        assertNotNull(ep3Slot);
        assertTrue(ep3Slot.isTransitioning());
        assertEquals(ep3, ep3Slot.naturalEndpoint());
        assertEquals(ep4, ep3Slot.pendingEndpoint());

        // ep4 should map to the same transitioning slot as ep3
        assertSame(ep3Slot, info100.endpointToSlot.get(ep4));

        // ep1 and ep2 should be stable
        assertFalse(info100.endpointToSlot.get(ep1).isTransitioning());
        assertFalse(info100.endpointToSlot.get(ep2).isTransitioning());

        // Token 200: no pending, all stable
        SlotGroupMaps.SlotGroupInfo info200 = result.getSlotInfoForToken(token(200));
        assertNotNull(info200);
        assertEquals(3, info200.endpointToSlot.size());
        for (ReplicaSlotGroup slot : info200.endpointToSlot.values())
        {
            assertFalse(slot.isTransitioning());
        }
    }

    @Test
    public void testSlotGroupMapsBuilderEmptyTokens()
    {
        Set<Token> allTokens = new TreeSet<>();
        Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot = new HashMap<>();

        SlotGroupMaps result = SlotGroupMaps.Builder.build(allTokens, tokenToPendingSlot, t -> null);

        assertTrue(result.isEmpty());
    }
}
