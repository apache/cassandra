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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import javax.annotation.Nullable;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Pair;

/**
 * Stores pre-computed replica slot groups indexed by token boundaries for efficient lookup at write time.
 * 
 * This class is designed for the write hot path:
 * - Slot groups are computed once on topology change
 * - Lookups use binary search: O(log n)
 * - Endpoint-to-slot mapping is pre-computed for O(1) lookup
 * - No allocation or computation at write time - just a reference lookup
 * 
 * Thread safety: This class is immutable after construction. The internal NavigableMap
 * and SlotGroupInfo instances are not modified after being added.
 */
public class SlotGroupMaps
{
    /**
     * Pre-computed info for a token range - avoids per-write computation.
     *
     * Maps each endpoint (natural or pending) to its ReplicaSlotGroup for O(1) lookup.
     * Multiple endpoints can map to the same slot (transitioning slot with natural + pending).
     *
     * This class is immutable and shared across all writes to the same token range.
     */
    public static class SlotGroupInfo
    {
        /** Maps each endpoint (natural or pending) to its ReplicaSlotGroup */
        public final Map<InetAddressAndPort, ReplicaSlotGroup> endpointToSlot;

        /**
         * Creates a SlotGroupInfo with pre-computed endpoint-to-slot mapping.
         *
         * @param slotGroups The slot groups for a token range
         */
        public SlotGroupInfo(List<ReplicaSlotGroup> slotGroups)
        {
            Map<InetAddressAndPort, ReplicaSlotGroup> epToSlot = new HashMap<>();

            for (ReplicaSlotGroup slot : slotGroups)
            {
                epToSlot.put(slot.naturalEndpoint(), slot);
                if (slot.pendingEndpoint() != null)
                {
                    epToSlot.put(slot.pendingEndpoint(), slot);
                }
            }

            this.endpointToSlot = Collections.unmodifiableMap(epToSlot);
        }

        @Override
        public String toString()
        {
            return "SlotGroupInfo{endpointToSlot=" + endpointToSlot + "}";
        }
    }

    /**
     * Sorted map: token boundary -> SlotGroupInfo for the range ending at that token.
     * For token T, this gives slots for range (predecessor(T), T].
     * 
     * Using NavigableMap allows O(log n) lookup via ceilingEntry().
     */
    private final NavigableMap<Token, SlotGroupInfo> tokenToSlotInfo;

    /**
     * Creates an empty SlotGroupMaps.
     */
    public SlotGroupMaps()
    {
        this.tokenToSlotInfo = new TreeMap<>();
    }

    /**
     * Add slot groups for a token range boundary.
     * Pre-computes endpoint-to-slot mapping for O(1) response handling.
     *
     * @param token The token that ends this range
     * @param slotGroups The slot groups for this range
     */
    public void addSlotGroups(Token token, List<ReplicaSlotGroup> slotGroups)
    {
        tokenToSlotInfo.put(token, new SlotGroupInfo(slotGroups));
    }

    /**
     * Get slot group info for a given token using binary search.
     * Finds the first token >= searchToken (the range containing searchToken).
     *
     * Performance: O(log n) where n is the number of token boundaries.
     *
     * @param searchToken The token to look up
     * @return SlotGroupInfo with pre-computed mappings, or null if not found
     */
    @Nullable
    public SlotGroupInfo getSlotInfoForToken(Token searchToken)
    {
        // Find first token >= searchToken: O(log n)
        Map.Entry<Token, SlotGroupInfo> entry = tokenToSlotInfo.ceilingEntry(searchToken);
        if (entry != null)
        {
            return entry.getValue();
        }
        // Wrap around to first token in ring (handles tokens after the last boundary)
        entry = tokenToSlotInfo.firstEntry();
        return entry != null ? entry.getValue() : null;
    }

    /**
     * @return true if this map contains any slot groups
     */
    public boolean isEmpty()
    {
        return tokenToSlotInfo.isEmpty();
    }

    /**
     * @return The number of token boundaries stored
     */
    public int size()
    {
        return tokenToSlotInfo.size();
    }

    @Override
    public String toString()
    {
        return "SlotGroupMaps{size=" + tokenToSlotInfo.size() + "}";
    }

    /**
     * Builds a SlotGroupMaps from transit pairs and natural replica information.
     * Used identically on both Cassandra 4.1 (TokenMetadata) and trunk (TCM).
     *
     * The caller provides:
     * - Token boundaries and transit pair mappings (from a version-specific calculator)
     * - A function to resolve natural replicas for a token (version-specific)
     */
    public static class Builder
    {
        /**
         * Build SlotGroupMaps from transit pairs and a natural replica provider.
         *
         * @param allTokens all token boundaries to build slot groups for
         * @param tokenToPendingSlot per-token mapping of (pendingEndpoint, replacedNaturalEndpoint)
         * @param naturalReplicasFn function: token -> EndpointsForRange (natural replicas for that token)
         * @return SlotGroupMaps with pre-computed slot groups for each token boundary
         */
        public static SlotGroupMaps build(
            Set<Token> allTokens,
            Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot,
            Function<Token, EndpointsForRange> naturalReplicasFn)
        {
            SlotGroupMaps result = new SlotGroupMaps();

            for (Token token : allTokens)
            {
                EndpointsForRange naturalReplicas = naturalReplicasFn.apply(token);
                Pair<InetAddressAndPort, InetAddressAndPort> pendingInfo = tokenToPendingSlot.get(token);

                List<ReplicaSlotGroup> slotGroups = new ArrayList<>();

                for (Replica replica : naturalReplicas)
                {
                    InetAddressAndPort naturalEp = replica.endpoint();

                    if (pendingInfo != null && naturalEp.equals(pendingInfo.right))
                    {
                        slotGroups.add(ReplicaSlotGroup.transitioningSlot(naturalEp, pendingInfo.left));
                    }
                    else
                    {
                        slotGroups.add(ReplicaSlotGroup.stableSlot(naturalEp));
                    }
                }

                result.addSlotGroups(token, slotGroups);
            }

            return result;
        }
    }
}
