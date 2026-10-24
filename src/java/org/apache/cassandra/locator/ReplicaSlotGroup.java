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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

/**
 * Represents a replica slot during topology changes.
 * 
 * A slot can be:
 * - Stable: Contains only a natural endpoint (no pending transition)
 * - Transitioning: Contains both a natural endpoint (current owner) and a pending endpoint (future owner)
 * 
 * For transitioning slots, BOTH endpoints must acknowledge a write for the slot to be satisfied.
 * This ensures durability regardless of whether the transition completes or aborts.
 * 
 * Key design constraint: For any given token, there can be at most ONE pending replica,
 * so slot groups are always either size 1 (stable) or size 2 (transitioning).
 */
public final class ReplicaSlotGroup
{
    // The natural (current) replica for this slot - always present
    private final InetAddressAndPort naturalEndpoint;

    // The pending replica transitioning into this slot - null if stable
    @Nullable
    private final InetAddressAndPort pendingEndpoint;

    private ReplicaSlotGroup(InetAddressAndPort naturalEndpoint, @Nullable InetAddressAndPort pendingEndpoint)
    {
        this.naturalEndpoint = naturalEndpoint;
        this.pendingEndpoint = pendingEndpoint;
    }

    /**
     * Create a stable slot (no transition happening).
     *
     * @param naturalEndpoint The current owner of this replica slot
     */
    public static ReplicaSlotGroup stableSlot(InetAddressAndPort naturalEndpoint)
    {
        Objects.requireNonNull(naturalEndpoint, "naturalEndpoint cannot be null");
        return new ReplicaSlotGroup(naturalEndpoint, null);
    }

    /**
     * Create a transitioning slot.
     * During a topology change, the natural endpoint is giving up this slot
     * to the pending endpoint.
     *
     * @param naturalEndpoint The current owner (will lose this slot after transition)
     * @param pendingEndpoint The future owner (will gain this slot after transition)
     */
    public static ReplicaSlotGroup transitioningSlot(InetAddressAndPort naturalEndpoint,
                                                    InetAddressAndPort pendingEndpoint)
    {
        Objects.requireNonNull(naturalEndpoint, "naturalEndpoint cannot be null");
        Objects.requireNonNull(pendingEndpoint, "pendingEndpoint cannot be null for transitioning slot");
        return new ReplicaSlotGroup(naturalEndpoint, pendingEndpoint);
    }

    /**
     * @return true if this slot is transitioning (has both natural and pending endpoints)
     */
    public boolean isTransitioning()
    {
        return pendingEndpoint != null;
    }

    /**
     * @return The natural (current) endpoint for this slot
     */
    public InetAddressAndPort naturalEndpoint()
    {
        return naturalEndpoint;
    }

    /**
     * @return The pending endpoint for this slot, or null if this is a stable slot
     */
    @Nullable
    public InetAddressAndPort pendingEndpoint()
    {
        return pendingEndpoint;
    }

    /**
     * Check if this slot is satisfied by the given acked endpoints.
     * - Stable slot: natural endpoint must have acked
     * - Transitioning slot: BOTH natural AND pending must have acked
     *
     * @param ackedEndpoints Set of endpoints that have acknowledged the write
     * @return true if this slot's requirements are satisfied
     */
    public boolean isSatisfied(Set<InetAddressAndPort> ackedEndpoints)
    {
        if (!ackedEndpoints.contains(naturalEndpoint))
        {
            return false;
        }
        if (pendingEndpoint != null && !ackedEndpoints.contains(pendingEndpoint))
        {
            return false;
        }
        return true;
    }

    /**
     * @return The number of acknowledgments required to satisfy this slot (1 for stable, 2 for transitioning)
     */
    public int requiredAcks()
    {
        return pendingEndpoint != null ? 2 : 1;
    }

    /**
     * @return All endpoint members of this slot
     */
    public Set<InetAddressAndPort> members()
    {
        if (pendingEndpoint == null)
        {
            return Collections.singleton(naturalEndpoint);
        }
        return ImmutableSet.of(naturalEndpoint, pendingEndpoint);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaSlotGroup that = (ReplicaSlotGroup) o;
        return Objects.equals(naturalEndpoint, that.naturalEndpoint) &&
               Objects.equals(pendingEndpoint, that.pendingEndpoint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(naturalEndpoint, pendingEndpoint);
    }

    @Override
    public String toString()
    {
        if (pendingEndpoint == null)
        {
            return "Slot[" + naturalEndpoint + "]";
        }
        return "Slot[" + naturalEndpoint + " -> " + pendingEndpoint + "]";
    }
}
