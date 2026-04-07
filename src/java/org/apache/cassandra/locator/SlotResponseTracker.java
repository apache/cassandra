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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generic per-slot ack counting for replica slot grouping.
 *
 * Thread-safe: the map structure is immutable after construction; only the atomic counters
 * and flags are mutated. Used by both the normal write path (AbstractWriteResponseHandler)
 * and all three Paxos V2 phases (Prepare, Propose, Commit).
 *
 * A slot is "satisfied" when all its members have responded successfully.
 * A slot is "failed" when any member has failed, making satisfaction impossible.
 */
public class SlotResponseTracker
{
    private final SlotGroupMaps.SlotGroupInfo slotInfo;
    private final int totalSlots;
    private final Map<ReplicaSlotGroup, AtomicInteger> acksPerSlot;
    private final Map<ReplicaSlotGroup, AtomicBoolean> failedPerSlot;
    private final AtomicInteger satisfiedSlotCount = new AtomicInteger(0);
    private final AtomicInteger failedSlotCount = new AtomicInteger(0);

    public SlotResponseTracker(SlotGroupMaps.SlotGroupInfo slotInfo)
    {
        this.slotInfo = slotInfo;

        Set<ReplicaSlotGroup> uniqueSlots = new HashSet<>(slotInfo.endpointToSlot.values());
        this.totalSlots = uniqueSlots.size();
        Map<ReplicaSlotGroup, AtomicInteger> counters = new HashMap<>(totalSlots);
        Map<ReplicaSlotGroup, AtomicBoolean> failed = new HashMap<>(totalSlots);
        for (ReplicaSlotGroup slot : uniqueSlots)
        {
            counters.put(slot, new AtomicInteger(0));
            failed.put(slot, new AtomicBoolean(false));
        }
        this.acksPerSlot = counters;
        this.failedPerSlot = failed;
    }

    /**
     * Record a successful response from an endpoint.
     *
     * @return the new satisfied slot count if this response caused a slot to become satisfied,
     *         or -1 otherwise. The returned count is the atomic snapshot at the moment of
     *         satisfaction, safe for == comparisons.
     */
    public int recordSuccess(InetAddressAndPort from)
    {
        ReplicaSlotGroup slot = slotInfo.endpointToSlot.get(from);
        assert slot != null;

        int acks = acksPerSlot.get(slot).incrementAndGet();
        if (acks == slot.requiredAcks())
            return satisfiedSlotCount.incrementAndGet();
        return -1;
    }

    /**
     * Record a failure from an endpoint, marking its slot as failed. A failed slot can never
     * be satisfied since all members must ack. Thread-safe: compareAndSet ensures each slot
     * is counted at most once.
     *
     * @return the new failed slot count if this failure caused a new slot to fail, or -1 if
     *         already failed. The returned count is the atomic snapshot at the moment of
     *         failure, safe for == comparisons.
     */
    public int recordFailure(InetAddressAndPort from)
    {
        ReplicaSlotGroup slot = slotInfo.endpointToSlot.get(from);
        assert slot != null;

        AtomicBoolean failed = failedPerSlot.get(slot);
        if (failed != null && failed.compareAndSet(false, true))
            return failedSlotCount.incrementAndGet();
        return -1;
    }

    /**
     * @return true if enough slots are still alive (not failed) to potentially reach the
     *         required quorum. For lock-free callers that need exactly-once semantics, use
     *         the return value of {@link #recordFailure} with {@link #canSucceedWithFailed}.
     */
    public boolean canSucceed(int required)
    {
        return totalSlots - failedSlotCount.get() >= required;
    }

    /**
     * Check using a specific failed count snapshot (from {@link #recordFailure}) rather than
     * the current state. Safe for exactly-once checks in lock-free contexts.
     */
    public boolean canSucceedWithFailed(int failedCount, int required)
    {
        return totalSlots - failedCount >= required;
    }

    /**
     * Reset all counters. Only safe when no concurrent calls are possible
     * (e.g. inside a synchronized block).
     */
    public void reset()
    {
        for (AtomicInteger counter : acksPerSlot.values())
            counter.set(0);
        for (AtomicBoolean failed : failedPerSlot.values())
            failed.set(false);
        satisfiedSlotCount.set(0);
        failedSlotCount.set(0);
    }

    /** @return number of slots where all members responded */
    public int satisfiedCount()
    {
        return satisfiedSlotCount.get();
    }
}
