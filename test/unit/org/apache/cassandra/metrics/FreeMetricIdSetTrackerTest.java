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

package org.apache.cassandra.metrics;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.metrics.ThreadLocalMetrics.FreeMetricIdSetTracker;

import static org.junit.Assert.assertEquals;

public class FreeMetricIdSetTrackerTest
{
    // free and release ids in interleaved portions, and verify that a later portion does not
    // become reusable before its own two-cycle delay has elapsed - i.e. only the ids that are
    // actually due are released, never the freshly freed ones.
    @Test
    public void testInterleavedFreeAndReleaseReleasesOnlyExpectedIds()
    {
        FreeMetricIdSetTracker tracker = getTrackerToTest();

        // portion A: free, then one release cycle (A is now in the delayed buffer, not yet reusable)
        Set<Integer> portionA = ImmutableSet.of(1, 2, 3);
        portionA.forEach(tracker::markAsFree);
        tracker.triggerRecycling();
        assertEquals(0, tracker.getFreeMetricSetCardinality());

        // portion B: free a fresh batch, then another release cycle.
        // this cycle is A's second cycle (A becomes reusable) but only B's first (B must stay held).
        Set<Integer> portionB = ImmutableSet.of(10, 11);
        portionB.forEach(tracker::markAsFree);
        tracker.triggerRecycling();

        // only portion A is released here - portion B must not leak out yet
        assertEquals(portionA.size(), tracker.getFreeMetricSetCardinality());
        assertEquals(portionA, drainFreeIds(tracker));

        // a further release cycle finally makes portion B reusable, and nothing else
        tracker.triggerRecycling();
        assertEquals(portionB.size(), tracker.getFreeMetricSetCardinality());
        assertEquals(portionB, drainFreeIds(tracker));

        assertEquals(0, tracker.getFreeMetricSetCardinality());
        assertEquals(-1, tracker.getFreeMetricId());
    }

    @Test
    public void testTriggerRecyclingIsNoOpWhenNothingFreed()
    {
        FreeMetricIdSetTracker tracker = getTrackerToTest();

        // triggering with an empty tracker must not produce phantom free ids
        tracker.triggerRecycling();
        tracker.triggerRecycling();

        assertEquals(0, tracker.getFreeMetricSetCardinality());
        assertEquals(-1, tracker.getFreeMetricId());
    }

    private static FreeMetricIdSetTracker getTrackerToTest()
    {
        return  new FreeMetricIdSetTracker()
        {
            @Override
            protected void scheduleCleanupTask()
            {
                // disable scheduling to make the test deterministic
            }
        };
    }

    /**
     * Drains every id currently available for reuse out of the tracker.
     */
    private static Set<Integer> drainFreeIds(FreeMetricIdSetTracker tracker)
    {
        Set<Integer> ids = new HashSet<>();
        int id;
        while ((id = tracker.getFreeMetricId()) >= 0)
            ids.add(id);
        return ids;
    }
}
