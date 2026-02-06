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

import java.util.Map;
import java.util.function.ToIntFunction;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Per-datacenter response tracker that requires ALL datacenters to independently reach quorum.
 * <p>
 * Composes multiple ResponseTrackers (one per datacenter) and delegates operations
 * to the appropriate tracker based on endpoint datacenter. Used for EACH_QUORUM consistency.
 * <p>
 * The composed trackers can be any ResponseTracker implementation:
 * <ul>
 *   <li>{@link SimpleResponseTracker} for basic quorum tracking</li>
 *   <li>{@link WriteResponseTracker} for writes with pending replicas (double-count model)</li>
 * </ul>
 * <p>
 * Thread-safe through delegation to thread-safe ResponseTracker implementations.
 */
public class PerDcResponseTracker implements ResponseTracker
{
    private final Map<String, ResponseTracker> trackerPerDc;
    private final Locator locator;

    /**
     * Create per-DC tracker with pre-built trackers for each datacenter.
     *
     * @param trackerPerDc map of datacenter name to tracker (must be non-empty)
     * @param locator      for looking up datacenter from endpoint
     */
    public PerDcResponseTracker(Map<String, ResponseTracker> trackerPerDc, Locator locator)
    {
        if (trackerPerDc == null || trackerPerDc.isEmpty())
            throw new IllegalArgumentException("trackerPerDc cannot be null or empty");
        if (locator == null)
            throw new IllegalArgumentException("locator cannot be null");

        this.locator = locator;
        this.trackerPerDc = trackerPerDc;
    }

    private int count(ToIntFunction<ResponseTracker> getter)
    {
        int total = 0;
        for (ResponseTracker tracker : trackerPerDc.values())
            total += getter.applyAsInt(tracker);
        return total;
    }

    @Override
    public void onResponse(InetAddressAndPort from)
    {
        ResponseTracker tracker = getTrackerForEndpoint(from);
        if (tracker != null)
            tracker.onResponse(from);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        ResponseTracker tracker = getTrackerForEndpoint(from);
        if (tracker != null)
            tracker.onFailure(from, reason);
    }

    @Override
    public boolean isComplete()
    {
        // Complete when ALL DCs are complete (either success or definite failure)
        for (ResponseTracker tracker : trackerPerDc.values())
        {
            if (!tracker.isComplete())
                return false;
        }
        return true;
    }

    @Override
    public boolean isSuccessful()
    {
        // Successful only if ALL DCs are successful
        for (ResponseTracker tracker : trackerPerDc.values())
        {
            if (!tracker.isSuccessful())
                return false;
        }
        return true;
    }

    @Override
    public int required()
    {
        return count(ResponseTracker::required);
    }

    @Override
    public int received()
    {
        return count(ResponseTracker::received);
    }

    @Override
    public int failures()
    {
        return count(ResponseTracker::failures);
    }

    @Override
    public boolean countsTowardQuorum(InetAddressAndPort from)
    {
        String dc = locator.location(from).datacenter;
        if (dc == null)
            return false;
        ResponseTracker tracker = trackerPerDc.get(dc);
        return tracker != null && tracker.countsTowardQuorum(from);
    }

    private ResponseTracker getTrackerForEndpoint(InetAddressAndPort from)
    {
        String dc = locator.location(from).datacenter;
        return trackerPerDc.get(dc);
    }

    /**
     * @return the tracker for the specified datacenter, or null if not tracked
     */
    public ResponseTracker getTrackerForDc(String datacenter)
    {
        return trackerPerDc.get(datacenter);
    }

    @Override
    public String toString()
    {
        return String.format("PerDcResponseTracker[datacenters=%s, trackerPerDc=%s]",
                           trackerPerDc.keySet(), trackerPerDc);
    }

    @Override
    public boolean isPending(InetAddressAndPort from)
    {
        String dc = locator.location(from).datacenter;
        if (dc == null)
            return false;
        ResponseTracker tracker = trackerPerDc.get(dc);
        return tracker != null && tracker.isPending(from);
    }

    @Override
    public int totalRequired()
    {
        return count(ResponseTracker::totalRequired);
    }

    @Override
    public int totalContacts()
    {
        return count(ResponseTracker::totalContacts);
    }

    @Override
    public int pendingContacts()
    {
        return count(ResponseTracker::pendingContacts);
    }
}
