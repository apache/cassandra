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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Predicate;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Simple response tracker that counts responses against a single threshold.
 * <p>
 * Supports optional predicate filtering to selectively count responses
 * (e.g., LOCAL_* consistency levels that only count local DC responses).
 */
public class SimpleResponseTracker implements ResponseTracker
{
    private static final AtomicIntegerFieldUpdater<SimpleResponseTracker> RESPONSES_UPDATER
        = AtomicIntegerFieldUpdater.newUpdater(SimpleResponseTracker.class, "responses");
    private static final AtomicIntegerFieldUpdater<SimpleResponseTracker> FAILURES_UPDATER
        = AtomicIntegerFieldUpdater.newUpdater(SimpleResponseTracker.class, "failures");
    private static final Predicate<InetAddressAndPort> NO_FILTER = address -> true;

    private final int blockFor;
    private final int totalReplicas;
    private final Predicate<InetAddressAndPort> filter;
    private volatile int responses = 0;
    private volatile int failures = 0;

    /**
     * Create unfiltered tracker
     *
     * @param blockFor      number of responses required for quorum
     * @param totalReplicas total replicas available (for early failure detection)
     */
    public SimpleResponseTracker(int blockFor, int totalReplicas)
    {
        this(blockFor, totalReplicas, NO_FILTER);
    }

    /**
     * Create filtered tracker
     *
     * @param blockFor      number of responses required for quorum
     * @param totalReplicas total replicas available (for early failure detection)
     * @param filter        predicate to test if response counts (null = all count)
     */
    public SimpleResponseTracker(int blockFor, int totalReplicas,
                                 Predicate<InetAddressAndPort> filter)
    {
        if (blockFor < 0)
            throw new IllegalArgumentException("blockFor must be non-negative: " + blockFor);
        if (totalReplicas < 0)
            throw new IllegalArgumentException("totalReplicas must be non-negative: " + totalReplicas);

        this.blockFor = blockFor;
        this.totalReplicas = totalReplicas;
        this.filter = filter != null ? filter : NO_FILTER;
    }

    @Override
    public void onResponse(InetAddressAndPort from)
    {
        if (countsTowardQuorum(from))
            RESPONSES_UPDATER.incrementAndGet(this);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        if (countsTowardQuorum(from))
            FAILURES_UPDATER.incrementAndGet(this);
    }

    @Override
    public boolean isComplete()
    {
        int r = responses;
        int f = failures;

        if (r >= blockFor)
            return true;

        // failure: can't reach blockFor
        int needed = blockFor - r;
        int remaining = totalReplicas - (r + f);
        return needed > remaining;
    }

    @Override
    public boolean isSuccessful()
    {
        return responses >= blockFor;
    }

    @Override
    public int required()
    {
        return blockFor;
    }

    @Override
    public int received()
    {
        return responses;
    }

    @Override
    public int failures()
    {
        return failures;
    }

    @Override
    public boolean countsTowardQuorum(InetAddressAndPort from)
    {
        return filter.test(from);
    }

    @Override
    public String toString()
    {
        return String.format("SimpleResponseTracker[blockFor=%d, totalReplicas=%d, responses=%d, failures=%d, filtered=%s]",
                             blockFor, totalReplicas, responses, failures, filter != NO_FILTER);
    }

    @Override
    public boolean isPending(InetAddressAndPort from)
    {
        return false;
    }

    @Override
    public int totalContacts()
    {
        return totalReplicas;
    }

    @Override
    public int pendingContacts()
    {
        return 0;
    }

    @Override
    public SimpleResponseTracker resetCopy()
    {
        return new SimpleResponseTracker(blockFor, totalReplicas, filter);
    }
}
