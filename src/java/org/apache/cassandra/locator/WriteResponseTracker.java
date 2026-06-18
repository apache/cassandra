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

import java.util.function.Predicate;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Response tracker for writes with pending replicas using the double count model.
 * <p>
 * Two requirements must be satisfied for success:
 * <ol>
 *   <li>Committed replicas must satisfy base CL: {@code naturalSuccesses >= baseBlockFor}</li>
 *   <li>Total replicas (natural + pending) must satisfy: {@code totalSuccesses >= totalBlockFor}</li>
 * </ol>
 * <p>
 * This ensures both consistency (natural replicas have the data) and bootstrap safety
 * (pending replicas also receive the write).
 * <p>
 * Implemented by composing two {@link SimpleResponseTracker}s:
 * <ul>
 *   <li>naturalTracker: tracks responses from natural replicas only</li>
 *   <li>totalTracker: tracks responses from all replicas (natural + pending)</li>
 * </ul>
 * <p>
 * Thread-safe through delegation to thread-safe SimpleResponseTrackers.
 */
public class WriteResponseTracker implements ResponseTracker
{
    private final SimpleResponseTracker natural;
    private final SimpleResponseTracker total;

    /**
     * Create a write response tracker with the double count model.
     *
     * @param baseBlockFor      number of natural replica responses required for CL
     * @param totalBlockFor     total responses required (baseBlockFor + pending count)
     * @param naturalReplicas number of natural replicas available
     * @param pendingReplicas   number of pending replicas available
     * @param isPending         predicate to determine if an endpoint is pending
     */
    public WriteResponseTracker(int baseBlockFor,
                                int totalBlockFor,
                                int naturalReplicas,
                                int pendingReplicas,
                                Predicate<InetAddressAndPort> isPending)
    {
        this(baseBlockFor, totalBlockFor, naturalReplicas, pendingReplicas, isPending, null);
    }

    /**
     * Create a write response tracker with the double count model and a filter.
     *
     * @param naturalBlockFor      number of natural replica responses required for CL
     * @param totalBlockFor     total responses required (naturalBlockFor + pending count)
     * @param naturalReplicas number of natural replicas available
     * @param pendingReplicas   number of pending replicas available
     * @param isPending         predicate to determine if an endpoint is pending
     * @param filter            predicate to filter which responses count (e.g., InOurDc for LOCAL_QUORUM)
     */
    public WriteResponseTracker(int naturalBlockFor,
                                int totalBlockFor,
                                int naturalReplicas,
                                int pendingReplicas,
                                Predicate<InetAddressAndPort> isPending,
                                Predicate<InetAddressAndPort> filter)
    {
        if (naturalBlockFor < 0)
            throw new IllegalArgumentException("naturalBlockFor must be non-negative: " + naturalBlockFor);
        if (totalBlockFor < naturalBlockFor)
            throw new IllegalArgumentException("totalBlockFor (" + totalBlockFor + ") must be >= naturalBlockFor (" + naturalBlockFor + ")");
        if (naturalReplicas < 0)
            throw new IllegalArgumentException("naturalReplicas must be non-negative: " + naturalReplicas);
        if (pendingReplicas < 0)
            throw new IllegalArgumentException("pendingReplicas must be non-negative: " + pendingReplicas);
        if (naturalBlockFor > naturalReplicas)
            throw new IllegalArgumentException("naturalBlockFor (" + naturalBlockFor + ") cannot exceed naturalReplicas (" + naturalReplicas + ")");
        if (totalBlockFor > naturalReplicas + pendingReplicas)
            throw new IllegalArgumentException("totalBlockFor (" + totalBlockFor + ") cannot exceed total replicas (" + (naturalReplicas + pendingReplicas) + ")");
        if (isPending == null)
            throw new IllegalArgumentException("isPending predicate cannot be null");

        Predicate<InetAddressAndPort> naturalFilter = isPending.negate();
        if (filter != null)
            naturalFilter = naturalFilter.and(filter);

        this.natural = new SimpleResponseTracker(naturalBlockFor, naturalReplicas, naturalFilter);
        this.total = new SimpleResponseTracker(totalBlockFor, naturalReplicas + pendingReplicas, filter);
    }

    private WriteResponseTracker(SimpleResponseTracker natural, SimpleResponseTracker total)
    {
        this.natural = natural;
        this.total = total;
    }

    @Override
    public void onResponse(InetAddressAndPort from)
    {
        natural.onResponse(from);
        total.onResponse(from);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        natural.onFailure(from, reason);
        total.onFailure(from, reason);
    }

    @Override
    public boolean isComplete()
    {
        // Early failure if either tracker fails
        if (natural.isComplete() && !natural.isSuccessful())
            return true;
        if (total.isComplete() && !total.isSuccessful())
            return true;

        // Success requires both to succeed
        return natural.isSuccessful() && total.isSuccessful();
    }

    @Override
    public boolean isSuccessful()
    {
        return natural.isSuccessful() && total.isSuccessful();
    }

    @Override
    public int required()
    {
        // Return total requirement for error messages
        return total.required();
    }

    @Override
    public int received()
    {
        // Return total successes for error messages
        return total.received();
    }

    @Override
    public int failures()
    {
        // Return total failures for error messages
        return total.failures();
    }

    @Override
    public boolean countsTowardQuorum(InetAddressAndPort from)
    {
        return total.countsTowardQuorum(from);
    }

    public int naturalReceived()
    {
        return natural.received();
    }

    public int pendingReceived()
    {
        return total.received() - natural.received();
    }

    public int naturalFailures()
    {
        return natural.failures();
    }

    public int pendingFailures()
    {
        return total.failures() - natural.failures();
    }

    public int baseBlockFor()
    {
        return natural.required();
    }

    @Override
    public String toString()
    {
        return String.format("WriteResponseTracker[baseBlockFor=%d, totalBlockFor=%d, " +
                             "naturalTracker=%s, totalTracker=%s]",
                             baseBlockFor(), required(),
                             natural, total);
    }

    @Override
    public boolean isPending(InetAddressAndPort from)
    {
        return total.countsTowardQuorum(from) && !natural.countsTowardQuorum(from);
    }

    @Override
    public int totalContacts()
    {
        return total.totalContacts();
    }

    @Override
    public int pendingContacts()
    {
        return total.totalContacts() - natural.totalContacts();
    }

    @Override
    public ResponseTracker resetCopy()
    {
        return new WriteResponseTracker(natural.resetCopy(), total.resetCopy());
    }
}
