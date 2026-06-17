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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Black-box response tracker encapsulating coordination completion logic.
 *
 * Created by replication strategy for each operation, this interface allows
 * strategies to customize how quorum requirements are calculated and enforced.
 * Different implementations can provide different semantics.
 *
 * The tracker is responsible for:
 * 1. Recording responses and failures from replicas
 * 2. Determining when the operation has completed (success or definite failure)
 * 3. Providing metrics for error messages and monitoring
 *
 * Thread safety: Implementations must be thread-safe as onResponse/onFailure
 * can be called concurrently from multiple network threads.
 */
public interface ResponseTracker
{
    // TODO: review replica plan members and move here as appropriate

    /**
     * Record a successful response from a replica.
     *
     * @param from endpoint that responded successfully
     */
    void onResponse(InetAddressAndPort from);

    /**
     * Record a failed response from a replica.
     *
     * @param from endpoint that failed
     * @param reason failure reason for metrics and error messages
     */
    void onFailure(InetAddressAndPort from, RequestFailureReason reason);

    // TODO: consider having an outcome method that returns an enum (PENDING, SUCCESS, FAILURE)
    /**
     * Has the operation completed (either success or definite failure)?
     *
     * An operation is complete when:
     * - Success: Required quorum has been achieved
     * - Definite failure: Not enough replicas remain to achieve quorum
     *
     * @return true if no more responses are needed to make a decision
     */
    boolean isComplete();

    /**
     * Did the operation succeed (quorum achieved)?
     *
     * Only meaningful if isComplete() returns true.
     *
     * @return true if required quorum was met
     */
    boolean isSuccessful();

    /**
     * How many responses are required for success?
     *
     * Used for error messages, metrics, and UnavailableException construction.
     * For complex trackers (e.g., EACH_QUORUM), this may be a sum or other
     * aggregate value rather than the actual completion criteria.
     *
     * @return number of responses required
     */
    int required();

    /**
     * How many successful responses have been received so far?
     *
     * @return number of successful responses
     */
    int received();

    /**
     * How many failures have been recorded so far?
     *
     * @return number of failures
     */
    int failures();

    /**
     * Should responses from this endpoint be counted toward quorum?
     *
     * Allows filtering of responses based on datacenter, state, or other criteria.
     * For example, LOCAL_QUORUM trackers would return false for remote DC replicas.
     *
     * @param from endpoint to check
     * @return true if responses from this endpoint count toward quorum
     */
    @VisibleForTesting
    boolean countsTowardQuorum(InetAddressAndPort from);

    /**
     * Indicates that the given address is a pending replica. Accepting writes but not reads
     */
    boolean isPending(InetAddressAndPort from);

    int totalContacts();

    int pendingContacts();

    /**
     * creates a copy of the tracker will all response counts reset
     */
    ResponseTracker resetCopy();
}
