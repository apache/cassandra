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

package org.apache.cassandra.net;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;


/**
 * We need to preserve the state on a per-node basis of a request to print out to logs for troubleshooting failures. This
 * is a composable ride-along to use in conjunction with the {@link RequestCallback} interface that wraps up a lot of the
 * shared logic around success and failure tracking, per-DC success or failure, and core information to support logging
 * details for each replica in timeout cases.
 *
 * Usage:
 * - {@link RequestCallback#onResponse} -> call {@link #recordResponse}
 * - {@link RequestCallback#onFailure} -> call {@link #recordFailure}
 * - immediately after "get" condition (await, awaitUntil, etc.), call {@link #endProcessing()} to grab a copy of the
 *   known state at time of signal for use in {@link #isTimeout} and {@link #buildParticipantString} calls.
 */
public class CallbackResponseTracker
{
    private static final Logger logger = LoggerFactory.getLogger(CallbackResponseTracker.class);

    private final Locator locator = DatabaseDescriptor.getLocator();

    private final Collection<InetAddressAndPort> possibleEndpoints;
    private final Set<InetAddressAndPort> successfulEndpoints;
    private final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;

    /**
     * Map of DC to required responses per DC to consider it a success. This should only be initialized if the
     * response handler needs the information contained therein (CL.EACH_QUORUM, CL.LOCAL_QUORUM)
     */
    private Map<String, AtomicInteger> pendingResponsesPerDC;

    /**
     * This allows us to track the ideal CL on a per-DC basis in a relatively lightweight way
     */
    private Map<String, AtomicInteger> idealCLTracker;

    /**
     * Users are expected to call {@link #endProcessing()} when they're done and want to finalize the state of
     * callbacks after signal is received; we enforce this on methods that access this Collection.
     */
    private Map<InetAddressAndPort, RequestFailureReason> finalizedFailures;

    /** The count of responses required to consider the tracked callback a success */
    public final int requiredResponses;

    public CallbackResponseTracker(Collection<InetAddressAndPort> endpoints, int requiredResponses)
    {
        this.requiredResponses = requiredResponses;
        possibleEndpoints = endpoints;
        // We can reasonably expect contention to be _very low_ on these data structures since our max # of participants
        // is <= endpoints. So no need for the heavyweight striped concurrent classes, instead do simple synchronized.
        // Given the "spin-loop" before parking threads (we have biased locking disabled...), synchronized should be more than
        // sufficient.
        successfulEndpoints = Collections.synchronizedSet(new HashSet<>(endpoints.size()));
        failureReasonByEndpoint = Collections.synchronizedMap(new HashMap<>(endpoints.size()));
    }

    /**
     * Given the async env in which this operates, we need to provide an easy way to take a copy of failures for processing
     * after callbacks have triggered an end state. Subsequent callback responses will update collections here and
     * subsequent calls to this method _will_ update the failure snapshot, but it's not an expected usage pattern so
     * we'll log a warning at least.
     *
     * @return The copy of the map of failures
     */
    public Map<InetAddressAndPort, RequestFailureReason> endProcessing()
    {
        if (finalizedFailures != null)
        {
            NoSpamLogger.log(logger,
                             NoSpamLogger.Level.WARN,
                             1,
                             TimeUnit.SECONDS,
                             "Saw duplicate call to CallbackResponseTracker.endProcessing; updating failure collection, but this is likely indicative of a bug.");
        }
        finalizedFailures = ImmutableMap.copyOf(failureReasonByEndpoint);
        return finalizedFailures;
    }

    public Map<InetAddressAndPort, RequestFailureReason> getFinalizedFailures()
    {
        Preconditions.checkNotNull(finalizedFailures, "Attempted to get failures before finalizing.");
        return finalizedFailures;
    }

    public boolean isSuccessful()
    {
        logger.error("isSuccessful check. responseCount: {}, required: {}", responseCount(), requiredResponses);
        return responseCount() >= requiredResponses;
    }

    public int responseCount()
    {
        return successfulEndpoints.size();
    }

    public int participantCount()
    {
        return possibleEndpoints.size();
    }

    /**
     * If the amount we need + the failures is more than the possible, we're done. This is expected to be called
     * while callbacks are in flight so we use the live map.
     */
    public boolean cannotComplete()
    {
        return requiredResponses + failureReasonByEndpoint.size() > possibleEndpoints.size();
    }

    /**
     * Note: this is idempotent based on the source address, though it will warn.
     */
    public void recordResponse(InetAddressAndPort from)
    {
        if (successfulEndpoints.add(from))
        {
            if (pendingResponsesPerDC != null)
                pendingResponsesPerDC.get(locator.location(from).datacenter).decrementAndGet();
            if (idealCLTracker != null)
                idealCLTracker.get(locator.location(from).datacenter).decrementAndGet();
        }
        else
        {
            logger.warn("Saw duplicate callback success response from endpoint: {}. Not adding to query cl success counter.", from);
        }
    }

    /**
     * We don't have any known cases where we expect to see duplicate failure calls from the same root endpoint, so we
     * warn here. All endpoints recorded here will count towards the tracker's view of CL; if a callback handler
     * needs to treat different responses differently (i.e. from a DC that we don't care about), that count should
     * be tracked separately in the callback.
     */
    public void recordFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        RequestFailureReason priorReason = failureReasonByEndpoint.put(from, failureReason);
        if (priorReason != null)
            logger.warn("Saw duplicate callback failure from endpoint: {}. Replacing {} with {}", from, priorReason, failureReason);
    }

    public boolean isTimeout()
    {
        Preconditions.checkNotNull(finalizedFailures, "accessed before failures finalized");
        // Since the isTimeout check considers empty collections as timeouts (since we may have unknown / not seen responses
        // if the timeout for the specific response hits after the wait on the callback signal), we need to also always
        // confirm the response count vs. required as well.
        //
        // NOTE: This is a mix of "live" data in the isSuccessful check and frozen immutable data in finalizedFailures.
        // Since we _want_ to keep processing callbacks even after we've finalized, our options here are to create yet
        // another copy of our successful callback responses as the time of finalization or just allow this blending.
        // Our risk here is a race where we don't throw an exception on a callback from racing with a success that came
        // through and tipped us past CL, however at that point we actually know more information about the attempted
        // operation (i.e. succeeded between the time we finalized failures and then checked this here) so it's arguably
        // a touch more correct to do things this way.
        return !isSuccessful() && RequestCallback.isTimeout(finalizedFailures);
    }

    public boolean processingIdealCL()
    {
        return idealCLTracker != null;
    }

    /**
     * If the original creation of this tracker didn't include multi-DC information, this will create and populate the
     * per-DC callback collection for record processing as well.
     */
    public void enableIdealCLTracking(ReplicaPlan.ForWrite replicaPlan)
    {
        idealCLTracker = buildDCMap(replicaPlan);
    }

    public boolean hitIdealConsistencyLevel()
    {
        return satisfiedCL(idealCLTracker);
    }

    public void enableDCTracking(ReplicaPlan.ForWrite replicaPlan)
    {
        pendingResponsesPerDC = buildDCMap(replicaPlan);
    }

    /**
     * @return true if we're tracking DC's and hit them, false for any other combination of situations
     */
    public boolean hitDCConsistencyLevel()
    {
        if (pendingResponsesPerDC != null)
            return satisfiedCL(pendingResponsesPerDC);
        return false;
    }

    /**
     * Relative to the rest of what we do in here this is somewhat heavyweight; iterating over all DC's and checking the
     * value of an atomic int for each one. In the grand scheme of things, since this is _post_ signaling when we've hit
     * the condition and this should happen in the messaging thread context, the cost here shouldn't be hot path facing.
     *
     * Also, # DC's is going to be low so that gives us an upper bound.
     */
    private boolean satisfiedCL(Map<String, AtomicInteger> context)
    {
        for (Map.Entry<String, AtomicInteger> pair : context.entrySet())
        {
            logger.error(" - satisfiedCL; DC: {}, count: {}", pair.getKey(), pair.getValue().get());
            if (pair.getValue().get() > 0)
            {
                logger.error(" -- FALSE");
                return false;
            }
        }
        logger.error(" -- TRUE");
        return true;
    }

    public boolean gotAllResponses()
    {
        return failureReasonByEndpoint.size() + successfulEndpoints.size() == possibleEndpoints.size();
    }

    /**
     * For a given {@link ReplicaPlan}, construct a set of per-DC counts for the # of responses we need to receive
     * to hit the CL of the plan, processed by {@link #recordResponse}
     */
    private Map<String, AtomicInteger> buildDCMap(ReplicaPlan.ForWrite replicaPlan)
    {
        Map<String, AtomicInteger> result = new HashMap<>();
        if (replicaPlan.replicationStrategy() instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicaPlan.replicationStrategy();
            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc).allReplicas;
                result.put(dc, new AtomicInteger((rf / 2) + 1));
            }
        }
        else
        {
            result.put(locator.local().datacenter, new AtomicInteger(ConsistencyLevel.quorumFor(replicaPlan.replicationStrategy())));
        }

        // During bootstrap, we have to include the pending endpoints, or we may fail the consistency level
        // guarantees (see #833)
        for (Replica pending : replicaPlan.pending())
            result.get(locator.location(pending.endpoint()).datacenter).incrementAndGet();

        return result;
    }

    @Override
    public String toString()
    {
        return String.format("successful: %s. successCount: %d. failCount: %d. participants: %d. finalized: %s",
                             isSuccessful(),
                             successfulEndpoints.size(),
                             failureReasonByEndpoint.size(),
                             participantCount(),
                             finalizedFailures != null);
    }

    /**
     * For use only in failure scenarios. Because it's Slow.
     */
    public String buildParticipantString(String msg)
    {
        Preconditions.checkNotNull(finalizedFailures, "accessed before failures finalized");

        StringBuilder sb = new StringBuilder();
        sb.append("Operation Failed - ");
        sb.append(msg);

        for (InetAddressAndPort ep : successfulEndpoints)
            sb.append(' ').append(ep).append(": SUCCESS ");

        possibleEndpoints.stream()
                         .filter(ep -> !successfulEndpoints.contains(ep))
                         .filter(ep -> !failureReasonByEndpoint.containsKey(ep))
                         .filter(ep -> !ep.equals(FBUtilities.getBroadcastAddressAndPort()))
                         .map(ep -> String.format(" %s: UNKNOWN ", ep))
                         .forEach(sb::append);

        failureReasonByEndpoint.entrySet().stream()
                       .map(e -> String.format(" %s: %s ", e.getKey(), e.getValue()))
                       .forEach(sb::append);

        return sb.toString();
    }
}