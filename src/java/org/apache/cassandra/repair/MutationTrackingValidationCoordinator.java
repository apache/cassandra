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
package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.MutationTrackingValidationRequest;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Coordinator side of a mutation tracking reconciled data validation. Sends
 * {@link Verb#MT_VALIDATION_REQ} to each participant, collects the {@link Verb#MT_VALIDATION_RSP}
 * merkle trees routed back by {@link RepairMessageVerbHandler}, and diffs them.
 */
public class MutationTrackingValidationCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingValidationCoordinator.class);

    /** Session-keyed registry so inbound {@link Verb#MT_VALIDATION_RSP} messages find the right coordinator. */
    private static final Map<RepairJobDesc, MutationTrackingValidationCoordinator> REGISTRY = new ConcurrentHashMap<>();

    // Bound how long we wait; a participant that never responds fails the whole validation
    // rather than hanging it forever.
    private static final long VALIDATION_TIMEOUT_SECONDS = 120;

    private final SharedContext ctx;
    private final RepairJobDesc desc;
    private final Set<InetAddressAndPort> participants;
    private final ValidationOffsets offset;
    private final AsyncPromise<Result> future = new AsyncPromise<>();

    // Trees received so far, keyed by participant.
    private final Map<InetAddressAndPort, MerkleTrees> trees = new ConcurrentHashMap<>();
    // Participants we're still waiting on.
    private final Set<InetAddressAndPort> pending = ConcurrentHashMap.newKeySet();

    MutationTrackingValidationCoordinator(SharedContext ctx, RepairJobDesc desc, Set<InetAddressAndPort> participants, ValidationOffsets offset)
    {
        this.ctx = ctx;
        this.desc = desc;
        this.participants = participants;
        this.offset = offset;
        this.pending.addAll(participants);
    }

    Future<Result> start()
    {
        // Register early so a very-fast participant can find us on the response path.
        if (REGISTRY.putIfAbsent(desc, this) != null)
        {
            future.tryFailure(new IllegalStateException("Duplicate validation coordinator for " + desc));
            return future;
        }
        future.addListener(() -> REGISTRY.remove(desc, this));

        ctx.scheduledTasks().schedule(() -> {
            if (!future.isDone())
                fail(new TimeoutException("MT validation timed out after " + VALIDATION_TIMEOUT_SECONDS + "s for " + desc));
        }, VALIDATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        MutationTrackingValidationRequest request = new MutationTrackingValidationRequest(desc, FBUtilities.nowInSeconds(), false, offset);
        for (InetAddressAndPort participant : participants)
        {
            logger.debug("Sending mutation tracking validation request to {} for {}", participant, desc);
            RepairMessage.sendMessageWithRetries(ctx,
                                                 RepairMessage.notDone(future),
                                                 request,
                                                 Verb.MT_VALIDATION_REQ,
                                                 participant);
        }
        return future;
    }

    /**
     * Invoked by {@link RepairMessageVerbHandler} on receipt of {@link Verb#MT_VALIDATION_RSP}.
     * Trees may be null if the participant reported validation failure. Synchronized with
     * {@link #complete()} and {@link #fail(Throwable)} so a concurrent timeout can't race
     * a tree being stored.
     */
    synchronized void onTreesReceived(InetAddressAndPort from, MerkleTrees trees)
    {
        if (future.isDone())
        {
            if (trees != null)
                trees.release();
            return;
        }

        if (!pending.remove(from))
        {
            // Duplicate response - RepairMessage retries can produce them.
            if (trees != null)
                trees.release();
            return;
        }

        if (trees == null)
        {
            fail(new RuntimeException("Mutation tracking validation failed on participant " + from));
            return;
        }

        this.trees.put(from, trees);
        logger.debug("Received merkle tree for validation {} from {}", desc, from);

        if (pending.isEmpty())
            complete();
    }

    private synchronized void complete()
    {
        List<InetAddressAndPort> endpoints = new ArrayList<>(trees.keySet());
        Set<Range<Token>> mismatchingRanges = new HashSet<>();
        for (int i = 0; i < endpoints.size(); i++)
        {
            for (int j = i + 1; j < endpoints.size(); j++)
            {
                MerkleTrees left = trees.get(endpoints.get(i));
                MerkleTrees right = trees.get(endpoints.get(j));
                mismatchingRanges.addAll(MerkleTrees.difference(left, right));
            }
        }
        if (future.trySuccess(new Result(mismatchingRanges)))
            releaseTrees();
    }

    public void cancel()
    {
        fail(new RuntimeException("Validation cancelled"));
    }

    private synchronized void fail(Throwable cause)
    {
        if (future.tryFailure(cause))
        {
            logger.warn("Validation coordinator failed for {}: {}", desc, cause.getMessage());
            releaseTrees();
        }
    }

    private void releaseTrees()
    {
        for (MerkleTrees t : trees.values())
        {
            if (t != null)
                t.release();
        }
        trees.clear();
    }

    /**
     * Deliver a validation response to the coordinator registered for its {@link RepairJobDesc}.
     * If no coordinator is registered (e.g. it already completed or timed out), release
     * any off-heap merkle trees to avoid leaks. Callers are responsible for sending an
     * ack for the underlying message; this method only handles tree delivery/release.
     */
    public static void deliverResponse(InetAddressAndPort from, ValidationResponse response)
    {
        MutationTrackingValidationCoordinator coord = REGISTRY.get(response.desc);
        if (coord == null)
        {
            if (response.trees != null)
                response.trees.release();
            return;
        }
        coord.onTreesReceived(from, response.trees);
    }

    static class Result
    {
        final Collection<Range<Token>> mismatchingRanges;

        Result(Collection<Range<Token>> mismatchingRanges)
        {
            this.mismatchingRanges = mismatchingRanges;
        }
    }
}
