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

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class MutationTrackingSyncCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingSyncCoordinator.class);

    private static final long EMPTY_TARGETS_TIMEOUT_MS = 3000;
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final String keyspace;
    private final Range<Token> range;
    private final AsyncPromise<Void> completionFuture = new AsyncPromise<>();
    private volatile long startTimeMs;

    // Per-shard state: tracks what each node has reported for that shard
    private final Map<Range<Token>, ShardSyncState> shardStates = new HashMap<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final Set<InetAddressAndPort> allParticipants = new HashSet<>();
    private final Set<InetAddressAndPort> reportedParticipants = ConcurrentHashMap.newKeySet();

    public MutationTrackingSyncCoordinator(String keyspace, Range<Token> range)
    {
        this.keyspace = keyspace;
        this.range = range;
    }

    public void start()
    {
        if (!started.compareAndSet(false, true))
            throw new IllegalStateException("Sync coordinator already started");

        startTimeMs = System.currentTimeMillis();

        List<Shard> overlappingShards;

        overlappingShards = new ArrayList<>();
        MutationTrackingService.instance.forEachShardInKeyspace(keyspace, shard -> {
            if (shard.range.intersects(range))
                overlappingShards.add(shard);
        });

        if (overlappingShards.isEmpty())
        {
            completionFuture.setSuccess(null);
            return;
        }

        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        for (Shard shard : overlappingShards)
        {
            allParticipants.addAll(shard.remoteReplicas());
            allParticipants.add(localAddress);
        }

        // Initialize state for each shard
        for (Shard shard : overlappingShards)
        {
            ShardSyncState state = new ShardSyncState(shard);
            shardStates.put(shard.range, state);
        }

        // Register to receive offset updates
        MutationTrackingService.instance.registerSyncCoordinator(this);

        // Mark self as reported and capture local targets
        reportedParticipants.add(localAddress);
        recaptureTargets();

        logger.info("Sync coordinator started for keyspace {} range {}, tracking {} shards, waiting for {} participants",
                   keyspace, range, overlappingShards.size(), allParticipants.size());

        // Check if we're the only participant and already complete
        checkIfReadyToComplete();

        // Schedule a delayed check for the empty targets timeout case
        scheduler.schedule(this::checkIfReadyToComplete, EMPTY_TARGETS_TIMEOUT_MS + 100, TimeUnit.MILLISECONDS);
    }

    private void complete()
    {
        if (!completed.compareAndSet(false, true))
            return;
        MutationTrackingService.instance.unregisterSyncCoordinator(this);
        completionFuture.setSuccess(null);
    }

    private boolean checkIfComplete()
    {
        for (ShardSyncState state : shardStates.values())
        {
            if (!state.isComplete())
                return false;
        }
        return true;
    }

    private void recaptureTargets()
    {
        for (ShardSyncState state : shardStates.values())
        {
            state.captureTargets();
        }
    }

    /**
     * Check if we're ready to complete. We can complete when:
     * 1. All participants have reported their offsets AND all targets are reconciled, OR
     * 2. No targets have been discovered after the timeout (no data to sync anywhere)
     */
    private void checkIfReadyToComplete()
    {
        if (completed.get())
            return;

        if (hasNoTargets() && (System.currentTimeMillis() - startTimeMs) > EMPTY_TARGETS_TIMEOUT_MS)
        {
            logger.info("Sync coordinator completed for keyspace {} range {} - no targets discovered after {}ms",
                        keyspace, range, EMPTY_TARGETS_TIMEOUT_MS);
            complete();
            return;
        }

        // Wait until all participants have reported
        if (!reportedParticipants.containsAll(allParticipants))
        {
            logger.trace("Sync coordinator waiting for participants. Reported: {}, All: {}",
                         reportedParticipants.size(), allParticipants.size());
            return;
        }

        // All participants have reported, check if targets are reconciled
        if (checkIfComplete())
        {
            logger.info("Sync coordinator completed for keyspace {} range {}", keyspace, range);
            complete();
        }
    }

    private boolean hasNoTargets()
    {
        for (ShardSyncState state : shardStates.values())
        {
            if (!state.targets.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Called when offset updates are received from a participant.
     * @param from The participant that sent the offsets
     */
    public void onOffsetsReceived(InetAddressAndPort from)
    {
        if (completed.get())
            return;

        boolean newParticipant = reportedParticipants.add(from);

        if (newParticipant)
        {
            logger.trace("Sync coordinator received offsets from new participant {}. Reported: {}/{}",
                         from, reportedParticipants.size(), allParticipants.size());
        }

        recaptureTargets(); // Recapture targets to include any new coordinator logs

        checkIfReadyToComplete();
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public Range<Token> getRange()
    {
        return range;
    }

    /**
     * Blocks until sync completes or timeout is reached.
     *
     * @param timeout Maximum time to wait
     * @param unit Time unit
     * @return true if completed, false if timed out
     */
    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException
    {
        try
        {
            completionFuture.get(timeout, unit);
            return true;
        }
        catch (java.util.concurrent.TimeoutException e)
        {
            return false;
        }
        catch (java.util.concurrent.ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public void cancel()
    {
        if (completed.compareAndSet(false, true))
        {
            MutationTrackingService.instance.unregisterSyncCoordinator(this);
            completionFuture.setFailure(new RuntimeException("Sync cancelled"));
        }
    }

    /**
     * Tracks sync state for a single shard.
     */
    private static class ShardSyncState
    {
        private final Shard shard;

        // Target offsets: LogId -> the offsets we're waiting for all nodes to have
        private final Map<CoordinatorLogId, Offsets.Immutable> targets = new ConcurrentHashMap<>();

        ShardSyncState(Shard shard)
        {
            this.shard = shard;
        }

        void captureTargets()
        {
            Map<CoordinatorLogId, Offsets.Immutable> unionOffsets = shard.collectUnionOfWitnessedOffsetsPerLog();
            targets.putAll(unionOffsets);
        }

        boolean isComplete()
        {
            Map<CoordinatorLogId, Offsets.Immutable> currentReconciled = shard.collectReconciledOffsetsPerLog();

            for (Map.Entry<CoordinatorLogId, Offsets.Immutable> entry : targets.entrySet())
            {
                CoordinatorLogId logId = entry.getKey();
                Offsets.Immutable target = entry.getValue();

                Offsets.Immutable reconciled = currentReconciled.get(logId);
                if (reconciled == null)
                    return false;

                // Check if reconciled contains all offsets in target
                if (!containsAll(reconciled, target))
                    return false;
            }
            return true;
        }

        private boolean containsAll(Offsets reconciled, Offsets target)
        {
            for (ShortMutationId id : target)
            {
                if (!reconciled.contains(id.offset()))
                    return false;
            }
            return true;
        }
    }
}
