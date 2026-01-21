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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

public class MutationTrackingSyncCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingSyncCoordinator.class);

    private final String keyspace;
    private final Range<Token> range;
    private final AsyncPromise<Void> completionFuture = new AsyncPromise<>();

    // Per-shard state: tracks what each node has reported for that shard
    private final Map<Range<Token>, ShardSyncState> shardStates = new ConcurrentHashMap<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public MutationTrackingSyncCoordinator(String keyspace, Range<Token> range)
    {
        this.keyspace = keyspace;
        this.range = range;
    }

    public void start()
    {
        if (!started.compareAndSet(false, true))
            throw new IllegalStateException("Sync coordinator already started");

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

        // Register to receive offset updates
        MutationTrackingService.instance.registerSyncCoordinator(this);

        // Initialize state for each shard and capture targets
        for (Shard shard : overlappingShards)
        {
            ShardSyncState state = new ShardSyncState(shard);
            state.captureTargets();
            shardStates.put(shard.range, state);
        }

        if (checkIfComplete())
        {
            complete();
            return;
        }

        logger.info("Sync coordinator started for keyspace {} range {}, tracking {} shards",
                   keyspace, range, overlappingShards.size());
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

    public void onOffsetsReceived()
    {
        if (completed.get())
            return;

        // The underlying CoordinatorLog already updates its reconciled offsets.
        // We just need to re-check if we're now complete.
        if (checkIfComplete())
        {
            complete();
        }
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
