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

package org.apache.cassandra.service.reads.tracked;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.NORMAL;

/**
 * Since the read reconciliations don't use 2 way callbacks, a map of active reconciliations
 * are maintained and expired here.
 *
 * Borrowed heavily from RequestCallbacks
 */
public class TrackedLocalReads implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedLocalReads.class);

    private final ConcurrentMap<TrackedRead.Id, TrackedLocalReadCoordinator> reads = new ConcurrentHashMap<>();
    private final ScheduledExecutorPlus executor = executorFactory().scheduled("Reconciliation-Map-Reaper", NORMAL);

    public TrackedLocalReads()
    {
        long expirationInterval = defaultExpirationInterval();
        executor.scheduleWithFixedDelay(this::expire, expirationInterval, expirationInterval, NANOSECONDS);
    }

    private void expire()
    {
        long start = Clock.Global.nanoTime();
        int n = 0;
        for (Map.Entry<TrackedRead.Id, TrackedLocalReadCoordinator> entry : reads.entrySet())
        {
            TrackedLocalReadCoordinator read = entry.getValue();
            if (read.isTimedOutOrComplete(start))
            {
                read.abort();
                if (reads.remove(entry.getKey(), entry.getValue()))
                    n++;
            }
        }
        if (n > 0)
            logger.trace("Expired {} entries", n);
    }

    private TrackedLocalReadCoordinator getOrCreate(TrackedRead.Id id)
    {
        return reads.computeIfAbsent(id, TrackedLocalReadCoordinator::new);
    }

    public void receiveSummary(InetAddressAndPort from, TrackedSummaryResponse summary)
    {
        getOrCreate(summary.readId()).receiveSummary(from, summary.summary());
    }

    public TrackedLocalReadCoordinator beginRead(TrackedRead.Id readId, ClusterMetadata metadata, ReadCommand command, ConsistencyLevel consistencyLevel, int[] summaryNodes, long expiresAtNanos)
    {
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;
        ReplicaPlan.AbstractForRead<?, ?> replicaPlan;
        if (command instanceof SinglePartitionReadCommand)
        {
            replicaPlan = ReplicaPlans.forRead(metadata,
                                               keyspace,
                                               ((SinglePartitionReadCommand) command).partitionKey().getToken(),
                                               command.indexQueryPlan(),
                                               consistencyLevel,
                                               retry);
        }
        else
        {
            // TODO: confirm range we're reading doesn't span multiple replica sets
            replicaPlan = ReplicaPlans.forRangeRead(keyspace,
                                                    command.indexQueryPlan(),
                                                    consistencyLevel,
                                                    command.dataRange().keyRange(),
                                                    1);
        }
        // TODO: confirm all summaryNodes are present in the replica plan

        TrackedLocalReadCoordinator coordinator = getOrCreate(readId);
        coordinator.startLocalRead(readId, command, replicaPlan, summaryNodes, expiresAtNanos);
        return coordinator;
    }

    public void acknowledgeSync(TrackedRead.Id readId, int syncId)
    {
        TrackedLocalReadCoordinator read = reads.get(readId);
        if (read == null)
            return;

        if (read.acknowledgeSync(syncId))
            reads.remove(readId);
    }

    public boolean receiveMutations(TrackedRead.Id readId, int syncId, List<Mutation> mutations)
    {
        TrackedLocalReadCoordinator read = reads.get(readId);
        if (read == null)
            return false;

        read.receiveMutations(mutations);
        if (read.acknowledgeSync(syncId))
            reads.remove(readId);
        return true;
    }

    public static long defaultExpirationInterval()
    {
        return DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2;
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    public void shutdownBlocking() throws InterruptedException
    {
        if (executor == null || executor.isTerminated())
            return;

        executor.shutdown();
        executor.awaitTermination(1, MINUTES);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }
}
