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

package org.apache.cassandra.service.reads.repair;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.tracing.Tracing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction. Breaks write
 *  atomicity in some situations
 */
public class BlockingReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        extends AbstractReadRepair<E, P>
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    protected final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    BlockingReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        super(command, replicaPlan, queryStartNanoTime);
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(P replicaPlan)
    {
        return new PartitionIteratorMergeListener<>(replicaPlan, command, this);
    }

    @Override
    Meter getRepairMeter()
    {
        return ReadRepairMetrics.repairedBlocking;
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        for (BlockingPartitionRepair repair: repairs)
        {
            repair.maybeSendAdditionalWrites(cfs.additionalWriteLatencyMicros, MICROSECONDS);
        }
    }

    @Override
    public void awaitWrites()
    {
        BlockingPartitionRepair timedOut = null;
        for (BlockingPartitionRepair repair : repairs)
        {
            if (!repair.awaitRepairsUntil(DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS) + queryStartNanoTime, NANOSECONDS))
            {
                timedOut = repair;
                break;
            }
        }
        if (timedOut != null)
        {
            // We got all responses, but timed out while repairing;
            // pick one of the repairs to throw, as this is better than completely manufacturing the error message
            int blockFor = timedOut.blockFor();
            int received = Math.min(blockFor - timedOut.waitingOn(), blockFor - 1);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(replicaPlan().consistencyLevel(), received, blockFor, true);
        }
    }

    @Override
    public void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(partitionKey, mutations, writePlan);
        blockingRepair.sendInitialRepairs();
        repairs.add(blockingRepair);
    }
}
