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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.tracing.Tracing;

/**
 * 'Classic' read repair. Doesn't allow the client read to return until
 *  updates have been written to nodes needing correction. Breaks write
 *  atomicity in some situations
 */
public class BlockingReadRepair extends AbstractReadRepair
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepair.class);

    protected final Queue<BlockingPartitionRepair> repairs = new ConcurrentLinkedQueue<>();

    public BlockingReadRepair(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        super(command, queryStartNanoTime, consistency);
    }

    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, consistency, this);
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
            repair.maybeSendAdditionalWrites(cfs.sampleLatencyNanos, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public void awaitWrites()
    {
        boolean timedOut = false;
        for (BlockingPartitionRepair repair: repairs)
        {
            if (!repair.awaitRepairs(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
            {
                timedOut = true;
            }
        }
        if (timedOut)
        {
            // We got all responses, but timed out while repairing
            int blockFor = consistency.blockFor(cfs.keyspace);
            if (Tracing.isTracing())
                Tracing.trace("Timed out while read-repairing after receiving all {} data and digest responses", blockFor);
            else
                logger.debug("Timeout while read-repairing after receiving all {} data and digest responses", blockFor);

            throw new ReadTimeoutException(consistency, blockFor-1, blockFor, true);
        }
    }

    @Override
    public void repairPartition(DecoratedKey key, Map<InetAddressAndPort, Mutation> mutations, InetAddressAndPort[] destinations)
    {
        BlockingPartitionRepair blockingRepair = new BlockingPartitionRepair(cfs.keyspace, key, consistency, mutations, consistency.blockFor(cfs.keyspace), destinations);
        blockingRepair.sendInitialRepairs();
        repairs.add(blockingRepair);
    }
}
