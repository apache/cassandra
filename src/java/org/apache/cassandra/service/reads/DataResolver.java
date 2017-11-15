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
package org.apache.cassandra.service.reads;

import java.net.InetAddress;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.transform.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.TraceState;

public class DataResolver extends ResponseResolver
{
    private static final boolean DROP_OVERSIZED_READ_REPAIR_MUTATIONS =
        Boolean.getBoolean("cassandra.drop_oversized_readrepair_mutations");

    @VisibleForTesting
    final List<AsyncOneResponse> repairResults = Collections.synchronizedList(new ArrayList<>());
    private final long queryStartNanoTime;
    private final boolean enforceStrictLiveness;

    public DataResolver(Keyspace keyspace, ReadCommand command, ConsistencyLevel consistency, int maxResponseCount, long queryStartNanoTime, ReadRepair readRepair)
    {
        super(keyspace, command, consistency, readRepair, maxResponseCount);
        this.queryStartNanoTime = queryStartNanoTime;
        this.enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    public PartitionIterator getData()
    {
        ReadResponse response = responses.iterator().next().payload;
        return UnfilteredPartitionIterators.filter(response.makeIterator(command), command.nowInSec());
    }

    public boolean isDataPresent()
    {
        return !responses.isEmpty();
    }

    public PartitionIterator resolve()
    {
        // We could get more responses while this method runs, which is ok (we're happy to ignore any response not here
        // at the beginning of this method), so grab the response count once and use that through the method.
        int count = responses.size();
        List<UnfilteredPartitionIterator> iters = new ArrayList<>(count);
        InetAddressAndPort[] sources = new InetAddressAndPort[count];
        for (int i = 0; i < count; i++)
        {
            MessageIn<ReadResponse> msg = responses.get(i);
            iters.add(msg.payload.makeIterator(command));
            sources[i] = msg.from;
        }

        /*
         * Even though every response, individually, will honor the limit, it is possible that we will, after the merge,
         * have more rows than the client requested. To make sure that we still conform to the original limit,
         * we apply a top-level post-reconciliation counter to the merged partition iterator.
         *
         * Short read protection logic (ShortReadRowsProtection.moreContents()) relies on this counter to be applied
         * to the current partition to work. For this reason we have to apply the counter transformation before
         * empty partition discard logic kicks in - for it will eagerly consume the iterator.
         *
         * That's why the order here is: 1) merge; 2) filter rows; 3) count; 4) discard empty partitions
         *
         * See CASSANDRA-13747 for more details.
         */

        DataLimits.Counter mergedResultCounter =
        command.limits().newCounter(command.nowInSec(), true, command.selectsFullPartition(), enforceStrictLiveness);

        UnfilteredPartitionIterator merged = mergeWithShortReadProtection(iters, sources, mergedResultCounter);
        FilteredPartitions filtered = FilteredPartitions.filter(merged, new Filter(command.nowInSec(), command.metadata().enforceStrictLiveness()));
        PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
        return Transformation.apply(counted, new EmptyPartitionsDiscarder());
    }

    private UnfilteredPartitionIterator mergeWithShortReadProtection(List<UnfilteredPartitionIterator> results,
                                                                     InetAddressAndPort[] sources,
                                                                     DataLimits.Counter mergedResultCounter)
    {
        // If we have only one results, there is no read repair to do and we can't get short reads
        if (results.size() == 1)
            return results.get(0);

        /*
         * So-called short reads stems from nodes returning only a subset of the results they have due to the limit,
         * but that subset not being enough post-reconciliation. So if we don't have a limit, don't bother.
         */
        if (!command.limits().isUnlimited())
            for (int i = 0; i < results.size(); i++)
            {
                results.set(i, ShortReadProtection.extend(sources[i], results.get(i), command, mergedResultCounter, queryStartNanoTime, enforceStrictLiveness));
            }

        return UnfilteredPartitionIterators.merge(results, command.nowInSec(), readRepair.getMergeListener(sources));
    }

    public void evaluateAllResponses()
    {
        // We need to fully consume the results to trigger read repairs if appropriate
        try (PartitionIterator iterator = resolve())
        {
            PartitionIterators.consume(iterator);
        }
    }

    public void evaluateAllResponses(TraceState traceState)
    {
        evaluateAllResponses();
    }
}
