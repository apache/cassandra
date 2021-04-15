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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

public class NonGroupingRangeCommandIterator extends RangeCommandIterator
{
    NonGroupingRangeCommandIterator(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                    PartitionRangeReadCommand command,
                                    int concurrencyFactor,
                                    int maxConcurrencyFactor,
                                    int totalRangeCount,
                                    long queryStartNanoTime)
    {
        super(replicaPlans, command, concurrencyFactor, maxConcurrencyFactor, totalRangeCount, queryStartNanoTime);
    }

    protected PartitionIterator sendNextRequests()
    {
        List<PartitionIterator> concurrentQueries = new ArrayList<>(concurrencyFactor);
        List<ReadRepair<?, ?>> readRepairs = new ArrayList<>(concurrencyFactor);

        try
        {
            for (int i = 0; i < concurrencyFactor() && replicaPlans.hasNext(); )
            {
                ReplicaPlan.ForRangeRead replicaPlan = replicaPlans.next();

                @SuppressWarnings("resource") // response will be closed by concatAndBlockOnRepair, or in the catch block below
                SingleRangeResponse response = query(replicaPlan, i == 0);
                concurrentQueries.add(response);
                readRepairs.add(response.getReadRepair());
                // due to RangeMerger, coordinator may fetch more ranges than required by concurrency factor.
                rangesQueried += replicaPlan.vnodeCount();
                i += replicaPlan.vnodeCount();
            }
            batchesRequested++;
        }
        catch (Throwable t)
        {
            for (PartitionIterator response : concurrentQueries)
                response.close();
            throw t;
        }

        Tracing.trace("Submitted {} concurrent range requests", concurrentQueries.size());
        // We want to count the results for the sake of updating the concurrency factor (see updateConcurrencyFactor)
        // but we don't want to enforce any particular limit at this point (this could break code than rely on
        // postReconciliationProcessing), hence the unlimited counter that uses DataLimits.NONE.
        counter = command.createUnlimitedCounter(true);
        return counter.applyTo(StorageProxy.concatAndBlockOnRepair(concurrentQueries, readRepairs));
    }

    /**
     * Queries the provided sub-range.
     *
     * @param replicaPlan the subRange to query.
     * @param isFirst in the case where multiple queries are sent in parallel, whether that's the first query on
     * that batch or not. The reason it matters is that whe paging queries, the command (more specifically the
     * {@code DataLimits}) may have "state" information and that state may only be valid for the first query (in
     * that it's the query that "continues" whatever we're previously queried).
     */
    private SingleRangeResponse query(ReplicaPlan.ForRangeRead replicaPlan, boolean isFirst)
    {
        PartitionRangeReadCommand rangeCommand = command.forSubRange(replicaPlan.range(), isFirst);

        // If enabled, request repaired data tracking info from full replicas but
        // only if there are multiple full replicas to compare results from
        boolean trackRepairData = DatabaseDescriptor.getRepairedDataTrackingForRangeReadsEnabled()
                                    && replicaPlan.contacts().filter(Replica::isFull).size() > 1;

        ReplicaPlan.SharedForRangeRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);
        ReadRepair<EndpointsForRange, ReplicaPlan.ForRangeRead> readRepair =
        ReadRepair.create(command, sharedReplicaPlan, queryStartNanoTime);
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver =
        new DataResolver<>(rangeCommand, sharedReplicaPlan, readRepair, queryStartNanoTime, trackRepairData);
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler =
        new ReadCallback<>(resolver, rangeCommand, sharedReplicaPlan, queryStartNanoTime);

        if (replicaPlan.contacts().size() == 1 && replicaPlan.contacts().get(0).isSelf())
        {
            Stage.READ.execute(new StorageProxy.LocalReadRunnable(rangeCommand, handler));
        }
        else
        {
            for (Replica replica : replicaPlan.contacts())
            {
                Tracing.trace("Enqueuing request to {}", replica);
                ReadCommand command = replica.isFull() ? rangeCommand : rangeCommand.copyAsTransientQuery(replica);
                Message<ReadCommand> message = command.createMessage(trackRepairData && replica.isFull());
                MessagingService.instance().sendWithCallback(message, replica.endpoint(), handler);
            }
        }

        return new SingleRangeResponse(resolver, handler, readRepair);
    }
}
