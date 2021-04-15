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

import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * A range command iterator that executes requests by endpoints and then merges responses in token order. It's designed to
 * reduce the number of range requests when scanning the whole token ring (eg. rows per range is low) for all range
 * reads that don't use digests and also to reduce the amount of disk-access for storage-attached indexes, as they will
 * be able to read index content for all required ranges at once.
 *
 * <ul>
 *     <li> With the non-grouping range command iterator, scanning the entire ring requires "num_of_nodes * num_of_tokens * consistency"
 *     range requests (assuming no ranges are merged by {@link ReplicaPlanMerger}) to their respective replicas.
 *
 *     <li> With the endpoint grouping range command iterator, scanning the entire ring only requires at most "num_of_nodes" multi-range
 *     requests to their respective replicas. So coordinator will cache up to "num_of_nodes" responses.
 * </ul>
 */
public class EndpointGroupingRangeCommandIterator extends RangeCommandIterator
{
    EndpointGroupingRangeCommandIterator(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                         PartitionRangeReadCommand command,
                                         int concurrencyFactor,
                                         int maxConcurrencyFactor,
                                         int totalRangeCount,
                                         long queryStartNanoTime)
    {
        super(replicaPlans, command, concurrencyFactor, maxConcurrencyFactor, totalRangeCount, queryStartNanoTime);
    }

    @Override
    protected PartitionIterator sendNextRequests()
    {
        counter = command.createUnlimitedCounter(true);

        EndpointGroupingCoordinator coordinator = new EndpointGroupingCoordinator(command,
                                                                                  counter,
                                                                                  replicaPlans,
                                                                                  concurrencyFactor(),
                                                                                  queryStartNanoTime);
        PartitionIterator partitions = coordinator.execute();

        rangesQueried += coordinator.vnodeRanges();
        batchesRequested++;
        Tracing.trace("Submitted concurrent grouped range read requests to {} endpoints", coordinator.endpoints());
        return partitions;
    }
}
