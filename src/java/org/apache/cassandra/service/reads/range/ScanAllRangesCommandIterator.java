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

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * A custom {@link RangeCommandIterator} that queries all replicas required by consistency level at once with data range
 * specify in {@link PartitionRangeReadCommand}.
 * <p>
 * This is to speed up {@link Index.QueryPlan#isTopK()} queries that needs to find global top-k rows in the cluster, because
 * existing {@link RangeCommandIterator} has to execute a top-k search per vnode range which is wasting resources.
 * <p>
 * The implementation combines the replica plans for each data range into a single shared replica plan. This results in
 * queries using reconciliation where it may not be expected. This is handled in the {@link DataResolver} for top-K queries
 * so any usage for queries other that top-K should bear this in mind.
 * <p>
 * It is important to note that this implementation can only be used with {@link ConsistencyLevel#ONE} and {@link ConsistencyLevel#LOCAL_ONE}
 */
public class ScanAllRangesCommandIterator extends RangeCommandIterator
{
    private final Keyspace keyspace;

    ScanAllRangesCommandIterator(Keyspace keyspace, CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                 PartitionRangeReadCommand command,
                                 int totalRangeCount,
                                 long queryStartNanoTime)
    {
        super(replicaPlans, command, totalRangeCount, totalRangeCount, totalRangeCount, queryStartNanoTime);
        Preconditions.checkState(command.isTopK());

        this.keyspace = keyspace;
    }

    @Override
    protected PartitionIterator sendNextRequests()
    {
        // get all replicas to contact
        Set<InetAddressAndPort> replicasToQuery = null;
        ConsistencyLevel consistencyLevel = null;
        while (replicaPlans.hasNext())
        {
            if (replicasToQuery == null)
                replicasToQuery = new HashSet<>();

            ReplicaPlan.ForRangeRead replicaPlan = replicaPlans.next();
            replicasToQuery.addAll(replicaPlan.contacts().endpoints());
            consistencyLevel = replicaPlan.consistencyLevel();
        }

        if (replicasToQuery == null || replicasToQuery.isEmpty())
            return EmptyIterators.partition();

        ReplicaPlan.ForRangeRead plan = ReplicaPlans.forFullRangeRead(keyspace, consistencyLevel, command.dataRange().keyRange(), replicasToQuery, totalRangeCount);
        ReplicaPlan.SharedForRangeRead sharedReplicaPlan = ReplicaPlan.shared(plan);
        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver = new DataResolver<>(command, sharedReplicaPlan, NoopReadRepair.instance, queryStartNanoTime, false);
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler = new ReadCallback<>(resolver, command, sharedReplicaPlan, queryStartNanoTime);

        int nodes = 0;
        for (InetAddressAndPort endpoint : replicasToQuery)
        {
            Tracing.trace("Enqueuing request to {}", endpoint);
            Message<ReadCommand> message = command.createMessage(false);
            MessagingService.instance().sendWithCallback(message, endpoint, handler);
            nodes++;
        }

        rangesQueried += plan.vnodeCount();
        batchesRequested++;

        Tracing.trace("Submitted scanning all ranges requests to {} nodes", nodes);

        // skip read-repair for top-k query because data mismatch may be caused by top-k algorithm instead of actual inconsistency.
        return new SingleRangeResponse(resolver, handler, NoopReadRepair.instance);
    }
}
