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

import com.codahale.metrics.Meter;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;

/**
 * Only performs the collection of data responses and reconciliation of them, doesn't send repair mutations
 * to replicas. This preserves write atomicity, but doesn't provide monotonic quorum reads
 */
public class ReadOnlyReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        extends AbstractReadRepair<E, P>
{
    ReadOnlyReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
    {
        super(command, replicaPlan, queryStartNanoTime);
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(P replicaPlan)
    {
        return UnfilteredPartitionIterators.MergeListener.NOOP;
    }

    @Override
    Meter getRepairMeter()
    {
        return ReadRepairMetrics.reconcileRead;
    }

    @Override
    public void maybeSendAdditionalWrites()
    {

    }

    @Override
    public void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        throw new UnsupportedOperationException("ReadOnlyReadRepair shouldn't be trying to repair partitions");
    }

    @Override
    public void awaitWrites()
    {

    }
}
