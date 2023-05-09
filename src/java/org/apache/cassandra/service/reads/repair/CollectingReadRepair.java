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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.service.reads.CassandraFollowupReader;

/**
 * Read repair that collects repair mutations and makes them available, but doesn't apply them
 */
public class CollectingReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends AbstractReadRepair<E, P>
{
    private static final Logger logger = LoggerFactory.getLogger(CollectingReadRepair.class);

    public final List<Mutation> repairs;

    public CollectingReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime, List<Mutation> repairs, CassandraFollowupReader followupReader)
    {
        super(command, replicaPlan, queryStartNanoTime, followupReader);
        this.repairs = repairs;
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(P replicaPlan)
    {
        return new PartitionIteratorMergeListener<>(replicaPlan, command, this);
    }

    @Override
    Meter getRepairMeter()
    {
        // TODO for now call this blocking?
        return ReadRepairMetrics.repairedBlocking;
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        throw new UnsupportedOperationException("Should never attempt additional writes");
    }

    @Override
    public void awaitWrites()
    {
        throw new UnsupportedOperationException("Should never attempt to wait on writes");
    }

    @Override
    public void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        // TODO Should the merge iterator produce a single mutation?
        repairs.addAll(mutations.values());
    }
}
