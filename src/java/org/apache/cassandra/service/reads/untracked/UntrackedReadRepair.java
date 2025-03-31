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

package org.apache.cassandra.service.reads.untracked;

import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.transport.Dispatcher;

public interface UntrackedReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> extends ReadRepair<E, P>
{
    interface Factory
    {
        <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        UntrackedReadRepair<E, P> create(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime);
    }

    /**
     * Used by DataResolver to generate corrections as the partition iterator is consumed
     */
    public abstract UnfilteredPartitionIterators.MergeListener getMergeListener(P replicaPlan);


    /**
     * Repairs a partition _after_ receiving data responses. This method receives replica list, since
     * we will block repair only on the replicas that have responded.
     */
    public abstract void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan);
}
