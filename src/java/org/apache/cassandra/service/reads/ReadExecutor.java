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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.replication.migration.MigrationRouter;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

public interface ReadExecutor
{
    static List<ReadExecutor> createExecutors(ClusterMetadata metadata,
                                              List<SinglePartitionReadCommand> commands,
                                              ConsistencyLevel consistencyLevel,
                                              ReadCoordinator coordinator,
                                              Dispatcher.RequestTime requestTime)
    {
        List<ReadExecutor> executors = new ArrayList<>(commands.size());

        for (SinglePartitionReadCommand command : commands)
        {
            if (MigrationRouter.shouldUseTracked(command))
            {
                executors.add(new TrackedReadExecutor(metadata, command, consistencyLevel, requestTime));
            }
            else
            {
                executors.add(AbstractReadExecutor.getReadExecutor(metadata, command, consistencyLevel, coordinator, requestTime));
            }
        }

        return executors;
    }

    ReplicaPlan.ForTokenRead replicaPlan();

    default boolean hasLocalRead()
    {
        return replicaPlan().lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
    }

    void executeAsync();

    void maybeTryAdditionalReplicas();

    void awaitResponses(boolean logBlockingReadRepairAttempts);

    void maybeSendAdditionalDataRequests();

    void awaitReadRepair();

    PartitionIterator getResult();

    ReadRepair<?, ?> getReadRepair();
}
