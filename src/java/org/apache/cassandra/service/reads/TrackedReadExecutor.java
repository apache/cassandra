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

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.reads.tracked.TrackedRead;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;

public class TrackedReadExecutor implements ReadExecutor
{
    private final TrackedRead.Partition read;
    private final Dispatcher.RequestTime requestTime;
    private PartitionIterator result;

    public TrackedReadExecutor(ClusterMetadata metadata,
                               SinglePartitionReadCommand command,
                               ConsistencyLevel consistencyLevel,
                               Dispatcher.RequestTime requestTime)
    {
        this.read = TrackedRead.Partition.create(metadata, command, consistencyLevel, requestTime);
        this.requestTime = requestTime;
    }

    @Override
    public ReplicaPlan.ForTokenRead replicaPlan()
    {
        return (ReplicaPlan.ForTokenRead) read.replicaPlan();
    }

    @Override
    public void executeAsync()
    {
        read.start(requestTime);
    }

    @Override
    public void maybeTryAdditionalReplicas() {}

    @Override
    public void awaitResponses(boolean logBlockingReadRepairAttempts)
    {
        result = read.awaitResults();
    }

    @Override
    public void maybeSendAdditionalDataRequests() {}

    @Override
    public void awaitReadRepair() {}

    @Override
    public PartitionIterator getResult()
    {
        return result;
    }

    @Override
    public ReadRepair<?, ?> getReadRepair()
    {
        return NoopReadRepair.instance;
    }
}
