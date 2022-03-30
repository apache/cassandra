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

import java.util.Collections;

import com.google.common.collect.Lists;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.repair.PartitionRepairEvent.PartitionRepairEventType;
import org.apache.cassandra.service.reads.repair.ReadRepairEvent.ReadRepairEventType;

final class ReadRepairDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private ReadRepairDiagnostics()
    {
    }

    static void startRepair(AbstractReadRepair readRepair, ReplicaPlan.ForRead<?, ?> fullPlan, DigestResolver digestResolver)
    {
        if (service.isEnabled(ReadRepairEvent.class, ReadRepairEventType.START_REPAIR))
            service.publish(new ReadRepairEvent(ReadRepairEventType.START_REPAIR,
                                                readRepair,
                                                fullPlan.contacts().endpoints(),
                                                fullPlan.readCandidates().endpoints(), digestResolver));
    }

    static void speculatedRead(AbstractReadRepair readRepair, InetAddressAndPort endpoint,
                               ReplicaPlan.ForRead<?, ?> fullPlan)
    {
        if (service.isEnabled(ReadRepairEvent.class, ReadRepairEventType.SPECULATED_READ))
            service.publish(new ReadRepairEvent(ReadRepairEventType.SPECULATED_READ,
                                                readRepair, Collections.singletonList(endpoint),
                                                Lists.newArrayList(fullPlan.readCandidates().endpoints()), null));
    }

    static void sendInitialRepair(BlockingPartitionRepair partitionRepair, InetAddressAndPort destination, Mutation mutation)
    {
        if (service.isEnabled(PartitionRepairEvent.class, PartitionRepairEventType.SEND_INITIAL_REPAIRS))
            service.publish(new PartitionRepairEvent(PartitionRepairEventType.SEND_INITIAL_REPAIRS, partitionRepair,
                                                     destination, mutation));
    }

    static void speculatedWrite(BlockingPartitionRepair partitionRepair, InetAddressAndPort destination, Mutation mutation)
    {
        if (service.isEnabled(PartitionRepairEvent.class, PartitionRepairEventType.SPECULATED_WRITE))
            service.publish(new PartitionRepairEvent(PartitionRepairEventType.SPECULATED_WRITE, partitionRepair,
                                                     destination, mutation));
    }

    static void speculatedWriteOversized(BlockingPartitionRepair partitionRepair, InetAddressAndPort destination)
    {
        if (service.isEnabled(PartitionRepairEvent.class, PartitionRepairEventType.UPDATE_OVERSIZED))
            service.publish(new PartitionRepairEvent(PartitionRepairEventType.UPDATE_OVERSIZED, partitionRepair,
                                                     destination, null));
    }
}
