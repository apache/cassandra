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

package org.apache.cassandra.service.consensus.migration;

import org.apache.cassandra.tcm.Epoch;

import static com.google.common.base.Preconditions.checkArgument;

public class ConsensusMigrationRepairResult
{
    public final ConsensusMigrationRepairType type;
    public final Epoch minEpoch;

    private ConsensusMigrationRepairResult(ConsensusMigrationRepairType type, Epoch minEpoch)
    {
        this.type = type;
        this.minEpoch = minEpoch;
    }

    public static ConsensusMigrationRepairResult fromCassandraRepair(Epoch minEpoch, boolean migrationEligibleRepair)
    {
        checkArgument(!migrationEligibleRepair || minEpoch.isAfter(Epoch.EMPTY), "Epoch should not be empty if Paxos and regular repairs were performed");
        if (migrationEligibleRepair)
            return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.paxos, minEpoch);
        else
            return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.ineligible, Epoch.EMPTY);
    }

    public static ConsensusMigrationRepairResult fromAccordRepair(Epoch minEpoch)
    {
        checkArgument(minEpoch.isAfter(Epoch.EMPTY), "Accord repairs should always occur at an Epoch");
        return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.accord, minEpoch);
    }
}
