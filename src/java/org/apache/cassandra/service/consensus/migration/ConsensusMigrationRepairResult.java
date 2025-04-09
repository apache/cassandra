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

import javax.annotation.Nullable;

import accord.primitives.Ranges;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.repair.AccordRepair.AccordRepairResult;
import org.apache.cassandra.tcm.Epoch;

import static com.google.common.base.Preconditions.checkArgument;

public class ConsensusMigrationRepairResult
{
    private static final ConsensusMigrationRepairResult INELIGIBLE = new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.INELIGIBLE, Epoch.EMPTY, null);
    public final ConsensusMigrationRepairType type;
    public final Epoch minEpoch;
    @Nullable
    public final Ranges accordRepairedRanges;

    private ConsensusMigrationRepairResult(ConsensusMigrationRepairType type, Epoch minEpoch, @Nullable Ranges accordRepairedRanges)
    {
        this.type = type;
        this.minEpoch = minEpoch;
        this.accordRepairedRanges = accordRepairedRanges;
    }

    public static ConsensusMigrationRepairResult fromRepair(Epoch minEpoch, AccordRepairResult accordRepairResult, boolean dataRepaired, boolean paxosRepaired, boolean accordRepaired, boolean deadNodesExcluded, boolean incremental)
    {
        checkArgument(!accordRepaired || minEpoch.isAfter(Epoch.EMPTY), "Epoch should not be empty if Accord repairs was performed");
        if (deadNodesExcluded) return INELIGIBLE;
        boolean eligibleAccordRepair = accordRepaired && accordRepairResult != null && accordRepairResult.maxHlc != IAccordService.NO_HLC;
        Ranges accordRepairedRanges = eligibleAccordRepair ? accordRepairResult.repairedRanges : null;
        // Incremental repair won't flush after Paxos repair (which is at QUORUM) and then be picked up in the incremental repair
        // and thus won't be repaired at ALL which is what Accord needs
        if (incremental)
            paxosRepaired = false;
        return new ConsensusMigrationRepairResult(new ConsensusMigrationRepairType(dataRepaired, paxosRepaired, eligibleAccordRepair), minEpoch, accordRepairedRanges);
    }
}
