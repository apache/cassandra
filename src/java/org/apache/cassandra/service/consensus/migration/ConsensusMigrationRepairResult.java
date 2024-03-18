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
import org.apache.cassandra.tcm.Epoch;

import static com.google.common.base.Preconditions.checkArgument;

public class ConsensusMigrationRepairResult
{
    private static final ConsensusMigrationRepairResult INELIGIBLE = new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.ineligible, Epoch.EMPTY, null);
    public final ConsensusMigrationRepairType type;
    public final Epoch minEpoch;
    @Nullable
    public final Ranges barrieredRanges;

    private ConsensusMigrationRepairResult(ConsensusMigrationRepairType type, Epoch minEpoch, @Nullable Ranges barrieredRanges)
    {
        this.type = type;
        this.minEpoch = minEpoch;
        this.barrieredRanges = barrieredRanges;
    }

    public static ConsensusMigrationRepairResult fromRepair(Epoch minEpoch, Ranges barrieredRanges, boolean paxosAndDataRepaired, boolean accordRepaired, boolean deadNodesExcluded)
    {
        checkArgument((!paxosAndDataRepaired && !accordRepaired) || minEpoch.isAfter(Epoch.EMPTY), "Epoch should not be empty if Paxos and regular repairs were performed");

        if (deadNodesExcluded) return INELIGIBLE;
        if (paxosAndDataRepaired && accordRepaired) return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.either, minEpoch, barrieredRanges);
        if (paxosAndDataRepaired) return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.paxos, minEpoch, barrieredRanges);
        if (accordRepaired) return new ConsensusMigrationRepairResult(ConsensusMigrationRepairType.accord, minEpoch, barrieredRanges);
        return INELIGIBLE;
    }

    public static ConsensusMigrationRepairResult fromPaxosOnlyRepair(Epoch minEpoch, boolean deadNodesExcluded)
    {
        return fromRepair(minEpoch, null, false, false, deadNodesExcluded);
    }

    public static ConsensusMigrationRepairResult fromAccordOnlyRepair(Epoch minEpoch, Ranges barrieredRanges, boolean deadNodesExcluded)
    {
        return fromRepair(minEpoch, barrieredRanges, false, true, deadNodesExcluded);
    }
}
