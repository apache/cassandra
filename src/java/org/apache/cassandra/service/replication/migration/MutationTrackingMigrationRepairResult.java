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

package org.apache.cassandra.service.replication.migration;

import org.apache.cassandra.tcm.Epoch;

/**
 * Tracks repair eligibility for mutation tracking migration advancement.
 *
 * // TODO: merge this with the accord migration state
 */
public class MutationTrackingMigrationRepairResult
{
    private static final MutationTrackingMigrationRepairResult INELIGIBLE = new MutationTrackingMigrationRepairResult(Epoch.EMPTY, false);

    public final Epoch minEpoch;
    public final boolean eligible;

    private MutationTrackingMigrationRepairResult(Epoch minEpoch, boolean eligible)
    {
        this.minEpoch = minEpoch;
        this.eligible = eligible;
    }

    public static MutationTrackingMigrationRepairResult fromRepair(Epoch minEpoch, boolean deadNodesExcluded, boolean isPreview)
    {
        if (deadNodesExcluded) return INELIGIBLE;
        if (isPreview) return INELIGIBLE;
        return new MutationTrackingMigrationRepairResult(minEpoch, true);
    }
}
