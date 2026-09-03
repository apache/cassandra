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

import javax.annotation.Nullable;

import org.apache.cassandra.tcm.Epoch;

/**
 * Tracks repair eligibility for mutation tracking migration advancement.
 *
 * // TODO: merge this with the accord migration state
 */
public class MutationTrackingMigrationRepairResult
{
    private static final MutationTrackingMigrationRepairResult DEAD_NODES_EXCLUDED =
        new MutationTrackingMigrationRepairResult(Epoch.EMPTY, false, "dead nodes were excluded from the repair");
    private static final MutationTrackingMigrationRepairResult PREVIEW =
        new MutationTrackingMigrationRepairResult(Epoch.EMPTY, false, "the repair was a preview");
    private static final MutationTrackingMigrationRepairResult NOT_INCREMENTAL =
        new MutationTrackingMigrationRepairResult(Epoch.EMPTY, false,
                                                 "the repair was not incremental, so it synced the pre-migration data " +
                                                 "without marking it repaired; migration requires incremental repair");

    public final Epoch minEpoch;
    public final boolean eligible;

    /** Why this repair cannot contribute to migration, for logging. Null when eligible. */
    @Nullable
    public final String ineligibleReason;

    private MutationTrackingMigrationRepairResult(Epoch minEpoch, boolean eligible, @Nullable String ineligibleReason)
    {
        this.minEpoch = minEpoch;
        this.eligible = eligible;
        this.ineligibleReason = ineligibleReason;
    }

    /**
     * Only an incremental repair may advance migration. A full repair syncs the pre-migration data but leaves it
     * unrepaired, and nothing can promote it afterwards: reconciliation has no ids to work from, and once the range is
     * migrated incremental repair no longer anticompacts it. Incremental repair marks exactly the ranges it verified,
     * which is why migration needs no separate promotion step at completion.
     *
     * A tracked keyspace with no migration in progress has its incremental flag cleared by
     * {@link org.apache.cassandra.repair.RepairCoordinator}, but such a repair never reaches this check: the handler
     * returns earlier because the keyspace is not migrating.
     */
    public static MutationTrackingMigrationRepairResult fromRepair(Epoch minEpoch,
                                                                  boolean deadNodesExcluded,
                                                                  boolean isPreview,
                                                                  boolean isIncremental)
    {
        if (deadNodesExcluded) return DEAD_NODES_EXCLUDED;
        if (isPreview) return PREVIEW;
        if (!isIncremental) return NOT_INCREMENTAL;
        return new MutationTrackingMigrationRepairResult(minEpoch, true, null);
    }
}
