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

package org.apache.cassandra.db.compaction;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Which compaction strategy holder an sstable belongs to.
 * <p>
 * Sstables are grouped into separate silos that aren't compacted together for correctness reasons related to
 * their replication and anti-entropy mechanisms. The group assignment is purely a function of sstable metadata and
 * this enum lists the top level buckets and contains the logic for classifying a given sstable
 */
public enum CompactionGroup
{
    UNREPAIRED, // untracked, unrepaired data
    PENDING_REPAIR,  // sstables currently involved in incremental repair - contains sub buckets per repair session
    UNRECONCILED,  // tracked data awaiting reconciliation - contains sub buckets per set of activated transfers
    REPAIRED;  // incrementall repaired or fully reconciled. Data in each bucket should eventually be promoted here

    public static CompactionGroup of(SSTableReader sstable)
    {
        StatsMetadata metadata = sstable.getSSTableMetadata();
        return of(metadata.repairedAt, metadata.pendingRepair, metadata.coordinatorLogOffsets);
    }

    public static CompactionGroup of(long repairedAt, TimeUUID pendingRepair, ImmutableCoordinatorLogOffsets offsets)
    {
        boolean isRepaired = repairedAt != ActiveRepairService.UNREPAIRED_SSTABLE;
        boolean isPendingRepair = pendingRepair != ActiveRepairService.NO_PENDING_REPAIR;
        Preconditions.checkArgument(!(isRepaired && isPendingRepair),
                                    "SSTables cannot be both repaired and pending repair");

        if (isRepaired)
            return REPAIRED;

        if (isPendingRepair)
            return PENDING_REPAIR;

        // Transfers and mutations both go here. Which sub bucket an sstable lands in is
        // TrackedCompactionManager's business, keyed on ImmutableCoordinatorLogOffsets.transferSiloKey().
        if (offsets != null && !offsets.isEmpty())
            return UNRECONCILED;

        return UNREPAIRED;
    }
}
